#!/usr/bin/env python3
"""Offline same-information comparison on the supplied function history.

No sockets, subprocesses, imports of retained upstream programs, or remote I/O.
Canonical requests/replies are serialized and decoded at every local RPC. This
measures payloads and instrumented work, not network performance. The base-only
query rule is fixed here before outcomes are computed; no history labels enter it.
"""
from __future__ import annotations
import asyncio
from collections import Counter
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import time

from src.checker import Checker
from src.continuation import count_work, merge_tokens, token_from_prefix
from src.continuation_checker import ContinuationChecker
from src.continuation_coordinator import ContinuationSession, FeedStore, fetch_events, fetch_prefix
from src.corpus import build
from src.model import PostingIndex, key, score
from src.real_history import build_real_history
from src.service import Replica, wire

ROOT = Path(__file__).resolve().parent
OWNERS = [[0, 1], [2, 3], [4, 5]]
POLICIES = ('continuation', 'extra-local', 'extra-global')
WORK_KEYS = ('assignment_visits', 'buffer_rows_visited', 'score_evaluations',
             'candidate_sort_rows', 'merge_rows_visited', 'tail_tests')


def independent_queries(base):
    """Eight base-only hash-ordered seeds/project, rare base labels, no alternates."""
    frequency = Counter()
    groups = {}
    for state in base['initial']:
        for ident, body in state.items():
            frequency.update(base['payloads'][body]['features'])
            groups.setdefault(ident.split('/')[0], []).append((ident, body))
    queries = []
    for repo, items in sorted(groups.items()):
        items = sorted(items, key=lambda item: hashlib.sha256(
            ('P016/base-only/v1:' + item[0]).encode()).digest())[:8]
        for ident, body in items:
            terms = sorted(base['payloads'][body]['features'],
                           key=lambda term: (frequency[term], term))[:4]
            queries.append({'id': f'iq{len(queries):03d}', 'plan': [terms], 'k': 5,
                            'seed_id': ident, 'source_body': body, 'origin_repo': repo})
    return queries


class CountedIndex(PostingIndex):
    def __init__(self, state, payloads, counts):
        self.counts = counts
        super().__init__(state, payloads)

    def put(self, ident, body):
        old = self.state.get(ident)
        # One membership operation for each loop iteration in PostingIndex.put.
        self.counts['posting_membership_updates'] += (
            len(self.payloads[old]['features']) if old is not None else 0)
        self.counts['posting_membership_updates'] += (
            len(self.payloads[body]['features']) if body is not None else 0)
        self.counts['identifier_assignments'] += 1
        super().put(ident, body)

    def rows(self, plan, exclude=None, *, limit=None):
        return super().rows(plan, exclude, limit=limit, work=self.counts)


class OfflineWire:
    def __init__(self, fixture):
        self.counts = Counter()
        self.replicas = []
        for shard in range(3):
            replica = Replica(shard*2, shard, fixture['initial'][shard], fixture['payloads'])
            replica.search = CountedIndex(fixture['initial'][shard], fixture['payloads'], self.counts)
            replica.index = replica.search.state
            self.replicas.append(replica)
        self.evidence = {}
        self.phase = 'acquisition'
        self.costs = {phase: Counter() for phase in ('acquisition', 'feed', 'refill')}

    async def rpc(self, node, request):
        raw = wire(request)
        frozen = json.loads(raw)
        started = time.process_time_ns()
        reply = self.replicas[node//2].handle(frozen)
        elapsed = time.process_time_ns() - started
        encoded = wire(reply)
        result = json.loads(encoded)
        ident = len(self.evidence) + 1
        self.evidence[ident] = {'node': node, 'request': frozen, 'reply': result}
        self.costs[self.phase].update(bytes=len(raw)+len(encoded), messages=2,
                                     source_cpu_ns=elapsed, returned_rows=len(result.get('rows', [])))
        return ident, result

    def deliver(self, event, lag):
        replica = self.replicas[event['shard']]
        reply = replica.handle(json.loads(wire({'op': 'receive', 'event': event})))
        if not reply.get('ok'):
            raise AssertionError(reply)
        reply = replica.handle({'op': 'advance', 'to': max(0, event['seq']-lag)})
        if not reply.get('ok'):
            raise AssertionError(reply)


class ExtraCandidates(ContinuationSession):
    """Overfetch+refill, independently updated; local or global stopping rule.

    Uses the SAME prefix, tail, feed, initial capacity and conservative k+d
    sizing as continuation. Global stopping is the closest information-equivalent
    variant, not a claimed independent novelty or a deliberately weak cache.
    """
    def __init__(self, payloads, feed, capacity, work, *, global_stop=False):
        super().__init__(payloads, feed, capacity, work=work)
        self.global_stop = global_stop

    def update_buffer(self, token, target):
        events = self.feed.receipt(token['shard'], token['at'], target)['events']
        if not events:
            return deepcopy(token)
        final = {}
        for event in events:
            for ident, body in event['changes']:
                count_work(self.work, 'assignment_visits')
                final[ident] = body
        candidates = {}
        for row in token['rows']:
            count_work(self.work, 'buffer_rows_visited')
            if row['id'] not in final:
                candidates[row['id']] = deepcopy(row)
        for ident, body in final.items():
            if body is not None:
                count_work(self.work, 'score_evaluations')
                value = score(self.payloads[body]['features'], token['query']['plan'])
                if value > 0:
                    candidates[ident] = {'id': ident, 'body': body, 'score': value}
        ordered = sorted(candidates.values(), key=key)
        count_work(self.work, 'candidate_sort_rows', len(ordered))
        capacity = token['capacity']
        bounds = [tuple(token['tail'])] if token['tail'] is not None else []
        if len(ordered) > capacity:
            bounds.append(key(ordered[capacity]))
        return {**deepcopy(token), 'at': target, 'rows': ordered[:capacity],
                'tail': list(min(bounds)) if bounds else None,
                'generation': token['generation']+1}

    async def query(self, network, query, cut, epoch, owners):
        current = self.tokens[query['id']]
        for shard, token in list(current.items()):
            if token['at'] < cut[shard]:
                current[shard] = self.update_buffer(token, cut[shard])
        repairs, attempted = {}, set()
        result = None
        while True:
            if self.global_stop:
                result = merge_tokens(query, cut, epoch, current, work=self.work)
                blockers = result['rank_blockers']
            else:
                blockers = []
                for shard, token in current.items():
                    count_work(self.work, 'tail_tests')
                    rows, tail = token['rows'], token['tail']
                    if tail is not None and (len(rows) < query['k'] or
                                            key(rows[query['k']-1]) >= tuple(tail)):
                        blockers.append(shard)
            eligible = [s for s in blockers if s not in attempted]
            if not eligible:
                break
            shard = min(eligible, key=lambda s: tuple(current[s]['tail']))
            attempted.add(shard)
            final = {}
            suffix = self.feed.receipt(shard, self.feed.base[shard], cut[shard])['events']
            for event in suffix:
                for ident, body in event['changes']:
                    count_work(self.work, 'assignment_visits')
                    final[ident] = body
            required = query['k'] + len(final)
            length = max(self.base_capacity, min(64, required))
            ident, prefix = await fetch_prefix(network, query, shard, epoch, owners, length,
                eligible=lambda p: p['at'] <= cut[shard] and
                    self.feed.covers(shard, p['at'], cut[shard]))
            if prefix is None:
                continue
            token = token_from_prefix(query, prefix, length)
            current[shard] = self.update_buffer(token, cut[shard])
            repairs[str(shard)] = {'receipt': ident, 'capacity': length}
        if not self.global_stop:
            result = merge_tokens(query, cut, epoch, current, work=self.work)
        result['repairs'] = repairs
        return result, repairs


async def trial(fixture, queries, policy, capacity, lag):
    network = OfflineWire(fixture)
    work = Counter()
    feed = FeedStore()
    session = (ContinuationSession(fixture['payloads'], feed, capacity, work=work)
               if policy == 'continuation' else ExtraCandidates(fixture['payloads'], feed,
                   capacity, work, global_stop=policy == 'extra-global'))
    checker = ContinuationChecker(fixture['payloads'])
    oracle = Checker(fixture['payloads'])
    check_ns = policy_ns = 0
    for query in queries:
        stamp = time.process_time_ns()
        specs = await session.acquire(network, query, [0, 0, 0], 0, OWNERS)
        policy_ns += time.process_time_ns()-stamp
        stamp = time.process_time_ns()
        for spec in specs.values():
            checker.install_prefix(query, network.evidence[spec['receipt']], OWNERS, 0, spec['capacity'])
        check_ns += time.process_time_ns()-stamp
    peak_state = len(wire({'tokens': session.tokens, 'feed': feed.logical_state()}))
    records = []
    previous = {q['id']: oracle.oracle(fixture['initial'], fixture['history'], [0, 0, 0], q)
                for q in queries}
    schedule = fixture.get('schedule', [[0, e['seq'], 0] for e in fixture['history'][1]])
    previous_cut = [0, 0, 0]
    for sequence, cut in enumerate(schedule, 1):
        for shard in range(3):
            for event in fixture['history'][shard][previous_cut[shard]:cut[shard]]:
                network.deliver(event, lag)
        previous_cut = list(cut)
        network.phase = 'feed'
        for shard in range(3):
            if feed.frontiers[shard] == cut[shard]:
                continue
            stamp = time.process_time_ns()
            ident, _ = await fetch_events(network, feed, shard, cut[shard], 0, OWNERS)
            policy_ns += time.process_time_ns()-stamp
            stamp = time.process_time_ns()
            checker.accept_events(network.evidence[ident], OWNERS, 0)
            check_ns += time.process_time_ns()-stamp
        network.phase = 'refill'
        for query in queries:
            stamp = time.process_time_ns()
            result, repairs = await session.query(network, query, cut, 0, OWNERS)
            policy_ns += time.process_time_ns()-stamp
            stamp = time.process_time_ns()
            checker.check_result(result, query, cut, 0, OWNERS, network.evidence, repairs)
            check_ns += time.process_time_ns()-stamp
            exact = oracle.oracle(fixture['initial'], fixture['history'], cut, query)
            if result['status'] != 'complete' or result['rows'] != exact:
                raise AssertionError((policy, query['id'], sequence, result))
            if oracle.sound_rows(result['rows'], fixture['initial'], fixture['history'], cut, query):
                raise AssertionError('unsound rows')
            records.append({'sequence': sequence, 'query': query['id'], 'result': result,
                            'oracle': exact, 'changed': exact != previous[query['id']]})
            previous[query['id']] = exact
        peak_state = max(peak_state, len(wire({'tokens': session.tokens, 'feed': feed.logical_state()})))
    source_cpu = sum(cost['source_cpu_ns'] for cost in network.costs.values())
    summary = {'policy': policy, 'capacity': capacity, 'index_lag': lag, 'calls': len(records),
        'exact_complete': len(records), 'false_complete': 0, 'unsound_rows': 0,
        'oracle_changes': sum(r['changed'] for r in records),
        'refill_shards': sum(len(r['result']['repairs']) for r in records),
        'source_costs': {phase: dict(cost) for phase, cost in network.costs.items()},
        'total_source_bytes': sum(cost['bytes'] for cost in network.costs.values()),
        'source_work': dict(network.counts), 'coordinator_work': {k: work[k] for k in WORK_KEYS},
        'max_logical_state_bytes': peak_state, 'source_cpu_ns': source_cpu,
        'coordinator_cpu_ns_excluding_source': policy_ns-source_cpu, 'checker_cpu_ns': check_ns}
    return {'summary': summary, 'queries': queries, 'records': records,
            'transcript': network.evidence, 'initial': fixture['initial'], 'history': fixture['history']}


async def campaign():
    started = time.monotonic()
    base = build(ROOT)
    # Freeze queries before even building the projected history.
    independent = independent_queries(base)
    fixture = build_real_history(ROOT, base)
    groups = {'ordinary': fixture['queries'][:64], 'directed': fixture['targeted_queries'],
              'base-independent': independent}
    trials = []
    for group, queries in groups.items():
        local_fixture = deepcopy(fixture)
        if group == 'base-independent':
            seeds = {q['seed_id'] for q in queries}
            for state in local_fixture['initial']:
                for ident in seeds:
                    state.pop(ident, None)
        for capacity in (5, 10, 16):
            for lag in (0, 2):
                matched = []
                for policy in POLICIES:
                    trial_result = await trial(local_fixture, queries, policy, capacity, lag)
                    trial_result['summary']['query_group'] = group
                    trials.append(trial_result)
                    matched.append(trial_result)
                # Check per-invocation answer agreement, never just aggregate counts.
                outputs = [[r['result']['rows'] for r in t['records']] for t in matched]
                if outputs[0] != outputs[1] or outputs[0] != outputs[2]:
                    raise AssertionError('matched answers differ')
                if group == 'ordinary' and matched[0]['summary']['oracle_changes'] != 0:
                    raise AssertionError('original ordinary query fact changed')
                if group == 'directed' and matched[0]['summary']['oracle_changes'] != 10:
                    raise AssertionError('original directed query fact changed')
    report = {'schema': 'same-information-incremental-v1', 'trials': [t['summary'] for t in trials],
        'wall_seconds': time.monotonic()-started,
        'query_rule': '8 hash-ordered indexed base bodies/project; 4 rarest base labels; no history information; base seed IDs excluded',
        'scope': 'offline canonical JSON payloads, 3 logical shards, no faults, six supplied function-history events; not socket latency or production performance',
        'cost_scope': 'acquisition/feed/refill wire payloads; source initial posting construction and applied update membership operations counted; writer dissemination, body transport, oracle, client delivery excluded',
        'work_scope': 'posting visits and positive rows built; posting membership operations; assignment, buffer, score, sort-input, merge and tail-test counts, not every instruction or sort comparison',
        'history': fixture['git_history']}
    out = ROOT/'results/incremental-comparison.json'
    out.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    with gzip.open(ROOT/'results/incremental-comparison-trace.json.gz', 'wt', encoding='utf-8') as stream:
        json.dump({'report': report, 'trials': trials}, stream, sort_keys=True, separators=(',', ':'))
    print(json.dumps({'result': 'PASS', 'trials': len(trials),
                      'calls': sum(t['summary']['calls'] for t in trials),
                      'wall_seconds': report['wall_seconds'], 'summary': str(out)}, sort_keys=True))


if __name__ == '__main__':
    asyncio.run(campaign())
