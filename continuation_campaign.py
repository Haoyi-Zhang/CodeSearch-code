#!/usr/bin/env python3
"""Bounded standing-query campaign for shared-feed continuation certificates."""
from __future__ import annotations
import argparse
import asyncio
import copy
import gzip
import json
import os
import resource
import time
from collections import defaultdict
from pathlib import Path

from src.corpus import build
from src.checker import Checker
from src.continuation_checker import ContinuationChecker
from src.continuation_coordinator import ContinuationSession, FeedStore, fetch_events
from src.model import PostingIndex, apply, key
from src.service import Replica, Network, wire
from src.coordinator import baseline
from src.workload import Writer, CASES

ROOT = Path(__file__).resolve().parent
POLICIES = ('continuation','overlay','cut-cache','mirror','unsafe-stale')
CONTINUATION_CASES = CASES + ('rank-churn',)


def difference(after: dict, before: dict) -> dict:
    return {name: after[name] - before[name] for name in before}


def add_cost(total: dict, delta: dict, wall_ns: int = 0, checker_ns: int = 0,
             client_ns: int = 0) -> None:
    for name in ('bytes','messages','virtual_ms','server_cpu_ns','dropped_requests_or_replies'):
        total[name] += delta.get(name, 0)
    total['wall_ns'] += wall_ns
    total['checker_ns'] += checker_ns
    total['client_ns'] += client_ns


def standing_queries(fixture: dict) -> list[dict]:
    by_repo: dict[str, list[dict]] = defaultdict(list)
    for query in fixture['queries'][1:]:
        by_repo[query['origin_repo']].append(copy.deepcopy(query))
    chosen = []
    for repo in sorted(by_repo):
        selected = sorted(by_repo[repo], key=lambda q:q['id'])[:2]
        if len(selected) != 2:
            raise ValueError('standing-query stratum has fewer than two queries')
        chosen.extend(selected)
    if len(chosen) != 16:
        raise ValueError('expected sixteen standing queries')
    return chosen


def active_queries(queries: list[dict], tick: int, active: int) -> list[dict]:
    if not 1 <= active <= len(queries):
        raise ValueError('active query count')
    start = ((tick - 1) * active) % len(queries)
    return [queries[(start + offset) % len(queries)] for offset in range(active)]


def empty_total() -> dict:
    return {name:0 for name in ('bytes','messages','virtual_ms','server_cpu_ns',
                                'dropped_requests_or_replies','wall_ns','checker_ns','client_ns')}


def cache_state(cache: dict) -> dict:
    return {
        query: {str(shard): item for shard,item in sorted(shards.items())}
        for query,shards in sorted(cache.items())
    }


def mirror_state(indexes: list[PostingIndex]) -> list[dict]:
    answer = []
    for index in indexes:
        answer.append({
            'state': sorted(index.state.items()),
            'postings': {term:sorted(ids) for term,ids in sorted(index.postings.items())},
        })
    return answer


def assess(rows: list[dict], exact: list[dict], checker: Checker, fixture: dict,
           history: list[list[dict]], cut: list[int], query: dict, claimed: bool) -> dict:
    unsound = checker.sound_rows(rows, fixture['initial'], history, cut, query)
    exact_equal = rows == exact
    ids = {row['id'] for row in rows}; target_ids = {row['id'] for row in exact}
    bodies = {(row['id'],row['body']) for row in rows}
    target_bodies = {(row['id'],row['body']) for row in exact}
    return {
        'exact': exact_equal,
        'claimed_complete': claimed,
        'false_complete': claimed and not exact_equal,
        'unsound_rows': unsound,
        'returned_rows': len(rows),
        'body_recall': 1.0 if not target_bodies else len(bodies & target_bodies)/len(target_bodies),
        'id_recall': 1.0 if not target_ids else len(ids & target_ids)/len(target_ids),
    }


async def fetch_snapshot(network: Network, owners: list[list[int]], cut: list[int], epoch: int):
    indexes = []; receipts = []
    for shard in range(3):
        found = None
        for node in owners[shard]:
            ident, reply = await network.rpc(node, {'op':'snapshot','shard':shard,
                                                    'epoch':epoch,'at':cut[shard]})
            if reply and reply.get('kind') == 'snapshot':
                found = (ident, reply); break
        if found is None:
            raise RuntimeError('mirror snapshot unavailable')
        ident, reply = found
        receipts.append(ident)
        indexes.append(PostingIndex(dict(reply['state']), network.replicas[0].payloads))
    return indexes, receipts


async def cut_cache_query(network: Network, cache: dict, query: dict, cut: list[int],
                          epoch: int, owners: list[list[int]]) -> dict:
    current = cache.setdefault(query['id'], {})
    all_rows = {}; receipts = {}; covered = []
    for shard in range(3):
        item = current.get(shard)
        if item is None or item['epoch'] != epoch or item['cut'] != cut[shard]:
            item = None
            for node in owners[shard]:
                ident, reply = await network.rpc(node, {'op':'overlay','shard':shard,
                    'epoch':epoch,'query':query,'to':cut[shard]})
                if reply and reply.get('kind') == 'overlay':
                    item = {'epoch':epoch,'cut':cut[shard],'rows':copy.deepcopy(reply['rows']),
                            'receipt':ident}
                    current[shard] = item
                    break
        if item is not None and item['epoch'] == epoch and item['cut'] == cut[shard]:
            covered.append(shard); receipts[str(shard)] = item['receipt']
            for row in item['rows']:
                all_rows[row['id']] = row
    return {'rows':sorted(all_rows.values(),key=key)[:query['k']],
            'claim_complete':covered == [0,1,2], 'coverage':covered, 'receipts':receipts}


async def campaign(fixture: dict, case: str, seed: int, active: int = 4,
                   capacity: int = 8, output: Path | None = None) -> dict:
    started_wall = time.monotonic(); started_cpu = time.process_time()
    checker = Checker(fixture['payloads'])
    continuation_checker = ContinuationChecker(fixture['payloads'])
    writer = Writer(fixture, seed)
    standing = standing_queries(fixture)
    standing_by_id = {query['id']:query for query in standing}
    unsafe_cache = {query['id']:checker.oracle(fixture['initial'],[[],[],[]],[0,0,0],query)
                    for query in standing}
    owners = [[0,1],[2,3],[4,5]]; epoch = 0
    replicas = [Replica(i,i//2,fixture['initial'][i//2],fixture['payloads']) for i in range(6)]
    received = [set() for _ in range(6)]
    feed = FeedStore(epoch,[0,0,0])
    session = ContinuationSession(fixture['payloads'],feed,capacity)
    cut_cache: dict = {}
    totals = {policy:empty_total() for policy in POLICIES}
    setup_records = []; timeline = []; records = []
    peak_state = {policy:0 for policy in ('continuation','cut-cache','mirror')}

    async with Network(replicas) as net:
        net.delay_ms = {i:(i%3)*3 for i in range(6)}

        async def acquire_continuations(tag: str, cut: list[int]) -> None:
            before = net.counters(); wall = time.monotonic_ns(); check_ns = 0
            all_specs = {}
            for query in standing:
                specs = await session.acquire(net,query,cut,epoch,owners)
                if set(specs) != {'0','1','2'}:
                    raise RuntimeError('standing token acquisition incomplete')
                all_specs[query['id']] = specs
                for label,spec in specs.items():
                    ts = time.monotonic_ns()
                    continuation_checker.install_prefix(
                        query, net.evidence[spec['receipt']], owners, epoch, spec['capacity'])
                    check_ns += time.monotonic_ns() - ts
            elapsed = time.monotonic_ns() - wall
            delta = difference(net.counters(),before)
            add_cost(totals['continuation'],delta,elapsed,check_ns)
            setup_records.append({'kind':'continuation-acquisition','tag':tag,'cut':list(cut),
                                  'receipts':all_specs,'cost':{**delta,'wall_ns':elapsed,
                                                               'checker_ns':check_ns}})

        async def acquire_mirror(tag: str, cut: list[int]):
            before = net.counters(); wall = time.monotonic_ns()
            indexes, receipts = await fetch_snapshot(net,owners,cut,epoch)
            elapsed = time.monotonic_ns() - wall; delta = difference(net.counters(),before)
            add_cost(totals['mirror'],delta,elapsed,0,elapsed)
            setup_records.append({'kind':'mirror-snapshot','tag':tag,'cut':list(cut),
                                  'receipts':receipts,'cost':{**delta,'wall_ns':elapsed}})
            return indexes

        await acquire_continuations('initial',[0,0,0])
        mirror_indexes = await acquire_mirror('initial',[0,0,0])
        mirror_frontiers = [0,0,0]
        peak_state['continuation'] = len(wire({'tokens':session.tokens,
                                                         'feed':feed.logical_state()}))
        peak_state['mirror'] = len(wire(mirror_state(mirror_indexes)))

        for tick, driver_query in enumerate(writer.queries,1):
            if time.monotonic() - started_wall > 170:
                raise TimeoutError('continuation case exceeded 170 seconds')
            if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss > int(3.1*1024*1024):
                raise MemoryError('continuation case exceeded RSS bound')
            planned_active = active_queries(standing,tick,active)
            if case == 'rank-churn':
                # Directed stress: every tick mutates against a currently watched plan.
                writer.step(tick,planned_active[0],'fresh')
            else:
                writer.step(tick,driver_query,case)
            cut = writer.cut
            if case == 'partition':
                net.blocked = {2,3} if 20 <= tick <= 35 else set()
            if case == 'crash' and tick == 20:
                for node in (0,4): await net.rpc(node,{'op':'crash'},administrative=True)
            if case == 'crash' and tick == 28:
                for node in (0,4): await net.rpc(node,{'op':'restart'},administrative=True)

            control_before = net.counters(); control_wall = time.monotonic_ns()
            reconfigured = False
            if case == 'rebalance' and tick == 32:
                epoch = 1; owners = [[2,3],[4,5],[0,1]]; reconfigured = True
                for shard in range(3):
                    for node in owners[shard]:
                        _, reply = await net.rpc(node,{'op':'install','epoch':epoch,'shard':shard,
                            'at':cut[shard],'state':writer.views[shard]},administrative=True)
                        if not reply or not reply.get('ok'):
                            raise RuntimeError('epoch install failed')
                        received[node] = set(range(1,cut[shard]+1))
                feed.reset(epoch,cut); session.reset(); cut_cache.clear()
                continuation_checker.reset_epoch(epoch,cut)
            else:
                # Retry all retained events; reverse order exercises pending queues.
                for shard in range(3):
                    for node in owners[shard]:
                        pending = [event for event in writer.history[shard]
                                   if event['seq'] not in received[node]]
                        for event in reversed(pending):
                            if case == 'log-gap' and shard == 1 and event['seq'] % 11 == 0 \
                                    and tick < event['seq'] + 4:
                                continue
                            lost = tick % 17 == 0 and event['seq'] == cut[shard] and node % 2 == 0
                            _, reply = await net.rpc(node,{'op':'receive','event':event},lost_reply=lost)
                            if reply and reply.get('ok'):
                                received[node].add(event['seq'])
                        lag = 0 if case == 'fresh' else 8 if case in {'index-lag','hot-shard'} else 3
                        await net.rpc(node,{'op':'advance','to':max(0,cut[shard]-lag)})
                        if case == 'compaction' and tick % 16 == 0:
                            await net.rpc(node,{'op':'compact','to':max(0,cut[shard]-lag)})
                if tick % 7 == 0 and writer.history[0]:
                    await net.rpc(owners[0][0],{'op':'receive','event':writer.history[0][-1]})
            control_elapsed = time.monotonic_ns() - control_wall
            control_cost = difference(net.counters(),control_before)

            if reconfigured:
                await acquire_continuations('rebalance',cut)
                mirror_indexes = await acquire_mirror('rebalance',cut)
                mirror_frontiers = list(cut)

            feed_receipts = {}; feed_cost = empty_total()
            feed_wall_start = time.monotonic_ns(); feed_check_ns = feed_client_ns = 0
            for shard in range(3):
                if feed.frontiers[shard] == cut[shard]:
                    continue
                before = net.counters(); wall = time.monotonic_ns()
                ident, reply = await fetch_events(net,feed,shard,cut[shard],epoch,owners)
                elapsed = time.monotonic_ns() - wall
                delta = difference(net.counters(),before)
                add_cost(feed_cost,delta,elapsed)
                if reply is not None:
                    feed_receipts[str(shard)] = ident
                    ts = time.monotonic_ns()
                    continuation_checker.accept_events(net.evidence[ident],owners,epoch)
                    feed_check_ns += time.monotonic_ns() - ts
                    ts = time.monotonic_ns()
                    for event in reply['events']:
                        mirror_indexes[shard].apply(event)
                    mirror_frontiers[shard] = reply['hi']
                    feed_client_ns += time.monotonic_ns() - ts
            feed_cost['checker_ns'] = feed_check_ns
            feed_cost['client_ns'] = feed_client_ns
            # Each alternative deployment needs the same feed; assign it to both.
            add_cost(totals['continuation'],feed_cost,feed_cost['wall_ns'],feed_check_ns,0)
            add_cost(totals['mirror'],feed_cost,feed_cost['wall_ns'],0,feed_client_ns)

            query_records = []
            active_set = planned_active
            for q_index,query in enumerate(active_set):
                oracle_start = time.monotonic_ns()
                exact = checker.oracle(fixture['initial'],writer.history,cut,query)
                oracle_ns = time.monotonic_ns() - oracle_start
                order = ('continuation','overlay','cut-cache')
                shift = (tick + q_index) % len(order)
                order = order[shift:] + order[:shift]
                observations = {}
                for policy in order:
                    before = net.counters(); wall = time.monotonic_ns(); check_ns = 0; client_ns = 0
                    if policy == 'continuation':
                        result, repairs = await session.query(net,query,cut,epoch,owners)
                        ts = time.monotonic_ns()
                        continuation_checker.check_result(
                            result,query,cut,epoch,owners,net.evidence,repairs)
                        check_ns = time.monotonic_ns() - ts
                        rows = result['rows']; claimed = result['status'] == 'complete'
                        response = result
                    elif policy == 'overlay':
                        response = await baseline(net,query,cut,epoch,owners,'overlay')
                        ts = time.monotonic_ns()
                        checker.check_overlay(response,net.evidence,query,cut,epoch,owners)
                        check_ns = time.monotonic_ns() - ts
                        rows = response['rows']; claimed = response['claim_complete']
                    else:
                        response = await cut_cache_query(net,cut_cache,query,cut,epoch,owners)
                        ts = time.monotonic_ns()
                        checker.check_overlay(response,net.evidence,query,cut,epoch,owners)
                        check_ns = time.monotonic_ns() - ts
                        rows = response['rows']; claimed = response['claim_complete']
                    elapsed = time.monotonic_ns() - wall
                    delta = difference(net.counters(),before)
                    add_cost(totals[policy],delta,elapsed,check_ns,client_ns)
                    metrics = assess(rows,exact,checker,fixture,writer.history,cut,query,claimed)
                    metrics.update({**delta,'wall_ns':elapsed,'checker_ns':check_ns,
                                    'certificate_bytes':len(wire(response))})
                    if metrics['unsound_rows'] or metrics['false_complete']:
                        raise AssertionError((case,seed,tick,query['id'],policy,metrics))
                    observations[policy] = {'metrics':metrics,'response':response}

                # Full mirror uses exactly the same query-independent feed.
                wall = time.monotonic_ns(); local_rows = {}; covered = []
                for shard in range(3):
                    if mirror_frontiers[shard] != cut[shard]:
                        continue
                    covered.append(shard)
                    for row in mirror_indexes[shard].rows(query['plan'])[:query['k']]:
                        local_rows[row['id']] = row
                mirror_rows = sorted(local_rows.values(),key=key)[:query['k']]
                elapsed = time.monotonic_ns() - wall
                add_cost(totals['mirror'],{},elapsed,0,elapsed)
                mirror_metrics = assess(mirror_rows,exact,checker,fixture,writer.history,cut,query,
                                        covered == [0,1,2])
                mirror_metrics.update({'bytes':0,'messages':0,'virtual_ms':0,'server_cpu_ns':0,
                                       'dropped_requests_or_replies':0,'wall_ns':elapsed,
                                       'checker_ns':0,'certificate_bytes':0})
                if mirror_metrics['unsound_rows'] or mirror_metrics['false_complete']:
                    raise AssertionError((case,seed,tick,query['id'],'mirror',mirror_metrics))
                observations['mirror'] = {'metrics':mirror_metrics,
                                          'response':{'rows':mirror_rows,'coverage':covered,
                                                      'claim_complete':covered == [0,1,2]}}

                stale_rows = copy.deepcopy(unsafe_cache[query['id']])
                stale_metrics = assess(stale_rows,exact,checker,fixture,writer.history,cut,query,True)
                stale_metrics.update({'bytes':0,'messages':0,'virtual_ms':0,'server_cpu_ns':0,
                                      'dropped_requests_or_replies':0,'wall_ns':0,'checker_ns':0,
                                      'certificate_bytes':0})
                observations['unsafe-stale'] = {'metrics':stale_metrics,
                                                'response':{'rows':stale_rows,'claim_complete':True}}
                records.append({'tick':tick,'query':query,'cut':list(cut),'epoch':epoch,
                                'oracle':exact,'oracle_wall_ns':oracle_ns,
                                'observations':observations})
                query_records.append(query['id'])

            # Once every standing token has consumed a feed prefix, both the
            # coordinator and optional stateful checker can discard it.
            consumed = session.minimum_frontiers()
            compact_to = feed.safe_compaction_floors(consumed)
            feed.compact(compact_to)
            continuation_checker.compact_events(compact_to)
            continuation_state = {'tokens':session.tokens,'feed':feed.logical_state()}
            peak_state['continuation'] = max(peak_state['continuation'],len(wire(continuation_state)))
            peak_state['cut-cache'] = max(peak_state['cut-cache'],len(wire(cache_state(cut_cache))))
            peak_state['mirror'] = max(peak_state['mirror'],len(wire(mirror_state(mirror_indexes))))
            timeline.append({'tick':tick,'cut':list(cut),'epoch':epoch,'owners':copy.deepcopy(owners),
                             'blocked':sorted(net.blocked),'active_queries':query_records,
                             'reconfigured':reconfigured,'feed_receipts':feed_receipts,
                             'feed_frontiers':list(feed.frontiers),
                             'mirror_frontiers':list(mirror_frontiers),
                             'control':{**control_cost,'wall_ns':control_elapsed},
                             'feed_cost':feed_cost})

        # Final source convergence is common control, not attributed query cost.
        net.blocked = set(); repair_before = net.counters(); repair_wall = time.monotonic_ns()
        for shard in range(3):
            for node in owners[shard]:
                if replicas[node].crashed:
                    await net.rpc(node,{'op':'restart'},administrative=True)
                for event in writer.history[shard]:
                    if event['seq'] > replicas[node].floor:
                        await net.rpc(node,{'op':'receive','event':event})
                await net.rpc(node,{'op':'advance','to':cut[shard]})
                await net.rpc(node,{'op':'compact','to':cut[shard]})
                if replicas[node].index != writer.views[shard]:
                    raise AssertionError('final convergence')
        repair_cost = difference(net.counters(),repair_before)
        repair = {**repair_cost,'wall_ns':time.monotonic_ns()-repair_wall,
                  'converged_replicas':6,'cut':list(cut),
                  'retained_events_after':[len(replica.journal) for replica in replicas],
                  'pending_events_after':[len(replica.pending) for replica in replicas]}

        trace = {
            'case':case,'seed':seed,'active_per_tick':active,'base_capacity':capacity,
            'standing_query_ids':[query['id'] for query in standing],
            'standing_queries':standing,'initial':fixture['initial'],'history':writer.history,
            'records':records,'timeline':timeline,'setup':setup_records,
            'policy_totals':totals,'peak_logical_state_bytes':peak_state,
            'transcript':net.evidence,'attempts':net.attempts,'repair':repair,
            'resources':{'wall_seconds':time.monotonic()-started_wall,
                         'cpu_seconds':time.process_time()-started_cpu,
                         'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                         'workers':1,'logical_tcp_nodes':6,
                         'peak_journal_events':max(replica.peak_journal for replica in replicas),
                         'peak_pending_events':max(replica.peak_pending for replica in replicas)},
            'scope':'six loopback endpoints in one process; standing-query schedule and repository changes are deterministic fixtures',
            'cost_scope':'source RPC bytes exclude common result delivery and source-body fetch; shared event-feed bytes are assigned independently to continuation and mirror',
            'state_scope':'continuation state includes retained tokens plus a compacted shared feed retaining one admitted journal window for lagged repair; mirror state includes full local indexes; optional checker duplication is excluded',
        }

    if output is not None:
        output.parent.mkdir(parents=True,exist_ok=True)
        temporary = output.with_suffix(output.suffix+'.tmp')
        try:
            with gzip.open(temporary,'wt',encoding='utf-8') as stream:
                json.dump(trace,stream,sort_keys=True,separators=(',',':'));stream.write('\n')
            os.replace(temporary,output)
        finally:
            if temporary.exists(): temporary.unlink()
    report = {
        'case':case,'seed':seed,'queries':len(records),'active_per_tick':active,
        'capacity':capacity,'wall_seconds':trace['resources']['wall_seconds'],
        'peak_rss_kib':trace['resources']['peak_rss_kib'],
        'mean_bytes':{policy:totals[policy]['bytes']/len(records) for policy in POLICIES},
        'peak_state_bytes':peak_state,
    }
    print(json.dumps(report,sort_keys=True),flush=True)
    return trace


async def main(args) -> None:
    fixture = build(ROOT)
    cases = CONTINUATION_CASES if args.all else (args.case,)
    seeds = (1,2,3) if args.all else (args.seed,)
    for case in cases:
        for seed in seeds:
            stem = f'continuation-{case}-seed{seed}'
            if args.active != 4: stem += f'-active{args.active}'
            if args.capacity != 8: stem += f'-capacity{args.capacity}'
            output = ROOT/'results/continuation-traces'/(stem+'.json.gz')
            await campaign(fixture,case,seed,args.active,args.capacity,output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',choices=CONTINUATION_CASES,default='index-lag')
    parser.add_argument('--seed',type=int,choices=(1,2,3),default=1)
    parser.add_argument('--active',type=int,choices=(1,4,16),default=4)
    parser.add_argument('--capacity',type=int,choices=(5,8,16,32),default=5)
    parser.add_argument('--all',action='store_true')
    asyncio.run(main(parser.parse_args()))
