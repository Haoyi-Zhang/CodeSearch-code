"""Independent stateful checker for continuation-query evidence.

This module imports no producer, service, coordinator, or ranking-index code.
It validates source-receipt bindings under the stated crash-only trust model,
recomputes every event-driven token transition, and independently merges rows.
A separate retained-history replay checks whether trusted sources told the truth.
"""
from __future__ import annotations
from copy import deepcopy
from .checker import Checker, InvalidCertificate, need


def _key(row: dict) -> tuple[int, str]:
    return (-row['score'], row['id'])


def _min_bound(left, right):
    values = [tuple(x) for x in (left, right) if x is not None]
    return list(min(values)) if values else None


class ContinuationChecker:
    def __init__(self, payloads: dict):
        self.rows = Checker(payloads)
        self.known_bodies = set(payloads)
        self.tokens: dict[str, dict[int, dict]] = {}
        self.events: list[dict[int, dict]] = [dict(), dict(), dict()]
        self.frontiers = [0, 0, 0]
        self.epoch = 0

    def reset_epoch(self, epoch: int, base: list[int]) -> None:
        need(type(epoch) is int and epoch >= self.epoch, 'checker epoch reset')
        need(len(base) == 3 and all(type(x) is int and x >= 0 for x in base), 'checker base')
        self.tokens.clear()
        self.events = [dict(), dict(), dict()]
        self.frontiers = list(base)
        self.epoch = epoch

    def reset_tokens(self, epoch: int) -> None:
        self.reset_epoch(epoch, self.frontiers)

    def compact_events(self, floors: list[int]) -> None:
        """Bound state after every installed token has consumed a prefix."""
        need(len(floors) == 3, 'checker compaction vector')
        staged = [dict(shard_events) for shard_events in self.events]
        for shard, floor in enumerate(floors):
            need(type(floor) is int and 0 <= floor <= self.frontiers[shard],
                 'checker compaction floor')
            for seq in [seq for seq in staged[shard] if seq <= floor]:
                del staged[shard][seq]
        self.events = staged

    def accept_events(self, entry: dict, owners: list[list[int]], expected_epoch: int) -> int:
        need(expected_epoch == self.epoch, 'event current epoch')
        req, rep = entry['request'], entry['reply']
        need(req.get('op') == 'events' and rep.get('kind') == 'events', 'event receipt kind')
        shard = rep['shard']
        need(shard in range(3) and entry['node'] in owners[shard], 'event owner')
        need(req.get('shard') == shard and req.get('epoch') == expected_epoch, 'event request binding')
        need(rep['epoch'] == expected_epoch and rep['lo'] == req.get('lo')
             and rep['hi'] == req.get('hi'), 'event reply binding')
        need(type(rep['lo']) is int and type(rep['hi']) is int
             and 0 <= rep['lo'] <= rep['hi'], 'event progress')
        need(rep['lo'] == self.frontiers[shard], 'event feed gap')
        events = rep['events']
        need([e.get('seq') for e in events] == list(range(rep['lo'] + 1, rep['hi'] + 1)),
             'event sequence')
        staged = dict(self.events[shard])
        for event in events:
            need(event.get('shard') == shard, 'event shard')
            changes = event.get('changes')
            need(isinstance(changes, list) and len(changes) <= 16, 'event batch')
            need(all(isinstance(pair, list) and len(pair) == 2 and isinstance(pair[0], str)
                     for pair in changes), 'event change schema')
            ids = [pair[0] for pair in changes]
            need(len(ids) == len(set(ids)), 'duplicate changed identifier')
            need(all(body is None or body in self.known_bodies for _, body in changes), 'event body')
            staged[event['seq']] = deepcopy(event)
        # Commit only after all events in the receipt have passed validation.
        self.events[shard] = staged
        self.frontiers[shard] = rep['hi']
        return len(events)

    def _check_prefix(self, entry: dict, query: dict, owners: list[list[int]],
                      epoch: int, capacity: int) -> dict:
        need(epoch == self.epoch, 'prefix current epoch')
        need(type(capacity) is int and query['k'] <= capacity <= 64, 'prefix capacity')
        req, rep = entry['request'], entry['reply']
        shard = rep.get('shard')
        need(req.get('op') == 'prefix' and rep.get('kind') == 'prefix', 'prefix receipt kind')
        need(shard in range(3) and entry['node'] in owners[shard], 'prefix owner')
        need(req.get('shard') == shard and req.get('epoch') == epoch
             and req.get('query') == query and req.get('length') == capacity, 'prefix request binding')
        need(rep.get('epoch') == epoch and rep.get('plan') == query['plan'], 'prefix reply binding')
        need(type(rep.get('at')) is int and 0 <= rep['at'] <= self.frontiers[shard], 'prefix cut')
        rows = rep.get('rows')
        need(isinstance(rows, list) and len(rows) <= capacity and rows == self.rows.ordered(rows),
             'prefix order')
        need(len({row.get('id') for row in rows}) == len(rows), 'prefix duplicate')
        for row in rows:
            need(set(row) == {'id', 'body', 'score'}, 'prefix row schema')
            need(type(row['score']) is int and row['score'] > 0
                 and row['score'] == self.rows.value(row['body'], query['plan']), 'prefix score')
        tail = rep.get('boundary')
        if tail is not None:
            need(isinstance(tail, list) and len(tail) == 2 and type(tail[0]) is int
                 and tail[0] < 0 and isinstance(tail[1], str), 'prefix tail')
            need(len(rows) == capacity, 'prefix unexhausted')
            need(not rows or _key(rows[-1]) < tuple(tail), 'prefix boundary order')
        return {
            'query': deepcopy(query), 'shard': shard, 'epoch': epoch, 'at': rep['at'],
            'capacity': capacity, 'rows': deepcopy(rows), 'tail': deepcopy(tail),
            'generation': 0,
        }

    def install_prefix(self, query: dict, entry: dict, owners: list[list[int]],
                       epoch: int, capacity: int) -> None:
        self._check_query(query)
        for old in self.tokens.get(query['id'], {}).values():
            need(old['query'] == query and old['epoch'] == epoch, 'token query/plan/k/epoch binding')
        token = self._check_prefix(entry, query, owners, epoch, capacity)
        self.tokens.setdefault(query['id'], {})[token['shard']] = token

    @staticmethod
    def _check_query(query: dict) -> None:
        need(isinstance(query, dict) and isinstance(query.get('id'), str), 'query id')
        need(type(query.get('k')) is int and 1 <= query['k'] <= 20, 'query k')
        plan = query.get('plan')
        need(isinstance(plan, list) and 1 <= len(plan) <= 2, 'query plan')
        for alternate in plan:
            need(isinstance(alternate, list) and 1 <= len(alternate) <= 4
                 and all(isinstance(label, str) for label in alternate)
                 and len(set(alternate)) == len(alternate), 'query alternate')

    def _events_between(self, shard: int, lo: int, hi: int) -> list[dict]:
        need(0 <= lo <= hi <= self.frontiers[shard], 'checker feed coverage')
        answer = []
        for seq in range(lo + 1, hi + 1):
            need(seq in self.events[shard], 'checker missing event')
            answer.append(self.events[shard][seq])
        return answer

    def _advance(self, token: dict, hi: int) -> dict:
        events = self._events_between(token['shard'], token['at'], hi)
        final = {}
        for event in events:
            for ident, body in event['changes']:
                final[ident] = body
        pool = {row['id']: deepcopy(row) for row in token['rows'] if row['id'] not in final}
        plan = token['query']['plan']
        for ident, body in final.items():
            if body is None:
                continue
            value = self.rows.value(body, plan)
            if value:
                pool[ident] = {'id': ident, 'body': body, 'score': value}
        ordered = self.rows.ordered(list(pool.values()))
        capacity = token['capacity']
        dropped = _key(ordered[capacity]) if len(ordered) > capacity else None
        return {
            **{name: deepcopy(token[name]) for name in ('query','shard','epoch','capacity')},
            'at': hi, 'rows': ordered[:capacity],
            'tail': _min_bound(token['tail'], dropped),
            'generation': token['generation'] + 1,
        }

    def check_result(self, result: dict, query: dict, cut: list[int], epoch: int,
                     owners: list[list[int]], trusted: dict[int, dict],
                     repairs: dict[str, dict]) -> bool:
        need(epoch == self.epoch and result.get('kind') == 'continuation-result', 'result epoch/kind')
        need(result.get('query') == query and result.get('cut') == cut
             and result.get('epoch') == epoch, 'result binding')
        self._check_query(query)
        need(len(cut) == 3 and all(type(x) is int and x >= 0 for x in cut), 'result cut')
        need(set(repairs).issubset({'0','1','2'}), 'repair labels')
        existing = self.tokens.get(query['id'], {})
        for token in existing.values():
            need(token['query'] == query and token['epoch'] == epoch, 'token query/plan/k/epoch binding')
        current = deepcopy(existing)
        missing = set()
        for shard in range(3):
            label = str(shard)
            if label in repairs:
                spec = repairs[label]
                ident = spec['receipt']
                need(type(ident) is int and ident in trusted, 'repair receipt')
                token = self._check_prefix(trusted[ident], query, owners, epoch, spec['capacity'])
                need(token['shard'] == shard, 'repair shard')
                current[shard] = token
            token = current.get(shard)
            if token is None or token['epoch'] != epoch or cut[shard] > self.frontiers[shard]:
                missing.add(shard)
                continue
            if token['at'] > cut[shard]:
                raise InvalidCertificate('token beyond cut')
            if token['at'] < cut[shard]:
                current[shard] = self._advance(token, cut[shard])
        pool = {}
        for shard, token in sorted(current.items()):
            if shard in missing:
                continue
            need(token['at'] == cut[shard], 'advanced token cut')
            for row in token['rows']:
                need(row['id'] not in pool, 'continuation cross-shard duplicate')
                pool[row['id']] = row
        rows = self.rows.ordered(list(pool.values()))[:query['k']]
        blockers = []
        for shard, token in sorted(current.items()):
            if shard in missing:
                continue
            tail = token['tail']
            if tail is not None and (len(rows) < query['k'] or _key(rows[-1]) >= tuple(tail)):
                blockers.append(shard)
        missing_list = sorted(missing | (set(range(3)) - set(current)))
        status = 'partial' if missing_list else 'rank-underdetermined' if blockers else 'complete'
        expected_summary = {
            str(shard): {'at': token['at'], 'capacity': token['capacity'],
                         'tail': deepcopy(token['tail']), 'generation': token['generation']}
            for shard, token in sorted(current.items()) if shard not in missing
        }
        need(result.get('rows') == rows, 'continuation merge')
        need(result.get('missing_shards') == missing_list, 'continuation missing')
        need(result.get('rank_blockers') == blockers, 'continuation blockers')
        need(result.get('status') == status, 'continuation status')
        need(result.get('token_summary') == expected_summary, 'continuation token summary')
        # Publish the whole query transition only after the response is verified.
        self.tokens[query['id']] = current
        return True
