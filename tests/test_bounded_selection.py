"""Portable pure-computation checks; no sockets, campaigns, or private paths."""
import asyncio
from copy import deepcopy
from itertools import product
import unittest

from src import model, service, coordinator, continuation_coordinator
from src import checker, continuation_checker


LABELS = {
    'b0': ['name:a'],
    'b1': ['name:x'],
    'b2': ['name:x', 'name:y'],
    'b3': ['name:x', 'name:y', 'name:z'],
    'b4': ['name:w', 'name:x', 'name:y', 'name:z'],
}
PLANS = [
    [['name:x']],
    [['name:x', 'name:y', 'name:z', 'name:w']],
    [['name:a'], ['name:x', 'name:y']],
    [['name:absent']],
]
LIMITS = (0, 1, 5, 20, 64, 65, 1024)
CURRENT = dict(model=model, service=service, coordinator=coordinator,
               continuation_coordinator=continuation_coordinator,
               checker=checker, continuation_checker=continuation_checker)


def payloads():
    # Authored literal labels, not extracted by the producer or its index.
    args = {'b0': 'a', 'b1': 'x', 'b2': 'x,y', 'b3': 'x,y,z', 'b4': 'w,x,y,z'}
    return {body: {'source': 'def f(' + names + '):\n    return ' +
                  '+'.join(names.split(',')) + '\n',
                  'features': list(LABELS[body])}
            for body, names in args.items()}


def scan_reference(state, plan, exclude=()):
    """Independent per-document literal score and full-sort reference."""
    rows = []
    for ident, body in state.items():
        if ident in exclude:
            continue
        value = max((sum(term in LABELS[body] for term in alternate)
                     for alternate in plan), default=0)
        if value:
            rows.append({'id': ident, 'body': body, 'score': value})
    return sorted(rows, key=lambda row: (-row['score'], row['id']))


def fixtures():
    return [
        ('empty', {}),
        ('no-positive', {'a': 'b0', 'z': 'b0'}),
        ('ties', {f'd{i:04d}': 'b2' for i in reversed(range(72))}),
        ('mixed-wide', {f'd{i:04d}': f'b{i % 5}' for i in reversed(range(1024))}),
        ('unicode-order', {ident: f'b{i % 5}' for i, ident in
                           enumerate(('z', 'A', 'a', 'a:1', 'ä', '中', 'Ω'))}),
    ]


def query(k=5, plan=None):
    return {'id': 'portable-q', 'plan': deepcopy(plan or PLANS[1]), 'k': k, 'choice': 0}


def replay(state, events):
    state = dict(state)
    for event in events:
        for ident, body in event['changes']:
            if body is None:
                state.pop(ident, None)
            else:
                state[ident] = body
    return state


def replica_state(replica):
    """All mutable local state and explicit admission counters, no timings."""
    return deepcopy({
        'node': replica.node, 'shard': replica.shard, 'epoch': replica.epoch,
        'checkpoint': replica.checkpoint, 'index': replica.index,
        'postings': {term: sorted(ids) for term, ids in replica.search.postings.items()},
        'floor': replica.floor, 'frontier': replica.frontier, 'index_at': replica.index_at,
        'journal': replica.journal, 'pending': replica.pending, 'crashed': replica.crashed,
        'peak_journal': replica.peak_journal, 'peak_pending': replica.peak_pending,
    })


class LocalReceipts:
    """Owned in-memory dispatch with canonical serialization, not a transport."""
    def __init__(self, api, initial):
        self.api = api
        self.replicas = [api['service'].Replica(s, s, state, payloads())
                         for s, state in enumerate(initial)]
        self.evidence = {}
        self.messages = self.bytes = 0

    async def rpc(self, node, request):
        wire = self.api['service'].wire
        import json
        raw = wire(request)
        sent = json.loads(raw)
        reply_raw = wire(self.replicas[node].handle(sent))
        reply = json.loads(reply_raw)
        self.messages += 2
        self.bytes += len(raw) + len(reply_raw)
        ident = len(self.evidence) + 1
        self.evidence[ident] = {'node': node, 'request': sent, 'reply': reply}
        return ident, reply


async def session_packet(api, size=72, steps=4, capacity=5, k=5):
    """Drive actual public modules independently; harness holds no ranking code."""
    initial = [{f's{s}-d{i:04d}': f'b{i % 5}' for i in range(size)}
               for s in range(3)]
    network = LocalReceipts(api, initial)
    owners = [[0], [1], [2]]
    q = query(k)
    feed = api['continuation_coordinator'].FeedStore()
    session = api['continuation_coordinator'].ContinuationSession(payloads(), feed, capacity)
    cc = api['continuation_checker'].ContinuationChecker(payloads())
    row_checker = api['checker'].Checker(payloads())
    specs = await session.acquire(network, q, [0, 0, 0], 0, owners)
    for spec in specs.values():
        cc.install_prefix(q, network.evidence[spec['receipt']], owners, 0, spec['capacity'])
    history = [[], [], []]
    observations = []

    async def record(cut):
        result, repairs = await session.query(network, q, cut, 0, owners)
        saved = deepcopy((cc.tokens, cc.events, cc.frontiers, cc.epoch))
        bad = deepcopy(result)
        bad['status'] = 'partial' if result['status'] != 'partial' else 'complete'
        try:
            cc.check_result(bad, q, cut, 0, owners, network.evidence, repairs)
        except api['checker'].InvalidCertificate as exc:
            rejection = str(exc)
        else:
            raise AssertionError('altered continuation status accepted')
        assert saved == (cc.tokens, cc.events, cc.frontiers, cc.epoch)
        cc.check_result(result, q, cut, 0, owners, network.evidence, repairs)
        cert = await api['coordinator'].certified(network, q, cut, 0, owners, capacity)
        row_checker.check(cert, network.evidence, q, cut, 0, owners)
        overlay = await api['coordinator'].baseline(network, q, cut, 0, owners, 'overlay')
        row_checker.check_overlay(overlay, network.evidence, q, cut, 0, owners)
        universe = {}
        for state, events in zip(initial, history):
            universe.update(replay(state, events))
        exact = scan_reference(universe, q['plan'])[:k]
        assert overlay['rows'] == exact
        assert result['status'] != 'complete' or result['rows'] == exact
        assert cert['status'] != 'complete' or cert['rows'] == exact
        assert all(row in scan_reference(universe, q['plan']) for row in result['rows'])
        observations.append(deepcopy({
            'result': result, 'certificate': cert, 'overlay': overlay, 'exact': exact,
            'session': session.tokens, 'checker': cc.tokens,
            'checker_events': cc.events, 'checker_frontiers': cc.frontiers,
            'feed': feed.logical_state(), 'replicas': [replica_state(r) for r in network.replicas],
            'messages': network.messages, 'bytes': network.bytes,
            'rejected_status': rejection,
        }))

    await record([0, 0, 0])
    for step in range(1, steps + 1):
        for shard in range(3):
            event = {'seq': step, 'shard': shard, 'changes': [
                [f's{shard}-d{step % max(size, 1):04d}', None if step % 2 else 'b4'],
                [f's{shard}-new', 'b4' if step % 2 else 'b1'],
            ]}
            history[shard].append(event)
            network.replicas[shard].handle({'op': 'receive', 'event': deepcopy(event)})
            network.replicas[shard].handle({'op': 'advance', 'to': max(0, step - shard)})
            ident, _ = await api['continuation_coordinator'].fetch_events(
                network, feed, shard, step, 0, owners)
            cc.accept_events(network.evidence[ident], owners, 0)
        await record([step] * 3)
    # Check each source receipt against literal replay, not the producer's loop.
    for entry in network.evidence.values():
        reply = entry['reply']
        shard = reply['shard']
        if reply['kind'] == 'prefix':
            ranked = scan_reference(replay(initial[shard], history[shard][:reply['at']]), q['plan'])
            length = entry['request']['length']
            assert reply['rows'] == ranked[:length]
            assert reply['boundary'] == ([-ranked[length]['score'], ranked[length]['id']]
                                         if len(ranked) > length else None)
        elif reply['kind'] == 'overlay':
            assert reply['rows'] == scan_reference(
                replay(initial[shard], history[shard][:reply['at']]), q['plan'])[:k]
        elif reply['kind'] == 'events':
            assert reply['events'] == history[shard][reply['lo']:reply['hi']]
        elif reply['kind'] == 'delta':
            final = {}
            for event in history[shard][reply['lo']:reply['hi']]:
                for ident, body in event['changes']:
                    final[ident] = body
            assert reply['changed'] == sorted(final)
            assert reply['rows'] == scan_reference(
                {ident: body for ident, body in final.items() if body is not None}, q['plan'])[:k]
        else:
            raise AssertionError('unexpected pure receipt')
    return {'observations': observations, 'receipts': network.evidence,
            'messages': network.messages, 'bytes': network.bytes, 'history': history}


class BoundedSelectionTests(unittest.TestCase):
    def test_literal_cartesian_rows(self):
        for values in product((None, 'b0', 'b1', 'b2'), repeat=3):
            state = {ident: body for ident, body in zip(('a', 'b', 'z'), values) if body}
            index = model.PostingIndex(state, payloads())
            for plan in PLANS + [[], [['name:x', 'name:x']]]:
                for excluded in (set(), {'a'}, set(state), {'absent'}):
                    exact = scan_reference(state, plan, excluded)
                    self.assertEqual(index.rows(plan, excluded), exact)
                    for limit in (0, 1, 2, 4):
                        self.assertEqual(index.rows(plan, excluded, limit=limit), exact[:limit])

    def test_wide_limits_alternatives_and_exclusions(self):
        for name, state in fixtures():
            index = model.PostingIndex(state, payloads())
            for plan in PLANS:
                for excluded in (set(), set(list(state)[::3]), set(state), {'absent'}):
                    exact = scan_reference(state, plan, excluded)
                    for limit in LIMITS:
                        with self.subTest(name=name, plan=plan, limit=limit):
                            self.assertEqual(index.rows(plan, excluded, limit=limit), exact[:limit])
                    self.assertEqual(index.rows(plan, excluded), exact)

    def test_updates_and_return_isolation(self):
        state = {'a': 'b4', 'b': 'b2', 'z': 'b1'}
        index = model.PostingIndex(state, payloads())
        events = [
            {'changes': [['a', None], ['new', 'b4']]},
            {'changes': [['b', 'b0'], ['a', 'b2']]},
            {'changes': [['new', 'b1'], ['a', None]]},
        ]
        for event in events:
            index.apply(event)
            state = replay(state, [event])
            for plan in PLANS:
                self.assertEqual(index.rows(plan), scan_reference(state, plan))
                for limit in LIMITS:
                    got = index.rows(plan, limit=limit)
                    self.assertEqual(got, scan_reference(state, plan)[:limit])
                    if got:
                        got[0]['body'] = 'not-a-body'
                        self.assertEqual(index.rows(plan, limit=limit),
                                         scan_reference(state, plan)[:limit])

    def test_prefix_boundaries_and_overlay_replay(self):
        for name, state in fixtures():
            for plan in PLANS:
                for k in (1, 5, 20):
                    q = query(k, plan)
                    replica = service.Replica(0, 0, state, payloads())
                    base = scan_reference(state, plan)
                    for length in (0, 1, 5, 20, 64):
                        request = dict(op='prefix', shard=0, epoch=0, query=q, length=length)
                        expected = dict(kind='prefix', shard=0, epoch=0, plan=plan, at=0,
                                        rows=base[:length], boundary=
                                        [-base[length]['score'], base[length]['id']]
                                        if len(base) > length else None)
                        self.assertEqual(replica.handle(request), expected, name)
                    ids = list(state)[:3]
                    events = [
                        {'seq': 1, 'shard': 0, 'changes':
                         [[ident, None] for ident in ids] + [['new', 'b4']]},
                        {'seq': 2, 'shard': 0, 'changes':
                         [[ident, 'b1'] for ident in ids] + [['new', 'b2']]},
                        {'seq': 3, 'shard': 0, 'changes': [['new', None], ['added', 'b3']]},
                    ]
                    for event in events:
                        self.assertTrue(replica.handle(dict(op='receive', event=event))['ok'])
                    for at in range(4):
                        replica.handle(dict(op='advance', to=at))
                        for target in range(at, 4):
                            got = replica.handle(dict(op='overlay', shard=0, epoch=0,
                                                      query=q, to=target))
                            self.assertEqual(got['rows'],
                                             scan_reference(replay(state, events[:target]), plan)[:k])

    def test_session_receipts_and_independent_checkers(self):
        for size, steps, capacity, k in ((0, 2, 5, 1), (8, 4, 5, 5),
                                        (72, 4, 5, 5), (72, 3, 64, 20)):
            packet = asyncio.run(session_packet(CURRENT, size, steps, capacity, k))
            self.assertEqual(len(packet['observations']), steps + 1)
            self.assertEqual(packet['messages'], 2 * len(packet['receipts']))

    def test_bounds_errors_and_complete_enumeration(self):
        index = model.PostingIndex({'a': 'b1'}, payloads())
        for limit in (-1, 1.5, True, '2'):
            with self.assertRaisesRegex(ValueError, 'row limit must be a nonnegative integer'):
                index.rows(PLANS[0], limit=limit)
        # A zero limit still visits every posting and performs state lookups.
        del index.state['a']
        with self.assertRaises(KeyError):
            index.rows(PLANS[0], limit=0)
        with self.assertRaises(KeyError):
            index.rows(PLANS[0])
        replica = service.Replica(0, 0, {}, payloads())
        for length in (-1, 65):
            self.assertEqual(replica.handle(dict(op='prefix', shard=0, epoch=0,
                             query=query(), length=length)), {'error': 'prefix-bound'})
        for k in (0, 21):
            self.assertEqual(replica.handle(dict(op='overlay', shard=0, epoch=0,
                             query=query(k), to=0)), {'error': 'query-bound'})


if __name__ == '__main__':
    unittest.main()
