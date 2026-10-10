"""In-memory owner replies and toy rows; no sockets, processes or histories."""
from copy import deepcopy
import unittest

from src.continuation import advance_token, token_from_prefix
from src.continuation_coordinator import ContinuationSession, FeedStore

QUERY = {'id': 'owned', 'k': 1, 'plan': [['name:x', 'name:y']]}
OWNERS = [[0, 1], [2, 3], [4, 5]]
PAYLOADS = {'high': {'features': ['name:x', 'name:y']},
            'held': {'features': ['name:x']}}


def prefix(shard, at, rows=(), boundary=None):
    return {'kind': 'prefix', 'shard': shard, 'epoch': 0, 'at': at,
            'plan': QUERY['plan'], 'rows': list(rows), 'boundary': boundary}


class ReplyOwners:
    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    async def rpc(self, node, request):
        self.calls.append((node, deepcopy(request)))
        return f'owned-receipt-{node}', deepcopy(self.replies.get(node))


def blocked_session():
    feed = FeedStore(base=[1, 0, 0])
    session = ContinuationSession(PAYLOADS, feed, base_capacity=1)
    initial = prefix(0, 0, [{'id': 'high', 'body': 'high', 'score': 2}], [-1, 'held'])
    token = advance_token(token_from_prefix(QUERY, initial, 1),
                          {'kind': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1,
                           'events': [{'seq': 1, 'shard': 0, 'changes': [['high', None]]}]},
                          PAYLOADS)
    session.tokens[QUERY['id']] = {0: token,
        1: token_from_prefix(QUERY, prefix(1, 0), 1),
        2: token_from_prefix(QUERY, prefix(2, 0), 1)}
    return session


class OwnerSelection(unittest.IsolatedAsyncioTestCase):
    async def test_acquisition_skips_wrong_cut_before_later_owner(self):
        session = ContinuationSession(PAYLOADS, FeedStore(), base_capacity=1)
        network = ReplyOwners({0: prefix(0, 0), 1: prefix(0, 1),
                               2: prefix(1, 0), 4: prefix(2, 0)})
        installed = await session.acquire(network, QUERY, [1, 0, 0], 0, OWNERS)
        self.assertEqual(installed['0']['receipt'], 'owned-receipt-1')
        self.assertEqual(session.tokens[QUERY['id']][0]['at'], 1)
        self.assertEqual([node for node, _ in network.calls], [0, 1, 2, 4])

    async def _repair_after_unusable_owner(self, first_cut):
        session = blocked_session()
        usable = prefix(0, 1, [{'id': 'held', 'body': 'held', 'score': 1}])
        network = ReplyOwners({0: prefix(0, first_cut), 1: usable})
        result, repairs = await session.query(network, QUERY, [1, 0, 0], 0, OWNERS)
        self.assertEqual([node for node, _ in network.calls], [0, 1])
        self.assertEqual(repairs['0']['receipt'], 'owned-receipt-1')
        self.assertEqual(result['status'], 'complete')
        self.assertEqual([row['id'] for row in result['rows']], ['held'])

    async def test_repair_skips_future_cut(self):
        await self._repair_after_unusable_owner(2)

    async def test_repair_skips_cut_below_retained_feed_floor(self):
        await self._repair_after_unusable_owner(0)

    async def test_no_eligible_owner_remains_underdetermined(self):
        session = blocked_session()
        network = ReplyOwners({0: prefix(0, 2), 1: prefix(0, 0)})
        result, repairs = await session.query(network, QUERY, [1, 0, 0], 0, OWNERS)
        self.assertEqual([node for node, _ in network.calls], [0, 1])
        self.assertEqual(result['status'], 'rank-underdetermined')
        self.assertEqual(result['rank_blockers'], [0])
        self.assertEqual(repairs, {})


if __name__ == '__main__':
    unittest.main()
