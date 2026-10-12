"""Small owned-data tests for fair capacities, counters and exercised refill."""
import unittest
from compare_incremental import trial, independent_queries, POLICIES
from src.model import features, PostingIndex


class IncrementalComparisonTests(unittest.IsolatedAsyncioTestCase):
    async def test_refill_and_global_stopping(self):
        payloads = {}
        for i, names in enumerate(('x', 'x,y', 'x,y,z,w'), 1):
            source = 'def f('+names+'):\n    return '+names.replace(',', '+')+'\n'
            payloads[f'b{i}'] = {'source': source, 'features': list(features(source))}
        fixture = {'payloads': payloads, 'initial': [{'global': 'b3'}, {'a': 'b2', 'b': 'b1'}, {}],
                   'history': [[], [{'shard': 1, 'seq': 1, 'changes': [['a', None]]}], []]}
        query = {'id': 'q', 'plan': [['name:x', 'name:y', 'name:z', 'name:w']], 'k': 1}
        for lag in (0, 1):
            results = {p: await trial(fixture, [query], p, 1, lag) for p in POLICIES}
            self.assertEqual(0, results['continuation']['summary']['refill_shards'])
            self.assertEqual(1, results['extra-local']['summary']['refill_shards'])
            self.assertEqual(0, results['extra-global']['summary']['refill_shards'])
            self.assertEqual(results['continuation']['summary']['total_source_bytes'],
                             results['extra-global']['summary']['total_source_bytes'])
            self.assertEqual(results['continuation']['summary']['coordinator_work'],
                             results['extra-global']['summary']['coordinator_work'])
            self.assertEqual(results['continuation']['summary']['source_work'],
                             results['extra-global']['summary']['source_work'])
            self.assertEqual(results['continuation']['records'][0]['oracle'],
                             results['extra-local']['records'][0]['result']['rows'])

    async def test_posting_census_does_not_change_rows(self):
        payloads = {'b': {'features': ['x', 'y']}}
        index = PostingIndex({'a': 'b', 'z': 'b'}, payloads)
        work = {}
        self.assertEqual(index.rows([['x', 'y']], limit=1),
                         index.rows([['x', 'y']], limit=1, work=work))
        self.assertEqual({'posting_visits': 4, 'positive_rows_built': 2}, work)

    async def test_query_choice_uses_only_base(self):
        base = {'initial': [{f'repo/mod.py:f{i}': f'b{i}' for i in range(16)}, {}, {}],
                'payloads': {f'b{i}': {'features': ['name:x', f'name:n{i}']} for i in range(16)}}
        first = independent_queries(base)
        base['history'] = [[{'unread': 'history'}], [], []]
        self.assertEqual(first, independent_queries(base))
        self.assertEqual(8, len(first))
        self.assertEqual(8, len({q['seed_id'] for q in first}))


if __name__ == '__main__':
    unittest.main()
