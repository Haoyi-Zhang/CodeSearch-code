"""Count actual construction once and independently recount real assignments."""
from copy import deepcopy
import unittest
from unittest.mock import patch
from compare_incremental import OfflineWire, trial, POLICIES
from recount_incremental import check_source_work
from src.checker import InvalidCertificate
from src.model import PostingIndex, features


class IncrementalCensusTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        sources = {'x': 'def f(x):\n    return x\n',
                   'xy': 'def f(x,y):\n    return x+y\n'}
        payloads = {b: {'source': text, 'features': list(features(text))} for b, text in sources.items()}
        return {'payloads': payloads, 'initial': [{'a': 'xy', 'b': 'x', 'c': 'x'}, {}, {}],
                'history': [[{'shard': 0, 'seq': 1, 'changes': [['a', None]]},
                             {'shard': 0, 'seq': 2, 'changes': [['b', 'xy'], ['c', None]]},
                             {'shard': 0, 'seq': 3, 'changes': [['b', 'x'], ['d', 'xy']]}], [], []],
                'schedule': [[1, 0, 0], [2, 0, 0], [3, 0, 0]]}

    async def test_exactly_one_real_posting_construction(self):
        fixture = self.fixture()
        calls = []
        original = PostingIndex.put
        def observed(index, ident, body):
            calls.append((ident, body))
            return original(index, ident, body)
        with patch.object(PostingIndex, 'put', observed):
            network = OfflineWire(fixture)
        self.assertEqual(3, len(calls))
        self.assertEqual(3, network.counts['identifier_assignments'])
        self.assertEqual(3, network.work_by_phase['initialization']['identifier_assignments'])

    async def test_source_census_all_phases_lag_and_repeated_writes(self):
        q = {'id': 'owned', 'plan': [['name:x', 'name:y']], 'k': 1}
        for lag in (0, 1, 2):
            matched = []
            for policy in POLICIES:
                result = await trial(self.fixture(), [q], policy, 1, lag)
                phases = check_source_work(result, self.fixture()['payloads'])
                self.assertEqual(3, phases['initialization']['identifier_assignments'])
                self.assertEqual((5, 3, 1)[lag], phases['maintenance']['identifier_assignments'])
                self.assertGreater(phases['acquisition']['posting_visits'], 0)
                self.assertGreater(phases['refill']['posting_visits'], 0)
                matched.append(result)
            for field in ('source_work', 'source_work_by_phase', 'coordinator_work',
                          'source_costs', 'max_logical_state_bytes'):
                left, right = deepcopy(matched[0]['summary'][field]), deepcopy(matched[2]['summary'][field])
                if field == 'source_costs':
                    for costs in (left, right):
                        for phase in costs.values():
                            phase.pop('source_cpu_ns', None)
                self.assertEqual(left, right)

    async def test_independent_census_rejects_counter_tampering(self):
        q = {'id': 'owned', 'plan': [['name:x', 'name:y']], 'k': 1}
        result = await trial(self.fixture(), [q], 'extra-global', 1, 1)
        for phase in ('initialization', 'maintenance', 'acquisition', 'refill'):
            bad = deepcopy(result)
            counts = bad['summary']['source_work_by_phase'][phase]
            name = next(k for k, v in counts.items() if v > 0)
            counts[name] += 1
            with self.assertRaises(InvalidCertificate):
                check_source_work(bad, self.fixture()['payloads'])
        bad = deepcopy(result)
        bad['summary']['source_work']['identifier_assignments'] = True
        with self.assertRaises(InvalidCertificate):
            check_source_work(bad, self.fixture()['payloads'])


if __name__ == '__main__':
    unittest.main()
