"""Owned JSON metadata regressions; type errors must be failure-atomic."""
from copy import deepcopy
import unittest
import test_score_admission
from src.checker import InvalidCertificate, same_json
from src.continuation import advance_token, merge_tokens


class MetadataBindingTests(unittest.TestCase):
    def setUp(self):
        self.f = test_score_admission.ScoreAdmissionTests()
        self.f.setUp()

    def test_result_binding_rejects_equal_boolean_and_real_values(self):
        f = self.f
        good = merge_tokens(f.q, f.cut, 0, f.tokens)
        for path, values in [(('cut', 0), (False, 0.0)),
                             (('epoch',), (False, 0.0)),
                             (('query', 'k'), (True, 1.0))]:
            for value in values:
                with self.subTest(path=path, value=value):
                    bad = deepcopy(good)
                    target = bad
                    for component in path[:-1]:
                        target = target[component]
                    target[path[-1]] = value
                    before = deepcopy(f.cc.tokens)
                    with self.assertRaises(InvalidCertificate):
                        f.cc.check_result(bad, f.q, f.cut, 0, f.owners, f.evidence, {})
                    self.assertEqual(before, f.cc.tokens)
        self.assertTrue(f.cc.check_result(good, f.q, f.cut, 0, f.owners, f.evidence, {}))

    def test_alias_rejection_does_not_commit_real_advancement(self):
        f = self.f
        event = {'node': 0, 'request': {'op': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1},
                 'reply': {'kind': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1,
                           'events': [{'seq': 1, 'shard': 0, 'changes': [['a', None]]}]}}
        f.cc.accept_events(event, f.owners, 0)
        tokens = deepcopy(f.tokens)
        tokens[0] = advance_token(tokens[0], event['reply'], f.payloads)
        good = merge_tokens(f.q, [1, 0, 0], 0, tokens)
        for value in (True, 1.0):
            bad = deepcopy(good)
            bad['cut'][0] = value
            before = deepcopy(f.cc.tokens)
            with self.assertRaises(InvalidCertificate):
                f.cc.check_result(bad, f.q, [1, 0, 0], 0, f.owners, f.evidence, {})
            self.assertEqual(before, f.cc.tokens)
        self.assertTrue(f.cc.check_result(good, f.q, [1, 0, 0], 0, f.owners, f.evidence, {}))
        self.assertEqual(1, f.cc.tokens[f.q['id']][0]['at'])
        self.assertEqual([], f.cc.tokens[f.q['id']][0]['rows'])

    def test_source_prefix_binding_is_typed(self):
        f = self.f
        for where, field, values in [('request', 'length', (True, 1.0)),
                                     ('request', 'shard', (False, 0.0)),
                                     ('reply', 'shard', (False, 0.0)),
                                     ('request', 'epoch', (False, 0.0)),
                                     ('reply', 'epoch', (False, 0.0))]:
            for value in values:
                bad = deepcopy(f.evidence[0])
                bad[where][field] = value
                before = deepcopy(f.cc.tokens)
                with self.assertRaises(InvalidCertificate):
                    f.cc.install_prefix(f.q, bad, f.owners, 0, 1)
                self.assertEqual(before, f.cc.tokens)

    def test_source_event_sequence_and_binding_are_typed(self):
        f = self.f
        good = {'node': 0, 'request': {'op': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1},
                'reply': {'kind': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1,
                          'events': [{'seq': 1, 'shard': 0, 'changes': [['a', None]]}]}}
        cases = [(('node',), False), (('node',), 0.0),
                 (('request', 'shard'), False), (('reply', 'shard'), 0.0),
                 (('request', 'hi'), True), (('request', 'lo'), 0.0),
                 (('reply', 'epoch'), False), (('reply', 'events', 0, 'seq'), True),
                 (('reply', 'events', 0, 'seq'), 1.0),
                 (('reply', 'events', 0, 'shard'), False)]
        for path, value in cases:
            bad = deepcopy(good)
            target = bad
            for component in path[:-1]:
                target = target[component]
            target[path[-1]] = value
            before = deepcopy((f.cc.events, f.cc.frontiers))
            with self.assertRaises(InvalidCertificate):
                f.cc.accept_events(bad, f.owners, 0)
            self.assertEqual(before, (f.cc.events, f.cc.frontiers))
        self.assertEqual(1, f.cc.accept_events(good, f.owners, 0))

    def test_malformed_event_body_is_typed_and_retry_is_atomic(self):
        f = self.f
        good = {'node': 0, 'request': {'op': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1},
                'reply': {'kind': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 1,
                          'events': [{'seq': 1, 'shard': 0,
                                      'changes': [['a', next(iter(f.payloads))]]}]}}
        for malformed in ([], {}, 0, False, 0.0):
            with self.subTest(malformed=malformed):
                bad = deepcopy(good)
                bad['reply']['events'][0]['changes'][0][1] = malformed
                original = deepcopy(bad)
                before = deepcopy((f.cc.events, f.cc.frontiers, f.cc.tokens))
                with self.assertRaisesRegex(InvalidCertificate, 'event body'):
                    f.cc.accept_events(bad, f.owners, 0)
                self.assertEqual(original, bad)
                self.assertEqual(before, (f.cc.events, f.cc.frontiers, f.cc.tokens))
        self.assertEqual(1, f.cc.accept_events(good, f.owners, 0))
        self.assertEqual(1, f.cc.frontiers[0])
        self.assertEqual(good['reply']['events'][0], f.cc.events[0][1])

    def test_overlay_flag_receipt_coverage_and_binding_are_typed(self):
        f = self.f
        good = {'rows': f.tokens[0]['rows'], 'receipts': f.overlay_ids,
                'coverage': [0, 1, 2], 'claim_complete': True}
        for field, value in [('claim_complete', 1), ('claim_complete', 1.0),
                             ('coverage', [False, 1, 2]), ('coverage', [0.0, 1, 2])]:
            bad = {**good, field: value}
            with self.assertRaises(InvalidCertificate):
                f.checker.check_overlay(bad, f.evidence, f.q, f.cut, 0, f.owners)
        for where, field, value in [('request', 'to', False), ('reply', 'at', 0.0),
                                    ('reply', 'epoch', False), ('request', 'shard', 0.0)]:
            evidence = deepcopy(f.evidence)
            evidence[2][where][field] = value
            with self.assertRaises(InvalidCertificate):
                f.checker.check_overlay(good, evidence, f.q, f.cut, 0, f.owners)
        self.assertTrue(f.checker.check_overlay(good, f.evidence, f.q, f.cut, 0, f.owners))

    def test_recursive_json_equality_preserves_types(self):
        self.assertFalse(same_json({'cut': [0], 'q': {'k': 1}}, {'cut': [False], 'q': {'k': 1}}))
        self.assertFalse(same_json({'q': {'k': 1}}, {'q': {'k': 1.0}}))
        self.assertTrue(same_json({'q': {'k': 1}, 'cut': [0]}, {'cut': [0], 'q': {'k': 1}}))
        f = self.f
        for token in f.cc.tokens[f.q['id']].values():
            token['query']['choice'] = 0
        q = {**f.q, 'choice': False}
        result = merge_tokens(f.q, f.cut, 0, f.tokens)
        result['query'] = q
        before = deepcopy(f.cc.tokens)
        with self.assertRaises(InvalidCertificate):
            f.cc.check_result(result, q, f.cut, 0, f.owners, f.evidence, {})
        self.assertEqual(before, f.cc.tokens)


if __name__ == '__main__':
    unittest.main()
