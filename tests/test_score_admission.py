"""Benign JSON type-admission checks; no network or upstream code execution."""
from copy import deepcopy
import json
import unittest

from src.checker import Checker, InvalidCertificate
from src.continuation import token_from_prefix, advance_token, merge_tokens, rebind_token
from src.continuation_checker import ContinuationChecker
from src.model import assemble, features
from src.service import Replica
from analyze_continuation import verify_source_receipts


def decoded(value):
    return json.loads(json.dumps(value))


class ScoreAdmissionTests(unittest.TestCase):
    def setUp(self):
        source = 'def f(x):\n    return x\n'
        self.payloads = {'body': {'source': source, 'features': list(features(source))}}
        self.q = {'id': 'q', 'plan': [['name:x']], 'k': 1}
        self.cut = [0, 0, 0]
        self.owners = [[0, 1], [2, 3], [4, 5]]
        self.checker = Checker(self.payloads)
        self.cc = ContinuationChecker(self.payloads)
        self.evidence, self.pairs, self.tokens, self.overlay_ids = {}, {}, {}, {}
        for s in range(3):
            rows = [{'id': 'a', 'body': 'body', 'score': 1}] if s == 0 else []
            p = {'kind': 'prefix', 'shard': s, 'epoch': 0, 'at': 0,
                 'plan': self.q['plan'], 'rows': rows, 'boundary': None}
            req = {'op': 'prefix', 'shard': s, 'epoch': 0, 'query': self.q, 'length': 1}
            self.evidence[3*s] = decoded({'node': 2*s, 'request': req, 'reply': p})
            self.cc.install_prefix(self.q, self.evidence[3*s], self.owners, 0, 1)
            self.tokens[s] = token_from_prefix(self.q, p, 1)
            self.evidence[3*s+1] = decoded({'node': 2*s,
                'request': {'op': 'delta', 'shard': s, 'epoch': 0, 'query': self.q, 'lo': 0, 'hi': 0},
                'reply': {'kind': 'delta', 'shard': s, 'epoch': 0, 'plan': self.q['plan'],
                          'lo': 0, 'hi': 0, 'changed': [], 'rows': []}})
            self.evidence[3*s+2] = decoded({'node': 2*s,
                'request': {'op': 'overlay', 'shard': s, 'epoch': 0, 'query': self.q, 'to': 0},
                'reply': {'kind': 'overlay', 'shard': s, 'epoch': 0, 'at': 0,
                          'plan': self.q['plan'], 'rows': rows}})
            self.pairs[str(s)] = [3*s, 3*s+1]
            self.overlay_ids[str(s)] = 3*s+2

    def test_output_scores_have_strict_integer_types(self):
        cert = assemble(self.q, self.cut, 0, self.pairs, self.evidence)
        cont = merge_tokens(self.q, self.cut, 0, self.tokens)
        overlay = {'rows': cert['rows'], 'receipts': self.overlay_ids,
                   'coverage': [0, 1, 2], 'claim_complete': True}
        for value in (True, False, 1.0, '1', None, 0, -1):
            for kind, valid in [('certificate', cert), ('overlay', overlay), ('continuation', cont)]:
                with self.subTest(kind=kind, value=value):
                    bad = decoded(valid); bad['rows'][0]['score'] = value
                    before = deepcopy(self.cc.tokens)
                    with self.assertRaises(InvalidCertificate):
                        if kind == 'certificate':
                            self.checker.check(bad, self.evidence, self.q, self.cut, 0, self.owners)
                        elif kind == 'overlay':
                            self.checker.check_overlay(bad, self.evidence, self.q, self.cut, 0, self.owners)
                        else:
                            self.cc.check_result(bad, self.q, self.cut, 0, self.owners, self.evidence, {})
                    self.assertEqual(before, self.cc.tokens)
        self.assertTrue(self.checker.check(cert, self.evidence, self.q, self.cut, 0, self.owners))
        self.assertTrue(self.checker.check_overlay(overlay, self.evidence, self.q, self.cut, 0, self.owners))
        self.assertTrue(self.cc.check_result(cont, self.q, self.cut, 0, self.owners, self.evidence, {}))

    def test_source_prefix_delta_overlay_scores(self):
        cert = assemble(self.q, self.cut, 0, self.pairs, self.evidence)
        overlay = {'rows': cert['rows'], 'receipts': self.overlay_ids,
                   'coverage': [0, 1, 2], 'claim_complete': True}
        for value in (True, False, 1.0, '1', None):
            for receipt in (0, 1, 2):
                with self.subTest(value=value, receipt=receipt):
                    evidence = deepcopy(self.evidence)
                    rep = evidence[receipt]['reply']
                    rep['rows'] = [{'id': 'a', 'body': 'body', 'score': value}]
                    if receipt == 1:
                        rep['changed'] = ['a']
                    with self.assertRaises(InvalidCertificate):
                        if receipt == 2:
                            self.checker.check_overlay(overlay, evidence, self.q, self.cut, 0, self.owners)
                        else:
                            self.checker.check(cert, evidence, self.q, self.cut, 0, self.owners)
                    if receipt == 0:
                        before = deepcopy(self.cc.tokens)
                        with self.assertRaises(InvalidCertificate):
                            self.cc.install_prefix(self.q, evidence[0], self.owners, 0, 1)
                        self.assertEqual(before, self.cc.tokens)

    def test_producer_token_admission_including_noop(self):
        suffix = {'kind': 'events', 'shard': 0, 'epoch': 0, 'lo': 0, 'hi': 0, 'events': []}
        for value in (True, False, 1.0, '1', None):
            p = deepcopy(self.evidence[0]['reply']); p['rows'][0]['score'] = value
            with self.assertRaises(ValueError): token_from_prefix(self.q, p, 1)
            token = deepcopy(self.tokens[0]); token['rows'][0]['score'] = value
            with self.assertRaises(ValueError): advance_token(token, suffix, self.payloads)
            with self.assertRaises(ValueError): merge_tokens(self.q, self.cut, 0, {0: token})
            with self.assertRaises(ValueError): rebind_token(token, 1)

    def test_tail_types_in_prefix_token_and_summary(self):
        for value in (True, False, -1.0, '-1', None):
            p = deepcopy(self.evidence[0]['reply']); p['boundary'] = [value, 'z']
            with self.assertRaises(ValueError): token_from_prefix(self.q, p, 1)
            entry = deepcopy(self.evidence[0]); entry['reply'] = p
            with self.assertRaises(InvalidCertificate):
                self.cc.install_prefix(self.q, entry, self.owners, 0, 1)
            result = merge_tokens(self.q, self.cut, 0, self.tokens)
            result['token_summary']['0']['tail'] = [value, 'z']
            with self.assertRaises(InvalidCertificate):
                self.cc.check_result(result, self.q, self.cut, 0, self.owners, self.evidence, {})

    def test_summary_integer_metadata_rejects_equal_json_values(self):
        valid = merge_tokens(self.q, self.cut, 0, self.tokens)
        for field, replacements in (('at', (False, 0.0)),
                                    ('capacity', (True, 1.0)),
                                    ('generation', (False, 0.0))):
            for value in replacements:
                with self.subTest(field=field, value=value):
                    bad = decoded(valid)
                    bad['token_summary']['0'][field] = value
                    before = deepcopy(self.cc.tokens)
                    with self.assertRaises(InvalidCertificate):
                        self.cc.check_result(bad, self.q, self.cut, 0,
                                             self.owners, self.evidence, {})
                    self.assertEqual(before, self.cc.tokens)
        self.assertTrue(self.cc.check_result(valid, self.q, self.cut, 0,
                                             self.owners, self.evidence, {}))

    def test_soundness_metric_counts_noninteger_scores(self):
        row = {'id': 'a', 'body': 'body', 'score': 1}
        for value in (True, False, 1.0, '1', None):
            self.assertEqual(1, self.checker.sound_rows([{**row, 'score': value}],
                [{'a': 'body'}, {}, {}], [[], [], []], self.cut, self.q))
        self.assertEqual(0, self.checker.sound_rows([row], [{'a': 'body'}, {}, {}],
                                                  [[], [], []], self.cut, self.q))

    def test_offline_receipt_replay_types(self):
        for value in (True, 1.0):
            for receipt in (0, 2):
                entry = deepcopy(self.evidence[receipt]); entry['reply']['rows'][0]['score'] = value
                trace = {'initial': [{'a': 'body'}, {}, {}], 'history': [[], [], []],
                         'transcript': {'1': entry}}
                with self.assertRaises(InvalidCertificate): verify_source_receipts(trace, self.payloads)

    def test_json_k_and_length_admission(self):
        replica = Replica(0, 0, {'a': 'body'}, self.payloads)
        for value in (True, False, 1.0, '1', None):
            q = {**self.q, 'k': value}
            self.assertEqual('query-bound', replica.handle(decoded({'op': 'prefix',
                'query': q, 'epoch': 0, 'shard': 0, 'length': 1}))['error'])
            self.assertEqual('prefix-bound', replica.handle(decoded({'op': 'prefix',
                'query': self.q, 'epoch': 0, 'shard': 0, 'length': value}))['error'])

    def test_rejected_score_advance_and_repair_then_valid_retry(self):
        feed = decoded({'node': 0, 'request': {'op': 'events', 'shard': 0,
            'epoch': 0, 'lo': 0, 'hi': 1}, 'reply': {'kind': 'events', 'shard': 0,
            'epoch': 0, 'lo': 0, 'hi': 1, 'events': [
                {'shard': 0, 'seq': 1, 'changes': [['a', 'body']]}]}})
        self.cc.accept_events(feed, self.owners, 0)
        tokens = deepcopy(self.tokens)
        tokens[0] = advance_token(tokens[0], feed['reply'], self.payloads)
        valid = merge_tokens(self.q, [1, 0, 0], 0, tokens)
        repair = deepcopy(self.evidence[0]); repair['reply']['at'] = 1
        evidence = {**self.evidence, 10: repair}
        for repairs in ({}, {'0': {'receipt': 10, 'capacity': 1}}):
            for value in (True, 1.0):
                bad = decoded(valid); bad['rows'][0]['score'] = value
                before = deepcopy(self.cc.tokens)
                with self.assertRaises(InvalidCertificate):
                    self.cc.check_result(bad, self.q, [1, 0, 0], 0, self.owners, evidence, repairs)
                self.assertEqual(before, self.cc.tokens)
        self.assertTrue(self.cc.check_result(valid, self.q, [1, 0, 0], 0,
                                             self.owners, evidence, {}))


if __name__ == '__main__':
    unittest.main()
