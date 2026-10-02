"""Directed regression tests for token identity and all-or-nothing checking.

These are tests of static boundary risks, not evidence of field failures.
"""
from copy import deepcopy
import unittest
from src.checker import InvalidCertificate
from src.continuation_checker import ContinuationChecker
from src.continuation import token_from_prefix, advance_token, merge_tokens
from test_protocol import small


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.payloads, self.query = small()
        self.query['k'] = 1
        self.query['plan'] = [['name:x']]
        self.owners = [[0, 1], [2, 3], [4, 5]]
        self.cc = ContinuationChecker(self.payloads)
        self.entries = {}; self.tokens = {}
        for shard in range(3):
            rows = [{'id':'a', 'body':'b1', 'score':1}] if shard == 0 else []
            tail = [-1, 'b'] if shard == 0 else None
            entry = {'node':shard*2, 'request':{'op':'prefix','shard':shard,'epoch':0,
                     'query':deepcopy(self.query),'length':1},
                     'reply':{'kind':'prefix','shard':shard,'epoch':0,'plan':deepcopy(self.query['plan']),
                              'at':0,'rows':rows,'boundary':tail}}
            self.entries[shard] = entry
            self.cc.install_prefix(self.query, entry, self.owners, 0, 1)
            self.tokens[shard] = token_from_prefix(self.query, entry['reply'], 1)

    def state(self):
        return deepcopy((self.cc.tokens,self.cc.events,self.cc.frontiers,self.cc.epoch))

    def event_entry(self, changes=None):
        return {'node':0,'request':{'op':'events','shard':0,'epoch':0,'lo':0,'hi':1},
                'reply':{'kind':'events','shard':0,'epoch':0,'lo':0,'hi':1,
                         'events':[{'seq':1,'shard':0,'changes':changes or [['a',None],['b','b1']]}]}}

    def test_tail_identity_is_conservative_not_necessary(self):
        feed = self.event_entry()
        self.cc.accept_events(feed,self.owners,0)
        self.tokens[0] = advance_token(self.tokens[0], feed['reply'], self.payloads)
        result = merge_tokens(self.query,[1,0,0],0,self.tokens)
        self.assertEqual(result['rows'],[{'id':'b','body':'b1','score':1}])
        self.assertEqual(result['rank_blockers'],[0])
        self.assertEqual(self.tokens[0]['tail'],[-1,'b'])
        self.assertTrue(self.cc.check_result(result,self.query,[1,0,0],0,self.owners,self.entries,{}))
        # The finite catalog's only remaining live positive is b. The blocker
        # is present even though b is the exact answer, by unique ID.
        state={'a':'b1','b':'b1'}
        for ident,body in feed['reply']['events'][0]['changes']:
            if body is None: state.pop(ident,None)
            else: state[ident]=body
        self.assertEqual(list(state),['b'])

    def test_rebound_plan_and_k_are_rejected_without_mutation(self):
        for field,value in [('plan',[['name:y']]),('k',2),('choice',1)]:
            q=deepcopy(self.query);q[field]=value
            response=merge_tokens(self.query,[0,0,0],0,self.tokens)
            response['query']=q
            before=self.state()
            with self.assertRaises(InvalidCertificate):
                self.cc.check_result(response,q,[0,0,0],0,self.owners,self.entries,{})
            self.assertEqual(before,self.state())

    def test_failed_advance_then_valid_retry(self):
        feed=self.event_entry(); self.cc.accept_events(feed,self.owners,0)
        self.tokens[0]=advance_token(self.tokens[0],feed['reply'],self.payloads)
        response=merge_tokens(self.query,[1,0,0],0,self.tokens)
        bad=deepcopy(response);bad['rows']=[]
        before=self.state()
        with self.assertRaises(InvalidCertificate):
            self.cc.check_result(bad,self.query,[1,0,0],0,self.owners,self.entries,{})
        self.assertEqual(before,self.state())
        self.assertTrue(self.cc.check_result(response,self.query,[1,0,0],0,self.owners,self.entries,{}))

    def test_failed_batch_then_valid_retry(self):
        entry=self.event_entry();entry['request']['hi']=2;entry['reply']['hi']=2
        entry['reply']['events'].append({'seq':2,'shard':0,'changes':[['z','not-in-catalog']]})
        before=self.state()
        with self.assertRaises(InvalidCertificate):self.cc.accept_events(entry,self.owners,0)
        self.assertEqual(before,self.state())
        entry['reply']['events'][1]['changes']=[['z',None]]
        self.assertEqual(self.cc.accept_events(entry,self.owners,0),2)

    def test_failed_repair_then_valid_retry(self):
        reply=merge_tokens(self.query,[0,0,0],0,self.tokens)
        bad=deepcopy(reply);bad['token_summary']={}
        repairs={'0':{'receipt':0,'capacity':1}}
        before=self.state()
        with self.assertRaises(InvalidCertificate):
            self.cc.check_result(bad,self.query,[0,0,0],0,self.owners,self.entries,repairs)
        self.assertEqual(before,self.state())
        self.assertTrue(self.cc.check_result(reply,self.query,[0,0,0],0,self.owners,self.entries,repairs))

    def test_event_epoch_and_rollback_are_rejected(self):
        self.cc.accept_events(self.event_entry(),self.owners,0)
        before=self.state()
        bad={'node':0,'request':{'op':'events','shard':0,'epoch':0,'lo':1,'hi':0},
             'reply':{'kind':'events','shard':0,'epoch':0,'lo':1,'hi':0,'events':[]}}
        with self.assertRaises(InvalidCertificate):self.cc.accept_events(bad,self.owners,0)
        self.assertEqual(before,self.state())
        bad=deepcopy(self.event_entry());bad['request']['epoch']=1;bad['reply']['epoch']=1
        with self.assertRaises(InvalidCertificate):self.cc.accept_events(bad,self.owners,1)
        self.assertEqual(before,self.state())

    def test_partial_compaction_is_not_committed(self):
        before=self.state()
        with self.assertRaises(InvalidCertificate):self.cc.compact_events([0,1,0])
        self.assertEqual(before,self.state())

if __name__=='__main__':unittest.main()
