"""Owned-fixture protocol tests; assertions are not general mechanized proofs."""
import asyncio
import copy
import unittest
from src.model import features, PostingIndex, ranked, prefix_receipt, delta_receipt, assemble
from src.service import Replica, Network
from src.coordinator import certified, baseline
from src.checker import Checker, InvalidCertificate


def small():
    sources = {'b1':'def f(x):\n    return x\n',
               'b2':'def f(x,y):\n    return x+y\n',
               'b3':'def f(x,y,z):\n    return x+y+z\n',
               'b4':'def f(w,x,y,z):\n    return w+x+y+z\n',
               'b0':'def f(a):\n    return a\n'}
    payloads={b:{'source':s,'features':list(features(s))} for b,s in sources.items()}
    return payloads, {'id':'q','choice':0,'plan':[['name:x','name:y','name:z','name:w']],'k':2}


def event(seq, changes, shard=0):
    return {'seq':seq,'shard':shard,'changes':changes}


class StateTests(unittest.TestCase):
    def setUp(self):
        self.p, self.q = small()
        self.r = Replica(0,0,{'a':'b4','b':'b3','z':'b1'},self.p)

    def test_gap_and_duplicate(self):
        e2=event(2,[['b',None]])
        self.assertEqual(self.r.handle({'op':'receive','event':e2})['frontier'],0)
        self.r.handle({'op':'receive','event':event(1,[['a','b1']])})
        self.assertEqual(self.r.frontier,2)
        self.assertTrue(self.r.handle({'op':'receive','event':e2})['duplicate'])
        self.assertEqual(self.r.handle({'op':'receive','event':event(2,[['b','b2']])})['error'],'conflicting-sequence')
        self.r.handle({'op':'advance','to':2})
        self.assertEqual(self.r.index,{'a':'b1','z':'b1'})

    def test_compaction_restart_no_resurrection(self):
        self.r.handle({'op':'receive','event':event(1,[['a',None]])})
        self.r.handle({'op':'advance','to':1})
        self.r.handle({'op':'compact','to':1})
        self.r.handle({'op':'crash'}); self.r.handle({'op':'restart'})
        self.r.handle({'op':'receive','event':event(1,[['a','b4']])})
        self.assertNotIn('a',self.r.index)
        self.assertEqual(self.r.handle({'op':'delta','epoch':0,'shard':0,'query':self.q,'lo':0,'hi':1})['error'],'journal-coverage')

    def test_admission_bounds(self):
        self.assertEqual(self.r.handle({'op':'receive','event':event(129,[['a',None]])})['error'],'retention-window')
        big=event(1,[[str(i),'b1'] for i in range(17)])
        self.assertEqual(self.r.handle({'op':'receive','event':big})['error'],'batch-schema')
        self.assertEqual(self.r.handle({'op':'receive','event':event(1,[['a','missing']])})['error'],'unknown-body')

    def test_checkpoint_opens_window(self):
        for n in range(1,129):
            self.assertTrue(self.r.handle({'op':'receive','event':event(n,[['a','b1']])})['ok'])
        self.r.handle({'op':'advance','to':128})
        self.r.handle({'op':'compact','to':128})
        self.assertTrue(self.r.handle({'op':'receive','event':event(129,[['a',None]])})['ok'])
        self.assertLessEqual(self.r.peak_journal,128)
        self.assertLessEqual(self.r.peak_pending,128)

    def test_epoch_install(self):
        self.r.handle({'op':'install','epoch':1,'shard':2,'state':{'c':'b1'},'at':7})
        self.assertEqual(self.r.index,{'c':'b1'})
        self.assertEqual(self.r.handle({'op':'prefix','epoch':0,'shard':0,'query':self.q,'length':5})['error'],'ownership-epoch')
        self.assertEqual(self.r.handle({'op':'install','epoch':1,'shard':2,'state':{},'at':7})['error'],'old-epoch')

    def test_postings_after_updates(self):
        state=dict(self.r.index); index=PostingIndex(state,self.p)
        events=[event(1,[['a',None],['n','b4']]),event(2,[['b','b0']]),event(3,[['a','b2']])]
        for e in events:
            index.apply(e)
            for i,b in e['changes']:
                if b is None: state.pop(i,None)
                else: state[i]=b
            self.assertEqual(index.rows(self.q['plan']),ranked(state,self.p,self.q['plan']))
        self.assertEqual(index.rows(self.q['plan'],{'a'}),[r for r in ranked(state,self.p,self.q['plan']) if r['id']!='a'])

    def test_atomic_rewrite_and_delete(self):
        self.r.handle({'op':'receive','event':event(1,[['a',None],['c','b4']])})
        self.r.handle({'op':'advance','to':1})
        self.assertNotIn('a',self.r.index);self.assertEqual(self.r.index['c'],'b4')

    def test_local_score_and_alternate(self):
        c=Checker(self.p)
        q={'plan':[['name:a'],['name:x','name:y']]}
        for b,p in self.p.items():
            self.assertEqual(set(p['features']),c.labels[b])
        self.assertEqual(c.value('b0',q['plan']),1)
        self.assertEqual(c.value('b4',q['plan']),2)

    def test_oracle_missing_history_rejected(self):
        with self.assertRaises(InvalidCertificate):
            Checker(self.p).oracle([{}, {}, {}],[[],[],[]],[1,0,0],self.q)

    def test_gap_free_receipt_required(self):
        with self.assertRaises(ValueError):
            delta_receipt([event(2,[['x','b1']])],self.p,self.q['plan'],2,0,0,0,2)


class QueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.p,self.q=small();self.checker=Checker(self.p)
        self.initial=[{'a':'b4','b':'b3','z':'b1'},{'c':'b2'},{}]
        self.replicas=[Replica(i,i//2,self.initial[i//2],self.p) for i in range(6)]
        self.net=await Network(self.replicas).__aenter__();self.owners=[[0,1],[2,3],[4,5]]
        self.events=[[event(1,[['b',None]])],[],[]];self.cut=[1,0,0]
        for n in (0,1): await self.net.rpc(n,{'op':'receive','event':self.events[0][0]})

    async def asyncTearDown(self):
        await self.net.__aexit__(None,None,None)

    async def cert(self,length=8):
        return await certified(self.net,self.q,self.cut,0,self.owners,length)

    async def test_exact_certificate(self):
        c=await self.cert();self.assertEqual(c['status'],'complete')
        self.assertTrue(self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners))
        self.assertEqual(c['rows'],self.checker.oracle(self.initial,self.events,self.cut,self.q))

    async def test_conservative_boundary_not_necessity(self):
        c=await self.cert(1)
        self.assertEqual(c['rows'],self.checker.oracle(self.initial,self.events,self.cut,self.q))
        self.assertEqual(c['status'],'rank-underdetermined')
        self.assertEqual(c['rank_blockers'],[0])

    async def test_strong_baseline_and_unsafe_control(self):
        exact=await baseline(self.net,self.q,self.cut,0,self.owners,'overlay')
        wrong=await baseline(self.net,self.q,self.cut,0,self.owners,'unverified')
        self.assertEqual(exact['rows'],self.checker.oracle(self.initial,self.events,self.cut,self.q))
        self.assertEqual(self.checker.sound_rows(wrong['rows'],self.initial,self.events,self.cut,self.q),1)
        self.assertTrue(wrong['claim_complete'])

    async def test_partition_sound_partial(self):
        self.net.blocked={2,3}
        c=await self.cert();self.assertEqual(c['status'],'partial')
        self.assertEqual(c['missing_shards'],[1])
        self.assertEqual(self.checker.sound_rows(c['rows'],self.initial,self.events,self.cut,self.q),0)
        self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners)

    async def test_forged_status_rejected(self):
        c=await self.cert(1);c['status']='complete'
        with self.assertRaises(InvalidCertificate):
            self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners)

    async def test_cross_request_rejected(self):
        c=await self.cert();other=copy.deepcopy(self.q);other['plan']=[['name:x']]
        with self.assertRaises(InvalidCertificate):
            self.checker.check(c,self.net.evidence,other,self.cut,0,self.owners)

    async def test_foreign_epoch_rejected(self):
        c=await self.cert();c['epoch']=1
        with self.assertRaises(InvalidCertificate):
            self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners)

    async def test_forged_row_rejected(self):
        c=await self.cert();c=copy.deepcopy(c);c['rows'][0]['score']=1
        with self.assertRaises(InvalidCertificate):
            self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners)

    async def test_lost_reply_retry_is_idempotent(self):
        e=event(2,[['a',None]])
        _,r=await self.net.rpc(0,{'op':'receive','event':e},lost_reply=True)
        self.assertIsNone(r)
        _,r=await self.net.rpc(0,{'op':'receive','event':e})
        self.assertTrue(r['duplicate']);self.assertEqual(self.replicas[0].frontier,2)

    async def test_wire_request_is_immutable_snapshot(self):
        state={'new':'b1'}
        ident,_=await self.net.rpc(0,{'op':'install','epoch':1,'shard':0,'state':state,'at':1})
        state['later']='b2'
        self.assertEqual(self.net.evidence[ident]['request']['state'],{'new':'b1'})

    async def test_cross_replica_receipt_composition(self):
        # A stale prefix on node 0 is bound to a complete journal on node 1.
        self.replicas[0].journal.clear();self.replicas[0].frontier=0
        c=await self.cert()
        p,d=c['pairs']['0']
        self.assertEqual(self.net.evidence[p]['node'],0)
        self.assertEqual(self.net.evidence[d]['node'],1)
        self.checker.check(c,self.net.evidence,self.q,self.cut,0,self.owners)
        self.assertEqual(c['rows'],self.checker.oracle(self.initial,self.events,self.cut,self.q))

    async def test_checker_does_not_authenticate_issuer(self):
        # Explicit trust-boundary control: a source's false exhausted-prefix
        # assertion is not magically detected by the receipt-only checker.
        c=await self.cert();t=copy.deepcopy(self.net.evidence)
        for pair in c['pairs'].values():
            t[pair[0]]['reply']['rows']=[];t[pair[0]]['reply']['boundary']=None
            t[pair[1]]['reply']['rows']=[]
        lie=assemble(self.q,self.cut,0,c['pairs'],t)
        self.checker.check(lie,t,self.q,self.cut,0,self.owners)
        self.assertNotEqual(lie['rows'],self.checker.oracle(self.initial,self.events,self.cut,self.q))


class CorpusStatisticsBoundaryTests(unittest.TestCase):
    def test_unchanged_document_rank_flip_and_false_old_boundary(self):
        # Illustrative TF-IDF/cosine algebra, not a FaCoY/Lucene execution.
        import math
        old_x, old_y = math.log(3), math.log(3/2)
        new_x, new_y = math.log(5/3), math.log(5/2)
        old_a = old_x/math.hypot(old_x,old_y)
        old_b = old_y/math.hypot(old_x,old_y)
        new_a = new_x/math.hypot(new_x,new_y)
        new_b = new_y/math.hypot(new_x,new_y)
        self.assertGreater(old_a,old_b)
        self.assertGreater(new_b,new_a)
        self.assertGreater(new_a,old_b)  # stale threshold would wrongly pass

    def test_co_resident_row_count_bound(self):
        for k in range(1,21):
            for length in (k,k+1,64):
                for old in range(25):
                    for changed in range(25):
                        self.assertLessEqual(min(k,old+changed), min(length,old)+min(k,changed))


class ContinuationTokenTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from src.continuation_checker import ContinuationChecker
        self.p, self.q = small()
        self.q['k'] = 2
        self.initial = [
            {'a':'b4','b':'b3','c':'b2','d':'b1'},
            {'e':'b2'},
            {},
        ]
        self.replicas = [Replica(i,i//2,self.initial[i//2],self.p) for i in range(6)]
        self.net = await Network(self.replicas).__aenter__()
        self.owners = [[0,1],[2,3],[4,5]]
        self.cc = ContinuationChecker(self.p)

    async def asyncTearDown(self):
        await self.net.__aexit__(None,None,None)

    async def test_buffer_tail_survives_top_deletion(self):
        from src.continuation import token_from_prefix, advance_token, merge_tokens
        tokens = {}
        for shard in range(3):
            ident, reply = await self.net.rpc(self.owners[shard][0], {
                'op':'prefix','shard':shard,'epoch':0,'query':self.q,'length':3})
            tokens[shard] = token_from_prefix(self.q, reply, 3)
            self.cc.install_prefix(self.q, self.net.evidence[ident], self.owners, 0, 3)
        update = event(1,[['a',None]],0)
        for node in self.owners[0]:
            await self.net.rpc(node, {'op':'receive','event':update})
        event_ids = {}
        for shard in range(3):
            hi = 1 if shard == 0 else 0
            ident, reply = await self.net.rpc(self.owners[shard][0], {
                'op':'events','shard':shard,'epoch':0,'lo':0,'hi':hi})
            event_ids[shard] = ident
            self.cc.accept_events(self.net.evidence[ident], self.owners, 0)
            tokens[shard] = advance_token(tokens[shard], reply, self.p)
        result = merge_tokens(self.q,[1,0,0],0,tokens)
        result['repairs'] = {}
        self.assertEqual(result['status'],'complete')
        self.assertEqual([r['id'] for r in result['rows']],['b','c'])
        self.cc.check_result(result,self.q,[1,0,0],0,self.owners,self.net.evidence,{})

    async def test_event_snapshot_and_forged_result_rejected(self):
        from src.continuation import token_from_prefix, merge_tokens
        ident, snap = await self.net.rpc(0, {'op':'snapshot','shard':0,'epoch':0,'at':0})
        self.assertEqual(snap['kind'],'snapshot')
        self.assertEqual(dict(snap['state']),self.initial[0])
        tokens = {}
        for shard in range(3):
            ident, reply = await self.net.rpc(self.owners[shard][0], {
                'op':'prefix','shard':shard,'epoch':0,'query':self.q,'length':2})
            tokens[shard] = token_from_prefix(self.q,reply,2)
            self.cc.install_prefix(self.q,self.net.evidence[ident],self.owners,0,2)
        result = merge_tokens(self.q,[0,0,0],0,tokens)
        forged = copy.deepcopy(result); forged['status']='partial'
        with self.assertRaises(InvalidCertificate):
            self.cc.check_result(forged,self.q,[0,0,0],0,self.owners,self.net.evidence,{})

class ContinuationPureTests(unittest.TestCase):
    def test_noop_suffix_preserves_token_and_generation(self):
        from src.continuation import advance_token
        token={'kind':'continuation-token','query':self._query(),'shard':0,'epoch':0,
               'at':3,'capacity':2,'rows':[{'id':'a','body':'b4','score':3}],
               'tail':None,'generation':7}
        got=advance_token(token,{'kind':'events','shard':0,'epoch':0,'lo':3,'hi':3,'events':[]},small()[0])
        self.assertEqual(got,token)

    def test_equal_rank_tail_is_a_blocker(self):
        from src.continuation import merge_tokens
        query=self._query(); query['k']=1
        token={'kind':'continuation-token','query':query,'shard':0,'epoch':0,'at':0,
               'capacity':1,'rows':[{'id':'b','body':'b3','score':2}],
               'tail':[-2,'a'],'generation':0}
        result=merge_tokens(query,[0,0,0],0,{0:token})
        self.assertEqual(result['status'],'partial')
        self.assertEqual(result['rank_blockers'],[0])

    def test_k_plus_dirty_repair_bound(self):
        from src.continuation import sufficient_repair_length
        query=self._query(); query['k']=5
        self.assertEqual(sufficient_repair_length(query,[]),5)
        events=[{'seq':1,'shard':0,'changes':[['a',None],['b','b1']]},
                {'seq':2,'shard':0,'changes':[['a','b2'],['c','b3']]}]
        self.assertEqual(sufficient_repair_length(query,events),8)
        self.assertEqual(sufficient_repair_length(query,events,reserve=2),10)
        with self.assertRaises(ValueError): sufficient_repair_length(query,events,reserve=-1)
        too_many=[{'seq':1,'shard':0,'changes':[[f'i{i}',None] for i in range(60)]}]
        with self.assertRaises(ValueError): sufficient_repair_length(query,too_many)

    def test_suffix_rollback_is_rejected(self):
        from src.continuation import advance_token
        token={'kind':'continuation-token','query':self._query(),'shard':0,'epoch':0,
               'at':3,'capacity':2,'rows':[],'tail':None,'generation':0}
        with self.assertRaises(ValueError):
            advance_token(token,{'kind':'events','shard':0,'epoch':0,'lo':3,'hi':2,'events':[]},small()[0])

    def test_continuation_checker_has_no_producer_imports(self):
        import ast
        from pathlib import Path
        path = Path(__file__).resolve().parents[1] / "src" / "continuation_checker.py"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        forbidden = {"continuation", "continuation_coordinator", "coordinator", "service"}
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.rsplit(".", 1)[-1] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.rsplit(".", 1)[-1])
        self.assertTrue(forbidden.isdisjoint(imported), (forbidden, imported))

    def test_feed_compaction_preserves_future_coverage(self):
        from src.continuation_coordinator import FeedStore
        feed=FeedStore(0,[0,0,0])
        feed.accept({'kind':'events','shard':0,'epoch':0,'lo':0,'hi':2,'events':[
            {'shard':0,'seq':1,'changes':[['a',None]]},
            {'shard':0,'seq':2,'changes':[['b',None]]}]})
        feed.compact([1,0,0])
        self.assertFalse(feed.covers(0,0,2))
        self.assertTrue(feed.covers(0,1,2))
        self.assertEqual([e['seq'] for e in feed.logical_state()['events'][0]],[2])
        # A short history stays intact because any admitted index may lag by
        # the full bounded journal window.
        other=FeedStore(0,[0,0,0])
        other.accept({'kind':'events','shard':0,'epoch':0,'lo':0,'hi':2,'events':[
            {'shard':0,'seq':1,'changes':[['a',None]]},
            {'shard':0,'seq':2,'changes':[['b',None]]}]})
        self.assertEqual(other.safe_compaction_floors([2,0,0]),[0,0,0])

    @staticmethod
    def _query():
        return {'id':'q','plan':[['name:a','name:b','name:c']],'k':2,'choice':0,
                'source_body':'b4','seed_id':'seed','origin_repo':'fixture'}


if __name__=='__main__':
    unittest.main()
