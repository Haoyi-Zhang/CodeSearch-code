#!/usr/bin/env python3
"""Exhaustive *finite* receipt cases and six-delivery schedules; not model checking of unbounded TCP."""
from __future__ import annotations
import argparse
import asyncio
import collections
import itertools
import json
import resource
import time
from pathlib import Path
from src.model import features, prefix_receipt, delta_receipt, assemble
from src.checker import Checker
from src.service import Replica
from src.coordinator import certified

ROOT=Path(__file__).resolve().parent


def domain():
    p={}
    for i,code in enumerate(('def f(x):\n    return x\n','def f(x,y):\n    return x+y\n'),1):
        p[f'b{i}']={'source':code,'features':list(features(code))}
    return p


def receipts():
    ts=time.monotonic();cpu=time.process_time();p=domain();ck=Checker(p)
    ids=['a','b','c','d'];shards=[0,0,1,2];owners=[[0,1],[2,3],[4,5]]
    totals=collections.Counter();groups=collections.defaultdict(collections.Counter);examples=[]
    for codes in itertools.product(range(3),repeat=8):
        base=[{}, {}, {}];events=[[],[],[]]
        for s in range(3):
            changes=[]
            for j,ident in enumerate(ids):
                if shards[j]!=s:continue
                old,new=codes[j],codes[j+4]
                if old:base[s][ident]=f'b{old}'
                if old!=new:changes.append([ident,f'b{new}' if new else None])
            if changes:events[s]=[{'shard':s,'seq':1,'changes':changes}]
        cut=list(map(len,events))
        for k in (1,2,3):
            q={'id':'finite','choice':0,'k':k,'plan':[['name:x','name:y']]}
            exact=ck.oracle(base,events,cut,q)
            for length in (0,1,2,4):
                evidence={};pairs={}
                for s in range(3):
                    pr=prefix_receipt(base[s],p,q['plan'],length,s,0,0)
                    dr=delta_receipt(events[s],p,q['plan'],k,s,0,0,cut[s])
                    a,b=2*s+1,2*s+2
                    evidence[a]={'node':2*s,'request':{'op':'prefix','shard':s,'epoch':0,'query':q,'length':length},'reply':pr}
                    evidence[b]={'node':2*s,'request':{'op':'delta','shard':s,'epoch':0,'query':q,'lo':0,'hi':cut[s]},'reply':dr}
                    pairs[str(s)]=[a,b]
                cert=assemble(q,cut,0,pairs,evidence)
                ck.check(cert,evidence,q,cut,0,owners)
                unsound=ck.sound_rows(cert['rows'],base,events,cut,q)
                same=cert['rows']==exact;complete=cert['status']=='complete'
                if unsound or (complete and not same):raise AssertionError((codes,k,length,cert,exact))
                for counter in (totals,groups[f'k{k}-prefix{length}']):
                    counter['cases']+=1;counter['complete']+=complete
                    counter['exact_result']+=same
                    counter['conservative_noncertification']+=(same and not complete)
                    counter['inexact_uncertified']+=(not same and not complete)
                if same and not complete and len(examples)<8:
                    examples.append({'base_codes':codes[:4],'target_codes':codes[4:],'k':k,'length':length,
                        'result':cert['rows'],'status':cert['status'],'blockers':cert['rank_blockers']})
    out={'domain':'4 IDs on shards [0,0,1,2]; absent/score1/score2 at each base and target; k=1,2,3; L=0,1,2,4',
         'state_pairs':3**8,'totals':dict(totals),'groups':dict(groups),'counterexamples':examples,
         'false_complete':0,'unsound_rows':0,
         'resources':{'cpu_seconds':time.process_time()-cpu,'wall_seconds':time.monotonic()-ts,
                      'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}}
    (ROOT/'results/finite-receipts.json').write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps({k:v for k,v in out.items() if k not in {'groups','counterexamples'}}),flush=True)


class DirectNetwork:
    """Exercise the real coordinator/state machine without claiming network timing."""
    def __init__(self,replicas):self.replicas=replicas;self.evidence={};self.number=0
    async def rpc(self,node,req):
        self.number+=1
        reply=self.replicas[node].handle(req)
        self.evidence[self.number]={'node':node,'request':req,'reply':reply}
        return self.number,reply


async def schedules():
    ts=time.monotonic();cpu=time.process_time();p=domain();ck=Checker(p)
    base=[{'a':'b1'},{'b':'b1'},{'c':'b1'}]
    events=[[{'shard':s,'seq':1,'changes':[[chr(97+s),'b2']]}] for s in range(3)]
    q={'id':'schedule','choice':0,'k':2,'plan':[['name:x','name:y']]};cut=[1,1,1];owners=[[0,1],[2,3],[4,5]]
    exact=ck.oracle(base,events,cut,q);counts=collections.Counter()
    for order in itertools.permutations(range(6)):
        replicas=[Replica(i,i//2,base[i//2],p) for i in range(6)];net=DirectNetwork(replicas)
        for depth in range(7):
            if depth:
                node=order[depth-1]
                replicas[node].handle({'op':'receive','event':events[node//2][0]})
            c=await certified(net,q,cut,0,owners,2)
            ck.check(c,net.evidence,q,cut,0,owners)
            if ck.sound_rows(c['rows'],base,events,cut,q):raise AssertionError('unsound schedule')
            if c['status']=='complete' and c['rows']!=exact:raise AssertionError('false complete schedule')
            counts[c['status']]+=1;counts['checks']+=1
    out={'permutations':720,'query_positions_per_permutation':7,'counts':dict(counts),
         'false_complete':0,'unsound_rows':0,
         'scope':'one delivered update per each of six replicas, three logical updates; query after each prefix, no intra-query faults; direct state-machine transport',
         'resources':{'cpu_seconds':time.process_time()-cpu,'wall_seconds':time.monotonic()-ts,
                      'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}}
    (ROOT/'results/finite-schedules.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--part',choices=('receipts','schedules','all'),default='all')
    args=parser.parse_args()
    if args.part in ('all','receipts'):receipts()
    if args.part in ('all','schedules'):asyncio.run(schedules())
