#!/usr/bin/env python3
"""Finite exhaustive checks for continuation-token and merge invariants."""
from __future__ import annotations
import itertools
import json
from pathlib import Path
from src.continuation import advance_token, merge_tokens, token_from_prefix
from src.model import PostingIndex, key, score

ROOT = Path(__file__).resolve().parent
PAYLOADS = {
    'm0': {'features': []},
    'm1': {'features': ['x']},
    'm2': {'features': ['x','y']},
}
QUERY2 = {'id':'finite-q2','k':2,'origin_repo':'finite','seeds':['x','y'],
          'plan':[['x','y']]}
VALUES = (None,'m0','m1','m2')


def prefix(query, state, shard, capacity):
    rows = PostingIndex(state,PAYLOADS).rows(query['plan'])
    return {'kind':'prefix','shard':shard,'epoch':0,'at':0,'plan':query['plan'],
            'rows':rows[:capacity],
            'boundary':list(key(rows[capacity])) if len(rows)>capacity else None}


def current_rows(query, state):
    return PostingIndex(state,PAYLOADS).rows(query['plan'])


def event_for(shard, base, target):
    ids = sorted(set(base)|set(target))
    changes = [[ident,target.get(ident)] for ident in ids if base.get(ident)!=target.get(ident)]
    return {'kind':'events','shard':shard,'epoch':0,'lo':0,'hi':1,
            'events':[{'shard':shard,'seq':1,'changes':changes}]}


def assignments(ids):
    for values in itertools.product(VALUES,repeat=len(ids)):
        yield {ident:body for ident,body in zip(ids,values) if body is not None}


def check_local():
    ids=('a','b','c','d'); checked=0; repair_checked=0
    for base in assignments(ids):
        for target in assignments(ids):
            receipt=event_for(0,base,target)
            d=len(receipt['events'][0]['changes'])
            exact=current_rows(QUERY2,target)
            for capacity in (2,3):
                token=token_from_prefix(QUERY2,prefix(QUERY2,base,0,capacity),capacity)
                token=advance_token(token,receipt,PAYLOADS)
                held={row['id']:row for row in token['rows']}
                for row in token['rows']:
                    assert row in exact
                omitted=[row for row in exact if row['id'] not in held]
                if token['tail'] is None:
                    assert not omitted
                else:
                    assert all(key(row)>=tuple(token['tail']) for row in omitted)
                checked+=1
            capacity=min(64,QUERY2['k']+d)
            token=token_from_prefix(QUERY2,prefix(QUERY2,base,0,capacity),capacity)
            token=advance_token(token,receipt,PAYLOADS)
            assert token['rows'][:QUERY2['k']]==exact[:QUERY2['k']]
            repair_checked+=1
    return checked,repair_checked


def variants(shard):
    query={'id':'finite-q1','k':1,'origin_repo':'finite','seeds':['x','y'],
           'plan':[['x','y']]}
    ids=(f's{shard}a',f's{shard}b')
    answer={}
    for base in assignments(ids):
        for target in assignments(ids):
            token=token_from_prefix(query,prefix(query,base,shard,1),1)
            token=advance_token(token,event_for(shard,base,target),PAYLOADS)
            exact=current_rows(query,target)
            signature=json.dumps({'token':token,'exact':exact},sort_keys=True,separators=(',',':'))
            answer[signature]=(token,exact)
    return query,list(answer.values())


def check_merge():
    query,v0=variants(0); _,v1=variants(1); _,v2=variants(2)
    checked=complete=blocked=0
    for a,b,c in itertools.product(v0,v1,v2):
        tokens={0:a[0],1:b[0],2:c[0]}
        exact=sorted(a[1]+b[1]+c[1],key=key)[:1]
        result=merge_tokens(query,[1,1,1],0,tokens)
        if result['status']=='complete':
            assert result['rows']==exact
            complete+=1
        else:
            assert result['status']=='rank-underdetermined'
            assert result['rank_blockers']
            blocked+=1
        checked+=1
    return checked,complete,blocked,len(v0),len(v1),len(v2)


def main():
    local,repair=check_local()
    merge,complete,blocked,*variants_per_shard=check_merge()
    result={'local_transition_cases':local,
            'k_plus_d_repair_cases':repair,
            'global_merge_cases':merge,
            'global_complete_cases':complete,
            'global_blocked_cases':blocked,
            'variants_per_shard':variants_per_shard,
            'failures':0}
    out=ROOT/'results/continuation-finite.json'
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
    print(json.dumps(result,sort_keys=True))

if __name__=='__main__':
    main()
