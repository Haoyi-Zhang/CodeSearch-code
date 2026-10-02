#!/usr/bin/env python3
"""Discriminating, bounded one-worker pilot; no external services or dependencies."""
import asyncio
import json
import resource
import time
from pathlib import Path
from src.corpus import build
from src.checker import Checker
from src.model import features
from src.service import Replica, Network
from src.coordinator import certified, baseline

ROOT = Path(__file__).resolve().parent

async def run():
    started = time.monotonic(); cpu = time.process_time()
    fixture = build(ROOT)
    checker = Checker(fixture['payloads'])
    assert all(set(p['features']) == checker.labels[b] for b,p in fixture['payloads'].items())
    replicas = [Replica(i, i//2, fixture['initial'][i//2], fixture['payloads']) for i in range(6)]
    owners = [[0,1],[2,3],[4,5]]
    rows=[]
    async with Network(replicas) as net:
        query = fixture['queries'][0]
        target = checker.oracle(fixture['initial'], [[],[],[]], [0,0,0], query)
        assert target, 'pilot requires a positive public-source match'
        ident = target[0]['id']
        shard = next(s for s,x in enumerate(fixture['initial']) if ident in x)
        event={'shard':shard,'seq':1,'changes':[[ident,None]]}
        history=[[],[],[]];history[shard]=[event]
        cut=[len(e) for e in history]
        for node in owners[shard]:
            _, reply = await net.rpc(node, {'op':'receive','event':event})
            assert reply.get('ok')
        exact=checker.oracle(fixture['initial'],history,cut,query)
        for policy in ('unverified','overlay','certified'):
            before=net.counters(); ts=time.monotonic_ns()
            if policy=='certified':
                response=await certified(net,query,cut,0,owners,8)
                checker.check(response,net.evidence,query,cut,0,owners)
                claimed=response['status']=='complete'
            else:
                response=await baseline(net,query,cut,0,owners,policy)
                claimed=response['claim_complete']
            delta={k:net.counters()[k]-v for k,v in before.items()}
            delta.update(policy=policy,wall_ms=(time.monotonic_ns()-ts)/1e6,
                         exact=response['rows']==exact,claimed_complete=claimed,
                         unsound_rows=checker.sound_rows(response['rows'],fixture['initial'],history,cut,query),
                         status=response.get('status'),result_count=len(response['rows']))
            rows.append(delta)
        assert rows[0]['unsound_rows']>0 and rows[0]['claimed_complete']
        assert rows[1]['exact'] and rows[1]['unsound_rows']==0
        assert rows[2]['unsound_rows']==0
        assert not rows[2]['claimed_complete'] or rows[2]['exact']
    report={'purpose':'pre-lock falsification pilot, public source plus directed deletion',
            'extraction':fixture['extraction'],'policies':rows,
            'workers':1,'logical_tcp_nodes':6,'cpu_seconds':time.process_time()-cpu,
            'wall_seconds':time.monotonic()-started,
            'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'negative_control_detected':True,'source_feature_agreement':len(checker.labels),
            'novelty_established':False,
            'interpretation':'A stale all-shard response is unsound. Both suffix repair and ordinary overlay fix the deletion. This pilot does not establish a new protocol or advantage.'}
    (ROOT/'results').mkdir(exist_ok=True)
    (ROOT/'results/pilot.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='extraction'},indent=2))
    print(json.dumps({k:v for k,v in fixture['extraction'].items() if k!='excluded'},indent=2))

if __name__=='__main__':
    asyncio.run(run())
