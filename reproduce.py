#!/usr/bin/env python3
"""Run deterministic, bounded loopback campaigns from frozen local source only.

Each case is independently restartable. Do not expose the toy services to a LAN.
"""
from __future__ import annotations
import argparse
import asyncio
import copy
import gzip
import json
import os
import resource
import time
from pathlib import Path
from src.corpus import build
from src.checker import Checker
from src.service import Replica, Network, wire
from src.coordinator import certified, baseline
from src.workload import Writer, CASES

ROOT = Path(__file__).resolve().parent
POLICIES = ('certified','overlay','local-only','random-shard','stale-cache','unverified')


def difference(after, before):
    return {k: after[k]-before[k] for k in before}


def write_json(path, obj):
    """Atomically publish deterministic JSON, including under parallel campaign runs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    try:
        temporary.write_text(json.dumps(obj, indent=2, sort_keys=True)+'\n')
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


async def campaign(fixture, checker, case, seed, length=8, alternates=True, output=None):
    wall = time.monotonic(); cpu = time.process_time()
    writer = Writer(fixture, seed)
    owners = [[0,1],[2,3],[4,5]]; epoch=0
    replicas = [Replica(i,i//2,fixture['initial'][i//2],fixture['payloads']) for i in range(6)]
    received = [set() for _ in range(6)]
    cache = {}; records=[]; timeline=[]
    expected_at_end = None
    async with Network(replicas) as net:
        # Modelled schedule costs are separately named, never wall-clock latency.
        net.delay_ms={i:(i%3)*3 for i in range(6)}
        for tick, original in enumerate(writer.queries,1):
            if time.monotonic()-wall > 170:
                raise TimeoutError('case exceeded 170-second repair-safe deadline')
            if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss > int(3.1*1024*1024):
                raise MemoryError('measured RSS exceeded project limit')
            query = copy.deepcopy(original)
            if not alternates:
                query['plan']=query['plan'][:1]
            # Cache is populated at the initial cut, independently of later faults.
            if query['id'] not in cache:
                cache[query['id']]=checker.oracle(fixture['initial'],[[],[],[]],[0,0,0],query)
            writer.step(tick, query, case)
            cut=writer.cut
            if case=='partition':
                net.blocked={2,3} if 20<=tick<=35 else set()
            if case=='crash' and tick==20:
                for node in (0,4): await net.rpc(node,{'op':'crash'},administrative=True)
            if case=='crash' and tick==28:
                for node in (0,4): await net.rpc(node,{'op':'restart'},administrative=True)
            if case=='rebalance' and tick==32:
                epoch=1;owners=[[2,3],[4,5],[0,1]]
                for shard in range(3):
                    for node in owners[shard]:
                        _, rep=await net.rpc(node,{'op':'install','epoch':epoch,'shard':shard,
                            'at':cut[shard],'state':writer.views[shard]},administrative=True)
                        if not rep.get('ok'): raise AssertionError(rep)
                        received[node]=set(range(1,cut[shard]+1))
            # Sender retries retained events, including replies deliberately lost.
            control_before=net.counters(); first_attempt=len(net.attempts)
            for shard in range(3):
                for node in owners[shard]:
                    pending=[e for e in writer.history[shard] if e['seq'] not in received[node]]
                    for e in reversed(pending):  # backlog delivery explicitly reordered
                        if case=='log-gap' and shard==1 and e['seq']%11==0 and tick<e['seq']+4:
                            continue
                        lost=(tick%17==0 and e['seq']==cut[shard] and node%2==0)
                        _, rep=await net.rpc(node,{'op':'receive','event':e},lost_reply=lost)
                        if rep and rep.get('ok'): received[node].add(e['seq'])
                    lag=0 if case=='fresh' else 8 if case in {'index-lag','hot-shard'} else 3
                    _, rep=await net.rpc(node,{'op':'advance','to':max(0,cut[shard]-lag)})
                    if case=='compaction' and tick%16==0:
                        await net.rpc(node,{'op':'compact','to':max(0,cut[shard]-lag)})
            if tick%7==0 and writer.history[0]:
                await net.rpc(owners[0][0],{'op':'receive','event':writer.history[0][-1]})
            timeline.append({'tick':tick,'cut':cut,'epoch':epoch,'owners':copy.deepcopy(owners),
                'blocked':sorted(net.blocked),'first_attempt':first_attempt,
                'after_control_attempt':len(net.attempts),
                'control':difference(net.counters(),control_before)})
            oracle_begin=time.monotonic_ns()
            exact=checker.oracle(fixture['initial'],writer.history,cut,query)
            oracle_ns=time.monotonic_ns()-oracle_begin
            old=cache[query['id']]
            old_ids={r['id'] for r in old}; exact_ids={r['id'] for r in exact}
            stability=1.0 if not exact_ids else len(old_ids & exact_ids)/len(exact_ids)
            # Rotate execution order; all policies see the identical frozen state.
            order=POLICIES[tick%len(POLICIES):]+POLICIES[:tick%len(POLICIES)]
            observations=[]
            for policy in order:
                before=net.counters(); ts=time.monotonic_ns(); checker_ns=0
                if policy=='certified':
                    response=await certified(net,query,cut,epoch,owners,length)
                    check_ts=time.monotonic_ns()
                    checker.check(response,net.evidence,query,cut,epoch,owners)
                    checker_ns=time.monotonic_ns()-check_ts
                    claimed=response['status']=='complete'
                elif policy=='stale-cache':
                    response={'rows':copy.deepcopy(old),'claim_complete':False,'coverage':[],
                              'cache_target':[0,0,0]}
                    claimed=False
                else:
                    response=await baseline(net,query,cut,epoch,owners,policy)
                    claimed=response['claim_complete']
                    if policy=='overlay':
                        check_ts=time.monotonic_ns()
                        checker.check_overlay(response,net.evidence,query,cut,epoch,owners)
                        checker_ns=time.monotonic_ns()-check_ts
                elapsed=time.monotonic_ns()-ts
                cost=difference(net.counters(),before)
                unsound=checker.sound_rows(response['rows'],fixture['initial'],writer.history,cut,query)
                actual=response['rows']==exact
                ids={r['id'] for r in response['rows']}
                # Recall distinguishes document IDs from correct current bodies.
                valid={(r['id'],r['body']) for r in response['rows']}
                target={(r['id'],r['body']) for r in exact}
                recall=1.0 if not target else len(valid & target)/len(target)
                cost.update(policy=policy,wall_ns=elapsed,checker_ns=checker_ns,
                    unsound_rows=unsound,returned_rows=len(response['rows']),exact=actual,
                    claimed_complete=claimed,false_complete=claimed and not actual,
                    target_body_recall=recall,
                    id_recall=1.0 if not exact_ids else len(ids & exact_ids)/len(exact_ids),
                    certificate_bytes=len(wire(response)) if policy=='certified' else 0)
                if policy in {'certified','overlay'}:
                    if unsound or (claimed and not actual):
                        raise AssertionError((case,seed,tick,policy,cost))
                observations.append({'metrics':cost,'response':response})
            records.append({'tick':tick,'query':query,'cut':cut,'epoch':epoch,
                'owners':copy.deepcopy(owners),'oracle':exact,'oracle_wall_ns':oracle_ns,
                'rank_stability_from_initial':stability,'observations':observations})
        # Complete retained-log repair is measured, then checked at quiescence.
        net.blocked=set(); before=net.counters(); repair_start=time.monotonic_ns()
        for shard in range(3):
            for node in owners[shard]:
                if replicas[node].crashed:
                    await net.rpc(node,{'op':'restart'},administrative=True)
                for e in writer.history[shard]:
                    if e['seq']>replicas[node].floor:
                        await net.rpc(node,{'op':'receive','event':e})
                await net.rpc(node,{'op':'advance','to':writer.cut[shard]})
                await net.rpc(node,{'op':'compact','to':writer.cut[shard]})
                if replicas[node].index != writer.views[shard]:
                    raise AssertionError('replica failed final convergence')
        repair=difference(net.counters(),before)
        repair.update(wall_ns=time.monotonic_ns()-repair_start,converged_replicas=6,
            cut=writer.cut,retained_events_after=[len(r.journal) for r in replicas],
            pending_events_after=[len(r.pending) for r in replicas])
        trace={'case':case,'seed':seed,'prefix_length':length,'alternates':alternates,
            'initial':fixture['initial'],'history':writer.history,'records':records,'timeline':timeline,
            'transcript':net.evidence,'attempts':net.attempts,'repair':repair,
            'resources':{'wall_seconds':time.monotonic()-wall,'cpu_seconds':time.process_time()-cpu,
                'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'workers':1,
                'logical_tcp_nodes':6,'peak_journal_events':max(r.peak_journal for r in replicas),
                'peak_pending_events':max(r.peak_pending for r in replicas)},
            'scope':'one process, six loopback TCP endpoints; logical faults, not independent hosts',
            'retrieval_payload':'document/body references and scores; source-body fetch excluded'}
    if output is not None:
        output.parent.mkdir(parents=True,exist_ok=True)
        temporary=output.with_name(output.name+'.tmp')
        try:
            with gzip.open(temporary,'wt',encoding='utf-8') as f:
                json.dump(trace,f,sort_keys=True,separators=(',',':'));f.write('\n')
            os.replace(temporary,output)
        finally:
            if temporary.exists(): temporary.unlink()
    report={'case':case,'seed':seed,'queries':len(records),'repair':repair,'resources':trace['resources']}
    print(json.dumps(report,sort_keys=True),flush=True)
    return trace


async def main(args):
    fixture=build(ROOT); checker=Checker(fixture['payloads'])
    write_json(ROOT/'results/extraction.json',fixture['extraction'])
    cases=CASES if args.all else (args.case,)
    seeds=(1,2,3) if args.all else (args.seed,)
    for case in cases:
        for seed in seeds:
            stem=f'{case}-seed{seed}'
            if args.length!=8: stem+=f'-prefix{args.length}'
            if args.no_alternates: stem+='-base-plan'
            out=ROOT/'results/traces'/(stem+'.json.gz')
            if args.resume and out.exists():
                with gzip.open(out,'rt',encoding='utf-8') as f: existing=json.load(f)
                if not (existing['case']==case and existing['seed']==seed
                        and existing['prefix_length']==args.length
                        and existing['alternates']==(not args.no_alternates)
                        and len(existing['records'])==63
                        and existing['repair']['converged_replicas']==6):
                    raise ValueError('existing trace is not a completed matching case; rerun without --resume')
                del existing
                print(json.dumps({'skipped_existing':str(out.relative_to(ROOT))}),flush=True)
                continue
            await campaign(fixture,checker,case,seed,args.length,not args.no_alternates,out)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',choices=CASES,default='index-lag')
    parser.add_argument('--seed',type=int,choices=(1,2,3),default=1)
    parser.add_argument('--length',type=int,choices=(1,5,8,16,32,64),default=8)
    parser.add_argument('--no-alternates',action='store_true')
    parser.add_argument('--all',action='store_true')
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args()
    asyncio.run(main(args))
