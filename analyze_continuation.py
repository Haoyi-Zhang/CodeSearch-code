#!/usr/bin/env python3
"""Independent replay and aggregation for standing-query continuation traces."""
from __future__ import annotations
import csv
import gzip
import json
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from src.corpus import build
from src.checker import Checker
from src.continuation_checker import ContinuationChecker
from src.model import apply, key

ROOT=Path(__file__).resolve().parent
CASES=('fresh','index-lag','log-gap','partition','crash','rebalance','hot-shard','compaction','rank-churn')
SEEDS=(2,3)
POLICIES=('continuation','overlay','cut-cache','mirror','unsafe-stale')


def load(path: Path):
    with gzip.open(path,'rt',encoding='utf-8') as stream:
        return json.load(stream)


def path_for(case,seed,active=4,capacity=5):
    stem=f'continuation-{case}-seed{seed}'
    if active!=4: stem+=f'-active{active}'
    if capacity!=8: stem+=f'-capacity{capacity}'
    return ROOT/'results/continuation-traces'/(stem+'.json.gz')


def states_by_cut(initial,history):
    answer=[]
    for shard in range(3):
        state=dict(initial[shard]); versions=[dict(state)]
        for event in history[shard]:
            apply(state,event)
            versions.append(dict(state))
        answer.append(versions)
    return answer


def expected_rows(cache,score_cache,states,checker,shard,at,plan):
    """Independently rank a materialized state without producer index code.

    Body scores are invariant for the frozen plan, so cache them across cuts;
    state scans then touch only identifier/body pairs rather than rebuilding
    postings for every receipt.
    """
    plan_key=tuple(tuple(part) for part in plan)
    ident=(shard,at,plan_key)
    if ident not in cache:
        values=score_cache.setdefault(plan_key,{})
        rows=[]
        for name,body in states[shard][at].items():
            if body not in values:
                values[body]=checker.value(body,plan)
            value=values[body]
            if value:
                rows.append({'id':name,'body':body,'score':value})
        cache[ident]=checker.ordered(rows)
    return cache[ident]


def verify_source_receipts(trace,payloads):
    states=states_by_cut(trace['initial'],trace['history']); cache={}; score_cache={}; checked=Counter(); checker=Checker(payloads)
    evidence={int(k):v for k,v in trace['transcript'].items()}
    for ident,entry in sorted(evidence.items()):
        req=entry['request']; rep=entry['reply']; op=req.get('op')
        if not isinstance(rep,dict): continue
        if rep.get('kind')=='prefix':
            shard=rep['shard']; at=rep['at']; rows=expected_rows(cache,score_cache,states,checker,shard,at,req['query']['plan'])
            length=req['length']
            assert rep['rows']==rows[:length]
            assert rep['boundary']==(list(key(rows[length])) if len(rows)>length else None)
            assert rep['plan']==req['query']['plan'] and rep['epoch']==req['epoch']
            checked['prefix']+=1
        elif rep.get('kind')=='overlay':
            shard=rep['shard']; at=rep['at']; rows=expected_rows(cache,score_cache,states,checker,shard,at,req['query']['plan'])
            assert rep['rows']==rows[:req['query']['k']]
            assert rep['at']==req['to'] and rep['epoch']==req['epoch']
            checked['overlay']+=1
        elif rep.get('kind')=='events':
            shard=rep['shard']; lo=rep['lo']; hi=rep['hi']
            assert rep['events']==trace['history'][shard][lo:hi]
            assert rep['epoch']==req['epoch'] and lo==req['lo'] and hi==req['hi']
            checked['events']+=1; checked['event_rows']+=len(rep['events'])
        elif rep.get('kind')=='snapshot':
            shard=rep['shard']; at=rep['at']
            assert rep['state']==[list(item) for item in sorted(states[shard][at].items())]
            assert at==req['at'] and rep['epoch']==req['epoch']
            checked['snapshot']+=1
        elif rep.get('kind')=='delta':
            # Continuation traces do not issue query-specific deltas.
            raise AssertionError('unexpected delta in continuation trace')
    return checked,evidence,states,cache


def install_acquisition(cc,setup,queries,evidence,owners,epoch):
    assert setup['kind']=='continuation-acquisition'
    for query_id,specs in setup['receipts'].items():
        query=queries[query_id]
        assert set(specs)=={'0','1','2'}
        for label,spec in specs.items():
            cc.install_prefix(query,evidence[spec['receipt']],owners,epoch,spec['capacity'])


def verify_trace(trace,fixture):
    source,evidence,states,rank_cache=verify_source_receipts(trace,fixture['payloads'])
    checker=Checker(fixture['payloads']); score_cache={}; cc=ContinuationChecker(fixture['payloads'])
    queries={query['id']:query for query in trace['standing_queries']}
    setup_cont=[item for item in trace['setup'] if item['kind']=='continuation-acquisition']
    install_acquisition(cc,setup_cont[0],queries,evidence,[[0,1],[2,3],[4,5]],0)
    by_tick=defaultdict(list)
    for record in trace['records']: by_tick[record['tick']].append(record)
    rebalance_index=1
    checked=Counter()
    for moment in trace['timeline']:
        tick=moment['tick']; owners=moment['owners']; epoch=moment['epoch']; cut=moment['cut']
        if moment['reconfigured']:
            cc.reset_epoch(epoch,cut)
            install_acquisition(cc,setup_cont[rebalance_index],queries,evidence,owners,epoch)
            rebalance_index+=1
        for label,ident in sorted(moment['feed_receipts'].items(),key=lambda item:int(item[0])):
            cc.accept_events(evidence[ident],owners,epoch); checked['feed_receipts']+=1
        for record in by_tick[tick]:
            query=record['query']; assert query==queries[query['id']]
            # Recompute the exact oracle directly from retained writer state.
            all_rows=[]
            for shard in range(3):
                all_rows.extend(expected_rows(rank_cache,score_cache,states,checker,shard,cut[shard],query['plan'])[:query['k']])
            oracle=sorted(all_rows,key=key)[:query['k']]
            assert record['oracle']==oracle
            cont=record['observations']['continuation']['response']
            cc.check_result(cont,query,cut,epoch,owners,evidence,cont.get('repairs',{}))
            checked['continuation_results']+=1
            for policy in ('overlay','cut-cache'):
                response=record['observations'][policy]['response']
                checker.check_overlay(response,evidence,query,cut,epoch,owners)
                checked[policy+'_results']+=1
            for policy in POLICIES:
                obs=record['observations'][policy]; metrics=obs['metrics']; rows=obs['response']['rows']
                claimed=(obs['response'].get('status')=='complete' if policy=='continuation'
                         else obs['response'].get('claim_complete',False))
                exact=rows==oracle
                unsound=checker.sound_rows(rows,trace['initial'],trace['history'],cut,query)
                assert metrics['exact']==exact and metrics['claimed_complete']==claimed
                assert metrics['false_complete']==(claimed and not exact)
                assert metrics['unsound_rows']==unsound
                if policy!='unsafe-stale':
                    assert not metrics['false_complete'] and unsound==0
            checked['query_observations']+=1
    assert rebalance_index==len(setup_cont)
    return source,checked


def aggregate(traces):
    counts={policy:Counter() for policy in POLICIES}; costs={policy:Counter() for policy in POLICIES}
    cases=[]; repairs=[]; setup_bytes=Counter(); feed_bytes=0
    state_peaks={policy:[] for policy in ('continuation','cut-cache','mirror')}
    source_total=Counter(); replay_total=Counter()
    for trace,source,replay in traces:
        source_total.update(source); replay_total.update(replay)
        n=len(trace['records']); case=Counter(q=n)
        for policy in POLICIES:
            for record in trace['records']:
                m=record['observations'][policy]['metrics']
                for name in ('exact','claimed_complete','false_complete','unsound_rows','returned_rows'):
                    counts[policy][name]+=m[name]; case[f'{policy}_{name}']+=m[name]
            costs[policy].update(trace['policy_totals'][policy])
            case[f'{policy}_bytes']=trace['policy_totals'][policy]['bytes']
            case[f'{policy}_messages']=trace['policy_totals'][policy]['messages']
        for item in trace['setup']:
            setup_bytes[item['kind']]+=item['cost']['bytes']
        feed_bytes+=sum(item['feed_cost']['bytes'] for item in trace['timeline'])
        for record in trace['records']:
            r=record['observations']['continuation']['response'].get('repairs',{})
            if r: repairs.append((trace['case'],trace['seed'],record['tick'],record['query']['id'],r))
        row={'case':trace['case'],'seed':trace['seed'],'queries':n,
             'repair_queries':sum(bool(record['observations']['continuation']['response'].get('repairs')) for record in trace['records']),
             'repair_shards':sum(len(record['observations']['continuation']['response'].get('repairs',{})) for record in trace['records'])}
        for policy in POLICIES:
            row[policy+'_exact']=case[f'{policy}_exact']; row[policy+'_complete']=case[f'{policy}_claimed_complete']
            row[policy+'_false_complete']=case[f'{policy}_false_complete']; row[policy+'_unsound_rows']=case[f'{policy}_unsound_rows']
            row[policy+'_mean_bytes']=case[f'{policy}_bytes']/n
            row[policy+'_mean_messages']=case[f'{policy}_messages']/n
        for policy in state_peaks:
            value=trace['peak_logical_state_bytes'][policy]; row[policy+'_state_bytes']=value; state_peaks[policy].append(value)
        cases.append(row)
    n=sum(len(trace['records']) for trace,_,_ in traces)
    policies={}
    for policy in POLICIES:
        policies[policy]={
            'queries':n,**dict(counts[policy]),
            'partial_or_unjustified':n-counts[policy]['claimed_complete'],
            'mean_source_bytes':costs[policy]['bytes']/n,
            'mean_source_messages':costs[policy]['messages']/n,
            'mean_server_cpu_us':costs[policy]['server_cpu_ns']/n/1000,
            'mean_policy_wall_us':costs[policy]['wall_ns']/n/1000,
            'mean_checker_us':costs[policy]['checker_ns']/n/1000,
            'total_source_bytes':costs[policy]['bytes'],
            'total_source_messages':costs[policy]['messages'],
        }
        if policy in state_peaks:
            policies[policy]['max_logical_state_bytes']=max(state_peaks[policy])
            policies[policy]['median_logical_state_bytes']=statistics.median(state_peaks[policy])
    summary={
        'primary_traces':len(traces),'primary_queries':n,'cases':list(CASES),'seeds':list(SEEDS),
        'standing_queries_per_trace':16,'active_queries_per_tick':4,'token_capacity':5,
        'policies':policies,
        'continuation_repairs':{'queries':len(repairs),'shards':sum(len(item[4]) for item in repairs),
                                'records':[{'case':c,'seed':s,'tick':t,'query':q,'shards':sorted(map(int,r))}
                                           for c,s,t,q,r in repairs]},
        'source_replay':dict(source_total),'checker_replay':dict(replay_total),
        'setup_source_bytes':dict(setup_bytes),'shared_feed_source_bytes':feed_bytes,
        'communication_reduction_vs_overlay_percent':
            100*(1-policies['continuation']['mean_source_bytes']/policies['overlay']['mean_source_bytes']),
        'communication_reduction_vs_cut_cache_percent':
            100*(1-policies['continuation']['mean_source_bytes']/policies['cut-cache']['mean_source_bytes']),
        'continuation_state_fraction_of_mirror':
            policies['continuation']['max_logical_state_bytes']/policies['mirror']['max_logical_state_bytes'],
    }
    return summary,cases


def sensitivity_rows():
    rows=[]
    specs=[]
    for case in ('fresh','rank-churn'):
        for active in (1,4,16): specs.append(('active',case,active,5))
    for capacity in (5,8,16): specs.append(('capacity','rank-churn',4,capacity))
    seen=set()
    for kind,case,active,capacity in specs:
        path=path_for(case,1,active,capacity)
        if path in seen: continue
        seen.add(path); trace=load(path); n=len(trace['records'])
        row={'analysis':kind,'case':case,'active_per_tick':active,'capacity':capacity,'queries':n,
             'repair_queries':sum(bool(r['observations']['continuation']['response'].get('repairs')) for r in trace['records']),
             'repair_shards':sum(len(r['observations']['continuation']['response'].get('repairs',{})) for r in trace['records'])}
        for policy in ('continuation','overlay','cut-cache','mirror'):
            row[policy+'_mean_bytes']=trace['policy_totals'][policy]['bytes']/n
            row[policy+'_state_bytes']=trace['peak_logical_state_bytes'].get(policy,0)
        rows.append(row)
    return rows


def write_csv(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    fields=[]
    for row in rows:
        for name in row:
            if name not in fields: fields.append(name)
    with path.open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(rows)


def write_latex(summary):
    p=summary['policies']; path=ROOT.parent/'paper/tables/generated-continuation-main.tex'
    labels={'continuation':'Continuation','overlay':'Exact overlay','cut-cache':'Cut-aware cache','mirror':'Full mirror','unsafe-stale':'Unsafe stale'}
    lines=['\\begin{tabular}{lrrrrr}','\\toprule','Policy & Complete & False & Bytes/query & Msg/query & State (KiB) \\\\','\\midrule']
    for policy in POLICIES:
        state=p[policy].get('max_logical_state_bytes',0)/1024
        lines.append(f"{labels[policy]} & {p[policy]['claimed_complete']:,} & {p[policy]['false_complete']:,} & {p[policy]['mean_source_bytes']:.1f} & {p[policy]['mean_source_messages']:.2f} & {state:.1f} \\\\")
    lines+=['\\bottomrule','\\end{tabular}']
    path.write_text('\n'.join(lines)+'\n')



def tex_number(value):
    if isinstance(value,int):
        return f'{value:,}'
    return f'{value:.1f}'


def write_paper_outputs(summary,cases,sensitivity):
    paper=ROOT.parent/'paper'; tables=paper/'tables'; figures=paper/'figures'
    tables.mkdir(parents=True,exist_ok=True); figures.mkdir(parents=True,exist_ok=True)
    policies=summary['policies']; cont=policies['continuation']; overlay=policies['overlay']
    cache=policies['cut-cache']; mirror=policies['mirror']; unsafe=policies['unsafe-stale']
    repair_rate=100*summary['continuation_repairs']['queries']/summary['primary_queries']
    exact_unjustified=cont['exact']-cont['claimed_complete']
    source_receipts=sum(summary['source_replay'][name] for name in ('events','overlay','prefix','snapshot'))
    finite=summary['finite_checks']; finite_total=(finite['local_transition_cases']+
        finite['k_plus_d_repair_cases']+finite['global_merge_cases'])
    macros={
        'PrimaryQueries':summary['primary_queries'],'PrimaryTraces':summary['primary_traces'],
        'PrimaryComplete':cont['claimed_complete'],'PrimaryExact':cont['exact'],
        'PrimaryPartial':cont['partial_or_unjustified'],'PrimaryExactUnjustified':exact_unjustified,
        'ContinuationBytes':cont['mean_source_bytes'],'OverlayBytes':overlay['mean_source_bytes'],
        'CutCacheBytes':cache['mean_source_bytes'],'MirrorBytes':mirror['mean_source_bytes'],
        'ContinuationMessages':cont['mean_source_messages'],'OverlayMessages':overlay['mean_source_messages'],
        'ContinuationReduction':summary['communication_reduction_vs_overlay_percent'],
        'ContinuationVsOverlay':overlay['mean_source_bytes']/cont['mean_source_bytes'],
        'ContinuationVsCache':cache['mean_source_bytes']/cont['mean_source_bytes'],
        'ContinuationVsMirror':mirror['mean_source_bytes']/cont['mean_source_bytes'],
        'ContinuationStateKiB':cont['max_logical_state_bytes']/1024,
        'CutCacheStateKiB':cache['max_logical_state_bytes']/1024,
        'MirrorStateKiB':mirror['max_logical_state_bytes']/1024,
        'MirrorStateRatio':mirror['max_logical_state_bytes']/cont['max_logical_state_bytes'],
        'RepairQueries':summary['continuation_repairs']['queries'],
        'RepairShards':summary['continuation_repairs']['shards'],'RepairRate':repair_rate,
        'UnsafeFalse':unsafe['false_complete'],'UnsafeUnsound':unsafe['unsound_rows'],
        'SourceReceipts':source_receipts,'SourceEventRows':summary['source_replay']['event_rows'],
        'FiniteTotal':finite_total,'FiniteLocal':finite['local_transition_cases'],
        'FiniteRepair':finite['k_plus_d_repair_cases'],'FiniteGlobal':finite['global_merge_cases'],
    }
    lines=['% Generated by artifact/analyze_continuation.py; do not edit.']
    for name,value in macros.items():
        if isinstance(value,int): text=f'{value:,}'
        elif name.endswith('Rate') or name.endswith('Reduction'): text=f'{value:.1f}'
        elif name.endswith('Messages'): text=f'{value:.2f}'
        else: text=f'{value:.1f}'
        lines.append(f'\\newcommand{{\\{name}}}{{{text}}}')
    (tables/'generated-continuation-macros.tex').write_text('\n'.join(lines)+'\n')

    # Aggregate the two primary seeds per case using exact totals.
    by_case={}
    for row in cases:
        target=by_case.setdefault(row['case'],{'queries':0,'repair_queries':0,'repair_shards':0})
        target['queries']+=row['queries'];target['repair_queries']+=row['repair_queries'];target['repair_shards']+=row['repair_shards']
        for policy in ('continuation','overlay','cut-cache','mirror'):
            for metric in ('exact','complete','false_complete','unsound_rows'):
                target[policy+'_'+metric]=target.get(policy+'_'+metric,0)+row[policy+'_'+metric]
            target[policy+'_bytes_total']=target.get(policy+'_bytes_total',0)+row[policy+'_mean_bytes']*row['queries']
    labels={'fresh':'Fresh','index-lag':'Index lag','log-gap':'Log gap','partition':'Partition',
            'crash':'Crash/restart','rebalance':'Rebalance','hot-shard':'Hot shard',
            'compaction':'Compaction','rank-churn':'Rank churn'}
    lines=['\\begin{tabular}{lrrrrr}','\\toprule',
           'Case & Complete & Exact & Repair & Cont. B/q & Overlay B/q \\\\','\\midrule']
    data=[]
    for case in CASES:
        row=by_case[case]; n=row['queries']
        lines.append(f"{labels[case]} & {row['continuation_complete']:,} & {row['continuation_exact']:,} & "
                     f"{row['repair_queries']:,} & {row['continuation_bytes_total']/n:.1f} & {row['overlay_bytes_total']/n:.1f} \\\\")
        data.append({'case':case,'complete':row['continuation_complete'],'exact':row['continuation_exact'],
                     'repairs':row['repair_queries'],'continuation_bytes':row['continuation_bytes_total']/n,
                     'overlay_bytes':row['overlay_bytes_total']/n})
    lines+=['\\bottomrule','\\end{tabular}']
    (tables/'generated-continuation-cases.tex').write_text('\n'.join(lines)+'\n')
    write_csv(ROOT/'results/continuation-case-aggregate.csv',data)

    # Active-query amortization and capacity stress are development-only sensitivity rows.
    active=[r for r in sensitivity if r['analysis']=='active']
    lines=['\\begin{tabular}{llrrrr}','\\toprule',
           'Case & Active/tick & Cont. B/q & Overlay B/q & Mirror B/q & State KiB \\\\','\\midrule']
    for row in active:
        lines.append(f"{labels[row['case']]} & {row['active_per_tick']} & {float(row['continuation_mean_bytes']):.1f} & "
                     f"{float(row['overlay_mean_bytes']):.1f} & {float(row['mirror_mean_bytes']):.1f} & "
                     f"{float(row['continuation_state_bytes'])/1024:.1f} \\\\")
    lines+=['\\bottomrule','\\end{tabular}']
    (tables/'generated-continuation-active.tex').write_text('\n'.join(lines)+'\n')

    capacity=[r for r in sensitivity if r['case']=='rank-churn' and r['active_per_tick']==4 and r['capacity'] in (5,8,16)]
    lines=['\\begin{tabular}{rrrr}','\\toprule','Capacity & Repair queries & Bytes/query & State KiB \\\\','\\midrule']
    for row in sorted(capacity,key=lambda x:x['capacity']):
        lines.append(f"{row['capacity']} & {row['repair_queries']} & {float(row['continuation_mean_bytes']):.1f} & "
                     f"{float(row['continuation_state_bytes'])/1024:.1f} \\\\")
    lines+=['\\bottomrule','\\end{tabular}']
    (tables/'generated-continuation-capacity.tex').write_text('\n'.join(lines)+'\n')

    md=['# Continuation-certificate result summary','',
        f"The primary campaign contains {summary['primary_traces']} traces and {summary['primary_queries']:,} standing-query observations.",
        f"Continuation, exact overlay, cut-aware cache, and full mirror each returned {cont['exact']:,} exact answers, justified {cont['claimed_complete']:,} complete answers, and produced zero false-completeness claims and zero target-unsound rows.",
        f"Continuation used {cont['mean_source_bytes']:.1f} source-RPC bytes/query versus {overlay['mean_source_bytes']:.1f} for exact overlay ({summary['communication_reduction_vs_overlay_percent']:.1f}% lower).",
        f"It repaired {summary['continuation_repairs']['shards']} shard tokens across {summary['continuation_repairs']['queries']} queries ({repair_rate:.2f}% of queries).",
        f"Maximum serialized logical state was {cont['max_logical_state_bytes']/1024:.1f} KiB for continuation and {mirror['max_logical_state_bytes']/1024:.1f} KiB for the full mirror.",
        f"The unsafe stale control made {unsafe['false_complete']:,} false-completeness claims and returned {unsafe['unsound_rows']:,} unsound rows.",
        '', 'These are deterministic generated-fixture and loopback results. They do not establish production latency, semantic code relevance, Byzantine source truth, or external venue novelty.']
    (ROOT/'results/continuation-summary.md').write_text('\n'.join(md)+'\n')

def main():
    fixture=build(ROOT); traces=[]
    for case in CASES:
        for seed in SEEDS:
            path=path_for(case,seed,4,5)
            if not path.exists(): raise FileNotFoundError(path)
            trace=load(path); source,replay=verify_trace(trace,fixture)
            traces.append((trace,source,replay))
    summary,cases=aggregate(traces); sensitivity=sensitivity_rows()
    finite=json.loads((ROOT/'results/continuation-finite.json').read_text())
    summary['finite_checks']=finite
    out=ROOT/'results/continuation-summary.json';out.write_text(json.dumps(summary,sort_keys=True,indent=2)+'\n')
    write_csv(ROOT/'results/continuation-cases.csv',cases)
    write_csv(ROOT/'results/continuation-sensitivity.csv',sensitivity)
    main_rows=[]
    for policy,data in summary['policies'].items():
        main_rows.append({'policy':policy,**data})
    write_csv(ROOT/'results/continuation-main.csv',main_rows)
    # A standalone artifact has no sibling paper directory. Reproduction must
    # still succeed there; manuscript tables are an optional project-level side effect.
    if (ROOT.parent/'paper').is_dir():
        write_latex(summary)
        write_paper_outputs(summary,cases,sensitivity)
    print(json.dumps({'summary':str(out.relative_to(ROOT)),'traces':len(traces),
                      'queries':summary['primary_queries'],
                      'false_complete':{p:d['false_complete'] for p,d in summary['policies'].items()},
                      'source_receipts':summary['source_replay'],
                      'finite':finite},sort_keys=True))

if __name__=='__main__':
    main()
