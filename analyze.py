#!/usr/bin/env python3
"""Independently replay retained source receipts and derive all reported aggregates.

This program imports the extraction wrapper and independent checker, never the
planner or service. It executes no retained upstream source bodies. A successful
replay is evidence about these finite traces, not authentication of new issuers.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import json
import math
import resource
import time
from collections import defaultdict, Counter
from pathlib import Path
from src.corpus import build
from src.checker import Checker

ROOT = Path(__file__).resolve().parent


def write_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, sort_keys=True, indent=2) + '\n')


def quantile(values: list[float], p: float) -> float:
    """Nearest-rank descriptive quantile, with no population interpretation."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def receipt_truth(trace: dict, checker: Checker) -> int:
    # Each state is reconstructed from the original map and ordered assignments.
    snapshots, rankings = {}, {}
    def snapshot(shard, at):
        key = (shard, at)
        if key not in snapshots:
            if not 0 <= at <= len(trace['history'][shard]):
                raise AssertionError('receipt cut outside retained history')
            view = dict(trace['initial'][shard])
            for seq, e in enumerate(trace['history'][shard][:at], 1):
                assert e['seq'] == seq and e['shard'] == shard
                for ident, body in e['changes']:
                    if body is None: view.pop(ident, None)
                    else: view[ident] = body
            snapshots[key] = view
        return snapshots[key]
    def rows(view, plan):
        result = []
        for ident, body in view.items():
            v = checker.value(body, plan)
            if v: result.append({'id':ident, 'body':body, 'score':v})
        return checker.ordered(result)
    checked = 0
    for entry in trace['transcript'].values():
        rep, req = entry['reply'], entry['request']
        kind = rep.get('kind')
        if kind not in {'prefix', 'delta', 'overlay'}: continue
        shard, plan = rep['shard'], rep['plan']
        if kind == 'delta':
            assert 0 <= rep['lo'] <= rep['hi'] <= len(trace['history'][shard])
            last = {}
            for e in trace['history'][shard][rep['lo']:rep['hi']]:
                for ident, body in e['changes']: last[ident] = body
            assert rep['changed'] == sorted(last)
            expected = rows({i:b for i,b in last.items() if b is not None}, plan)[:req['query']['k']]
        else:
            cache_key = (shard, rep['at'], tuple(map(tuple, plan)))
            if cache_key not in rankings:
                rankings[cache_key] = rows(snapshot(shard,rep['at']),plan)
            full = rankings[cache_key]
            if kind == 'prefix':
                length = req['length']; expected = full[:length]
                boundary = [-full[length]['score'], full[length]['id']] if len(full)>length else None
                assert rep['boundary'] == boundary
            else: expected = full[:req['query']['k']]
        assert rep['rows'] == expected, ('issuer truth failure', kind, shard)
        checked += 1
    return checked


def inspect(path: Path, fixture: dict, checker: Checker) -> tuple[list[dict],dict]:
    with gzip.open(path, 'rt', encoding='utf-8') as f: t = json.load(f)
    assert t['initial'] == fixture['initial'] and len(t['records']) == 63
    source_count = receipt_truth(t, checker)
    trusted = {int(k):v for k,v in t['transcript'].items()}
    frozen_queries = {q['id']:q for q in fixture['queries']}
    flat = []
    for r in t['records']:
        q = r['query']; frozen = dict(frozen_queries[q['id']])
        if not t['alternates']: frozen['plan'] = frozen['plan'][:1]
        assert q == frozen and q['id'] != 'q000'
        exact = checker.oracle(t['initial'], t['history'], r['cut'], q)
        assert r['oracle'] == exact
        bodies = {(x['id'], x['body']) for x in exact}; ids = {x['id'] for x in exact}
        for o in r['observations']:
            m, response = o['metrics'], o['response']; policy=m['policy']
            if policy == 'certified':
                checker.check(response, trusted, q, r['cut'], r['epoch'], r['owners'])
            if policy == 'overlay':
                checker.check_overlay(response, trusted, q, r['cut'], r['epoch'], r['owners'])
            got = response['rows']; good = got == exact
            bad = checker.sound_rows(got, t['initial'], t['history'], r['cut'], q)
            recall = len({(x['id'],x['body']) for x in got}&bodies)/len(bodies) if bodies else 1.0
            idrecall = len({x['id'] for x in got}&ids)/len(ids) if ids else 1.0
            assert m['exact'] == good and m['unsound_rows'] == bad
            assert m['target_body_recall'] == recall and m['id_recall'] == idrecall
            assert m['false_complete'] == (m['claimed_complete'] and not good)
            if policy in {'certified','overlay'}: assert not bad and not m['false_complete']
            flat.append(dict(trace=path.name, case=t['case'],seed=t['seed'],length=t['prefix_length'],
                alternates=t['alternates'],primary=(t['seed'] in (2,3) and t['prefix_length']==8 and t['alternates']),
                tick=r['tick'],query=q['id'],plan_alternatives=len(q['plan']),
                status=response.get('status','complete' if m['claimed_complete'] else 'unasserted'),
                rank_stability=r['rank_stability_from_initial'],**m))
    assert t['repair']['converged_replicas']==6
    assert t['repair']['retained_events_after']==[0]*6 and t['repair']['pending_events_after']==[0]*6
    return flat, dict(trace=path.name,case=t['case'],seed=t['seed'],length=t['prefix_length'],
        alternates=t['alternates'],verified_receipts=source_count,queries=len(t['records']),
        update_events=sum(map(len,t['history'])),attempts=len(t['attempts']),
        resources=t['resources'],repair=t['repair'])


def summarize(rows: list[dict]) -> dict:
    assert rows
    n=len(rows)
    return dict(n=n,exact=sum(r['exact'] for r in rows),complete=sum(r['claimed_complete'] for r in rows),
        false_complete=sum(r['false_complete'] for r in rows),unsound_rows=sum(r['unsound_rows'] for r in rows),
        returned_rows=sum(r['returned_rows'] for r in rows),
        mean_body_recall=sum(r['target_body_recall'] for r in rows)/n,
        mean_id_recall=sum(r['id_recall'] for r in rows)/n,
        mean_bytes=sum(r['bytes'] for r in rows)/n,mean_messages=sum(r['messages'] for r in rows)/n,
        mean_certificate_bytes=sum(r['certificate_bytes'] for r in rows)/n,
        wall_p50_ms=quantile([r['wall_ns']/1e6 for r in rows],.5),
        wall_p95_ms=quantile([r['wall_ns']/1e6 for r in rows],.95),
        check_p50_us=quantile([r['checker_ns']/1e3 for r in rows],.5),
        check_p95_us=quantile([r['checker_ns']/1e3 for r in rows],.95),
        source_cpu_p50_us=quantile([r['server_cpu_ns']/1e3 for r in rows],.5),
        statuses=dict(Counter(r['status'] for r in rows)))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trace',help='One retained trace basename; all by default')
    args=parser.parse_args(); start=time.monotonic(); cpu=time.process_time()
    fixture=build(ROOT); checker=Checker(fixture['payloads'])
    paths=[ROOT/'results/traces'/args.trace] if args.trace else sorted((ROOT/'results/traces').glob('*.json.gz'))
    observations=[]; checks=[]
    for path in paths:
        rows, info=inspect(path,fixture,checker); observations.extend(rows); checks.append(info)
    if not args.trace:
        mainrows=[r for r in observations if r['primary']]
        assert len(mainrows)==1008*6 and len(paths)==33
        groups=defaultdict(list)
        for r in mainrows: groups[(r['case'],r['policy'])].append(r)
        cases={case:{policy:summarize(group) for (c,policy),group in groups.items() if c==case}
               for case in sorted({r['case'] for r in mainrows})}
        policies={policy:summarize([r for r in mainrows if r['policy']==policy])
                  for policy in sorted({r['policy'] for r in mainrows})}
        devgroups=defaultdict(list)
        for r in observations:
            if r['seed']==1 and r['policy']=='certified':
                devgroups[(r['case'],r['length'],r['alternates'])].append(r)
        sens=[dict(case=c,length=l,alternates=a,**summarize(rows))
              for (c,l,a),rows in sorted(devgroups.items())]
        summary={'primary_policies':policies,'primary_cases':cases,'development_sensitivity':sens,
                 'primary_seeds':[2,3],'queries_per_primary_policy':1008,
                 'quantile':'nearest-rank descriptive empirical quantile; no population CI',
                 'verification':{'traces':len(paths),'observations':len(observations),
                     'source_receipts':sum(x['verified_receipts'] for x in checks),
                     'update_events':sum(x['update_events'] for x in checks)},
                 'resource_cases':{'sum_recorded_cpu_seconds':sum(x['resources']['cpu_seconds'] for x in checks),
                     'maximum_case_rss_kib':max(x['resources']['peak_rss_kib'] for x in checks),
                     'maximum_case_wall_seconds':max(x['resources']['wall_seconds'] for x in checks)},
                 'scope':'synthetic schedules on static public distribution-source subsets; co-resident logical nodes'}
        write_json(ROOT/'results/summary.json',summary)
        with (ROOT/'results/observations.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(observations[0]));w.writeheader();w.writerows(observations)
        write_json(ROOT/'results/trace-checks.json',checks)
        # Data files directly consumed by paper PGFPlots, without a second analysis.
        with (ROOT/'results/prefix-sensitivity.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow(['length','index_complete_percent','index_exact_percent','hot_complete_percent'])
            for l in (1,5,8,16,32,64):
                a=next(x for x in sens if x['case']=='index-lag' and x['length']==l and x['alternates'])
                h=next((x for x in sens if x['case']=='hot-shard' and x['length']==l and x['alternates']),None)
                w.writerow([l,100*a['complete']/a['n'],100*a['exact']/a['n'],100*h['complete']/h['n'] if h else 'nan'])
    report={'traces':len(paths),'source_receipts':sum(x['verified_receipts'] for x in checks),
            'observations':len(observations),'elapsed_seconds':time.monotonic()-start,
            'cpu_seconds':time.process_time()-cpu,'peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'result':'all retained source receipts, answers and recorded correctness/recall metrics agree with replay'}
    print(json.dumps(report,sort_keys=True))
    if not args.trace: write_json(ROOT/'results/analysis-check.json',report)

if __name__=='__main__': main()
