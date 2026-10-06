#!/usr/bin/env python3
"""Reproduce the continuation-certificate campaign in bounded stages.

The suite regenerates all primary and development traces used by the paper,
independently replays every source receipt, and compares deterministic semantic
and wire outcomes with the frozen contract. It is separate from run_suite.py,
which retains the earlier one-shot falsification campaign.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from continuation_limits import SUITE_SECONDS

ROOT=Path(__file__).resolve().parent
CASES=('fresh','index-lag','log-gap','partition','crash','rebalance','hot-shard','compaction','rank-churn')
MAX_TRACE_WORKERS=2


def limits() -> None:
    resource.setrlimit(resource.RLIMIT_AS,(3*1024**3,3*1024**3))
    resource.setrlimit(resource.RLIMIT_CPU,(165,170))


def compare(expected: object, actual: object, where: str='root') -> None:
    if isinstance(expected,dict):
        if not isinstance(actual,dict): raise AssertionError(where)
        for key,value in expected.items():
            if key=='purpose': continue
            if key not in actual: raise AssertionError(where+'.'+key)
            compare(value,actual[key],where+'.'+key)
    elif isinstance(expected,list):
        if not isinstance(actual,list) or len(expected)!=len(actual): raise AssertionError(where)
        for index,(x,y) in enumerate(zip(expected,actual)):
            compare(x,y,f'{where}[{index}]')
    elif expected!=actual:
        raise AssertionError((where,expected,actual))


def projected(summary: dict) -> dict:
    fields=('queries','exact','claimed_complete','false_complete','unsound_rows',
            'returned_rows','partial_or_unjustified','total_source_bytes','total_source_messages')
    answer={
        'primary_traces':summary['primary_traces'],'primary_queries':summary['primary_queries'],
        'cases':summary['cases'],'seeds':summary['seeds'],
        'standing_queries_per_trace':summary['standing_queries_per_trace'],
        'active_queries_per_tick':summary['active_queries_per_tick'],
        'token_capacity':summary['token_capacity'],'policies':{},
        'continuation_repairs':{k:summary['continuation_repairs'][k] for k in ('queries','shards')},
        'source_replay':summary['source_replay'],'checker_replay':summary['checker_replay'],
        'setup_source_bytes':summary['setup_source_bytes'],
        'shared_feed_source_bytes':summary['shared_feed_source_bytes'],
        'finite_checks':summary['finite_checks'],
    }
    for policy,data in summary['policies'].items():
        answer['policies'][policy]={k:data[k] for k in fields}
        if 'max_logical_state_bytes' in data:
            answer['policies'][policy]['max_logical_state_bytes']=data['max_logical_state_bytes']
    return answer


def main() -> None:
    if sys.flags.optimize:
        raise SystemExit('Assertions are part of validation; do not use python -O.')
    start=time.monotonic(); parent_cpu=time.process_time()
    children0=resource.getrusage(resource.RUSAGE_CHILDREN)
    results=ROOT/'results';results.mkdir(exist_ok=True)
    logs=results/'continuation-reproduction-logs';logs.mkdir(exist_ok=True)
    for stale in logs.glob('command-*.txt'):
        stale.unlink()
    commands=[
        ['-m','unittest','discover','-s','tests','-v'],
        ['finite_continuation.py'],
        ['continuation_batch.py'],
        ['analyze_continuation.py'],
    ]
    records=[];error=None
    try:
        for index,parts in enumerate(commands,1):
            remaining=SUITE_SECONDS-(time.monotonic()-start)
            if remaining<=0: raise TimeoutError(f'overall {SUITE_SECONDS}-second continuation-suite deadline')
            wall=time.monotonic();before=resource.getrusage(resource.RUSAGE_CHILDREN)
            log=logs/f'command-{index:02d}.txt'
            with log.open('w',encoding='utf-8') as output:
                output.write('Command: python '+' '.join(parts)+'\n');output.flush()
                process=subprocess.run([sys.executable,*parts],cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,
                    timeout=remaining,check=False,preexec_fn=limits,
                    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONHASHSEED':'0'})
            after=resource.getrusage(resource.RUSAGE_CHILDREN)
            record={'command':['python',*parts],'exit_code':process.returncode,
                    'wall_seconds':time.monotonic()-wall,
                    'cpu_seconds':(after.ru_utime+after.ru_stime)-(before.ru_utime+before.ru_stime),
                    'log':str(log.relative_to(ROOT))}
            records.append(record);print(json.dumps(record),flush=True)
            if process.returncode:
                raise RuntimeError('child command failed: '+' '.join(parts))
        expected=json.loads((ROOT/'docs/expected_continuation_semantics.json').read_text())
        actual=json.loads((results/'continuation-summary.json').read_text())
        compare(expected,projected(actual))
    except (AssertionError,RuntimeError,TimeoutError,subprocess.TimeoutExpired) as exc:
        error=repr(exc)
    finally:
        after=resource.getrusage(resource.RUSAGE_CHILDREN)
        report={'status':'PASS' if error is None else 'FAIL',
                'meaning':'command success plus frozen finite/semantic/wire outcomes; not production or novelty validation',
                'commands':records,'error':error,'wall_seconds':time.monotonic()-start,
                'child_cpu_seconds':(after.ru_utime+after.ru_stime)-(children0.ru_utime+children0.ru_stime),
                'parent_cpu_seconds':time.process_time()-parent_cpu,
                'maximum_child_rss_kib':after.ru_maxrss,
                'driver_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'bounds':{'workers':MAX_TRACE_WORKERS,'stage_workers':1,'maximum_trace_workers':MAX_TRACE_WORKERS,
                          'child_address_space_bytes':3*1024**3,'overall_deadline_seconds':SUITE_SECONDS},
                'scope':('unit tests, finite token cases, 18 primary traces, eight development traces, '
                         'and independent source/checker replay; test, finite, batch, and analysis stages '
                         'are sequential, while the batch uses at most two independent trace workers')}
        (results/'continuation-clean-reproduction.json').write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
        print(json.dumps({k:v for k,v in report.items() if k!='commands'},sort_keys=True),flush=True)
    if error is not None: raise SystemExit(1)


if __name__=='__main__':
    main()
