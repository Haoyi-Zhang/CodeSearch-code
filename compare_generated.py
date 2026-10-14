#!/usr/bin/env python3
"""Same-information refill comparison on supplied generated rank-churn histories."""
import asyncio
from copy import deepcopy
import gzip
import json
from pathlib import Path
import time
from compare_incremental import independent_queries, trial, POLICIES
from src.corpus import build

ROOT = Path(__file__).resolve().parent


async def main(output_dir=None):
    started = time.monotonic()
    base = build(ROOT)
    # Two of the already fixed hash-selected seeds per project, before trace read.
    independent = independent_queries(base)
    queries = [q for i, q in enumerate(independent) if i % 8 < 2]
    trials = []
    inputs = []
    for seed in (2, 3):
        path = ROOT/f'results/continuation-traces/continuation-rank-churn-seed{seed}-capacity5.json.gz'
        with gzip.open(path, 'rt', encoding='utf-8') as stream:
            supplied = json.load(stream)
        if supplied['initial'] != base['initial'] or supplied['case'] != 'rank-churn':
            raise AssertionError('supplied history binding')
        if any(t['epoch'] != 0 or t['blocked'] for t in supplied['timeline']):
            raise AssertionError('comparison expects fault-free ownership')
        fixture = deepcopy(base)
        for state in fixture['initial']:
            for q in queries:
                state.pop(q['seed_id'], None)
        fixture['history'] = supplied['history']
        fixture['schedule'] = [t['cut'] for t in supplied['timeline']]
        inputs.append(str(path.relative_to(ROOT)))
        for capacity in (5, 10, 16):
            matched = []
            for policy in POLICIES:
                result = await trial(fixture, queries, policy, capacity=capacity, lag=2)
                result['summary'].update(query_group='base-independent-16', seed=seed)
                trials.append(result)
                matched.append(result)
            rows = [[r['result']['rows'] for r in t['records']] for t in matched]
            if rows[0] != rows[1] or rows[0] != rows[2]:
                raise AssertionError('matched answers')
    report = {'schema': 'same-information-generated-v1', 'inputs': inputs,
        'trials': [t['summary'] for t in trials], 'wall_seconds': time.monotonic()-started,
        'scope': 'supplied generated rank-churn seeds 2/3; fixed base-only independent 16-query subset; every query active at all 63 ticks; matched L=5/10/16, k=5, two-event index lag; offline payload/work comparison, not the original activation schedule or new public history',
        'query_rule': 'first two of the frozen eight hash-selected base-only queries/project, selected before reading supplied histories; their initial seed IDs excluded',
        'cost_scope': 'same acquisition/feed/refill, state and instrumented source/coordinator work definitions as compare_incremental.py'}
    output_dir = Path(output_dir) if output_dir is not None else ROOT/'results'
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir/'incremental-generated.json').write_text(
        json.dumps(report, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    with gzip.open(output_dir/'incremental-generated-trace.json.gz', 'wt', encoding='utf-8') as stream:
        json.dump({'report': report, 'trials': trials}, stream, sort_keys=True, separators=(',', ':'))
    print(json.dumps({'result': 'PASS', 'trials': len(trials),
        'calls': sum(t['summary']['calls'] for t in trials), 'wall_seconds': report['wall_seconds']}, sort_keys=True))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path)
    asyncio.run(main(parser.parse_args().output_dir))
