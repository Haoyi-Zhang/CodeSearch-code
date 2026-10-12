#!/usr/bin/env python3
"""Replay the offline comparison using only histories, receipts and checkers."""
from collections import Counter
import gzip
import json
from pathlib import Path
import time
from analyze_continuation import verify_source_receipts
from src.checker import Checker
from src.continuation_checker import ContinuationChecker
from src.corpus import build
from src.real_history import build_real_history

ROOT = Path(__file__).resolve().parent


def verify(packet, payloads):
    checked = Counter()
    for trial in packet['trials']:
        source, evidence, _, _ = verify_source_receipts(trial, payloads)
        checked.update(source)
        oracle = Checker(payloads)
        checker = ContinuationChecker(payloads)
        owners = [[0, 1], [2, 3], [4, 5]]
        queries = {q['id']: q for q in trial['queries']}
        acquisition = len(queries)*3
        for ident in range(1, acquisition+1):
            entry = evidence[ident]
            query = entry['request']['query']
            checker.install_prefix(query, entry, owners, 0, entry['request']['length'])
        last = {q['id']: oracle.oracle(trial['initial'], trial['history'], [0, 0, 0], q)
                for q in queries.values()}
        accepted = set()
        changes = 0
        for record in trial['records']:
            sequence = record['sequence']
            cut = record['result']['cut']
            if sequence not in accepted:
                for shard in range(3):
                    lo, hi = checker.frontiers[shard], cut[shard]
                    if lo == hi:
                        continue
                    matches = [e for e in evidence.values() if e['reply'].get('kind') == 'events'
                               and e['reply']['shard'] == shard and e['reply']['hi'] == hi
                               and e['reply']['lo'] == lo]
                    if len(matches) != 1:
                        raise AssertionError('unique feed receipt')
                    checker.accept_events(matches[0], owners, 0)
                accepted.add(sequence)
            query = queries[record['query']]
            result = record['result']
            checker.check_result(result, query, cut, 0, owners, evidence, result['repairs'])
            exact = oracle.oracle(trial['initial'], trial['history'], cut, query)
            oracle.check_rows(record['oracle'])
            changed = exact != last[query['id']]
            if record['oracle'] != exact or record['changed'] != changed:
                raise AssertionError('recorded oracle')
            if result['rows'] != exact or result['status'] != 'complete':
                raise AssertionError('policy correctness')
            changes += int(changed)
            last[query['id']] = exact
            checked['results'] += 1
        costs = Counter()
        returned_rows = Counter()
        messages = Counter()
        for ident, entry in evidence.items():
            phase = ('acquisition' if ident <= acquisition else
                     'feed' if entry['request']['op'] == 'events' else 'refill')
            # Independent canonical encoder, same declared wire format.
            for value in (entry['request'], entry['reply']):
                costs[phase] += len((json.dumps(value, sort_keys=True,
                    separators=(',', ':'))+'\n').encode())
                messages[phase] += 1
            returned_rows[phase] += len(entry['reply'].get('rows', []))
        summary = trial['summary']
        for phase, recorded in summary['source_costs'].items():
            if costs[phase] != recorded.get('bytes', 0) or messages[phase] != recorded.get('messages', 0):
                raise AssertionError('phase payload accounting')
            if returned_rows[phase] != recorded.get('returned_rows', 0):
                raise AssertionError('source row accounting')
        if sum(costs.values()) != summary['total_source_bytes'] or changes != summary['oracle_changes']:
            raise AssertionError('summary aggregation')
        refills = sum(len(r['result']['repairs']) for r in trial['records'])
        if refills != summary['refill_shards']:
            raise AssertionError('refill aggregation')
        checked['trials'] += 1
    return checked


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--generated', action='store_true')
    args = parser.parse_args()
    started = time.monotonic()
    stem = 'incremental-generated' if args.generated else 'incremental-comparison'
    with gzip.open(ROOT/f'results/{stem}-trace.json.gz', 'rt', encoding='utf-8') as stream:
        packet = json.load(stream)
    fixture = build_real_history(ROOT, build(ROOT))
    result = {'result': 'PASS', 'counts': dict(verify(packet, fixture['payloads'])),
              'wall_seconds': time.monotonic()-started,
              'scope': 'source truth, strict score types, checker transitions, exact answers, oracle change markers, phase bytes/messages/rows and refill counts; CPU and operation counters not independently recounted'}
    (ROOT/f'results/{stem}-replay.json').write_text(
        json.dumps(result, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
