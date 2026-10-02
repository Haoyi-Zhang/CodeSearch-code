#!/usr/bin/env python3
"""Reproduce the retained one-shot campaign in bounded offline subprocesses.

Run in a disposable clean extraction: result files are regenerated in place.
No upstream input source is imported or executed. Ports are loopback-only.
An overall 180-second deadline is enforced, not promised as a completion time.
Independent trace jobs use at most three workers; tests, finite checks, and final
analysis remain sequential.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
OVERALL_DEADLINE_SECONDS = 180
MAX_TRACE_WORKERS = 3
CHILD_ADDRESS_SPACE_BYTES = 3 * 1024**3
CHILD_CPU_SOFT_SECONDS = 160
CHILD_CPU_HARD_SECONDS = 165
CASES = ('fresh', 'index-lag', 'log-gap', 'partition', 'crash', 'rebalance', 'hot-shard', 'compaction')
FIELDS = ('n', 'exact', 'complete', 'false_complete', 'unsound_rows', 'returned_rows',
          'mean_body_recall', 'mean_id_recall', 'mean_bytes', 'mean_messages',
          'mean_certificate_bytes', 'statuses')


def compare(expected: object, actual: object, where: str = 'root') -> None:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertionError(where)
        for key, value in expected.items():
            if key == 'purpose':
                continue
            if key not in actual:
                raise AssertionError(where + '.' + key)
            compare(value, actual[key], where + '.' + key)
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise AssertionError(where)
        for index, (left, right) in enumerate(zip(expected, actual)):
            compare(left, right, f'{where}[{index}]')
    elif isinstance(expected, float):
        if not isinstance(actual, (float, int)) or not math.isclose(
                expected, actual, rel_tol=1e-12, abs_tol=1e-12):
            raise AssertionError((where, expected, actual))
    elif expected != actual:
        raise AssertionError((where, expected, actual))


def parse_timing(path: Path) -> tuple[float | None, int | None]:
    if not path.exists():
        return None, None
    parts = path.read_text().strip().split('\t')
    if len(parts) != 3:
        return None, None
    try:
        return float(parts[0]) + float(parts[1]), int(parts[2])
    except ValueError:
        return None, None


def run_one(index: int, parts: list[str], start: float, logs: Path) -> dict:
    remaining = OVERALL_DEADLINE_SECONDS - (time.monotonic() - start)
    if remaining <= 0:
        raise TimeoutError(f'overall {OVERALL_DEADLINE_SECONDS}-second reproduction deadline')
    log = logs / f'command-{index:02d}.txt'
    timing = logs / f'.command-{index:02d}.time'
    if timing.exists():
        timing.unlink()
    command = [
        '/usr/bin/time', '-f', '%U\t%S\t%M', '-o', str(timing),
        '/usr/bin/prlimit',
        f'--as={CHILD_ADDRESS_SPACE_BYTES}:{CHILD_ADDRESS_SPACE_BYTES}',
        f'--cpu={CHILD_CPU_SOFT_SECONDS}:{CHILD_CPU_HARD_SECONDS}', '--',
        sys.executable, *parts,
    ]
    wall = time.monotonic()
    with log.open('w', encoding='utf-8') as output:
        output.write('Command: python ' + ' '.join(parts) + '\n')
        output.flush()
        process = subprocess.Popen(
            command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONHASHSEED': '0'},
        )
        try:
            process.wait(timeout=min(remaining, CHILD_CPU_HARD_SECONDS))
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    cpu_seconds, max_rss_kib = parse_timing(timing)
    if timing.exists():
        timing.unlink()
    return {
        'command': ['python', *parts],
        'exit_code': process.returncode,
        'wall_seconds': time.monotonic() - wall,
        'cpu_seconds': cpu_seconds,
        'maximum_rss_kib': max_rss_kib,
        'log': str(log.relative_to(ROOT)),
    }


def main() -> None:
    if sys.flags.optimize:
        raise SystemExit('Assertions are part of validation; do not use python -O.')
    start = time.monotonic()
    parent_cpu = time.process_time()
    results = ROOT / 'results'
    results.mkdir(exist_ok=True)
    logs = results / 'reproduction-logs'
    logs.mkdir(exist_ok=True)
    for stale in logs.glob('command-*.txt'):
        stale.unlink()
    for stale in logs.glob('.command-*.time'):
        stale.unlink()

    commands: list[list[str]] = [
        ['-m', 'unittest', 'discover', '-s', 'tests', '-v'],
        ['finite.py'],
    ]
    commands += [['reproduce.py', '--case', case, '--seed', str(seed)]
                 for case in CASES for seed in (1, 2, 3)]
    commands += [['reproduce.py', '--case', 'index-lag', '--seed', '1', '--length', str(length)]
                 for length in (1, 5, 16, 32, 64)]
    commands += [['reproduce.py', '--case', 'hot-shard', '--seed', '1', '--length', str(length)]
                 for length in (1, 5, 16)]
    commands += [
        ['reproduce.py', '--case', 'index-lag', '--seed', '1', '--no-alternates'],
        ['analyze.py'],
    ]

    records_by_index: dict[int, dict] = {}
    error: str | None = None
    try:
        # Admission checks precede all trace work.
        for index in (1, 2):
            record = run_one(index, commands[index - 1], start, logs)
            records_by_index[index] = record
            print(json.dumps(record), flush=True)
            if record['exit_code']:
                raise RuntimeError('child command failed: ' + ' '.join(commands[index - 1]))

        # Trace outputs are disjoint. Extraction metadata is identical and is
        # atomically replaced, so at most three trace jobs can safely overlap.
        trace_items = list(enumerate(commands[2:-1], start=3))
        with ThreadPoolExecutor(max_workers=MAX_TRACE_WORKERS) as executor:
            future_to_item = {
                executor.submit(run_one, index, parts, start, logs): (index, parts)
                for index, parts in trace_items
            }
            for future in as_completed(future_to_item):
                index, parts = future_to_item[future]
                record = future.result()
                records_by_index[index] = record
                print(json.dumps(record), flush=True)
                if record['exit_code']:
                    raise RuntimeError('child command failed: ' + ' '.join(parts))

        analyze_index = len(commands)
        record = run_one(analyze_index, commands[-1], start, logs)
        records_by_index[analyze_index] = record
        print(json.dumps(record), flush=True)
        if record['exit_code']:
            raise RuntimeError('child command failed: ' + ' '.join(commands[-1]))

        expected = json.loads((ROOT / 'docs/expected_semantics.json').read_text())
        actual = json.loads((results / 'summary.json').read_text())
        compare(expected, actual)
        finite = json.loads((results / 'finite-receipts.json').read_text())
        assert finite['totals']['cases'] == 78732
        assert finite['false_complete'] == finite['unsound_rows'] == 0
        assert finite['totals']['complete'] == 57811
        assert finite['totals']['conservative_noncertification'] == 9329
        schedules = json.loads((results / 'finite-schedules.json').read_text())
        assert schedules['counts']['checks'] == 5040
        assert schedules['false_complete'] == schedules['unsound_rows'] == 0
    except (AssertionError, RuntimeError, TimeoutError, subprocess.TimeoutExpired) as exc:
        error = repr(exc)
    finally:
        records = [records_by_index[index] for index in sorted(records_by_index)]
        measured_cpu = [record['cpu_seconds'] for record in records if record['cpu_seconds'] is not None]
        measured_rss = [record['maximum_rss_kib'] for record in records if record['maximum_rss_kib'] is not None]
        report = {
            'status': 'PASS' if error is None else 'FAIL',
            'meaning': 'command success and frozen finite outcomes, not scientific novelty or deployment correctness',
            'commands': records,
            'error': error,
            'wall_seconds': time.monotonic() - start,
            'child_cpu_seconds': sum(measured_cpu),
            'parent_cpu_seconds': time.process_time() - parent_cpu,
            'maximum_child_rss_kib': max(measured_rss, default=None),
            'driver_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'bounds': {
                'workers': MAX_TRACE_WORKERS,
                'child_address_space_bytes': CHILD_ADDRESS_SPACE_BYTES,
                'overall_deadline_seconds': OVERALL_DEADLINE_SECONDS,
            },
            'scope': ('unit tests, finite receipt/schedule checks, retained TCP campaign and final replay; '
                      'independent trace jobs use no more than three workers; the pre-lock pilot is a '
                      'separate focused command; source inputs are not executed'),
        }
        (results / 'clean-reproduction.json').write_text(
            json.dumps(report, sort_keys=True, indent=2) + '\n')
        print(json.dumps({key: value for key, value in report.items() if key != 'commands'},
                         sort_keys=True), flush=True)
    if error is not None:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
