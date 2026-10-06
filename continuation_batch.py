#!/usr/bin/env python3
"""Regenerate every continuation trace with bounded independent workers.

Each job constructs a fresh six-endpoint loopback network and writes a disjoint
trace. At most two jobs overlap; the final analyzer separately replays all
receipts and checker transitions. The manifest is the exact primary/development
set consumed by analyze_continuation.py.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
from continuation_limits import BATCH_SECONDS

MAX_WORKERS = 2
BATCH_DEADLINE_SECONDS = BATCH_SECONDS
TRACE_DEADLINE_SECONDS = 60
CHILD_ADDRESS_SPACE_BYTES = 3 * 1024**3
PRIMARY_CASES = (
    'fresh', 'index-lag', 'log-gap', 'partition', 'crash', 'rebalance',
    'hot-shard', 'compaction', 'rank-churn',
)


def trace_path(case: str, seed: int, active: int, capacity: int) -> Path:
    stem = f'continuation-{case}-seed{seed}'
    if active != 4:
        stem += f'-active{active}'
    if capacity != 8:
        stem += f'-capacity{capacity}'
    return ROOT / 'results' / 'continuation-traces' / f'{stem}.json.gz'


def manifest() -> list[tuple[str, int, int, int]]:
    jobs = [(case, seed, 4, 5) for case in PRIMARY_CASES for seed in (2, 3)]
    jobs.extend([
        ('fresh', 1, 1, 5),
        ('fresh', 1, 4, 5),
        ('fresh', 1, 16, 5),
        ('rank-churn', 1, 1, 5),
        ('rank-churn', 1, 4, 5),
        ('rank-churn', 1, 16, 5),
        ('rank-churn', 1, 4, 8),
        ('rank-churn', 1, 4, 16),
    ])
    assert len(jobs) == 26 and len(set(jobs)) == 26
    return jobs


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


def run_job(index: int, job: tuple[str, int, int, int], started_wall: float,
            log_dir: Path) -> dict:
    case, seed, active, capacity = job
    remaining = BATCH_DEADLINE_SECONDS - (time.monotonic() - started_wall)
    if remaining <= 0:
        raise TimeoutError(f'overall {BATCH_DEADLINE_SECONDS}-second continuation-batch deadline')
    output = trace_path(case, seed, active, capacity)
    log = log_dir / f'trace-{index:02d}.txt'
    timing = log_dir / f'.trace-{index:02d}.time'
    if timing.exists():
        timing.unlink()
    parts = [
        'continuation_campaign.py', '--case', case, '--seed', str(seed),
        '--active', str(active), '--capacity', str(capacity),
    ]
    command = [
        '/usr/bin/time', '-f', '%U\t%S\t%M', '-o', str(timing),
        '/usr/bin/prlimit',
        f'--as={CHILD_ADDRESS_SPACE_BYTES}:{CHILD_ADDRESS_SPACE_BYTES}',
        f'--cpu={TRACE_DEADLINE_SECONDS}:{TRACE_DEADLINE_SECONDS + 5}', '--',
        sys.executable, *parts,
    ]
    wall = time.monotonic()
    with log.open('w', encoding='utf-8') as stream:
        stream.write('Command: python ' + ' '.join(parts) + '\n')
        stream.flush()
        process = subprocess.Popen(
            command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONHASHSEED': '0'},
        )
        try:
            process.wait(timeout=min(remaining, TRACE_DEADLINE_SECONDS))
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    cpu_seconds, peak_rss_kib = parse_timing(timing)
    if timing.exists():
        timing.unlink()
    record = {
        'index': index,
        'case': case,
        'seed': seed,
        'active': active,
        'capacity': capacity,
        'output': str(output.relative_to(ROOT)),
        'exit_code': process.returncode,
        'wall_seconds': time.monotonic() - wall,
        'cpu_seconds': cpu_seconds,
        'peak_rss_kib': peak_rss_kib,
        'log': str(log.relative_to(ROOT)),
    }
    if process.returncode:
        raise RuntimeError(f'continuation trace failed: {job!r}; see {log}')
    if not output.exists():
        raise RuntimeError(f'continuation trace was not published: {output}')
    return record


def main() -> None:
    if sys.flags.optimize:
        raise SystemExit('Assertions are part of validation; do not use python -O.')
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    jobs = manifest()
    trace_dir = ROOT / 'results' / 'continuation-traces'
    trace_dir.mkdir(parents=True, exist_ok=True)
    for stale in trace_dir.glob('continuation-*.json.gz'):
        stale.unlink()
    for stale in trace_dir.glob('*.tmp'):
        stale.unlink()
    log_dir = ROOT / 'results' / 'continuation-batch-logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    for stale in log_dir.iterdir():
        if stale.is_file():
            stale.unlink()

    records_by_index: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_job = {
            executor.submit(run_job, index, job, started_wall, log_dir): (index, job)
            for index, job in enumerate(jobs, 1)
        }
        for future in as_completed(future_to_job):
            index, _ = future_to_job[future]
            record = future.result()
            records_by_index[index] = record
            print(json.dumps({'batch_record': record}, sort_keys=True), flush=True)

    records = [records_by_index[index] for index in range(1, len(jobs) + 1)]
    cpu_values = [record['cpu_seconds'] for record in records if record['cpu_seconds'] is not None]
    rss_values = [record['peak_rss_kib'] for record in records if record['peak_rss_kib'] is not None]
    report = {
        'status': 'PASS',
        'traces': len(records),
        'primary_traces': 18,
        'development_traces': 8,
        'wall_seconds': time.monotonic() - started_wall,
        'cpu_seconds': sum(cpu_values),
        'peak_rss_kib': max(rss_values, default=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        'driver_cpu_seconds': time.process_time() - started_cpu,
        'driver_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'workers': MAX_WORKERS,
        'deadline_seconds': BATCH_DEADLINE_SECONDS,
        'records': records,
    }
    path = ROOT / 'results' / 'continuation-batch.json'
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'records'},
                     sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
