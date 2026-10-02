#!/usr/bin/env python3
"""Reproduce the fixed Git-history and six-process validation."""
from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent


def limits() -> None:
    resource.setrlimit(resource.RLIMIT_AS, (4 * 1024**3, 4 * 1024**3))
    resource.setrlimit(resource.RLIMIT_CPU, (90, 95))


def compare(expected: object, actual: object, where: str = "root") -> None:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertionError(where)
        for key, value in expected.items():
            if key not in actual:
                raise AssertionError(where + "." + key)
            compare(value, actual[key], where + "." + key)
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise AssertionError(where)
        for index, (left, right) in enumerate(zip(expected, actual)):
            compare(left, right, f"{where}[{index}]")
    elif expected != actual:
        raise AssertionError((where, expected, actual))


def projected(summary: dict) -> dict:
    fields = (
        "queries",
        "exact",
        "claimed_complete",
        "false_complete",
        "unsound_rows",
        "returned_rows",
        "total_source_bytes",
        "total_source_messages",
        "dropped_requests_or_replies",
    )
    answer = {
        key: summary[key]
        for key in (
            "schema",
            "repository",
            "base_commit",
            "observed_head_commit",
            "selected_commits",
            "function_assignments",
            "base_queries",
            "targeted_queries",
            "queries_per_policy",
            "oracle_change_observations",
            "queries_with_any_oracle_change",
            "source_replay",
            "checker_replay",
            "fault_probe",
            "process_recovery",
        )
    }
    answer["policies"] = {}
    for policy, data in summary["policies"].items():
        answer["policies"][policy] = {key: data[key] for key in fields}
        if "max_logical_state_bytes" in data:
            answer["policies"][policy]["max_logical_state_bytes"] = data[
                "max_logical_state_bytes"
            ]
    return answer


def main() -> None:
    if sys.flags.optimize:
        raise SystemExit("Assertions are part of validation; do not use python -O.")
    started = time.monotonic()
    parent_cpu = time.process_time()
    before_children = resource.getrusage(resource.RUSAGE_CHILDREN)
    logs = ROOT / "results/real-history-reproduction-logs"
    logs.mkdir(parents=True, exist_ok=True)
    for path in logs.glob("command-*.txt"):
        path.unlink()
    commands = [
        ["-m", "unittest", "tests.test_real_history", "-v"],
        ["real_history_campaign.py"],
        ["analyze_real_history.py"],
    ]
    records = []
    error = None
    try:
        for index, parts in enumerate(commands, 1):
            remaining = 90 - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("overall 90-second real-history-suite deadline")
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            wall = time.monotonic()
            log = logs / f"command-{index:02d}.txt"
            with log.open("w", encoding="utf-8") as output:
                output.write("Command: python " + " ".join(parts) + "\n")
                output.flush()
                process = subprocess.run(
                    [sys.executable, *parts],
                    cwd=ROOT,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=min(remaining, 85),
                    check=False,
                    preexec_fn=limits,
                    env={
                        **os.environ,
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "PYTHONHASHSEED": "0",
                    },
                )
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            record = {
                "command": ["python", *parts],
                "exit_code": process.returncode,
                "wall_seconds": time.monotonic() - wall,
                "cpu_seconds": (after.ru_utime + after.ru_stime)
                - (before.ru_utime + before.ru_stime),
                "log": str(log.relative_to(ROOT)),
            }
            records.append(record)
            print(json.dumps(record), flush=True)
            if process.returncode:
                raise RuntimeError("child command failed: " + " ".join(parts))
        expected = json.loads((ROOT / "docs/expected_real_history_semantics.json").read_text())
        actual = json.loads((ROOT / "results/real-history-summary.json").read_text())
        compare(expected, projected(actual))
    except (AssertionError, RuntimeError, TimeoutError, subprocess.TimeoutExpired) as exc:
        error = repr(exc)
    finally:
        after_children = resource.getrusage(resource.RUSAGE_CHILDREN)
        report = {
            "status": "PASS" if error is None else "FAIL",
            "meaning": (
                "fixed function-level Git history, six independent source processes, "
                "OS-process termination/restart, source receipt replay, checker replay, "
                "and frozen semantic/wire outcomes; not a multi-host or user-log validation"
            ),
            "commands": records,
            "error": error,
            "wall_seconds": time.monotonic() - started,
            "child_cpu_seconds": (after_children.ru_utime + after_children.ru_stime)
            - (before_children.ru_utime + before_children.ru_stime),
            "parent_cpu_seconds": time.process_time() - parent_cpu,
            "maximum_child_rss_kib": after_children.ru_maxrss,
            "driver_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "bounds": {
                "source_processes": 6,
                "coordinator_processes": 1,
                "child_address_space_bytes": 4 * 1024**3,
                "overall_deadline_seconds": 90,
            },
        }
        (ROOT / "results/real-history-clean-reproduction.json").write_text(
            json.dumps(report, sort_keys=True, indent=2) + "\n"
        )
        print(
            json.dumps(
                {key: value for key, value in report.items() if key != "commands"},
                sort_keys=True,
            ),
            flush=True,
        )
    if error is not None:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
