#!/usr/bin/env python3
"""Regenerate the human- and machine-readable resource ledger from retained runs.

This script performs no experiment and no network access.  It only projects the
three clean reproduction records and their stage logs into docs/resource-ledger.*
so that prose cannot silently drift from the retained measurements.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
DOCS = ROOT / "docs"


def load(name: str) -> dict:
    value = json.loads((RESULTS / name).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{name} is not a JSON object")
    return value


def test_count(log: Path) -> int:
    text = log.read_text(encoding="utf-8")
    matches = re.findall(r"Ran (\d+) tests?", text)
    if not matches:
        raise ValueError(f"could not find unittest count in {log}")
    return int(matches[-1])


def fmt(value: float) -> str:
    return f"{value:.6f}"


def stage(report: dict, index: int) -> dict:
    command = report["commands"][index]
    return {
        "command": command["command"],
        "exit_code": command["exit_code"],
        "wall_seconds": command["wall_seconds"],
        "cpu_seconds": command["cpu_seconds"],
        "log": command["log"],
    }


def main() -> None:
    continuation = load("continuation-clean-reproduction.json")
    real = load("real-history-clean-reproduction.json")
    cold = load("clean-reproduction.json")
    batch = load("continuation-batch.json")
    finite = load("continuation-finite.json")
    primary = load("continuation-summary.json")
    real_summary = load("real-history-summary.json")
    pilot = load("pilot.json")

    for name, report in (("continuation", continuation), ("real history", real), ("cold", cold)):
        if report.get("status") != "PASS" or report.get("error") is not None:
            raise ValueError(f"{name} clean reproduction is not PASS")
        if any(item.get("exit_code") != 0 for item in report.get("commands", [])):
            raise ValueError(f"{name} clean reproduction contains a failed command")

    directed_tests = test_count(ROOT / continuation["commands"][0]["log"])
    real_tests = test_count(ROOT / real["commands"][0]["log"])
    cold_tests = test_count(ROOT / cold["commands"][0]["log"])
    finite_total = (
        finite["local_transition_cases"]
        + finite["k_plus_d_repair_cases"]
        + finite["global_merge_cases"]
    )
    primary_receipts = sum(primary["source_replay"][key] for key in ("events", "overlay", "prefix", "snapshot"))
    real_receipts = sum(real_summary["source_replay"][key] for key in ("events", "overlay", "prefix", "snapshot"))

    ledger = {
        "scope": "Retained final executions; not exact cumulative development accounting",
        "environment": {
            "logical_endpoints": 6,
            "primary_topology": "one process with six persistent logical loopback endpoints per trace",
            "held_out_topology": "six independent OS source processes plus one coordinator process on one host",
            "stage_workers": 1,
            "maximum_trace_workers": continuation["bounds"]["maximum_trace_workers"],
            "primary_child_address_space_limit_bytes": continuation["bounds"]["child_address_space_bytes"],
            "held_out_child_address_space_limit_bytes": real["bounds"]["child_address_space_bytes"],
            "continuation_suite_deadline_seconds": continuation["bounds"]["overall_deadline_seconds"],
            "real_history_suite_deadline_seconds": real["bounds"]["overall_deadline_seconds"],
            "one_shot_suite_deadline_seconds": cold["bounds"]["overall_deadline_seconds"],
            "archive_ceiling_bytes": 128 * 1024**2,
        },
        "test_counts": {
            "directed_unit_tests": directed_tests,
            "real_history_directed_tests": real_tests,
            "cold_suite_discovered_tests": cold_tests,
        },
        "continuation_clean_reproduction": {
            "status": continuation["status"],
            "commands": len(continuation["commands"]),
            "wall_seconds": continuation["wall_seconds"],
            "child_cpu_seconds": continuation["child_cpu_seconds"],
            "parent_cpu_seconds": continuation["parent_cpu_seconds"],
            "maximum_child_rss_kib": continuation["maximum_child_rss_kib"],
            "driver_rss_kib": continuation["driver_rss_kib"],
            "primary_traces": 18,
            "development_traces": 8,
            "trace_jobs": 26,
            "maximum_trace_workers": continuation["bounds"]["maximum_trace_workers"],
            "stage_records": [stage(continuation, index) for index in range(len(continuation["commands"]))],
        },
        "continuation_batch": {
            "status": batch["status"],
            "wall_seconds": batch["wall_seconds"],
            "cpu_seconds": batch["cpu_seconds"],
            "driver_cpu_seconds": batch["driver_cpu_seconds"],
            "peak_rss_kib": batch["peak_rss_kib"],
            "driver_rss_kib": batch["driver_rss_kib"],
            "workers": batch["workers"],
            "traces": batch["traces"],
            "primary_traces": batch["primary_traces"],
            "development_traces": batch["development_traces"],
        },
        "continuation_finite": {
            "local_transitions": finite["local_transition_cases"],
            "repair_cases": finite["k_plus_d_repair_cases"],
            "global_cases": finite["global_merge_cases"],
            "total_cases": finite_total,
            "latest_clean_stage_wall_seconds": continuation["commands"][1]["wall_seconds"],
            "latest_clean_stage_cpu_seconds": continuation["commands"][1]["cpu_seconds"],
        },
        "continuation_independent_analysis": {
            "primary_traces": primary["primary_traces"],
            "queries": primary["primary_queries"],
            "source_receipts": primary_receipts,
            "checker_transitions": primary["checker_replay"]["query_observations"],
            "latest_clean_stage_wall_seconds": continuation["commands"][3]["wall_seconds"],
            "latest_clean_stage_cpu_seconds": continuation["commands"][3]["cpu_seconds"],
        },
        "real_history_clean_reproduction": {
            "status": real["status"],
            "commands": len(real["commands"]),
            "wall_seconds": real["wall_seconds"],
            "child_cpu_seconds": real["child_cpu_seconds"],
            "parent_cpu_seconds": real["parent_cpu_seconds"],
            "maximum_child_rss_kib": real["maximum_child_rss_kib"],
            "driver_rss_kib": real["driver_rss_kib"],
            "source_processes": real["bounds"]["source_processes"],
            "coordinator_processes": real["bounds"]["coordinator_processes"],
            "selected_commits": real_summary["selected_commits"],
            "function_assignments": real_summary["function_assignments"],
            "queries_per_policy": real_summary["queries_per_policy"],
            "delivered_source_replies_replayed": real_receipts,
            "stage_records": [stage(real, index) for index in range(len(real["commands"]))],
        },
        "one_shot_clean_reproduction_final": {
            "status": cold["status"],
            "commands": len(cold["commands"]),
            "wall_seconds": cold["wall_seconds"],
            "child_cpu_seconds": cold["child_cpu_seconds"],
            "parent_cpu_seconds": cold["parent_cpu_seconds"],
            "maximum_child_rss_kib": cold["maximum_child_rss_kib"],
            "driver_rss_kib": cold["driver_rss_kib"],
            "maximum_trace_workers": cold["bounds"]["workers"],
        },
        "pilot_final": {
            "status": "PASS" if pilot["negative_control_detected"] else "FAIL",
            "wall_seconds": pilot["wall_seconds"],
            "maximum_rss_kib": pilot["peak_rss_kib"],
            "workers": pilot["workers"],
        },
        "scientific_runtime_network_download_bytes": 0,
        "web_literature_transfer_bytes": None,
        "historical_cumulative_cpu_seconds": None,
        "historical_accounting_limitation": (
            "Exploratory, interrupted, and repeated development commands were not all instrumented. "
            "Final clean suites and retained trace jobs are measured separately; duplicate reruns are "
            "not counted as distinct scientific observations."
        ),
    }
    (DOCS / "resource-ledger.json").write_text(
        json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    c = ledger["continuation_clean_reproduction"]
    r = ledger["real_history_clean_reproduction"]
    o = ledger["one_shot_clean_reproduction_final"]
    pl = ledger["pilot_final"]
    stages = c["stage_records"]
    markdown = f"""# Resource and accounting ledger

This file is generated by `refresh_resource_ledger.py` from the retained JSON run records. It is descriptive execution evidence, not a production benchmark or a claim of scientific acceptance.

## Final primary continuation execution

`results/continuation-clean-reproduction.json` is the strongest primary execution record. Four stages pass in sequence: {directed_tests} unit tests; {finite_total:,} finite instances; a 26-trace batch containing 18 primary and eight development traces; and independent replay. The final suite takes {fmt(c['wall_seconds'])} wall seconds and {fmt(c['child_cpu_seconds'])} aggregate child CPU seconds; driver CPU is {fmt(c['parent_cpu_seconds'])} seconds. Maximum child RSS is {c['maximum_child_rss_kib']:,} KiB and driver RSS is {c['driver_rss_kib']:,} KiB. Every child has a 3 GiB address-space limit and the suite has a 170-second deadline. Stages are sequential; only the trace batch uses up to {c['maximum_trace_workers']} independent workers.

The latest clean-stage records are: tests {fmt(stages[0]['wall_seconds'])} wall/{fmt(stages[0]['cpu_seconds'])} CPU seconds; finite checking {fmt(stages[1]['wall_seconds'])}/{fmt(stages[1]['cpu_seconds'])}; trace batch {fmt(stages[2]['wall_seconds'])}/{fmt(stages[2]['cpu_seconds'])}; and analysis {fmt(stages[3]['wall_seconds'])}/{fmt(stages[3]['cpu_seconds'])}. The finite cases comprise {finite['local_transition_cases']:,} local token transitions, {finite['k_plus_d_repair_cases']:,} repair cases, and {finite['global_merge_cases']:,} global merges. Primary replay covers 18 traces, {primary['primary_queries']:,} queries, {primary_receipts:,} source receipts, and {primary['checker_replay']['query_observations']:,} checker transitions.

## Held-out published-history and process execution

`results/real-history-clean-reproduction.json` records the fixed function-level history and six-process validation. {real_tests} directed tests, the process campaign, and independent replay pass in {fmt(r['wall_seconds'])} wall seconds with {fmt(r['child_cpu_seconds'])} aggregate child CPU seconds. Maximum child RSS is {r['maximum_child_rss_kib']:,} KiB; driver RSS is {r['driver_rss_kib']:,} KiB. Six source processes plus one coordinator run on a single host under a 90-second deadline and 4 GiB child address-space bound. The campaign covers six selected commits, seven function assignments, 408 observations per policy, one real process termination/replacement, final six-process convergence, and independent reconstruction of {real_receipts:,} delivered source replies. This is process isolation on one host, not multi-host availability.

## Retained one-shot evidence

The retained one-shot driver passes {o['commands']} commands in {fmt(o['wall_seconds'])} wall seconds with {fmt(o['child_cpu_seconds'])} aggregate child CPU seconds and {fmt(o['parent_cpu_seconds'])} driver CPU seconds. Maximum child RSS is {o['maximum_child_rss_kib']:,} KiB and driver RSS is {o['driver_rss_kib']:,} KiB. Its discovery stage runs the same {cold_tests} tests; finite checks run sequentially, 33 disjoint trace jobs use at most three workers, and final analysis is serial. The pre-lock pilot is a separate focused command; it completed in {pl['wall_seconds']:.2f} wall seconds with {pl['maximum_rss_kib']:,} KiB maximum RSS.

## Metric boundaries

Source communication counts serialized JSON request and reply bytes attributable to each policy. Continuation setup, reconfiguration, and shared-feed traffic are allocated over active queries; mirror update traffic is charged. Client-to-coordinator result delivery, source-body acquisition, process-control dissemination in the held-out harness, TCP/IP headers, TLS, production serialization, durable-storage traffic, and the oracle are excluded for every policy. Loopback wall time is descriptive; no production-latency claim is made.

Logical state is the canonical serialized payload needed by a policy: tokens plus retained coordinator feed, cached local results/cuts, or complete mirror maps/postings. It is not Python heap usage. RSS is separately reported by process-level measurements.

## Inputs and network use

Runtime reproduction consumes the included 107 Python source files, their retained notices, and the pinned function-level history input. Third-party source text is parsed but never imported or executed. Scientific reproduction performs no network download, package installation, GPU/API call, account operation, or external compute job. Literature was checked against public scholarly or official records; transfer bytes were not metered and are not reported as an exact total.

## Historical accounting limit

Exact cumulative CPU used during all exploratory design, debugging, interrupted outer-tool calls, and repeated validation is unknown because every interactive command was not instrumented. Those costs are not reported as zero. Final clean suites, batch jobs, and finite stages are measured separately, and duplicate reruns are not relabeled as unique scientific observations. No retained run approaches the project ceilings of eight CPU-hours, 4 GiB memory, 1 GiB downloads, or a 128 MiB final archive.
"""
    (DOCS / "resource-ledger.md").write_text(markdown, encoding="utf-8")
    print(json.dumps({
        "status": "PASS",
        "directed_unit_tests": directed_tests,
        "finite_instances": finite_total,
        "primary_source_receipts": primary_receipts,
        "real_history_source_receipts": real_receipts,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
