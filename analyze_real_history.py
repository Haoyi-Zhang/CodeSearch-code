#!/usr/bin/env python3
"""Independent replay and aggregation for the real-history process trace."""
from __future__ import annotations

import csv
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path

from analyze_continuation import verify_source_receipts
from src.checker import Checker
from src.continuation_checker import ContinuationChecker
from src.corpus import build
from src.real_history import build_real_history

ROOT = Path(__file__).resolve().parent
POLICIES = ("continuation", "overlay", "cut-cache", "unsafe-stale")


def load(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def verify(trace: dict, fixture: dict) -> tuple[Counter, Counter]:
    source, evidence, _, _ = verify_source_receipts(trace, fixture["payloads"])
    checker = Checker(fixture["payloads"])
    continuation = ContinuationChecker(fixture["payloads"])
    owners = [[0, 1], [2, 3], [4, 5]]
    queries = {query["id"]: query for query in fixture["queries"]}
    checked = Counter()

    installed = trace["setup"]["installed"]
    if set(installed) != set(queries):
        raise AssertionError("acquisition/query inventory mismatch")
    for query_id, specs in installed.items():
        query = queries[query_id]
        if set(specs) != {"0", "1", "2"}:
            raise AssertionError("incomplete initial token inventory")
        for spec in specs.values():
            continuation.install_prefix(
                query,
                evidence[spec["receipt"]],
                owners,
                0,
                spec["capacity"],
            )
            checked["installed_prefixes"] += 1

    records = defaultdict(list)
    for record in trace["records"]:
        records[record["sequence"]].append(record)
    previous = {
        query["id"]: checker.oracle(fixture["initial"], fixture["history"], [0, 0, 0], query)
        for query in fixture["queries"]
    }
    for moment in trace["timeline"]:
        sequence = moment["sequence"]
        cut = moment["cut"]
        continuation.accept_events(evidence[moment["feed"]["receipt"]], owners, 0)
        checked["feed_receipts"] += 1
        if len(records[sequence]) != len(fixture["queries"]):
            raise AssertionError("query count at commit")
        for record in records[sequence]:
            query = record["query"]
            if query != queries[query["id"]]:
                raise AssertionError("query fixture mismatch")
            oracle = checker.oracle(fixture["initial"], fixture["history"], cut, query)
            if record["oracle"] != oracle:
                raise AssertionError("oracle mismatch")
            if record["oracle_changed"] != (oracle != previous[query["id"]]):
                raise AssertionError("oracle change marker")
            previous[query["id"]] = oracle
            cont = record["observations"]["continuation"]["response"]
            continuation.check_result(
                cont,
                query,
                cut,
                0,
                owners,
                evidence,
                cont.get("repairs", {}),
            )
            checked["continuation_results"] += 1
            for policy in ("overlay", "cut-cache"):
                response = record["observations"][policy]["response"]
                checker.check_overlay(response, evidence, query, cut, 0, owners)
                checked[policy + "_results"] += 1
            for policy in POLICIES:
                observation = record["observations"][policy]
                response = observation["response"]
                metrics = observation["metrics"]
                rows = response["rows"]
                claimed = (
                    response.get("status") == "complete"
                    if policy == "continuation"
                    else response.get("claim_complete", False)
                )
                exact = rows == oracle
                unsound = checker.sound_rows(
                    rows, fixture["initial"], fixture["history"], cut, query
                )
                if metrics["exact"] != exact:
                    raise AssertionError("metric exact")
                if metrics["claimed_complete"] != claimed:
                    raise AssertionError("metric complete")
                if metrics["false_complete"] != (claimed and not exact):
                    raise AssertionError("metric false complete")
                if metrics["unsound_rows"] != unsound:
                    raise AssertionError("metric unsound")
                if policy != "unsafe-stale" and (not exact or not claimed or unsound):
                    raise AssertionError("safe policy failed real-history replay")
                checked[policy + "_observations"] += 1

    probe = trace["fault_probe"]
    probe_checker = ContinuationChecker(fixture["payloads"])
    probe_checker.reset_epoch(0, probe["cut"])
    for spec in probe["blocked_specs"].values():
        probe_checker.install_prefix(
            probe["query"], evidence[spec["receipt"]], owners, 0, spec["capacity"]
        )
    probe_checker.check_result(
        probe["partial"],
        probe["query"],
        probe["cut"],
        0,
        owners,
        evidence,
        probe["partial_repairs"],
    )
    checker.check_overlay(
        probe["overlay_partial"], evidence, probe["query"], probe["cut"], 0, owners
    )
    for spec in probe["healed_specs"].values():
        probe_checker.install_prefix(
            probe["query"], evidence[spec["receipt"]], owners, 0, spec["capacity"]
        )
    probe_checker.check_result(
        probe["healed"],
        probe["query"],
        probe["cut"],
        0,
        owners,
        evidence,
        probe["healed_repairs"],
    )
    oracle = checker.oracle(
        fixture["initial"], fixture["history"], probe["cut"], probe["query"]
    )
    if probe["exact"] != oracle or probe["healed"]["rows"] != oracle:
        raise AssertionError("fault-probe oracle")
    if probe["partial"]["status"] != "partial" or 0 not in probe["partial"]["missing_shards"]:
        raise AssertionError("fault-probe partial status")
    if probe["overlay_partial"]["claim_complete"]:
        raise AssertionError("fault-probe overlay status")
    checked["fault_probes"] += 1

    recovery = trace["recovery"]
    if recovery["distinct_initial_processes"] != 6 or recovery["distinct_final_processes"] != 6:
        raise AssertionError("process isolation evidence")
    if not recovery["killed_pid_replaced"] or not recovery["restarted_pid_is_new"]:
        raise AssertionError("process replacement evidence")
    if not all(item["exact"] for item in recovery["convergence"]):
        raise AssertionError("process convergence evidence")
    checked["process_terminations"] += 1
    checked["converged_processes"] += len(recovery["convergence"])
    return source, checked


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise ValueError("rows required")
    fields = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def aggregate(trace: dict, source: Counter, checked: Counter) -> dict:
    policies = {}
    queries = len(trace["records"])
    for policy in POLICIES:
        metrics = trace["policy_metrics"][policy]
        costs = trace["policy_costs"][policy]
        item = {**metrics}
        item.update(
            {
                "total_source_bytes": costs["bytes"],
                "total_source_messages": costs["messages"],
                "mean_source_bytes": costs["bytes"] / queries,
                "mean_source_messages": costs["messages"] / queries,
                "dropped_requests_or_replies": costs["dropped_requests_or_replies"],
            }
        )
        if policy in trace["peak_logical_state_bytes"]:
            item["max_logical_state_bytes"] = trace["peak_logical_state_bytes"][policy]
        policies[policy] = item
    cont = policies["continuation"]
    overlay = policies["overlay"]
    cut_cache = policies["cut-cache"]
    changed = sorted(
        {
            record["query"]["id"]
            for record in trace["records"]
            if record["oracle_changed"]
        }
    )
    assignments = sum(
        len(event["changes"]) for shard in trace["history"] for event in shard
    )
    summary = {
        "schema": trace["schema"],
        "repository": trace["git_history"]["repository"],
        "base_commit": trace["git_history"]["base"]["commit"],
        "observed_head_commit": trace["git_history"]["observed_head"]["commit"],
        "selected_commits": len(trace["git_history"]["stages"]),
        "function_assignments": assignments,
        "base_queries": trace["base_query_count"],
        "targeted_queries": len(trace["targeted_queries"]),
        "queries_per_policy": queries,
        "oracle_change_observations": sum(
            record["oracle_changed"] for record in trace["records"]
        ),
        "queries_with_any_oracle_change": changed,
        "policies": policies,
        "communication_reduction_vs_overlay_percent": 100
        * (1 - cont["mean_source_bytes"] / overlay["mean_source_bytes"]),
        "communication_reduction_vs_cut_cache_percent": 100
        * (1 - cont["mean_source_bytes"] / cut_cache["mean_source_bytes"]),
        "source_replay": dict(source),
        "checker_replay": dict(checked),
        "fault_probe": {
            "partial_status": trace["fault_probe"]["partial_status"],
            "missing_shards": trace["fault_probe"]["missing_shards"],
            "overlay_claim_complete": trace["fault_probe"]["overlay_claim_complete"],
            "healed_status": trace["fault_probe"]["healed_status"],
            "healed_exact": trace["fault_probe"]["healed_exact"],
        },
        "process_recovery": {
            key: trace["recovery"][key]
            for key in (
                "distinct_initial_processes",
                "distinct_final_processes",
                "killed_pid_replaced",
                "restarted_pid_is_new",
            )
        },
        "maximum_source_process_rss_kib": max(
            item["rss_kib"] for item in trace["recovery"]["process_stats"]
        ),
        "coordinator_peak_rss_kib": trace["resources"]["coordinator_peak_rss_kib"],
        "wall_seconds": trace["resources"]["wall_seconds"],
        "scope": trace["scope"],
        "cost_scope": trace["cost_scope"],
    }
    return summary


def write_paper_outputs(summary: dict) -> None:
    paper = ROOT.parent / "paper"
    if not paper.is_dir():
        return
    tables = paper / "tables"
    tables.mkdir(exist_ok=True)
    p = summary["policies"]
    macros = {
        "RealCommits": summary["selected_commits"],
        "RealFunctionAssignments": summary["function_assignments"],
        "RealQueries": summary["queries_per_policy"],
        "RealBaseQueries": summary["base_queries"],
        "RealTargetedQueries": summary["targeted_queries"],
        "RealChangedObservations": summary["oracle_change_observations"],
        "RealContinuationBytes": p["continuation"]["mean_source_bytes"],
        "RealOverlayBytes": p["overlay"]["mean_source_bytes"],
        "RealCutCacheBytes": p["cut-cache"]["mean_source_bytes"],
        "RealOverlayReduction": summary["communication_reduction_vs_overlay_percent"],
        "RealCutCacheReduction": summary["communication_reduction_vs_cut_cache_percent"],
        "RealUnsafeFalse": p["unsafe-stale"]["false_complete"],
        "RealUnsafeUnsound": p["unsafe-stale"]["unsound_rows"],
        "RealSourceProcesses": summary["process_recovery"]["distinct_final_processes"],
        "RealContinuationStateKiB": p["continuation"]["max_logical_state_bytes"] / 1024,
        "RealCutCacheStateKiB": p["cut-cache"]["max_logical_state_bytes"] / 1024,
    }
    lines = ["% Generated by artifact/analyze_real_history.py; do not edit."]
    for name, value in macros.items():
        if isinstance(value, int):
            rendered = f"{value:,}"
        elif name.endswith("Reduction"):
            rendered = f"{value:.1f}"
        else:
            rendered = f"{value:.1f}"
        lines.append(f"\\newcommand{{\\{name}}}{{{rendered}}}")
    (tables / "generated-real-history-macros.tex").write_text("\n".join(lines) + "\n")

    labels = {
        "continuation": "Continuation",
        "overlay": "Exact overlay",
        "cut-cache": "Cut-aware cache",
        "unsafe-stale": "Unsafe stale",
    }
    rows = [
        "\\begin{tabular}{lrrrr}",
        "\\toprule",
        "Policy & Exact & False complete & B/query & Msg/query \\\\",
        "\\midrule",
    ]
    for policy in POLICIES:
        item = p[policy]
        rows.append(
            f"{labels[policy]} & {item['exact']:,} & {item['false_complete']:,} & "
            f"{item['mean_source_bytes']:.1f} & {item['mean_source_messages']:.2f} \\\\" 
        )
    rows += ["\\bottomrule", "\\end{tabular}"]
    (tables / "generated-real-history.tex").write_text("\n".join(rows) + "\n")


def main() -> None:
    path = ROOT / "results/real-history-trace.json.gz"
    trace = load(path)
    fixture = build_real_history(ROOT, build(ROOT))
    if trace["git_history"] != fixture["git_history"]:
        raise AssertionError("real-history provenance changed")
    if trace["history"] != fixture["history"]:
        raise AssertionError("real-history events changed")
    source, checked = verify(trace, fixture)
    summary = aggregate(trace, source, checked)
    output = ROOT / "results/real-history-summary.json"
    output.write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    rows = []
    for policy, item in summary["policies"].items():
        rows.append({"policy": policy, **item})
    write_csv(ROOT / "results/real-history-policies.csv", rows)
    write_paper_outputs(summary)
    print(
        json.dumps(
            {
                "status": "PASS",
                "summary": str(output.relative_to(ROOT)),
                "queries": summary["queries_per_policy"],
                "source_replay": summary["source_replay"],
                "checker_replay": summary["checker_replay"],
                "false_complete": {
                    policy: item["false_complete"]
                    for policy, item in summary["policies"].items()
                },
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
