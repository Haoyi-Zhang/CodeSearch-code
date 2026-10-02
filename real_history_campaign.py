#!/usr/bin/env python3
"""Real-commit and six-process validation for continuation certificates."""
from __future__ import annotations

import asyncio
import copy
import gzip
import json
import os
import resource
import time
from pathlib import Path

from continuation_campaign import (
    add_cost,
    assess,
    cache_state,
    cut_cache_query,
    difference,
    empty_total,
)
from src.checker import Checker
from src.continuation_checker import ContinuationChecker
from src.continuation_coordinator import ContinuationSession, FeedStore, fetch_events
from src.coordinator import baseline
from src.corpus import build
from src.model import apply
from src.process_service import ProcessNetwork
from src.real_history import build_real_history
from src.service import wire

ROOT = Path(__file__).resolve().parent
POLICIES = ("continuation", "overlay", "cut-cache", "unsafe-stale")


def _metric_total() -> dict:
    return {
        "queries": 0,
        "exact": 0,
        "claimed_complete": 0,
        "false_complete": 0,
        "unsound_rows": 0,
        "returned_rows": 0,
        "partial_or_unjustified": 0,
        "changed_oracle_queries": 0,
    }


def _add_metric(total: dict, metrics: dict, changed: bool) -> None:
    total["queries"] += 1
    total["exact"] += int(metrics["exact"])
    total["claimed_complete"] += int(metrics["claimed_complete"])
    total["false_complete"] += int(metrics["false_complete"])
    total["unsound_rows"] += metrics["unsound_rows"]
    total["returned_rows"] += metrics["returned_rows"]
    total["partial_or_unjustified"] += int(not metrics["claimed_complete"])
    total["changed_oracle_queries"] += int(changed)


async def run(output: Path) -> dict:
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    fixture = build_real_history(ROOT, build(ROOT))
    checker = Checker(fixture["payloads"])
    continuation_checker = ContinuationChecker(fixture["payloads"])
    owners = [[0, 1], [2, 3], [4, 5]]
    epoch = 0
    cut = [0, 0, 0]
    feed = FeedStore(epoch, cut)
    session = ContinuationSession(fixture["payloads"], feed, base_capacity=5)
    cut_cache: dict = {}
    totals = {policy: empty_total() for policy in POLICIES}
    metric_totals = {policy: _metric_total() for policy in POLICIES}
    peak_state = {"continuation": 0, "cut-cache": 0}
    records = []
    timeline = []
    setup = {}
    recovery = {}
    query_ids = [query["id"] for query in fixture["queries"]]
    previous_oracles = {
        query["id"]: checker.oracle(fixture["initial"], fixture["history"], cut, query)
        for query in fixture["queries"]
    }
    unsafe_cache = copy.deepcopy(previous_oracles)

    async with ProcessNetwork(fixture["initial"], fixture["payloads"]) as network:
        network.delay_ms = {node: node % 3 for node in range(6)}
        initial_pids = network.alive_pids()

        before = network.counters()
        wall = time.monotonic_ns()
        installed = {}
        check_ns = 0
        for query in fixture["queries"]:
            specs = await session.acquire(network, query, cut, epoch, owners)
            if set(specs) != {"0", "1", "2"}:
                raise AssertionError("initial real-history token acquisition incomplete")
            installed[query["id"]] = specs
            for spec in specs.values():
                stamp = time.monotonic_ns()
                continuation_checker.install_prefix(
                    query,
                    network.evidence[spec["receipt"]],
                    owners,
                    epoch,
                    spec["capacity"],
                )
                check_ns += time.monotonic_ns() - stamp
        elapsed = time.monotonic_ns() - wall
        setup_cost = difference(network.counters(), before)
        add_cost(totals["continuation"], setup_cost, elapsed, check_ns)
        setup = {
            "queries": len(installed),
            "receipts": sum(len(value) for value in installed.values()),
            "installed": installed,
            "cost": {**setup_cost, "wall_ns": elapsed, "checker_ns": check_ns},
        }
        peak_state["continuation"] = len(
            wire({"tokens": session.tokens, "feed": feed.logical_state()})
        )

        for sequence, event in enumerate(fixture["history"][1], 1):
            control_before = network.counters()
            control_wall = time.monotonic_ns()
            failure = None
            if sequence == 3:
                killed = await network.kill(2)
                failure = {"action": "terminate", "node": 2, "pid": killed}
                recovery["terminated_pid"] = killed
            if sequence == 4:
                restarted = await network.restart(2)
                for previous in fixture["history"][1][: sequence - 1]:
                    _, reply = await network.rpc(
                        2, {"op": "receive", "event": previous}, administrative=True
                    )
                    if not reply or not reply.get("ok"):
                        raise AssertionError("restart history replay failed")
                _, reply = await network.rpc(
                    2, {"op": "advance", "to": sequence - 1}, administrative=True
                )
                if not reply or reply.get("index_at") != sequence - 1:
                    raise AssertionError("restart index replay failed")
                failure = {"action": "restart-and-replay", "node": 2, "pid": restarted}
                recovery["restarted_pid"] = restarted

            for node in owners[1]:
                _, reply = await network.rpc(node, {"op": "receive", "event": event})
                if node == 3 or sequence != 3:
                    if not reply or not reply.get("ok"):
                        raise AssertionError(("real-history receive", sequence, node, reply))
                _, reply = await network.rpc(node, {"op": "advance", "to": sequence})
                if node == 3 or sequence != 3:
                    if not reply or reply.get("index_at") != sequence:
                        raise AssertionError(("real-history advance", sequence, node, reply))
            cut = [0, sequence, 0]
            control_cost = difference(network.counters(), control_before)
            control_elapsed = time.monotonic_ns() - control_wall

            feed_before = network.counters()
            feed_wall = time.monotonic_ns()
            receipt, reply = await fetch_events(network, feed, 1, sequence, epoch, owners)
            if reply is None or receipt is None:
                raise AssertionError("real-history shared feed unavailable")
            stamp = time.monotonic_ns()
            continuation_checker.accept_events(network.evidence[receipt], owners, epoch)
            feed_check_ns = time.monotonic_ns() - stamp
            feed_elapsed = time.monotonic_ns() - feed_wall
            feed_cost = difference(network.counters(), feed_before)
            add_cost(totals["continuation"], feed_cost, feed_elapsed, feed_check_ns)

            changed_queries = []
            tick_records = []
            for query_index, query in enumerate(fixture["queries"]):
                exact = checker.oracle(fixture["initial"], fixture["history"], cut, query)
                changed = exact != previous_oracles[query["id"]]
                if changed:
                    changed_queries.append(query["id"])
                previous_oracles[query["id"]] = exact
                observations = {}
                order = ["continuation", "overlay", "cut-cache"]
                shift = (sequence + query_index) % len(order)
                order = order[shift:] + order[:shift]
                for policy in order:
                    before = network.counters()
                    wall = time.monotonic_ns()
                    check_ns = 0
                    if policy == "continuation":
                        response, repairs = await session.query(
                            network, query, cut, epoch, owners
                        )
                        stamp = time.monotonic_ns()
                        continuation_checker.check_result(
                            response,
                            query,
                            cut,
                            epoch,
                            owners,
                            network.evidence,
                            repairs,
                        )
                        check_ns = time.monotonic_ns() - stamp
                        rows = response["rows"]
                        claimed = response["status"] == "complete"
                    elif policy == "overlay":
                        response = await baseline(
                            network, query, cut, epoch, owners, "overlay"
                        )
                        stamp = time.monotonic_ns()
                        checker.check_overlay(
                            response, network.evidence, query, cut, epoch, owners
                        )
                        check_ns = time.monotonic_ns() - stamp
                        rows = response["rows"]
                        claimed = response["claim_complete"]
                    else:
                        response = await cut_cache_query(
                            network, cut_cache, query, cut, epoch, owners
                        )
                        stamp = time.monotonic_ns()
                        checker.check_overlay(
                            response, network.evidence, query, cut, epoch, owners
                        )
                        check_ns = time.monotonic_ns() - stamp
                        rows = response["rows"]
                        claimed = response["claim_complete"]
                    elapsed = time.monotonic_ns() - wall
                    delta = difference(network.counters(), before)
                    add_cost(totals[policy], delta, elapsed, check_ns)
                    metrics = assess(
                        rows,
                        exact,
                        checker,
                        fixture,
                        fixture["history"],
                        cut,
                        query,
                        claimed,
                    )
                    metrics.update(
                        {
                            **delta,
                            "wall_ns": elapsed,
                            "checker_ns": check_ns,
                            "certificate_bytes": len(wire(response)),
                        }
                    )
                    if metrics["unsound_rows"] or metrics["false_complete"]:
                        raise AssertionError((sequence, query["id"], policy, metrics))
                    _add_metric(metric_totals[policy], metrics, changed)
                    observations[policy] = {"metrics": metrics, "response": response}

                stale_rows = copy.deepcopy(unsafe_cache[query["id"]])
                stale = assess(
                    stale_rows,
                    exact,
                    checker,
                    fixture,
                    fixture["history"],
                    cut,
                    query,
                    True,
                )
                stale.update(
                    {
                        "bytes": 0,
                        "messages": 0,
                        "virtual_ms": 0,
                        "server_cpu_ns": 0,
                        "dropped_requests_or_replies": 0,
                        "wall_ns": 0,
                        "checker_ns": 0,
                        "certificate_bytes": 0,
                    }
                )
                _add_metric(metric_totals["unsafe-stale"], stale, changed)
                observations["unsafe-stale"] = {
                    "metrics": stale,
                    "response": {"rows": stale_rows, "claim_complete": True},
                }
                record = {
                    "sequence": sequence,
                    "commit": event["commit"],
                    "query": query,
                    "cut": list(cut),
                    "oracle": exact,
                    "oracle_changed": changed,
                    "observations": observations,
                }
                records.append(record)
                tick_records.append(query["id"])

            peak_state["continuation"] = max(
                peak_state["continuation"],
                len(wire({"tokens": session.tokens, "feed": feed.logical_state()})),
            )
            peak_state["cut-cache"] = max(
                peak_state["cut-cache"], len(wire(cache_state(cut_cache)))
            )
            timeline.append(
                {
                    "sequence": sequence,
                    "commit": event["commit"],
                    "cut": list(cut),
                    "changed_queries": changed_queries,
                    "queried": tick_records,
                    "failure": failure,
                    "alive_pids": network.alive_pids(),
                    "control": {**control_cost, "wall_ns": control_elapsed},
                    "feed": {
                        **feed_cost,
                        "wall_ns": feed_elapsed,
                        "checker_ns": feed_check_ns,
                        "receipt": receipt,
                    },
                }
            )

        # Fail-closed probe: both owners of one shard are unavailable during
        # cold acquisition.  Healing restores a complete exact result.
        probe_query = copy.deepcopy(fixture["targeted_queries"][0])
        probe_query["id"] = "real-history-unavailable-shard-probe"
        probe_session = ContinuationSession(fixture["payloads"], feed, base_capacity=5)
        probe_checker = ContinuationChecker(fixture["payloads"])
        probe_checker.reset_epoch(epoch, cut)
        network.blocked = {0, 1}
        probe_before = network.counters()
        specs = await probe_session.acquire(network, probe_query, cut, epoch, owners)
        for spec in specs.values():
            probe_checker.install_prefix(
                probe_query,
                network.evidence[spec["receipt"]],
                owners,
                epoch,
                spec["capacity"],
            )
        partial, repairs = await probe_session.query(
            network, probe_query, cut, epoch, owners
        )
        probe_checker.check_result(
            partial,
            probe_query,
            cut,
            epoch,
            owners,
            network.evidence,
            repairs,
        )
        overlay_partial = await baseline(
            network, probe_query, cut, epoch, owners, "overlay"
        )
        checker.check_overlay(
            overlay_partial, network.evidence, probe_query, cut, epoch, owners
        )
        if partial["status"] != "partial" or 0 not in partial["missing_shards"]:
            raise AssertionError("continuation did not fail closed under owner unavailability")
        if overlay_partial["claim_complete"]:
            raise AssertionError("overlay falsely claimed completeness under owner unavailability")
        blocked_cost = difference(network.counters(), probe_before)
        network.blocked = set()
        healed_specs = await probe_session.acquire(network, probe_query, cut, epoch, owners)
        for spec in healed_specs.values():
            probe_checker.install_prefix(
                probe_query,
                network.evidence[spec["receipt"]],
                owners,
                epoch,
                spec["capacity"],
            )
        healed, repairs = await probe_session.query(
            network, probe_query, cut, epoch, owners
        )
        probe_checker.check_result(
            healed,
            probe_query,
            cut,
            epoch,
            owners,
            network.evidence,
            repairs,
        )
        exact_probe = checker.oracle(
            fixture["initial"], fixture["history"], cut, probe_query
        )
        if healed["status"] != "complete" or healed["rows"] != exact_probe:
            raise AssertionError("healed cold acquisition did not become exact")
        fault_probe = {
            "query": probe_query,
            "cut": list(cut),
            "blocked_nodes": [0, 1],
            "blocked_specs": specs,
            "partial": partial,
            "partial_repairs": repairs if partial.get("status") == "complete" else {},
            "overlay_partial": overlay_partial,
            "healed_specs": healed_specs,
            "healed": healed,
            "healed_repairs": repairs,
            "exact": exact_probe,
            "partial_status": partial["status"],
            "missing_shards": partial["missing_shards"],
            "overlay_claim_complete": overlay_partial["claim_complete"],
            "healed_status": healed["status"],
            "healed_exact": healed["rows"] == exact_probe,
            "blocked_cost": blocked_cost,
        }

        # Every restarted source is replayed to the final commit and every
        # source process must expose the exact independently reconstructed state.
        expected = [dict(state) for state in fixture["initial"]]
        for shard in range(3):
            for event in fixture["history"][shard]:
                apply(expected[shard], event)
        convergence = []
        process_stats = []
        for node in range(6):
            shard = node // 2
            at = cut[shard]
            _, snapshot = await network.rpc(
                node,
                {"op": "snapshot", "shard": shard, "epoch": epoch, "at": at},
                administrative=True,
            )
            if snapshot is None or dict(snapshot.get("state", [])) != expected[shard]:
                raise AssertionError(("final process convergence", node, snapshot))
            _, stat = await network.rpc(
                node, {"op": "process-stat"}, administrative=True
            )
            if stat is None:
                raise AssertionError("missing process stat")
            process_stats.append(stat)
            convergence.append(
                {"node": node, "pid": stat["pid"], "shard": shard, "at": at, "exact": True}
            )
        final_pids = network.alive_pids()
        if len(final_pids) != 6 or len(set(final_pids)) != 6:
            raise AssertionError("six distinct source processes not alive at final cut")
        recovery.update(
            {
                "initial_pids": initial_pids,
                "final_pids": final_pids,
                "distinct_initial_processes": len(set(initial_pids)),
                "distinct_final_processes": len(set(final_pids)),
                "killed_pid_replaced": recovery.get("terminated_pid")
                not in final_pids,
                "restarted_pid_is_new": recovery.get("restarted_pid")
                not in initial_pids,
                "convergence": convergence,
                "process_stats": process_stats,
            }
        )

        trace = {
            "schema": "real-history-six-process-v1",
            "git_history": fixture["git_history"],
            "query_ids": query_ids,
            "base_query_count": fixture["base_query_count"],
            "targeted_queries": fixture["targeted_queries"],
            "initial": fixture["initial"],
            "history": fixture["history"],
            "records": records,
            "timeline": timeline,
            "setup": setup,
            "policy_costs": totals,
            "policy_metrics": metric_totals,
            "peak_logical_state_bytes": peak_state,
            "fault_probe": fault_probe,
            "recovery": recovery,
            "transcript": network.evidence,
            "attempts": network.attempts,
            "resources": {
                "wall_seconds": time.monotonic() - started_wall,
                "coordinator_cpu_seconds": time.process_time() - started_cpu,
                "coordinator_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "source_processes": 6,
                "coordinator_processes": 1,
                "start_method": network.start_method,
                "loopback_host": "127.0.0.1",
            },
            "scope": (
                "six independent OS source processes on one host; fixed function-level projection "
                "of six published cachetools commits; 64 disjoint corpus queries plus four "
                "deterministic change-targeted queries; not a user query log or multi-host WAN test"
            ),
            "cost_scope": (
                "source RPC bytes; continuation includes cold token acquisition and one shared event "
                "feed; dissemination control, result delivery, and source-body transport excluded"
            ),
        }

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8") as stream:
        json.dump(trace, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
    os.replace(temporary, output)
    print(
        json.dumps(
            {
                "status": "PASS",
                "trace": str(output.relative_to(ROOT)),
                "queries_per_policy": len(records),
                "commits": len(fixture["history"][1]),
                "source_processes": 6,
                "wall_seconds": trace["resources"]["wall_seconds"],
                "false_complete": {
                    policy: metric_totals[policy]["false_complete"] for policy in POLICIES
                },
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return trace


if __name__ == "__main__":
    asyncio.run(run(ROOT / "results/real-history-trace.json.gz"))
