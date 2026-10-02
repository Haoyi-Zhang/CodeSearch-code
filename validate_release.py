#!/usr/bin/env python3
"""Offline release validator for the retained research packet.

The validator checks internal evidence closure rather than scientific novelty or
production validity.  It intentionally performs no network access and uses only
the Python standard library.  When the sibling paper directory is supplied, it
also cross-checks every citation key and bibliographic record in the manuscript.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
MIN_REFERENCES = 60
EXPECTED_REFERENCES = 67
EXPECTED_PROJECT_ENTRIES = {"paper", "artifact", "README.md"}
SAFE_CONTINUATION_POLICIES = ("continuation", "overlay", "cut-cache", "mirror")
SAFE_REAL_HISTORY_POLICIES = ("continuation", "overlay", "cut-cache")
ALLOWED_REFERENCE_DOMAINS = {
    "doi.org",
    "www.usenix.org",
    "www.vldb.org",
    "www.cidrdb.org",
    "www.microsoft.com",
    "arxiv.org",
    "openreview.net",
}
FORBIDDEN_NAMES = {
    ".git", ".hg", ".svn", "__pycache__", ".pytest_cache", ".mypy_cache",
}
FORBIDDEN_PUBLICATION_SUBSTRINGS = (
    "PROMPT", "TEMPLATE.zip", "chat", "conversation", "anonymous.4open.science",
    "github.com/REPLACE", "example.com/repository",
)


class ValidationError(AssertionError):
    """A release invariant did not hold."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def read_csv(path: Path) -> list[dict[str, str]]:
    require(path.is_file(), f"missing CSV: {path}")
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    require(bool(rows), f"empty CSV: {path}")
    return rows


def load_json(path: Path) -> object:
    require(path.is_file(), f"missing JSON: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_title(value: str) -> str:
    value = value.replace("\\'e", "e").replace("\\'", "")
    value = re.sub(r"\\[A-Za-z]+", " ", value)
    value = value.replace("$", " ").replace("{", " ").replace("}", " ")
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(value.split())


def parse_bib(path: Path) -> dict[str, dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    entries: dict[str, dict[str, str]] = {}
    position = 0
    while True:
        match = re.search(r"(?m)^@(\w+)\{([^,]+),", text[position:])
        if not match:
            break
        start = position + match.start()
        body_start = position + match.end()
        depth = 1
        index = body_start
        while index < len(text) and depth:
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
            index += 1
        require(depth == 0, f"unbalanced BibTeX entry near {match.group(2)}")
        key = match.group(2).strip()
        require(key not in entries, f"duplicate BibTeX key: {key}")
        body = text[body_start:index - 1]
        fields: dict[str, str] = {"entry_type": match.group(1).lower()}
        field_pattern = re.compile(r"(?ms)^\s*(\w+)\s*=\s*\{(.*?)\}\s*,?\s*(?=\n\s*\w+\s*=|\Z)")
        for field in field_pattern.finditer(body):
            fields[field.group(1).lower()] = re.sub(r"\s+", " ", field.group(2).strip())
        entries[key] = fields
        position = index
    return entries


def manuscript_citations(paper_dir: Path) -> list[str]:
    keys: list[str] = []
    for path in sorted(paper_dir.rglob("*.tex")):
        if path.name == "references.tex":
            continue
        for match in re.finditer(r"\\cite\{([^}]+)\}", path.read_text(encoding="utf-8")):
            keys.extend(part.strip() for part in match.group(1).split(",") if part.strip())
    return keys


def validate_references(paper_dir: Path | None) -> dict[str, object]:
    audit_rows = read_csv(ROOT / "docs/reference-audit.csv")
    verification_rows = read_csv(ROOT / "docs/reference-verification.csv")
    audit = {row["key"]: row for row in audit_rows}
    verification = {row["key"]: row for row in verification_rows}
    require(len(audit) == len(audit_rows), "duplicate key in reference-audit.csv")
    require(len(verification) == len(verification_rows), "duplicate key in reference-verification.csv")
    require(len(audit) >= MIN_REFERENCES, f"fewer than {MIN_REFERENCES} references")
    require(len(audit) == EXPECTED_REFERENCES, f"expected {EXPECTED_REFERENCES} references, found {len(audit)}")
    require(set(audit) == set(verification), "reference audit/verification key sets differ")

    category_counts = Counter(row["category"] for row in audit_rows)
    expected_categories = {
        "top-k and continuous queries": 10,
        "incremental maintenance and caching": 14,
        "replication, consistency, and logs": 24,
        "federated retrieval and code search": 12,
        "authenticated and verifiable queries": 7,
    }
    require(category_counts == expected_categories, f"reference category counts changed: {dict(category_counts)}")
    depth_counts = Counter(row["verification_depth"] for row in audit_rows)
    require(sum("complete-main-text" in key for key in depth_counts for _ in range(depth_counts[key])) == 21,
            f"expected 21 complete-main-text calibration records: {dict(depth_counts)}")
    require(sum("no complete-paper claim" in key for key in depth_counts for _ in range(depth_counts[key])) == 46,
            f"expected 46 record/selected-passage checks: {dict(depth_counts)}")

    titles: dict[str, str] = {}
    identifiers: dict[str, str] = {}
    basis_counts: Counter[str] = Counter()
    for key in sorted(audit):
        row = audit[key]
        verified = verification[key]
        require(verified["verification_status"] == "record-linked-and-locally-consistent",
                f"reference not verified: {key}")
        require(verified["metadata_match"] == "title; year; venue; author list",
                f"incomplete metadata match declaration: {key}")
        require(verified["checked_on"] == "2026-09-18", f"inherited reference ledger date changed: {key}")
        require(row["title"] == verified["title"], f"title differs across ledgers: {key}")
        require(row["year"] == verified["year"], f"year differs across ledgers: {key}")
        require(row["venue"] == verified["venue"], f"venue differs across ledgers: {key}")
        require(row["stable_source"] == verified["authoritative_url"], f"source differs across ledgers: {key}")
        url = verified["authoritative_url"]
        parsed = urlparse(url)
        require(parsed.scheme == "https", f"reference source is not HTTPS: {key}: {url}")
        require(parsed.netloc in ALLOWED_REFERENCE_DOMAINS,
                f"reference source is not an approved scholarly/official domain: {key}: {parsed.netloc}")
        title_norm = normalize_title(row["title"])
        require(title_norm not in titles, f"duplicate normalized title: {key} and {titles.get(title_norm)}")
        titles[title_norm] = key
        identifier = verified["persistent_identifier"]
        require(identifier, f"missing persistent identifier/official record: {key}")
        ident_norm = identifier.lower()
        require(ident_norm not in identifiers, f"duplicate identifier: {key} and {identifiers.get(ident_norm)}")
        identifiers[ident_norm] = key
        require(verified["authors"].strip(), f"missing author list: {key}")
        require("others" not in verified["authors"].lower(), f"abbreviated author list: {key}")
        basis_counts[verified["authoritative_record_type"]] += 1

    require(sum(basis_counts.values()) == EXPECTED_REFERENCES, "reference basis count mismatch")

    result: dict[str, object] = {
        "references": len(audit),
        "minimum_required": MIN_REFERENCES,
        "category_counts": dict(sorted(category_counts.items())),
        "verification_basis_counts": dict(sorted(basis_counts.items())),
        "complete_main_text_records": 21,
        "record_or_selected_passage_checks": 46,
    }

    if paper_dir is not None:
        require(paper_dir.is_dir(), f"paper directory not found: {paper_dir}")
        bib = parse_bib(paper_dir / "references.bib")
        rendered_text = (paper_dir / "main.bbl").read_text(encoding="utf-8")
        bibitems = re.findall(r"\\bibitem(?:\[[\s\S]*?\])?\s*%?\s*\{([^}]+)\}", rendered_text)
        citations = manuscript_citations(paper_dir)
        require(len(bib) == EXPECTED_REFERENCES, f"BibTeX count is {len(bib)}")
        require(len(bibitems) == EXPECTED_REFERENCES, f"rendered bibliography count is {len(bibitems)}")
        require(len(set(bibitems)) == len(bibitems), "duplicate rendered bibliography key")
        require(set(bib) == set(audit), "BibTeX/audit key sets differ")
        require(set(bibitems) == set(audit), "rendered bibliography/audit key sets differ")
        require(set(citations) == set(audit), "manuscript citation/reference key sets differ")
        require(len(citations) >= len(audit), "missing manuscript citation occurrences")
        require("and others" not in (paper_dir / "references.bib").read_text(encoding="utf-8").lower(),
                "abbreviated BibTeX author list remains")
        require("\\bibitem" in rendered_text, "ACM bibliography was not built")
        for key, fields in bib.items():
            for required in ("author", "title", "year", "url"):
                require(fields.get(required, "").strip(), f"BibTeX {key} lacks {required}")
            require(fields["year"] == audit[key]["year"], f"BibTeX/audit year differs: {key}")
            require(fields["url"] == audit[key]["stable_source"], f"BibTeX/audit URL differs: {key}")
            require(fields["author"] == verification[key]["authors"],
                    f"BibTeX/verification author list differs: {key}")
            require(normalize_title(fields["title"]) == normalize_title(audit[key]["title"]),
                    f"BibTeX/audit title differs: {key}")
        result.update({
            "bibtex_records": len(bib),
            "rendered_bibitems": len(bibitems),
            "unique_cited_keys": len(set(citations)),
            "citation_key_occurrences": len(citations),
        })
    return result



def parse_tex_macros(path: Path) -> dict[str, str]:
    require(path.is_file(), f"missing generated TeX macro file: {path}")
    macros: dict[str, str] = {}
    for name, value in re.findall(
            r"\\newcommand\{\\([A-Za-z][A-Za-z0-9]*)\}\{([^{}]*)\}",
            path.read_text(encoding="utf-8")):
        require(name not in macros, f"duplicate TeX macro: {name}")
        macros[name] = value
    require(bool(macros), f"no generated TeX macros found: {path}")
    return macros


def validate_paper_results(paper_dir: Path) -> dict[str, object]:
    """Check that manuscript-facing generated numbers match retained JSON.

    This is intentionally independent of ``paper/sync_results.py`` so the
    release gate does not merely trust the generator that produced the files.
    """
    continuation = load_json(ROOT / "results/continuation-summary.json")
    real = load_json(ROOT / "results/real-history-summary.json")
    finite = continuation["finite_checks"]
    policies = continuation["policies"]
    cont = policies["continuation"]
    overlay = policies["overlay"]
    cache = policies["cut-cache"]
    mirror = policies["mirror"]
    unsafe = policies["unsafe-stale"]
    source_receipts = sum(
        continuation["source_replay"][name]
        for name in ("events", "overlay", "prefix", "snapshot")
    )
    finite_total = sum(finite[name] for name in (
        "local_transition_cases", "k_plus_d_repair_cases", "global_merge_cases"
    ))

    def integer(value: object) -> str:
        return f"{int(value):,}"

    expected_continuation: dict[str, str] = {
        "PrimaryQueries": integer(continuation["primary_queries"]),
        "PrimaryTraces": integer(continuation["primary_traces"]),
        "PrimaryComplete": integer(cont["claimed_complete"]),
        "PrimaryExact": integer(cont["exact"]),
        "PrimaryPartial": integer(cont["partial_or_unjustified"]),
        "PrimaryExactUnjustified": integer(cont["exact"] - cont["claimed_complete"]),
        "ContinuationBytes": f"{cont['mean_source_bytes']:.1f}",
        "OverlayBytes": f"{overlay['mean_source_bytes']:.1f}",
        "CutCacheBytes": f"{cache['mean_source_bytes']:.1f}",
        "MirrorBytes": f"{mirror['mean_source_bytes']:.1f}",
        "ContinuationMessages": f"{cont['mean_source_messages']:.2f}",
        "OverlayMessages": f"{overlay['mean_source_messages']:.2f}",
        "ContinuationReduction": f"{continuation['communication_reduction_vs_overlay_percent']:.1f}",
        "ContinuationVsOverlay": f"{overlay['mean_source_bytes']/cont['mean_source_bytes']:.1f}",
        "ContinuationVsCache": f"{cache['mean_source_bytes']/cont['mean_source_bytes']:.1f}",
        "ContinuationVsMirror": f"{mirror['mean_source_bytes']/cont['mean_source_bytes']:.1f}",
        "ContinuationStateKiB": f"{cont['max_logical_state_bytes']/1024:.1f}",
        "CutCacheStateKiB": f"{cache['max_logical_state_bytes']/1024:.1f}",
        "MirrorStateKiB": f"{mirror['max_logical_state_bytes']/1024:.1f}",
        "MirrorStateRatio": f"{mirror['max_logical_state_bytes']/cont['max_logical_state_bytes']:.1f}",
        "RepairQueries": integer(continuation["continuation_repairs"]["queries"]),
        "RepairShards": integer(continuation["continuation_repairs"]["shards"]),
        "RepairRate": f"{100*continuation['continuation_repairs']['queries']/continuation['primary_queries']:.1f}",
        "UnsafeFalse": integer(unsafe["false_complete"]),
        "UnsafeUnsound": integer(unsafe["unsound_rows"]),
        "SourceReceipts": integer(source_receipts),
        "SourceEventRows": integer(continuation["source_replay"]["event_rows"]),
        "FiniteTotal": integer(finite_total),
        "FiniteLocal": integer(finite["local_transition_cases"]),
        "FiniteRepair": integer(finite["k_plus_d_repair_cases"]),
        "FiniteGlobal": integer(finite["global_merge_cases"]),
    }
    actual_continuation = parse_tex_macros(
        paper_dir / "tables/generated-continuation-macros.tex"
    )
    require(actual_continuation == expected_continuation,
            "generated continuation manuscript macros are stale or inconsistent")

    rp = real["policies"]
    expected_real: dict[str, str] = {
        "RealCommits": integer(real["selected_commits"]),
        "RealFunctionAssignments": integer(real["function_assignments"]),
        "RealQueries": integer(real["queries_per_policy"]),
        "RealBaseQueries": integer(real["base_queries"]),
        "RealTargetedQueries": integer(real["targeted_queries"]),
        "RealChangedObservations": integer(real["oracle_change_observations"]),
        "RealContinuationBytes": f"{rp['continuation']['mean_source_bytes']:.1f}",
        "RealOverlayBytes": f"{rp['overlay']['mean_source_bytes']:.1f}",
        "RealCutCacheBytes": f"{rp['cut-cache']['mean_source_bytes']:.1f}",
        "RealOverlayReduction": f"{real['communication_reduction_vs_overlay_percent']:.1f}",
        "RealCutCacheReduction": f"{real['communication_reduction_vs_cut_cache_percent']:.1f}",
        "RealUnsafeFalse": integer(rp["unsafe-stale"]["false_complete"]),
        "RealUnsafeUnsound": integer(rp["unsafe-stale"]["unsound_rows"]),
        "RealSourceProcesses": integer(real["process_recovery"]["distinct_final_processes"]),
        "RealContinuationStateKiB": f"{rp['continuation']['max_logical_state_bytes']/1024:.1f}",
        "RealCutCacheStateKiB": f"{rp['cut-cache']['max_logical_state_bytes']/1024:.1f}",
    }
    actual_real = parse_tex_macros(paper_dir / "tables/generated-real-history-macros.tex")
    require(actual_real == expected_real,
            "generated real-history manuscript macros are stale or inconsistent")

    continuation_report = load_json(ROOT / "results/continuation-clean-reproduction.json")
    real_report = load_json(ROOT / "results/real-history-clean-reproduction.json")
    cold_report = load_json(ROOT / "results/clean-reproduction.json")
    test_log = (ROOT / continuation_report["commands"][0]["log"]).read_text(encoding="utf-8")
    match = re.search(r"Ran (\d+) tests?", test_log)
    require(match is not None, "unit-test count missing from continuation log")
    expected_reproduction = {
        "DirectedTests": integer(match.group(1)),
        "ReferenceRecords": integer(EXPECTED_REFERENCES),
        "EvidenceClaims": integer(len(read_csv(ROOT / "claim_evidence_ledger.csv"))),
        "ContinuationWallSeconds": f"{continuation_report['wall_seconds']:.3f}",
        "ContinuationChildCpuSeconds": f"{continuation_report['child_cpu_seconds']:.3f}",
        "ContinuationMaxRssKiB": integer(continuation_report["maximum_child_rss_kib"]),
        "RealWallSeconds": f"{real_report['wall_seconds']:.3f}",
        "RealChildCpuSeconds": f"{real_report['child_cpu_seconds']:.3f}",
        "RealMaxRssKiB": integer(real_report["maximum_child_rss_kib"]),
        "ColdWallSeconds": f"{cold_report['wall_seconds']:.3f}",
        "ColdChildCpuSeconds": f"{cold_report['child_cpu_seconds']:.3f}",
        "ColdMaxRssKiB": integer(cold_report["maximum_child_rss_kib"]),
    }
    actual_reproduction = parse_tex_macros(
        paper_dir / "tables/generated-reproduction-macros.tex"
    )
    actual_reproduction.update(parse_tex_macros(paper_dir / "generated/heldout_resource_macros.tex"))
    require(actual_reproduction == expected_reproduction,
            "generated reproduction manuscript macros are stale or inconsistent")

    main = (paper_dir / "main.tex").read_text(encoding="utf-8")
    for name in (
        "generated-continuation-macros", "generated-real-history-macros",
        "generated-reproduction-macros",
    ):
        require(f"\\input{{tables/{name}}}" in main, f"paper does not import {name}")
    require("\\bibliography{references}" in main and "\\bibliographystyle{ACM-Reference-Format}" in main,
            "ACM bibliography configuration is missing")
    for relative in (
        "tables/generated-continuation-main.tex",
        "tables/generated-continuation-cases.tex",
        "tables/generated-continuation-active.tex",
        "tables/generated-continuation-capacity.tex",
        "tables/generated-real-history.tex",
        "figures/tradeoff.csv",
    ):
        path = paper_dir / relative
        require(path.is_file() and path.stat().st_size > 0,
                f"missing or empty generated paper result file: {relative}")
    return {
        "continuation_macros": len(actual_continuation),
        "real_history_macros": len(actual_real),
        "reproduction_macros": len(actual_reproduction),
        "generated_result_files": 6,
    }

def evidence_paths(row: dict[str, str]) -> list[Path]:
    return [ROOT / value.strip() for value in row["evidence_files"].split(";") if value.strip()]


def validate_claim_ledger() -> dict[str, object]:
    rows = read_csv(ROOT / "claim_evidence_ledger.csv")
    ids = [row["claim_id"] for row in rows]
    require(len(ids) == len(set(ids)), "duplicate claim ID")
    missing: list[str] = []
    for row in rows:
        require(row["status"] == "supported", f"unsupported material claim in final ledger: {row['claim_id']}")
        require(row["boundary"].strip(), f"missing claim boundary: {row['claim_id']}")
        require(row["verification_command"].strip(), f"missing verification command: {row['claim_id']}")
        for path in evidence_paths(row):
            if not path.exists():
                missing.append(f"{row['claim_id']}:{path.relative_to(ROOT)}")
    require(not missing, "missing claim evidence paths: " + ", ".join(missing))
    return {"claims": len(rows), "supported": len(rows), "missing_evidence_paths": 0}


def close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def validate_results(check_reproductions: bool = True, check_trace_inventory: bool = True) -> dict[str, object]:
    continuation = load_json(ROOT / "results/continuation-summary.json")
    require(isinstance(continuation, dict), "invalid continuation summary")
    policies = continuation["policies"]
    require(continuation["primary_queries"] == 4536, "primary query count changed")
    require(continuation["primary_traces"] == 18, "primary trace count changed")
    reference = policies["continuation"]
    for policy in SAFE_CONTINUATION_POLICIES:
        item = policies[policy]
        require(item["exact"] == 4340, f"unexpected exact count: {policy}")
        require(item["claimed_complete"] == 4248, f"unexpected complete count: {policy}")
        require(item["false_complete"] == 0, f"false completeness in safe policy: {policy}")
        require(item["unsound_rows"] == 0, f"unsound row in safe policy: {policy}")
    expected_primary_costs = {
        "continuation": (353.84149029982365, 1.8606701940035273, 60078),
        "overlay": (2157.173941798942, 6.070546737213404, None),
        "cut-cache": (2076.886463844797, 5.851851851851852, 18237),
        "mirror": (729.1093474426808, 1.4426807760141094, 1735030),
    }
    for policy, (expected_bytes, expected_messages, expected_state) in expected_primary_costs.items():
        item = policies[policy]
        require(close(item["mean_source_bytes"], expected_bytes),
                f"primary source-byte result changed: {policy}")
        require(close(item["mean_source_messages"], expected_messages),
                f"primary message result changed: {policy}")
        if expected_state is not None:
            require(item["max_logical_state_bytes"] == expected_state,
                    f"primary logical-state result changed: {policy}")
    require(reference["mean_source_bytes"] < policies["overlay"]["mean_source_bytes"],
            "continuation no longer beats overlay on the frozen standing-query fixture")
    require(reference["mean_source_bytes"] < policies["cut-cache"]["mean_source_bytes"],
            "continuation no longer beats cut-cache on the frozen standing-query fixture")
    require(reference["mean_source_bytes"] < policies["mirror"]["mean_source_bytes"],
            "continuation no longer beats mirror source bytes on the frozen fixture")
    require(continuation["continuation_repairs"]["queries"] == 48 and
            continuation["continuation_repairs"]["shards"] == 48 and
            len(continuation["continuation_repairs"]["records"]) == 48,
            "blocker repair coverage changed")
    unsafe = policies["unsafe-stale"]
    require((unsafe["false_complete"], unsafe["unsound_rows"]) == (1910, 2075),
            "primary negative control changed")
    finite = continuation["finite_checks"]
    finite_total = finite["local_transition_cases"] + finite["k_plus_d_repair_cases"] + finite["global_merge_cases"]
    require(finite_total == 220997 and finite["failures"] == 0, "finite checks changed or failed")
    source_receipts = sum(continuation["source_replay"][key] for key in ("events", "overlay", "prefix", "snapshot"))
    require(source_receipts == 30312, f"primary replay count changed: {source_receipts}")

    real = load_json(ROOT / "results/real-history-summary.json")
    require(isinstance(real, dict), "invalid real-history summary")
    require(real["selected_commits"] == 6 and real["function_assignments"] == 7,
            "real-history selection changed")
    require(real["queries_per_policy"] == 408, "real-history query count changed")
    for policy in SAFE_REAL_HISTORY_POLICIES:
        item = real["policies"][policy]
        require(item["exact"] == 408 and item["claimed_complete"] == 408,
                f"real-history safe result changed: {policy}")
        require(item["false_complete"] == 0 and item["unsound_rows"] == 0,
                f"real-history safety failure: {policy}")
    expected_real_costs = {
        "continuation": (400.0686274509804, 1.0294117647058822, 145912),
        "overlay": (2193.6127450980393, 6.0, None),
        "cut-cache": (1014.5441176470588, 2.6666666666666665, 75176),
    }
    for policy, (expected_bytes, expected_messages, expected_state) in expected_real_costs.items():
        item = real["policies"][policy]
        require(close(item["mean_source_bytes"], expected_bytes),
                f"real-history source-byte result changed: {policy}")
        require(close(item["mean_source_messages"], expected_messages),
                f"real-history message result changed: {policy}")
        if expected_state is not None:
            require(item["max_logical_state_bytes"] == expected_state,
                    f"real-history logical-state result changed: {policy}")
    require((real["policies"]["unsafe-stale"]["false_complete"],
             real["policies"]["unsafe-stale"]["unsound_rows"]) == (22, 21),
            "real-history negative control changed")
    require(real["process_recovery"] == {
        "distinct_initial_processes": 6,
        "distinct_final_processes": 6,
        "killed_pid_replaced": True,
        "restarted_pid_is_new": True,
    }, "process recovery evidence changed")
    require(real["fault_probe"] == {
        "partial_status": "partial",
        "missing_shards": [0],
        "overlay_claim_complete": False,
        "healed_status": "complete",
        "healed_exact": True,
    }, "fault-probe evidence changed")
    real_receipts = sum(real["source_replay"][key] for key in ("events", "overlay", "prefix", "snapshot"))
    require(real_receipts == 1991, f"real-history replay count changed: {real_receipts}")

    cold = load_json(ROOT / "results/summary.json")
    require(isinstance(cold, dict), "invalid one-shot summary")
    require(cold["queries_per_primary_policy"] == 1008, "cold-query count changed")
    certified = cold["primary_policies"]["certified"]
    overlay = cold["primary_policies"]["overlay"]
    for item in (certified, overlay):
        require(item["exact"] == 969 and item["complete"] == 936,
                "cold safe outcome changed")
        require(item["false_complete"] == 0 and item["unsound_rows"] == 0,
                "cold safe outcome became unsound")
    require(close(certified["mean_bytes"], 4673.660714285715), "cold certificate bytes changed")
    require(close(overlay["mean_bytes"], 2173.1825396825398), "cold overlay bytes changed")
    require(certified["mean_bytes"] > overlay["mean_bytes"], "cold negative result disappeared")

    reproductions = {}
    if check_reproductions:
        for name in ("continuation-clean-reproduction.json", "real-history-clean-reproduction.json", "clean-reproduction.json"):
            report = load_json(ROOT / "results" / name)
            require(isinstance(report, dict) and report["status"] == "PASS", f"reproduction not PASS: {name}")
            require(report["error"] is None, f"reproduction records an error: {name}")
            require(all(command["exit_code"] == 0 for command in report["commands"]),
                    f"failed command retained in {name}")
            reproductions[name] = len(report["commands"])

    if check_trace_inventory:
        require(len(list((ROOT / "results/continuation-traces").glob("*.json.gz"))) == 26,
                "continuation trace count changed")
        require(len(list((ROOT / "results/traces").glob("*.json.gz"))) == 33,
                "one-shot trace count changed")
    return {
        "primary_queries_per_policy": continuation["primary_queries"],
        "primary_source_receipts_replayed": source_receipts,
        "finite_instances": finite_total,
        "real_history_queries_per_policy": real["queries_per_policy"],
        "real_history_source_receipts_replayed": real_receipts,
        "cold_queries_per_policy": cold["queries_per_primary_policy"],
        "reproduction_command_counts": reproductions,
    }



def validate_resource_ledger() -> dict[str, object]:
    ledger = load_json(ROOT / "docs/resource-ledger.json")
    require(isinstance(ledger, dict), "invalid resource ledger")
    continuation = load_json(ROOT / "results/continuation-clean-reproduction.json")
    real = load_json(ROOT / "results/real-history-clean-reproduction.json")
    cold = load_json(ROOT / "results/clean-reproduction.json")
    require(isinstance(continuation, dict) and isinstance(real, dict) and isinstance(cold, dict),
            "invalid reproduction record")

    def same(actual: object, expected: object, label: str) -> None:
        if isinstance(expected, float):
            require(isinstance(actual, (int, float)) and math.isclose(float(actual), expected,
                    rel_tol=0.0, abs_tol=1e-9), f"resource ledger differs: {label}")
        else:
            require(actual == expected, f"resource ledger differs: {label}")

    mappings = (
        ("continuation_clean_reproduction", continuation),
        ("real_history_clean_reproduction", real),
        ("one_shot_clean_reproduction_final", cold),
    )
    for ledger_key, report in mappings:
        item = ledger[ledger_key]
        for field in ("status", "wall_seconds", "child_cpu_seconds", "parent_cpu_seconds",
                      "maximum_child_rss_kib", "driver_rss_kib"):
            same(item[field], report[field], f"{ledger_key}.{field}")
        same(item["commands"], len(report["commands"]), f"{ledger_key}.commands")

    test_log = (ROOT / continuation["commands"][0]["log"]).read_text(encoding="utf-8")
    match = re.search(r"Ran (\d+) tests?", test_log)
    require(match is not None, "unit-test count missing from continuation log")
    directed = int(match.group(1))
    require(ledger["test_counts"]["directed_unit_tests"] == directed,
            "resource ledger unit-test count differs")
    require(directed >= 40, "release should retain at least 40 directed tests")

    markdown = (ROOT / "docs/resource-ledger.md").read_text(encoding="utf-8")
    require(f"{directed} unit tests" in markdown, "resource-ledger.md test count is stale")
    require(f"{continuation['wall_seconds']:.6f} wall seconds" in markdown,
            "resource-ledger.md continuation timing is stale")
    require(f"{real['wall_seconds']:.6f} wall seconds" in markdown,
            "resource-ledger.md real-history timing is stale")
    require(f"{cold['wall_seconds']:.6f} wall seconds" in markdown,
            "resource-ledger.md cold timing is stale")
    return {"directed_unit_tests": directed, "three_clean_records_match": True}


def validate_history() -> dict[str, object]:
    history = load_json(ROOT / "inputs/git-history/cachetools-function-history.json")
    require(isinstance(history, dict), "invalid Git-history fixture")
    require(history["repository"] == "tkem/cachetools", "history repository changed")
    commits = history["commits"]
    require(len(commits) == 6, "expected six selected commits")
    require(len({commit["sha"] for commit in commits}) == 6, "duplicate selected commit")
    assignments = 0
    for commit in commits:
        require(re.fullmatch(r"[0-9a-f]{40}", commit["sha"]) is not None,
                f"invalid commit identifier: {commit['sha']}")
        require(commit["url"] == f"https://github.com/tkem/cachetools/commit/{commit['sha']}",
                f"commit URL mismatch: {commit['sha']}")
        require(commit["operations"], f"commit has no projected operation: {commit['sha']}")
        assignments += len(commit["operations"])
        require(re.fullmatch(r"[0-9a-f]{64}", commit["expected_projected_sha256"]) is not None,
                f"invalid stage digest: {commit['sha']}")
    require(assignments == 7, f"expected seven function assignments, found {assignments}")
    base = history["base"]
    base_path = ROOT / base["file"]
    require(base_path.is_file(), "retained history base file missing")
    actual = hashlib.sha256(base_path.read_bytes()).hexdigest()
    require(actual == base["file_sha256"], "retained history base file digest mismatch")
    return {"selected_commits": len(commits), "function_assignments": assignments, "base_file_verified": True}


def validate_inputs() -> dict[str, object]:
    rows = read_csv(ROOT / "inputs/source_inventory.csv")
    require(len(rows) == 8, "source inventory should contain eight distributions")
    declared_files = sum(int(row["files"]) for row in rows)
    declared_bytes = sum(int(row["bytes"]) for row in rows)
    require(declared_files == 107, f"source inventory file total changed: {declared_files}")
    source_root = ROOT / "inputs/sources"
    actual_python = list(source_root.rglob("*.py"))
    require(len(actual_python) == 107, f"retained Python source count changed: {len(actual_python)}")
    for row in rows:
        require(urlparse(row["url"]).scheme == "https", f"non-HTTPS input source: {row['repository_origin']}")
        require(row["boundary"].strip(), f"missing input boundary: {row['repository_origin']}")
    require((ROOT / "LICENSE").is_file(), "project license missing")
    require((ROOT / "inputs/ACQUISITION.md").is_file(), "acquisition record missing")
    return {"source_distributions": 8, "retained_python_files": 107, "declared_source_bytes": declared_bytes}



def validate_external_resources() -> dict[str, object]:
    rows = read_csv(ROOT / "external_resources.csv")
    require(len(rows) == 79, f"expected 79 external resource rows, found {len(rows)}")
    names = [row["name"].strip() for row in rows]
    urls = [row["url"].strip() for row in rows]
    require(len(names) == len(set(name.lower() for name in names)),
            "duplicate external resource name")
    require(len(urls) == len(set(urls)), "duplicate external resource URL")
    for row in rows:
        require(all(row[field].strip() for field in (
            "name", "url", "license", "access_date", "resource_type",
            "acquisition_method", "integration_mode", "supported_claim",
            "internals_modified",
        )), f"incomplete external resource row: {row.get('name', '<unknown>')}")
        require(urlparse(row["url"]).scheme == "https",
                f"non-HTTPS external resource: {row['name']}")
        require(re.fullmatch(r"2026-09-(10|11|12|13|16|18)", row["access_date"]) is not None,
                f"unexpected external-resource access date: {row['name']}")
        require(row["internals_modified"].lower().startswith("no"),
                f"external resource was internally modified: {row['name']}")

    types = Counter(row["resource_type"] for row in rows)
    expected_types = {
        "scholarly paper": 67,
        "public licensed source text": 8,
        "official venue rule": 1,
        "formatting asset": 1,
        "official recognition record": 1,
        "public Git commit metadata and patches": 1,
    }
    require(types == expected_types, f"external resource type counts changed: {dict(types)}")

    reference_urls = {
        row["stable_source"] for row in read_csv(ROOT / "docs/reference-audit.csv")
    }
    scholarly_urls = {
        row["url"] for row in rows if row["resource_type"] == "scholarly paper"
    }
    require(scholarly_urls == reference_urls,
            "external-resource scholarly URLs differ from the 67 reference records")

    source_urls = {row["url"] for row in read_csv(ROOT / "inputs/source_inventory.csv")}
    retained_source_urls = {
        row["url"] for row in rows if row["resource_type"] == "public licensed source text"
    }
    require(retained_source_urls == source_urls,
            "external-resource source URLs differ from source_inventory.csv")
    return {
        "resources": len(rows),
        "type_counts": dict(sorted(types.items())),
        "unique_names": len(names),
        "unique_urls": len(urls),
        "reference_urls_closed": True,
        "source_urls_closed": True,
    }

def validate_hygiene(project_root: Path | None) -> dict[str, object]:
    forbidden: list[str] = []
    symlinks: list[str] = []
    for path in ROOT.rglob("*"):
        relative = path.relative_to(ROOT)
        if path.is_symlink():
            symlinks.append(str(relative))
        if path.name in FORBIDDEN_NAMES or path.suffix in {".pyc", ".pyo"}:
            forbidden.append(str(relative))
        name_upper = path.name.upper()
        if "PROMPT" in name_upper or "TEMPLATE" in name_upper:
            forbidden.append(str(relative))
    require(not symlinks, "symlinks present: " + ", ".join(symlinks))
    require(not forbidden, "forbidden publication files: " + ", ".join(forbidden))
    for path in [ROOT / "README.md", ROOT / "docs/third-party.md"]:
        text = path.read_text(encoding="utf-8")
        for marker in FORBIDDEN_PUBLICATION_SUBSTRINGS:
            require(marker not in text, f"forbidden placeholder/workflow string in {path.name}: {marker}")
    result: dict[str, object] = {"symlinks": 0, "forbidden_files": 0}
    if project_root is not None:
        require(project_root.is_dir(), f"project root not found: {project_root}")
        entries = {path.name for path in project_root.iterdir()}
        require(entries == EXPECTED_PROJECT_ENTRIES,
                f"project root entries differ: {sorted(entries)}")
        require((project_root / "artifact").resolve() == ROOT.resolve(),
                "validator is not running from the supplied project artifact")
        result["project_root_entries"] = sorted(entries)
    return result


def validate_all(paper_dir: Path | None = None, project_root: Path | None = None) -> dict[str, object]:
    checks = {
        "references": validate_references(paper_dir),
        "claim_evidence": validate_claim_ledger(),
        "results": validate_results(),
        "resource_ledger": validate_resource_ledger(),
        "real_history": validate_history(),
        "inputs": validate_inputs(),
        "external_resources": validate_external_resources(),
        "hygiene": validate_hygiene(project_root),
    }
    if paper_dir is not None:
        checks["paper_results"] = validate_paper_results(paper_dir)
    return {
        "status": "PASS",
        "meaning": (
            "offline internal consistency, evidence-path, reference-record, frozen-result, "
            "input, and package-hygiene checks; not external peer review, general proof, or deployment validation"
        ),
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-dir", type=Path, default=None,
                        help="optional paper directory for citation and bibliography cross-checks")
    parser.add_argument("--project-root", type=Path, default=None,
                        help="optional project root for paper/artifact/README packaging check")
    parser.add_argument("--no-write", action="store_true", help="do not update results/release-validation.json")
    args = parser.parse_args()
    try:
        result = validate_all(args.paper_dir, args.project_root)
    except (ValidationError, KeyError, ValueError, json.JSONDecodeError) as exc:
        failure = {"status": "FAIL", "error": str(exc)}
        print(json.dumps(failure, sort_keys=True))
        raise SystemExit(1) from exc
    if not args.no_write:
        output = ROOT / "results/release-validation.json"
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
