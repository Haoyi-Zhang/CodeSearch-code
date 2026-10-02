"""Deterministic function-level projection of fixed upstream Git commits.

The retained input records six published cachetools commits between the exact
v7.1.4 source file already present in the artifact and the v7.1.8 head.  Only
function-body hunks are projected: this is a real-change validation trace, not
an assertion that the artifact reconstructs complete repository snapshots.
"""
from __future__ import annotations

import ast
import copy
import hashlib
import json
import textwrap
from collections import Counter
from pathlib import Path
from typing import Any

from .model import features

HISTORY_INPUT = Path("inputs/git-history/cachetools-function-history.json")
SOURCE_RELATIVE = Path("inputs/sources/cachetools/cachetools/__init__.py")
IDENT_PREFIX = "cachetools/cachetools/__init__.py:"


def _class(tree: ast.Module, name: str) -> ast.ClassDef:
    matches = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name]
    if len(matches) != 1:
        raise ValueError(f"expected one class {name!r}, found {len(matches)}")
    return matches[0]


def _method(text: str, class_name: str, method_name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(text)
    owner = _class(tree, class_name)
    matches = [
        node for node in owner.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected one method {class_name}.{method_name}, found {len(matches)}"
        )
    return matches[0]


def _replacement(source: str) -> str:
    clean = textwrap.dedent(source).strip("\n")
    return textwrap.indent(clean, "    ") + "\n"


def _replace_method(text: str, operation: dict[str, Any]) -> str:
    node = _method(text, operation["class"], operation["method"])
    start = min([node.lineno] + [decorator.lineno for decorator in node.decorator_list])
    lines = text.splitlines(keepends=True)
    return "".join(lines[: start - 1]) + _replacement(operation["source"]) + "".join(
        lines[node.end_lineno :]
    )


def _insert_after_method(text: str, operation: dict[str, Any]) -> str:
    node = _method(text, operation["class"], operation["method"])
    lines = text.splitlines(keepends=True)
    inserted = "\n" + _replacement(operation["source"])
    return "".join(lines[: node.end_lineno]) + inserted + "".join(lines[node.end_lineno :])


def apply_operation(text: str, operation: dict[str, Any]) -> str:
    kind = operation.get("kind")
    if kind == "replace_method":
        return _replace_method(text, operation)
    if kind == "insert_after_method":
        return _insert_after_method(text, operation)
    raise ValueError(f"unknown history operation {kind!r}")


def function_sources(text: str) -> dict[str, str]:
    """Return unique qualified function sources using corpus-builder naming."""
    tree = ast.parse(text)
    lines = text.splitlines(keepends=True)
    answer: dict[str, str] = {}
    counts: Counter[str] = Counter()

    def visit(node: ast.AST, parents: list[str]) -> None:
        nested = parents
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            qualifier = ".".join(parents + [node.name])
            nested = parents + [node.name]
            if not isinstance(node, ast.ClassDef):
                counts[qualifier] += 1
                label = qualifier if counts[qualifier] == 1 else f"{qualifier}#{counts[qualifier]}"
                start = min([node.lineno] + [item.lineno for item in node.decorator_list])
                answer[label] = "".join(lines[start - 1 : node.end_lineno])
        for child in ast.iter_child_nodes(node):
            visit(child, nested)

    visit(tree, [])
    return answer


def _body_id(source: str) -> str:
    return "rh" + hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]


def _identifier(qualifier: str) -> str:
    if "#" in qualifier:
        name, occurrence = qualifier.rsplit("#", 1)
    else:
        name, occurrence = qualifier, "1"
    return f"{IDENT_PREFIX}{name}:{occurrence}"


def _targeted_queries(
    initial: list[dict[str, str]], payloads: dict[str, dict], versions: dict[str, list[set[str]]]
) -> list[dict]:
    # Frequency is computed before the experiment from the frozen base corpus
    # plus each distinct projected body.  The query rule is deterministic.
    frequency: Counter[str] = Counter()
    for state in initial:
        for body in state.values():
            frequency.update(payloads[body]["features"])
    for body, value in payloads.items():
        if body.startswith("rh"):
            frequency.update(value["features"])

    queries = []
    for index, identifier in enumerate(sorted(versions)):
        observed = versions[identifier]
        present_at_base = any(identifier in state for state in initial)
        pool = set.intersection(*observed) if present_at_base else set.union(*observed)
        if len(pool) < 2:
            raise ValueError(f"insufficient stable query labels for {identifier}")
        terms = sorted(
            pool,
            key=lambda term: (
                frequency[term],
                0 if term.startswith("call:") else 1,
                term,
            ),
        )[:4]
        queries.append(
            {
                "id": f"rhq{index:03d}",
                "source_body": None,
                "seed_id": None,
                "origin_repo": "cachetools-real-history",
                "plan": [terms],
                "k": 5,
                "choice": index,
                "target_id": identifier,
                "selection": "rarest stable syntax labels of the changed function",
            }
        )
    return queries


def build_real_history(root: Path, fixture: dict | None = None) -> dict:
    """Extend a corpus fixture with real function changes and fixed queries."""
    if fixture is None:
        from .corpus import build

        fixture = build(root)
    fixture = copy.deepcopy(fixture)
    payloads = fixture["payloads"]
    input_path = root / HISTORY_INPUT
    specification = json.loads(input_path.read_text(encoding="utf-8"))
    source_path = root / SOURCE_RELATIVE
    text = source_path.read_text(encoding="utf-8")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if digest != specification["base"]["file_sha256"]:
        raise ValueError("retained cachetools base file does not match history input")

    initial_functions = function_sources(text)
    selected = {
        qualifier
        for commit in specification["commits"]
        for qualifier in commit["expected_changed_functions"]
    }
    versions: dict[str, list[set[str]]] = {}
    for qualifier in selected:
        source = initial_functions.get(qualifier)
        if source is not None:
            versions.setdefault(_identifier(qualifier), []).append(set(features(source)))

    history: list[list[dict]] = [[], [], []]
    stages = []
    previous = initial_functions
    shard = fixture["extraction"]["repositories"]["cachetools"]["shard"]
    if shard != 1:
        raise ValueError("frozen cachetools ownership changed")

    for sequence, commit in enumerate(specification["commits"], 1):
        for operation in commit["operations"]:
            text = apply_operation(text, operation)
        stage_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if stage_digest != commit["expected_projected_sha256"]:
            raise ValueError(f"projected source mismatch at {commit['sha']}")
        current = function_sources(text)
        changed = sorted(
            qualifier for qualifier in set(previous) | set(current)
            if previous.get(qualifier) != current.get(qualifier)
        )
        if changed != sorted(commit["expected_changed_functions"]):
            raise ValueError(f"unexpected function changes at {commit['sha']}: {changed}")

        changes: list[list[str | None]] = []
        for qualifier in changed:
            identifier = _identifier(qualifier)
            source = current.get(qualifier)
            if source is None:
                body = None
            else:
                terms = list(features(source))
                if len(terms) < 2 or len(source.encode("utf-8")) > 20_000:
                    raise ValueError(f"projected function violates corpus bounds: {qualifier}")
                body = _body_id(source)
                value = {
                    "source": source,
                    "features": terms,
                    "origin": (
                        f"github:tkem/cachetools@{commit['sha']}:"
                        "src/cachetools/__init__.py"
                    ),
                    "lines": None,
                    "commit": commit["sha"],
                }
                if body in payloads and payloads[body] != value:
                    raise ValueError("projected body identifier collision")
                payloads[body] = value
                versions.setdefault(identifier, []).append(set(terms))
            changes.append([identifier, body])

        event = {
            "shard": shard,
            "seq": sequence,
            "changes": changes,
            "commit": commit["sha"],
            "commit_date": commit["date"],
            "commit_url": commit["url"],
            "projected_source_sha256": stage_digest,
        }
        history[shard].append(event)
        stages.append(
            {
                "seq": sequence,
                "commit": commit["sha"],
                "date": commit["date"],
                "message": commit["message"],
                "changed_functions": changed,
                "changes": copy.deepcopy(changes),
                "projected_source_sha256": stage_digest,
            }
        )
        previous = current

    targeted = _targeted_queries(fixture["initial"], payloads, versions)
    queries = copy.deepcopy(fixture["queries"]) + targeted
    return {
        **fixture,
        "payloads": payloads,
        "history": history,
        "queries": queries,
        "base_query_count": len(fixture["queries"]),
        "targeted_queries": targeted,
        "git_history": {
            "input": str(HISTORY_INPUT),
            "repository": specification["repository"],
            "base": specification["base"],
            "observed_head": specification["observed_head"],
            "selection": specification["selection"],
            "projection": specification["projection"],
            "query_scope": specification["query_scope"],
            "stages": stages,
        },
    }
