"""Frozen syntactic retrieval model and certificate construction (standard library)."""
from __future__ import annotations
import ast
from heapq import nsmallest
import textwrap
from typing import Any

MAX_EVENTS = 128
MAX_PENDING = 128
MAX_BATCH = 16
MAX_PREFIX = 64
MAX_K = 20
SHARDS = 3


def features(source: str) -> tuple[str, ...]:
    """Typed syntactic labels, not inferred types or program semantics."""
    tree = ast.parse(textwrap.dedent(source))
    terms: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            terms.add('name:' + node.id.lower())
        elif isinstance(node, ast.Attribute):
            terms.add('attr:' + node.attr.lower())
        elif isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Name):
                terms.add('call:' + f.id.lower())
            elif isinstance(f, ast.Attribute):
                terms.add('call:' + f.attr.lower())
    return tuple(sorted(terms))


def score(terms: tuple[str, ...] | list[str], plan: list[list[str]]) -> int:
    t = set(terms)
    return max((sum(x in t for x in alternate) for alternate in plan), default=0)


def key(row: dict[str, Any]) -> tuple[int, str]:
    return (-row['score'], row['id'])


def ranked(state: dict[str, str], payloads: dict[str, dict], plan: list[list[str]]) -> list[dict]:
    ans = []
    for ident, body in state.items():
        value = score(payloads[body]['features'], plan)
        if value:
            ans.append({'id': ident, 'body': body, 'score': value})
    return sorted(ans, key=key)


def apply(state: dict[str, str], event: dict) -> None:
    for ident, body in event['changes']:
        if body is None:
            state.pop(ident, None)
        else:
            state[ident] = body


def prefix_receipt(state: dict[str, str], payloads: dict, plan: list[list[str]],
                   length: int, shard: int, epoch: int, at: int) -> dict:
    rows = ranked(state, payloads, plan)
    boundary = list(key(rows[length])) if len(rows) > length else None
    return {'kind': 'prefix', 'shard': shard, 'epoch': epoch, 'plan': plan,
            'at': at, 'rows': rows[:length], 'boundary': boundary}


def delta_receipt(events: list[dict], payloads: dict, plan: list[list[str]],
                  k: int, shard: int, epoch: int, lo: int, hi: int) -> dict:
    if [e['seq'] for e in events] != list(range(lo + 1, hi + 1)):
        raise ValueError('noncontiguous delta')
    final: dict[str, str | None] = {}
    for event in events:
        for ident, body in event['changes']:
            final[ident] = body
    state = {i: b for i, b in final.items() if b is not None}
    return {'kind': 'delta', 'shard': shard, 'epoch': epoch, 'plan': plan,
            'lo': lo, 'hi': hi, 'changed': sorted(final),
            'rows': ranked(state, payloads, plan)[:k]}


def assemble(query: dict, cut: list[int], epoch: int, pairs: dict[str, list[int]],
             evidence: dict[int, dict]) -> dict:
    """Merge trusted source receipts. This is not the independent checker."""
    pool: dict[str, dict] = {}
    boundaries: dict[str, list | None] = {}
    lags: dict[str, int] = {}
    for shard, (p_id, d_id) in pairs.items():
        p, d = evidence[p_id]['reply'], evidence[d_id]['reply']
        changed = set(d['changed'])
        for row in p['rows']:
            if row['id'] not in changed:
                pool[row['id']] = row
        for row in d['rows']:
            pool[row['id']] = row
        boundaries[shard] = p['boundary']
        lags[shard] = cut[int(shard)] - p['at']
    rows = sorted(pool.values(), key=key)[:query['k']]
    missing = [s for s in range(SHARDS) if str(s) not in pairs]
    blockers = []
    for shard, boundary in sorted(boundaries.items()):
        if boundary is not None and (len(rows) < query['k'] or key(rows[-1]) >= tuple(boundary)):
            blockers.append(int(shard))
    status = ('partial' if missing else 'rank-underdetermined' if blockers else 'complete')
    return {'query': query, 'cut': cut, 'epoch': epoch, 'pairs': pairs,
            'rows': rows, 'status': status, 'missing_shards': missing,
            'rank_blockers': blockers, 'index_lag_events': lags}


class PostingIndex:
    """Mutable typed-label inverted index. Bodies are immutable catalog entries."""
    def __init__(self, state: dict[str, str], payloads: dict):
        self.payloads = payloads
        self.state: dict[str, str] = {}
        self.postings: dict[str, set[str]] = {}
        for ident, body in state.items():
            self.put(ident, body)

    def put(self, ident: str, body: str | None) -> None:
        old = self.state.pop(ident, None)
        if old is not None:
            for term in self.payloads[old]['features']:
                bucket = self.postings[term]
                bucket.remove(ident)
                if not bucket:
                    del self.postings[term]
        if body is not None:
            self.state[ident] = body
            for term in self.payloads[body]['features']:
                self.postings.setdefault(term, set()).add(ident)

    def apply(self, event: dict) -> None:
        for ident, body in event['changes']:
            self.put(ident, body)

    def rows(self, plan: list[list[str]], exclude: set[str] | None = None,
             *, limit: int | None = None) -> list[dict]:
        """Keep full enumeration; optionally select an exact ordered prefix."""
        if limit is not None and (type(limit) is not int or limit < 0):
            raise ValueError('row limit must be a nonnegative integer')
        excluded = exclude or set()
        maximum: dict[str, int] = {}
        for alternate in plan:
            counts: dict[str, int] = {}
            for term in alternate:
                for ident in self.postings.get(term, ()):
                    if ident not in excluded:
                        counts[ident] = counts.get(ident, 0) + 1
            for ident, count in counts.items():
                maximum[ident] = max(maximum.get(ident, 0), count)
        # Construct every positive row even for limit zero: bounded selection
        # does not skip posting enumeration or state lookup validation.
        rows = [{'id':i, 'body':self.state[i], 'score':v} for i,v in maximum.items()]
        return sorted(rows, key=key) if limit is None else nsmallest(limit, rows, key=key)
