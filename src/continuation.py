"""Stateful continuation tokens for exact top-k over a shared ordered event feed.

The producer functions in this module deliberately contain no network policy.
A token is a bounded candidate buffer plus a conservative rank-key lower bound
for every positive row not held in the buffer.  A complete event suffix advances
that invariant without consulting an index.  Only shards whose bound can still
enter the global top-k need an index-prefix repair.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Iterable
from .model import MAX_PREFIX, key, score


def _rank_key(row: dict[str, Any]) -> tuple[int, str]:
    return (-row['score'], row['id'])


def _minimum_bound(*values: list | tuple | None) -> list | None:
    present = [tuple(value) for value in values if value is not None]
    return list(min(present)) if present else None


def validate_contiguous(events: list[dict], shard: int, lo: int, hi: int) -> None:
    if type(shard) is not int or type(lo) is not int or type(hi) is not int or not 0 <= lo <= hi:
        raise ValueError('event suffix bounds')
    expected = list(range(lo + 1, hi + 1))
    actual = [event.get('seq') for event in events]
    if actual != expected or any(event.get('shard') != shard for event in events):
        raise ValueError('noncontiguous event suffix')


def final_assignments(events: Iterable[dict]) -> dict[str, str | None]:
    final: dict[str, str | None] = {}
    for event in events:
        for ident, body in event['changes']:
            final[ident] = body
    return final


def token_from_prefix(query: dict, prefix: dict, capacity: int, *, generation: int = 0) -> dict:
    """Create a token from a trusted local prefix receipt.

    The receipt issuer is trusted for local ranking in the crash-only model.
    The separate checker validates bindings, score arithmetic and transitions;
    retained-history replay checks issuer truth in the artifact.
    """
    if prefix.get('kind') != 'prefix':
        raise ValueError('prefix receipt required')
    if not query['k'] <= capacity <= MAX_PREFIX:
        raise ValueError('token capacity')
    if len(prefix['rows']) > capacity:
        raise ValueError('prefix exceeds capacity')
    return {
        'kind': 'continuation-token',
        'query': deepcopy(query),
        'shard': prefix['shard'],
        'epoch': prefix['epoch'],
        'at': prefix['at'],
        'capacity': capacity,
        'rows': deepcopy(prefix['rows']),
        'tail': deepcopy(prefix['boundary']),
        'generation': generation,
    }


def advance_token(token: dict, event_receipt: dict, payloads: dict[str, dict]) -> dict:
    """Advance a valid token over one complete suffix.

    Invariant: every target-live positive row omitted from ``rows`` has a rank
    key greater than or equal to ``tail``.  ``tail is None`` means exhaustion.
    """
    if event_receipt.get('kind') != 'events':
        raise ValueError('events receipt required')
    if (event_receipt['shard'] != token['shard'] or
            event_receipt['epoch'] != token['epoch'] or
            event_receipt['lo'] != token['at']):
        raise ValueError('token/event binding')
    events = event_receipt['events']
    validate_contiguous(events, token['shard'], event_receipt['lo'], event_receipt['hi'])
    if event_receipt['hi'] == token['at']:
        return deepcopy(token)
    final = final_assignments(events)
    pool = {row['id']: deepcopy(row) for row in token['rows'] if row['id'] not in final}
    plan = token['query']['plan']
    for ident, body in final.items():
        if body is None:
            continue
        value = score(payloads[body]['features'], plan)
        if value:
            pool[ident] = {'id': ident, 'body': body, 'score': value}
    ordered = sorted(pool.values(), key=_rank_key)
    capacity = token['capacity']
    rows = ordered[:capacity]
    dropped = _rank_key(ordered[capacity]) if len(ordered) > capacity else None
    return {
        **{name: deepcopy(token[name]) for name in ('kind', 'query', 'shard', 'epoch', 'capacity')},
        'at': event_receipt['hi'],
        'rows': rows,
        'tail': _minimum_bound(token['tail'], dropped),
        'generation': token['generation'] + 1,
    }


def rebind_token(token: dict, new_epoch: int) -> dict:
    """Rebind a logical-shard token after a trusted exact ownership barrier."""
    if type(new_epoch) is not int or new_epoch <= token['epoch']:
        raise ValueError('new epoch required')
    rebound = deepcopy(token)
    rebound['epoch'] = new_epoch
    rebound['generation'] += 1
    return rebound


def merge_tokens(query: dict, cut: list[int], epoch: int,
                 tokens: dict[int, dict], missing: Iterable[int] = ()) -> dict:
    """Merge current tokens and expose every shard whose tail can enter top-k."""
    missing_set = set(missing)
    pool: dict[str, dict] = {}
    for shard, token in sorted(tokens.items()):
        if shard in missing_set:
            continue
        if token['query'] != query or token['shard'] != shard or token['epoch'] != epoch:
            raise ValueError('token request binding')
        if token['at'] != cut[shard]:
            raise ValueError('token is not at requested cut')
        for row in token['rows']:
            if row['id'] in pool:
                raise ValueError('cross-shard duplicate identifier')
            pool[row['id']] = row
    rows = sorted(pool.values(), key=key)[:query['k']]
    blockers: list[int] = []
    for shard, token in sorted(tokens.items()):
        if shard in missing_set:
            continue
        tail = token['tail']
        if tail is not None and (len(rows) < query['k'] or key(rows[-1]) >= tuple(tail)):
            blockers.append(shard)
    missing_list = sorted(missing_set | (set(range(3)) - set(tokens)))
    status = 'partial' if missing_list else 'rank-underdetermined' if blockers else 'complete'
    return {
        'kind': 'continuation-result',
        'query': deepcopy(query),
        'cut': list(cut),
        'epoch': epoch,
        'rows': deepcopy(rows),
        'status': status,
        'missing_shards': missing_list,
        'rank_blockers': blockers,
        'token_summary': {
            str(shard): {
                'at': token['at'],
                'capacity': token['capacity'],
                'tail': deepcopy(token['tail']),
                'generation': token['generation'],
            }
            for shard, token in sorted(tokens.items()) if shard not in missing_set
        },
    }


def distinct_changes(events: Iterable[dict]) -> int:
    return len(final_assignments(events))


def sufficient_repair_length(query: dict, events: Iterable[dict], reserve: int = 0) -> int:
    """Top-(k+d) survives at most d dirty IDs; optional reserve aids reuse."""
    if reserve < 0:
        raise ValueError('negative reserve')
    required = query['k'] + distinct_changes(events) + reserve
    if required > MAX_PREFIX:
        raise ValueError('repair length exceeds supported prefix')
    return required
