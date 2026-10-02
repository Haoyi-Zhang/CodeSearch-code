"""Query policies; only RPC replies, the caller's sealed cut and ownership map."""
from __future__ import annotations
from .model import SHARDS, assemble, key


async def certified(network, query, cut, epoch, owners, length):
    pairs = {}
    diagnostics = {}
    for shard in range(SHARDS):
        p_id = p = None
        for node in owners[shard]:
            r_id, reply = await network.rpc(node, {'op': 'prefix', 'shard': shard,
                'epoch': epoch, 'query': query, 'length': length})
            if reply and reply.get('kind') == 'prefix' and reply['at'] <= cut[shard]:
                p_id, p = r_id, reply
                break
        if p is None:
            diagnostics[str(shard)] = 'missing-prefix'
            continue
        for node in owners[shard]:
            d_id, d = await network.rpc(node, {'op': 'delta', 'shard': shard,
                'epoch': epoch, 'query': query, 'lo': p['at'], 'hi': cut[shard]})
            if d and d.get('kind') == 'delta':
                pairs[str(shard)] = [p_id, d_id]
                break
        else:
            diagnostics[str(shard)] = 'missing-contiguous-delta'
    cert = assemble(query, cut, epoch, pairs, network.evidence)
    cert['diagnostics'] = diagnostics
    return cert


async def baseline(network, query, cut, epoch, owners, policy):
    all_rows = {}
    covered = set()
    receipts = {}
    for shard in range(SHARDS):
        if policy == 'local-only' and shard != 0:
            continue
        if policy == 'random-shard' and shard != query['choice'] % SHARDS:
            continue
        for node in owners[shard]:
            req = {'shard': shard, 'epoch': epoch, 'query': query}
            if policy == 'overlay':
                req.update(op='overlay', to=cut[shard])
            else:
                req.update(op='prefix', length=query['k'])
            receipt, reply = await network.rpc(node, req)
            if reply and reply.get('kind') in {'prefix', 'overlay'}:
                for row in reply['rows']:
                    all_rows[row['id']] = row
                covered.add(shard)
                receipts[str(shard)] = receipt
                break
    rows = sorted(all_rows.values(), key=key)[:query['k']]
    # The unverified policy deliberately conflates availability with freshness.
    claim = policy == 'unverified' and len(covered) == SHARDS
    if policy == 'overlay':
        claim = len(covered) == SHARDS
    return {'rows': rows, 'claim_complete': claim, 'coverage': sorted(covered), 'receipts':receipts}
