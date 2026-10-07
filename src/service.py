"""Six independent logical replicas over real loopback TCP, one event loop.

The emulator injects only faults in these owned services. Logical crashes are
not OS-process crashes. Source journals and index tables have separate frontiers.
"""
from __future__ import annotations
import asyncio
import json
import time
from typing import Any
from .model import (MAX_BATCH, MAX_EVENTS, MAX_PENDING, MAX_PREFIX, MAX_K,
                    apply, prefix_receipt, delta_receipt, ranked, PostingIndex, key)


def wire(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


class Replica:
    def __init__(self, node: int, shard: int, initial: dict[str, str], payloads: dict):
        self.node, self.shard, self.epoch = node, shard, 0
        self.payloads = payloads
        self.search = PostingIndex(initial, payloads)
        self.index = self.search.state
        self.checkpoint = dict(initial)
        self.floor = self.frontier = self.index_at = 0
        self.journal: dict[int, dict] = {}
        self.pending: dict[int, dict] = {}
        self.crashed = False
        self.peak_journal = self.peak_pending = 0

    def handle(self, req: dict) -> dict:
        op = req.get('op')
        if op == 'restart':
            self.search = PostingIndex(self.checkpoint, self.payloads)
            self.index = self.search.state
            self.index_at = self.floor
            self.crashed = False
            return {'ok': True, 'index_at': self.index_at}
        if op == 'crash':
            self.crashed = True
            return {'ok': True}
        if self.crashed:
            return {'error': 'crashed'}
        if op == 'stat':
            return {'node': self.node, 'shard': self.shard, 'epoch': self.epoch,
                    'floor': self.floor, 'frontier': self.frontier, 'index_at': self.index_at}
        if op == 'receive':
            e = req['event']; n = e['seq']
            if type(n) is not int or n < 1 or e['shard'] != self.shard:
                return {'error': 'event-schema'}
            changes = e['changes']
            if len(changes) > MAX_BATCH or len({p[0] for p in changes}) != len(changes):
                return {'error': 'batch-schema'}
            if any(b is not None and b not in self.payloads for _, b in changes):
                return {'error': 'unknown-body'}
            if n <= self.floor:
                return {'ok': True, 'old': True, 'frontier': self.frontier}
            if n in self.journal:
                return ({'ok': True, 'duplicate': True, 'frontier': self.frontier}
                        if self.journal[n] == e else {'error': 'conflicting-sequence'})
            if n in self.pending and self.pending[n] != e:
                return {'error': 'conflicting-sequence'}
            if n > self.floor + MAX_EVENTS:
                return {'error': 'retention-window'}
            if n not in self.pending and len(self.pending) >= MAX_PENDING:
                return {'error': 'pending-window'}
            self.pending[n] = e
            self.peak_pending = max(self.peak_pending, len(self.pending))
            while self.frontier + 1 in self.pending:
                self.frontier += 1
                self.journal[self.frontier] = self.pending.pop(self.frontier)
            self.peak_journal = max(self.peak_journal, len(self.journal))
            return {'ok': True, 'frontier': self.frontier}
        if op == 'advance':
            target = min(req['to'], self.frontier)
            if target < self.index_at:
                return {'error': 'index-rollback'}
            for n in range(self.index_at + 1, target + 1):
                self.search.apply(self.journal[n])
            self.index_at = target
            return {'ok': True, 'index_at': target}
        if op == 'compact':
            # A real checkpoint is formed before deleting its tombstone history.
            new_floor = min(req['to'], self.index_at)
            if new_floor < self.floor:
                return {'error': 'floor-rollback'}
            cp = dict(self.checkpoint)
            for n in range(self.floor + 1, new_floor + 1):
                apply(cp, self.journal[n])
            self.checkpoint = cp
            for n in range(self.floor + 1, new_floor + 1):
                del self.journal[n]
            self.floor = new_floor
            return {'ok': True, 'floor': self.floor}
        if op == 'install':
            # Administrative epoch barrier, not an asynchronous consensus protocol.
            if req['epoch'] <= self.epoch:
                return {'error': 'old-epoch'}
            self.shard, self.epoch = req['shard'], req['epoch']
            self.search = PostingIndex(req['state'], self.payloads)
            self.index = self.search.state; self.checkpoint = dict(req['state'])
            self.floor = self.frontier = self.index_at = req['at']
            self.journal.clear(); self.pending.clear()
            return {'ok': True}
        if req.get('epoch') != self.epoch or req.get('shard') != self.shard:
            return {'error': 'ownership-epoch'}
        if op == 'events':
            lo, hi = req['lo'], req['hi']
            if not self.floor <= lo <= hi <= self.frontier:
                return {'error': 'journal-coverage', 'floor': self.floor, 'frontier': self.frontier}
            return {'kind': 'events', 'shard': self.shard, 'epoch': self.epoch,
                    'lo': lo, 'hi': hi,
                    'events': [self.journal[n] for n in range(lo + 1, hi + 1)]}
        if op == 'snapshot':
            if req.get('at') != self.index_at:
                return {'error': 'snapshot-frontier', 'index_at': self.index_at}
            return {'kind': 'snapshot', 'shard': self.shard, 'epoch': self.epoch,
                    'at': self.index_at, 'state': sorted(self.index.items())}
        if op in {'prefix', 'delta', 'overlay'}:
            q = req['query']; k = q['k']; plan = q['plan']
            if not 1 <= k <= MAX_K or not 1 <= len(plan) <= 2:
                return {'error': 'query-bound'}
            if any(not a or len(a) > 4 or len(a) != len(set(a)) for a in plan):
                return {'error': 'query-schema'}
        if op == 'prefix':
            length = req['length']
            if not 0 <= length <= MAX_PREFIX:
                return {'error': 'prefix-bound'}
            rows = self.search.rows(plan, limit=length + 1 if isinstance(length, int) else None)
            return {'kind':'prefix', 'shard':self.shard, 'epoch':self.epoch,
                    'plan':plan, 'at':self.index_at, 'rows':rows[:length],
                    'boundary':list(key(rows[length])) if len(rows)>length else None}
        if op == 'delta':
            lo, hi = req['lo'], req['hi']
            if not self.floor <= lo <= hi <= self.frontier:
                return {'error': 'journal-coverage', 'floor': self.floor, 'frontier': self.frontier}
            return delta_receipt([self.journal[n] for n in range(lo + 1, hi + 1)],
                                 self.payloads, plan, k, self.shard, self.epoch, lo, hi)
        if op == 'overlay':
            target = req['to']
            if not self.index_at <= target <= self.frontier:
                return {'error': 'journal-coverage'}
            # Strong exact baseline: reuse postings, exclude overwritten IDs,
            # and merge the suffix; no full index rebuild or artificial stall.
            final = {}
            for n in range(self.index_at + 1, target + 1):
                for ident, body in self.journal[n]['changes']:
                    final[ident] = body
            live = {i:b for i,b in final.items() if b is not None}
            candidates = self.search.rows(plan, set(final),
                                          limit=int(k) if isinstance(k, int) else None)[:k]
            candidates += ranked(live, self.payloads, plan)[:k]
            return {'kind': 'overlay', 'shard': self.shard, 'epoch': self.epoch,
                    'at': target, 'plan': plan, 'rows': sorted(candidates, key=key)[:k]}
        return {'error': 'unknown-operation'}


class Network:
    """RPC transport plus deterministic message faults; sequential one-worker I/O."""
    def __init__(self, replicas: list[Replica]):
        self.replicas = replicas
        self.servers: list[asyncio.Server] = []
        self.ports: list[int] = []
        self.readers: list[asyncio.StreamReader] = []
        self.writers: list[asyncio.StreamWriter] = []
        self.blocked: set[int] = set()
        self.delay_ms: dict[int, int] = {}
        self.next_id = 0
        self.evidence: dict[int, dict] = {}
        self.attempts: list[dict] = []
        self.sent_bytes = self.reply_bytes = self.messages = self.drops = 0
        self.virtual_ms = 0
        self.server_cpu_ns = 0

    async def __aenter__(self):
        for replica in self.replicas:
            async def handler(reader, writer, node=replica):
                try:
                    while True:
                        raw = await reader.readline()
                        if not raw:
                            break
                        try:
                            if len(raw) > 8 * 1024 * 1024:
                                reply = {'error': 'message-bound'}
                            else:
                                req = json.loads(raw)
                                started = time.process_time_ns()
                                reply = node.handle(req)
                                self.server_cpu_ns += time.process_time_ns() - started
                        except (ValueError, KeyError, TypeError, ConnectionError) as exc:
                            reply = {'error': 'malformed-request', 'type': type(exc).__name__}
                        writer.write(wire(reply)); await writer.drain()
                finally:
                    writer.close()
                    try:
                        await asyncio.wait_for(writer.wait_closed(), 1)
                    except (TimeoutError, ConnectionError, BrokenPipeError):
                        pass
            server = await asyncio.start_server(
                handler, '127.0.0.1', 0, limit=8 * 1024 * 1024 + 1)
            self.servers.append(server)
            self.ports.append(server.sockets[0].getsockname()[1])
        for port in self.ports:
            reader, writer = await asyncio.open_connection(
                '127.0.0.1', port, limit=8 * 1024 * 1024 + 1)
            self.readers.append(reader); self.writers.append(writer)
        return self

    async def __aexit__(self, *exc):
        # Teardown is outside measured query cost. Bound close handshakes so a
        # dead peer cannot strand a multi-case reproduction after all replies
        # have already been recorded.
        for server in self.servers:
            server.close()
        for writer in self.writers:
            writer.close()
        for writer in self.writers:
            try:
                await asyncio.wait_for(writer.wait_closed(), 1)
            except (TimeoutError, ConnectionError, BrokenPipeError):
                pass
        for server in self.servers:
            await server.wait_closed()

    async def rpc(self, node: int, req: dict, *, lost_reply: bool = False,
                  administrative: bool = False) -> tuple[int | None, dict | None]:
        # Freeze the actual wire request: an install may otherwise retain a
        # reference to a writer map that changes after this RPC.
        raw = wire(req)
        sent_req = json.loads(raw)
        attempt = {'node':node, 'request':sent_req, 'lost_reply':lost_reply, 'administrative':administrative}
        self.attempts.append(attempt)
        if node in self.blocked and not administrative:
            attempt['outcome'] = 'blocked'
            self.drops += 1; self.virtual_ms += 25
            return None, None
        self.next_id += 1; ident = self.next_id
        attempt['receipt'] = ident
        self.sent_bytes += len(raw); self.messages += 1
        self.virtual_ms += 1 + self.delay_ms.get(node, 0)
        reader, writer = self.readers[node], self.writers[node]
        writer.write(raw); await asyncio.wait_for(writer.drain(), 2)
        received = await asyncio.wait_for(reader.readline(), 2)
        if not received:
            raise ConnectionError('persistent endpoint closed')
        self.reply_bytes += len(received); self.messages += 1
        reply = json.loads(received)
        if lost_reply:
            attempt['outcome'] = 'reply-lost'
            self.drops += 1
            return None, None
        attempt['outcome'] = 'delivered'
        self.evidence[ident] = {'node': node, 'request': sent_req, 'reply': reply}
        return ident, reply

    def counters(self) -> dict:
        return {'bytes': self.sent_bytes + self.reply_bytes, 'messages': self.messages,
                'virtual_ms': self.virtual_ms, 'server_cpu_ns': self.server_cpu_ns,
                'dropped_requests_or_replies': self.drops}
