"""Network policy for shared event feeds and bounded continuation tokens."""
from __future__ import annotations
from copy import deepcopy
from .continuation import (advance_token, merge_tokens, token_from_prefix,
                           sufficient_repair_length)
from .model import MAX_EVENTS, MAX_PREFIX


class FeedStore:
    """Coordinator-held, query-independent contiguous update feed."""
    def __init__(self, epoch: int = 0, base: list[int] | None = None):
        self.reset(epoch, base or [0, 0, 0])

    def reset(self, epoch: int, base: list[int]) -> None:
        self.epoch = epoch
        self.base = list(base)
        self.frontiers = list(base)
        self.events: list[dict[int, dict]] = [dict(), dict(), dict()]

    def accept(self, receipt: dict) -> None:
        shard = receipt['shard']
        if receipt.get('kind') != 'events' or receipt['epoch'] != self.epoch:
            raise ValueError('feed receipt binding')
        if receipt['lo'] != self.frontiers[shard]:
            raise ValueError('feed gap')
        expected = list(range(receipt['lo'] + 1, receipt['hi'] + 1))
        if [event['seq'] for event in receipt['events']] != expected:
            raise ValueError('feed sequence')
        for event in receipt['events']:
            if event['shard'] != shard:
                raise ValueError('feed shard')
            self.events[shard][event['seq']] = deepcopy(event)
        self.frontiers[shard] = receipt['hi']

    def covers(self, shard: int, lo: int, hi: int) -> bool:
        if not self.base[shard] <= lo <= hi <= self.frontiers[shard]:
            return False
        return all(seq in self.events[shard] for seq in range(lo + 1, hi + 1))

    def receipt(self, shard: int, lo: int, hi: int) -> dict:
        if not self.covers(shard, lo, hi):
            raise ValueError('feed coverage')
        return {'kind':'events','shard':shard,'epoch':self.epoch,'lo':lo,'hi':hi,
                'events':[deepcopy(self.events[shard][seq]) for seq in range(lo + 1, hi + 1)]}

    def compact(self, floors: list[int]) -> None:
        """Discard events that every live token has already consumed."""
        if len(floors) != 3:
            raise ValueError('feed compaction vector')
        for shard, floor in enumerate(floors):
            if type(floor) is not int or not self.base[shard] <= floor <= self.frontiers[shard]:
                raise ValueError('feed compaction floor')
            for seq in [seq for seq in self.events[shard] if seq <= floor]:
                del self.events[shard][seq]
            self.base[shard] = floor

    def safe_compaction_floors(self, consumed: list[int]) -> list[int]:
        """Keep one full admitted journal window behind each frontier.

        A repair prefix may lag behind every carried token.  Retaining the last
        MAX_EVENTS events is sufficient for any prefix that an admitted source
        journal can still bridge; older prefixes must fail closed or install a
        checkpoint.
        """
        if len(consumed) != 3:
            raise ValueError('feed compaction vector')
        floors = []
        for shard, value in enumerate(consumed):
            if type(value) is not int or value < self.base[shard]:
                raise ValueError('feed compaction floor')
            retention_floor = max(self.base[shard], self.frontiers[shard] - MAX_EVENTS)
            floors.append(min(value, retention_floor))
        return floors

    def logical_state(self) -> dict:
        return {
            'epoch': self.epoch,
            'base': list(self.base),
            'frontiers': list(self.frontiers),
            'events': [
                [deepcopy(events[seq]) for seq in sorted(events)]
                for events in self.events
            ],
        }


async def fetch_events(network, feed: FeedStore, shard: int, hi: int,
                       epoch: int, owners: list[list[int]]):
    """Fetch one query-independent suffix; return receipt identity and reply."""
    lo = feed.frontiers[shard]
    if hi == lo:
        return None, None
    for node in owners[shard]:
        ident, reply = await network.rpc(node, {'op':'events','shard':shard,'epoch':epoch,
                                                'lo':lo,'hi':hi})
        if reply and reply.get('kind') == 'events':
            feed.accept(reply)
            return ident, reply
    return None, None


async def fetch_prefix(network, query: dict, shard: int, epoch: int,
                       owners: list[list[int]], length: int):
    for node in owners[shard]:
        ident, reply = await network.rpc(node, {'op':'prefix','shard':shard,'epoch':epoch,
                                                'query':query,'length':length})
        if reply and reply.get('kind') == 'prefix':
            return ident, reply
    return None, None


class ContinuationSession:
    def __init__(self, payloads: dict, feed: FeedStore, base_capacity: int = 5):
        if not 1 <= base_capacity <= MAX_PREFIX:
            raise ValueError('base capacity')
        self.payloads = payloads
        self.feed = feed
        self.base_capacity = base_capacity
        self.tokens: dict[str, dict[int, dict]] = {}

    def reset(self) -> None:
        self.tokens.clear()

    def minimum_frontiers(self) -> list[int]:
        """Lowest consumed cut across all retained standing-query tokens."""
        floors = list(self.feed.frontiers)
        for shard in range(3):
            ats = [shards[shard]['at'] for shards in self.tokens.values() if shard in shards]
            if ats:
                floors[shard] = min(ats)
        return floors

    async def acquire(self, network, query: dict, cut: list[int], epoch: int,
                      owners: list[list[int]]) -> dict[str, dict]:
        """Cold-start one standing query; returns checker-facing receipt specs."""
        installed: dict[str, dict] = {}
        current = self.tokens.setdefault(query['id'], {})
        for shard in range(3):
            ident, reply = await fetch_prefix(network, query, shard, epoch, owners,
                                              max(query['k'], self.base_capacity))
            if reply is None or reply['at'] != cut[shard]:
                continue
            token = token_from_prefix(query, reply, max(query['k'], self.base_capacity))
            current[shard] = token
            installed[str(shard)] = {'receipt':ident,'capacity':token['capacity']}
        return installed

    def _advance_current(self, query: dict, cut: list[int], epoch: int) -> set[int]:
        current = self.tokens.setdefault(query['id'], {})
        missing = set()
        for shard in range(3):
            token = current.get(shard)
            if token is None or token['epoch'] != epoch or token['at'] > cut[shard]:
                missing.add(shard); continue
            if token['at'] < cut[shard]:
                if not self.feed.covers(shard, token['at'], cut[shard]):
                    missing.add(shard); continue
                current[shard] = advance_token(
                    token, self.feed.receipt(shard, token['at'], cut[shard]), self.payloads)
        return missing

    async def query(self, network, query: dict, cut: list[int], epoch: int,
                    owners: list[list[int]]) -> tuple[dict, dict[str, dict]]:
        """Advance locally, then repair only rank-blocking shards."""
        missing = self._advance_current(query, cut, epoch)
        current = self.tokens.setdefault(query['id'], {})
        result = merge_tokens(query, cut, epoch,
                              {s:t for s,t in current.items() if s not in missing}, missing)
        repairs: dict[str, dict] = {}
        # A blocker may disappear after another shard raises the global kth key.
        # Repair one most-threatening shard at a time.  A fresh top-(k+d)
        # prefix is sufficient when d distinct IDs changed after its base cut.
        attempted: set[int] = set()
        while result['status'] != 'partial' and result['rank_blockers']:
            blocker = min((s for s in result['rank_blockers'] if s not in attempted),
                          key=lambda s: tuple(current[s]['tail']), default=None)
            if blocker is None:
                break
            attempted.add(blocker)
            token = current[blocker]
            # The best available index prefix may be older than the carried
            # token.  Count changes from the compacted feed floor, which is a
            # safe upper bound for every prefix cut that the feed can repair.
            suffix = self.feed.receipt(
                blocker, self.feed.base[blocker], cut[blocker])['events']
            try:
                required = sufficient_repair_length(query, suffix)
                guaranteed = True
            except ValueError:
                required = MAX_PREFIX
                guaranteed = False
            length = max(self.base_capacity, required)
            ident, prefix = await fetch_prefix(network, query, blocker, epoch, owners, length)
            if prefix is None or prefix['at'] > cut[blocker] or \
                    not self.feed.covers(blocker, prefix['at'], cut[blocker]):
                continue
            refreshed = token_from_prefix(query, prefix, length)
            if refreshed['at'] < cut[blocker]:
                refreshed = advance_token(
                    refreshed, self.feed.receipt(blocker, refreshed['at'], cut[blocker]),
                    self.payloads)
            current[blocker] = refreshed
            repairs[str(blocker)] = {
                'receipt': ident, 'capacity': length,
                'required_by_k_plus_d': required, 'guaranteed': guaranteed,
            }
            result = merge_tokens(query, cut, epoch,
                                  {s:t for s,t in current.items() if s not in missing}, missing)
            if guaranteed and blocker in result['rank_blockers']:
                raise AssertionError('k+d repair failed to remove blocker')
        result['repairs'] = deepcopy(repairs)
        return result, repairs
