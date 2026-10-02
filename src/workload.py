"""Frozen synthetic repository changes over the retained public-source bodies."""
import random
from .model import ranked, apply

CASES = ('fresh','index-lag','log-gap','partition','crash','rebalance','hot-shard','compaction')

class Writer:
    def __init__(self, fixture, seed):
        self.fixture = fixture
        self.rng = random.Random(seed)
        self.views = [dict(x) for x in fixture['initial']]
        self.history = [[],[],[]]
        # No query-seed body is injected into the index as a generated addition.
        self.bodies = sorted({b for state in self.views for b in state.values()})
        self.queries = list(fixture['queries'][1:])  # pilot seed q000 is excluded
        self.rng.shuffle(self.queries)

    def step(self, tick, query, case):
        new = []
        for shard in range(3):
            if case == 'hot-shard' and shard != 0 and tick % 8:
                continue
            state = self.views[shard]
            existing = sorted(state)
            positive = ranked(state, self.fixture['payloads'], query['plan'])
            chosen = (positive[0]['id'] if tick % 2 and positive else self.rng.choice(existing)) if existing else None
            action = tick % 4
            if action == 1 and chosen is not None:
                changes = [[chosen, None]]
            elif action == 2 and chosen is not None:
                pool = [b for b in self.bodies if b != state[chosen]]
                changes = [[chosen, self.rng.choice(pool)]]
            elif action == 3:
                changes = [[f'generated/{shard}/{tick}', self.rng.choice(self.bodies)]]
            else:
                # A single-shard, atomic branch-style replacement of two paths.
                changes = [[f'branch/{shard}/{tick}', self.rng.choice(self.bodies)]]
                if chosen is not None:
                    changes.append([chosen, None])
            event = {'shard':shard, 'seq':len(self.history[shard])+1, 'changes':changes,
                     'operation':('delete','rewrite','add','branch-replacement')[(action-1)%4]}
            self.history[shard].append(event)
            apply(state, event)
            new.append(event)
        return new

    @property
    def cut(self):
        return [len(x) for x in self.history]
