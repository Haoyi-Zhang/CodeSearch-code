"""Independent source-work census from maps, histories and receipt cuts.

No producer, coordinator, service or PostingIndex import. Named operations are
reconstructed, not CPU, instructions, JSON work or sorting comparisons.
"""
from collections import Counter
from src.checker import Checker, need, same_json

PHASES = ('initialization', 'maintenance', 'acquisition', 'refill')


def source_work(trial, payloads):
    labels = Checker(payloads).labels
    counts = {p: Counter() for p in PHASES}
    current = [dict(s) for s in trial['initial']]
    versions = [[dict(s)] for s in current]

    def assignment(shard, ident, body, phase):
        old = current[shard].get(ident)
        counts[phase]['identifier_assignments'] += 1
        counts[phase]['posting_membership_updates'] += (
            len(labels[old]) if old is not None else 0) + (
            len(labels[body]) if body is not None else 0)
        if body is None:
            current[shard].pop(ident, None)
        else:
            current[shard][ident] = body

    for shard, state in enumerate(trial['initial']):
        counts['initialization']['identifier_assignments'] += len(state)
        counts['initialization']['posting_membership_updates'] += sum(len(labels[b]) for b in state.values())
    # Materialize independent snapshots, including events not yet indexed.
    for shard, events in enumerate(trial['history']):
        for event in events:
            view = dict(versions[shard][-1])
            for ident, body in event['changes']:
                if body is None:
                    view.pop(ident, None)
                else:
                    view[ident] = body
            versions[shard].append(view)
    indexed = [0, 0, 0]
    delivered = [0, 0, 0]
    moments = {}
    for record in trial['records']:
        cut = record['result']['cut']
        previous = moments.setdefault(record['sequence'], cut)
        need(same_json(previous, cut), 'one target vector per comparison tick')
    lag = trial['summary']['index_lag']
    need(type(lag) is int and lag >= 0, 'index lag')
    for _, cut in sorted(moments.items()):
        for shard in range(3):
            for sequence in range(delivered[shard]+1, cut[shard]+1):
                target = max(0, sequence-lag)
                for at in range(indexed[shard]+1, target+1):
                    event = trial['history'][shard][at-1]
                    for ident, body in event['changes']:
                        assignment(shard, ident, body, 'maintenance')
                indexed[shard] = target
            delivered[shard] = cut[shard]
    acquisition = len(trial['queries'])*3
    for ident, entry in sorted(trial['transcript'].items(), key=lambda pair: int(pair[0])):
        req, rep = entry['request'], entry['reply']
        if req['op'] != 'prefix':
            continue
        phase = 'acquisition' if int(ident) <= acquisition else 'refill'
        state = versions[rep['shard']][rep['at']]
        positive = set()
        for alternate in req['query']['plan']:
            for term in alternate:
                for name, body in state.items():
                    if term in labels[body]:
                        counts[phase]['posting_visits'] += 1
                        positive.add(name)
        counts[phase]['positive_rows_built'] += len(positive)
    return {p: dict(c) for p, c in counts.items()}


def check_source_work(trial, payloads):
    phases = source_work(trial, payloads)
    total = Counter()
    for count in phases.values():
        total.update(count)
    recorded = trial['summary']['source_work']
    # Counter equality admits absent zero-valued fields; scalar types do not.
    need(all(type(v) is int and v >= 0 for v in recorded.values()), 'source counter type')
    need(Counter(recorded) == total, 'source work census')
    if 'source_work_by_phase' in trial['summary']:
        supplied = trial['summary']['source_work_by_phase']
        need(set(supplied) == set(PHASES), 'source phase inventory')
        for phase in PHASES:
            need(all(type(v) is int and v >= 0 for v in supplied[phase].values()), 'phase counter type')
            need(Counter(supplied[phase]) == Counter(phases[phase]), 'source phase census')
    return phases
