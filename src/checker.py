"""Independent receipt checker and full replay oracle.

No import of retrieval, coordinator, service, or corpus-builder code. Receipt
truth/authenticity is an explicit external trust input in the crash-only model.
The optional raw replay oracle checks that assumption on the supplied fixtures.
"""
from __future__ import annotations
import ast
import textwrap


class InvalidCertificate(ValueError):
    pass


def need(condition: bool, reason: str) -> None:
    if not condition:
        raise InvalidCertificate(reason)


def same_json(left, right) -> bool:
    """JSON binding equality must not identify booleans/reals with integers."""
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(same_json(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(same_json(a, b) for a, b in zip(left, right))
    return left == right


def syntax_labels(source: str) -> set[str]:
    # Independently recursive traversal, rather than producer ast.walk.
    answer: set[str] = set()
    def visit(n):
        kind = n.__class__.__name__
        if kind == 'Name':
            answer.add('name:' + n.id.lower())
        if kind == 'Attribute':
            answer.add('attr:' + n.attr.lower())
        if kind == 'Call':
            f = n.func
            if f.__class__.__name__ == 'Name':
                answer.add('call:' + f.id.lower())
            elif f.__class__.__name__ == 'Attribute':
                answer.add('call:' + f.attr.lower())
        for _, value in ast.iter_fields(n):
            if isinstance(value, list):
                for child in value:
                    if isinstance(child, ast.AST):
                        visit(child)
            elif isinstance(value, ast.AST):
                visit(value)
    visit(ast.parse(textwrap.dedent(source)))
    return answer


class Checker:
    def __init__(self, payloads: dict):
        self.labels = {b: syntax_labels(d['source']) for b, d in payloads.items()}

    def value(self, body: str, plan: list[list[str]]) -> int:
        need(body in self.labels, 'unknown body')
        return max(len(self.labels[body].intersection(alternate)) for alternate in plan)

    def ordered(self, rows: list[dict]) -> list[dict]:
        self.check_rows(rows)
        return sorted(rows, key=lambda row: (-row['score'], row['id']))

    @staticmethod
    def check_rows(rows: list[dict]) -> None:
        """Validate wire types before equality can equate True or 1.0 with 1."""
        need(type(rows) is list, 'row list')
        for row in rows:
            need(type(row) is dict and set(row) == {'id', 'body', 'score'}, 'row schema')
            need(isinstance(row['id'], str) and isinstance(row['body'], str), 'row identifiers')
            need(type(row['score']) is int and row['score'] > 0, 'positive integer score')

    @staticmethod
    def check_tail(tail) -> None:
        need(tail is None or (type(tail) is list and len(tail) == 2
             and type(tail[0]) is int and tail[0] < 0
             and isinstance(tail[1], str)), 'boundary schema')

    @staticmethod
    def check_request(query, cut, epoch) -> None:
        need(type(epoch) is int and epoch >= 0, 'epoch')
        need(type(cut) is list and len(cut) == 3
             and all(type(x) is int and x >= 0 for x in cut), 'cut')
        need(type(query) is dict and type(query.get('k')) is int
             and 1 <= query['k'] <= 20, 'k')
        plan = query.get('plan')
        need(type(plan) is list and 1 <= len(plan) <= 2, 'plan')
        need(all(type(a) is list and 1 <= len(a) <= 4
                 and all(type(t) is str for t in a) and len(a) == len(set(a))
                 for a in plan), 'plan alternate')

    def check(self, cert: dict, trusted: dict[int, dict], query: dict, cut: list[int],
              epoch: int, owners: list[list[int]]) -> bool:
        self.check_request(query, cut, epoch)
        need(same_json(cert['query'], query) and same_json(cert['cut'], cut)
             and same_json(cert['epoch'], epoch),
             'request binding')
        need(len(cut) == 3 and all(type(x) is int and x >= 0 for x in cut), 'cut')
        need(type(query['k']) is int and 1 <= query['k'] <= 20, 'k')
        plan = query['plan']
        need(1 <= len(plan) <= 2 and all(1 <= len(a) <= 4 and len(a) == len(set(a)) for a in plan), 'plan')
        all_candidates = {}
        tails, lags = {}, {}
        pairs = cert['pairs']
        need(set(pairs).issubset({'0', '1', '2'}), 'shards')
        for label, ids in pairs.items():
            shard = int(label)
            need(len(ids) == 2 and ids[0] != ids[1], 'receipt pair')
            need(all(type(i) is int and i in trusted for i in ids), 'receipt identity')
            pe, de = (trusted[i] for i in ids)
            need(type(pe['node']) is int and type(de['node']) is int
                 and pe['node'] in owners[shard] and de['node'] in owners[shard], 'owner')
            p, d = pe['reply'], de['reply']
            need(p.get('kind') == 'prefix' and d.get('kind') == 'delta', 'receipt kind')
            for e in (pe, de):
                r, req = e['reply'], e['request']
                need(same_json(r['shard'], shard) and same_json(r['epoch'], epoch) and r['plan'] == plan,
                     'reply binding')
                need(same_json(req['shard'], shard) and same_json(req['epoch'], epoch)
                     and same_json(req['query'], query),
                     'RPC binding')
            need(pe['request']['op'] == 'prefix' and de['request']['op'] == 'delta', 'operation')
            need(type(p['at']) is int and 0 <= p['at'] <= cut[shard], 'base cut')
            need(same_json(d['lo'], p['at']) and same_json(d['hi'], cut[shard]), 'interval')
            need(same_json(de['request']['lo'], d['lo']) and same_json(de['request']['hi'], d['hi']), 'range binding')
            need(type(pe['request']['length']) is int and 0 <= pe['request']['length'] <= 64
                 and len(p['rows']) <= pe['request']['length'] and len(d['rows']) <= query['k'], 'length')
            changed = set(d['changed'])
            need(len(changed) == len(d['changed']) and d['changed'] == sorted(changed), 'dirty set')
            for rlist in (p['rows'], d['rows']):
                need(rlist == self.ordered(rlist), 'source order')
                need(len({x['id'] for x in rlist}) == len(rlist), 'duplicate source ID')
                for row in rlist:
                    need(set(row) == {'id', 'body', 'score'}, 'row schema')
                    need(type(row['score']) is int and row['score'] > 0, 'positive integer score')
                    need(row['score'] == self.value(row['body'], plan), 'score')
            need(all(x['id'] in changed for x in d['rows']), 'delta membership')
            local = [x for x in p['rows'] if x['id'] not in changed] + d['rows']
            for row in local:
                need(row['id'] not in all_candidates, 'cross-shard duplicate')
                all_candidates[row['id']] = row
            boundary = p['boundary']
            if boundary is not None:
                need(len(boundary) == 2 and type(boundary[0]) is int and boundary[0] < 0
                     and isinstance(boundary[1], str), 'boundary schema')
                need(len(p['rows']) == pe['request']['length'], 'unexhausted prefix')
                need(not p['rows'] or (-p['rows'][-1]['score'], p['rows'][-1]['id']) < tuple(boundary),
                     'boundary order')
            tails[shard] = boundary
            lags[label] = cut[shard] - p['at']
        expected = self.ordered(list(all_candidates.values()))[:query['k']]
        missing = [x for x in range(3) if str(x) not in pairs]
        obstructed = []
        for shard in sorted(tails):
            boundary = tails[shard]
            if boundary is not None:
                if len(expected) < query['k'] or (-expected[-1]['score'], expected[-1]['id']) >= tuple(boundary):
                    obstructed.append(shard)
        status = 'partial' if missing else 'rank-underdetermined' if obstructed else 'complete'
        self.check_rows(cert['rows'])
        need(cert['rows'] == expected, 'candidate merge')
        need(same_json(cert['missing_shards'], missing), 'coverage')
        need(same_json(cert['rank_blockers'], obstructed), 'rank diagnosis')
        need(same_json(cert['index_lag_events'], lags), 'freshness vector')
        need(cert['status'] == status, 'completeness status')
        return True

    def check_overlay(self, result: dict, trusted: dict[int, dict], query: dict,
                      cut: list[int], epoch: int, owners: list[list[int]]) -> bool:
        """The strong baseline receives the same independent merge-checking aid."""
        self.check_request(query, cut, epoch)
        need(set(result['receipts']).issubset({'0','1','2'}), 'overlay shards')
        candidates = {}
        for label, ident in result['receipts'].items():
            shard = int(label)
            need(type(ident) is int and ident in trusted, 'overlay receipt')
            entry = trusted[ident]; req, reply = entry['request'], entry['reply']
            need(type(entry['node']) is int and entry['node'] in owners[shard], 'overlay owner')
            need(req['op'] == 'overlay' and same_json(req['to'], cut[shard])
                 and same_json(req['query'], query) and same_json(req['epoch'], epoch)
                 and same_json(req['shard'], shard), 'overlay request')
            need(reply['kind'] == 'overlay' and same_json(reply['shard'], shard)
                 and same_json(reply['at'], cut[shard]) and same_json(reply['epoch'], epoch)
                 and reply['plan'] == query['plan'], 'overlay reply')
            rows = reply['rows']
            need(len(rows) <= query['k'] and self.ordered(rows) == rows, 'overlay order')
            for row in rows:
                need(row['id'] not in candidates and type(row['score']) is int
                     and row['score'] > 0 and row['score'] == self.value(row['body'],query['plan']),
                     'overlay row')
                candidates[row['id']] = row
        expected = self.ordered(list(candidates.values()))[:query['k']]
        covered = sorted(map(int,result['receipts']))
        self.check_rows(result['rows'])
        need(result['rows'] == expected and same_json(result['coverage'], covered), 'overlay merge')
        need(type(result['claim_complete']) is bool
             and result['claim_complete'] == (covered == [0,1,2]), 'overlay completeness')
        return True

    def oracle(self, initial: list[dict], events: list[list[dict]], cut: list[int], query: dict) -> list[dict]:
        need(len(initial)==len(events)==len(cut)==3 and all(type(cut[s]) is int
             and 0<=cut[s]<=len(events[s]) for s in range(3)), 'oracle cut coverage')
        universe = {}
        for shard in range(3):
            view = dict(initial[shard])
            # Deliberately direct last-assignment replay, independent of service state.
            for expected, event in enumerate(events[shard][:cut[shard]], 1):
                need(event['seq'] == expected and event['shard'] == shard, 'oracle history')
                for ident, body in event['changes']:
                    if body is None:
                        if ident in view:
                            del view[ident]
                    else:
                        view[ident] = body
            for ident, body in view.items():
                need(ident not in universe, 'oracle ownership')
                universe[ident] = body
        rows = []
        for ident, body in universe.items():
            value = self.value(body, query['plan'])
            if value:
                rows.append({'id': ident, 'body': body, 'score': value})
        return self.ordered(rows)[:query['k']]

    def sound_rows(self, rows: list[dict], initial: list[dict], events: list[list[dict]],
                   cut: list[int], query: dict) -> int:
        need(len(initial)==len(events)==len(cut)==3 and all(type(cut[s]) is int
             and 0<=cut[s]<=len(events[s]) for s in range(3)), 'soundness cut coverage')
        current = {}
        for shard, base in enumerate(initial):
            view = dict(base)
            for event in events[shard][:cut[shard]]:
                for ident, body in event['changes']:
                    if body is None:
                        view.pop(ident, None)
                    else:
                        view[ident] = body
            current.update(view)
        return sum(type(r['score']) is not int or current.get(r['id']) != r['body'] or self.value(r['body'], query['plan']) != r['score']
                   or r['score'] <= 0 for r in rows)
