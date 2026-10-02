"""Deterministic extraction of function-sized public source and query fixtures."""
from __future__ import annotations
import ast
import json
import textwrap
from collections import Counter
from pathlib import Path
from .model import features

# Explicit syntactic substitutions, not assertions of semantic equivalence.
ALTERNATES = {'call:loads':'call:load', 'call:dumps':'call:dump',
              'call:get':'call:__getitem__', 'call:items':'call:values',
              'call:encode':'call:decode', 'call:append':'call:extend',
              'call:join':'call:split', 'name:args':'name:kwargs'}


def build(root: Path) -> dict:
    payloads = {}; by_repo = {}; excluded = []; files = 0
    for repo in sorted((root/'inputs/sources').iterdir()):
        if not repo.is_dir():
            continue
        items = []
        for path in sorted(repo.rglob('*.py')):
            files += 1
            try:
                # Preserve the consumed file bytes; decoding policy is explicit UTF-8.
                text = path.read_text(encoding='utf-8')
                tree = ast.parse(text)
            except (UnicodeDecodeError, SyntaxError) as exc:
                excluded.append({'path':str(path.relative_to(root)), 'reason':type(exc).__name__})
                continue
            lines = text.splitlines(keepends=True)
            counter = Counter()
            def visit(node, parents):
                kind = type(node).__name__
                nested = parents
                if kind in {'ClassDef', 'FunctionDef', 'AsyncFunctionDef'}:
                    qual = '.'.join(parents + [node.name]); nested = parents + [node.name]
                    if kind != 'ClassDef':
                        counter[qual] += 1
                        start = min([node.lineno] + [x.lineno for x in node.decorator_list])
                        code = ''.join(lines[start-1:node.end_lineno])
                        ident = f'{repo.name}/{path.relative_to(repo)}:{qual}:{counter[qual]}'
                        if len(code.encode()) > 20000:
                            excluded.append({'id':ident,'reason':'fragment-byte-bound'})
                        else:
                            try:
                                terms = features(code)
                            except (SyntaxError, IndentationError, RecursionError) as exc:
                                excluded.append({'id':ident,'reason':type(exc).__name__})
                            else:
                                if len(terms) >= 2:
                                    body = 'b' + str(len(payloads)).zfill(5)
                                    payloads[body] = {'source':code, 'features':list(terms),
                                        'origin':str(path.relative_to(root)), 'lines':[start,node.end_lineno]}
                                    items.append((ident,body))
                                else:
                                    excluded.append({'id':ident,'reason':'fewer-than-two-labels'})
                for child in ast.iter_child_nodes(node):
                    visit(child,nested)
            visit(tree,[])
        by_repo[repo.name] = items
    # Query seeds are disjoint documents; deterministic stratification precedes all tests.
    seeds = []; initial = [dict() for _ in range(3)]; counts = {}
    for ordinal, (name, items) in enumerate(sorted(by_repo.items())):
        n = min(8, len(items)//2)
        positions = {((j+1)*len(items))//(n+1) for j in range(n)}
        for i, item in enumerate(items):
            if i in positions:
                seeds.append((name,)+item)
            else:
                initial[ordinal % 3][item[0]] = item[1]
        counts[name] = {'functions':len(items), 'query_seeds':len(positions),
                        'indexed':len(items)-len(positions), 'shard':ordinal%3}
    df = Counter()
    for state in initial:
        for body in state.values():
            df.update(payloads[body]['features'])
    queries = []
    for i, (repo, ident, body) in enumerate(seeds):
        terms = payloads[body]['features']
        base = sorted(terms,key=lambda t:(0 if t.startswith('call:') else 1,df[t],t))[:4]
        other = list(dict.fromkeys(ALTERNATES.get(t,t) for t in base))
        plan = [base] if other == base else [base,other]
        queries.append({'id':f'q{i:03d}', 'source_body':body, 'seed_id':ident,
                        'origin_repo':repo,'plan':plan,'k':5,'choice':i})
    report = {'source_files':files, 'payload_count':len(payloads), 'indexed_fragments':sum(map(len,initial)),
              'queries':len(queries),'alternate_queries':sum(len(q['plan'])>1 for q in queries),
              'repositories':counts,'excluded':excluded,
              'selection':'all parsable functions <=20000 bytes with >=2 labels; eight disjoint query seeds per project'}
    return {'initial':initial,'payloads':payloads,'queries':queries,'extraction':report}
