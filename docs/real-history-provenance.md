# Held-out cachetools function-history provenance

## Purpose and boundary

This input closes two narrow validation gaps in the primary campaign: all primary changes were generated, and all primary source replicas shared one process. It does **not** create a full repository replay. The validation projects published Python function-body changes from one module onto the exact retained base module, then runs the same source protocol in six independent operating-system processes on one host.

Repository: `tkem/cachetools`. License: MIT. Exact base: tag `v7.1.4`, commit `48284d73d0a8834c9c50f8d41bb99e6f93b2dfed`, retained path `inputs/sources/cachetools/cachetools/__init__.py`, SHA-256 `fadf75eeab248d737ee23903efd5363e117719b47c434d7fd02a2f166a955b3c`, Git blob `1b299c544519583cbcd765a01d58c75ad8802062`. Observed interval head: `4500e3d04288738d25acbb4973eb3c3e1bf41db9` (Release v7.1.8.).

## Selection rule

All six commits in the base-to-head interval whose published patch changes or adds a Python function body in src/cachetools/__init__.py. Module-only, test, documentation, CI, and type-stub edits are excluded. The six commits below are therefore a complete set under that rule, not a cherry-picked performance subset. Tests, documentation, workflow files, type stubs, version metadata, and non-function module edits are excluded. The projection stores the exact replacement/inserted function text and verifies a frozen whole-module digest after every stage.

| Commit | Date | Published message | Projected functions | Stage SHA-256 |
|---|---|---|---|---|
| `d5c7eea7e52d` | 2026-07-07 | Reject negative cache item sizes | Cache.__setitem__ | `a4869ebbdf7be86c5b4f699dba1555d0e2783cf608cf83f7baa2e7e99c7ddc19` |
| `c0fdf6abab38` | 2026-07-22 | Fix TLRUCache silently keeping stale value on expired overwrite | TLRUCache.__setitem__ | `6254b5c84ae6eb13f36790d5d7d0f3a4d13377663cb2c3df81aafd1b3d97b09a` |
| `13bb86a55e36` | 2026-07-23 | Minor style improvements to keep ruff happy. | Cache.__init__ | `2716058087ae0753b6f32938f2fa18e41881189d396b99894ab5978868540c7a` |
| `39b31bc9b63a` | 2026-08-01 | Fix Cache.__setitem__ over-evicting when growing an existing key | Cache.__setitem__ | `37c8008957eb187cdfd13c5ff2cf9509b77c1315b604748e711ba438f24bb907` |
| `ccaa8c8c882b` | 2026-08-01 | Minor stylistic improvements. | TLRUCache.__delitem, TLRUCache.__setitem__ | `0b03601412b793a85820de3dd283e780253d971d259e614b9fe62f62b94384d6` |
| `dd181c5a72a7` | 2026-08-28 | Reject negative maxsize in Cache.__init__ | Cache.__init__ | `011dff068944057af87e7779fc62d709158fcf0b2040d73ec73b1a6d4b4aba35` |

## Query and execution scope

The campaign runs all 64 query seeds already excluded from the indexed corpus plus four deterministic plans derived from rare stable labels in the changed functions. The four targeted plans ensure the oracle changes on ten query/commit observations; they are selected by a frozen rule, not by measured communication. There is no human or production query log.

Each of three logical shards has two source replicas, each in a distinct spawned child process and loopback listener. Before delivery of the third projected commit, one process is terminated rather than logically disabled. A fresh process with a different PID starts from the frozen base and replays the admitted events. Final snapshots from all six processes are compared with an independently reconstructed map. A separate both-owner outage probe must return partial and refuse overlay completeness, then recover to an exact complete answer after healing.

## Reproduction and non-claims

`python run_real_history_suite.py` checks base and stage digests, four directed tests, the six-process campaign, source-receipt replay, checker replay, the unsafe control, and the frozen semantic/wire contract. It does not establish multi-host failure isolation, disk durability, network latency, repository-wide build semantics, Git rename/merge behavior, query prevalence, or semantic-search quality. Published commit URLs are retained in `inputs/git-history/cachetools-function-history.json`; no changing default branch is needed for reproduction.
