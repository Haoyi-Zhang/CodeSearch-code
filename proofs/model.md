# Model and arguments for continuation-certified top-k

## 1. Objects, cuts, and ranking

There are three logical shards. Shard `s` owns a map from document identifiers to immutable body identifiers. A writer emits an ordered sequence of atomic per-shard events. Event `n` has sequence number `n` and replaces zero or more identifiers with new bodies or tombstones; identifiers within one batch are distinct. A request names a writer-declared target vector `T=(T_0,T_1,T_2)` and an ownership epoch. It does not mean “the latest state” during a partition and does not imply a causally closed multi-shard transaction.

For the artifact, a query contains one or two alternatives, each containing at most four typed AST labels. A body’s score is the maximum label overlap with an alternative. Positive rows are ordered by the total key `(-score, identifier)`. The score is body-local and the plan is frozen. Any change to the plan, parser, score definition, ownership epoch, or target cut invalidates reuse unless explicitly rebound.

## 2. Replacement decomposition

Let `I_v` be a shard map at prefix `v`; let `E(v,T]` be every admitted event from `v+1` through `T`; let `D` be the set of identifiers assigned in that suffix; and let `N` be the final non-tombstone assignment for each identifier in `D`. Then

`I_T = (I_v restricted outside D) union N`.

For an identifier not in `D`, no suffix event changes its membership or body. For an identifier in `D`, its last assignment determines its target membership and body. The two domains are disjoint, proving the identity pointwise. This covers an atomic branch-style replacement only when its paths are one admitted shard batch. It is not a cross-shard transaction argument.

## 3. Continuation-token invariant

A valid token for query `q`, shard `s`, epoch `e`, and cut `v` contains:

- a bounded candidate buffer `B` of target-live positive rows;
- a capacity `L >= k`;
- a tail key `b`, or `None` for exhaustion; and
- the request binding `(q,s,e,v)`.

The invariant is:

1. every row in `B` is live at `v` with the recorded body and score;
2. identifiers in `B` are unique and rows are ordered; and
3. every positive live row not in `B` has rank key at least `b`; if `b=None`, no omitted positive row exists.

A trusted local top-`L` prefix and its next key establish this invariant. The online checker validates binding, schema, score arithmetic, order, and all subsequent transitions. In the crash-only model it treats the local prefix issuer as truthful about omissions. The retained-history replay separately reconstructs every prefix to test that assumption for the supplied traces. No Byzantine or cryptographic completeness claim follows.

## 4. Invariant preservation under a complete suffix

Given a valid token at `v` and a contiguous event receipt for exactly `(v,T]`, fold the events to their last assignment per identifier. Remove every dirty identifier from `B`; insert every target-live dirty identifier whose new body has positive score; sort the resulting known pool; retain the first `L` rows; and let `d` be the first dropped known key, if any. The new tail is `min(b,d)`, treating `None` as positive infinity.

**Lemma 1 (sound rows).** Every retained row is target-live with the recorded score.

By replacement decomposition, an old retained row survives only if its identifier is not dirty. Every inserted dirty row is scored from the suffix’s final live assignment. Thus no stale body or tombstone remains.

**Lemma 2 (tail preservation).** Every target-live positive row omitted from the new buffer has key at least `min(b,d)`.

An omitted row is either: (i) an unchanged row omitted before, hence at least `b`; or (ii) a known row discarded after sorting the updated pool, hence at least `d`. Dirty old rows do not survive, and all positive final dirty assignments were inserted. Taking the smaller bound covers both classes. Therefore the advanced token is valid at `T` without querying an index.

The token need not remain the exact local top-`L`; it is a sound candidate buffer plus a conservative omitted-row bound. Requiring an exact top-`L` after every update would cause unnecessary repairs and is not the implemented invariant.

## 5. Global exactness and blockers

At target vector `T`, merge the candidate buffers of all covered shards and take their first `k` rows `R`. For each covered shard with tail `b_s`, mark a blocker when `R` has fewer than `k` rows or the last key of `R` is not strictly better than `b_s`.

**Theorem 1 (no blocker implies exact top-k).** If all shards are covered and there is no blocker, `R` is exactly the top-k at `T`.

Every row in `R` is sound by Lemma 1. Any target-live positive row omitted from the union belongs to some shard and has key at least that shard’s tail by Lemma 2. With `k` returned rows and the kth key strictly better than every nonempty tail, no omitted row can enter the result. When fewer than `k` rows exist, exactness requires every tail to be exhausted. Deterministic identifier tie-breaking makes the strict comparison sufficient at equal scores.

If a shard is missing, rows from covered shards remain sound but completeness is not asserted. If a tail blocks, the returned rows may already equal the oracle; the conservative online test does not establish that equality. The status therefore distinguishes `partial`, `rank-underdetermined`, and `complete`.

## 6. Conservative blockers and compatible witnesses

A blocking boundary is a sufficient reason for this conservative checker to withhold a complete status, not a necessary characterization of incomplete information. Fix the entire query, plan, k, epoch, catalog, receipts, and events. Only when two states satisfy all of that evidence and have different exact top-k answers does indistinguishability require distinguishing evidence or a non-complete status. An omitted row strictly better than the kth key must be constructible without contradicting any of the fixed evidence. A free choice of a tail value is not enough.

For k=L=1, take unique IDs a<b with tied score one, prefix a and tail (-1,b). The complete suffix deletes a and assigns a score-one body to b. All other shards are empty. The advanced token returns b and still has tail (-1,b), so the strict blocker test fires. But the ID b is already represented; every other compatible omitted row is strictly worse. The answer is exact although this conservative rule does not certify it. The regression test test_tail_identity_is_conservative_not_necessary retains this counterexample.

## 7. Bounded repair

Let `d` be the number of distinct identifiers written in the complete suffix after a selected prefix cut `v`. Requesting the top `k+d` rows at `v` is sufficient to expose at least `k` unchanged survivors unless the positive list is exhausted: at most `d` returned identifiers can be removed or replaced by the suffix. Add all final positive dirty assignments, then apply the same tail test.

**Theorem 2 (k+d sufficiency).** Under the frozen body-local score and complete suffix, a prefix of length `k+d` plus the suffix removes that shard as a blocker whenever its target state contains enough candidates relevant to the global threshold; otherwise exhaustion is explicit.

The implementation fails closed when `k+d` exceeds `MAX_PREFIX=64`; it does not truncate the claimed sufficient length. A reserve can be requested for future reuse, but the primary campaign uses capacity `k=5` and repairs only an actual blocker.

## 8. Stateful checker and event-feed boundary

Token reuse is bound to the entire query object, including its plan and k, and its ownership epoch. Checking a whole result stages token changes and commits them only after every response field passes. Event receipts validate every batch entry before feed progress changes. Directed rejected-repair, rejected-advance, and rejected-batch tests verify unchanged state and successful legal retries. These are static-boundary regressions, not field-failure observations.

Events are fetched once into a coordinator-held, query-independent feed. Each source receipt is bound to shard, epoch, lower and upper sequence numbers and must contain exactly the contiguous sequence. The independent checker stores the same feed, recomputes token transitions without importing coordinator or producer code, installs only bound repair prefixes, and recomputes rows, blockers, and status. Events are compacted only after every live token has consumed them, while retaining one admitted journal window for a lagging repair prefix.

The checker’s event continuity detects gaps and malformed assignments. It does not authenticate a malicious writer or prove that a local prefix source omitted nothing. Epoch installation is a trusted administrative barrier, not consensus or online shard migration. These are explicit trust boundaries.

## 9. Failure-closed behavior and bounded state

A replica advances its receive frontier only through consecutive sequence numbers; duplicate identical events are idempotent and conflicting retained events are rejected. Index and journal frontiers are separate. A missing event range, crashed owner, partition, request below the retained floor, unsupported repair length, or foreign epoch yields missing/underdetermined status rather than a false complete answer. Compaction first materializes a checkpoint and then removes old events.

Each source admits at most 128 post-checkpoint event positions, 128 pending entries, 16 identifiers per batch, `k<=20`, and prefix capacity `<=64`. The coordinator feed and checker compact consumed events and retain a bounded repair window. Query tokens contain at most `L` rows per query-shard. These bounds cover coordination metadata in the finite artifact, not arbitrary corpus size, source strings, production queues, or indefinite offline clients.

## 10. Cold-query dominance boundary

For a one-shot query at a co-resident source, an exact delta overlay can filter dirty identifiers from stale postings and merge final dirty rows while returning at most the local top-k. A prefix-plus-delta certificate transmits up to `L` old rows, dirty identifiers, and changed rows. With `L>=k`, it has no row-count advantage and was 2.15x larger in the retained one-shot campaign. The new result does not contradict that boundary: its advantage comes from amortizing validated query state and a shared event feed across repeated queries. Cold, one-use queries should use exact overlay.

A full coordinator mirror is another strong alternative. It can answer without per-query source fetch after receiving updates, but retains the complete shard maps and postings. The evaluation counts its update traffic and state. The continuation design occupies a middle point: more state than a cut-only cache, substantially less state than a full mirror, and less source communication in the tested standing-query schedule.

## 11. Corpus-dependent score boundary

Replacement decomposition preserves memberships and bodies, not corpus-dependent scores. With TF-IDF/cosine ranking, adding documents can change document frequencies and reverse two unchanged documents’ order. An old tail can then be unsafe even when all changed bodies are known. The artifact retains a directed counterexample.

Supporting such a ranker requires binding a score/statistics epoch, freezing old weights as part of semantics, or obtaining valid bounds under the new weights. The implementation deliberately uses a body-local integer score. It is not FaCoY, BM25, Lucene, semantic equivalence, or learned retrieval.

## 12. Finite and empirical evidence

`finite_continuation.py` checks 131,072 local token transitions, 65,536 `k+d` repairs, and 24,389 global merges (220,997 total), including 8,484 blocked and 15,905 complete global cases, with zero detected failures. These are finite instances, not model checking of arbitrary Python/TCP executions.

The independent analyzer reconstructs source snapshots and update intervals for all 18 primary traces, verifies 30,312 source receipts, and replays all 4,536 checker transitions. The directed unit tests cover binding, gaps, malformed updates, foreign epochs, compaction, persistence, blocker repair, and unsafe controls. The clean four-stage reproduction passed. This evidence supports the stated bounded implementation and theorems; it is not a proof of production reliability or a substitute for independent review.
