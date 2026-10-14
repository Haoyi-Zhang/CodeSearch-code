# Same-information incremental comparison

## Policies and input equality

All three policies receive the same prefix rows and next omitted key, immutable
body catalog, fixed query, ordered assignments, requested cuts and initial
capacity. Source selection uses the existing inverted postings and bounded
selection, not a deliberately expensive full rebuild. Feed intervals are shared
once per group/tick. Cold acquisition, feed and refill canonical JSON payloads
are all charged. All three are checked with the same independent stateful
checker; the oracle is invoked only after a response for assessment.

`continuation` is the existing session with optional operation counters.
`extra-local` independently folds replacements into an extra-candidate buffer
and refills when the tail prevents certification of local top-k.
`extra-global` uses the same independently implemented transition, but adopts
global rather than local stopping. It is the nearest information-equivalent
variant: the bounded buffer, tail and global stopping contract are effectively
the continuation method. The shared interface and checker do not establish
independent authorship or a separate algorithmic novelty.

Extra-candidate reserves and refill maintenance are established by Yi et al.,
*Efficient Maintenance of Materialized Top-k Views* (ICDE 2003, pp. 189--200).
The comparator here matches the distributed evidence interfaces and fixed
capacity grid. It does not implement that paper's adaptive reserve procedure
or reproduce its probabilistic cost analysis.

Both refill variants use the same k + distinct-dirty-ID upper bound from the
retained feed floor and the same 64-row cap. Successful refills retain that
capacity. No source keeps registered-query state. No policy sees the oracle
before deciding whether to fetch. Acquired rows and feed are stored and counted
in the logical-state maximum. Indexes are current or two events behind as
declared; no source access is disabled to favor continuation.

## Histories and query selection

The public projection is the supplied six cachetools function-changing commits.
Base and projected digests are checked by `build_real_history`; no remote input
is downloaded or executed. Three groups run separately: the original 64
ordinary queries, the original four directed queries, and 64 new base-only
queries. The original facts remain zero ordinary changes across 384 calls and
ten directed changes across 24 calls. The new group has four changes across
384 calls.

The base-only rule is frozen in code before the projected history is built:
hash-order the indexed identifiers using `P016/base-only/v1:` plus ID, select
eight per project, and use the four rarest labels under base-only frequencies.
This hash is a deterministic selection device, not an integrity certificate.
There are no alternates or changed-function labels in this selector. Remove its
seed IDs only from the initial index; later supplied writes are not filtered.
The corpus therefore differs between query groups, but is identical across
policies within a group. No user-log or relevance claim follows.

The public grid uses L = 5, 10, 16 and index lag = 0, 2. There are 54 executions
and 14,256 calls across all policies, not independent statistical replicates.
All policies are exact/complete/sound and have identical payloads with no refill.

The history specification was available during implementation. The rule consumes
only base data and was fixed before its outcomes were computed, with no
outcome-driven hash-salt or query reselection. This is not a blinded query study.

The generated comparison loads the two supplied primary rank-churn histories
(seeds 2 and 3) as data. It uses two of the already selected eight base-only
queries per project, fixed before the traces are read. All 16 queries run at
each of 63 ticks. This is not the original four-active-query schedule. With
L = 5, 10, 16 and lag = 2 there are 18 executions and 18,144 calls. The two
histories produce ten and fourteen changed-oracle observations per execution.
At L=5, continuation/global extra-refill make 1/2 shard refills and the local
variant makes 9/11. At L=10 and 16 all have zero refills. Do not combine these
generated changes with the public-projection ten-change fact.

## Costs and counters

Local dispatch serializes/decodes the exact canonical request and reply bytes
at each call, without sockets. There are three logical source objects, not six
new process replicas or a fault campaign. Ownership names are kept for checker
binding; no outage/failover result is inferred.

Communication phases are acquisition, feed and refill, including both directions
and newline delimiters. Writer dissemination, body transport, oracle evaluation,
client delivery and transport headers are excluded for every policy. The feed
is charged once within each separately run query group. There is no free cache
fill, acquisition from oracle state, or uncharged refill. Prefix requests share
the same source implementation, so metadata bytes are matched too.

Source counters include posting entries visited and positive rows built on every
prefix query. Initial source index construction and applied index updates count
identifier assignments and posting membership operations using the actual old
and new body label lengths. Receive-only journal updates are not called posting
updates. No full source CPU or instruction count is inferred from these fields.

Coordinator counters count assignments visited (including refill-size folding),
buffer rows visited on non-no-op transitions, live-final score evaluations,
sort-input candidate rows, merge rows and tail tests. A local tail test followed
by the global certificate merge is counted twice; this is not a byte advantage.
Validation, copying, JSON work and sort comparisons are not collapsed into an
invented generic operation total. Source/coordinator/checker CPU fields are real
single-run measurements, but host quantization produces zeros and unstable
small differences. They do not support comparative timing conclusions.

## Results and verification limits

At generated L=5 across both seeds, each policy pays 67,776 acquisition bytes
and 86,732 feed bytes. Continuation/global refill costs are 7,887 bytes versus
36,075 for local refill; totals are 162,395 and 190,583 bytes. The closest global
variant matches continuation's logical state and all operation counters. Larger
initial reserves eliminate the local difference on these histories. These are
conditional global-stopping savings, not an advantage over incremental top-k
methods generally or production performance evidence.

`replay_incremental.py` invokes no policy, service, or coordinator implementation.
It uses the extractor for the body catalog and the independent checker for
body scores, then rebuilds
source snapshots, validates strict row/tail types, installs acquisition receipts,
checks every transition/result, recomputes oracle changes and recomputes phase
bytes/messages/returned rows and refill counts. The replay also invokes
`recount_incremental.py`, which derives source posting work from independent
syntax labels, initial maps, actual assignments and prefix cuts, with no
source/index/producer import. It checks total and per-phase initialization,
maintenance, acquisition and refill counts. Each index is built once; the
offline harness no longer builds an uncounted ordinary index before constructing
its counted index. CPU and coordinator work counters are not independently
recounted; small owned fixtures test their counting semantics and exercise both
a local-only refill and a lagging prefix refill. RPC CPU is not a complete
initialization-and-maintenance CPU budget.

The saved summaries and compressed traces retain query plans, per-call results,
oracles, source requests/replies, costs and counters. The generated manuscript
tables consume successful matching replay records. Earlier campaign inputs,
negative controls, counts and resource records remain unchanged.
