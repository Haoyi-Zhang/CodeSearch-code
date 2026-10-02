# Continuation-certificate result summary

The primary campaign contains 18 traces and 4,536 standing-query observations.
Continuation, exact overlay, cut-aware cache, and full mirror each returned 4,340 exact answers, justified 4,248 complete answers, and produced zero false-completeness claims and zero target-unsound rows.
Continuation used 353.8 source-RPC bytes/query versus 2157.2 for exact overlay (83.6% lower).
It repaired 48 shard tokens across 48 queries (1.06% of queries).
Maximum serialized logical state was 58.7 KiB for continuation and 1694.4 KiB for the full mirror.
The unsafe stale control made 1,910 false-completeness claims and returned 2,075 unsound rows.

These are deterministic generated-fixture and loopback results. They do not establish production latency, semantic code relevance, Byzantine source truth, or external venue novelty.
