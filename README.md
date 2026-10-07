# Federated code search: continuation certificates

This standalone artifact accompanies **Continuation Certificates for Federated Code Search under Repository Churn**, prepared for ACM Transactions on Software Engineering and Methodology (TOSEM). It implements target-cut checking and bounded state reuse for repeated ranked queries. It does not implement a new semantic ranker or production search service.

## Reproduction

Requirements: Python 3.10 or later on a POSIX system, ordinary local TCP loopback access and sufficient memory for the bounded harness. No third-party Python package, network download, GPU or model service is needed. Retained third-party programs are parsed as source text, not imported or executed.

Run from this directory:

```sh
python run_all_validation.py
```

The driver runs three complete bounded suites and then validates the evidence. It does not silently skip missing scripts. Detailed logs and actual timings are in `results/`; a failure returns a nonzero exit code. Individual entrypoints are `run_continuation_suite.py`, `run_real_history_suite.py`, and `run_suite.py`. `refresh_resource_ledger.py` synchronizes the resource notes after a run. `validate_release.py` checks the existing evidence without rerunning it. The paper directory is optional for scientific execution; `--paper-dir ../paper --project-root ..` adds manuscript checks after the paper is built.

The standalone repository's `.github/workflows/scientific-checks.yml` runs this complete offline driver on pushes to `main` and manual dispatch. The job has a 30-minute limit and a 1,500-second campaign timeout, installs no scientific dependencies, and retains raw output and failures. It regenerates a disposable copy while preserving the checkout and a separate copy of the retained input results. Artifact collection runs even when the campaign fails; a workflow definition alone is not evidence of successful execution.

The continuation deadlines are 300 seconds for the batch and 420 seconds for
the suite, both for ordinary commands and the hosted workflow. They may be
increased with `P016_BATCH_SECONDS` and `P016_SUITE_SECONDS`, up to 600 seconds;
the suite allowance must exceed the batch allowance. Workloads, semantic
comparisons, memory limits, and the two-worker bound are unchanged. The retained
run used these allowances and does not establish completion within the historical
135-second batch and 170-second suite limits.

## Evidence and analysis units

For a portable, computation-only ranking regression, run
`python -B -m unittest discover -s tests -p test_bounded_selection.py -v`.
It compares bounded selection with an independent literal scan, checks prefix
boundaries and exact overlay after repeated writes, and validates in-memory
continuation receipts with the independent checkers. It opens no sockets and
runs no campaign. The complete suite's existing unit discovery includes these
tests. Prefix selection keeps length + 1 rows internally; overlay selects the
top-k unchanged rows using the same score/ID order. Posting enumeration and the
full-result default remain unchanged. No runtime gain has been measured for
this selection path; retained campaign timings and test counts are not a fresh
execution of it.

There are 18 primary case/seed traces and eight development traces, with real batch logs and the independent replay stage. Each primary policy has 4,536 calls. The public-history projection has 408 calls per policy and six independent source processes on one host. The cold-query suite preserves the unfavorable prefix-repair comparison. Unit and regression tests cover query/plan/k/epoch binding and failure-atomic state transitions; finite enumerations cover 220,997 explicitly bounded cases.

The retained campaign assets record executions of the bounded implementation, not the additional selection regression above. They are not claimed to be restored original files. The three canonical run JSON files and their named logs are the resource sources. Source--coordinator bytes include setup, reconfiguration, update feed and repair; client response delivery and transport framing are excluded. Continuation and mirror consume the same update feed. Their finite-horizon total difference is not evidence of lower steady-state update traffic.

Independent analysis reconstructs source history before checking responses; it does not import the coordinator or service implementations. The paper generator uses 18 complete case/seed pairs for descriptive uncertainty. Queries inside a shared trace are not treated as independent observations.

## Model and limits

The ranker is fixed body-local AST-label overlap with deterministic unique-ID tie breaking. A missing-tail blocker is conservative, not an unconditional necessity theorem. Necessary new evidence is asserted only under the explicitly stated compatible strict-witness premise. Source identities, ordered logs, ownership installation and local-prefix completeness are trusted; Byzantine sources are outside the model. Public history is a function-level projection, and all processes share one host. There is no user study, workload prevalence claim, WAN benchmark, durable multi-host recovery result, or acceptance guarantee.

The retained reference ledgers are inherited records of scholarly sources and reading depth, not a claim that the current build reread every paper. Upstream licenses and source provenance remain in `inputs/` and `docs/third-party.md`. Experimental code, scripts, proofs and inputs are directly present, not enclosed in another archive.
