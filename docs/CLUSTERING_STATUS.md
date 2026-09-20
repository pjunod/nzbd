# Cluster completion — progress and remaining work

**Updated:** 2026-09-19 · **State:** repair candidate complete; final affected-lane rerun pending ·
**Branch:** `codex/clustering-completion` · **PR:** [#227](https://github.com/pjunod/nzbd/pull/227)

Companion to [CLUSTERING.md](CLUSTERING.md) (the existing implementation) and
[CLUSTERING_COMPLETION_PLAN.md](CLUSTERING_COMPLETION_PLAN.md) (the finite
completion plan). This is the status page for the remaining Usenet cluster
work. Implementation, review, tests, and deployment are separate facts.

## Current checkpoint

Fresh standalone clones were taken from GitHub. The inspected revisions are
`nzbd` at `7b81e84` and `plurx` at `8663d6c0`. Neither user's working checkout
is used for this work. Forgejo's `forge.lan` and SSH's `lab3` names did not
resolve from this environment; the reference is the pinned GitHub copy, not
a claimed verification of a newer Forgejo tip or the deployed fleet.

| Work | State | Evidence / next action |
|---|---|---|
| Current source comparison | Complete | Coordination, publication, transport recovery, PP ownership, provider budgets, and CI inspected at the revisions above |
| Documentation and finite implementation plan | Complete | Existing claims reconciled; recommended dependency/migration decision and P0–P5 acceptance recorded in the linked plan |
| P0 — delivery lane | Implemented; reviewed; targeted validation green | Draft PRs allocate no validation jobs; readiness starts the affected lane; full suites remain explicit; unrelated BitTorrent M0/fuzz matrices no longer run on PR events |
| P1 — transactional control authority | Implemented; reviewed; targeted validation green | Pinned plurx Hiqlite/WAL, atomic schema install, request-atomic queue deltas with rollback, quorum-backed health, adoption-before-write ordering, durable lease reconstruction, and idempotent migration startup |
| P2 — worker lifetime and publication | Implemented; reviewed; targeted validation green | Bounded RPCs/deadlines; immediate PP cancellation; lost-renewal resynchronization inside one fence; exact completion receipts; job-bound, fsynced immutable generations; receipt-timestamped history |
| P3 — weighted placement and account budgets | Implemented; reviewed; targeted validation green | Weighted remote-only execution plus persisted generation handoff; only active pool tasks acknowledge at batch boundaries; dead-holder capacity remains reserved through bounded lease expiry |
| P4 — segment distribution | Implemented; reviewed; targeted validation green | Fixed explicit article ranges; failed ranges cannot publish; exact range-set/byte coverage and CRC validation; one exact assembly lease |
| P5 — operator surface and final acceptance | Implemented; reviewed; targeted validation green | Cached diagnostics, pre-extraction peer authentication, unrestricted Settings → Dev enable controls with advisory evidence, updated docs, bounded serial acceptance harnesses, and vendored RustSec inventory |
| One adversarial review | Complete; findings addressed | Independent combined-change review found authority, failover, publication, budget, range, acceptance, audit, and CI gaps; all actionable findings were implemented before validation |
| Fast lane | Affected repair cases green; combined rerun pending | The first combined run exposed dependency/CI contracts plus auth ordering, quorum health, adoption ordering, restart identity, lost-renewal failover, dead-holder capacity, and PP-history defects. Each affected clustering regression now passes in isolation. The complete bounded E2E target runs serially to isolate its multi-process voters before merge. |
| PR / merge / deployment | PR #227 open and ready; not merged; not deployed | Next: satisfy the repaired candidate's required checks and merge. Deployment remains a separate operation. |

## Implementation decisions

- Provider accounts use the configured server name as the initial explicit
  account key. This avoids a second credential-bearing provider model in the
  bounded change; heterogeneous account aliases remain outside this delivery.
- Range distribution activates only for one active file with at least 64
  articles and uses fixed 32-article ranges. Small and multi-file jobs retain
  whole-job leases.
- Authority publication preserves replaced output under a uniquely named
  `.superseded-*` sibling. Automatic deletion was intentionally avoided; an
  operator may remove the recoverable copy after verifying the selected result.
- Script receipts choose safety over automatic replay: `started` without
  `done` becomes a visible `unknown-not-replayed` result and a script failure.
- The maintained Hiqlite/WAL source is pinned to the inspected plurx revision;
  nzbd's Rust floor is 1.95 and the repository toolchain is 1.97.1.
- nzbd adds one dependency-surface correction to the pinned Hiqlite copy:
  `cryptr/s3` follows Hiqlite's `s3` feature instead of compiling for the
  SQLite-only cluster build. This removes the unrelated S3/XML advisory path
  without expanding the repository's exact reviewed exception set.
- The authority does not execute downloads or PP locally. This is the bounded
  way to make every output-producing path use one lease/generation contract;
  configured capacity returns automatically when that node is a worker.
- The familiar completed directory is a compatibility alias. Durable history
  and `*Cluster:result-ref` expose the immutable selected generation.
- API writes wait for completed authority adoption; this is a transactional
  correctness boundary, not a feature gate. Cluster enablement remains wholly
  operator-controlled.
- Provider capacity held by an unreachable process is released only after all
  its durable work leases expire and its process-local deadline has drained
  connections. Missing a heartbeat alone never reallocates sockets.
- Coordinator priority remains a sub-interval election bias. Extending it into
  a deterministic multi-interval preference would consume worker lease time;
  replicated lease ownership, not priority, is the authority boundary.

## Bounded acceptance inventory

The final fast lane includes the `nzbd-cluster` library plus its complete
bounded E2E target. Together they cover three-voter quorum loss and concurrent
startup migration, mutation rejection, leader and worker death, running-lease
adoption, shared-volume journal reuse, two-worker range assembly, low-disk
admission, provider budgets, remote PP/history, queue restart, exact
lost-response matching, and process-local expiry cancellation. The range and
PP scenarios exercise private generations and single-winner publication; the
worker/leader death scenarios exercise reassign/adopt behavior.

## Compile evidence

`cargo check --workspace --all-targets` is green after the adversarial fixes.
The seven clustering scenarios that failed or timed out in the first remote
candidate were rerun individually after repair and are green: peer auth,
three-voter quorum loss, snapshot-repair adoption, queue restart, leader death,
worker death, and remote PP/history. This is affected repair evidence; the
combined remote fast lane remains the required merge result.

## Working rules

- Make normal, coherent commits; batch the implementation into one main-bound
  PR. Do not create a PR train for these milestones.
- During development, compile/format as useful; execute tests only after the
  final adversarial review. Fix failures and repeat only the affected checks.
- Full unit suites, coverage, soak campaigns, and release builds are separate
  batch/manual work. Their absence is visible, not a runtime feature gate.
- Operator enable/disable remains available. Requirements are advisory in
  Settings → Dev; there is no evidence receipt, approval flag, or readiness
  score that prevents enabling a feature.
- Keep only the active working clone and necessary evidence. No deployed node is
  stopped, upgraded, or reconfigured by this documentation pass.
