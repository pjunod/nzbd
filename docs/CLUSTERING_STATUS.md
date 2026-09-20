# Cluster completion — progress and remaining work

**Updated:** 2026-09-19 · **State:** adversarial findings addressed; final validation rerun pending ·
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
| P0 — delivery lane | Implemented; reviewed; not yet tested | Draft PRs allocate no validation jobs; readiness starts the affected lane; full suites remain explicit; unrelated BitTorrent M0/fuzz matrices no longer run on PR events |
| P1 — transactional control authority | Implemented; reviewed; compile-checked; not yet tested | Pinned plurx Hiqlite/WAL, atomic schema install, request-atomic queue deltas with rollback, exact projection takeover, durable full lease reconstruction, and idempotent migration startup |
| P2 — worker lifetime and publication | Implemented; reviewed; compile-checked; not yet tested | Bounded RPCs/deadlines; immediate PP cancellation; exact lost-response receipts; job-bound, fsynced immutable generations; immutable result references and idempotent history |
| P3 — weighted placement and account budgets | Implemented; reviewed; compile-checked; not yet tested | Weighted remote-only execution plus persisted generation handoff; only active pool tasks acknowledge at batch boundaries; startup is fail-closed and uncertain sockets remain reserved |
| P4 — segment distribution | Implemented; reviewed; compile-checked; not yet tested | Fixed explicit article ranges; failed ranges cannot publish; exact range-set/byte coverage and CRC validation; one exact assembly lease |
| P5 — operator surface and final acceptance | Implemented; reviewed; compile-checked; not yet tested | Cached diagnostics, unrestricted Settings → Dev enable controls with advisory evidence, updated behavior/config docs, bounded acceptance harnesses, and vendored RustSec inventory |
| One adversarial review | Complete; findings addressed | Independent combined-change review found authority, failover, publication, budget, range, acceptance, audit, and CI gaps; all actionable findings were implemented before validation |
| Fast lane | First merge-candidate run started; rerun pending | The first run exposed an unintended Hiqlite S3/XML feature edge and a cargo-audit/deny exception mismatch. The S3 edge is removed rather than waived; the exact existing exception set is shared by both advisory tools. Push the repaired candidate and require its affected lane to pass. |
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
This is static development evidence, not the final fast lane. Per the delivery
contract, no test target has run yet; the tests run once at the final
merge-candidate stage.

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
