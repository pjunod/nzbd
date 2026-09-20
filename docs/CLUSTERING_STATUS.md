# Cluster completion — progress and remaining work

**Updated:** 2026-09-19 · **State:** implementation in progress ·
**Branch:** `codex/clustering-completion`

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
| P0 — delivery lane | Implemented; not yet reviewed or tested | Draft PRs allocate no validation jobs; readiness starts the affected lane; full suites remain explicit |
| P1 — transactional control authority | Implemented; compile-checked; not reviewed or tested | Pinned plurx Hiqlite/WAL, Rust 1.95 floor / 1.97.1 pin, fixed voter config, exact durable leases, receipts, and resumable one-time queue migration |
| P2 — worker lifetime and publication | Implemented; compile-checked; not reviewed or tested | Bounded RPCs and deadlines; revision cancellation; full private PP attempts; script started/done/ambiguous receipts; verified immutable generations; authority-side publication and idempotent history |
| P3 — weighted placement and account budgets | Implemented; compile-checked; not reviewed or tested | Weighted backlog-aware placement plus persisted generation handoff; pool tasks acknowledge at batch boundaries; uncertain sockets remain reserved |
| P4 — segment distribution | Implemented; compile-checked; not reviewed or tested | Fixed explicit article ranges, durable accepted-range rows, sparse private range outputs, CRC/offset validation, and one exact assembly lease |
| P5 — operator surface and final acceptance | Implemented; compile-checked; not reviewed or tested | Cached native diagnostics, unrestricted Settings → Dev enable controls with advisory evidence, updated behavior/config docs, and bounded range/quorum harness coverage |
| One adversarial review | Not requested | Request only when the implementation PR is ready to merge |
| Fast lane | Not run | Run after review findings are addressed |
| PR / merge / deployment | None | Implementation is being prepared for its one combined adversarial review; no deployment is part of this task |

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

## Compile evidence

`cargo check --workspace --all-targets` is green at the implementation
checkpoint. This is static development evidence, not the final fast lane. Per
the delivery contract, no test target has run yet; tests run once after the
combined adversarial review.

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
