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
| P2 — worker lifetime and publication | Implemented; compile-checked; not reviewed or tested | Bounded RPCs, independent worker deadlines, revision/token propagation, sealed private generations, and fenced idempotent publication |
| P3 — weighted placement and account budgets | In progress | Weighted backlog-aware placement is implemented; acknowledged shrink-before-expand transfer remains |
| P4 — segment distribution | Not started | Explicit ranges, isolated attempts, one assembler |
| P5 — operator surface and final acceptance | Not started | Advisory requirements, live diagnostics, finite failure scenarios |
| One adversarial review | Not requested | Request only when the implementation PR is ready to merge |
| Fast lane | Not run | Run after review findings are addressed |
| PR / merge / deployment | None | P0 makes an eventual draft safe; the batched PR remains unopened while runtime work is active |

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
