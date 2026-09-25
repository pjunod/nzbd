# History loading implementation status

**Status:** combined adversarial review in progress ·
**Updated:** 2026-09-25

The [reviewed plan](HISTORY_LOADING_PLAN.md) records the diagnosis, nuc3
measurements, and recovery contracts. This page tracks delivery of its first
release. Work uses an independent clone at
`/private/tmp/nzbd-history-loading`, branch `codex/history-loading`, based on
main `2f84dfb`. Delivery PR: [#237](https://github.com/pjunod/runner/pull/237)
(draft during review). The user's working repositories are not used for implementation.

| Step | State | Evidence / next action |
|------|-------|------------------------|
| 1. Remove replay from history reads | Complete | Native and compat listings use the indexed view; page and count share a read transaction. |
| 2. Own and bound reconciliation | Complete | Single worker and pass gate, local-only repair, mutation fencing, transactional batches, unchanged-log skips. |
| 3. Preserve storage and recovery | Complete | Optional local index relocation preserves cursor IDs, committed WAL state, observations, tombstones, and spool location. |
| 4. Browser and operator controls | Complete | Latest-request rendering, loading feedback, persisted pause/resume, Dev enable control with advisory readiness. |
| 5. Adversarial review | In progress | Reviewing commits `3cb9e16` and `b91ca96`; address findings before tests. |
| 6. Final verification | Pending | Run unit tests only after review remediation. Use the required PR CI lane and avoid duplicate suites. |
| 7. Merge combined PR | Pending | Merge after final checks pass; record the PR and validation evidence here. |

## Validation policy and evidence

Before the user's latest CI instructions, affected Rust tests, UI harnesses,
clippy, and a synthetic performance probe had passed on the earlier base.
Those results are preliminary, not qualification of this branch. An ongoing
full-workspace run was stopped when the new instructions arrived. No unit
tests have been run since that instruction. Final results belong below after
the adversarial review and remediation.

The preliminary 1,000-row storage probe measured p95 1.575 ms while three
replays ran, with a maximum batch of 15 ms. This was a local Mac debug build,
not HTTP latency or post-fix production evidence. The nuc3 baseline remains
12.3–31.7 seconds for slow 20-row requests and 15–37 ms for warm reads.

## Decisions and remaining operational work

- Performance fixes are always active. Dev settings can pause or enable
  synchronization immediately; readiness never blocks enablement.
- M6 incremental per-file contributions remain outside this first release,
  as the reviewed plan requires separate proof of equivalent merge semantics.
  Shared history still performs a bounded-transaction full replay when changed.
- Retain the original index during relocation. Rejecting a stale destination
  or missing active index protects cursor state; it is not a feature gate.
- No production configuration, data migration, or deployment has been done.
  Deployment qualification must measure nuc3 latency and shared-peer freshness
  against the plan before claiming production performance improvement.

## Final review and verification

Pending the combined adversarial review.
