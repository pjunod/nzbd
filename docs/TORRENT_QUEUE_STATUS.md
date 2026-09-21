# Torrent queue — implementation and merge status

**Status:** implementation complete; preparing the combined review candidate.
**Updated:** 2026-09-20 · **Branch:** `codex/torrent-queue-lifecycle`.

Companion to [USAGE.md](USAGE.md) (operator behavior) and
[CONFIGURATION.md](CONFIGURATION.md) (seeding defaults). This page records
progress, decisions, review findings, and final validation for the queue work.

## Delivery progress

- [x] Preserve the existing queue with collapsible Seeding, Completed, and
  Waiting sections; collapse state persists in the browser.
- [x] Expose upload totals/rates, ratio, peers, seeded duration, completion
  time, policy, and stop reason in the queue and torrent detail panel.
- [x] Add durable manual stopping and stop-after-download, ratio, and time
  policies, with global/category defaults and per-torrent overrides.
- [x] Add Settings → Dev enable controls with live advisory readiness.
- [x] Transfer work into an independent clone and restore the original
  checkout without changing its branch or existing commits.
- [ ] Make focused commits and open one draft PR.
- [ ] Complete the adversarial agent review and address actionable findings.
- [ ] Run final affected unit/DOM checks and required CI; fix failures.
- [ ] Merge the green PR into main and clean temporary artifacts.

## Decisions and scope

1. **Retain design A and B's instrumentation.** Detail panels expose metrics
   and policy controls without requiring a separate transfer workspace.
2. **Stopping preserves payload files.** Removal and payload deletion stay
   separate. Editing a policy never silently restarts a stopped seed.
3. **Defaults are copied when applied.** An existing torrent does not change
   its policy when global or category defaults change later.
4. **Advisories do not gate enabling.** Unverified readiness says `unknown`;
   configuration validation and runtime data-integrity rules remain intact.
5. **Preserve the latest main recovery fixes.** The independent clone starts
   at main's PR #231 and keeps its resume-phase and progress fixes.
6. **Web UI scope.** The additive API data remains available to other clients;
   this change does not redesign the native mobile app.

## Validation evidence

Before the new workflow instruction, the earlier checkout passed 589 DOM
assertions and 308 affected Rust library tests, plus formatting, workspace
compilation, and affected-package Clippy. Three unrelated connection-pool
shutdown tests stalled and were excluded from that preliminary run. These are
historical results, not certification of this final candidate.

No tests have run after the workflow instruction. Final validation will follow
adversarial review; draft CI is skipped by the repository's existing workflow.
The required main check is `Main promotion gate`; protection stays intact.

## Review and merge record

Pending the combined candidate. Findings and their fixes will be recorded here
before final tests. The PR link and GitHub checks will provide merge evidence.
