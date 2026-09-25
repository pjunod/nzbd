# File lifecycle — implementation status

**Status:** building · **Updated:** 2026-09-25 · **Tests:** deferred until final review

Companion to [the implementation plan](FILE_LIFECYCLE_PLAN.md) and
[the design review](FILE_LIFECYCLE_REVIEW.md). This page tracks delivery;
unchecked rows describe planned work, not shipped behavior.

## 1. Delivery

Work uses dedicated sibling clones under `/private/tmp/file-lifecycle-20260925`.
Runner starts at `4410e0e`; Curator starts at `8e6cced`. Both use
`codex/file-lifecycle`. User checkouts and live media are untouched.
Focused commits will be combined into one PR per repository. Final adversarial
review precedes unit tests. Review findings and test failures must be resolved
before merging. No deployment is included.

| Milestone | State | Acceptance |
|---|---|---|
| F0: truthful deletion and consumer cleanup | Building | Failed/pending deletion preserves records and cannot trigger direct Curator removal |
| F1: durable ownership and operation journal | Planned | Allocations precede writes; History retention cannot erase ownership |
| F2: discovery and Files attention view | Planned | Bounded cached scans distinguish unknown, active and retained files |
| F3: checked deletion and retention | Planned | Exact owned files only; Keep, undo and dual clocks survive restart |
| F4: recovery publication | Planned | Verified independent copies, source holds and crash reconciliation |
| F5: Curator recovery import | Planned | Target coordination, durable placement and receipt outbox |
| F6: settings, cleanup and operations | Planned | Advisory Dev enable controls; receipt-driven cleanup; docs and final validation |
| Final adversarial review | Pending | Findings resolved before tests |
| Final tests and PR merge | Pending | Required suites and CI green |

## 2. Decisions and instruction precedence

1. **Full approved scope remains.** This combines a bug fix with robustness
   improvements; the historical orphan cause remains unproven.
2. **Enable controls are advisory.** Dev settings must explain prerequisites
   and show whether they are met, while always accepting enablement. Individual
   file operations must still prove identity, ownership and quiescence.
3. **User delivery instructions override Curator checkout and repeated-test
   rules.** Work stays in owned clones and tests run after final review.
4. **CI draft behavior is awaiting the user preference.** Runner already skips
   draft PR tests; Curator currently runs them. No draft PR has been opened.

## 3. Validation record

No tests have run for this implementation. No PR has been opened or merged.
