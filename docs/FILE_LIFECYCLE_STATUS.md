# File lifecycle — implementation status

**Status:** final review preparation · **Updated:** 2026-09-25 · **Tests:** deferred until final review

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
| F1: durable ownership and operation journal | Building | Allocations precede writes; History retention cannot erase ownership |
| F2: discovery and Files attention view | Building | Bounded cached scans distinguish unknown, active and retained files |
| F3: checked deletion and retention | Building | Exact owned files only; Keep, undo and dual clocks survive restart |
| F4: recovery publication | Building | Verified independent copies, source holds and crash reconciliation |
| F5: Curator recovery import | Building | Target coordination, durable placement and receipt outbox |
| F6: settings, cleanup and operations | Building | Advisory Dev enable controls; receipt-driven cleanup; docs and final validation |
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
4. **CI behavior stays unchanged.** PRs remain unopened until the final review
   boundary so Curator does not start unit tests during construction.

## 3. Validation record

F0 code and failure regression tests are written; execution is deferred.
No tests have run for this implementation. No PR has been opened or merged.

## 4. Build checkpoint

Runner now has an independent FULL-synchronous SQLite inventory, allocation
sidecars, checked deletion journal, dual-clock retention and copy publication.
Queue writer stop acknowledgements precede payload deletion. Existing active
folders migrate without receiving automatic deletion authority. The Files UI
and Dev advisory controls are wired. Recovery import integration and crash
reconciliation are still in progress; these are not merge-ready changes.
`cargo check -p nzbd-engine` and `cargo check -p nzbd-api` passed before the
latest History routing changes. These are compilation checks, not test runs.

Curator now has durable placement intentions, rollback copies retained until
metadata commit, atomic per-file import receipts, an outbox delivery worker,
recovery API generation and Recovery/Dev settings panels. Compilation of its
Go API passed. Web dependency installation needed network permission; tests
remain deferred. Crash reconciliation, capacity checks, recovery retries,
explicit episode mapping and retention cleanup are still being completed.

The shared Curator target coordinator initially serializes library imports.
This trades parallel library writes for one clear authority across ordinary
and recovery imports. Network downloading remains concurrent.

## 5. Crash recovery checkpoint

Recovery publication now reconciles a durable manifest against directory
identity, exact file count, sizes and SHA-256 digests. Interrupted incomplete
copies become visible retained inventory for review. Queue retirement is
reconciled before writers start. Category moves and failed-payload parking use
a journaled, exclusive publication; cross-volume copies verify their bytes and
retire the exact original entries only after publication is committed.

Curator records receipt delivery and local completion in one transaction.
Placement reconciliation cleans verified temporary/rollback files after the
metadata transaction. The Recovery view restores persisted activity and exposes
cancellation, with a worker quiescence acknowledgement before releasing a claim.
Quality, source and episode metadata are committed with each file receipt.

Remaining construction includes platform and cluster compatibility, operational
backup/retention controls, deeper failure regression coverage and final docs.
No adversarial implementation review or unit tests have run yet.

## 6. Operational controls checkpoint

Added existing-retention preview/apply, offline inventory backup, protected root
checks at execution, receipt-scoped source cleanup, paged recovery selection,
capacity previews and low-cardinality lifecycle metrics. Large staging copies
release the global mutation coordinator while their durable source hold remains
in force; new downloads need not wait for the copy. Legacy recursive move and
failure-disposition helpers were removed in favor of the ownership journal.

Curator now uses a transaction-local library revision to detect concurrent API
edits before committing recovery metadata. Native probing, concrete destination
names, per-file progress and informational claim heartbeats are wired. The unit
regression cases are written but have not run. Final review/test evidence and
performance measurements are still pending; no PR has been opened or merged.

## 7. Review boundary

Implementation and operator documentation are ready for adversarial inspection.
Recovery preview hashing now runs as a persisted Curator background operation;
receipt delivery runs independently of long copies. Regression sources cover
transaction rollback, concurrent target edits, cancelled imports, interrupted
publication, protected roots and receipt-scoped cleanup. An ignored million-row
inventory performance fixture is reserved for the final verification phase.

Compilation/type checks are the only validation performed so far. Unit tests,
repository gates, the performance fixture, PR creation and merge are next, after
review findings are addressed. No deployment or live-media cleanup is planned.

## 8. Adversarial review corrections and verification

Three reviewers completed read-only implementation reviews. Corrections retain
writer-stop acknowledgements across retries, fence retiring jobs, invalidate
stale expiry authorization, cache active writer allocation, reconcile abandoned
allocations, and atomically journal cross-volume source retirement. Discovery
now runs every 15 minutes when enabled and includes category roots; handoff
listing is paginated.

Curator rejects colliding destinations/episode assignments, imports complete
staged handoffs, verifies surviving publications before receipts, repeats fsync
when reconciling publication, and rolls back uncommitted publication before
acknowledging cancellation. Ordinary reimports get a fresh placement generation;
ebook replacement cleanup is journaled. Recognized obfuscated media uses its
content-derived destination extension without changing the original filename.

Decision: select the recovery subset in Runner before staging. Curator imports
that immutable handoff together; it does not claim a subset and strand the rest.
Enable controls remain advisory and unrestricted. Final test pass is starting;
PRs and merges are still pending.
