# Incremental history ingestion

**Status:** review addressed; 73 state tests passed; final CI tracked in PR #238 ·
**Updated:** 2026-09-25 · **Branch:** `codex/history-incremental`

This completes M6 from [HISTORY_LOADING_PLAN.md](HISTORY_LOADING_PLAN.md).
The first PR removed reconciliation from history requests. This change makes
normal shared-log catch-up read new bytes and reconcile affected completion
keys. The JSONL format, cursor contract, and merge precedence stay intact.
Work uses the independent clone at `/private/tmp/nzbd-history-loading`.
[PR #238](https://github.com/pjunod/runner/pull/238) is the live delivery status
page for final review, checks, and merge.

## 1. Delivery status

| Step | State | Evidence / next action |
|---|---|---|
| Incremental reader and contribution cache | Implemented | Complete-line offsets, stable file identity, bounded boundary checks, per-file contributions. |
| Merge and recovery behavior | Implemented | Sorted-file precedence, first immutable payload, last effective mutable values, monotone tombstones, retention, stable cursors. |
| Concurrency and failure handling | Implemented | Publish cache only after successful database batches; fence both edges of local mutations. |
| Operator visibility | Implemented | History shows scan kind, affected entry count, rebuilt files, incomplete tails, and unrecognized lines. |
| Adversarial review | Addressed | One P2: appended tombstones scanned the lifetime set using the page-reader connection. Now indexed writer-only work. |
| Final tests | Local affected suite passed | 73 state tests passed; one opt-in benchmark ignored. [Live CI checks](https://github.com/pjunod/runner/pull/238/checks). |
| Merge | Tracked live | [PR #238](https://github.com/pjunod/runner/pull/238) records final checks and merge. |

## 2. Contract

The implementation is in
[history/sync/incremental.rs](../crates/nzbd-state/src/history/sync/incremental.rs).
For each file, the cache retains its inode/device identity on Unix, size and
modification time, last committed newline offset, a 128-byte boundary sample,
and contributions keyed by `(job_id, completed_at)`.

Each contribution retains the first immutable payload and its byte position,
the last `hidden` value, and the last non-null `removed_at`, `picked_up_by`, and
`record` values. Recomputing a changed key folds contributions in filename
order. The SQLite update continues to preserve existing IDs, immutable fields,
consumer observations, and publication durability markers. New entries are
applied in their first filename/line encounter order, preserving cursor order.

```text
file metadata + committed boundary
               |
               v
read complete appended lines --> changed per-file contributions
               |                              |
               v                              v
        new tombstones              affected keys in file order
               |                              |
               +--> bounded SQL commits <-----+
                              |
                    local mutation fence
                              |
                              v
                  publish offsets + contributions
```

Tombstones commit before entries, in batches of 16 keys. Entries retain the
16-row / 128 KiB transaction caps. A paused, stopped, or invalidated pass never
publishes its staged offsets. Already committed database batches can be
reapplied without changing existing cursor IDs.

**Key decisions:**

1. **Keep offsets and contributions ephemeral.** They advance together only
   after all associated database updates succeed. Restart reconstructs them
   from portable logs, so there is no durable cursor ahead of applied data and
   no new migration or rollback schema.
2. **Preserve per-file contributions.** Offsets alone would let a new append
   to file A incorrectly override file B, despite B sorting last. Missing
   contributions never clear retained SQLite values under `COALESCE`.
3. **Retain conservative full verification.** Startup, failed local publication,
   and a 60-second verification interval rebuild contributions from all logs.
   Replacement, truncation, or failed boundary verification rebuilds the affected
   file. A same-size edit also rebuilds that file when metadata changes.
4. **Fence mutation completion as well as admission.** A scanner can start
   while a local mutation is already in progress. Incrementing the generation
   again before releasing the mutation lock prevents that scanner from
   overwriting the newly acknowledged value or publishing stale offsets.

## 3. Recovery cases

| Case | Behavior |
|---|---|
| Ordinary append | Read the suffix plus at most two 128-byte boundary samples; recompute touched keys using all files' contributions. |
| New file | Scan from zero; insert it at its sorted precedence. |
| Partial line, including valid JSON without newline | Keep the offset before the line; publish it only after its newline arrives. |
| Malformed or unknown complete line | Advance past it and report one aggregate count for the pass. |
| Replacement or shrink | Rebuild that file and recompute keys in both its old and new contributions. |
| Truncate and regrow | Boundary mismatch detects ordinary rewrites; periodic full verification covers edits away from the boundary. |
| Missing peer file | Recompute its keys from remaining files; never infer a history deletion or forget known tombstones. |
| Unavailable directory or read error | Retain the previous committed cache and return an error; no offset advances. |
| Failure after some database batches commit | Retain previous offsets and retry the same suffix; existing row IDs remain stable. |
| Process restart | Reconstruct the cache and reconcile against the existing durable SQLite index. |
| Retention | Entries below the effective retention floor are not published. |
| Pause/resume | Use the existing persisted control; resume catches up without a separate enable flag. |

## 4. How to read the status

The existing History status includes `last_scan` (`full`, `incremental`,
`unchanged`, or initial `none`), `last_entries_reconciled`,
`last_files_rebuilt`, `last_incomplete_tails`, and `last_malformed_lines`.
These describe the last successful scan. The existing `last_error` and
`repair_pending` fields identify failed or interrupted work.

For a small append, expect `incremental`, bytes close to the new suffix, few
reconciled keys, and zero rebuilt files. A scheduled `full` scan is expected
at the verification interval. Repeated rebuilds on ordinary appends merit
checking log replacement and filesystem identity. Incomplete tails are normal
while another process is writing; persistent unrecognized-line counts deserve
inspection of the writer's log format.

There are no rollout flags. Settings → Dev → Enable history synchronization
remains the live persisted control, with readiness advisory only.

## 5. Verification

The prior full replay remains a test-only oracle. Regression tests compare the
entire indexed result, including cursor IDs and consumer metadata, after
cross-file appends, replacements, disappearance, compaction-like removal of
versions, and generated update sequences. Separate tests exercise unfinished
lines, partial commit failure, restart reconstruction, retention, and a local
mutation already in progress when scanning begins.

A deterministic resource assertion uses 1,000 entries with embedded 1 KiB
records, appends one entry, and requires exactly the suffix plus 256 bytes of
boundary reads on Unix. It also requires only one reconciled key. This avoids
a fragile wall-clock threshold while proving the accumulated history is not
reread for an ordinary append.

Run unit tests only after the combined adversarial review and remediation:

```bash
cargo test --locked -p nzbd-state --lib
```

The single adversarial review found one P2 performance defect: the reused
replay helper scanned all historical tombstones while holding the page reader.
It now checks incoming keys through the unique index in bounded writer
transactions and skips deletes for already-known tombstones. A regression
adds new and duplicate tombstones against 5,000 existing tombstones while
holding the page-reader connection; ingestion must complete independently.

After the review fix, all 73 state unit tests passed (one opt-in benchmark
ignored). This includes all 11 new incremental-ingestion regressions. The
state crate also passed clippy with warnings denied. Required CI and the
final merge are tracked on [PR #238](https://github.com/pjunod/runner/pull/238),
so recording results does not retrigger the unit suites.

## 6. Limits

The cache uses memory proportional to unique completion keys per file and
retained payload size; it does not retain every historical version. Recovery
and periodic verification still read full logs. On platforms without stable
file identity, changed files are conservatively rebuilt rather than using
unsafe offsets. Arbitrary in-place edits away from the boundary may remain
undetected until verification; normal writers append and compact by rename.

This change does not version the portable mutation format, change conflict
resolution, or deploy production software. Production rollout and latency
measurement remain explicit operational work; they are not prerequisites or
code gates for incremental ingestion.
