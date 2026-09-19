# Cluster completion — reuse plurx's authority contracts and finish C3

**Status:** analysis complete; proposed implementation plan; no runtime changes ·
**Written:** 2026-09-19 · **Scope:** Usenet clustering

Read [CLUSTERING.md](CLUSTERING.md) for today's C1/C2 implementation and
[CLUSTERING_STATUS.md](CLUSTERING_STATUS.md) for progress. This document is
the comparison, recommended architectural amendment, and finite implementation
handoff. It replaces the assumption that the remaining work is only a scheduler
and dashboard. Work in the order below, with normal commits and one batched
implementation PR. Do not turn each milestone into a separate project.

## 1. Evidence — what was compared

| Repository | Inspected main revision | Evidence boundary |
|---|---|---|
| nzbd | `7b81e84a32602a9169559dac38766e0191b577b3` | Fresh standalone GitHub clone; code and existing test sources inspected |
| plurx | `8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176` | Fresh standalone GitHub clone, including maintained Hiqlite/WAL sources and the September CI correction |

The documented Forgejo hostname and `lab3` SSH name did not resolve from the
analysis host. These are exact source baselines, not a claim that a newer
Forgejo tip or deployed fleet was inspected. Refresh the reference once at
implementation start if that origin is reachable; do not repeatedly chase its
moving main during this effort. No runtime tests were executed for this
documentation pass. The earlier conversation's 23 passing cluster tests were
on a different nzbd checkout and are not new evidence for this branch.

Primary plurx source anchors, all at the inspected revision:

- [Coordination token and TTL policy][p-coordination] and
  [replicated acquire/renew/release][p-store]: exact resource, owner, fence,
  revision, previous expiry; counters survive release.
- [Publication transactions][p-publication] and
  [process-local publication lifetime][p-lifetime]: renewal and result
  publication share a transaction, and failed renewal revokes local authority.
- [Membership][p-membership]: committed role and removal state participate in
  authority; a shared peer credential alone does not authorize leadership.
- [Maintained Raft transport][p-transport] and
  [frame delivery][p-frame]: flush completion, writer termination, bounded
  snapshot recovery, and ownership that survives socket cancellation.
- [Current fast lane][p-ci] and [pipeline correction][p-pipeline]: draft PRs
  allocate no jobs; readiness starts the lane; exhaustive checks are manual.

### 1.1 The differences that change the work

These are code-inspection findings. Failure scenarios below are requirements
for regression coverage, not claims of reproduced production incidents.

| Area | nzbd now | Useful plurx contract | Completion decision |
|---|---|---|---|
| Authority | `election_task` reads/writes `leader.json`; `persist_guard` checks before a separate snapshot rename | Fence comparison and authoritative mutation commit atomically | Add a narrow replicated control store; another pre-rename check cannot close the race |
| Epoch lifetime | Missing/corrupt leader data produces `None`; candidacy derives the next epoch from that file, despite the design's recovery claim | Reusable resources retain monotone counters | Persist identities and counters; distinguish missing bootstrap state from unreadable activated state |
| Work ownership | `LeaseInfo` is an in-memory map; adoption reconstructs kind from job status | Durable exact-token leases and current owner eligibility | Persist work kind, job generation, owner incarnation, scope, revision, and completion receipt |
| Completion | `work_complete` accepts an unknown lease if the same node is assigned, removes a known lease before import, ignores the import result, and returns success | Commit work result and ownership transition together | Exact-token idempotent completion; acknowledge only durable acceptance |
| Worker lifetime | `post_json` has no whole-call deadline/body bound; heartbeat errors only log; `PpCtx` checks a local active map | Bounded attempt lifetime and local revocation | Independent expiry/cancellation owner, bounded HTTP, stale-result rejection at publication |
| Post-processing | Extraction uses `.pp.<lease>`, but rename, repair, cleanup, scripts, and some moves touch the shared job tree; startup deletes other staging directories | Private generations plus fenced publication | Isolate the complete mutating PP attempt, preserve live attempts, publish an immutable result reference |
| Download persistence | `journal_suffix` is the node name, not the lease ID described in parts of the design | Owner identity includes incarnation and fence | Keep journal recovery; namespace attempt output and evidence explicitly |
| Split downloads | Whole `Job` imports replace existing jobs; each engine can finalize the same file | Separate work ownership from publication ownership | Explicit article ranges and one file assembler; never mark unassigned articles failed/done to disguise a partial job |
| Provider budgets | Integer shares sum to the cap at one instant; workers receive changed shares independently; pool tasks drop excess connections at a later loop boundary | Resource ownership transfers have explicit completion | Drain-and-ack budget handoff, generation numbers, bounded work, and retained uncertain reservations |
| Controls | Heartbeats cancel deleted jobs; pause/resume and file controls are not a durable revisioned worker-control protocol | Versioned intent, idempotency, no blind mutation retry | Preserve leader-owned control state when results arrive; propagate pause/delete/speed intent explicitly |
| Diagnostics | Local `/api/v1/cluster` returns leader and nodes, reads the shared directory per request, and omits the documented `leases` | Passive bounded observation with freshness | One background snapshot, explicit staleness, active leases and desired/applied capacity |
| Delivery | nzbd PRs automatically run workspace tests, coverage, MSRV, and other jobs; no fast-lane target exists | One review, then draft-to-ready affected checks | Port the small workflow shape first; do not copy the entire plurx validation framework |

Relevant nzbd implementation entry points:
[election](../crates/nzbd-cluster/src/election.rs),
[leader](../crates/nzbd-cluster/src/leader.rs),
[worker](../crates/nzbd-cluster/src/worker.rs),
[HTTP](../crates/nzbd-cluster/src/http.rs),
[runtime and diagnostics](../crates/nzbd-cluster/src/lib.rs),
[engine owner](../crates/nzbd-engine/src/owner.rs),
[connection pools](../crates/nzbd-engine/src/pool.rs),
[writers](../crates/nzbd-engine/src/writer.rs),
[state](../crates/nzbd-state/src/lib.rs), and
[post-processing](../crates/nzbd-post/src/manager.rs).

## 2. Recommended decision — a small control store, existing data plane

**Proposed amendment to ADR-13/15/16:** reuse the maintained plurx Hiqlite
and WAL dependency at the pinned revision for cluster control metadata.
Retain nzbd's owner-task engine, HTTP API, shared payload volume, local history
index, NNTP provider logic, and single-node storage path. This is a design
recommendation for the upcoming implementation; no dependency or runtime
behavior changes in this documentation commit.

```text
client command ──▶ queue owner ──▶ replicated control transaction
                                      │
                  job intent · lease · accepted output reference
                                      │
                                  executors
                                      │
                     shared-volume attempt data and journals
                                      │
                  sealed range / PP output ──▶ fenced publication
```

1. **Replicate decisions, not download traffic.** Store queue control changes,
   ID allocation, lease transitions, range/result references, and terminal
   receipts. Batch progress locally. Do not send article bodies, every
   article counter, logs, or one-second UI ticks through Raft.
2. **Keep one authority for cluster control.** In cluster mode the replicated
   state is authoritative; the existing `queue.json` becomes an export or
   migration input, never a competing writable authority. Single-node mode
   keeps its existing snapshot/journal path. Each voter stores its database
   and Raft/WAL files on its own local disk, never the shared payload mount.
3. **Reuse the maintained dependency, not a fork of plurx's application.** Pin
   source revision, license notices, and checksum/provenance for the minimal
   Hiqlite/WAL source set. Preserve its transport/durability fixes intact. Do
   not backport its Raft algorithm or build a filesystem compare-and-swap.
4. **Take the compiler cost explicitly.** nzbd declares Rust 1.85. The inspected
   patched Hiqlite declares 1.95, its WAL 1.88, and plurx pins 1.97.1. P1 pins
   a usable toolchain and updates the supported floor, lockfile, build images,
   and CI together. Start from plurx's 1.97.1 pin; do not label a dependency
   import compatible with 1.85 or spend this effort maintaining a backport.
5. **Keep topology small.** Start with explicitly configured voting peers and
   worker-only nodes. Three voters tolerate one unavailable voter; a two-voter
   cluster needs both. A third voter can be a small coordinator without
   download or PP work. One/two voters remain selectable with their limits
   displayed. No online voter reconfiguration, learner promotion service,
   fleet registry, or automatic membership repair in this completion batch.
6. **Feature switches express operator intent.** Cluster enablement and any
   optional scheduling controls are normal settings. No approval flag,
   qualification receipt, soak result, machine class, or advisory readiness
   score is required to turn them on. Settings → Dev may show requirements
   with `met`, `unmet`, or `unknown`; its toggle remains usable. Authentication,
   rejecting an expired lease, and returning a failed I/O operation remain
   correctness checks on operations, not feature-access restrictions.

### 2.1 Alternatives and the bounded choice

| Option | Cost / consequence | Decision |
|---|---|---|
| Retain file election and tighten checks | Smallest diff; leaves a check/write race and cannot provide plurx's stale-publication guarantee | Useful containment, insufficient as the final authority contract |
| Narrow maintained Hiqlite control store | Compiler/dependency and one migration cost; reuses the already implemented quorum and transport work | Recommended |
| External etcd/Postgres | Avoids embedded transport work but adds a separately operated service | Do not add a new deployment dependency for this effort |
| Import all plurx cluster facilities | Brings application-specific sessions, read policies, membership operations, and validation machinery | Outside the finite scope |

P1 begins with a compileable minimal control-store adapter, not a prolonged
comparison project. Record any unexpected dependency/platform incompatibility
as a concrete scope decision; do not respond by silently dropping fencing or
starting an independent consensus implementation.

## 3. Implementation contracts — what each boundary must guarantee

Names below describe **new interfaces**, not existing nzbd APIs. Re-verify
surrounding types against the linked source before implementation.

### 3.1 Ownership, commands, and recovery

Use the plurx lease identity:

```rust
struct LeaseToken {
    resource: String,
    owner_node_id: String,
    fence: u64,
    revision: u64,
    expires_at_unix_ms: i64,
}
```

Work records also carry a cluster ID, boot incarnation, job incarnation,
lease kind (`Download`, `Post`, `Segment`), exact scope, and job control
revision. These distinguish a restarted process or re-added job from the
previous owner of a human-readable node/job name.

The control-store adapter needs `acquire`, `renew`, `release`,
`apply_command(command_id, expected_revision, command)`, and
`publish_result(token, expected_job_revision, result_ref)`. The last two
return a durable receipt or a typed conflict/unknown outcome. Publication
validates owner eligibility, the exact live token, and job identity in the
same transaction as accepting output and closing work. Retry uses the same
command ID. A timeout after submission is an unknown outcome to reconcile,
not permission to issue a new side effect.

Persist tombstones/counters for reusable identities. Adopt only a stored live
lease for that owner/incarnation/scope, never a worker's assertion that an
unassigned job must be its old lease. A returned Job cannot overwrite a newer
pause, category, priority, deletion, file edit, or PP-complete record.

Renewal, control delivery, cancellation, and output publication share one
local owner for each lease. Its monotonic deadline is conservative relative
to the confirmed grant: an HTTP retry does not extend it. Stop scheduling and
cancel owned subprocesses on lost authority; publication still checks the
store because process suspension can prevent a timer from firing on time.
Bound HTTP connect/headers/body/total time and response size. Keep SSE streaming
under its separate streaming policy, not the short work-RPC body timeout.

### 3.2 Outputs and post-processing

An attempt writes only its generation directory. Workers never rename over
the selected final output or delete another attempt merely because its tag
differs. PP repair, deobfuscation, unpack, cleanup, and moves operate on that
private tree; use reflinks where available and a bounded copy fallback, never
writable hardlinks to the source. Account for its temporary disk use.

Flush and close output, write a manifest of identities/lengths/checksums, and
seal the generation before publishing its reference. The replicated record
selects the accepted generation. Expose its actual path through native API,
history, and NZBGet `FinalDir`; consumers must not require a mutable canonical
directory alias. A stale process may finish obsolete scratch work but cannot
select that work as the current result.

Arbitrary external scripts cannot promise exactly-once effects across crashes.
Record a durable invocation ID and start/outcome receipt. Do not automatically
repeat an invocation with an ambiguous outcome; expose it for an explicit
operator retry. Pass the attempt/job identity to scripts so cooperative
scripts can deduplicate. This is an honest outcome, not a new feature switch.
History JSONL remains portable evidence; project only accepted terminal
receipts into user-visible history, deduplicated by job incarnation/result ID.

GC removes only abandoned, unreferenced generations after the lease is
terminal and the retention interval has elapsed. Never delete a live owner's
staging from `remove_stale_staging`. Retry cleanup after failures and record
leftover bytes rather than claiming successful disposal.

### 3.3 Placement and provider budgets

Use explicit positive `download_weight` and `pp_weight` (default 1), existing
role switches, job-slot limits, and live disk state. Pick the lowest
assigned-work/weight ratio with a stable tie-breaker; count assignments awaiting
poll as well as running leases. PP still prefers nodes not downloading.
Do not add a learned model or continuous performance benchmark to place work.

Identify shared provider accounts explicitly, initially defaulting the account
key to the configured server name. Advertise local account availability and
connection ceilings without credentials. Allocate integer weighted shares
among actual demand, cap each by local capacity, and redistribute idle shares.
PP recovery fetches consume the same account pool.

Budget generation N+1 first shrinks old holders. The pool drains/cancels
in-flight batches, closes affected sockets, and acknowledges applied capacity.
Only then expand other holders. Persist desired and acknowledged grants through
leader changes. A stopped or unreachable process can retain provider sockets:
lease TTL alone does not prove the provider released them. Reserve uncertain
capacity until closure is established; display the lost capacity. Never claim
that shares summing to a cap proves simultaneous sockets stay below it.

### 3.4 Split downloads and one assembler

Split at bounded article ranges within a file, with explicit file IDs and
segment numbers. Keep full job metadata at the authority. An executor imports
only its authorized work view; absent articles remain unassigned, never falsely
failed or completed. Start with fixed target range size and static weights;
do not add adaptive resharding or live range migration.

Write per-attempt range data and completion manifests on the shared volume.
One current assembly lease consumes accepted ranges into the output file,
checks yEnc offsets/size and combined CRC, and publishes the completed file.
The assembler alone performs final rename/selection. It cannot publish until
all required ranges are terminal and their data is durable. Preserve delayed
PAR, file pause/delete, health calculation, and force-priority semantics.

Recovery reuses committed ranges and validates recoverable journal evidence;
only incomplete or invalid ranges need NNTP refetch. Concurrent attempts never
share a mutable `.part` file. This trades some shared-volume I/O for a simple
ownership boundary; measure bytes written and temporary space in the final
smoke, not in a new performance programme.

### 3.5 Migration and rollback

Perform one explicit stopped-cluster conversion. Back up the old control
snapshot/history, import job/file IDs, intent, journals, and accepted history
once, and record the source fingerprint and target cluster identity durably.
Crash/retry must resume that same conversion without duplicate admission.

Old file-election daemons cannot coexist with the new authority on the same
state tree. Separate the activated cluster control directory and generation
payload namespace; preserve the old tree as a read-only backup. Restarting
the old binary is not a live downgrade procedure. Rollback stops the new
cluster and either restores the pre-conversion backup (losing later control
changes) or uses a deliberate export that represents completed new work.
Document both consequences. Keep the single-node format unchanged where
possible; if shared types change, make schema compatibility explicit.

## 4. Finite delivery — six milestones, one implementation PR

These are commit-sized groups within one branch, not six review/CI queues.
The acceptance scenarios are authored with their changes and executed after
the single final adversarial review under §5.

| ID | Concrete work and file ownership | Acceptance fact |
|---|---|---|
| P0 | `.github/workflows/`, `.githooks/`, `Makefile`: draft-to-ready fast lane, explicit full-suite dispatch, bounded concurrency/cache use, docs-only selection | Draft allocates no expensive jobs; ready runs the selected lane; reverting to draft cancels it; no opt-in label can omit the lane |
| P1 | `nzbd-cluster` control adapter and startup, `nzbd-state` authority seam, config/daemon, dependency provenance/toolchain: narrow quorum store, durable exact-token leases, one-time conversion | Duplicate/reordered acquire-renew-release cannot revive authority; stale publication changes zero rows; restart retains fences and IDs; quorum loss never selects a new output |
| P2 | `worker.rs`, `http.rs`, engine import/result boundary, `nzbd-post`: cancellation/deadline owner, control revisions, isolated attempts, durable publication and history receipts | Stop/resume old worker past takeover; obsolete output stays unselected; dropped completion response retries once logically; PP loser cannot rename/delete winner output or repeat an ambiguous script automatically |
| P3 | `leader.rs`, `proto.rs`, `registry.rs`, engine pools/config: weighted assignment and acknowledged provider-budget transfer | Assignment backlog consumes slots; low-disk node gets no new writes; two nodes under delayed shrink/leader loss never receive overlapping acknowledged capacity; uncertain sockets retain reservations |
| P4 | Cluster range scheduling, engine queue/owner/writer, state journals: explicit segment work and single assembly | A one-file multi-segment NZB uses at least two nodes, produces byte-identical output, and resumes committed ranges after worker/leader failure without refetch; no worker independently finalizes the whole file |
| P5 | Native cluster diagnostics, existing embedded UI/config route, docs and focused harness | Settings → Dev enables/disables with advisory requirements; healthy/stale/unknown facts are distinct; cluster-disabled single-node behavior still works; the bounded scenarios in §6 pass |

**Stop line:** at the end of P5, ship the working Usenet cluster. BitTorrent
M6 execution, WAN clustering, object-storage payloads, automatic voter
reconfiguration, multi-cluster routing, transport tuning campaigns, broad UI
redesign, and full-suite unrelated failures are not added to this PR. A tiny
shared-store seam may be reused by future torrent work; this does not activate
the torrent backend or replace its separate implementation plan.

## 5. CI/CD — spend validation on the merge candidate

The September 10/13 correction at the top of plurx's pipeline document and
its actual `main-fast-lane.yml` control this workflow. Older paragraphs in
that document still describe full promotion fan-out; do not import those
superseded instructions or the historical repeated-review requirements.

1. Commit milestones normally on one branch in a standalone clone. Update
   [the status page](CLUSTERING_STATUS.md) in each meaningful commit. Compile,
   format, and inspect while building; do not run unit suites per commit.
2. P0 must make nzbd draft-safe before opening the implementation PR. Today
   even a draft starts full tests and coverage. A workflow change within the
   first PR may not control every base-owned event/required check, so make
   this transition observable and verify the event configuration explicitly.
   Until it exists, keep the branch without an early draft PR.
3. When all implementation and documentation are ready, obtain **one
   adversarial agent review** of the combined change. Address every actionable
   issue. Do not request repeated review cycles on each milestone.
4. Run the fast lane only after those fixes. It compiles affected Rust targets
   (including test targets) and checks formatting/lints, parses changed JS,
   validates workflow/doc contracts, and runs only the bounded cluster
   regressions named in §6. A docs-only diff runs link/diff checks without
   invoking Cargo. Name the target `make fast-check`; it is proposed here,
   not present at the inspected nzbd baseline.
5. Use that lane as the merge-candidate check, not a duplicate local run plus
   an identical remote run. Repairs rerun affected checks; reuse evidence
   only while the relevant source and dependencies are unchanged. Record the
   reviewed revision, final candidate/base, commands, results, and limitations.
6. Merge when that lane is green for the candidate being merged. Full unit
   suites, coverage, fuzzing, long cluster campaigns, and release builds run
   only by explicit dispatch/release or the separate batch process. Preserve
   dependency/security audits separately; do not delete their checks.
7. A merge does not implicitly deploy or restart nodes. Delete merged task
   branches, superseded scratch data, and reference clones after durable
   source/evidence links have been recorded. Keep the status page in the repo.

Avoid a generic impact-analysis service or the entire plurx validation catalog.
For this repository, a small changed-path selector with conservative handling
of lockfiles/shared types is sufficient. Set a normal warm fast-lane target of
about ten minutes; separate provisioning/cold compiler time in the record.
Do not skip a selected correctness scenario to make a stopwatch green.

## 6. Acceptance — a short set of failure scenarios

Extend the existing mock NNTP and cluster harness. Add a real-process helper
for suspension/termination so task cancellation is not mistaken for a stopped
OS process. Use the same scenarios for focused local debugging and final
lane execution; do not invent a second acceptance framework.

| Scenario | Required observable result |
|---|---|
| Three voters, one lost; then quorum lost | Surviving majority can publish; minority cannot publish; diagnostics remain available and report the condition |
| Leader/worker suspended past expiry, then resumed | No old token publishes queue state, PP selection, file completion, or history; committed ranges are retained |
| Heartbeat body hangs / response is lost | Absolute deadline fires independently; cancellation closes owned work; retried publication reconciles the same receipt |
| Pause/delete/file edit races completion | Newer intent survives; deleted job cannot resurrect; no new forbidden articles are handed out after control application |
| Worker dies halfway through a range | Other committed ranges have zero repeat NNTP hits; incomplete range resumes or retries without early whole-file finalization |
| Two PP attempts overlap after failure | Private scratch only; losing attempt cannot delete selected data; ambiguous script invocation is visible and not silently replayed |
| Account has fewer sockets than executors | Zero shares are allowed, progress is fair, old holder drains before expansion, unknown old sockets remain reserved |
| Low disk / shared mount becomes unresponsive | Bounded control/diagnostic response, no false output success, resumable work after recovery; temporary copy usage counted |
| Migration crash and retry | Same cluster/job identities, no duplicate commands/history, explicit rollback boundary |
| Settings advisory is unmet or unknown | Operator can still toggle the feature; no hidden capability approval or proof requirement |

Retain existing C1/C2 scenario coverage. Run the selected regression group
after review; broader historical failures go to the separate batch process
with their identities recorded. Do one finite real-Gluster smoke when node
access is available: one download/PP handoff, one worker restart, and one
controlled volume interruption/recovery on test data. An unavailable fleet
is recorded as unrun evidence and does not become an enablement gate or an
unbounded wait on the implementation PR. Tests that suspend or interrupt
storage run only in the isolated harness/test cluster, never against active
user workloads by inference.

[p-coordination]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/crates/plurx-core/src/cluster/coordination.rs
[p-store]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/crates/plurx-core/src/store/hiqlite_coordination.rs
[p-publication]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/crates/plurx-core/src/store/hiqlite_publication.rs
[p-lifetime]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/crates/plurx-core/src/store/publication.rs
[p-membership]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/crates/plurx-core/src/cluster/membership.rs
[p-transport]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/vendor/hiqlite/src/network/raft_client.rs
[p-frame]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/vendor/hiqlite/src/network/frame_io.rs
[p-ci]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/.github/workflows/main-fast-lane.yml
[p-pipeline]: https://github.com/pjunod/plurx/blob/8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176/docs/DEVELOPMENT_PIPELINE.md
