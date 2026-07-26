# Defect — "forget" doesn't forget, and the history cursor pays for it

**Status:** open · confirmed by probe 2026-07-26 · **Found:** during the
post-build review of integration phase 1 ([INTEGRATION_PLAN.md](INTEGRATION_PLAN.md))
· **Present since:** `1fdad15` (2026-07-17), when delete and the JSONL
rebuild first shipped together — though until `9f402d8` (same day, cluster
C2) added the throttled `refresh()` on the read path, the entry only came
back at the next daemon start, which is rare enough to look like something
else · **Needs:** a decision before a fix (§5)

Companion to [ARCHITECTURE.md](ARCHITECTURE.md) §8.6 (how history is
stored) — this is *one specific thing that store gets wrong and what to do
about it*.

`HistoryDb::delete` removes the SQLite index row and leaves the
append-only JSONL line in place, so the next `refresh()` imports the entry
straight back with a fresh, higher rowid. Two consequences, one for
operators and one for the integration surface:

- **The UI's "forget" is undone within one history poll.** Clicking it
  again just repeats the cycle. There is no way to remove a history entry.
- **`GET /api/v1/history?since_seq=` delivers that entry twice**, under two
  different cursor values, because the resurrected row is a new row as far
  as the index is concerned.

The second is why this was found: phase 1 made the rowid a public cursor,
which turned a latent storage bug into a wrong answer on a documented API.
The first is the one users actually hit, and it predates all of that.

---

## 1. Root cause — delete is the only transition that isn't durable

The JSONL is the authoritative, mergeable log; the SQLite index is a
derived read model rebuilt from it (ARCHITECTURE §8.6 / ADR-16). Every
state change to a history entry is written back to the log so it survives a
rebuild — except one:

| Transition | Written to the JSONL? | Survives a rebuild? |
|---|---|---|
| `record` (a job finished) | yes — the entry is appended | yes |
| `hide` (a client imported it) | yes — `set_hidden` re-appends the updated entry | yes |
| `restore` (un-hide) | yes — same path | yes |
| `mark_seen` (a client's poll listed it) | no — index-local by design, and cheap to lose | n/a, advisory |
| **`delete` (forget this entry)** | **no** | **no — it comes back** |

`hide` re-appends deliberately, with a comment saying why: *"so the hidden
state survives an index rebuild (the upsert makes the last JSONL line
win)"*. `delete` has no equivalent, so the rebuild's `INSERT … ON CONFLICT`
puts the row back with nothing to say it shouldn't.

The rebuild is not a rare recovery path. `refresh()` runs on **every**
`GET /api/v1/history`, on the compat `history` method, and in the
duplicate check `append` performs — throttled to one re-union per 5 s, and
`open()` seeds the index without touching the throttle, so the first read
after a daemon start rebuilds immediately rather than waiting out a
window. An *arr polling history every 30 s is, incidentally, a machine for
triggering this.

```
 client: POST /api/v1/history/2/actions/delete
            │
            ▼
   delete()  ── DELETE FROM history WHERE job_id = 2 ──▶ index: row gone
            └─ drop_spool(2) ─────────────────────────▶ NZB spool: gone
               (JSONL untouched — the line for job 2 is still there)
            │
 client: GET /api/v1/history            (the first poll past the 5 s
            │                                    refresh throttle — usually
            ▼                                    the very next one)
            │
            ▼
   refresh() ── rebuild_from_jsonl ── re-reads every history*.jsonl
            └─ insert(job 2) ─▶ no conflicting row ─▶ NEW rowid ─▶ back
```

---

## 2. Reproduction — confirmed, not inferred

Probe against `HistoryDb` directly on 2026-07-26, single-node layout
(`jsonl_dir = Some(state_dir/history)`, which is what the daemon uses).
Four entries recorded, then:

```
initial                       [(job 1, seq 1), (job 2, seq 2), (job 3, seq 3), (job 4, seq 4)]
delete(job 2)                 [(job 1, seq 1),                 (job 3, seq 3), (job 4, seq 4)]
refresh()                     [(job 1, seq 1), (job 3, seq 3), (job 4, seq 4), (job 2, seq 6)]
                                                                              ^^^^^^^^^^^^^^^ back
delete(job 2); refresh()      job 2 returns at seq 15      ← deleting again does not help
delete(job 4) ×3; refresh()   job 4 returns at seq 32      ← unbounded, one new rowid per cycle
```

**A consumer's view.** A consumer that had walked the cursor to `seq 4`
and asks `?since_seq=4` is handed job 2 at `seq 6` — an entry it already
imported, with a different cursor value, so no amount of remembering
"the last seq I saw" prevents it.

**The hidden case is different and worth stating separately.** An entry a
client hid (the "imported" signal) and someone then forgot comes back
**with `hidden = true` and `picked_up_by` intact**, because `hide` did
append. So it stays invisible to compat clients — Sonarr will not re-import
it — but it *is* visible to `?since_seq=` (hidden-inclusive by design,
§3.3 of the plan) and to the UI's own hidden-inclusive view. The
integration duplicate happens either way; the *arr re-import risk does not.

**Untouched rows keep their `seq`.** Job 1 stayed at 1 and job 3 at 3
across every cycle above. The cursor is stable and monotone for rows that
are never deleted, which is why this shows up as a duplicate rather than
as wholesale renumbering.

**The `seq` jump is a red herring, but explain it once so nobody chases
it.** Job 2 went 2 → 6, not 2 → 5, because SQLite's `AUTOINCREMENT`
advances `sqlite_sequence` on the whole re-import pass, not just on rows
that were actually inserted. On a real history of a few thousand entries,
each rebuild burns a few thousand ids. It is ugly and it is harmless:
`i64` has room for centuries of it, existing rows keep their ids, and
ordering is unaffected.

---

## 3. What it breaks, concretely

**Operators.** The UI's per-entry **forget** ("remove the history record,
keep files") and **delete files** both call `HistoryDb::delete`. Neither
sticks. The entry is back on the next render, and clicking again just
repeats the cycle — with the spool already dropped, so the row that comes
back has `can_requeue: false` and has lost its Undo. This is very likely
one of the two mechanisms behind the *"deleted items came back after
refresh"* field report of 2026-07-26 (STATUS.md, round 5); that
investigation found and fixed a queue-side cause, and this history-side
cause was never in scope.

**The undo-a-delete flow.** `history_requeue` re-adds the job and then
deletes the parked `DELETED` record, on the reasoning that *"the job is
queued again, so a `DELETED` record for it would be a lie"*. That delete
does not stick either, so after an Undo the operator has both a live job
and a `DELETED` history row claiming otherwise — exactly the lie the code
set out to avoid.

**Compat clients.** `HistoryFinalDelete` (nzbget's permanent-delete verb)
maps to the same call and is equally ineffective. Low blast radius: the
*arrs use `HistoryDelete` (which hides, and which works), not this.

**Native consumers on the cursor.** Duplicate delivery, as above. monarr's
importer is idempotent per download row, so the practical damage today is
a redundant scan notification rather than a double import — but "the
duplicate happens to be harmless downstream" is not a property this
repo gets to assume about every future consumer.

**Disk.** The JSONL never shrinks for deletes. It already never shrinks
(append-only), so this is not a new leak, but a fix that adds tombstones
should say what it does about compaction rather than leaving it open.

---

## 4. Workarounds, such as they are

- **Consumers:** dedupe on `(job, completed_at)` — the key the index is
  already `UNIQUE` on, so a duplicate is guaranteed byte-identical to the
  entry you already have. Documented in [USAGE.md](USAGE.md) and at
  `HistoryDb::list_since`.
- **Operators:** none. Deleting again does not help. Stopping the daemon
  and hand-editing `history*.jsonl` does, and nothing in the product should
  require that.

---

## 5. The decision this needs before anyone writes code

The fix is small either way. What it isn't is obvious, because it turns on
a semantic question nobody has answered yet:

> **When an operator forgets a history entry, do they mean "forget it
> here" or "forget it everywhere"?**

Today's code says "here" — `delete` is index-local, and the existing test
`record_list_delete_and_jsonl_rebuild` asserts on purpose that a fresh
index on another node still rebuilds the locally-deleted row. That is
defensible for a derived read model. It is also almost certainly not what
someone clicking "forget" expects, and it is unimplementable as stated,
because *this* node's own index is rebuilt constantly from the same log.

### Option A — durable tombstone in the JSONL

Append a tombstone record for `(job, completed_at)`; `rebuild_from_jsonl`
skips any key with a later tombstone than its entry.

- **Semantics:** forget means everywhere. Tombstones ride the shared
  volume like any other record and every node converges.
- **Cost:** a JSONL record shape that isn't a `HistoryEntry`, so the
  reader gains a variant (today it is "parse or skip the line"). The
  existing test's asserted behavior changes and its comment needs
  rewriting. Compaction needs a rule — a tombstone can be dropped only
  once every entry it covers has been dropped, which realistically means
  never, which is fine at this scale but should be a sentence in
  ARCHITECTURE rather than an accident.
- **Ordering:** the last line wins, as it already does for `hidden`, so a
  delete followed by a re-record of the same `(job, completed_at)` behaves
  the way the rest of the log does.

### Option B — local tombstone table in SQLite

A `deleted (job_id, completed_at)` table; the rebuild's insert skips
matching keys.

- **Semantics:** exactly today's stated design — index-local — but
  actually implemented, so it survives the refresh that currently undoes
  it. A wiped local index resurrects everything, which is correct: that
  is a rebuild from authority, and the authority never heard about the
  delete.
- **Cost:** the smallest change here. One table, one `NOT EXISTS` in the
  insert, no JSONL format change, no cross-node question opened.
- **Consequence to accept:** in a cluster, forgetting an entry on node A
  leaves it on node B. Whether that is a bug or the design is precisely
  the question above.

### Rejected

- **Delete the JSONL line.** Rewriting an append-only log on a shared
  volume, under concurrent appends from other nodes, to service a UI
  click. No.
- **Make `seq` a stored counter instead of the rowid**, so a resurrected
  row keeps its old cursor value. It cannot: the JSONL deliberately does
  not carry `seq` (a rowid is index-local; publishing one node's numbering
  would publish a number that is wrong everywhere else), so there is
  nothing to restore it from. And it fixes only the duplicate, leaving
  "forget doesn't forget" untouched — the smaller half of the defect.
- **Dedupe inside `list_since`.** Requires remembering what each consumer
  has already been handed. The cursor exists so that the server doesn't
  have to.

**Recommendation:** Option B if the answer is "forget it here", Option A
if it's "forget it everywhere". Option A is the better product behavior and
the one worth the extra work, but it is a cross-node semantic change and
therefore Paul's call, not an implementer's.

---

## 6. Acceptance — what a fix has to demonstrate

Runnable checks, so "fixed" isn't a feeling:

1. `delete` then `refresh()` on the same `HistoryDb`: the entry is gone
   and stays gone across three further refresh cycles.
2. Same, for an entry that was `hide`-ed first — the tombstone must beat
   the hidden re-append regardless of which line is last in the log.
3. `list_since(0, …)` never returns two rows with the same
   `(job, completed_at)`, across a delete/refresh cycle.
4. A consumer holding cursor `N` from before the delete is never handed
   the deleted entry at any cursor after it.
5. `history_requeue` leaves no `DELETED` row behind for a job it put back
   in the queue — the symptom in §3, asserted end to end.
6. Option A only: a second `HistoryDb` opened on the same JSONL directory
   with an empty index does not rebuild the deleted entry. Option B only:
   it *does*, and the test says in its name that this is the chosen
   semantics rather than an oversight.
7. `crates/nzbd-compat/tests/golden.rs` passes unmodified.

Update in the same commit: this file's status line,
[ARCHITECTURE.md](ARCHITECTURE.md) §8.6 (the durability table in §1 above
belongs there once it's true), the `HistoryDb::list_since` doc comment,
[USAGE.md](USAGE.md)'s dedupe advice (it can go away under either option),
and the "Flagged, not fixed" entry in [STATUS.md](../STATUS.md).
