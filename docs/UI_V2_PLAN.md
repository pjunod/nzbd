# UI v2 — a queue you can watch

**Status:** ready to build · **Fixes:** field report 2026-07-25 #6 (dead
queue · eaten delete click · confirm() popup · job that wouldn't leave) ·
**Written:** 2026-07-25 · **Verified against:** `f84a33e` — re-verify every
file:line anchor below before editing; the tree moves fast.

This plan was agreed with Paul on 2026-07-25 (decisions in §2). Read it top
to bottom once, then work milestone by milestone (§6): one milestone = one
commit, each with tests, docs, a STATUS.md update, and the full gate set
(`cargo fmt --all` · `cargo clippy --workspace --all-targets -- -D warnings`
· `cargo test --workspace` · `cargo +1.85 check --workspace`). Before
coding, read [ARCHITECTURE.md](ARCHITECTURE.md) §5 + §8.1 + §10.1,
the phase-4 bullets in [../STATUS.md](../STATUS.md), and the three source
anchors: `crates/nzbd-api/ui/index.html` (the whole UI),
`crates/nzbd-api/src/lib.rs` (API + SSE), `crates/nzbd-engine/src/owner.rs`
(queue owner). Standing instruction: if a step seems to require changing
anything in §9 (non-goals), or an interface differs from §5's contract in a
way that changes the design, **stop and flag it** instead of improvising.

## 1. What Paul hit, and what each symptom actually was

Four symptoms, three distinct causes — only one of them is fixed at HEAD:

| Symptom (field report) | Root cause | State at `f84a33e` |
|---|---|---|
| Queue frozen, "never updates", rate dead | Deployed nuc3 container predates 9df6b7d: old UI polled with header-less `fetch()`, browser heuristically cached `/api/v1/jobs` and re-served the first response forever | **Fixed at HEAD** (router-wide `Cache-Control: no-store` + 1 Hz SSE `tick`), **not deployed** → §6 M0 |
| Deleted job still in the queue, even after the daemon logged `job deleted`, even after page reload | Same stale-cache bug — the server deleted it fine (`owner.rs:1664` removes the job, publishes a fresh snapshot, emits `job_deleted`); the browser kept rendering the cached list | **Fixed at HEAD, not deployed** → M0 |
| First delete click did nothing | `renderQueue()` assigns `tbody.innerHTML` on every 1 Hz tick (`index.html:637`). A tick landing between mousedown and mouseup destroys the pressed button — the browser never fires `click`. At 1 Hz this is a coin flip. Same cause: detail-panel scroll reset every second, progress-bar transition restarts, text selection impossible | **Live bug** → M1 |
| Confirm popup on delete; no feedback if an action fails | `confirm()` hardcoded (`index.html:630`, also history at 785/786); `jobAction()` (`index.html:727`) never checks the response — a failed POST is indistinguishable from success, and even a successful delete waits a full round trip before the row reacts | **Live bug** → M2 + M4 |

The lesson that shapes everything below: **a UI that rebuilds the world
every second cannot be interacted with, and a UI that trusts its POSTs
cannot be trusted.** v2 renders in place and treats every action as
pending-until-confirmed.

## 2. Decisions already made — do not reopen

Settled with Paul 2026-07-25; each lists what was rejected and why.

1. **Stay no-build, single embedded file; hand-roll a keyed reconciler.**
   `index.html` remains one self-contained page compiled in via
   `include_str!` — zero toolchain, an explicit simplification from the
   original Svelte plan (ARCHITECTURE.md §12 still says Svelte; M7 fixes
   that doc). Rejected: vendoring preact+htm (13 KB of third-party JS in
   the tree to diff a `<table>` we can diff in ~150 lines); the Svelte SPA
   (adds a Node build chain to CI for no behavior we need). The reconciler
   is small because the problem is small: one keyed list of rows plus a
   handful of scalar tiles.
2. **Delete is instant and undoable — no `confirm()`, anywhere.**
   One click: the row vanishes optimistically, the POST fires, a toast
   offers Undo for ~8 s. Server side, delete *parks*: the regenerated NZB
   (the `/jobs/{id}/nzb` exporter already proves queue state round-trips)
   is spooled and a `DELETED` history entry written, with a `requeue`
   action to bring it back — which is also NZBGet-parity behavior (deleted
   groups land in history there too). Rejected: bare instant delete (a
   misclick on a 60 GiB job costs a full re-download); two-step inline arm
   (still two clicks — the thing Paul was complaining about). History's
   "delete files" (actually destructive) gets a two-step inline arm, never
   a popup.
3. **All four watchability extras are in scope** (§6 M5/M6): rate-history
   sparkline, live Logs tab over SSE, background-tab title ticker,
   per-server rate chips.
4. **The look stays.** Same layout, tabs, palette, CSS. This is a
   behavioral rebuild, not a reskin — Paul's complaint is liveness, not
   aesthetics. Diffs that only move pixels are scope creep.

## 3. Client architecture — render in place, act optimistically

```
  /api/v1/events (SSE) ──────────────┐
   tick · job_* · queue_* · hb · log │        ┌──────────────────────────┐
                                     ├──▶ store ──▶ render(store, pend)  │
  poll fallback (SSE quiet > 6.5s) ──┘   status   │  keyed reconcile:    │
                                         jobs[]   │  queue rows, history │
  user click ─▶ pending op ──────────▶ overlay    │  rows, stat tiles,   │
   (delete/pause/move)  │                 ▲       │  badges, chips,      │
                        ▼                 │       │  sparkline, title,   │
                   POST /actions ─▶ ok? ──┘       │  detail panel, logs  │
                        │ !ok / timeout           └──────────────────────┘
                        ▼
                 revert + error toast
```

**The store** is a plain object — `{status, jobs, history, clients, logs,
conn}` — written only by SSE handlers, the poll fallback, and pending-op
reconciliation. Render never fetches; fetch never renders. (Today's code
interleaves both freely — that's how silent failure hid.)

**Five rendering laws** (M1 enforces these; the node test in §7 pins them):

1. After boot, `innerHTML` is never assigned to a container that holds live
   rows. It may build the *inside of a brand-new detached node* (row
   templates); updates after attachment are targeted: `textContent`,
   `style.width`, `classList.toggle`, `disabled`, `title`.
   Exception: swapping to/from the `.empty` placeholder row.
2. Row identity is `tr.dataset.jobId`. The reconciler keeps
   `Map<jobId, tr>`; update = mutate in place, add = insert at position,
   remove = drop node, reorder = `insertBefore`. An untouched job must keep
   the *same DOM node* across ticks — that is the property that makes
   clicks, hover, selection, and CSS transitions survive.
3. A cell is written only when its rendered value changed (compare against
   a `prev` model cached on the row). 1 Hz writes to 2 of 6 cells, not 6.
4. One delegated `click` listener on `document` dispatches on
   `closest("[data-action]")` → `{action, jobId}`. No inline `onclick`
   strings — this also ends the escape-job-names-into-JS quoting pattern
   (`index.html:630`) that delegation makes unnecessary.
5. The job-detail panel is a stable subtree: files table reconciled by
   file id, activity tail appended incrementally, scroll position never
   reset by an update.

**Pending-op overlay.** Every mutating click registers
`{key: jobId+":"+kind, kind, jobId, at, revert()}` and applies its effect
to the view immediately: `delete` adds the id to a `hiddenPending` set the
reconciler filters out; `pause`/`resume` overrides the status chip; `move`
reorders locally. Resolution, in order: server state reflects the intent
(tick or event) → drop the op; POST returns `!r.ok` or throws → revert now,
error toast with the server's `error` string; op older than 5 s → revert,
toast "…didn't take". The overlay re-applies after every reconcile so a
tick can't flash the old state back mid-flight.

**Toasts** replace both `confirm()` and silence: one fixed bottom-right
stack (max 3, `aria-live="polite"`),
`toast({text, kind: "info"|"error", action?: {label, fn}, ttlMs})`.
Delete's toast carries `Undo` (§4.2); errors stay 10 s or until dismissed.

**Connection honesty.** `conn` state machine: `live` (tick or `hb` ≤ 6.5 s
old) · `reconnecting` (EventSource errored; it retries itself; poll
fallback active) · `unreachable` (polls failing too → banner across the
top, not just a gray dot). Today's UI can't distinguish "idle queue" from
"stream silently dead" because idle ticks dedupe away — the `hb` event
(§4.1) closes exactly that hole.

## 4. Server additions — four small, separable pieces

### 4.1 `hb` heartbeat event

The tick loop (`nzbd-api/src/lib.rs:805-820`) dedupes identical payloads so
an idle queue costs nothing — correct, but it makes client-side staleness
detection impossible. Add: when the tick was suppressed as a duplicate and
>5 s have passed since the last frame of any kind, emit
`event: hb` / `data: {"now_unix":N}`. Axum's `KeepAlive` comment lines stay
(proxies need them); `hb` is for the client's staleness clock, comments are
invisible to `EventSource` by design.

### 4.2 Delete parks to history + `requeue`

The engine's `delete_job` (`owner.rs:1664`) stays byte-for-byte as is —
single-writer authority is not renegotiated by a UI plan. The parking
happens in the API layer, which already holds both handles:

- `job_action` delete branch (`nzbd-api/src/lib.rs:631`): when
  `st.history` is `Some`, first `engine.export_job(id)` (exists —
  `nzbd-engine/src/lib.rs:499`, used by `get_job_nzb` at `lib.rs:498`),
  render with the existing `job_to_nzb()`, then `engine.delete_job(...)`.
  On `Ok(true)`: spool the XML to `<history-local-dir>/nzbs/<job>.nzb` and
  write a history entry with status `DELETED`, the job's name / category /
  size / `completed_at_unix = now`, and `can_requeue: true`. Response
  becomes `{"ok": true, "parked": true|false}` — the UI shows Undo only
  when `parked`. The export→delete gap is a benign race: if the job
  finishes in between, delete returns `Ok(false)` → 404 → the tick
  reconciles the UI; do not add locking for this.
- Race note: `delete-files` parks the record too (the files are gone but
  requeue can re-download). A job still in `Fetching` has no articles to
  export — park its `url` instead and requeue via `add_url`; re-verify the
  URL's location on the job/queue types at build time.
- New history action `requeue` (`history_action`, `lib.rs:1406`, routes at
  `lib.rs:1466`): read the spooled NZB, `engine.add_nzb_opts(name, bytes,
  AddOpts{category, priority, ..})` (`nzbd-engine/src/lib.rs:396`), and on
  success remove the history entry + spool file; respond `{"id": new_id}`.
  Missing spool file or unknown entry → 404 with a plain `error` string.
- Spool hygiene: delete the `.nzb` whenever its entry is removed (`delete`,
  `delete-files`, `requeue`); on `HistoryDb` open, sweep orphans. NZBs are
  single-digit MB; no quota needed.
- Where finished jobs get their history entries written today, mirror the
  same insert shape for `DELETED` (locate it from `HistoryDb` usages —
  `crates/nzbd/src/main.rs:488-545` wires the handles; re-verify, it may
  live in the post manager).
- Compat shim: NZBGet also parks deleted groups into history, so wiring
  the same helper behind `GroupDelete` is parity-aligned — do it only if
  it drops in cleanly; otherwise flag it as a follow-up rather than
  distorting this milestone.

### 4.3 Per-server rates

`SpeedMeter` already attributes wire bytes per *job* (`rate.rs:131
add_for`, drained by the owner tick into per-job EMAs). Mirror it per
*server*: `add_for_server(server, n)` fed at the same transport read sites
(the connection task knows its `ServerId`), `drain_servers()`, owner tick
computes the same EMA it uses for jobs, snapshot carries it. Surface:
extend the existing `ServerVolume` rows (`snapshot.rs`) with `rate_bps` and
`name` (owner knows server configs), and include them in the status DTO /
tick payload. Re-verify `status_dto()` (`nzbd-api/src/lib.rs:296`) and the
owner's per-job EMA site before copying the pattern.

### 4.4 `log` SSE events

`LogBuffer` already has a monotonic cursor (`logbuf.rs:30` `LogRecord.id`,
`logbuf.rs:81` `since(after, limit)`). In the per-connection SSE task, keep
`last_log_id`; each loop iteration drain `since(last_log_id, 200)` and, if
non-empty, emit one `event: log` with
`data: {"entries":[LogRecord…], "dropped": n}` (`dropped` = how many the
200 cap cut; the client shows "… N lines skipped"). Records carry
`scope` + `job` already — the client filters; the server does not grow
per-connection filter state. Batching rides the existing 1 s loop: a log
"tail -f" that updates once a second is the right cost/fidelity trade.

## 5. Contract — exact shapes (re-verify anchors at build time)

**Existing anchors this plan builds on** (all at `f84a33e`):

| Thing | Where |
|---|---|
| Full-table `innerHTML` render | `crates/nzbd-api/ui/index.html:585-638` (`renderQueue`), history at `:774`, logs at `:828` |
| `confirm()` calls to remove | `index.html:630` (queue), `:785`, `:786` (history) |
| Fire-and-forget actions | `index.html:727-730` (`jobAction`), `:790-793` (`histAction`) |
| SSE client + tick handler + poll fallback | `index.html:939-969`, `:1450-1466` |
| SSE server loop + `tick_payload` | `nzbd-api/src/lib.rs:792-843`, `:845-848` |
| Event name wire mapping | `lib.rs:846-908`; enum: `nzbd-engine/src/events.rs` |
| `job_action` / `history_action` / routes | `lib.rs:622-649` / `:1406-1450` / `:1455-1484` |
| Engine delete / export / add | `owner.rs:1664-1697` / `engine/src/lib.rs:499` / `:396` (`AddOpts` at `:260`) |
| Speed metering | `engine/src/rate.rs:86-155` |
| Read model | `engine/src/snapshot.rs` (`JobSummary`, `QueueSnapshot`) |
| UI boot test net | `crates/nzbd/tests/ui_boot.rs` + `ui_boot_harness.js` |

**Wire changes** (complete list — nothing else changes shape):

| Surface | Change |
|---|---|
| `POST /api/v1/jobs/{id}/actions/delete` (+`delete-files`) | Response `{"ok":true}` → `{"ok":true,"parked":bool}` |
| `POST /api/v1/history/{id}/actions/requeue` | New. `200 {"id":u32}` on success · `404 {"error":…}` unknown entry or spool gone · `501` no history store |
| `GET /api/v1/history` entries | New fields: `can_requeue: bool`; `DELETED` becomes a status the native store writes (the UI's `histClass` already styles it) |
| SSE `event: hb` | New. `{"now_unix":i64}`, only when tick is dedup-suppressed >5 s |
| SSE `event: log` | New. `{"entries":[{id,kind,time_unix,text,scope,job}], "dropped":u32}` |
| `tick` payload `status` | Gains per-server `{server,name,rate_bps}` rows (exact field placement per §4.3 re-verify) |

Client JS — the reconciler's required shape (names matter; tests import
them via the harness's `__nzbd_test` hook, §7):

```js
// pure: JobSummary + ctx -> row view-model (strings/flags only, no DOM)
rowModel(j, {idx, count, healthAbortArmed, pendingOps}) -> {
  id, name, sub, st, stCls, pct, fill, detail, size, health, acts }
// keyed reconcile of tbody against models; DOM ops ONLY via `dom` adapter
reconcileRows(tbody, models, dom)   // add/move/update/remove, law #2/#3
applyRow(tr, model)                 // per-cell diff against tr.__prev
pending.apply(op) / pending.resolve(tickState) / pending.revert(key)
toast({text, kind, action, ttlMs})
```

`dom` adapter = the only DOM surface the reconciler may touch:
`createElement · insertBefore · removeChild · nextSibling · dataset ·
textContent · className · classList.toggle · style · disabled · title ·
setAttribute`. That list is what the node fake implements; using anything
outside it breaks the CI net by construction.

## 6. Milestones — one commit each, in this order

### M0 · Ship what's already fixed (Paul, ops — not the coding agent)

`main` is pushed through `f84a33e`; nuc3 still runs a pre-9df6b7d image.
Rebuild + redeploy, and fix the known compose gap first (deployed compose
lacks the `./config:/etc/nzbd` bind — wizard config dies on rebuild
without it): `mkdir config && docker cp nzbd:/etc/nzbd/nzbd.toml ./config/
&& chown 1000 config/nzbd.toml`, add the mount, `docker compose up -d
--build`. **Acceptance:** the UI header shows `● live updates` and the rate
tile moves every second during a download. Half of report #6 dies here
before a line of v2 is written.

### M1 · Keyed renderer — stop destroying the DOM

Rewrite queue/history/logs/stats/badges rendering per §3's five laws:
store object, `rowModel`/`reconcileRows`/`applyRow`, delegated clicks
(inline `onclick` attributes deleted), detail panel as a stable subtree.
Behavior is otherwise pixel-identical — `confirm()` and fire-and-forget
POSTs survive until M2/M4 so this diff stays reviewable. **Acceptance:**
(a) new node test `crates/nzbd/tests/ui_dom.rs` (§7) green: row identity
stable across 50 simulated ticks, cell-write counts minimal, reorder
preserves nodes; (b) `ui_boot` still green; (c) manual: hold the mouse
down on `delete` across a tick — the click fires (was: coin flip);
select a job name and watch it stay selected while progress moves.

### M2 · Honest actions — optimistic ops + toasts

Pending-op overlay per §3 for pause/resume/move/delete-visual;
`jobAction`/`histAction` check `r.ok`, surface the server's `error` in an
error toast, revert on failure/timeout; connection states `live /
reconnecting / unreachable` with banner. **Acceptance:** node test drives
apply→confirm and apply→timeout→revert paths; manually: `docker stop
nzbd` mid-click shows the error toast and the row springs back; pause
lands visually in <50 ms with the server confirming within a tick.

### M3 · Parked delete, server side

§4.2 in full: spool + `DELETED` entry + `parked` flag + `requeue` action +
spool hygiene. Engine untouched. **Acceptance:** new e2e in
`crates/nzbd/tests/daemon.rs` (nserv-backed): add → delete → job absent
from `/api/v1/jobs` + history shows `DELETED` with `can_requeue` → POST
`requeue` → `200 {id}` → job downloading again → history entry and spool
file gone. Plus: delete with no history store returns `parked:false`;
requeue of an unknown id → 404.

### M4 · Delete UX — kill `confirm()`

Queue delete: instant optimistic removal + POST; toast `Deleted <name> —
Undo` (8 s) wired to `requeue` when `parked`; on undo, the job's return
rides the normal `job_added` event. History `forget`: plain instant.
History `delete files`: two-step inline arm (button morphs to `sure?` for
3 s, then disarms). Zero `confirm(`/`alert(` calls left in the file —
grep-clean. **Acceptance:** e2e-driven UI check in the node harness for
the toast/undo state machine; `grep -c "confirm(" index.html` = 0; manual:
delete a downloading job → row gone instantly, Undo brings it back with a
fresh id, no dialog anywhere.

### M5 · Liveness extras, server side

§4.1 `hb` + §4.3 per-server rates + §4.4 `log` events. **Acceptance:**
Rust tests: idle SSE stream emits `hb` within 6 s (exists test pattern:
first-frame-is-tick test nearby); `log` batches respect the 200 cap and
set `dropped`; per-server EMA sums ≈ header rate in the e2e harness
(same-measurement invariant the per-job rates already keep).

### M6 · Liveness extras, client side

Sparkline (`<canvas>` in the rate tile, ring of last 180 tick rates, DPR-
aware, redraw ≤1/s, hidden-tab skip); title ticker (`▼ 93 MiB/s · 12m —
nzbd`, `⏸ paused — nzbd`, reset when idle); per-server chips next to the
badges (name + rate, red when in `blocked_servers`); Logs tab consumes
`log` events into a 500-line client ring (DOM capped, drop-oldest,
"N skipped" marker on `dropped`), scope toggles filter client-side, poll
remains only as backfill on tab open; job-detail activity tail feeds from
the same stream filtered by `job`. **Acceptance:** node tests for ring
math + filter logic; manual: Logs tab behaves as `tail -f` during a
download; tab title updates in the background; per-server chips sum ≈
header rate.

### M7 · Docs sweep + close-out

Rewrite ARCHITECTURE.md §12 to describe the as-built UI (single embedded
file, store + keyed reconciler + pending ops + SSE tick/hb/log — replacing
the stale Svelte paragraph, with the decision note and its why), extend
§10.1's SSE paragraph with `hb`/`log`, document `requeue` + `parked` in
USAGE.md's UI/API sections, final STATUS.md pass. **Acceptance:** no doc
in `docs/` contradicts shipped behavior; `git grep -i svelte docs/` returns
only history/decision context, not present-tense claims.

## 7. Test strategy — the net that keeps this honest

- **`crates/nzbd/tests/ui_dom.rs` + `ui_dom_harness.js` (new, M1):** same
  self-skip-without-node pattern as `ui_boot.rs`. The harness implements
  the §5 `dom` adapter as a ~80-line fake (children arrays, dataset,
  write-counters on every mutating call), extracts the page script, and
  reaches the pure functions through a `window.__nzbd_test` hook the page
  exposes only when `typeof __nzbd_test_enable !== "undefined"`. It
  asserts: node identity across ticks, minimal writes (law #3, via the
  counters), keyed reorder, pending-op apply/confirm/revert, toast/undo
  state machine, log-ring cap. No jsdom, no npm — the adapter interface
  exists precisely so a fake this small is sufficient.
- **`ui_boot.rs`** stays as-is and must stay green every milestone — it
  catches "parses but dies at load", which big single-file refactors love
  to produce.
- **Rust:** M3's e2e lifecycle test; M5's SSE `hb`/`log`/rate tests beside
  the existing cache-header matrix and first-frame-is-tick tests in
  `daemon.rs`; history-store unit tests for `record_deleted`/requeue/spool
  sweep next to the existing history tests.
- **Manual, once per milestone, on the real nuc3 deployment:** the
  acceptance lines marked "manual" above. The click-across-a-tick and
  select-text checks are the ones a shim can't fully prove.
- **Gates every commit** (see orientation) — and note in the commit message
  that they ran, per repo convention.

## 8. Commit discipline

Repo style (`git log` is the reference): `type(scope): summary`, prose
body explaining field report → cause → change list → what the regression
test proves → running test count, then the `Co-Authored-By` +
`Claude-Session` trailers. **STATUS.md is updated in every feature commit**
(standing rule, 2026-07-17) — each milestone adds/flips its line under the
phase-5 "UI v2" entry. Suggested subjects:

```text
M1  feat(ui): keyed in-place renderer — clicks, selection and scroll survive ticks
M2  feat(ui): optimistic actions with revert + toasts; honest connection states
M3  feat(api,state): delete parks NZB to history as DELETED + requeue action
M4  feat(ui): instant undoable delete — confirm() removed everywhere
M5  feat(api,engine): SSE hb + log events; per-server wire rates
M6  feat(ui): sparkline, title ticker, server chips, live log tail
M7  docs: ARCHITECTURE §12 as-built UI; USAGE requeue/parked
```

## 9. Non-goals — guardrails, each with its reason

- **No npm, no bundler, no framework, no TypeScript.** The zero-toolchain
  UI is a recorded decision (STATUS.md phase 4); reversing it is a
  Paul-level decision, not an implementation detail.
- **No WebSocket migration.** SSE + auto-reconnect is doing its job
  (ADR-11 in ARCHITECTURE.md §15); the liveness gaps were rendering and
  cache bugs, not transport bugs.
- **No visual redesign.** §2.4. Same CSS, same layout. Resist.
- **No virtualized tables yet.** Home queues are tens of rows; the keyed
  reconciler makes 1 Hz updates cheap. Flag for revisit only if a real
  queue exceeds ~500 rows.
- **No engine-internal changes** beyond §4.3's metering mirror. In
  particular `delete_job`'s semantics (`owner.rs:1664`) — single-writer
  authority and the teardown invariant are load-bearing (STATUS.md,
  896f466).
- **No splitting `index.html` into multiple embedded assets** without
  flagging first — it's probably the right call past ~120 KB, but it
  changes the "one self-contained page" story that's documented in three
  places.
- **No auth work, no compat-surface redesign.** `requeue` is native-API
  only for now; compat `HistoryRedownload` parity is a flagged follow-up.
- **Don't "fix" the idle-tick dedupe by making ticks unconditional** —
  the quiet-stream property is deliberate (battery/radio on phone PWAs);
  that's exactly why `hb` exists.

## 10. Done means

Paul opens the deployed UI during a real download and: the rate tile,
progress bars and sparkline move every second; he can select a job name,
hover a row, and scroll the detail panel while it updates; he deletes a
job with one click and the row is gone before his finger lifts, with Undo
available for 8 s and the job actually gone from `/api/v1/jobs` and parked
in history; a killed daemon produces a visible banner within seconds, and
a failed action tells him so instead of pretending. Zero `confirm()`
dialogs remain. 211+ tests still green, clippy clean, MSRV 1.85 intact,
and every milestone's line is flipped in STATUS.md.
