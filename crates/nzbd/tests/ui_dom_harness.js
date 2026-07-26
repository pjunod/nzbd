// UI DOM harness: proves the embedded page's renderer obeys the five
// rendering laws in docs/UI_V2_PLAN.md §3, using a ~100-line fake of the
// `dom` adapter instead of jsdom. No npm, no browser.
//
// The adapter interface exists precisely so a fake this small is enough:
// the reconciler and every row builder may touch the DOM ONLY through it,
// so counting calls on the fake counts every write the renderer makes.
//
// What this catches, concretely (field report 2026-07-25): a renderer that
// rebuilds rows every tick destroys the button you are pressing between
// mousedown and mouseup, so the click never fires. `identity across ticks`
// below is that bug's regression test.
"use strict";
const fs = require("fs");
const vm = require("vm");

const htmlPath = process.argv[2];
const html = fs.readFileSync(htmlPath, "utf8");
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
if (!scriptMatch) { console.error("no inline <script> found"); process.exit(2); }

// ---------------------------------------------------------------------------
// The fake DOM adapter. Nodes are plain objects with a children array; every
// mutating call bumps a counter so the tests can assert "writes only what
// changed".
// ---------------------------------------------------------------------------
const counts = { create: 0, insert: 0, remove: 0, text: 0, cls: 0, style: 0, prop: 0, attr: 0, data: 0 };
function resetCounts() { for (const k of Object.keys(counts)) counts[k] = 0; }

function node(tag, cls) {
  return {
    tag, className: cls || "", children: [], parent: null,
    dataset: {}, style: {}, attrs: {}, textContent: "",
    title: "", disabled: false, hidden: false,
    scrollTop: 0, clientHeight: 0, scrollHeight: 0,
  };
}
const fake = {
  create(tag, cls) { counts.create++; return node(tag, cls); },
  append(parent, n) { n.parent = parent; parent.children.push(n); return n; },
  insertBefore(parent, n, ref) {
    counts.insert++;
    if (n.parent) {
      const i = n.parent.children.indexOf(n);
      if (i >= 0) n.parent.children.splice(i, 1);
    }
    const at = ref ? parent.children.indexOf(ref) : -1;
    if (at >= 0) parent.children.splice(at, 0, n); else parent.children.push(n);
    n.parent = parent;
  },
  remove(parent, n) {
    counts.remove++;
    const i = parent.children.indexOf(n);
    if (i >= 0) parent.children.splice(i, 1);
    n.parent = null;
  },
  first(parent) { return parent.children[0] || null; },
  next(n) {
    if (!n.parent) return null;
    return n.parent.children[n.parent.children.indexOf(n) + 1] || null;
  },
  text(n, s) { counts.text++; n.textContent = s; },
  cls(n, s) { counts.cls++; n.className = s; },
  toggle(n, c, on) { counts.cls++; n[c] = !!on; },
  style(n, k, v) { counts.style++; n.style[k] = v; },
  prop(n, k, v) { counts.prop++; n[k] = v; },
  attr(n, k, v) { counts.attr++; n.attrs[k] = v; },
  data(n, k, v) { counts.data++; n.dataset[k] = v; },
};

// ---------------------------------------------------------------------------
// Minimal sandbox: enough for the page script to reach its own bottom, where
// it publishes `window.__nzbd_test`.
// ---------------------------------------------------------------------------
// Elements the page reaches by id are a mini-DOM (real child lists), so the
// live renderer can run end to end against them — that is how the toast
// stack and the pending overlay get exercised for real.
const failures = [];
const ids = new Set([...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]));
function stubEl(id) {
  const t = {
    id, style: {}, dataset: {}, attrs: {}, hidden: false, disabled: false,
    value: "", textContent: "", innerHTML: "", className: "", title: "",
    checked: false, scrollTop: 0, clientHeight: 0, scrollHeight: 0,
    children: [], parentNode: null,
    classList: { toggle() {}, add() {}, remove() {} },
    querySelectorAll: () => [], querySelector: () => null,
    addEventListener() {}, replaceWith() {},
    appendChild(n) { if (n.parentNode) n.parentNode.removeChild(n); t.children.push(n); n.parentNode = t; return n; },
    removeChild(n) { const i = t.children.indexOf(n); if (i >= 0) t.children.splice(i, 1); n.parentNode = null; },
    insertBefore(n, ref) {
      if (n.parentNode) n.parentNode.removeChild(n);
      const at = ref ? t.children.indexOf(ref) : -1;
      if (at >= 0) t.children.splice(at, 0, n); else t.children.push(n);
      n.parentNode = t;
    },
    get firstChild() { return t.children[0] || null; },
    get nextSibling() {
      if (!t.parentNode) return null;
      const s = t.parentNode.children;
      return s[s.indexOf(t) + 1] || null;
    },
    select() {}, click() {}, focus() {}, matches: () => false,
    reportValidity: () => true,
    setAttribute(k, v) { t.attrs[k] = v; }, removeAttribute() {}, getAttribute: () => null,
    closest: () => null,
  };
  return t;
}
const elCache = new Map();
// Selector-routed queryAll, for the handful of places the page reaches for
// a *set* of elements. `.wrap > *` is the one that matters: the first-run
// wizard hides the app through it.
const qsa = new Map();
function selMatch(el, sel) {
  return sel.split(",").map((x) => x.trim()).filter(Boolean).some((one) => {
    if (one.startsWith("#")) return el.id === one.slice(1);
    if (one.startsWith(".")) return (" " + el.className + " ").includes(" " + one.slice(1) + " ");
    return el.tag === one;
  });
}
function wrapChild(tag, id, cls) {
  // Reuse the id-addressed stub so the page and the test see one object.
  const el = id ? (elCache.get(id) || (elCache.set(id, stubEl(id)), elCache.get(id))) : stubEl("_" + tag);
  el.tag = tag;
  if (cls) el.className = cls;
  el.matches = (sel) => selMatch(el, sel);
  return el;
}
// Scripted daemon. `routes` maps a URL substring to {status, body}; the
// default is a dead daemon, which is what the boot path should survive.
const routes = new Map();
const seen = [];
async function routeFetch(url, init) {
  seen.push({ url, method: (init && init.method) || "GET" });
  for (const [frag, res] of routes) {
    if (String(url).includes(frag))
      return { ok: res.status < 400, status: res.status, json: async () => res.body, text: async () => "" };
  }
  return { ok: false, status: 503, json: async () => ({}), text: async () => "" };
}
const sandbox = {
  console,
  __nzbd_test_enable: true,
  document: {
    getElementById(id) {
      if (!ids.has(id)) failures.push(`$("${id}") — no element with that id in the markup`);
      if (!elCache.has(id)) elCache.set(id, stubEl(id));
      return elCache.get(id);
    },
    querySelectorAll: (sel) => qsa.get(sel) || [],
    createElement: (t) => stubEl("_" + t),
    documentElement: stubEl("_root"),
    addEventListener() {},
  },
  navigator: { serviceWorker: { register: () => Promise.resolve() } },
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  location: { reload() {} },
  // Swappable so the action tests can script the daemon's answers.
  fetch: async (url, init) => routeFetch(url, init),
  EventSource: class {
    constructor(u) { this.url = u; this.readyState = 0; this.listeners = {}; }
    addEventListener(n, f) { this.listeners[n] = f; }
    close() { this.readyState = 2; }
  },
  setInterval: () => 0, clearInterval() {},
  setTimeout: () => 0, clearTimeout() {},
  confirm: () => true, alert() {},
  URL: { createObjectURL: () => "blob:x", revokeObjectURL() {} },
  Blob: class {}, Date, Math, JSON, Promise, Number, String, Array, Object, Set, Map,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
process.on("unhandledRejection", (e) => {
  console.error("UI DOM HARNESS: unhandled rejection: " + (e && e.stack ? e.stack : e));
  process.exit(1);
});

vm.createContext(sandbox);
try {
  vm.runInContext(scriptMatch[1], sandbox, { filename: "index.html<script>" });
} catch (e) {
  console.error("script threw at load: " + e.message);
  process.exit(1);
}
const T = sandbox.window.__nzbd_test;
if (!T) { console.error("page did not expose window.__nzbd_test"); process.exit(1); }

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------
let checks = 0;
function ok(cond, what) {
  checks++;
  if (!cond) failures.push(what);
}
function eq(a, b, what) { ok(a === b, `${what}: expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); }

function job(id, over) {
  return Object.assign({
    id, name: "job " + id, status: "downloading", category: "tv", priority: 0,
    size_bytes: 1000, downloaded_bytes: 100, failed_bytes: 0, remaining_bytes: 900,
    total_articles: 10, done_articles: 1, failed_articles: 0,
    files_total: 2, files_done: 0, health: 1000, critical_health: 850,
    rate_bps: 1024, retried_articles: 0, assigned_node: null, pp_done: false,
    dupe_key: "", dupe_score: 0,
  }, over || {});
}
const models = (jobs) => jobs.map((j, i) => T.rowModel(j, { idx: i, count: jobs.length }));

// --- 1. rowModel is pure: no DOM, strings and flags only -------------------
{
  const m = T.rowModel(job(7, { downloaded_bytes: 500 }), { idx: 0, count: 2 });
  eq(m.key, "j7", "row key is the job id");
  eq(m.pct, "50%", "percent from downloaded/size");
  eq(m.st, "DOWNLOADING", "status uppercased");
  eq(m.upDisabled, true, "first row cannot move up");
  eq(m.downDisabled, false, "not the last row");
  for (const [k, v] of Object.entries(m))
    ok(v === null || ["string", "number", "boolean"].includes(typeof v),
      `rowModel.${k} is a scalar (got ${typeof v})`);
}

// --- 2. identity across ticks (THE regression test) ------------------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2), job(3)];
  T.reconcileRows(tbody, models(jobs), fake);
  const before = tbody.children.slice();
  eq(before.length, 3, "three rows built");
  const delBtn = before[0].children[5].children[0].children[5];
  eq(delBtn.dataset.action, "delete", "last action button is delete");

  for (let tick = 0; tick < 50; tick++) {
    jobs.forEach((j, i) => { j.downloaded_bytes = 100 + tick * 10 + i; j.rate_bps = 1000 + tick; });
    T.reconcileRows(tbody, models(jobs), fake);
  }
  const after = tbody.children;
  eq(after.length, 3, "still three rows after 50 ticks");
  for (let i = 0; i < 3; i++)
    ok(before[i] === after[i], `row ${i} is the SAME node after 50 ticks`);
  ok(before[0].children[5].children[0].children[5] === delBtn,
    "the delete button survives 50 ticks — a click across a tick still fires");
}

// --- 3. law #3: a tick writes only the cells that changed ------------------
{
  const tbody = node("tbody");
  const jobs = [job(1)];
  T.reconcileRows(tbody, models(jobs), fake);
  // Nothing changed at all: zero writes.
  resetCounts();
  T.reconcileRows(tbody, models(jobs), fake);
  eq(counts.text + counts.cls + counts.style + counts.prop + counts.attr + counts.data, 0,
    "an unchanged job costs zero DOM writes");
  eq(counts.create, 0, "an unchanged job creates no nodes");
  // Only progress moved: the fill width, the percent and the detail line.
  jobs[0].downloaded_bytes = 200;
  jobs[0].remaining_bytes = 800;
  resetCounts();
  T.reconcileRows(tbody, models(jobs), fake);
  eq(counts.create, 0, "progress change creates no nodes");
  eq(counts.style, 1, "one style write (the bar width)");
  // Two of six cells: the percent and the bold "downloaded" figure. The
  // rest of the detail line (size, rate, ETA) rendered identically, so it
  // is not written — that is law #3 doing its job.
  eq(counts.text, 2, "two text writes (percent, bold bytes)");
  ok(counts.text + counts.style + counts.cls + counts.prop + counts.attr + counts.data <= 5,
    `a 1 Hz progress tick writes a handful of cells, not the row (was ${JSON.stringify(counts)})`);
}

// --- 4. keyed reorder moves nodes, it does not rebuild them ---------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2), job(3)];
  T.reconcileRows(tbody, models(jobs), fake);
  const [n1, n2, n3] = tbody.children;
  resetCounts();
  T.reconcileRows(tbody, models([jobs[2], jobs[0], jobs[1]]), fake);
  eq(counts.create, 0, "a reorder creates nothing");
  ok(tbody.children[0] === n3 && tbody.children[1] === n1 && tbody.children[2] === n2,
    "reorder preserves every node, in the new order");
  eq(tbody.children[0].dataset.jobId, "3", "moved row keeps its identity");
}

// --- 5. add and remove ----------------------------------------------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2)];
  T.reconcileRows(tbody, models(jobs), fake);
  const keep = tbody.children[1];
  T.reconcileRows(tbody, models([jobs[1]]), fake);
  eq(tbody.children.length, 1, "removed job's row is gone");
  ok(tbody.children[0] === keep, "the surviving row is the same node");
  T.reconcileRows(tbody, models([job(9), jobs[1]]), fake);
  eq(tbody.children.length, 2, "new job inserted");
  eq(tbody.children[0].dataset.jobId, "9", "inserted at its model position");
  ok(tbody.children[1] === keep, "the existing row still is not rebuilt");
}

// --- 6. foreign boot markup is swept once, then never again ---------------
{
  const tbody = node("tbody");
  fake.append(tbody, node("tr")); // the page's "Loading the queue…" row
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(tbody.children.length, 1, "boot placeholder replaced");
  resetCounts();
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(counts.remove, 0, "the sweep does not run again");
}

// --- 7. the empty placeholder is just another key -------------------------
{
  const tbody = node("tbody");
  T.reconcileRows(tbody, [{ key: "__empty", kind: "empty", span: 6, text: "nothing here" }], fake);
  eq(tbody.children.length, 1, "placeholder row present");
  eq(tbody.children[0].children[0].textContent, "nothing here", "placeholder text set");
  eq(tbody.children[0].children[0].attrs.colspan, "6", "placeholder spans the table");
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(tbody.children.length, 1, "placeholder swapped for the real row");
  eq(tbody.children[0].dataset.jobId, "1", "…and it is the job row");
}

// --- 8. status semantics the queue leans on ------------------------------
{
  eq(T.statusName({ post: { stage: "unpack" } }), "extracting", "post stage reads as words");
  const f = T.rowModel(job(1, { status: "fetching", size_bytes: 0, downloaded_bytes: 0 }), { idx: 0, count: 1 });
  eq(f.detail, "fetching the NZB from the indexer…", "a URL job says what it is doing");
  eq(f.size, "—", "no size until the NZB lands");
  eq(f.pauseHidden, true, "nothing to pause while fetching");
  const doomed = T.rowModel(job(1, { health: 700 }), { idx: 0, count: 1, healthAbortArmed: true });
  eq(doomed.hNote, "unrepairable · aborting", "armed health-abort is stated on the row");
  const doomed2 = T.rowModel(job(1, { health: 700 }), { idx: 0, count: 1, healthAbortArmed: false });
  eq(doomed2.hNote, "unrepairable · will fail at end", "…and so is the un-armed case");
}

// --- 9. detail panel is a stable subtree ---------------------------------
{
  const tbody = node("tbody");
  const j = job(1);
  T.store.jobFiles = { job: 1, files: [
    { id: 1, filename: "a.rar", size_bytes: 10, done_segments: 1, total_segments: 2, failed_segments: 0, paused: false, is_par2: false, assembled: false },
    { id: 2, filename: "b.par2", size_bytes: 20, done_segments: 2, total_segments: 2, failed_segments: 0, paused: false, is_par2: true, assembled: true },
  ] };
  T.store.jobLogs = { job: 1, entries: [{ id: 5, kind: "INFO", time_unix: 1, text: "hello" }] };
  const withDetail = () => [T.rowModel(j, { idx: 0, count: 1 }), T.detailModel(j)];
  T.reconcileRows(tbody, withDetail(), fake);
  eq(tbody.children.length, 2, "detail row sits under its job row");
  const detail = tbody.children[1];
  const filesBody = detail.children[0].children[0].children[1].children[1];
  eq(filesBody.children.length, 2, "one row per file");
  const fileRow = filesBody.children[0];
  const logsBox = detail.children[0].children[0].children[2];
  eq(logsBox.children.length, 1, "one activity line");
  const logLine = logsBox.children[0];

  // A tick with more segments done must not rebuild the panel.
  T.store.jobFiles.files[0].done_segments = 2;
  T.store.jobLogs.entries.push({ id: 6, kind: "INFO", time_unix: 2, text: "world" });
  resetCounts();
  T.reconcileRows(tbody, withDetail(), fake);
  ok(tbody.children[1] === detail, "the detail row is the same node across a tick");
  ok(filesBody.children[0] === fileRow, "a file row is mutated, not rebuilt");
  ok(logsBox.children[0] === logLine, "the activity tail is appended to, not replaced");
  eq(logsBox.children.length, 2, "the new activity line was appended");
  eq(counts.remove, 0, "nothing is torn down — scroll position survives");
}

// --- 10. history rows -----------------------------------------------------
{
  const e = {
    job: 4, name: "old job", category: "tv", final_dir: "/dest/x", status: "SUCCESS",
    size: 2048, completed_at_unix: 1000, hidden: false, seen_count: 0,
    last_seen_at_unix: null, removed_at_unix: null, picked_up_by: null,
  };
  const m = T.histModel(e);
  eq(m.key, "h4", "history rows are keyed by job");
  eq(m.visAction, "h-hide", "a visible entry offers hide");
  eq(T.histModel(Object.assign({}, e, { hidden: true })).visAction, "h-restore",
    "a hidden entry offers restore");
  eq(T.histModel(Object.assign({}, e, { status: "DELETED" })).stCls, "st dim",
    "DELETED is already a styled history status");
  const tbody = node("tbody");
  T.reconcileRows(tbody, [m], fake);
  const acts = tbody.children[0].children[5].children[0];
  eq(acts.children.length, 4, "requeue / hide / forget / delete-files");
  eq(acts.children[3].dataset.action, "h-delete-files", "destructive action is data-driven");
  eq(acts.children[0].hidden, true, "requeue is hidden unless the entry is parked");
  T.reconcileRows(tbody, [T.histModel(Object.assign({}, e, { status: "DELETED", can_requeue: true }))], fake);
  eq(acts.children[0].hidden, false, "a parked entry can go back to the queue");
}

// --- 11. laws #1 and #4 as grep-able properties of the source -------------
// The live-rows renderer runs from the store declaration to the settings
// editor. Inside that span: no `innerHTML` (law #1) and no generated
// `onclick=` strings (law #4 — one delegated listener, `data-action` only).
// The settings/setup forms below it are template-built by design: they are
// re-rendered on user intent, never on a tick, and hold no live rows.
{
  const src = scriptMatch[1];
  const from = src.indexOf("// ---- the store ---");
  const to = src.indexOf("// ---- settings: a real form over nzbd.toml");
  ok(from > 0 && to > from, "renderer span located in the page source");
  const renderer = src.slice(from, to);
  const assigns = (renderer.match(/\.innerHTML\s*=/g) || []).length;
  eq(assigns, 0, "the live-rows renderer never assigns innerHTML");
  const onclicks = (renderer.match(/onclick\s*=\s*["'`]/g) || []).length;
  eq(onclicks, 0, "no generated onclick= strings in rendered rows");
  for (const container of ["queue-body", "history-body", "logbox", "badges", "clients-strip"])
    ok(!new RegExp(`\\$\\("${container}"\\)\\.innerHTML`).test(src),
      `#${container} is never rebuilt with innerHTML`);
}

// --- 12. toasts: stack, cap, dismiss, action ------------------------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  const t1 = T.toast({ text: "one" });
  eq(stack.children.length, 1, "a toast lands in the stack");
  eq(t1.el.children[0].textContent, "one", "…carrying its message");
  T.toast({ text: "two", kind: "error" });
  ok(stack.children[1].className.includes("bad"), "an error toast is styled as one");
  T.toast({ text: "three" });
  T.toast({ text: "four" });
  eq(stack.children.length, 3, "the stack is capped at three");
  eq(stack.children[0].children[0].textContent, "two", "the oldest is the one dropped");
  let ran = 0;
  const t5 = T.toast({ text: "undo me", action: { label: "Undo", fn: () => ran++ } });
  const undoBtn = t5.el.children[1];
  eq(undoBtn.textContent, "Undo", "the action button carries its label");
  undoBtn.onclick();
  eq(ran, 1, "clicking the action runs it");
  ok(!stack.children.includes(t5.el), "…and dismisses the toast");
  const t6 = T.toast({ text: "bye" });
  t6.el.children[1].onclick(); // the × button
  ok(!stack.children.includes(t6.el), "the dismiss button removes the toast");
  stack.children.length = 0;
}

// --- 13. pending overlay: apply hides the row, confirm drops the op -------
{
  const jobs = [job(1), job(2)];
  T.store.jobs = jobs;
  T.store.jobsLoaded = true;
  T.pending.clear();
  eq(T.queueModels().filter(m => m.kind === "job").length, 2, "both jobs visible");
  T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  const after = T.queueModels().filter(m => m.kind === "job");
  eq(after.length, 1, "the deleted row is gone before the server answers");
  eq(after[0].id, 2, "…and it is the right one");
  // The tick that still lists job 1 must NOT flash it back.
  T.pending.reconcile(jobs);
  eq(T.queueModels().filter(m => m.kind === "job").length, 1,
    "a tick mid-flight cannot flash the deleted row back");
  // Now the server agrees it is gone: the op retires.
  T.pending.reconcile([jobs[1]]);
  eq(T.pending.ops.size, 0, "server state matching the intent drops the op");
  T.store.jobs = [jobs[1]];
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "and the view agrees");
}

// --- 14. pending overlay: pause overrides the chip, then retires ----------
{
  const j = job(1, { status: "downloading" });
  T.store.jobs = [j];
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(1, "pause"), kind: "pause", jobId: 1, label: "Pausing job 1" });
  const m = T.queueModels().find(x => x.kind === "job");
  eq(m.st, "PAUSED", "the chip flips the instant the button is clicked");
  eq(m.pauseLabel, "resume", "…and the button offers the opposite action");
  eq(m.rowCls, "pending", "the row reads as in-flight");
  T.pending.reconcile([j]); // server still says downloading
  eq(T.pending.ops.size, 1, "an unconfirmed pause stays applied");
  T.pending.reconcile([job(1, { status: "paused" })]);
  eq(T.pending.ops.size, 0, "the server agreeing retires the op");
}

// --- 15. pending overlay: timeout reverts and says so --------------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  T.store.jobs = [job(1)];
  T.pending.clear();
  const op = T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  op.at = Date.now() - (T.PENDING_TTL_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 0, "an op nothing ever confirmed is dropped");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row springs back");
  eq(stack.children.length, 1, "…and the user is told");
  ok(stack.children[0].children[0].textContent.includes("didn't take"),
    "the message names the failure, not a generic error");
  // An op whose POST is still in flight is NOT swept out from under it…
  const op2 = T.pending.apply({ key: T.pending.key(1, "pause"), kind: "pause", jobId: 1 });
  op2.inflight = true;
  op2.at = Date.now() - (T.PENDING_TTL_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 1, "a POST still in flight is given its time");
  // …but it gets a longer leash, not an exemption. A daemon that accepts
  // the connection and then never answers must not leave a row invisible
  // until the page is reloaded.
  op2.at = Date.now() - (T.PENDING_HARD_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 0, "even an in-flight op is eventually reverted");
  T.pending.clear();
  stack.children.length = 0;
}

// --- 15b. logs are a live tail, not a poll ------------------------------
// Re-polling over a healthy stream would replace the ring every 5 s,
// discarding the lines the stream just delivered (and any skipped-line
// markers) and re-downloading 300 entries to do it.
{
  const logPolls = () => seen.filter(r => r.url.includes("/api/v1/logs")).length;
  routes.clear();
  seen.length = 0;
  routes.set("/api/v1/logs", { status: 200, body: { entries: [] } });
  T.setActiveTab("logs");

  // Nothing loaded yet: backfill, whatever the stream is doing.
  T.store.logs = null;
  T.connState("live");
  T.refreshTab();
  eq(logPolls(), 1, "an empty Logs tab backfills once");

  // Loaded and the stream is healthy: the tail carries it, no poll.
  seen.length = 0;
  T.store.logs = [{ id: 1, scope: "system", kind: "INFO", time_unix: 1, text: "live line" }];
  T.refreshTab();
  T.refreshTab();
  eq(logPolls(), 0, "a live stream is not second-guessed every 5 s");

  // Stream down: the poll is the only thing carrying the page, so it runs.
  T.connState("reconnecting");
  T.refreshTab();
  eq(logPolls(), 1, "…but a dropped stream falls back to polling");

  T.setActiveTab("queue");
  T.connState("live");
  routes.clear();
  seen.length = 0;
}

// --- 16. pending overlay: explicit revert on a failed POST ---------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  T.store.jobs = [job(1)];
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  T.pending.revert(T.pending.key(1, "delete"), "Deleting job 1 failed — job not found");
  eq(T.pending.ops.size, 0, "revert drops the op");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row is back immediately");
  eq(stack.children[0].children[0].textContent, "Deleting job 1 failed — job not found",
    "the toast carries the daemon's own error string");
  stack.children.length = 0;
}

// --- 17. move ops reorder locally, and retire on the server's order ------
{
  const jobs = [job(1), job(2), job(3)];
  T.store.jobs = jobs;
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(3, "move"), kind: "move", jobId: 3, wantIdx: 0, label: "Moving job 3" });
  const ids2 = T.queueModels().filter(m => m.kind === "job").map(m => m.id);
  eq(ids2.join(","), "3,1,2", "the row is where the user put it, immediately");
  T.pending.reconcile(jobs);
  eq(T.pending.ops.size, 1, "the old order does not satisfy the move");
  T.pending.reconcile([jobs[2], jobs[0], jobs[1]]);
  eq(T.pending.ops.size, 0, "the server's new order retires the move");
  T.pending.clear();
}

// --- 18. satisfied() is the whole resolution rule, in one place ----------
{
  const byId = new Map([[1, job(1, { status: "paused" })]]);
  ok(T.satisfied({ kind: "pause", jobId: 1 }, byId), "paused satisfies a pause");
  ok(!T.satisfied({ kind: "resume", jobId: 1 }, byId), "paused does not satisfy a resume");
  ok(!T.satisfied({ kind: "delete", jobId: 1 }, byId), "a present job does not satisfy a delete");
  ok(T.satisfied({ kind: "delete", jobId: 9 }, byId), "an absent job does");
  ok(T.satisfied({ kind: "pause", jobId: 9 }, byId),
    "a job that left the queue stops being overridden");
}

// --- 19. connection states, including the one the old UI could not see ---
{
  const conn = sandbox.document.getElementById("conn");
  const off = sandbox.document.getElementById("offline");
  T.connState("live");
  eq(T.conn(), "live", "live is live");
  eq(off.className, "", "no banner while connected");
  T.connState("reconnecting");
  ok(conn.className.includes("warn"), "reconnecting reads as a warning");
  eq(off.className, "", "…but still no page-wide banner: polls are carrying us");
  // One failed poll is a race with a restart; two is a dead daemon.
  T.pollResult(false);
  ok(T.conn() !== "unreachable", "a single miss is not a verdict");
  T.pollResult(false);
  eq(T.conn(), "unreachable", "two misses in a row is");
  eq(off.className, "show", "…and that gets a banner, not a gray dot");
  ok(off.textContent.includes("Can't reach the daemon"), "the banner says what is wrong");
  T.pollResult(true);
  ok(T.conn() !== "unreachable", "an answered poll clears it");
  eq(off.className, "", "banner gone");
}

// --- 19b. hb is liveness, not freshness ----------------------------------
// The daemon heartbeats a stream whose ticks it deduplicated. While the
// wire was moving, deduplicated ticks are impossible — missing ticks mean
// the ENGINE stopped publishing (wedged on slow storage). The old page fed
// hb into the same clock as ticks, told the user "● live updates", and
// never engaged the poll fallback: frozen page, green dot. That is the
// "dash goes long times without any update" field report (2026-07-26).
{
  const fullStatus = (over) => Object.assign({
    version: "0.1.0", up_since_unix: 1, download_rate_bps: 10 * 1048576,
    remaining_bytes: 1e9, session_downloaded_bytes: 5e7,
    download_paused: false, disk_low: false, quota_reached: false,
    blocked_servers: [], health_abort: false, speed_limit_bps: null,
    jobs_queued: 1, jobs_downloading: 1, jobs_finished: 0, servers: [],
  }, over || {});
  routes.clear(); seen.length = 0;
  routes.set("/api/v1/status", { status: 200, body: fullStatus() });
  routes.set("/api/v1/jobs", { status: 200, body: { jobs: [] } });
  T.store.status = fullStatus();
  const now = Date.now();

  // Ticks stale, hb fresh, wire active -> the engine is wedged: say so.
  T.connState("live");
  T.__setClocks(now - 20000, now - 1000);
  T.pollPass();
  eq(T.conn(), "stalled", "hb without ticks while downloading reads as stalled, not live");
  ok(seen.some(r => r.url.includes("/api/v1/status")),
    "…and the poll fallback engages instead of trusting the heartbeat");

  // A real tick is the one thing that clears it.
  T.applyTick({ status: fullStatus(), jobs: [] });
  eq(T.conn(), "live", "fresh data clears stalled");

  // Same gaps with an idle wire: a quiet stream is healthy, not stalled.
  seen.length = 0;
  T.store.status = fullStatus({ download_rate_bps: 0, jobs_downloading: 0 });
  T.__setClocks(now - 20000, now - 1000);
  T.pollPass();
  eq(T.conn(), "live", "an idle queue's deduplicated ticks stay live on hb alone");
  ok(!seen.some(r => r.url.includes("/api/v1/status")),
    "…without burning a status poll on a healthy idle stream");

  // hb stale too: the stream itself is gone.
  T.__setClocks(now - 20000, now - 20000);
  T.pollPass();
  eq(T.conn(), "reconnecting", "no ticks and no hb is a dead stream");

  // And hb may repair connection-level states, but never a stall.
  T.store.status = fullStatus();
  T.__setClocks(now - 20000, now);
  T.connState("reconnecting");
  T.applyHb();
  eq(T.conn(), "stalled", "hb upgrades reconnecting only as far as honesty allows");
  T.connState("stalled");
  T.applyHb();
  eq(T.conn(), "stalled", "hb alone never clears a stall — only a tick does");

  T.__setClocks(Date.now(), Date.now());
  T.connState("live");
  routes.clear(); seen.length = 0;
}

// --- 19c. a fatally-closed EventSource is rebuilt, not mourned -----------
// EventSource retries by itself only after clean network failures. Any
// non-200 answer (a 503 mid-restart, a proxy error page) closes it for
// good — the spec's "fail the connection" — and the old page would sit on
// "polling — reconnecting…" forever with a stream that was never coming
// back. The page owns that retry now.
{
  T.startStream();
  const s1 = T.__stream();
  ok(s1.es && s1.es.url.includes("/api/v1/events"), "stream points at the daemon");
  eq(s1.retryPending, false, "no rebuild pending while it is healthy");

  s1.es.readyState = 2; // the fatal close: non-200 answer, no built-in retry
  s1.es.onerror();
  const s2 = T.__stream();
  eq(s2.retryPending, true, "a fatal close schedules our own rebuild");
  eq(T.conn(), "reconnecting", "…and the page says what is happening");
  ok(s2.retryMs > 1000, "the retry backs off instead of hammering a restarting daemon");

  // The 5 s pass is the belt to that suspender: a CLOSED stream with no
  // rebuild pending (however it got that way) is given one.
  T.startStream();
  T.__stream().es.readyState = 2;
  T.__setClocks(Date.now(), Date.now()); // fresh clocks: pollPass must act on readyState alone
  T.pollPass();
  eq(T.__stream().retryPending, true, "ensureStream rebuilds a closed stream found by the timer");

  T.startStream(); // leave a healthy stream behind for later tests
  T.connState("live");
}

// --- 19c². the version identifies the build, from the footer --------------
// Field request 2026-07-26: a version pinned at the top that never changes
// identifies nothing. It lives in the footer now and renderStatus feeds it
// the daemon's build identity (version+hash, compile stamp in the tooltip).
{
  ok(html.indexOf("<footer>") >= 0 && html.indexOf("<footer>") < html.indexOf('id="ver"'),
    "the version element lives in the footer, not the header");
  const ver = sandbox.document.getElementById("ver");
  ver.textContent = ""; ver.__t = undefined; ver.title = "";
  T.store.status = {
    version: "0.1.0+gdeadbee42", built: "2026-07-26 21:14 UTC",
    download_paused: false, download_rate_bps: 0, remaining_bytes: 0,
    session_downloaded_bytes: 0, jobs_downloading: 0, jobs_queued: 0,
    blocked_servers: [], servers: [], health_abort: false,
    speed_limit_bps: null, disk_low: false, quota_reached: false,
  };
  T.renderStatus();
  eq(ver.textContent, "v0.1.0+gdeadbee42", "footer shows version + git hash");
  ok(ver.title.includes("built 2026-07-26 21:14 UTC"),
    "…and the tooltip carries the compile stamp");
}

// --- 19d. the sparkline survives on polls, sampled ------------------------
// The old page fed the sparkline only from SSE ticks: on a dead stream the
// rate number kept polling while the shape froze. Poll data feeds it now,
// through a ~1 Hz sampler so a nudge-burst cannot smear the time axis.
{
  T.spark.n = 0; T.spark.buf.fill(0);
  T.__setRateClock(0); // rewind the sampler (earlier tests just pushed)
  T.pushRateSampled(1000);
  T.pushRateSampled(2000); // within the sampling window: dropped
  eq(T.rateSeries().length, 1, "burst pushes collapse to one sample");
  eq(T.rateSeries()[0], 1000, "…keeping the first, not smearing the axis");
  T.spark.n = 0; T.spark.buf.fill(0);
}

// --- 20. no blocking dialogs anywhere in the page ------------------------
// The acceptance line for M4: a `confirm(` or `alert(` call, anywhere, is a
// regression. Comments are allowed to talk about them; code is not.
{
  const calls = [];
  scriptMatch[1].split("\n").forEach((line, i) => {
    const code = line.replace(/^\s*\/\/.*$/, "");
    if (/(^|[^.\w])(confirm|alert)\s*\(/.test(code)) calls.push(`line ${i + 1}: ${line.trim()}`);
  });
  eq(calls.length, 0, `no confirm()/alert() calls remain (${calls.join(" | ")})`);
}

// --- 21. "delete files" arms in place instead of opening a dialog --------
{
  const e = {
    job: 4, name: "old job", category: "tv", final_dir: "/dest/x", status: "SUCCESS",
    size: 2048, completed_at_unix: 1000, hidden: false, seen_count: 0,
    last_seen_at_unix: null, removed_at_unix: null, picked_up_by: null,
  };
  T.disarmButton();
  eq(T.histModel(e).delLabel, "delete files", "unarmed: the plain label");
  eq(T.histModel(e).delCls, "del", "…and the plain class");
  T.armButton(T.histKey(4));
  ok(T.armIsSet(T.histKey(4)), "the button is armed");
  const armedModel = T.histModel(e);
  eq(armedModel.delLabel, "sure?", "armed: the button says what the next click does");
  eq(armedModel.delCls, "del armed", "…and looks like it");
  ok(armedModel.delTip.includes("cannot be undone"),
    "the tooltip is explicit that this one is the irreversible action");
  // Arming lives outside the DOM, so it survives a reconcile — a `sure?`
  // written straight onto the node would be wiped by the next tick.
  const tbody = node("tbody");
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  const btn = tbody.children[0].children[5].children[0].children[3];
  eq(btn.textContent, "sure?", "armed state renders");
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  eq(btn.textContent, "sure?", "…and survives a re-render");
  // Arming a different button disarms the first: only one at a time.
  T.armButton(T.histKey(9));
  ok(!T.armIsSet(T.histKey(4)), "arming elsewhere disarms the previous button");
  T.disarmButton();
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  eq(btn.textContent, "delete files", "disarming restores the plain label");
}

// --- 23. rate ring: three minutes, wrapping correctly --------------------
{
  T.spark.n = 0;
  T.spark.buf.fill(0);
  eq(T.rateSeries().length, 0, "empty until something ticks");
  for (let i = 1; i <= 5; i++) T.pushRate(i * 100);
  eq(T.rateSeries().join(","), "100,200,300,400,500", "oldest first while filling");
  // Overflow: the ring must keep the NEWEST SPARK_N, in order.
  for (let i = 6; i <= 250; i++) T.pushRate(i * 100);
  const s = T.rateSeries();
  eq(s.length, T.SPARK_N, "capped at the window size");
  eq(s[0], (250 - T.SPARK_N + 1) * 100, "…starting at the oldest surviving sample");
  eq(s[s.length - 1], 25000, "…ending at the newest");
  ok(s.every((v, i) => i === 0 || v > s[i - 1]), "and in order across the wrap");
  T.spark.n = 0;
  T.spark.buf.fill(0);
}

// --- 24. title ticker ----------------------------------------------------
{
  eq(T.titleFor(null), "nzbd", "no status yet: a plain title");
  eq(T.titleFor({ download_paused: true, download_rate_bps: 0 }), "⏸ paused — nzbd",
    "paused says so");
  const busy = T.titleFor({ download_paused: false, download_rate_bps: 1048576, remaining_bytes: 10485760 });
  ok(busy.startsWith("▼ 1.0 MiB/s"), `rate leads the title (got ${busy})`);
  ok(busy.endsWith("— nzbd"), "…and the app name still ends it");
  ok(busy.includes("10s"), "…with the time left, which is the other half of the question");
  eq(T.titleFor({ download_paused: false, download_rate_bps: 0 }), "nzbd",
    "idle resets — no stale number left in the tab");
}

// --- 25. per-server chips ------------------------------------------------
{
  const s = {
    blocked_servers: [1],
    servers: [
      { server: 0, name: "eweka", rate_bps: 2048, day_bytes: 100, total_bytes: 900 },
      { server: 1, name: "blocknews", rate_bps: 0, day_bytes: 0, total_bytes: 5 },
      { server: 2, name: "idle-fill", rate_bps: 0, day_bytes: 0, total_bytes: 0 },
    ],
  };
  const chips = T.serverChipModels(s);
  eq(chips.length, 3, "one chip per configured server, including the quiet one");
  eq(chips[0].name, "eweka", "named, not numbered");
  eq(chips[0].rate, " 2.0 KiB/s", "carrying its share of the wire rate");
  ok(chips[0].cls.includes("live"), "a delivering server reads as live");
  eq(chips[1].rate, " blocked", "a blocked server says so instead of showing 0");
  ok(chips[1].cls.includes("on-bad"), "…and reads as a problem");
  ok(!chips[2].cls.includes("live"), "a quiet server is not marked live");
  ok(chips[0].tip.includes("add up to it"),
    "the tooltip states the same-measurement invariant these numbers rely on");
}

// --- 26. the log ring: bounded, and honest about what it missed ----------
{
  const rec = (id, scope, text) => ({ id, scope, kind: "INFO", time_unix: 1, text });
  T.store.logs = [];
  T.appendLogs([rec(1, "system", "boot"), rec(2, "job", "added")], 0);
  eq(T.store.logs.length, 2, "entries append");
  T.appendLogs([rec(3, "job", "finished")], 7);
  eq(T.store.logs.length, 4, "a skipped-lines marker is inserted with the batch");
  eq(T.store.logs[2].skipped, 7, "…carrying the count the server reported");
  // Filtering happens client-side; markers are never filtered away, because
  // "you are missing lines" is true regardless of which scopes you picked.
  ok(!T.logMatches(rec(9, "file", "x"), ["system", "job"]), "per-file lines filter out");
  ok(T.logMatches(rec(9, "file", "x"), ["system", "job", "file"]), "…and back in when ticked");
  ok(T.logMatches({ id: "s1", skipped: 3 }, []), "a skipped marker survives every filter");
  // Bounded: an all-night download must not grow the ring without limit.
  T.store.logs = [];
  for (let i = 0; i < T.LOG_RING_MAX + 250; i++) T.appendLogs([rec(i, "job", "line " + i)], 0);
  eq(T.store.logs.length, T.LOG_RING_MAX, "the ring is capped");
  eq(T.store.logs[T.store.logs.length - 1].text, "line " + (T.LOG_RING_MAX + 249),
    "…dropping the oldest, keeping the tail");
  // And it renders, marker and all.
  T.store.logs = [rec(1, "system", "hello"), { id: "s2", skipped: 4 }, rec(2, "system", "world")];
  for (const s of ["system", "job", "file"])
    sandbox.document.getElementById("lg-" + s).checked = true;
  const box = sandbox.document.getElementById("logbox");
  box.children.length = 0;
  delete box.__rows;
  T.renderLogs();
  eq(box.children.length, 3, "every line rendered");
  ok(box.children[1].textContent.includes("4 lines skipped"),
    "the gap is stated in the log itself, where it happened");
  T.store.logs = [];
}

// --- 27. paging ----------------------------------------------------------
// The tick carries the whole queue, so paging is a view concern. The thing
// that must not break: "move up" is a QUEUE operation, so a row's arrows
// have to reflect its global position, not its position on the page.
{
  const many = (n) => Array.from({ length: n }, (_, i) => job(i + 1));
  const bar = sandbox.document.getElementById("queue-pager");
  const info = sandbox.document.getElementById("pager-info");
  const rows = () => T.queueModels().filter(m => m.kind === "job");

  T.store.jobsLoaded = true;
  T.pending.clear();
  T.setPageSize(20);
  T.setPage(0);
  eq(T.PAGE_SIZE_DEFAULT, 20, "20 per page by default");
  ok(T.PAGE_SIZES.includes(0), "…and 'all' is one of the options");

  // 45 jobs, 20 to a page.
  T.store.jobs = many(45);
  let p = T.pageSlice(T.store.jobs);
  eq(p.pages, 3, "45 jobs at 20/page is three pages");
  eq(p.shown.length, 20, "the first page holds 20");
  eq(p.start, 0, "…starting at the top");
  eq(rows().length, 20, "and that is what renders");
  eq(rows()[0].id, 1, "first job on page 1");
  eq(rows()[0].upDisabled, true, "the very first row cannot move up");
  eq(bar.hidden, false, "the pager appears once it could do something");
  ok(info.textContent.includes("1–20 of 45"), `range is stated (got ${info.textContent})`);
  ok(info.textContent.includes("page 1/3"), "…and so is the position");

  // Page 2: the arrows must know these rows are NOT at the queue's edges.
  T.setPage(1);
  eq(rows()[0].id, 21, "page 2 starts at job 21");
  eq(rows()[0].upDisabled, false,
    "the first row of page 2 can still move up — paging is a view, not the queue");
  eq(rows()[19].downDisabled, false, "…and its last row can still move down");

  // Last page: partial, and the true last row is pinned.
  T.setPage(2);
  p = T.pageSlice(T.store.jobs);
  eq(p.shown.length, 5, "the last page holds the remainder");
  eq(rows()[4].id, 45, "…ending at the last job");
  eq(rows()[4].downDisabled, true, "the very last row cannot move down");
  ok(info.textContent.includes("41–45 of 45"), "the range follows");

  // The queue shrinking under you (jobs finishing) must not strand the view.
  T.store.jobs = many(5);
  eq(rows().length, 5, "a shrunk queue renders");
  eq(T.pageState().page, 0, "…and the page clamps instead of showing nothing");

  // "All" really is all.
  T.store.jobs = many(137);
  T.setPageSize(0);
  p = T.pageSlice(T.store.jobs);
  eq(p.pages, 1, "'all' is a single page");
  eq(rows().length, 137, "…holding every job");
  eq(rows()[136].downDisabled, true, "the last row is still the last row");

  // Changing the page size keeps you near where you were looking, rather
  // than dumping you back at the top.
  T.setPageSize(20);
  T.setPage(4); // rows 81–100
  T.setPageSize(50);
  eq(T.pageState().page, 1, "80 rows in => page 2 of 50 (rows 51–100), not page 1");
  T.setPageSize(20);
  T.setPage(0);

  // The detail panel belongs to its row, so it lives on that row's page.
  T.store.jobs = many(45);
  T.store.jobFiles = null;
  T.store.jobLogs = null;
  T.setOpenJob(25);
  eq(T.queueModels().some(m => m.kind === "detail"), false,
    "the open job's panel is not on a page that does not contain it");
  T.setPage(1);
  eq(T.queueModels().some(m => m.kind === "detail"), true,
    "…and comes back with it");
  T.setOpenJob(null);
  T.setPage(0);

  // A queue that fits gets no chrome at all.
  T.store.jobs = many(7);
  T.queueModels();
  eq(bar.hidden, true, "no pager for a queue that fits on one page");
  // …unless the reader chose a non-default size, which they need a way back from.
  T.setPageSize(0);
  T.queueModels();
  eq(bar.hidden, false, "the control stays reachable once it has been used");
  T.setPageSize(20);
  T.store.jobs = [];
  T.pending.clear();
}

// --- 22. the delete -> Undo state machine, end to end -------------------
// This is the whole point of M4: one click deletes, the toast offers Undo
// for as long as the server says the job is parked, and Undo requeues it.
(async () => {
  const stack = sandbox.document.getElementById("toasts");
  const reset = () => { stack.children.length = 0; routes.clear(); seen.length = 0; T.pending.clear(); };

  // (a) a parked delete offers Undo
  reset();
  T.store.jobs = [job(1, { name: "big movie" })];
  T.store.jobsLoaded = true;
  routes.set("/actions/delete", { status: 200, body: { ok: true, parked: true } });
  await T.deleteJob(1, "big movie");
  eq(seen.filter(r => r.url.includes("/jobs/1/actions/delete") && r.method === "POST").length, 1,
    "exactly one delete POST, fired without a dialog");
  eq(stack.children.length, 1, "the user is told the job is gone");
  const t = stack.children[0];
  ok(t.children[0].textContent.includes("big movie"), "…by name");
  eq(t.children[1].textContent, "Undo", "…with an Undo on offer");
  eq(T.UNDO_MS, 8000, "Undo stays available for 8 s");

  // (b) Undo requeues it
  routes.set("/actions/requeue", { status: 200, body: { id: 42 } });
  await t.children[1].onclick();
  eq(seen.filter(r => r.url.includes("/history/1/actions/requeue")).length, 1,
    "Undo goes through the requeue action");
  ok(stack.children.some(x => x.children[0].textContent.includes("back in the queue")),
    "…and says so when it worked");

  // (c) nothing parked -> no Undo is promised
  reset();
  T.store.jobs = [job(2, { name: "no undo" })];
  routes.set("/actions/delete", { status: 200, body: { ok: true, parked: false } });
  await T.deleteJob(2, "no undo");
  eq(stack.children.length, 1, "still reported");
  eq(stack.children[0].children.length, 2, "but with no action button — just the dismiss ×");
  ok(stack.children[0].children[0].textContent.includes("can't be undone"),
    "…and it says why");

  // (d) a failed delete reverts the row and quotes the daemon
  reset();
  T.store.jobs = [job(3, { name: "stubborn" })];
  routes.set("/actions/delete", { status: 503, body: { error: "engine is shutting down" } });
  await T.deleteJob(3, "stubborn");
  eq(T.pending.ops.size, 0, "the optimistic hide is rolled back");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row is visible again");
  ok(stack.children[0].children[0].textContent.includes("engine is shutting down"),
    "the toast carries the daemon's own words, not 'something went wrong'");
  ok(stack.children[0].className.includes("bad"), "…and reads as an error");

  // (e) a failed Undo says so instead of pretending
  reset();
  routes.set("/actions/requeue", { status: 404, body: { error: "no parked NZB for this entry" } });
  await T.requeueJob(7, "vanished");
  ok(stack.children[0].children[0].textContent.includes("no parked NZB"),
    "a requeue that cannot work says why");

  // --- 23. the first-run wizard takes the page OVER -----------------------
  // Field report 2026-07-26: the wizard rendered on top of a live-looking
  // dashboard — stat tiles, queue controls and tabs all still on screen,
  // all showing dashes, on a daemon that had no config at all. Two halves,
  // both pinned here.
  //
  // (a) the script hides every panel except the ones the wizard needs.
  const wrapKids = [
    wrapChild("header", null, ""),
    wrapChild("div", "cfgwarn", ""),
    wrapChild("section", "setup", ""),
    wrapChild("div", "offline", ""),
    wrapChild("div", null, "stats"),
    wrapChild("div", null, "controls"),
    wrapChild("nav", null, "tabs"),
    wrapChild("section", "tab-queue", ""),
    wrapChild("footer", null, ""),
  ];
  qsa.set(".wrap > *", wrapKids);
  T.hideAppForSetup();
  const shown = (el) => el.hidden === false;
  ok(shown(wrapKids[0]), "the header stays — the wizard is still nzbd");
  ok(shown(wrapKids[1]), "the config-durability warning stays: it explains the wizard");
  ok(shown(wrapKids[2]), "the wizard itself is revealed");
  ok(shown(wrapKids[8]), "the footer stays (it carries the build identity)");
  for (const el of [wrapKids[3], wrapKids[4], wrapKids[5], wrapKids[6], wrapKids[7]])
    ok(el.hidden === true, `${el.id || el.className} is hidden behind the wizard`);

  // (b) and `hidden` actually wins in the stylesheet. This is the half that
  // broke: .stats is a grid and .controls/nav.tabs are flexes, so the UA's
  // `[hidden] { display: none }` lost on specificity and those three — and
  // exactly those three — stayed on screen. A global !important override is
  // the only fix that also protects every element added later.
  const styleBlock = (html.match(/<style>([\s\S]*?)<\/style>/) || [])[1] || "";
  ok(/(^|[^.\w\[])\[hidden\]\s*\{[^}]*display:\s*none\s*!important/m.test(styleBlock),
    "the stylesheet carries a global [hidden] { display: none !important }");
  // Every rule that sets `display` on a class/tag is a candidate to shadow
  // it, so the override is not optional housekeeping — prove some exist.
  const displayRules = [...styleBlock.matchAll(/([^{}]+)\{[^}]*display:\s*(grid|flex|block|inline[\w-]*)/g)]
    .map((m) => m[1].trim()).filter((sel) => !sel.includes("[hidden]"));
  ok(displayRules.length > 0, "…and there are display: rules it has to outrank");

  // --- 24. the config-durability banner ----------------------------------
  // The root cause behind the wizard: a config directory that is writable
  // but ephemeral. Saves work, the operator believes the install is
  // configured, and the next container recreate serves this wizard again.
  const warn = sandbox.document.getElementById("cfgwarn");

  // (a) healthy mounts say nothing at all
  eq(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: true }), false,
    "a durable config directory raises no banner");
  ok(warn.hidden === true, "…and the banner stays out of the page");

  // (b) unknown (non-Linux, no /proc) is not an accusation
  eq(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: null }), false,
    "an unknown durability is not reported as a fault");

  // (c) ephemeral warns BEFORE anything is lost, and names the fix
  ok(T.showConfigDurability({
    config_path: "/etc/nzbd/nzbd.toml", durable: false,
    mirror_path: "/data/queue/nzbd.toml.saved",
  }), "an ephemeral config directory raises the banner");
  ok(warn.hidden === false, "…which is visible");
  ok(warn.innerHTML.includes("/etc/nzbd/nzbd.toml"), "…names the path at risk");
  ok(warn.innerHTML.includes("./config:/etc/nzbd"), "…shows the volume line that fixes it");
  ok(warn.innerHTML.includes("/data/queue/nzbd.toml.saved"), "…and where the durable copy lives");

  // (c2) in the WIZARD there is no config yet, so there is no state dir to
  // name — the banner must promise the behaviour instead of printing a
  // mirror path derived from a main_dir the operator has not chosen. Live
  // check 2026-07-26 against nuc3: setup mode reports writable=true,
  // container=true and no config at all, which is exactly this state.
  ok(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: false }),
    "the wizard still warns about an ephemeral config directory");
  ok(!/nzbd\.toml\.saved/.test(warn.innerHTML),
    "…without inventing a path for a config that does not exist yet");
  ok(warn.innerHTML.includes("as soon as you save"),
    "…promising the durable copy from the moment there is something to copy");

  // (d) after a recovery it reports what happened, not just what might
  ok(T.showConfigDurability({
    config_path: "/etc/nzbd/nzbd.toml", durable: false,
    recovered_from: "/data/queue/nzbd.toml.saved",
  }), "a recovered boot raises the banner");
  ok(warn.innerHTML.includes("recovered"), "…says the config was recovered");
  ok(warn.innerHTML.includes("/data/queue/nzbd.toml.saved"), "…from where");
  ok(warn.innerHTML.includes("./config:/etc/nzbd"), "…and still shows the mount to add");

  // --- 25. the masked TOML must not masquerade as a backup ---------------
  // Field report 2026-07-26: "it imported the config file but lost my
  // password". The settings editor shows every secret as ***unchanged***,
  // and its Download button handed that text back named `nzbd.toml` — the
  // exact filename you drop into /etc/nzbd. Restored later, the daemon
  // authenticated with the placeholder and the provider looked broken.
  const plain = "[api]\nbind = \"0.0.0.0:6789\"\n";
  const dPlain = T.advDownloadFile(plain);
  eq(dPlain.name, "nzbd.toml", "a config with no secrets downloads as the real thing");
  eq(dPlain.body, plain, "…unaltered");

  const maskedToml = "[[server]]\nhost = \"news\"\npassword = \"***unchanged***\"\n";
  const dMasked = T.advDownloadFile(maskedToml);
  eq(dMasked.name, "nzbd-masked.toml", "a masked config does NOT download as nzbd.toml");
  ok(dMasked.body.startsWith("# NOT A USABLE CONFIG"), "…and says so on line one");
  ok(dMasked.body.includes("refuses to start"), "…naming what happens if you use it anyway");
  ok(dMasked.body.includes(maskedToml), "…while still containing the config itself");

  if (failures.length) {
    console.error("UI DOM FAILURES:");
    for (const f of failures) console.error("  - " + f);
    process.exit(1);
  }
  console.log(`ui dom ok: ${checks} assertions`);
  process.exit(0);
})();
