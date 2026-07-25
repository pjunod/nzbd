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
const failures = [];
const ids = new Set([...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]));
function stubEl(id) {
  const t = {
    id, style: {}, dataset: {}, hidden: false, disabled: false,
    value: "", textContent: "", innerHTML: "", className: "", title: "",
    checked: false, scrollTop: 0, clientHeight: 0, scrollHeight: 0,
    classList: { toggle() {}, add() {}, remove() {} },
    querySelectorAll: () => [], querySelector: () => null,
    addEventListener() {}, appendChild() {}, replaceWith() {},
    insertBefore() {}, removeChild() {}, firstChild: null,
    select() {}, click() {}, focus() {}, matches: () => false,
    reportValidity: () => true,
    setAttribute() {}, removeAttribute() {}, getAttribute: () => null,
    closest: () => null,
  };
  return new Proxy(t, { get: (o, k) => (k in o ? o[k] : undefined), set: (o, k, v) => ((o[k] = v), true) });
}
const sandbox = {
  console,
  __nzbd_test_enable: true,
  document: {
    getElementById(id) {
      if (!ids.has(id)) failures.push(`$("${id}") — no element with that id in the markup`);
      return stubEl(id);
    },
    querySelectorAll: () => [],
    createElement: (t) => stubEl("_" + t),
    documentElement: stubEl("_root"),
    addEventListener() {},
  },
  navigator: { serviceWorker: { register: () => Promise.resolve() } },
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  location: { reload() {} },
  fetch: async () => ({ ok: false, status: 503, json: async () => ({}), text: async () => "" }),
  EventSource: class { constructor(u) { this.url = u; } addEventListener() {} },
  setInterval: () => 0, clearInterval() {},
  setTimeout: () => 0, clearTimeout() {},
  confirm: () => true, alert() {},
  URL: { createObjectURL: () => "blob:x", revokeObjectURL() {} },
  Blob: class {}, Date, Math, JSON, Promise, Number, String, Array, Object, Set, Map,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
process.on("unhandledRejection", () => {});

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
  eq(acts.children.length, 3, "hide / forget / delete-files");
  eq(acts.children[2].dataset.action, "h-delete-files", "destructive action is data-driven");
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

if (failures.length) {
  console.error("UI DOM FAILURES:");
  for (const f of failures) console.error("  - " + f);
  process.exit(1);
}
console.log(`ui dom ok: ${checks} assertions`);
process.exit(0);
