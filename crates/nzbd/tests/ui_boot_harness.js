// UI boot harness: executes the embedded page script against a minimal
// DOM shim and fails on ANY uncaught error, unhandled rejection, missing
// element id, or a boot that never reaches the SSE connection. This is
// the net for "the script parses but dies at load" regressions (e.g. a
// refactor deleting a function another block still calls).
"use strict";
const fs = require("fs");
const vm = require("vm");

const htmlPath = process.argv[2];
const html = fs.readFileSync(htmlPath, "utf8");
const failures = [];

// Every id present in the markup; $() lookups outside this set are bugs.
const ids = new Set([...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]));
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
if (!scriptMatch) { console.error("no inline <script> found"); process.exit(2); }

function makeElement(id) {
  const target = {
    id, style: {}, dataset: {}, hidden: false, disabled: false,
    value: "", textContent: "", innerHTML: "", className: "", title: "",
    checked: false,
    classList: { toggle() {}, add() {}, remove() {} },
    querySelectorAll: () => [], querySelector: () => null,
    addEventListener() {}, appendChild() {}, replaceWith() {},
    select() {}, click() {}, focus() {}, matches: () => false,
    reportValidity: () => true,
    setAttribute() {}, removeAttribute() {}, getAttribute: () => null,
  };
  return new Proxy(target, {
    get(t, k) { return k in t ? t[k] : undefined; },
    set(t, k, v) { t[k] = v; return true; },
  });
}

const requestedIds = [];
const events = { sources: [] };
class EventSourceStub {
  constructor(url) { this.url = url; events.sources.push(this); }
  addEventListener() {}
  set onopen(f) { this._open = f; }
  set onerror(f) { this._err = f; }
  get onopen() { return this._open; }
  get onerror() { return this._err; }
}

const sandbox = {
  console,
  document: {
    getElementById(id) {
      requestedIds.push(id);
      if (!ids.has(id)) failures.push(`$("${id}") — no element with that id in the markup`);
      return makeElement(id);
    },
    querySelectorAll: () => [],
    createElement: () => makeElement("_created"),
    documentElement: makeElement("_root"),
    addEventListener() {},
  },
  navigator: { serviceWorker: { register: () => Promise.resolve() } },
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  location: { reload() {} },
  fetch: async () => ({ ok: false, status: 503, json: async () => ({}), text: async () => "" }),
  EventSource: EventSourceStub,
  setInterval: () => 0, clearInterval() {},
  setTimeout: (f) => 0, clearTimeout() {},
  confirm: () => false, alert() {},
  URL: { createObjectURL: () => "blob:x", revokeObjectURL() {} },
  Blob: class {}, Date, Math, JSON, Promise, Number, String, Array, Object,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;

process.on("unhandledRejection", (e) => failures.push(`unhandled rejection: ${e && e.message}`));

try {
  vm.createContext(sandbox);
  vm.runInContext(scriptMatch[1], sandbox, { filename: "index.html<script>" });
} catch (e) {
  failures.push(`script threw at load: ${e.message}`);
}

// Let the async boot IIFE settle (checkSetup fetch -> connectSse).
setImmediate(() => setImmediate(() => setImmediate(() => {
  if (events.sources.length === 0)
    failures.push("boot never constructed an EventSource — the SSE/refresh plumbing did not start");
  if (failures.length) {
    console.error("UI BOOT FAILURES:");
    for (const f of failures) console.error("  - " + f);
    process.exit(1);
  }
  console.log(`ui boot ok: ${requestedIds.length} id lookups, SSE started (${events.sources[0].url})`);
  process.exit(0);
})));
