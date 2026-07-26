# Using nzbd

## The CLI

```sh
nzbd run [--config nzbd.toml] [--bind 0.0.0.0:6789]   # the daemon
nzbd add show.nzb [--url 127.0.0.1:6789] [--name N] [--category tv] [--priority 50]
nzbd status [--url 127.0.0.1:6789]                    # queue/rate/remaining as JSON
nzbd import-config nzbget.conf [--out nzbd.toml]      # migrate from NZBGet
```

**First-run setup:** if the `--config` path doesn't exist yet, the daemon
boots anyway and the web UI serves a setup form — paths, one news server,
optional UI password. Submitting writes the config file to that path and
restarts the daemon with it, no manual restart needed. (Everything the
wizard writes is ordinary `nzbd.toml`; edit it by hand afterwards.)

`add` and `status` are thin API clients — they talk to a running daemon,
local or remote. Logs go to stderr; set `RUST_LOG` for verbosity
(`RUST_LOG=debug nzbd run …`), and the same stream feeds the in-daemon
log ring visible in the UI and API.

Other ways to queue work: drop `.nzb` files into `paths.nzb_watch_dir`,
let a feed rule accept items ([CONFIGURATION.md](CONFIGURATION.md)
`[[feed]]`), POST to the native API, or let Sonarr/Radarr do it.

## The web UI

Open `http://<host>:6789/`. One embedded page (no separate frontend to
deploy): live queue with per-job and per-file actions, pause/resume,
speed limit control, history, log tail, settings, dark/light. It updates
over SSE at 1 Hz — progress bars, rates and the sparkline move without
you touching anything.

**Deleting is one click, and undoable.** There is no confirmation dialog
anywhere in this UI. Click *delete* and the row is gone immediately; a
toast in the corner offers **Undo** for 8 seconds. That works because the
daemon *parks* the job rather than dropping it: it regenerates the NZB
from queue state, spools it beside the history index, and writes a
`DELETED` history entry. Undo re-queues from that spool, so a misclick on
a 60 GiB download costs one more click instead of a re-download. The
parked entry stays in History with a **requeue** button long after the
toast has faded — until you forget it or delete its files.

Two things are *not* undoable, and behave differently on purpose:

- **delete files** in History removes the downloaded files from disk. It
  arms in place — the button becomes `sure?` for three seconds, and
  clicking anywhere else cancels it. Two clicks, both on the button you
  already aimed at.
- Deleting when no history store is configured. The toast says so rather
  than offering an Undo that would fail.

**How to read the header.** The connection indicator is the thing to
check first when the page looks stuck: `● live updates` means the event
stream is delivering; `◌ updates delayed` means the stream is fine but
the daemon itself has stopped publishing fresh data — its engine is busy
or its state disk is slow; the page shows the last data it sent and
clears the moment it catches up (the daemon logs `engine tick ran long`
with timings when this happens — that log line names the culprit);
`◌ polling — reconnecting…` means the stream dropped and 5-second polls
are carrying the page while it is rebuilt automatically; a red banner
across the top means the daemon is not answering at all and every number
below it is the last state seen. The rate tile's sparkline is the last three minutes,
one point per second — a provider that dies for ten seconds a minute is
invisible in the number and obvious in the shape. The chips beside the
badges are your news servers with their current share of the wire rate;
they add up to the header rate exactly — the tile IS their sum, the
same bytes counted per server — and one turns red when that server is
blocked after connection failures. The browser tab title tickers
`▼ 93 MiB/s · 12m — nzbd` while downloading, so you can watch it from
another tab.

**If an action fails, the page says so** — with the daemon's own error
text — and the row springs back to where it was. An action that gets no
answer within five seconds reverts the same way. Nothing is ever shown as
done merely because the click happened.

**Long queues are paged** — 20 rows at a time by default, with 50, 100
and *all* in the picker under the table; your choice is remembered in the
browser. The controls only appear once there is more than one page (or
once you have changed the setting, so you can get back). Paging is
display only: the move arrows still move a job through the whole queue,
so the first row of page 2 can move up into page 1, and the page index
follows along as jobs finish and the queue shrinks.

## On your phone (PWA)

The UI is a progressive web app: responsive on small screens, installable
to the home screen with its own icon, standalone (no browser chrome).
Browsers only grant the full install + offline shell to origins they
consider **secure**, which gives three tiers:

1. **Plain HTTP on the LAN** — works fine as a responsive site; on iOS,
   Safari's Share → *Add to Home Screen* still gives an icon and
   full-screen launch. No service worker, no Android install prompt.
2. **nzbd's built-in HTTPS** — set `[api] tls = true` (nothing else): a
   self-signed certificate is generated once, persisted under the state
   dir, and its sha256 fingerprint is printed at startup. Then trust that
   cert on the phone — *clicking through the browser warning is not
   enough for Chrome to enable service workers*: download `cert.pem` to
   the device and install it (Android: Settings → Security → More →
   Install certificates → CA certificate; iOS: open the file → install
   the profile → Settings → General → About → Certificate Trust Settings
   → enable). After that the origin is secure and install works.
   Custom certs: `tls_cert`/`tls_key`; extra hostnames/IPs for the
   generated one: `tls_sans`.
3. **A real certificate** — any TLS reverse proxy (Caddy, Traefik,
   Tailscale `tailscale serve`) in front of nzbd. Zero warnings, nothing
   to install on devices; the best option when you have a domain or a
   tailnet.

## Settings

The **Settings** tab edits the running configuration as a normal form:
paths, news servers (add/remove), speed & queue, web UI & API,
post-processing, and categories. Passwords stay stored — type a new one
only to change it. Saving applies what a running daemon can absorb
immediately (the speed limit today); anything else flags a **restart
required** banner listing the affected sections, with a *Restart nzbd*
button that bounces the daemon in place (downloads resume from the
journal). Feeds and cluster settings live in the collapsible raw-TOML
editor at the bottom, which edits the same file.

## Connecting Sonarr / Radarr / Lidarr

Add a download client of type **NZBGet** (not SABnzbd):

- Host: where nzbd runs · Port: `6789` · SSL: off (or your reverse proxy)
- Username/password: whatever `[api]` has (empty if auth is off)
- Category: e.g. `tv` — create a matching `[[category]]`

Everything the *arr apps use is implemented against NZBGet's real wire
behavior and locked with golden tests: `version`, `append` (v13+ and
legacy call forms), `listgroups`, `history`, `editqueue`
(`Group*`/`File*`/`History*` verbs), `status`, `config`, `rate`, pause
family, `listfiles`, `log`/`writelog`, `scan`, `servervolumes`,
`sysinfo`, `testserver`. Duplicate handling (dupe key/score/mode) and
per-job passwords (`*Unpack:Password`) behave like NZBGet. XML-RPC
(including `system.multicall`) is served on `/xmlrpc` for older tooling.

## The *arr handoff, demystified

There is no push: **Sonarr/Radarr poll nzbd's history** (every ~30 s)
for entries they queued, import the files themselves, and then delete
the history entry (NZBGet `HistoryDelete` — which *hides*, not erases).
If the *arr is down when a download finishes, nothing is lost — the
entry waits in history and gets picked up on the next poll after it
returns. Downloads only rot on disk when an import fails silently on
the *arr side, which is exactly the case nzbd now makes visible.

The **History** tab shows each stage of that pull:

- **connected clients** strip — every API consumer seen (User-Agent),
  whether it's actively polling, and when it last called. If your *arr
  isn't listed or shows "quiet", it isn't talking to nzbd at all.
- **⏳ awaiting pickup** — finished, but no client history poll has
  listed it yet (normal for ~30 s; suspicious after hours).
- **seen by <client> ×N** — the client's polls have returned this entry
  N times. It knows. If this state persists, the *arr saw the download
  but hasn't imported it — check its Activity queue for import errors.
- **✓ imported by <client>** — the client deleted the entry after
  import (shown dimmed, kept for the record).

Manual controls per entry: **restore** re-exposes a hidden entry so a
connected *arr re-imports it on its next poll (the fix for "imported
but the files went missing"); **hide** does the reverse; **forget**
drops the record and keeps files; **delete files** removes both.


## Native API

The compat shim is for NZBGet clients; automation you write yourself
should prefer the native JSON API (self-describing at
`/api/v1/openapi.json`):

```
GET  /api/v1/status                 queue totals, rate, health
GET  /api/v1/jobs                   the queue
POST /api/v1/jobs                   add a job (NZB content or URL)
GET  /api/v1/jobs/{id}
GET  /api/v1/jobs/{id}/files        per-file segment progress
GET  /api/v1/jobs/{id}/nzb          the job's NZB, regenerated from queue state
POST /api/v1/jobs/{id}/actions/{action}     pause|resume|delete|delete-files|move-*
POST /api/v1/queue/actions/{action}
PUT  /api/v1/queue/speed-limit
GET  /api/v1/history
POST /api/v1/history/{id}/actions/{action}  hide|restore|delete|delete-files|requeue
GET  /api/v1/events                 SSE stream of queue changes
GET  /api/v1/logs                   recent daemon log
GET  /metrics                       Prometheus metrics
GET  /healthz                       liveness (always unauthenticated)
```

With `[api] password` set, authenticate with HTTP Basic or
`Authorization: Bearer <token>`.

**Delete and requeue.** `actions/delete` answers
`{"ok":true,"parked":true|false}`. When `parked` is true the daemon has
spooled the job's regenerated NZB and written a `DELETED` history entry,
and `POST /api/v1/history/{id}/actions/requeue` will put it back —
`200 {"id":<new job id>}` on success, `404` if the entry or its spooled
NZB is gone, `501` with no history store configured. A successful requeue
consumes the entry and its spool: the job is queued again, so a `DELETED`
record for it would be a lie. Entries in `GET /api/v1/history` carry
`can_requeue`, which is derived at read time rather than stored — it
answers "is the requeue source still on *this* node?", and the spool is
local, not shared cluster state.

**The event stream** (`/api/v1/events`) carries every engine event under
its own `event:` name, plus three of its own: `tick` (`{status, jobs}` at
1 Hz, the whole read model from one snapshot, suppressed while nothing
changes); `hb` (`{now_unix}`, sent when a tick was suppressed and nothing
has gone out for 5 s — that is how a client tells an idle queue from a
dead stream, since `EventSource` cannot see keep-alive comments); and
`log` (`{entries, dropped}` at 1 Hz, capped at 200 lines per frame, with
`dropped` reporting what the cap cut). A new connection tails the log
from the newest id rather than replaying the ring — use `GET
/api/v1/logs` for backfill.

## RSS feeds and the filter language

Feeds poll on an interval, run each item through the filter, and queue
whatever is accepted (once — a persistent guid ledger dedupes across
restarts and cluster failovers). `fetchfeeds` forces a poll;
`viewfeed(id)` previews what a feed's filter would do — each item comes
back flagged ACCEPTED/REJECTED and NEW/BACKLOG.

The filter is a line-oriented subset of NZBGet's language:

```
# comments start with '#'
Require: expression        # every Require must pass, or the item is rejected
Accept(options): expression
Reject: expression         # first matching Accept/Reject decides
expression                 # bare line = Accept
# Short forms: Q: (require), A: (accept), R: (reject)
```

An expression is space-separated terms, ALL of which must match:

| Term | Meaning |
|---|---|
| `pattern` or `title:pattern` | wildcard match on the title (`*` any run, `?` one char, case-insensitive) |
| `category:pattern` / `url:pattern` | same matching on those fields |
| `size:>4GB` · `size:<900MB` · `size:500MB-2GB` | decoded-size window (K/M/G/T suffixes) |
| `age:>3d` · `age:<30d` | item age in days |
| `-term` | negates any term (`-*x265*`, `-category:foreign`) |

Accept options are carried onto the queued job: `category`, `priority`,
`pause` (yes/no), `dupekey`, `dupescore` — e.g.
`Accept(category:tv-hd, priority:100, dupescore:10): *2160p*`.

If no Accept rule exists at all, everything passing the Requires is
accepted (pure Reject-filtering works).

## Post-processing

The per-job pipeline and its knobs are described in
[CONFIGURATION.md](CONFIGURATION.md) `[post]`. Operational notes:

- **Verification is usually free.** nzbd records CRCs while downloading,
  so an intact par2 set is proven without re-reading data. Repair spawns
  `par2` only when something is actually damaged.
- **Deobfuscation is layered.** Evidence first: par2 16k-hashes recover
  real names even for fully-hex posts (including obfuscated `.par2` files
  found by magic bytes), and archive signatures fix mislabeled volumes.
  Then, post-unpack, a heuristic pass renames what evidence couldn't:
  a dominant file gets the job name (SABnzbd's rule, its heuristics
  ported); a fully-obfuscated season pack gets stable `<job> - NN`
  numbers. Names the par2 set vouches for are never overridden. The
  queue shows the `post_unpack_rename` stage while it runs, each rename
  is logged, and the applied list persists in history as
  `Deobfuscate:Count` / `Deobfuscate:Files` parameters.
- **Extension scripts** are NZBGet's: a directory of scripts (legacy
  header or v2 `manifest.json`), `NZBPP_*`/`NZBPR_*` environment,
  `[NZB] FINALDIR=…` and friends on stdout, exit codes 92–95. Point
  `post.scripts_dir` at your existing NZBGet scripts.
- **Health actions**: failed-health jobs can be left (`none`), parked, or
  deleted from disk (`delete`), mirroring NZBGet's HealthCheck.

## History, duplicates, quotas

Finished jobs retire from the queue into history (SQLite, with an
append-only JSONL mirror per node in cluster mode). Duplicate handling
follows NZBGet: dupe key/score/mode on jobs, checked against queue and
history on `append`, with `DELETED/DUPE` history records for rejects.
Daily/monthly quotas soft-hold the queue when exhausted and release on
rollover; `servervolumes` exposes per-server counters.

## Clustering, day to day

Point everything (the *arr apps, your browser, `nzbd add`) at **any**
node — every node serves the full API and transparently proxies to the
current leader. `GET /api/v1/cluster` shows nodes, roles, and the
leader. Feeds, the watch dir, and PP scheduling are leader-gated;
downloads and PP run wherever leases land (PP prefers nodes that aren't
downloading). Nothing needs draining for a rolling restart — leases
expire and are adopted. Deployment: [DEPLOY.md](DEPLOY.md); semantics
and failure matrix: [CLUSTERING.md](CLUSTERING.md).
