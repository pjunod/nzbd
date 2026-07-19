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
speed limit control, history, log tail, dark/light. It refreshes over
SSE, so state changes appear without polling.

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

## Native API

The compat shim is for NZBGet clients; automation you write yourself
should prefer the native JSON API (self-describing at
`/api/v1/openapi.json`):

```
GET  /api/v1/status                 queue totals, rate, health
GET  /api/v1/jobs                   the queue
POST /api/v1/jobs                   add a job (NZB content or URL)
GET  /api/v1/jobs/{id}
POST /api/v1/jobs/{id}/actions/{action}     pause|resume|delete|…
POST /api/v1/queue/actions/{action}
PUT  /api/v1/queue/speed-limit
GET  /api/v1/history
GET  /api/v1/events                 SSE stream of queue changes
GET  /api/v1/logs                   recent daemon log
GET  /metrics                       Prometheus metrics
GET  /healthz                       liveness (always unauthenticated)
```

With `[api] password` set, authenticate with HTTP Basic or
`Authorization: Bearer <token>`.

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
