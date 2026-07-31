# Configuration reference — `nzbd.toml`

nzbd reads one TOML file (`nzbd run --config nzbd.toml`). Every section
and key is optional — omitted keys take the defaults shown here, and a
bare `nzbd run` with no file at all works with the defaults. Unknown keys
are rejected at startup (typos fail loudly rather than silently doing
nothing). Paths accept a leading `~`.

Converting from NZBGet? `nzbd import-config nzbget.conf --out nzbd.toml`
maps an existing configuration onto this format and reports what mapped,
what was recognized but not applicable, and what needs review by hand.

## `[paths]`

```toml
[paths]
main_dir = "~/downloads"            # working root; state lives under it
dest_dir = "~/downloads/complete"   # finished downloads (per-category overrides below)
# inter_dir = "~/downloads/inter"   # optional intermediate/download area
# nzb_watch_dir = "~/downloads/nzb" # drop .nzb files here to auto-queue them
# queue_dir = "~/downloads/queue"   # journal + queue snapshots (default: <main_dir>/queue)
# temp_dir = "/tmp/nzbd"            # scratch space
```

The watch dir is polled by the daemon; a dropped `.nzb` is queued and the
file removed. In cluster mode only the current leader watches it.

## `[[server]]` — one block per news server

```toml
[[server]]
name = "primary"          # unique label; same name on several nodes of a
                          # cluster means "one shared account" (budget is split)
host = "news.example.com"
port = 563                # default 563
tls = true                # default true
username = "user"
password = "pass"
active = true
tier = 0                  # failover ladder level: 0 = main, 1+ = backups
group = 0                 # servers in the same group never run concurrently
fill = false              # true = fill server (tried only for missing articles)
connections = 8           # concurrent NNTP connections
pipeline_depth = 2        # commands in flight per connection (adaptive AIMD
                          # raises/lowers the effective depth at runtime)
retention_days = 0        # 0 = unlimited; skips articles older than this
cert_verification = "strict"   # strict | minimal | none
```

Tiers implement NZBGet's ladder: every article is tried on tier 0 first,
then tier 1, and so on. `fill` servers are consulted only after the
regular servers of their tier miss an article.

## `[[category]]`

```toml
[[category]]
name = "tv"
dest_dir = "/data/complete/tv"   # optional override of paths.dest_dir
unpack = true                    # optional per-category unpack override
extensions = []                  # extension scripts to run for this category
```

`name` is matched against the job's category case-insensitively, so an
*arr sending `TV` lands on `name = "tv"`.

`dest_dir` is a **move at the end of post-processing**, not a different
download target: the engine always writes under `paths.dest_dir`, and the
finished folder is relocated to `<category dest_dir>/<job name>` before
extension scripts run and before any path is reported. Cross-filesystem
destinations work (the move falls back to copy-then-remove), which is the
usual homelab shape — download on the SSD, library on the NAS. If the
move fails, the failure is logged loudly and every reported path names
where the files actually are, not where they were meant to go.

`unpack` overrides `[post] unpack` for this category only. `extensions`
names the post-processing scripts this category runs, by file name or
stem (`"Clean.py"` and `"clean"` both select `Clean.py`); an empty list —
the default — runs every discovered script, which is the global behavior.

> **Behavior change (integration phase 1).** These three keys were parsed
> and advertised to compat clients as `CategoryN.DestDir` / `.Unpack` /
> `.Extensions` for a long time while post-processing ignored all of
> them. A config that set `dest_dir` "expecting nothing to happen" will
> now see files move there. This was fixed rather than documented as a
> quirk because an *arr path-maps off the advertised value: advertised
> paths that are not actual paths are a silent import failure with
> nothing in any log to explain it.

## `[queue]`

```toml
[queue]
article_retries = 3          # per-article retry attempts
retry_interval_secs = 10
article_timeout_secs = 60
article_cache_mb = 0         # reserved; DirectWrite keeps this at 0
direct_write = true          # positional writes straight into sparse files
crc_check = true             # verify per-article CRC32 while downloading
continue_partial = true      # resume partially-downloaded files on restart
propagation_delay_mins = 0   # ignore posts younger than this
min_free_disk_mb = 250       # pause grabbing new work below this free space
# speed_limit_kib = 10240    # global rate cap (KiB/s); absent = unlimited
max_active_downloads = 1     # how many jobs download at once (1..=100)
daily_quota_mb = 0           # 0 = unlimited (NZBGet DailyQuota)
monthly_quota_mb = 0         # NZBGet MonthlyQuota
quota_start_day = 1          # day of month the monthly quota resets
```

`max_active_downloads` decides how many jobs are worked on at the same
time. At `1` — the default, and what nzbd has always done — the top job
takes every connection until it has no segments left to hand out. Raising
it splits the connection pool evenly between that many jobs; priority
still decides *which* jobs are in the set, this decides how many.

It does not make anything faster. The same connections move the same
bytes either way; they simply arrive spread across several jobs instead
of completing one at a time, so the first job finishes later and the last
finishes at about the same moment. Raise it when you want several things
moving at once — a small job not stuck behind a 60 GB remux — not when
you want more throughput.

Both this and the speed limit can be changed while nzbd runs, from the
box on the Queue page or from Settings; the value in the file is the
starting position after a restart.

Per-server `connections` also applies without a restart when you *lower*
it. Raising it above the number nzbd started with needs a restart, since
the sockets are opened at boot — the settings page says so when it
happens rather than pretending the new number is in force.

When a quota is exhausted the queue soft-holds (downloads pause, the API
stays up, the queue keeps accepting jobs); it releases automatically when
the day/month rolls over. Volume accounting is per server and survives
restarts (`servervolumes` in the compat API shows it).

## `[api]`

```toml
[api]
bind = "127.0.0.1:6789"     # use 0.0.0.0:6789 to serve the LAN
tls = false                 # true = serve HTTPS (NZBGet SecureControl).
                            # With no cert configured, a self-signed cert is
                            # generated once under the state dir and reused;
                            # the startup log prints its sha256 fingerprint.
# tls_cert = "/etc/nzbd/cert.pem"   # bring your own PEM chain + key instead
# tls_key  = "/etc/nzbd/key.pem"    # (NZBGet SecureCert / SecureKey)
# tls_sans = ["nas.lan", "192.168.1.10"]  # extra names for the generated cert
compat_version = "26.2"     # version string the NZBGet shim reports
username = "nzbd"           # HTTP Basic user (compat ControlUsername)
# password = "secret"       # setting a password ENABLES auth everywhere
#
# Secrets and the settings editor: the web UI never shows a real password —
# it displays `***unchanged***` and swaps the real value back in when you
# save. That means the TOML you see (or Download) in the Settings tab is a
# DISPLAY, NOT A BACKUP. A file restored from that copy would carry the
# placeholder as your password; nzbd refuses to start on such a config and
# tells you which field to fix, and the Download button names the masked
# copy `nzbd-masked.toml` so it cannot be mistaken for the real file. For a
# real backup, copy `nzbd.toml` off the config volume itself.
# token = "long-random"     # optional Bearer token alternative
allow_legacy_default_credentials = false   # opt-in nzbget/tegbzn migration aid
```

With no password set the API is open (bind to localhost!). With one set,
every endpoint except `/healthz` requires HTTP Basic (or the Bearer
token). The *arr apps pass username/password in their NZBGet client
settings unchanged.

## `[post]` — post-processing

```toml
[post]
enabled = true
par2_cmd = "par2"           # external tools; names or absolute paths
unrar_cmd = "unrar"
sevenzip_cmd = "7z"
# scripts_dir = "~/nzbd-scripts"   # NZBGet extension scripts live here
unpack = true
cleanup = true              # delete archives/par2/sfv after successful unpack
deobfuscate_final = true    # rename still-obfuscated files to the job name
                            # (season packs get "<job> - NN"); par2-proven
                            # names are never touched
strategy = "balanced"       # sequential | balanced | aggressive | rocket
                            # (1 / 2 / 3 / 6 concurrent PP jobs)
failure_action = "delete"   # none | park | delete — what happens to the
                            # FILES of a job that ended in a terminal
                            # failure: par failure, unpack failure, script
                            # failure, health abort, post crash. Deleting
                            # loses nothing: the job's NZB is parked with
                            # its history row, so requeue re-downloads it.
                            # "none" leaves ~90 GB per failed grab in the
                            # tree your importer watches — that is how a
                            # terabyte of duplicates happened. Anything but
                            # "none" also aborts a download the moment its
                            # health drops below critical health (the point
                            # where even all par2 blocks can't repair it),
                            # instead of finishing a doomed download.
                            # Accepts the old name `health_action`, which
                            # only ever governed the health gate
failed_dir = "/data/usenet/failed"   # where "park" puts them; defaults to
                            # <main_dir>/failed — deliberately off the
                            # category tree
tool_timeout_secs = 3600
script_timeout_secs = 3600
par_fetch_timeout_secs = 600   # wait for delayed par files during repair
```

The PP pipeline per job: par-rename → rar-rename → par verify (native
quick-verify from download CRCs; repair only on damage) → unpack (with a
repair-and-retry loop for archives that fail) → cleanup → deobfuscate →
extension scripts. Scripts get NZBGet's exact `NZBPP_*` environment and
`[NZB] KEY=value` command channel; exit codes 92–95 mean what they mean
in NZBGet.

## `[history]` — how much finished-job history to keep

```toml
[history]
keep_max = 1000    # keep at most this many entries (0 = unlimited)
keep_days = 90     # drop entries finished longer ago than this (0 = forever)
```

Both bounds apply and whichever is reached first wins. They answer
different questions, which is why neither one alone is enough: `keep_max`
answers *how big may this get* and holds when a week's backlog lands in a
day; `keep_days` answers *how far back do I care* and holds when the
daemon sits quiet for months.

Trimming is not tidiness. The authoritative history log
(`state/history/history.jsonl`) is re-read end to end on every history
read, so the **log's length** — not the number of rows you asked for — is
what a history page costs. On a network state volume that showed up as
179 entries taking 3.1 s (nuc3, 2026-07-29); paging the UI would not have
fixed it, because the expensive part happened before the page was chosen.
A trim deletes the index rows, compacts the log to the survivors, and
raises a watermark so a later rebuild will not re-import what went.

Trimming runs at startup and on a 60-second throttle as jobs finish, so
lowering a bound takes effect when you restart, not when the next job
happens to complete. Nothing else changes: an entry's parked NZB is
reaped with it, exactly as `forget` already does.

Importing an `nzbget.conf` maps `KeepHistory` onto `keep_days` — the units
already agree, so your existing retention window comes across rather than
being replaced by nzbd's default.

## `[[feed]]` — RSS/Atom indexer feeds

```toml
[[feed]]
name = "indexer-tv"
url = "https://indexer.example/rss?apikey=…&t=5000"
interval_mins = 15
category = "tv"       # default category for accepted items
priority = 0
pause = false         # queue items paused
filter = """
# NZBGet-style filter: first matching Accept/Reject wins;
# Require lines must ALL pass first. See USAGE.md for the language.
Require: size:>200MB -age:>30d
Accept(category:tv-hd, priority:50): *1080p* -*x265*
Reject: *cam* *telesync*
Accept: *
"""
```

Feed state (a guid ledger with 90-day retention) prevents re-downloading
items across restarts — and across failovers in cluster mode, where only
the leader polls. `fetchfeeds`/`viewfeed` in the compat API trigger and
preview feeds on demand.

## `[cluster]` — multi-node mode

Off by default; a single-node daemon needs none of this. Full semantics:
[CLUSTERING.md](CLUSTERING.md). Deployment recipes: [DEPLOY.md](DEPLOY.md).

```toml
[cluster]
enabled = true
node_name = "node-a"                      # unique + stable per node
shared_dir = "/mnt/work"                  # the shared POSIX volume (all nodes)
advertise_url = "http://10.0.0.11:6789"   # how PEERS reach this node
secret_file = "/etc/nzbd/cluster.secret"  # same secret on every node
# secret = "inline-secret"                # alternative to secret_file
coordinator = true          # eligible for leader election
priority = 10               # lower = preferred leader
download = true             # takes download-job leases
max_download_jobs = 2
post_process = true         # PP executor (anti-affinity prefers idle nodes)
pp_slots = 1
lease_interval_secs = 5     # heartbeat cadence
takeover_after_secs = 20    # leader considered dead after this silence
worker_ttl_secs = 30        # work lease expiry (another node then adopts)
```

## Complete minimal example

```toml
[paths]
main_dir = "/data"
dest_dir = "/data/complete"

[[server]]
name = "primary"
host = "news.example.com"
port = 563
tls = true
username = "user"
password = "pass"
connections = 20

[[category]]
name = "tv"

[[category]]
name = "movies"

[api]
bind = "0.0.0.0:6789"
password = "change-me"
```
