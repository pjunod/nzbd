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
daily_quota_mb = 0           # 0 = unlimited (NZBGet DailyQuota)
monthly_quota_mb = 0         # NZBGet MonthlyQuota
quota_start_day = 1          # day of month the monthly quota resets
```

When a quota is exhausted the queue soft-holds (downloads pause, the API
stays up, the queue keeps accepting jobs); it releases automatically when
the day/month rolls over. Volume accounting is per server and survives
restarts (`servervolumes` in the compat API shows it).

## `[api]`

```toml
[api]
bind = "127.0.0.1:6789"     # use 0.0.0.0:6789 to serve the LAN
compat_version = "26.2"     # version string the NZBGet shim reports
username = "nzbd"           # HTTP Basic user (compat ControlUsername)
# password = "secret"       # setting a password ENABLES auth everywhere
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
health_action = "none"      # none | park | delete — what to do with
                            # failed-health downloads on disk
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
