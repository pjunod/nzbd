# nzbd

A ground-up Rust reimplementation of the [NZBGet](https://nzbget.com) Usenet
downloader — modern architecture, same soul: tiny footprint, line-rate
throughput, direct-to-disk writing, and drop-in compatibility with the
Sonarr/Radarr ecosystem and NZBGet's post-processing script protocol.

> **Status: Phase 1 (core engine) complete.** The daemon downloads NZBs
> end-to-end: async queue owner, per-server connection pools with NNTP
> pipelining, rustls transport, the NZBGet server-failover ladder,
> DirectWrite disk assembly, crash-safe journal + resume, and a token-bucket
> rate limiter — all covered by an in-tree mock NNTP server (`nzbd-nserv`)
> including a kill-and-resume test. Design: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
> `nzbd` is a working title.

## What exists today

| Crate | State |
|---|---|
| `nzbd-types` | ✅ Domain model + NZBGet's exact health formulas, tested |
| `nzbd-yenc` | ✅ Incremental (chunk-boundary-safe) yEnc decoder with NNTP dot-unstuffing, terminator-aware bounded consumption (pipelining-safe), CRC32 + `crc32_combine`, tested. SIMD via a `rapidyenc-sys` FFI feature comes in phase 3 |
| `nzbd-nzb` | ✅ Streaming NZB parser (quick-xml), password/category meta, tested |
| `nzbd-nntp` | ✅ Codec + async transport: TCP/TLS (rustls; `Strict`/`Minimal`/`None` cert levels), AUTHINFO, pipelined commands, streamed bodies, tested. COMPRESS DEFLATE later |
| `nzbd-engine` | ✅ Phase 1 core: single-owner queue task (commands/arc-swap snapshots/broadcast events), priority scheduler + failover ladder (tiers/groups/fill/retention) in pull mode, per-server connection tasks with per-server pipelining depth, per-file DirectWrite writers (sparse preallocate, positional writes, gap zero-fill, atomic rename), delayed-par pausing, health gating, token-bucket rate limiter + 30×1 s speed meter, crash journal + snapshot + kill -9 resume |
| `nzbd-nserv` | ✅ Mock NNTP server: generated yEnc posts + NZBs, failure injection (430 / CRC corruption / mid-body disconnect / latency), per-article hit counting |
| `nzbd-state` | ✅ Append-only segment journal (torn-tail tolerant), atomic queue snapshots, unclean marker. SQLite history: phase 2 |
| `nzbd-post` | 🔶 `ParEngine`/`Extractor`/`ScriptRunner` trait boundaries (impls: phase 2) |
| `nzbd-config` | 🔶 TOML config model + path helpers; `nzbget.conf` importer stub (phase 3) |
| `nzbd-api` | 🔶 `/api/v1`: status, jobs (add/list/detail), job + queue actions, speed limit. SSE/auth/OpenAPI: phase 3 |
| `nzbd-compat` | 🔶 `/jsonrpc` shim speaking NZBGet's JSON-RPC 1.1 dialect: `version`, `status`, `listgroups` with live data and `*Lo/*Hi/*MB` triplets. Full C1 (`append`, `editqueue`, …): phase 3 |
| `nzbd` | ✅ Daemon: engine + API + shim; CLI `run` / `add` / `status`; graceful shutdown; whole-daemon integration test |

```sh
cargo test          # all green (unit + e2e incl. crash-resume)

# run against a real provider:
cat > nzbd.toml <<'EOF'
[paths]
main_dir = "~/downloads"
dest_dir = "~/downloads/complete"

[[server]]
name = "primary"
host = "news.example.com"
port = 563
tls = true
username = "user"
password = "pass"
connections = 20
pipeline_depth = 2
EOF
nzbd run --config nzbd.toml
nzbd add show.nzb            # queue an NZB via the API
nzbd status                  # queue/rate/remaining as JSON
curl localhost:6789/jsonrpc -d '{"method":"listgroups"}'   # NZBGet-dialect view
```

## Roadmap

Phase 2 post-processing (par2 verify/repair, unpack, script protocol, SQLite
history) → Phase 3 native API completion + full *arr-compatible shim
(`append`/`history`/`editqueue` + golden tests) + config importer +
`rapidyenc` FFI → Phase 4 web UI + extensions + feeds. Details and exit
criteria: `docs/ARCHITECTURE.md` §16.
