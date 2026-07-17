# nzbd

A ground-up Rust reimplementation of the [NZBGet](https://nzbget.com) Usenet
downloader — modern architecture, same soul: tiny footprint, line-rate
throughput, direct-to-disk writing, and drop-in compatibility with the
Sonarr/Radarr ecosystem and NZBGet's post-processing script protocol.

> **Status: Phase 0 (scaffold).** The design is complete
> (see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)); this tree contains the
> workspace skeleton with the first real components implemented and tested.
> `nzbd` is a working title.

## What exists today

| Crate | State |
|---|---|
| `nzbd-types` | ✅ Domain model + NZBGet's exact health formulas, tested |
| `nzbd-yenc` | ✅ Incremental (chunk-boundary-safe) yEnc decoder with NNTP dot-unstuffing, CRC32 + `crc32_combine`, tested. SIMD via a `rapidyenc-sys` FFI feature comes in phase 3 |
| `nzbd-nzb` | ✅ Streaming NZB parser (quick-xml), password/category meta, tested |
| `nzbd-nntp` | ✅ Response codec + command serialization + multiline dot-decoder, tested. Transport lands in phase 1 |
| `nzbd-engine` | 🔶 The server-failover ladder (tiers/groups/fill/retries) as a pure, scenario-tested function; queue owner task is phase 1 |
| `nzbd-post` | 🔶 `ParEngine`/`Extractor`/`ScriptRunner` trait boundaries (impls: phase 2) |
| `nzbd-state` | 🔶 Persistence traits (journal + SQLite history: phases 1–2) |
| `nzbd-config` | 🔶 TOML config model; `nzbget.conf` importer stub (phase 3) |
| `nzbd-api` | 🔶 axum `/api/v1/status` + `/healthz` stubs |
| `nzbd-compat` | 🔶 `/jsonrpc` shim skeleton speaking NZBGet's JSON-RPC 1.1 dialect (`version`, `status`) |
| `nzbd` | 🔶 Daemon binary: boots, serves the API on `:6789` |

```sh
cargo test          # all green
cargo run -p nzbd -- run   # then: curl localhost:6789/jsonrpc -d '{"method":"version"}'
```

## Roadmap

Phase 1 core download engine → Phase 2 post-processing (par2/unpack/scripts) →
Phase 3 native API + full *arr-compatible shim + config importer → Phase 4 web
UI + extensions + feeds. Details and exit criteria: `docs/ARCHITECTURE.md` §16.
