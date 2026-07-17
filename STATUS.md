# nzbd — Project Status

The explicit ledger of what this project intends to do and whether it is
done. **Update this file in every feature commit.** Derived from the
roadmaps in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §16 and
[`docs/CLUSTERING.md`](docs/CLUSTERING.md) §13.

Legend: ✅ done (implemented, tested, committed) · 🔶 partial · ⬜ not
started · 👤 operator action (Paul)

**Snapshot (2026-07-17):** 116 tests · clippy clean · phases 0, 1, 2,
cluster C1+C2, *arr compat core, importer, auth/SSE/metrics complete ·
next up: **XML-RPC, golden tests, phase 4 web UI**

| Phase | State | Evidence |
|---|---|---|
| 0 — Scaffold | ✅ complete | `10eed82` |
| 1 — Core engine | ✅ complete | `2f45cd5` |
| C1 — Cluster foundation | ✅ complete | `e4178b2` (design), `0969a79` (impl) |
| CI & quality gates | ✅ complete (2 decisions open) | `b0b5530`, `0de429b` |
| 2 — Post-processing | ✅ core complete | `1fdad15` |
| C2 — PP leases + anti-affinity | ✅ complete | `9f402d8` |
| 3a — *arr compat core (append/history/editqueue) | ✅ complete | this commit |
| 3b — importer, XML-RPC, SSE, auth, metrics | ⬜ next | — |
| 4 — Web UI + ecosystem | ⬜ | — |
| 5 — Beyond parity (+ C3) | ⬜ | — |

---

## Phase 0 — Scaffold ✅

- ✅ Cargo workspace (13 crates), edition 2021, MSRV 1.85
- ✅ Domain model + NZBGet's exact health formulas (`nzbd-types`)
- ✅ Streaming NZB parser: entities, DOCTYPE, unordered/dup segments (`nzbd-nzb`)
- ✅ Incremental chunk-boundary-safe yEnc decoder + CRC32 + `crc32_combine` (`nzbd-yenc`)
- ✅ NNTP codec: responses, command injection guards, multiline reader (`nzbd-nntp`)
- ✅ Server failover ladder as a pure, scenario-tested function (`nzbd-engine`)

## Phase 1 — Core engine ✅

- ✅ Single-owner queue task: mpsc commands, arc-swap snapshots, broadcast events
- ✅ Scheduler wired to the ladder: tiers, groups, fill servers, per-server retention pre-fail, per-server retry reset, force priority, PropagationDelay
- ✅ Per-server connection pools, connect-on-demand, 5 s idle retirement
- ✅ NNTP pipelining (per-server depth), terminator-aware bounded yEnc consumption
- ✅ rustls transport: TLS with Strict/Minimal/None cert levels, AUTHINFO
- ✅ DirectWrite writers: sparse preallocate, positional writes, gap zero-fill, atomic rename, combined whole-file CRCs
- ✅ Delayed-par pausing (`*.volNNN+MM.par2` queued paused)
- ✅ Health-gated completion (Completed vs Failed below critical health)
- ✅ Token-bucket rate limiter (debt model) + 30×1 s speed meter
- ✅ Crash safety: append-only journal + atomic snapshots + unclean marker; kill -9 resume proven in e2e (no re-fetch of journaled segments)
- ✅ `nzbd-nserv` mock NNTP server: generated posts, 430/CRC/disconnect/latency injection, hit + concurrency gauges
- ✅ Native API subset: status, jobs add/list/detail, job + queue actions, speed limit
- ✅ Compat shim: `version`, `status`, `listgroups` in NZBGet's JSON-RPC 1.1 dialect with Lo/Hi/MB triplets
- ✅ CLI `run` / `add` / `status`; whole-daemon test (real binary, real CLI, SIGINT)
- ⬜ COMPRESS DEFLATE (RFC 8054)
- ⬜ RAM article cache (`ArticleCache`, default-off in NZBGet too)
- ⬜ Quotas + per-server volume counters
- ⬜ URL jobs (`AddUrl` / fetch)
- ⬜ Min-free-disk-space check
- ⬜ Filename deobfuscation beyond the quoted-subject heuristic; direct rename
- 👤 Real-provider smoke test (point `nzbd run` at an actual news server) — never yet done

## Cluster — C1 foundation ✅ (design ADR-13…16 accepted)

- ✅ Leader election on the shared volume: monotonic staleness observation, write–wait–verify, priority stagger (observing), epoch fencing via verify-before-commit snapshot guard
- ✅ Node registry (presence, capabilities, load; seq-progression liveness)
- ✅ Per-job fenced journals with union replay (`jobs/<id>/journal.<node>`) — overlap-safe reclaim without locks
- ✅ Work-lease protocol: poll/heartbeat/complete, TTL reclaim, **adoption** of running leases across leader failover
- ✅ Whole-job download distribution; engine worker mode (import/export, delegation, mirror overlay, crash-only demotion)
- ✅ Cluster-wide provider connection-budget partitioning (non-download nodes pinned to zero)
- ✅ Any-node API: full API + shim everywhere, transparent proxy to the leader
- ✅ `[cluster]` config + validation; single-node mode untouched
- ✅ 5 multi-node e2e tests: single-leader invariant, distributed download via proxied add with budget held, worker-death reclaim (zero re-fetch), leader-death failover with lease adoption, restart persistence
- ✅ C2: PP work leases — `LeaseKind::Post` in the poll/heartbeat/complete protocol; leader **anti-affinity scheduler** (idle PP nodes first, downloading nodes last, capacity-aware incl. in-flight backlog); fenced `.pp.<lease>/` staging with verify-lease-then-rename commit; superseded-staging GC; lease adoption across leader failover for PP too; dead-node delegation reconcile; download-only connection-budget divisor; per-node `history.<node>.jsonl` on the shared volume (cross-client O_APPEND is not trusted), union rebuild into each local SQLite index
- ✅ C2 e2e: leader downloads a real par2-set job, the idle non-download node quick-verifies it, stamps it, appends shared history, hands it back — bit-identical payload, zero staging residue
- ⬜ C3: segment-split downloads, weighted scheduling, budget rebalancing
- 👤 Real-Gluster soak checklist (CLUSTERING.md §11): quorum on, node reboots, volume heal mid-download

## CI & quality gates ✅

- ✅ Workflows: **Tests** (full suite + MSRV 1.85), **Lint** (fmt + clippy -D warnings), **Coverage** (cargo-llvm-cov → self-hosted badges → `badges` branch + lcov/HTML artifact)
- ✅ Git hooks (`.githooks/`): pre-commit fmt, pre-push clippy + tests — `git config core.hooksPath .githooks`
- ✅ rustfmt enforced workspace-wide; clippy zero warnings; MSRV verified
- ✅ First Coverage run on GitHub succeeded (it published the `badges` branch)
- ✅ 87 tests / 87.3% line coverage (local measurement matching CI methodology)
- 👤 Branch protection on `main` requiring Tests/Lint/Coverage (repo Settings)
- 👤 Badge rendering decision: badges don't render on a **private** repo README (GitHub proxies images anonymously). Either make the repo public (current setup then works as-is) or ask for the private-repo rework (CI commits relative-path SVGs to `main`)

## Phase 2 — Post-processing ✅ core (cluster-native completion lands with C2)

- ✅ par2 packet parser + **native quick-verify** from download CRCs — zero data re-read for intact sets (`nzbd-post/src/par2.rs`, proven against real `par2 create` output)
- ✅ par2 verify/repair subprocess wrapper (par2cmdline-compatible output parsing: Intact / Repairable / NeedMoreBlocks / Unrepairable)
- ✅ Delayed-par unpause: `UnpauseParBlocks` engine command, smallest covering set from `.volXX+NN` names; repair loop waits for the fetched blocks
- ✅ Unpack: unrar/7z subprocess, hardened (argv-only, scrubbed env, timeouts, 256 KiB output caps, kill-on-drop); NZBGet exit-code maps (unrar 11=password, 5=disk; 7z requires "Everything is Ok"); `.zip`/`.7z`/`.rar` multi-volume first-only/`.001`
- ✅ PP orchestrator: par verify → repair → unpack (⇆ forced-repair retry once) → cleanup → scripts; PostStrategy slots (sequential/balanced/aggressive/rocket); `*PP:done` stamp makes restarts idempotent; 30 s rescan covers leader takeover + lagged events
- ✅ NZBGet extension-script protocol: `NZBPP_*`/`NZBPR_*` env, `[LEVEL]` stdout log lines, `[NZB] KEY=value` commands (FINALDIR honored), exit codes 92–95, legacy header + v2 `manifest.json` discovery
- ✅ History: local SQLite index + authoritative append-only JSONL (shared volume in cluster mode per ADR-16; SQLite never on network FS; index rebuilt from JSONL on divergence)
- ✅ `[post]` config section; daemon wiring single-node **and** cluster (PP runs on the leader, gated live on election state)
- ✅ 6-test e2e suite against real binaries: intact fast path + script env/FINALDIR, corrupt→repaired bit-identical, unrepairable→PAR_FAILURE, unpack+cleanup, script-error→SCRIPT_FAILURE, event-driven manager + restart-skip
- ⬜ Direct unpack (`unrar -vp` volume feed during download)
- ⬜ par-rename / rar-rename (obfuscated-name recovery)
- ⬜ Per-job passwords + password-file retry loop
- ⬜ Dupe handling (key/score/mode)
- ⬜ Health-check actions on failure (park/delete per config)
- ⬜ Fixture suite extras: par2 damage matrices, multi-volume/passworded rar
- ✅ C2: PP work-lease type + anti-affinity scheduling (a job downloaded on node B post-processes on node C) — see the cluster section
- ⬜ C2 fixture extras: kill-mid-PP reclaim e2e (reclaim machinery itself is exercised by the download-lease tests)

## Phase 3 — Native API + compat 🔶 (3a done)

- ✅ Compat C1 — the Sonarr/Radarr certification surface: `append` (v13+ 9-arg form AND legacy 5-arg positional form; base64 or raw XML; AddPaused honored; returns NZBID or 0), `history` (full NZBGet field shape: `TOTAL/DETAIL` statuses, Lo/Hi/MB triplets, Parameters, FinalDir/DestDir, Par/Unpack/Script statuses, deprecated aliases), `editqueue` (3-arg v16+ AND 4-arg v13 forms: Group Pause/Resume/Delete/FinalDelete/SetPriority/SetCategory/SetParameter, HistoryDelete; GroupDelete records a `DELETED/MANUAL` history entry), `config`/`loadconfig` (option projection incl. `CategoryN.*`), `rate`, `pausedownload`/`resumedownload`
- ✅ Queue→history lifecycle (NZBGet parity): post-processed jobs retire out of the queue — immediately after local PP, via the leader sweep in cluster mode; health-failed jobs stamped + retired the same way
- ✅ Native `GET /api/v1/history` (limit param; cluster-aware via throttled JSONL union refresh)
- ✅ Post-stage queue status vocabulary in `listgroups` (VERIFYING_SOURCES / REPAIRING / UNPACKING / EXECUTING_SCRIPT / …)
- ✅ e2e: `sonarr_style_flow_over_jsonrpc` against the real daemon binary — version gate → config category check → base64 append → listgroups poll to empty → history shows SUCCESS/ALL + FinalDir → file imported bit-identical
- ✅ HTTP auth: Basic (NZBGet `ControlUsername`/`ControlPassword` parity, constant-time compare, `WWW-Authenticate` challenge) + Bearer token; enforced across native API and compat shim when configured; `/healthz` open; cluster peer endpoints keep their own shared-secret auth; importer maps `ControlUsername`/`ControlPassword` (with a warning on NZBGet's well-known default)
- ✅ `GET /api/v1/events` — engine events as SSE (job added/finished/deleted, file finished, segment exhausted, server blocked; lagged signal)
- ✅ `GET /metrics` — Prometheus text exposition (rate, remaining, session bytes, paused, speed limit, jobs by status)
- ⬜ Native REST completion: servers/config/logs endpoints, roles, OpenAPI
- ⬜ Compat C2: `listfiles`, `listgroups` file details, logs, scan, per-file editqueue actions
- ⬜ XML-RPC + `system.multicall` + JSON-P + GET-form safe methods; auth tiers
- ⬜ Golden structural tests vs recorded NZBGet 26.2 responses; nightly live *arr containers
- ✅ `nzbget.conf` importer: KEY=value + `ServerN.*`/`CategoryN.*` blocks, recursive `${Var}` expansion, NZBGet→nzbd vocabulary (Level→tier, Optional→fill, Encryption→tls), mapped/skipped/unknown/warnings report, hostless-server drop, zero-connection raise; `nzbd import-config <nzbget.conf> -o nzbd.toml` writes the converted file + prints the report; round-trips through the TOML parser
- ⬜ `rapidyenc-sys` FFI feature (vendored) + differential fuzzing vs scalar decoder

## Phase 4 — Web UI + ecosystem ⬜

- ⬜ Svelte SPA (queue/history/config/stats), SSE-driven, embedded via rust-embed
- ⬜ Extension manager UI; RSS feeds + filter language
- ⬜ Compat C3 (server volumes, sysinfo, testserver, …)
- ⬜ Packaging: static musl builds, Docker, Homebrew, Windows

## Phase 5 — Beyond parity ⬜

- ⬜ Native Rust par2 repair swap-in · io_uring file I/O · article-streaming/mount groundwork · per-provider adaptive pipelining · cluster C3

## Operator checklist 👤

- ✅ Push `main` (done — CI ran; `badges` branch is CI-owned, never push it: `git branch -D badges && git fetch --prune`)
- ⬜ Enable hooks on your clone: `git config core.hooksPath .githooks`
- ⬜ Branch protection for `main`
- ⬜ Repo visibility / badge-rendering decision (see CI section)
- ⬜ Real-provider download smoke test
- ⬜ Real-Gluster cluster soak (CLUSTERING.md §11)
