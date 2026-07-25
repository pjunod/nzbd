# nzbd — Project Status

The explicit ledger of what this project intends to do and whether it is
done. **Update this file in every feature commit.** Derived from the
roadmaps in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §16 and
[`docs/CLUSTERING.md`](docs/CLUSTERING.md) §13.

Legend: ✅ done (implemented, tested, committed) · 🔶 partial · ⬜ not
started · 👤 operator action (Paul)

**Snapshot (2026-07-25):** 212 tests · clippy clean · **phases 0–4
complete incl. RSS feeds, cluster C1+C2 complete, phase 5 partial** —
every NZBGet user-facing surface exists; what remains is beyond-parity
performance work and operator actions

| Phase | State | Evidence |
|---|---|---|
| 0 — Scaffold | ✅ complete | `10eed82` |
| 1 — Core engine | ✅ complete | `2f45cd5` |
| C1 — Cluster foundation | ✅ complete | `e4178b2` (design), `0969a79` (impl) |
| CI & quality gates | ✅ complete (2 decisions open) | `b0b5530`, `0de429b` |
| 2 — Post-processing | ✅ complete | `1fdad15` + this commit |
| C2 — PP leases + anti-affinity | ✅ complete | `9f402d8` |
| 3a — *arr compat core | ✅ complete | `3793ad8` |
| 3b — importer · auth · SSE · metrics | ✅ complete | `e00990c`, `b4c422d` |
| 3c — compat C2 + XML-RPC + golden tests | ✅ complete | `fe6d2be` |
| 4 — Web UI + ecosystem | ✅ complete | `77b7660` |
| 5 — Beyond parity (+ C3) | 🔶 adaptive pipelining done; rest scoped | this commit |

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
- ✅ Diagnosable state I/O (2026-07-24): every filesystem call in `nzbd-state` goes through `fsx` wrappers that attach the **operation and path** to the error (`StateError::Io { op, path, source }`), so a startup failure reads `state: write /data/queue/unclean.local: Permission denied (os error 13)` instead of a bare errno. Startup logs the resolved `state_dir`/`dest_dir` before touching them, a permission failure appends an actionable hint (walks the source chain, so it survives the cluster wrapper), and `anyhow_lite::Error` prints via `Display` so multi-line hints aren't Debug-escaped. Guard test fails the build if a raw `std::fs` call is added back to the crate
- ✅ `nzbd-nserv` mock NNTP server: generated posts, 430/CRC/disconnect/latency injection, hit + concurrency gauges
- ✅ Native API subset: status, jobs add/list/detail, job + queue actions, speed limit
- ✅ Compat shim: `version`, `status`, `listgroups` in NZBGet's JSON-RPC 1.1 dialect with Lo/Hi/MB triplets
- ✅ CLI `run` / `add` / `status`; whole-daemon test (real binary, real CLI, SIGINT)
- ✅ URL jobs: `AddUrl` via API/append — registered instantly (`Fetching`), NZB fetched over HTTPS (hyper on the NNTP rustls stack, redirects, 64 MiB cap), then queued; fetch failure → `FAILURE/FETCH` in history
- ✅ Min-free-disk-space guard: statvfs on the dest volume every 10 s; below the floor ALL leasing stops (even force jobs), auto-resumes
- ✅ Quotas + per-server volume counters: daily/monthly windows (`QuotaStartDay` civil-date periods), per-node `volumes.<node>.json` summed cluster-wide, force-priority bypasses quota, `QuotaReached` live in compat status, counters in snapshots
- ✅ Filename deobfuscation: par-rename (16k-MD5 match incl. content-detected par2s) + rar-rename (RAR4/RAR5/7z/zip signatures, RAR5 volume numbers) — see Phase 2
- ⬜ COMPRESS DEFLATE (RFC 8054) — deferred: single-digit % savings on yEnc bodies; scoped for a later pass
- ✖ RAM article cache — intentionally not applicable: `ArticleCache` exists in NZBGet to reduce fragmentation when DirectWrite is off; nzbd's DirectWrite positional writer is always on, so there is nothing for a cache to fix
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
- ✅ External-tool tests (par2/7z fixtures) self-skip with a notice on machines without the binaries, so the pre-push hook passes on a stock Mac; CI installs the tools and sets `NZBD_REQUIRE_TOOLS=1` so a skip there is a hard failure — `brew install par2 p7zip` for full local coverage
- ✅ First Coverage run on GitHub succeeded (it published the `badges` branch)
- ✅ 87 tests / 87.3% line coverage (local measurement matching CI methodology)
- 👤 Branch protection on `main` requiring Tests/Lint/Coverage (repo Settings)
- ✅ Badge rendering decision (2026-07-18): **repo goes public** — the badges-branch + raw-URL setup then works as-is, no code changes. Flip: repo Settings → General → Danger Zone → Change visibility

## Docs ✅

- ✅ Operator documentation (2026-07-18): reworked `README` (accurate status, quickstart, doc index) + `docs/INSTALL.md` (binaries/Docker/Homebrew/source/musl), `docs/CONFIGURATION.md` (full annotated `nzbd.toml` reference), `docs/USAGE.md` (CLI, UI, *arr hookup, feed filter language, scripts, deobfuscation), `docs/DEPLOY.md` (copy-paste recipes: Docker by hand incl. volume map + lifecycle, Compose, Kubernetes, systemd, multi-node cluster)
- ✅ Docker config mount fixed (2026-07-25): the shipped Compose recipe delivered the config through a Compose `configs:` entry — always mounted **read-only** — so every settings-editor save failed with `Read-only file system (os error 30)`; `docs/INSTALL.md`, `docs/DEPLOY.md` and the README used `:ro` config binds for the same net effect. All four now bind the config **directory** read-write (`./config:/etc/nzbd`), which also sidesteps the Docker-invents-a-directory trap for a missing config file and lets the first-run wizard write one. `dev/docker-compose.yml`'s ownership hint corrected to `chown -R 1000:1000` (the container runs as UID 1000; `$USER` only works if you happen to be 1000). Guard test `shipped_compose_files_mount_the_config_writable` fails the build if a shipped compose file goes read-only again; Kubernetes stays deliberately read-only and DEPLOY.md now says the editor is view-and-copy there
- ✅ Deployable examples under `examples/`: `docker-compose/` (compose + `nzbd.toml.example`), `kubernetes/` (namespace/secret/PVC/deployment/service/kustomization + README incl. RWX cluster shape), `systemd/` (hardened unit)
- ✅ `dev/` local-build compose (image from the repo Dockerfile, `compose watch` rebuilds, throwaway `dev/data/`, gitignored dev config) + root `.dockerignore` (target/ was going into every build context); example configs are parse-tested against the real validator (`nzbd-config/tests/examples.rs`)

## Phase 2 — Post-processing ✅ complete

- ✅ par2 packet parser + **native quick-verify** from download CRCs — zero data re-read for intact sets (`nzbd-post/src/par2.rs`, proven against real `par2 create` output)
- ✅ par2 verify/repair subprocess wrapper (par2cmdline-compatible output parsing: Intact / Repairable / NeedMoreBlocks / Unrepairable)
- ✅ Delayed-par unpause: `UnpauseParBlocks` engine command, smallest covering set from `.volXX+NN` names; repair loop waits for the fetched blocks
- ✅ Unpack: unrar/7z subprocess, hardened (argv-only, scrubbed env, timeouts, 256 KiB output caps, kill-on-drop); NZBGet exit-code maps (unrar 11=password, 5=disk; 7z requires "Everything is Ok"); `.zip`/`.7z`/`.rar` multi-volume first-only/`.001`
- ✅ PP orchestrator: par verify → repair → unpack (⇆ forced-repair retry once) → cleanup → scripts; PostStrategy slots (sequential/balanced/aggressive/rocket); `*PP:done` stamp makes restarts idempotent; 30 s rescan covers leader takeover + lagged events
- ✅ NZBGet extension-script protocol: `NZBPP_*`/`NZBPR_*` env, `[LEVEL]` stdout log lines, `[NZB] KEY=value` commands (FINALDIR honored), exit codes 92–95, legacy header + v2 `manifest.json` discovery
- ✅ History: local SQLite index + authoritative append-only JSONL (shared volume in cluster mode per ADR-16; SQLite never on network FS; index rebuilt from JSONL on divergence)
- ✅ `[post]` config section; daemon wiring single-node **and** cluster (PP runs on the leader, gated live on election state)
- ✅ 6-test e2e suite against real binaries: intact fast path + script env/FINALDIR, corrupt→repaired bit-identical, unrepairable→PAR_FAILURE, unpack+cleanup, script-error→SCRIPT_FAILURE, event-driven manager + restart-skip
- ✅ par-rename / rar-rename: obfuscated posts recover real names before verify/unpack — par2 16k-MD5 catalog (obfuscated `.par2`s found by magic), RAR4/RAR5/7z/zip signatures, RAR5 internal volume numbers, evidence paths remapped so quick-verify still runs; e2e proves obfuscated → renamed → Intact
- ✅ Final-name deobfuscation (`post.deobfuscate_final`, default on): after unpack, whatever still carries a meaningless name gets the job name — SABnzbd's dominant-file rule (biggest ≥ 3× next) with its heuristics ported, plus **season packs** (which SABnzbd skips): several similar-sized videos, all hex/uuid-grade obfuscated → stable `<job> - NN` numbering, logged as heuristic. par2-set names are evidence-protected (never overridden); companions (`.srt`, `-sample`) follow their media file; per-daemon e2e through the real binary. Discrete status: queue shows the `post_unpack_rename` stage (compat `RENAMING`) while it runs; every applied rename is logged and recorded as `Deobfuscate:Count`/`Deobfuscate:Files` job params that persist into history and the compat `Parameters` array
- ✅ Per-job unpack passwords (`*Unpack:Password` job parameter, NZBGet convention) — e2e with a passworded archive
- ✅ Dupe handling (key/score/mode): append carries DupeKey/Score/Mode onto the job; Score/All block against queue + history successes, Force overrides; rejects recorded as `DELETED/DUPE`; real dupe fields in listgroups/history
- ✅ Health-check actions (`HealthCheck`: none/park/delete) — delete removes the failed download's files; recorded `FAILURE/HEALTH`
- ⬜ Direct unpack (`unrar -vp` volume feed during download) — deferred (deep coupling with the download pipeline; unpack-after-download covers the outcome)
- ⬜ Fixture suite extras: par2 damage matrices, multi-volume/passworded rar
- ✅ C2: PP work-lease type + anti-affinity scheduling (a job downloaded on node B post-processes on node C) — see the cluster section
- ⬜ C2 fixture extras: kill-mid-PP reclaim e2e (reclaim machinery itself is exercised by the download-lease tests)

## Phase 3 — Native API + compat ✅ complete

- ✅ Compat C1 — the Sonarr/Radarr certification surface: `append` (v13+ 9-arg form AND legacy 5-arg positional form; base64 or raw XML; AddPaused honored; returns NZBID or 0), `history` (full NZBGet field shape: `TOTAL/DETAIL` statuses, Lo/Hi/MB triplets, Parameters, FinalDir/DestDir, Par/Unpack/Script statuses, deprecated aliases), `editqueue` (3-arg v16+ AND 4-arg v13 forms: Group Pause/Resume/Delete/FinalDelete/SetPriority/SetCategory/SetParameter, HistoryDelete; GroupDelete records a `DELETED/MANUAL` history entry), `config`/`loadconfig` (option projection incl. `CategoryN.*`), `rate`, `pausedownload`/`resumedownload`
- ✅ Queue→history lifecycle (NZBGet parity): post-processed jobs retire out of the queue — immediately after local PP, via the leader sweep in cluster mode; health-failed jobs stamped + retired the same way
- ✅ Native `GET /api/v1/history` (limit param; cluster-aware via throttled JSONL union refresh)
- ✅ Post-stage queue status vocabulary in `listgroups` (VERIFYING_SOURCES / REPAIRING / UNPACKING / EXECUTING_SCRIPT / …)
- ✅ e2e: `sonarr_style_flow_over_jsonrpc` against the real daemon binary — version gate → config category check → base64 append → listgroups poll to empty → history shows SUCCESS/ALL + FinalDir → file imported bit-identical
- ✅ HTTP auth: Basic (NZBGet `ControlUsername`/`ControlPassword` parity, constant-time compare, `WWW-Authenticate` challenge) + Bearer token; enforced across native API and compat shim when configured; `/healthz` open; cluster peer endpoints keep their own shared-secret auth; importer maps `ControlUsername`/`ControlPassword` (with a warning on NZBGet's well-known default)
- ✅ `GET /api/v1/events` — engine events as SSE (job added/finished/deleted, file finished, segment exhausted, server blocked; lagged signal)
- ✅ `GET /metrics` — Prometheus text exposition (rate, remaining, session bytes, paused, speed limit, jobs by status)
- ✅ Compat C2: `listfiles` (full file detail), per-file editqueue actions (FilePause/FileResume/FileDelete via new engine file commands), `log`/`writelog` on the daemon log ring, `scan` + NzbDir watch-dir (30 s + on-demand, `.queued`/`.error` renames, authority-only in cluster mode)
- ✅ XML-RPC (`/xmlrpc`): full value codec (string/int/i4/i8/boolean/double/base64/nil/array/struct, entity refs), `system.multicall`, fault responses — same method table as JSON-RPC
- ✅ JSON-P + GET forms: `GET /jsonrpc?method=…&params=…[&callback=…]`, `/jsonprpc`
- ✅ Golden structural tests: exact wire field sets locked for status/listgroups/history/listfiles/log/envelope — a renamed field fails the suite
- ✅ Native: `GET /api/v1/logs` + `/api/v1/openapi.json` surface summary; log ring fed by a tracing layer
- ⬜ Nightly live *arr containers (CI workflow using real Sonarr/Radarr images) — operator infrastructure; the golden suite + sonarr-flow e2e cover the wire contract in-repo
- ⬜ Auth roles (restricted/add-only users) — full-control auth shipped in 3b
- ✅ `nzbget.conf` importer: KEY=value + `ServerN.*`/`CategoryN.*` blocks, recursive `${Var}` expansion, NZBGet→nzbd vocabulary (Level→tier, Optional→fill, Encryption→tls), mapped/skipped/unknown/warnings report, hostless-server drop, zero-connection raise; `nzbd import-config <nzbget.conf> -o nzbd.toml` writes the converted file + prints the report; round-trips through the TOML parser
- ⬜ `rapidyenc-sys` FFI feature (vendored) + differential fuzzing — deferred to phase 5 (the scalar decoder saturates typical line rates)

## Phase 4 — Web UI + ecosystem ✅ complete

- ✅ Embedded web UI at `/`: one self-contained page compiled into the binary (`include_str!` — zero build toolchain, an explicit simplification from the Svelte plan). Queue with live progress/actions, history, log tail, pause/resume/speed-limit controls, quota/paused badges, SSE-driven refresh with poll fallback, dark/light
- ✅ Restart hang fixed + queue-load state: an open SSE stream (`/api/v1/events`, always held by the browser) blocked axum's graceful shutdown, so a UI-triggered restart hung the daemon mid-reload and it never re-served ("clicking restart does nothing"). SSE streams now end when shutdown is signaled (watch channel → forwarding task), and the drain has a 3s hard deadline as a backstop; e2e holds an SSE open across a restart and asserts the stream closes + daemon re-serves (verified it hangs without the fix). Also fixed the "queue is empty" flash: the engine now seeds its recovered snapshot synchronously before `spawn` returns, so the first API read is authoritative (regression-tested); UI shows "Loading the queue…" vs "No downloads in the queue."
- ✅ UI boot smoke test: node harness executes the embedded page's inline JS against a DOM shim on every `cargo test` — fails on load-time errors, unhandled rejections, `$()` lookups of missing ids, or boot never starting the SSE plumbing (the class of bug where the script parses but dies at load; proven against the real connectSse regression). Self-skips without node; NZBD_REQUIRE_TOOLS makes CI strict
- ✅ Health-abort transparency (field feedback: "hit 57% health and it still hasn't been cut off"): critical-health abort is armed only when `[post] health_action` is park/delete (matching NZBGet's default-off HealthCheck); the queue now shows each job's par2 **repair limit** under its health %, and a downloading job below that limit is badged **unrepairable · aborting** (armed) or **unrepairable · will fail at end** with a how-to-enable tooltip (disarmed) — `health_abort` exposed via QueueSnapshot → StatusDto
- ✅ Handoff visibility (field feedback: "the *arr handoff is a black box"): the pull is now observed — history entries record first/last-seen (client polls), seen-count and imported-by; NZBGet `HistoryDelete` = hide + stamp (matching real *arr cleanup), `FinalDelete` = remove; `history(true)` returns hidden rows; native history includes hidden + handoff fields; `GET /api/v1/clients` + UI strip show every compat consumer (UA, polling/quiet, last call); History tab handoff chips (awaiting pickup / seen ×N / ✓ imported by) with per-entry restore (re-expose → *arr re-imports), hide, forget, delete-files; hidden state survives index rebuilds (JSONL re-append + upsert); compat test drives the full lifecycle
- ✅ Settings tab v2 (field feedback: "make it a normal settings page"): form-based UI over structured config JSON — grouped cards (paths / servers with add-remove / speed & queue / web UI & API / post-processing / categories), masked passwords kept unless retyped, dirty-tracked sticky save bar; save live-applies what the daemon can absorb (speed limit) and section-diffs the rest into a persistent **restart required** banner with a Restart button (`POST /api/v1/restart`); raw-TOML editor demoted to a collapsed Advanced panel (feeds/cluster); e2e drives live-apply → restart-flag → restart → secrets-preserved
- ✅ Settings API v1: `GET/PUT /api/v1/config` — the live TOML with secrets masked (`***unchanged***` round-trips without retyping passwords), strict validation on save, write + hot-reload via the generalized setup/reload handle (always present now); UI editor with save/discard/download, read-only fallbacks for unwritable paths and `--config`-less runs; fixed a latent bug where a configured speed limit was ignored whenever a queue snapshot existed (config now wins when set)
- ✅ Test depth: SIGTERM graceful shutdown (what docker stop/tini/systemd send) + e2e teardown uses it, so spawned-daemon coverage flushes; full-suite line coverage 85.9% (was 83.2 measured, 26.8 when only `--lib` is run — the suite is e2e-heavy by design); tls.rs 6→90%, nzbd-api 58→80%, main.rs 49→64%; new unit tests for TLS cert lifecycle, compat XML-RPC edges, mask/merge, main.rs option projections
- ✅ Queue UX round 2 (field feedback): job-name cleaning (glued indexer query params stripped, NZB meta-title / par2 set-name fallback for obfuscated hashes); per-job rate EMA + downloaded/ETA in API and UI; global time-left tile; health as green→red gradient against the par-repair limit with tooltip; critical-health early abort (`health_action != none` fails pending segments the moment repair becomes impossible — e2e proves it finishes in 0.25s instead of downloading a doomed job); queue reordering (move top/up/down/bottom) through engine + native API + compat GroupMove*; scheduler tiebreaker = queue position (stable within priority); stall transparency (disk-low / quota / blocked-server holds surfaced as a UI banner + status DTO, block logs at warn); reconnect stagger after resume; dark-default theme following the system with manual auto/dark/light toggle; mobile layout fixes; UI screenshot-verified via headless Chromium (dark/light/hold/mobile)
- ✅ PWA + built-in HTTPS: web manifest, generated icon set (192/512/maskable/apple-touch), app-shell service worker (never caches live data), standalone display + iOS meta, responsive phone layout; PWA assets auth-exempt (browsers fetch them credential-less). `[api] tls = true` serves HTTPS natively — self-signed cert generated once under the state dir (fingerprint logged, `tls_sans` for extra names) or bring-your-own `tls_cert`/`tls_key`; importer maps `SecureControl`/`SecureCert`/`SecureKey`; e2e proves HTTPS handshake → healthz + manifest + icons over TLS
- ✅ First-run setup wizard: a missing `--config` boots setup mode instead of erroring; container-proof saving — boot-time writability probe surfaced in the UI, `preview` mode renders the TOML without writing, failed writes return the TOML with copy/download fallback (read-only mounts/ConfigMaps), directory-at-config-path yields an actionable boot error — the UI serves a form (paths, one server, optional UI password), `POST /api/v1/setup` writes the TOML (round-tripped through the strict parser first) and the daemon hot-reloads with it (`RunOutcome` loop, no restart); Docker image's `/etc/nzbd` is nzbd-writable so a mounted empty config dir + the wizard is the zero-config container path; e2e proves boot → wizard → reload → auth-on
- ✅ Compat C3: `servervolumes` (live per-server total/day/month counters), `sysinfo` (OS/arch + tool paths), `testserver` (real NNTP connect + greeting + AUTHINFO through the production transport — proven against nserv in tests)
- ✅ Packaging: multi-stage `Dockerfile` (tini + par2/unrar/7z, unprivileged user), tag-triggered release workflow (musl static x86_64 + aarch64, macOS aarch64, sha256 sums, ghcr.io Docker push), Homebrew formula with service block
- ✅ Live *arr smoke workflow (`arr-live.yml`, weekly + manual): boots real Sonarr against nzbd and asserts the NZBGet download-client validation passes
- ✅ RSS feeds + filter language (`nzbd-feed`): per-feed pollers over the URL-job fetcher; RSS 2.0 / Atom / newznab parsing (enclosures, `newznab:attr` size, entity refs, CDATA); NZBGet-style filter language (Accept/Reject/Require + `A:`/`R:`/`Q:`, wildcard title/category/url terms, `size:` windows, `age:`, negation, Accept options category/priority/pause/dupekey/dupescore); guid seen-ledger (90-day retention, shared-volume in cluster mode so failover never re-downloads a backlog); leader-gated polling; `fetchfeeds`/`viewfeed` compat RPCs; `FeedN.*` mapped by the nzbget.conf importer (`%` → newline in filters); e2e: feed poll → filter → URL job queued once, deduped on re-poll
- ✅ Live dashboard round 3 (field report 2026-07-25: "screen frozen until browser refresh · unpause showed nothing · header speed 4× off from the row's · titles full of query junk"): **(1) cache discipline** — every live JSON endpoint now sends `Cache-Control: no-store` via a router-wide default layer (some browsers heuristically cache header-less `fetch()` GETs, so the 5 s poll re-served its first response forever; header rate and row rate were two *differently-stale* cache entries — hence 6.4 vs 24.8 MiB/s); the UI's fetches all pass `cache: "no-store"` as the client-side belt, the shell revalidates (`no-cache`), icons keep `max-age`; **(2) SSE `tick`** — the event stream now pushes the full `{status, jobs}` read model at 1 Hz from ONE snapshot (progress/rate/ETA move live with zero fetch round-trips; header and rows can't disagree by construction; idle ticks dedupe to keep-alives; poll drops to a quiet-stream fallback); **(3) complete event wiring** — `queue_pause_changed`/`speed_limit_changed`/`job_assigned` cross under their own names now (previously an opaque `event` the UI ignored) and the UI subscribes to every one; **(4) pause/resume feedback** — the current-state button latches (disabled + amber), the time-left tile flips to "paused"/back optimistically on click, the server confirms via tick + event within a second; **(5) fetching rows** say "fetching the NZB from the indexer…" instead of the broken-looking "0 B of 0 B · 0%" and drop the pause button (the engine rejects pausing a fetch anyway). Regression tests: cache-header matrix, SSE first-frame-is-tick with status+jobs payload, event-wire names, UI boot harness
- ✅ Restart never waits on post-processing (field report 2026-07-25, #2: clicked Restart in Settings, the page never came back — the container was up, the engine kept serving fetches, but teardown sat in `pp_tracker.wait()` behind one running PP job): PP job tasks are cancel-aware at both await points now — a queued job won't *start* mid-shutdown, and a running pipeline aborts on cancel (safe by the pipeline's own crash model: no `*PP:done` stamp yet, kill-on-drop subprocesses, next pass's rescan re-runs it); the daemon also bounds the feed/PP drain at 10 s with a warning as a belt, so no future subsystem can hold the listener offline. Regression e2e: full download → PP reaches a 600 s extension script → `POST /api/v1/restart` → daemon re-serves in seconds with the job still queued for re-PP
- ✅ News-server "test connection" (field request 2026-07-25): `POST /api/v1/servers/test` — live connect + greeting + AUTHINFO through the production NNTP transport (same `probe_server` now backs the compat `testserver` RPC); buttons in **Settings → each server card** (tests the form as it stands, unsaved edits included — a stored password the browser only knows as `***unchanged***` is resolved server-side via `server_index`, no retyping) and in the **first-run wizard** (verify before saving anything); inline ✓/✗ result with the greeting or the failure. Tests: live probe against nserv (good + wrong password), dead port, mask resolution, mask-without-config refusal
- ✅ Trustworthy dashboard round 4 (field reports 2026-07-25 #3–5: queue pause flapping every second · row rate 55.9 vs header 93.1 MiB/s and "the download looks super stale" · per-file lines drowning the Logs tab): **(1) pause attribution** — the engine has exactly two writers of the pause flag, both client commands; every pause/resume now carries WHO (UI sends `X-Nzbd-Client: web-ui`, compat calls use the caller's UA), logged at info (`queue pause requested source=…`), carried on the `queue_pause_changed` event, and shown on the paused badge ("paused · by monarr/1.0") — a flapping queue is a one-glance diagnosis, plus every state-changing compat RPC logs method+client; **(2) wire-true job rates** — the header rate is wire bytes but per-job rates were completed-article deltas: two different quantities whose gap (retries/discards) read as a stale lying row; wire bytes are now attributed per job at the meter and the row rate is the SAME measurement as the header, with a per-job `retried_articles` counter surfaced in the row ("N retries") so wasted bandwidth is visible instead of mysterious; **(3) scoped logs** — every ring entry is tagged `system`/`job`/`file` (+ job id) at the tracing layer; `/api/v1/logs?scope=&job=`; the Logs tab gets scope toggles with per-file OFF by default; **(4)** the "history index rebuilt imported=57" line no longer fires on every 5 s poll (the JSONL union is an upsert and conflict-updates were counted as imports; true-insert counting via row-count delta, poll path logs at debug). e2e: pause holds across owner ticks until a client resumes; scope/job filter unit tests; event-wire carries source
- ✅ Job detail + uploads + activity legibility (field request 2026-07-25): click any queue row → inline detail panel with per-file table (`GET /api/v1/jobs/{id}/files`: sizes, segment done/failed counts, paused/par2/assembled), a live per-job activity tail (`/api/v1/logs?job=N`), article counts + health + dupe key, and **download nzb** (`GET /api/v1/jobs/{id}/nzb` — regenerated from queue state, round-trips through the real parser); **+ add nzb** button uploads .nzb files straight into the queue (multi-file, inline result); post-processing stages render as plain words in the queue — checking integrity / repairing / extracting / cleaning up / moving / running scripts — so what a job is doing is never a mystery **(1)** placeholder names run through the junk-stripper at add time — glued query strings (and the API key riding in them) never reach the UI, history, or disk paths; **(2)** adding a URL that is already `Fetching` returns the existing job instead of queueing another copy (client retries were how one release became four 51 GiB downloads); **(3)** recovery re-spawns the NZB fetch for jobs restarted mid-fetch — the fetch task died with the old process and nothing ever picked the job back up, which is exactly how they got stuck at "0 B of 0 B" — one fetch per unique URL, same-URL pile-ups failed as `duplicate of job #N`; **(4)** in-flight fetches abort on shutdown (previously they could hold graceful shutdown for the 60 s hop timeout). e2e drives the full arc: junky-URL add → clean name asserted → same-URL dedupe → stalled fetch → shutdown under 10 s → restart → auto re-fetch → bit-identical download; plus refetch-plan unit tests
- ✖ Windows packaging — cut (per Paul, 2026-07-17)
- ⬜ Extension manager UI — scripts are discovered + run; a management surface remains

## Phase 5 — Beyond parity 🔶

- ✅ Per-provider adaptive pipelining: AIMD depth controller per connection — climbs one step after sustained clean batches, halves on connection failure; configured `pipeline_depth` is the ceiling, 1 the floor. Weak providers settle low, healthy ones ride the ceiling (exercised by the full e2e suite)
- ⬜ Native Rust par2 repair swap-in — the GF(2^16) Reed-Solomon engine is a project of its own; the subprocess boundary (`Par2Tool`) was designed for exactly this swap
- ⬜ COMPRESS DEFLATE (RFC 8054) — carried from phase 1; single-digit % on yEnc bodies
- ⬜ io_uring file I/O — blocked on tokio-uring maturity; DirectWrite already avoids the copy-heavy paths
- ⬜ Article-streaming / mount-mode groundwork — design work first (ARCHITECTURE.md §15)
- ⬜ Cluster C3: segment-split downloads, weighted scheduling, budget rebalancing — the lease protocol carries a `kind` field so a `Segment` lease slots in without wire changes
- ✅ RSS feeds + filter language — shipped (see phase 4)
- ⬜ `rapidyenc-sys` FFI + differential fuzzing — scalar decoder saturates typical line rates today
- 🔶 UI v2 — live-by-construction dashboard (field report 2026-07-25 #6: innerHTML-per-tick rendering eats clicks; confirm() on delete; silent action failures). Decisions locked with Paul 2026-07-25; plan + wire contracts in [docs/UI_V2_PLAN.md](docs/UI_V2_PLAN.md), one commit per milestone:
  - ⬜ M0 👤 redeploy nuc3 on a post-`9df6b7d` image (and add the missing `./config:/etc/nzbd` bind first) — kills the stale-cache half of the report before any v2 code lands
  - ✅ M1 keyed in-place renderer — store + `rowModel`/`reconcileRows`/`applyRow`, one delegated click listener, detail panel as a stable subtree; `crates/nzbd/tests/ui_dom.rs` pins the five rendering laws
  - ⬜ M2 optimistic actions with revert + toasts; honest `live`/`reconnecting`/`unreachable` connection states
  - ⬜ M3 delete parks the regenerated NZB to history as `DELETED` + `requeue` action
  - ⬜ M4 instant undoable delete — `confirm()` removed everywhere
  - ⬜ M5 SSE `hb` + `log` events, per-server wire rates
  - ⬜ M6 sparkline, title ticker, server chips, live log tail
  - ⬜ M7 docs sweep (ARCHITECTURE §12 as-built UI, USAGE `requeue`/`parked`)

## Operator checklist 👤

- ✅ Push `main` (done — CI ran; `badges` branch is CI-owned, never push it: `git branch -D badges && git fetch --prune`)
- ✅ Enable hooks on your clone (done — your pre-push ran the suite)
- ⬜ Optional: `brew install par2 p7zip` for full local test coverage (without them the tool-backed tests self-skip; CI always runs them)
- ⬜ Branch protection for `main`
- ⬜ Flip repo to public (decided 2026-07-18; Settings → Danger Zone → Change visibility) — Coverage/Test-count badges render once flipped
- ⬜ Real-provider download smoke test
- ⬜ Real-Gluster cluster soak (CLUSTERING.md §11)
