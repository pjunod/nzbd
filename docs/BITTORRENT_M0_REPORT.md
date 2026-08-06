# BitTorrent M0 report — stable `librqbit` proves the data path, not the ownership contract

**Status:** complete; no-go for daemon integration · **Run:** 2026-08-05 ·
**Engine:** `librqbit =8.1.1`, `default-features = false`, `rust-tls` ·
**Host:** macOS 26.6 arm64 · **Decision owner:** ADR-19 in
[BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

The isolated spike proves that stable `librqbit` can download and seed nzbd’s
first-release v1/TCP/IPv4 workload. It does **not** yet justify wiring a torrent
session into the daemon. Two public-API gaps break accepted proposal contracts:

1. enabling fast-resume persistence automatically restores every library
   record during session construction, before nzbd can reconcile its durable
   job store; and
2. public per-torrent stats do not expose tracker/DHT health or the last
   tracker error required for honest stalled-state reporting.

The implementation therefore stops at the isolated `nzbd-torrent` boundary
and queue schema-version groundwork. No config switch, API, daemon dependency,
peer listener, or production torrent admission path has been added.

---

## 1. Result against the eleven gates

| Gate | Result | Evidence and consequence |
|---:|---|---|
| 1. Rust 1.85 and platform packaging | **Pass** | A real Rust 1.85.1 macOS build passes, and the release harness links only macOS system libraries, not OpenSSL. The first checked-in `BitTorrent M0` run caught Tokio 1.53.0 using Rust 1.86's `OnceLock::wait` on Windows. After the workspace pinned Tokio 1.52.4, the [corrected run completed successfully](https://github.com/pjunod/nzbd/actions/runs/31060326800) on 2026-08-06 UTC: the isolated adapter suite passed on Linux glibc, macOS arm64, Windows MSVC, and x86-64/aarch64 musl under Rust 1.85, and the exact-engine/Rust-TLS dependency policy also passed. |
| 2. v1 `.torrent`, magnet, TCP/IPv4, then seed | **Pass** | Deterministic generated payloads download through both admission paths; a local seeder accounts for both uploads and exact bytes match. |
| 3. Controls and live limits | **Pass** | Pause/resume is exercised before completion, the live download limit is removed during transfer, and idempotent keep-data delete, idempotent delete-data, and unrelated-sibling retention pass. |
| 4. Kill/restart never trusts partial data | **Blocked** | The accepted fast-resume design cannot be constructed without failing gate 8. A persistence-disabled full hash recheck is a possible safe but slower product decision, not an equivalent test of the accepted design. |
| 5. Private-torrent discovery | **Partial** | A one-tracker private torrent downloads through a loopback HTTP tracker, and a deterministic peer-wire control proves public torrents consume an injected PEX peer while private torrents ignore it. A new Linux capture harness starts live DHT, redirects both stable-8.1.1 bootstrap ports to a local KRPC probe, and observes separate public control hashes before and during a 15-second private canary window. It scans all captured UDP—not just redirected DHT—for the private hash in binary or LSD-style text form. The gate remains partial until that harness has a successful Actions run. Tracker order is also lost through a hash set before truncation, so the adapter rejects private metainfo unless it has exactly one unique tracker. |
| 6. Path and delete safety | **Pass** | Traversal metainfo is rejected before an escape file exists. Delete-data removes only parsed torrent content; an unrelated sibling survives. Higher layers must still prove the persisted canonical root before requesting deletion. |
| 7. Public observability | **Fail** | Public stats expose phase, total/progress/upload bytes, file progress, rates, ETA inputs, peer counts, completion, and error. Stable 8.1.1 does not expose per-torrent tracker state, DHT state, or last tracker error. A tested upstream patch now supplies that snapshot, but this gate remains failed until an accepted stable release contains it. “No peers” cannot safely substitute for those facts. |
| 8. nzbd-authoritative persistence | **Fail** | The contract test proves that `Session::new_with_opts` auto-restores the library record before returning. The persistence module and store injection point are private in 8.1.1, so nzbd cannot filter first. A tested upstream patch now supplies the missing opt-out, but this gate remains failed until an accepted stable release contains it. |
| 9. Resource, package, and license delta | **Partial** | Measurements are recorded in §4. A blocking cargo-deny 0.20.2 policy passes locally across all features and the locked graph, and its [first Actions run passed](https://github.com/pjunod/nzbd/actions/runs/31064446916) on 2026-08-06 UTC. It runs on every PR, main/tag push, and a daily schedule. Gate 9 still needs reviewer acceptance of the measurements and three exact, unreachable-path advisory exceptions. |
| 10. One explicit rustls provider | **Pass** | The process starts without a provider, explicitly installs aws-lc, and constructs librqbit’s rustls client without the mixed-provider panic. |
| 11. v1-only boundary | **Pass** | Stable input uses v1 pieces/`btih`; v2-only and hybrid `.torrent` files and magnets return separate named errors before librqbit admission. |

Gates 7 and 8 are stop conditions in §4.3 of the proposal. This is an M0
**no-go**, even though the data-path tests are healthy.

---

## 2. What was built

### 2.1 Isolated engine adapter

`crates/nzbd-torrent` pins stable 8.1.1 and contains the only librqbit-facing
code. The daemon does not depend on it. The boundary currently provides:

- explicit process-wide aws-lc provider installation;
- session construction with DHT and UPnP off by default, DHT persistence
  disabled unconditionally, and proxy+DHT rejected because DHT bypasses the
  engine's SOCKS path;
- raw v1 metainfo and v1 magnet admission;
- named v2-only and hybrid rejection for both metainfo and magnets;
- exact-one-tracker validation for private metainfo;
- pause, resume, idempotent forget, and bounded delete-data delegation;
- live session upload/download rate changes;
- info hash, phase, progress, upload, per-file progress, rates, derived ETA,
  peer aggregates, completion, and error facts;
- split SOCKS URL/username/password input with password redaction, a strict
  credential form, named rejection of UDP trackers, and a recording relay
  proving the peer path did not also connect directly; and
- an idle release harness used only for M0 measurements.

The crate forbids unsafe code. It is intentionally not a complete production
backend: there is no queue owner channel, durable torrent record, config/API,
URL fetcher, watch folder, seed policy, or session lifecycle in the daemon.

The dependency graph also required a production-daemon TLS correction outside
the isolated crate: `crates/nzbd/src/main.rs` and `crates/nzbd/src/tls.rs`
install the process-wide aws-lc provider before any daemon rustls client or
server is constructed. That idempotent provider selection is the only runtime
daemon behavior changed by the spike.

### 2.2 Deterministic tests

The isolated suite covers:

- `.torrent` and magnet download from one local TCP seeder;
- exact-byte verification and upload accounting;
- a live 16 KiB/s limit followed by an unlimited transfer;
- pause and resume before download completion;
- keep-data and delete-data behavior, including idempotence;
- authenticated SOCKS5 peer routing through a loopback proxy, with a
  recording relay proving no parallel direct connection;
- named rejection of proxy+DHT and proxy+UDP-tracker leak paths;
- password redaction and invalid proxy combinations;
- one-tracker private-torrent discovery and multi-tracker rejection;
- a positive-control PEX peer that public torrents contact and private torrents
  ignore; the canary address is available only through the peer-wire message;
- traversal rejection before filesystem escape;
- the explicit aws-lc provider invariant;
- v2-only and hybrid named rejection for metainfo and magnets; and
- the persistence auto-restore behavior that fails gate 8.

All peers, trackers, proxies, and DHT responders in the suite are local and
generated by the tests. The ordinary suite requires permission to bind
loopback ports but does not need the internet. The Linux-only capture harness
resolves rqbit's two bootstrap names, redirects their IPv4 UDP traffic to its
local DHT probe, and blocks matching IPv6 bootstrap traffic before the capture
point. Separate public lookups prove the probe is live before and throughout a
15-second private canary window. The harness scans every captured UDP packet,
not only redirected DHT traffic, for the private binary hash and the ASCII hash
used by LSD.

### 2.3 Queue schema version fallback

The independent M1a groundwork adds a versioned queue envelope:

- missing `schema_version` reads as legacy version 1;
- new writes use version 2;
- a future version fails by name. Healthy current snapshots parse once; if a
  typed parse fails, a header-only fallback distinguishes a future version
  from ordinary corruption so a future enum cannot masquerade as a generic
  parse failure;
- versions older than the supported floor fail by name; and
- the writer refuses to emit a non-current schema.

This is useful even while the engine decision is blocked. No torrent enum
variant or scheduler path was added.

### 2.4 Executable platform and dependency gate

`.github/workflows/bittorrent-m0.yml` turns gate 1 from a workstation claim
into a repeatable native matrix. Under Rust 1.85 it runs the real isolated
adapter suite — local seeder, tracker, authenticated SOCKS relay, persistence
contract, provider check, and input validation — on:

- Linux x86-64 glibc;
- macOS arm64;
- Windows x86-64 MSVC;
- Linux x86-64 musl; and
- Linux arm64 musl.

The two musl jobs run on their native CPU architectures and link and execute
the tests; they are not `cargo check` substitutes. The workflow is path-gated
to the adapter, lockfile, workspace dependency declaration, daemon TLS
provider initialization, its own workflow, and the policy script, so unrelated
documentation changes do not burn five native builders.

The first matrix run did its job before this report claimed success: Windows
failed while compiling Tokio 1.53.0 because that target's signal path calls
`OnceLock::wait`, which is unstable on Rust 1.85. The Linux-only workspace MSRV
check could not see the target-specific code. The workspace therefore pins
Tokio 1.52.4: upstream declares Rust 1.71 and its Windows signal path uses
`OnceLock::get_or_init` instead. The
[corrected native run](https://github.com/pjunod/nzbd/actions/runs/31060326800)
completed successfully on 2026-08-06 UTC. All five platform jobs executed the
isolated adapter suite under Rust 1.85, including the corrected Windows path,
and the dependency-policy job passed.

Before those builders run, `scripts/check-bittorrent-deps.sh` checks the normal
and feature graphs under the MSRV toolchain. It requires exactly
`librqbit 8.1.1`, requires `rust-tls`, rejects librqbit's default feature set,
and rejects `openssl`, `openssl-sys`, or `native-tls` anywhere in the adapter's
normal dependency closure. This guards the accepted dependency shape; it does
not replace binary linkage inspection or the gate 9 license/vulnerability
review.

Adding the gate starts no session in the daemon and changes no config, API,
listener, admission, or recovery behavior. The successful corrected run makes
gate 1 a pass. Gates 4, 7, and 8 remain blocked regardless of the platform
result.

---

## 3. The two blocking engine gaps

### 3.1 Fast resume cannot remain subordinate to nzbd state

The proposal requires this startup order:

1. load and validate nzbd’s queue snapshot;
2. determine the authoritative durable torrent jobs;
3. create the library session without starting anything else; and
4. explicitly restore only those jobs, using library resume data as an
   accelerator.

Stable 8.1.1 does something different whenever `SessionOptions.persistence`
is set: `Session::new_with_opts` opens the store, streams every stored record,
adds them all, and only then returns the session. `fastresume` shares that
persistence path. The public option selects JSON or Postgres storage, but it
does not provide an `auto_restore = false` switch, an authoritative ID filter,
or a public custom-store constructor. The `session_persistence` module itself
is private.

The contract test creates one persisted paused torrent, stops the session, and
constructs a second session without re-adding any nzbd job. The second session
already contains the torrent. That is a second queue authority, not merely a
cache.

Rejected workarounds:

- parsing or rewriting librqbit’s private `session.json` schema before boot;
- deleting resume records opportunistically and hoping crash order agrees;
- auto-restoring first, then stopping “ghost” torrents after they may already
  have announced or opened payload files; and
- copying the private persistence module into nzbd.

The acceptable engine API is small: either separate resume storage from
automatic admission, add an authoritative restore predicate/list, or accept a
public persistence store whose enumeration nzbd controls before session start.

### 3.2 Discovery health is not a public fact

`ManagedTorrent::stats()` is useful and the adapter now projects its stable
transfer/peer facts. Tracker monitors, however, keep their last errors inside
private tasks and tracing output. The public torrent stats model does not
answer:

- which tracker is working, backing off, or rejected;
- the last redacted tracker failure;
- whether DHT is healthy, disabled, or degraded for this job; or
- whether a trackerless magnet is stalled on discovery rather than peers.

Inferring these states from a zero peer count would make the API lie: a healthy
tracker can return no peers, and a failed tracker can coexist with live peers.
The accepted observability contract therefore needs a public discovery-health
snapshot or events with stable meanings.

### 3.3 The authoritative-restore patch is ready, not released

[`contrib/rqbit/0001-allow-persistence-without-auto-restore.patch`](../contrib/rqbit/0001-allow-persistence-without-auto-restore.patch)
targets rqbit v8.1.1 commit `00b97485160ff5b5aa2b379ea0815d568ec665f0`.
It adds one backwards-compatible `SessionOptions::disable_auto_restore` flag.
The default remains false, so existing rqbit callers still restore every
record. Setting it true keeps JSON persistence and fast resume active while
skipping only the constructor's implicit admission loop.

Both the exact v8.1.1 backport and the contribution patch for rqbit main at
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` carry the same contract. The test
persists two paused torrents, constructs a session that
starts empty, explicitly restores only the authoritative torrent with its
persisted `preferred_id`, and finally proves that a legacy session still
restores both records. That verifies the ownership seam without exposing the
private persistence schema or deleting library state before startup.
It does not independently prove restoration of non-empty piece progress, so
the patch comment is deliberately limited to the persistence identity the
test demonstrates.

[`scripts/check-rqbit-authoritative-restore-patch.sh`](../scripts/check-rqbit-authoritative-restore-patch.sh)
clones a supplied clean v8.1.1 or rqbit-main tree locally, requires the exact
stable tag or a descendant of the documented main base, reports the verified
SHA, applies the matching patch directly or by three-way fallback on main,
verifies formatting, and runs the isolated Rust-TLS test. The dedicated
`rqbit contribution patches` workflow checks both variants on relevant changes
and weekly for upstream-main drift. Both variants passed locally on 2026-08-06. This is
contribution evidence, not a production dependency: nzbd still pins
unmodified stable 8.1.1, gate 8 remains failed, and no daemon session consumes
the new option.

### 3.4 The discovery-health patch is ready, not released

[`contrib/rqbit/0003-expose-per-torrent-discovery-health.patch`](../contrib/rqbit/0003-expose-per-torrent-discovery-health.patch)
targets the exact v8.1.1 commit. Its public `ManagedTorrent::discovery_health`
snapshot distinguishes disabled, private-suppressed, inactive, searching,
working, and degraded DHT states, with current-run request and peer counters.
Each tracker reports a stable state, next-announcement delay, and bounded last
failure category. Public endpoints retain only scheme, host, and port; path,
query, user information, response body, and credentials never enter the
snapshot.

The corresponding rqbit-main patch starts at the same documented main base as
the authoritative-restore contribution. Tests prove endpoint and serialized
snapshot redaction, private-torrent DHT suppression, DHT success/failure
transitions, HTTP failure classification, tracker backoff, and rejected
tracker retention. The focused stable suites pass 37 tests with three
pre-existing network tests ignored; the focused main suites pass 69 tests
with six upstream integration tests ignored.

[`scripts/check-rqbit-discovery-health-patch.sh`](../scripts/check-rqbit-discovery-health-patch.sh)
uses the same exact-stable and descendant-of-main rules as the restore
verifier, reports the tested SHA, allows a three-way fallback only for main,
checks formatting, and runs all three affected crate suites. The dedicated
workflow tests both contributions against stable and main on relevant changes
and weekly for upstream drift. This is contribution evidence only: nzbd still
pins unmodified stable 8.1.1, gate 7 remains failed, and production wiring
remains prohibited.

---

## 4. Measurements and dependency review

Measurements are from the isolated optimized `m0_idle` harness on macOS 26.6
arm64. They are not the final daemon delta because the daemon intentionally does
not link the blocked adapter.

| Measurement | Result |
|---|---:|
| Unstripped optimized harness | 10,092,928 bytes (9.63 MiB) |
| Maximum resident set while starting/stopping one idle session | 8,732,672 bytes (8.33 MiB) |
| Normal dependency closure of `nzbd-torrent` | 220 unique package/version identities |
| New package/version identities in the workspace lockfile | 176 |
| OpenSSL dynamic link | none; only CoreFoundation, libiconv, and libSystem were listed on this host |

The normal closure reports permissive licenses: MIT, Apache-2.0, BSD, ISC,
Unicode-3.0, Zlib, Unlicense, CDLA-Permissive-2.0, and related compatible
expressions. `option-ext 0.2.0` is MPL-2.0. No GPL, AGPL, LGPL, SSPL, or BUSL
identifier appeared in the normal closure. This is an inventory, not legal
advice or final acceptance.

The initial resolver selected 2026 transitive releases requiring Rust 1.86 or
1.88. The lockfile now pins compatible releases in the allowed semver ranges,
including `serde_with 3.14.1`, `idna_adapter 1.2.1`, and ICU4X 2.1.x. With the
existing `time 0.3.41` MSRV pin restored, the actual Rust 1.85.1 toolchain
passes `cargo check --workspace --all-targets` on macOS.

The checked-in [`deny.toml`](../deny.toml) policy passes locally with
cargo-deny 0.20.2 across all features and the locked graph. It fails on an
unapproved license, registry, Git source, wildcard dependency, OpenSSL/native
TLS dependency, vulnerable or yanked crate, or unmaintained direct workspace
dependency. `option-ext 0.2.0` is the only license exception; its MPL-2.0
file-level copyleft is accepted at that exact version.

The first advisory run found and removed the daemon's direct
`rustls-pemfile 2.2.0` dependency. The daemon now parses certificates and
private keys through the maintained `rustls-pki-types` API already re-exported
by rustls. Three exact advisory exceptions remain:

- [`RUSTSEC-2026-0194`](https://rustsec.org/advisories/RUSTSEC-2026-0194)
  and
  [`RUSTSEC-2026-0195`](https://rustsec.org/advisories/RUSTSEC-2026-0195)
  affect quick-xml 0.37 through librqbit 8.1.1's unconditional UPnP helper.
  The path is reachable only when UPnP port forwarding is enabled, which M0
  keeps off. A production gate must remove both exceptions or keep UPnP
  unavailable.
- [`RUSTSEC-2026-0009`](https://rustsec.org/advisories/RUSTSEC-2026-0009)
  affects `time`'s RFC 2822 parser. rcgen compiles `time` without its parsing
  feature, so the vulnerable parser is absent. The fixed `time 0.3.47` raises
  its MSRV to Rust 1.88, while nzbd's verified floor is Rust 1.85.

[`supply-chain.yml`](../.github/workflows/supply-chain.yml) pins
`cargo-deny-action` to an immutable revision and runs separate blocking policy
and RustSec jobs on every pull request, main or release-tag push, a daily
schedule, and manual dispatch. The
[first blocking run](https://github.com/pjunod/nzbd/actions/runs/31064446916)
passed both jobs on 2026-08-06 UTC. Gate 9 remains **Partial** until a reviewer
accepts both the recorded delta and the narrow advisory dispositions. A green
exception is not evidence that the underlying code became safe to enable.

---

## 5. Recommended next decision

Preferred path:

1. open an upstream design issue for the discovery-health contract before
   submitting its multi-crate implementation, so rqbit maintainers can shape
   the public surface before nzbd treats the patch as an integration plan;
2. review and submit the prepared authoritative-restore patch and the agreed
   discovery-health implementation upstream;
3. reconcile both changes with upstream feedback without weakening nzbd's
   ownership, privacy, or observability contracts;
4. pin the first stable release containing both accepted APIs;
5. rerun all eleven M0 gates on native macOS, Linux glibc/musl, and Windows;
6. run the packet-capture private-mode test and obtain reviewer acceptance of
   the recorded resource, package, license, and advisory dispositions; and
7. only then resume M2 daemon integration.

The human-review checklist, submission order, issue draft, PR draft, exact
patch mapping, and reproduction commands are collected in the
[`contrib/rqbit` contribution kit](../contrib/rqbit/README.md). Nothing in that
kit has been posted upstream, and rqbit's AI policy requires human review and
editing before it is.

The two upstream changes may be designed, reviewed, and released separately,
but they are not separate production gates. Starting M2 with authoritative
restore alone would leave gate 7 failed and make tracker or DHT failure
indistinguishable from an ordinary lack of peers. That degraded view is not an
accepted shortcut around the observability contract.

Two alternatives require an explicit ADR-19 amendment:

- disable library persistence and accept full payload hash rechecks after each
  restart, together with measured restart cost and reduced availability; or
- evaluate `libtorrent-rasterbar`, accepting its C++/FFI and packaging cost in
  exchange for the required resume and observability surface.

The later ADR-19 re-check separated M1b from this engine gate: a fake-only,
protocol-neutral queue/backend seam is useful for either engine and starts no
networking, so it may proceed under the dormant limits recorded in
[BITTORRENT_M1B_REPORT.md](BITTORRENT_M1B_REPORT.md). Until an engine path is
approved, keep `nzbd-torrent` as a tested spike boundary and do not expose a
half-wired feature flag, admission route, or peer listener.

---

## 6. Reproduction summary

The key local gates are:

```sh
scripts/check-bittorrent-deps.sh
cargo deny --all-features --locked check bans licenses sources
cargo deny --all-features --locked check advisories
cargo test -p nzbd-torrent -- --nocapture
cargo test -p nzbd-state snapshot
cargo check -p nzbd-torrent

# Linux only; requires passwordless sudo, iptables/ip6tables, and tcpdump.
scripts/check-private-discovery-leaks.sh

# Against a clean local checkout of rqbit v8.1.1 or main at the documented SHA.
scripts/check-rqbit-authoritative-restore-patch.sh /path/to/rqbit
scripts/check-rqbit-discovery-health-patch.sh /path/to/rqbit
```

The Rust 1.85 check must select both the 1.85 Cargo and `rustc`; this host also
has a newer Homebrew compiler on `PATH`. The native platform matrix should run
in CI rather than treating missing cross-C compilers on macOS as a code result.

For public API evidence, see the pinned
[`SessionOptions`](https://docs.rs/librqbit/8.1.1/librqbit/struct.SessionOptions.html),
[`Session` construction source](https://docs.rs/librqbit/8.1.1/src/librqbit/session.rs.html#695-726),
and [`TorrentStats`](https://docs.rs/librqbit/8.1.1/librqbit/struct.TorrentStats.html).
