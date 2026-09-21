# Magnet DHT build status — implementation and verification record

**Status:** complete; merged in [PR #233](https://github.com/pjunod/nzbd/pull/233) ·
**Started:** 2026-09-20 · **Qualified and merged:** 2026-09-21 ·
**Branch:** `codex/magnet-dht-discovery`

Companion to [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md) (the product
contract), [CONFIGURATION.md](CONFIGURATION.md) (operator settings), and
[BITTORRENT_RELEASE_REVIEW.md](BITTORRENT_RELEASE_REVIEW.md) (release
evidence). This page records what has actually been built and verified for
trackerless public magnet discovery. A checked implementation item means the
code exists; it does not imply that the final test gate has run.

## Delivery state — one review and test gate before merge

- [x] Isolated clone created from `origin/main`; the user's working checkout
  remains untouched.
- [x] Adapter policy and bounded metadata resolution.
- [x] Durable admission cleanup and recovery classification.
- [x] Developer settings enablement guidance and current-policy docs.
- [x] Adversarial review after the implementation is otherwise merge-ready.
- [x] Review findings resolved in the review-fix commit.
- [x] Focused and workspace test gates run after review fixes.
- [x] Pull request merged to `main` as `5ba95d9`.

## Contract — what this change must make true

With torrent support enabled, DHT enabled, and no SOCKS proxy, nzbd accepts a
valid public v1 magnet that has no tracker. Metadata is resolved in list-only
mode before managed storage exists, the info hash and metainfo limits remain
authoritative, and private metadata discovered through DHT is rejected before
admission.

Unknown privacy is not safety. A public lookup can expose the magnet hash
before metadata reveals the private bit. The Developer settings UI will state
that limit and show the current prerequisites as advice; it will not gate the
operator's enable control.

## Decisions — choices made during implementation

1. **Keep DHT disabled by default.** Existing installations retain their
   network posture; the operator makes the exposure trade-off explicitly.
2. **Use the existing rqbit session and list-only resolver.** One session keeps
   proxy, peer, and memory budgets coherent and avoids a hidden discovery path.
3. **Put the 120-second deadline in the adapter.** Every caller, including
   recovery, gets the same ownership and cancellation boundary.
4. **Cancel only pre-commit explicit failures.** Recovery keeps transient
   failures durable, removes deterministic failures, and never cancels a job
   after `finish` commits it.
5. **Do not use public swarms in automated tests.** Loopback fixtures keep CI
   deterministic and prevent tests from publishing real users' hashes.
6. **Validate the selected category root before commit.** Metadata-only
   resolution cannot know the queue-owned destination; `finish` applies the
   filesystem preflight to the canonical selected root before persistence.
7. **Classify hash-valid malformed metadata explicitly.** The maintained
   engine marks structural construction failures so recovery removes them as
   deterministic input instead of retrying them forever.

## Implementation record — reviewed and qualified

| Commit | Scope | Observable contract |
|---|---|---|
| `e31c936` | Adapter and maintained engine | Trackerless public magnets use loopback-proven DHT lookup; list-only resolution is bounded and private results fail before managed admission. |
| `d709fb0` | Queue owner and production admission | Explicit failures cancel only still-pending reservations durably; recovery removes deterministic failures and keeps transient failures. The native path proves peer rediscovery and payload hash. |
| `2f7fc0d` | Developer UI and operator docs | Advisory enablement requirements are visible without gating the enable control. |
| `e700970`–`c2366a0` | Adversarial-review repairs | Category-root timing, typed malformed metadata, timeout cleanup, recovery, tracker-only regression, and deterministic fixtures are covered. |
| `e2c72bb` | Maintained rqbit integration | The upstream CLI initializer remains complete after the authoritative PEX option was added. |
| `c2eccaf` | Final-gate repair | A newly elected leader returns retryable 503s until authority adoption completes, preserving healthy remote leases; the diagnostics fixture also waits for registry convergence. |

The cluster repair was a pre-existing `origin/main` failure reproduced in a
separate clean worktree during the final gate. It is included because the
requested merge standard requires a genuinely green workspace, not a waived
failure.

## Verification record

| Check | Platform | Result | Evidence |
|---|---|---|---|
| Adversarial agent review | local | findings resolved | Found category-root timing, malformed-metadata classification, timeout/restart proof, tracker-only discovery proof, port-race, and documentation gaps. No tests were run by the reviewer. |
| Focused BitTorrent tests | macOS | pass | Magnet preflight: 6; API admission: 20. |
| `make ui-test` | macOS | pass | Boot harness: 34 ID lookups; DOM harness: 602 assertions. |
| `scripts/check-bittorrent-release-review.sh` | macOS | pass | Maintained M0 state, operator domains, and active single-node wiring remain explicit. |
| `cargo test --locked -p nzbd-torrent` | macOS | pass | 57 library tests plus all portable integration suites passed; two native probes remained intentionally ignored. |
| `scripts/check-rqbit-maintained-patch-series.sh` | macOS | pass | Eleven-patch derivation/vendor drift, upstream library/tracker/DHT tests, release DHT dispatch, and upstream workspace compile passed. |
| `make check` | macOS | pass | Format, Clippy, all workspace/unit/integration/doc tests, and Rust 1.95 MSRV check passed on the final local head. |
| `make bittorrent-policy` | macOS | pass | Dependency, release-review, and reviewed-exception policies passed. |
| Private discovery packet capture | Linux (`nynuc`) | pass | Public controls were captured before and during the private window; the private hash was absent in binary and text forms. The temporary clone and bundle were removed. |
| PR CI and promotion gate | GitHub Actions | pass | Rust merge candidate, torrent transfer/recovery regressions, mobile compile/unit, static contracts, RustSec, dependency policy, and main promotion all passed before merge. |

## Non-goals — boundaries that remain intact

- No automatic DHT enablement, public tracker injection, privacy inference
  from names or tracker URLs, or per-magnet privacy selector.
- No SOCKS bypass, UDP tracker proxy claim, v2/hybrid magnet support, or
  second engine session.
- No public-swarm dependency in CI and no claim that pre-metadata DHT lookup
  can provide zero hash exposure for a torrent later found to be private.
