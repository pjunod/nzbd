# Magnet DHT build status — implementation and verification record

**Status:** implementation in progress · **Started:** 2026-09-20 ·
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
- [~] Adapter policy and bounded metadata resolution — implementation active.
- [ ] Durable admission cleanup and recovery classification.
- [ ] Developer settings enablement guidance and current-policy docs.
- [ ] Adversarial review after the implementation is otherwise merge-ready.
- [ ] Review findings resolved.
- [ ] Focused and workspace test gates run once after review fixes.
- [ ] Pull request merged to `main`.

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

## Verification record — blank until the final gate runs

| Check | Platform | Result | Evidence |
|---|---|---|---|
| Adversarial agent review | local | not run | Deferred until merge-ready. |
| Focused BitTorrent tests | local | not run | Runs after review fixes. |
| `make ui-test` | local | not run | Runs after review fixes. |
| `scripts/check-bittorrent-release-review.sh` | local | not run | Runs after review fixes. |
| `cargo test --locked -p nzbd-torrent` | local | not run | Runs after review fixes. |
| `make check` | local | not run | Runs after review fixes. |
| `make bittorrent-policy` | local | not run | Runs after review fixes. |
| Private discovery packet capture | Linux | not run | Requires an isolated Linux host. |

## Non-goals — boundaries that remain intact

- No automatic DHT enablement, public tracker injection, privacy inference
  from names or tracker URLs, or per-magnet privacy selector.
- No SOCKS bypass, UDP tracker proxy claim, v2/hybrid magnet support, or
  second engine session.
- No public-swarm dependency in CI and no claim that pre-metadata DHT lookup
  can provide zero hash exposure for a torrent later found to be private.
