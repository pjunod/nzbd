# rqbit maintained engine — reproducible v8.1.1 boundary for nzbd

**Status:** selected by ADR-19; isolated M0 dependency only · **Upstream base:**
rqbit v8.1.1, commit `00b97485160ff5b5aa2b379ea0815d568ec665f0` ·
**Stable patch delta:** 2,819 lines across exactly nine ordered patches ·
**Production daemon:** still does not depend on or start rqbit

Companion to
[BITTORRENT_PROPOSAL.md](../../docs/BITTORRENT_PROPOSAL.md) (the decision),
[BITTORRENT_M0_REPORT.md](../../docs/BITTORRENT_M0_REPORT.md) (the gate evidence),
and [BITTORRENT_GATE9_REVIEW.md](../../docs/BITTORRENT_GATE9_REVIEW.md)
(accepted limits and renewal rules).

This directory has two distinct jobs:

1. define the exact rqbit source derivation used by nzbd's dormant M0 adapter;
2. retain optional upstream contribution material without silently adding it
   to that dependency.

The first job is release-blocking. The second is useful maintenance work, but
upstream acceptance is not a condition of the selected M0 engine.

## 1. Maintained dependency contract

The dependency is derived from three reviewed inputs:

- GitHub's immutable rqbit `v8.1.1` source archive;
- its SHA-256 in [`upstream-v8.1.1.sha256`](upstream-v8.1.1.sha256); and
- the exact order in [`maintained-series.txt`](maintained-series.txt).

The maintained series is:

| Order | Patch | Contract |
|---:|---|---|
| 1 | `0001-allow-persistence-without-auto-restore.patch` | Keep persistence available while disabling implicit admission, so nzbd selects every restore. |
| 2 | `0005-bound-tracker-requests.patch` | Cap decoded responses at 1 MiB, complete requests within 30 seconds, and clamp unforced announces to at least 60 seconds. |
| 3 | `0007-bound-session-peers.patch` | Enforce 80 live peers per torrent and 400 across the session. |
| 4 | `0009-bound-pending-incoming-handshakes.patch` | Bound pre-routing TCP handshake checks at 256. |
| 5 | `0010-bound-known-peer-records.patch` | Enforce 1,024 retained peer records per torrent and 4,096 across the session. |
| 6 | `0012-bound-peer-response-backlog.patch` | Bound each established peer to 128 queued piece/metadata responses through socket write. |
| 7 | `0014-bound-discovery-pressure.patch` | Bound stable-line DHT and magnet-metadata queues, active work, and retained candidates. |
| 8 | `0016-limit-peer-metadata-before-allocation.patch` | Enforce the adapter's 10 MiB BEP 9 ceiling before allocation or requests. |
| 9 | `0018-propagate-file-sizing-errors.patch` | Stop initialization on the first selected-file sizing failure and preserve useful error context. |

The checked-in [`vendor/`](vendor/) tree is generated from those inputs. It
contains the derived upstream `LICENSE`, `README.md`, and `crates/` tree needed
by the workspace path dependency. Do not edit it directly. Change the source
patch, rerun the derivation check, and replace the generated result together.

Changing the archive, checksum, patch membership, patch order, generated
vendor, engine features, or a reviewed dependency path reopens ADR-19 and the
gate-9 disposition. A broad semver update is not permitted.

## 2. Why this remains rqbit, not libtorrent

The stable series is 2,819 patch lines, applies cleanly to one immutable source
archive, and has focused tests at every changed behavior boundary. It preserves
the Rust/Tokio single-binary architecture and adds no FFI or second daemon.
That stays within ADR-19's small-maintainable-fix branch, so the heavier
`libtorrent-rasterbar` C++/FFI and packaging fallback is not triggered.

The fallback must be reconsidered if this series stops being small,
reproducible, portable, independently reviewable, or economical to keep current.
Do not hide growing engine maintenance by weakening the drift check.

## 3. Reproduction and drift proof

Run the complete check from the repository root:

```sh
scripts/check-rqbit-maintained-patch-series.sh
```

The script:

1. checks exact series membership and order;
2. downloads the v8.1.1 archive, or uses `RQBIT_UPSTREAM_ARCHIVE`;
3. verifies the SHA-256 before extraction;
4. applies each patch with Git's conflict and whitespace checks;
5. compares the derived `LICENSE`, `README.md`, and `crates/` tree byte for
   byte with `vendor/`;
6. requires every named behavior proof to exist;
7. runs the affected `librqbit`, tracker, and DHT suites; and
8. checks the derived upstream workspace, excluding the desktop package.

For the quick, network-independent derivation check used by the ordinary
policy gate:

```sh
RQBIT_UPSTREAM_ARCHIVE=/path/to/rqbit-v8.1.1.tar.gz \
  RQBIT_SERIES_DERIVE_ONLY=1 \
  scripts/check-rqbit-maintained-patch-series.sh
```

CI runs the complete proof. The ordinary workspace resolves `librqbit` through
`contrib/rqbit/vendor/crates/librqbit`, keeps default features off, enables only
`rust-tls`, and separately proves that the production daemon graph remains
free of `nzbd-torrent` and every `librqbit*` package.

Every maintained behavior has a test that fails when its guard, permit
lifetime, propagation, or admission boundary is reverted. A patch applying is
not sufficient evidence.

## 4. Optional upstream material

The following files are not part of `maintained-series.txt` and therefore do
not affect the nzbd dependency:

- `0003-expose-per-torrent-discovery-health.patch` and its `0004` main variant;
- every even-numbered/current-main counterpart (`0002`, `0006`, `0008`,
  `0011`, `0013`, `0015`, `0017`, and `0019`); and
- the issue/PR drafts next to those patches.

Detailed tracker/DHT health is intentionally optional. The M0 public contract
requires transfer phase, verified progress, completion, peer counts, admission
facts, rates/ETA inputs, and a bounded credential-safe last error. When the
engine cannot supply detailed discovery diagnostics, nzbd reports `unknown`.
It never infers health from peer availability and never publishes a synthetic
torrent-health percentage.

Current-main variants are compatibility and possible upstream contribution
material only. Submit them from fresh human-reviewed branches, follow rqbit's
contribution policy, and reconcile upstream feedback without changing the
checked stable dependency implicitly.

## 5. Ownership and non-goals

The maintained restore model is nzbd-authoritative selective restore:

1. load and validate nzbd's durable queue;
2. construct rqbit with automatic restore disabled;
3. explicitly restore only the selected durable records; and
4. use rqbit resume state only as an accelerator for those records.

A full hash recheck is a safe degradation path, not the normal ownership model.
rqbit's private persistence schema is not copied into nzbd, and auto-restoring
then removing ghost jobs is not accepted.

This directory does not authorize production configuration, admission, a peer
listener, tracker/DHT work, payload I/O, UI, or qBittorrent compatibility. It
does not implement M2 daemon lifecycle or storage-fault routing. Passing all
M0 checks authorizes the next milestone to be decomposed and reviewed; it does
not make the dormant adapter reachable from `nzbd`.
