# BitTorrent release review — prove the boundary before enabling it

**Status:** pre-release review surface; production BitTorrent remains disabled ·
**Decision:** no-go while M0 gates 7 and 8 fail and gate 9 is Partial ·
**Owner:** ADR-19 in [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

This is the short operations and release-review surface for nzbd's proposed
BitTorrent backend. It tells a reviewer what network traffic, ports, storage,
seeding, and deletion behavior must be true before the feature can ship. It is
not an operator setup guide: the daemon has no torrent configuration, API,
listener, admission route, or session lifecycle today.

> **No-go:** do not add a production switch or weaken the daemon-isolation
> check while [M0 gates 7 and 8](BITTORRENT_M0_REPORT.md#1-result-against-the-eleven-gates)
> remain failed. Green adapter tests prove isolated behavior, not release
> authority.

## 1. Decision at a glance

| Review area | Current truth | Condition for release |
|---|---|---|
| Production reachability | None; `nzbd` does not depend on `nzbd-torrent` or `librqbit` | A separately reviewed milestone may wire the backend only after every stop gate passes |
| Public observability | **Fail** — stable rqbit does not expose per-torrent tracker/DHT health or last tracker error | Accepted stable API plus nzbd state, API, and UI contract tests |
| Authoritative restore | **Fail** — stable rqbit restores its own records before nzbd can reconcile durable jobs | Accepted stable opt-out plus restart/reconciliation tests |
| Resource and dependency decision | **Partial** | Human acceptance of the measured cost and bounded runtime policies in [BITTORRENT_GATE9_REVIEW.md](BITTORRENT_GATE9_REVIEW.md) |
| Adversarial M5 work | In progress | The remaining resource, mounted-filesystem, production shutdown, auth-limiting, and sustained-fuzz evidence is green |
| Operator action today | None | Do not publish torrent ports, paths, or config; they are proposal contracts, not supported settings |

The release decision is conjunctive. Passing one row never compensates for a
failed row, and a green workflow never changes the recorded gate state by
itself.

## 2. Public traffic

### Current boundary

The production daemon starts no peer listener, tracker client, DHT node, PEX
exchange, local discovery, or payload transfer. `make bittorrent-policy`
proves the daemon's normal dependency graph contains neither `nzbd-torrent`
nor any `librqbit*` package. Any change to that result is a release-boundary
change and requires its own reviewed milestone.

### First-release contract

| Traffic | Public v1 torrent | Private v1 torrent |
|---|---|---|
| TCP/IPv4 peers | Inbound on the configured peer port and outbound to discovered peers | Allowed only after private policy is known |
| HTTP(S) trackers | Allowed; secrets and queries stay redacted | Only the one validated metainfo tracker in the first release |
| UDP trackers | Allowed only without a SOCKS proxy | Same validation rule; proxy plus UDP fails closed |
| PEX | Enabled by default for public torrents | Disabled regardless of operator settings |
| DHT | Disabled by default; enabling it requires the accepted pre-metadata policy and health APIs | Disabled regardless of operator settings |
| Local discovery (LSD) | Disabled by default | Disabled regardless of operator settings |
| UPnP | Unavailable | Unavailable |

An unresolved magnet is privacy-unknown. Stable rqbit cannot suppress DHT for
one add before metadata reveals the private bit, so a DHT-enabled session must
reject magnet admission rather than risk public discovery. A proxy covers
eligible TCP peer and HTTP(S) tracker traffic only; it is not a VPN kill
switch or an anonymity guarantee. Deployments that require forced routing
must enforce it in the host or container network namespace and firewall.

**Reviewer acceptance:** packet capture must show the expected public controls,
no private hash through DHT/PEX/LSD, and no direct UDP path for a proxied job.
Logs and the Settings UI must display the redacted traffic policy before a
session starts.

## 3. Ports

There is no BitTorrent port to publish today. The existing nzbd API port is
unrelated to peer traffic.

The proposed first release owns exactly one explicit, non-zero TCP/IPv4 peer
port, defaulting to `6881` only after the feature is deliberately enabled.
There is no port range probe, random fallback, ephemeral port, automatic
router mutation, or UPnP. Failure to bind the configured port is a startup
failure for the torrent feature, not permission to choose another port.

DHT remains off by default. A later DHT-enabled release must document its UDP
socket and egress requirements explicitly; operators must never have to infer
them from library behavior.

**Reviewer acceptance:** the startup summary, Settings UI, container examples,
and firewall guidance must agree on every inbound and outbound transport. An
upgrade with torrent support disabled must open no new socket.

## 4. Paths

There is no production `torrent_dir` setting today. The proposed storage
contract separates immutable seed payloads from the existing completed-media
destination:

| Path role | Required behavior |
|---|---|
| Torrent payload root | Dedicated, canonicalized root; validated relative content only |
| Completed-media root | Existing import/post-processing destination; never silently used as the seed root |
| Category override | Must remain inside an authorized root and preserve the same portable path rules |
| *arr-visible path | The path nzbd reports must be mounted identically in nzbd and the media manager |

The dormant adapter rejects traversal, symlinks declared by metainfo, unsafe
existing prefixes, duplicate or prefix-overlapping files, reserved Windows
names, and portable Unicode/case aliases before storage construction. The
hosted native probes describe their temporary filesystems; they do not prove
descriptor-relative containment or every filesystem used by operators.

The isolated fault harness can inject storage exhaustion after one successful
piece write and proves that stable rqbit contains the failure to one incomplete
torrent while the same session completes a normal control transfer. Stable
rqbit reports that torrent as `Error`, not the proposed disk-paused state. The
harness has no daemon API or enforcing multi-root guard, so it does not prove
operator-visible ENOSPC behavior or authorize production wiring.

**Reviewer acceptance:** supported deployment examples must exercise the
actual bind mounts or volumes, case/normalization behavior, low-space guard,
symlink race boundary, import path, and restart authority. Until that matrix
is green, adapter path preflight is defense in depth rather than an operator
mount guarantee.

## 5. Seeding

No production seed policy exists today because no production torrent can be
admitted.

The first-release contract marks a verified torrent `ready` while it may still
seed. The default seed ratio and seed time are unlimited until the caller or
operator chooses a limit; nzbd must warn about an unlimited active seed rather
than invent a potentially tracker-hostile default. Reaching a ratio or time
limit pauses seeding and retains data. It never silently deletes payloads or
races the media manager's import.

Upload limits, achieved ratio/time, current upload rate, and paused-seed state
must be visible through the native API/UI and the supported qBittorrent
compatibility projection. Private tracker policy remains authoritative.

**Reviewer acceptance:** docs and examples must distinguish readiness,
seeding, paused seeding, import, and removal. A seed-limit test must prove that
the payload still exists and that explicit resume or removal remains possible.

## 6. Deletion

Deletion is always an explicit caller choice after the torrent handle is
stopped:

| Operation | Control-plane result | Payload result |
|---|---|---|
| Forget, keep data | Remove live metadata/control and write terminal history | Retain the validated payload and report its location |
| Forget, delete data | Remove live metadata/control and write terminal history | Remove only the persisted, authorized torrent content |

The isolated adapter proves idempotent keep/delete calls and retention of an
unrelated sibling. That is not the complete production proof: the durable job
must own the authoritative delete root, restart reconciliation must not widen
it, open handles must be stopped, and every refusal or partial failure must be
visible. A seed limit never implies either deletion operation.

**Reviewer acceptance:** restart, missing-file, symlink, external-move,
read-only, and partial-delete cases must leave honest history and must never
remove a sibling, parent, completed-media import, or path outside the durable
root.

## 7. Release evidence

Run the deterministic gate before every review:

```sh
make gate
```

Run the bounded local fuzz campaigns when the pinned nightly toolchain is
available:

```sh
make gate-fuzz
```

The release reviewer also checks the native and security evidence rather than
relying on one local host:

| Evidence | Last recorded proof |
|---|---|
| Native 100-torrent admission/shutdown pressure | [2026-08-09 matrix run](https://github.com/pjunod/nzbd/actions/runs/31330178035) |
| Private DHT/LSD packet capture | [2026-08-06 capture run](https://github.com/pjunod/nzbd/actions/runs/31128106994) |
| Cross-platform filesystem behavior | [2026-08-08 probe run](https://github.com/pjunod/nzbd/actions/runs/31240896567) and [review correction](https://github.com/pjunod/nzbd/actions/runs/31264422471) |
| Native adapter matrix and daemon isolation | [2026-08-09 M0 run](https://github.com/pjunod/nzbd/actions/runs/31331872561) |

These links are evidence snapshots, not evergreen approval. The reviewed
commit must have its own green required checks.

## 8. Sign-off and stop conditions

Before any production wiring PR can be approved, the reviewer records all of
the following:

- M0 gates 7 and 8 pass against an accepted stable engine release.
- Gate 9 is Pass with the resource ceilings and dependency exceptions accepted.
- The remaining M5 resource, filesystem, shutdown, auth, and sustained-fuzz
  work is executable and green.
- Public/private captures match §2 and every port is visible before startup.
- Supported operator mounts pass the path, disk-full, restart, and deletion
  matrix.
- Seed limits pause without deleting, while explicit keep/delete behavior is
  correct and visible.
- Disabled upgrades open no peer socket and admit no torrent.
- Rollback and downgrade drain/export steps have been exercised.

This document's drift check intentionally pins today's no-go state. When the
stop gates genuinely pass, the production-wiring PR must update the proposal,
M0 report, this review, and the check together. Bypassing the check is not a
substitute for that decision.
