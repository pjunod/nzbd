# BitTorrent gate 9 review — accept measured cost, not silent risk drift

**Status:** ready for reviewer decision; gate 9 remains Partial ·
**Date:** 2026-08-07 · **Engine:** `librqbit =8.1.1` ·
**Decision owner:** ADR-19 in
[BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

Companion to
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md) (the complete spike result)
— this is the shortest review surface for gate 9's resource, package, license,
and advisory decision. Review the table and §4; do not mark the gate Pass
merely because CI is green. The remaining decision is whether the recorded
cost and three constrained exceptions are acceptable for the eventual first
release.

No production BitTorrent path is enabled by this review. Gates 7 and 8 still
fail until accepted stable rqbit APIs provide authoritative restore and honest
per-torrent discovery health.

## 1. Decision requested — four costs and three exact exceptions

The isolated M0 adapter measured the following in one macOS 26.6 arm64 run:

| Measurement | Recorded result | Reviewer question |
|---|---:|---|
| Unstripped optimized `m0_idle` harness | 10,074,960 bytes (9.61 MiB) | Is this binary delta acceptable for the single-binary release? |
| Maximum resident set for one idle session | 8,798,208 bytes (8.39 MiB) | Is this idle memory cost acceptable before real swarm load? |
| Normal `nzbd-torrent` dependency closure | 220 package/version identities | Is the maintenance and audit surface acceptable? |
| New workspace lockfile identities | 176 package/version identities | Is the dependency expansion proportionate to not implementing BitTorrent ourselves? |

These are spike measurements, not permanent limits. The complete M0 rerun
must measure the final daemon after an accepted rqbit release is linked. A
reviewer can accept this preliminary cost without claiming the eventual daemon
has the same size or resident set.

The normal closure is permissively licensed except for exact
`option-ext 0.2.0`, which is MPL-2.0 file-level copyleft. The checked-in
`deny.toml` accepts only that package/version under MPL-2.0; another MPL package
or version fails the blocking policy job.

## 2. Advisory scope — an exception is a capability restriction

| Advisory | Locked crate | Affected surface | Enforced disposition |
|---|---|---|---|
| [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html) | `quick-xml 0.37.5` | Attribute iteration can consume quadratic CPU on hostile XML | The only normal path is `librqbit-upnp 1.0.0`; the adapter has no UPnP input and always passes `enable_upnp_port_forwarding: false`. |
| [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html) | `quick-xml 0.37.5` | `NsReader` can allocate unbounded namespace state on hostile XML | Same compiled-but-disabled UPnP path. UPnP remains unavailable until the affected dependency is removed or patched. |
| [RUSTSEC-2026-0009](https://rustsec.org/advisories/RUSTSEC-2026-0009.html) | `time 0.3.41` | RFC 2822 parsing can exhaust the stack | The workspace reaches `time` only through `rcgen`/`yasna`; Cargo's resolved feature graph contains `alloc` and `std`, not `parsing`. |

The distinction matters: `quick-xml` is still compiled, so the exception is
not proof the crate is safe. It is acceptable only while nzbd cannot construct
the affected UPnP runtime path. The `time` functions named by its advisory are
not compiled without the `parsing` feature.

[`scripts/check-reviewed-dependency-exceptions.sh`](../scripts/check-reviewed-dependency-exceptions.sh)
turns those statements into a repository-wide blocking graph check. It fails
if:

- the `quick-xml 0.37.5` package set differs from the reviewed
  `nzbd-torrent → librqbit → librqbit-upnp` chain;
- `time 0.3.41` gains another package path or its exact `alloc`/`std` feature
  set changes;
- the exact MPL-2.0 `option-ext 0.2.0` path changes; or
- `deny.toml` ignores any RustSec identifier other than the three above.

Workspace package versions are normalized before comparison, so an nzbd
release bump does not masquerade as third-party graph drift. If a pinned
package disappears, the check fails closed with the exception or license
entry that must be reconsidered instead of exposing Cargo's unmatched-package
error as the only diagnosis.

The check lives in the repository-wide Supply chain workflow because the
`time` exception and complete RustSec ignore set are not BitTorrent-only. That
workflow runs on every pull request and push plus a daily schedule. The daily
run refreshes RustSec data; the locked graph assertions rerun alongside it but
can change only when tracked dependency inputs change.

The adapter unit test separately requires the constructed rqbit options to
keep UPnP false. Graph evidence and runtime-option evidence are intentionally
separate: either can regress without the other.

## 3. Reproduction — inspect the same evidence CI enforces

Run the exact dependency and exception checks from the repository root:

```bash
scripts/check-bittorrent-deps.sh                    # Exact rqbit/Rust-TLS graph; no OpenSSL.
scripts/check-reviewed-dependency-exceptions.sh    # Exact exception package and feature sets.
cargo test -p nzbd-torrent --lib                    # Includes the no-UPnP construction invariant.
cargo deny --all-features --locked check bans licenses sources
cargo deny --all-features --locked check advisories
```

Reproduce the macOS harness measurements rather than comparing a debug binary:

```bash
cargo build --release -p nzbd-torrent --example m0_idle
wc -c target/release/examples/m0_idle
/usr/bin/time -l target/release/examples/m0_idle
```

`maximum resident set size` is bytes on macOS. Linux reports different units
and allocator behavior, so cross-platform numbers are useful context but not
byte-for-byte acceptance comparisons.

## 4. Reviewer disposition — accept, reject, or require another boundary

Gate 9 may move from Partial to Pass only if a reviewer accepts all of these:

1. **Binary and idle-memory cost.** The one-sample 9.61 MiB harness and
   8.39 MiB idle RSS are acceptable preliminary costs, subject to
   final-daemon remeasurement.
2. **Dependency and license cost.** The 220-package closure, 176 new lockfile
   identities, and exact MPL-2.0 exception are acceptable.
3. **UPnP restriction.** Compiling the affected `quick-xml` is acceptable only
   because the first release cannot enable UPnP and CI guards that boundary.
4. **`time` restriction.** Retaining `time 0.3.41` for Rust 1.85 is acceptable
   only while its vulnerable parsing feature remains absent.
5. **Renewal rule.** Any engine, feature, advisory, dependency-path, or MSRV
   change reopens this decision; a previous green run is not a waiver.

If any item is rejected, gate 9 remains Partial and the remedy must be named:
upgrade or patch the dependency, remove the capability, change engines, or set
an accepted resource budget. Silence is not acceptance.

## 5. Non-goals — this review cannot authorize M2

- It does not make rqbit's persistence subordinate to nzbd. Gate 8 still
  requires an accepted stable authoritative-restore API.
- It does not expose tracker or DHT health. Gate 7 still requires an accepted
  stable discovery-health API.
- It does not add torrent configuration, admission, listeners, trackers, DHT,
  payload I/O, qBittorrent compatibility, or UI.
- It does not submit either prepared patch upstream.

Gate 9 is one required decision among eleven, not permission to skip the two
engine stop conditions.
