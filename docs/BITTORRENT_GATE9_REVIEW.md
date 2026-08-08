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
release, and whether the missing live-peer, retained-peer, tracker-request,
pre-routing handshake, and established-peer response-backlog controls must be
resolved before this gate can pass. DHT, metadata-resolution, and current-main
LSD queues also need explicit discovery-pressure boundaries.

No production BitTorrent path is enabled by this review. Gates 7 and 8 still
fail until accepted stable rqbit APIs provide authoritative restore and honest
per-torrent discovery health.

## 1. Decision requested — measured costs, exact exceptions, and resource gaps

The isolated M0 adapter measured the following in one macOS 26.6 arm64 run:

| Measurement | Recorded result | Reviewer question |
|---|---:|---|
| Unstripped optimized `m0_idle` harness | 10,111,360 bytes (9.64 MiB) | Is this binary delta acceptable for the single-binary release? |
| Maximum resident set for one idle session | 8,814,592 bytes (8.41 MiB) | Is this idle memory cost acceptable before real swarm load? |
| Normal `nzbd-torrent` dependency closure | 222 package/version identities | Is the maintenance and audit surface acceptable? |
| New workspace lockfile identities | 178 package/version identities | Is the dependency expansion proportionate to not implementing BitTorrent ourselves? |

These are spike measurements, not permanent limits. The complete M0 rerun
must measure the final daemon after an accepted rqbit release is linked. A
reviewer can accept this preliminary cost without claiming the eventual daemon
has the same size or resident set.

The proposed `max_peers_per_torrent = 80` and `max_peers_total = 400` are not
implemented by stable 8.1.1. Stable hard-codes 128 live peer permits per
torrent and exposes no shared session cap. The adapter's 80-entry guard applies
only to explicit/resolved bootstrap peers; later tracker, PEX, or DHT discovery
can fill all 128 engine slots. The pinned rqbit-main snapshot exposes a
per-torrent `peer_limit`, but still no session-total budget. Preliminary
binary/dependency acceptance therefore cannot authorize the advertised 80/400
runtime contract. The contribution kit proves a candidate per-torrent default,
per-add override, and shared session semaphore on both source lines. Incoming
and outgoing peer managers hold both applicable permits through release, and
two-torrent boundary tests exercise the aggregate ceiling. Until equivalent
controls ship in an accepted stable release, the candidate remains evidence,
not production capability.

The live-peer ceiling also does not bound retained peer records. Stable 8.1.1
and current main insert every unique tracker, DHT, PEX, explicit, or incoming
address into a map and unbounded peer-adder channel before a live permit is
acquired. Queued, backoff, dead, and not-needed records can therefore grow
without the 80/400 live-manager budget. A contribution candidate adds
caller-selected per-torrent and shared-session record permits, holds them for
the map lifetime, removes incoming-only records when their manager exits, and
prevents alternate-address reconnects from being queued repeatedly. The
proposal's preliminary 1,024/4,096 policy is informed by an exact-8.1.1 macOS
arm64 measurement: the 296-byte `Peer` struct makes 4,096 raw records
1,212,416 bytes (1.16 MiB), excluding map, allocator, and live-bitfield
overhead. That incomplete size measurement requires reviewer acceptance; it
does not turn the candidate into shipped capability.

The tracker-count preflight is also not a tracker-request budget. Stable 8.1.1
and the pinned rqbit-main snapshot issue HTTP tracker requests without a
tracker-owned deadline, buffer the full decoded response, and accept a
tracker-provided zero-second HTTP announce interval; UDP accepts five seconds.
The contribution kit proves a candidate 30-second complete-request deadline,
1 MiB streamed response cap, and 60-second minimum unforced HTTP/UDP interval
on both source lines. Until equivalent controls ship in an accepted stable
release, preliminary resource acceptance cannot authorize tracker networking.

The live-peer permits also start after an incoming client has completed enough
of its handshake to be routed to a torrent. Stable 8.1.1 accepts every TCP
socket into an unbounded pending-check set, where each incomplete client can
retain a socket, buffers, and a task for the 10-second handshake read timeout.
Current rqbit main has a configurable per-listener ceiling of 256 pending
checks. The contribution kit carries a stable-only 256-check backport and a
verifier for main's native boundary. This is a distinct pre-routing resource
budget: neither the 80/400 live-peer candidate nor the fixed timeout closes an
unbounded concurrency set. Until an accepted stable release enforces a
reviewed limit, production listener activation remains unauthorized.

Established live-peer work has another independent gap. Valid BEP 3 piece
requests enter an unbounded upload-scheduler channel and then an unbounded
per-peer writer; valid BEP 9 metadata requests enter the writer directly. A
rate limit or socket timeout slows draining but does not cap the number of
queued records. The contribution kit proves one candidate 128-permit response
window on both source lines. The permit follows a piece response across both
queues and a metadata response through the writer, and is released only after
the writer drops the item. The exact test remains blocked when work moves
between queues, reopens after a writer drop, and fails under an intentional
guard bypass. Until an accepted stable release enforces a reviewer-approved
window and backpressure policy, established peer traffic remains unauthorized.

Discovery has its own retained-work chain before a peer becomes live. Stable
8.1.1 and the pinned rqbit-main snapshot use unbounded channels for outgoing
DHT datagrams, recursive nodes, and delivered peers, and can grow the active
recursive future set without a fixed ceiling. Magnet metadata resolution uses
a 128-permit semaphore around I/O but can retain an unbounded future queue and
unique-address set before those permits. DHT maintenance uses unbounded
refresher/pinger channels and active future sets, and bootstrap starts every
configured hostname at once. Current main adds an unbounded LSD result channel
whose periodic announce task survives result-stream drop. The
contribution kit now proves candidate 256-record DHT send, recursive-node,
delivered-peer, DHT-maintenance, and LSD queues; 32 active recursive and 32
active maintenance requests per worker; eight concurrent bootstraps; 128
active metadata attempts; and 4,096 retained metadata candidates. It also
cancels LSD work with its owning stream, protects replacement registrations,
and removes an existing duplicate DHT request per recursive step. These fixed
values and UDP drop/backpressure choices are preliminary review evidence, not
released capability.

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

# Against clean rqbit v8.1.1 and current-main trees, respectively.
scripts/check-rqbit-tracker-request-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-session-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-pending-handshake-budget.sh /path/to/rqbit
scripts/check-rqbit-known-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-peer-response-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-discovery-pressure-patch.sh /path/to/rqbit
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

1. **Binary and idle-memory cost.** The one-sample 9.64 MiB harness and
   8.41 MiB idle RSS are acceptable preliminary costs, subject to
   final-daemon remeasurement.
2. **Dependency and license cost.** The 222-package closure, 178 new lockfile
   identities, and exact MPL-2.0 exception are acceptable.
3. **UPnP restriction.** Compiling the affected `quick-xml` is acceptable only
   because the first release cannot enable UPnP and CI guards that boundary.
4. **`time` restriction.** Retaining `time 0.3.41` for Rust 1.85 is acceptable
   only while its vulnerable parsing feature remains absent.
5. **Renewal rule.** Any engine, feature, advisory, dependency-path, or MSRV
   change reopens this decision; a previous green run is not a waiver.
6. **Runtime peer budgets.** Gate 9 cannot pass on the claim that the proposed
   80/400 peer budgets are enforced. An accepted stable per-torrent limit and
   shared-session cap must exist first, or the proposal must be explicitly
   amended with new measured and reviewed limits. The prepared combined-permit
   candidate is evidence for that review, not shipped capability.
7. **Tracker request budgets.** An accepted stable engine must bound the
   complete HTTP tracker request, decoded response body, and hostile unforced
   HTTP/UDP announce intervals. The prepared 30-second, 1 MiB, and 60-second
   candidate values are evidence for review, not shipped capability.
8. **Pending incoming handshakes.** An accepted stable engine must bound
   incomplete pre-routing handshake work separately from routed live peers.
   The prepared stable 256-check backport and main's native per-listener
   boundary are evidence for review, not shipped capability. A reviewer must
   explicitly accept the preliminary value and the TCP-only first-release
   scope or require a different measured ceiling.
9. **Retained peer records.** An accepted stable engine must separately bound
   queued, backoff, dead, and not-needed peer records per torrent and across
   the session. The prepared permit candidate and 296-byte raw-struct
   measurement are evidence for review, not shipped capability. A reviewer
   must accept or revise the preliminary 1,024/4,096 limits and require
   additional target-specific memory evidence if the raw measurement is not
   sufficient.
10. **Established-peer response backlog.** An accepted stable engine must
    bound remote-triggered piece and metadata responses across the upload
    scheduler and socket writer. The prepared 128-permit candidate and its
    failing negative control are evidence for review, not shipped capability.
    A reviewer must accept or revise the window and backpressure behavior.
11. **Discovery-pressure budgets.** An accepted stable engine must bound DHT
    datagrams, recursive work, delivered peers, and retained metadata
    candidates. Any stable line that includes LSD must also tie its bounded
    result stream and announce task to one lifecycle. The prepared
    256-record queue, 32-request worker, eight-bootstrap, 128-attempt, and
    4,096-candidate limits, UDP overload policy, exact-boundary proofs, and
    failing negative controls are evidence for review, not shipped capability.
    A reviewer must accept or revise each boundary.

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
- It does not treat the 80-entry bootstrap-input guard as a live-peer resource
  cap or accept stable 8.1.1's hard-coded 128-per-torrent behavior. The prepared
  shared-session patch still requires upstream acceptance and a stable release.
- It does not treat the 64-tracker input cap as a request lifetime, body, or
  announce-rate budget.
- It does not treat the 10-second read timeout or live-peer permits as a limit
  on the number of incomplete incoming handshake tasks.
- It does not treat the 80/400 live-manager limits as caps on retained queued,
  backoff, dead, or not-needed peer records.
- It does not treat upload rate limiting or a socket timeout as a bound on
  queued piece and metadata responses from an established peer.
- It does not treat live-peer or retained-peer permits as bounds on DHT, LSD,
  or magnet-metadata discovery queues and candidate sets.
- It does not submit any prepared patch upstream.

Gate 9 is one required decision among eleven, not permission to skip the two
engine stop conditions.
