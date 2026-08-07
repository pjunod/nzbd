# Draft rqbit PR — bound retained peer records per torrent and session

**Status:** human review required; do not submit verbatim · **Target:** rqbit
main · **Stable evidence:** exact v8.1.1 backport · **Prepared:** 2026-08-07

This draft accompanies
[`0011-bound-known-peer-records-main.patch`](0011-bound-known-peer-records-main.patch).
The matching [`0010`](0010-bound-known-peer-records.patch) backport proves the
same behavior on rqbit 8.1.1; it is evidence, not the proposed upstream base.

> **Submission stop:** rqbit's `AI_POLICY.md` requires disclosure plus human
> review and editing. Before posting, read every peer admission, retry, and
> removal path, choose the final public option names and defaults with the
> maintainer, edit this text in your own words, and be prepared to explain the
> permit lifetime without an AI tool.

## Suggested title

Bound retained peer records per torrent and session

## Suggested PR body

### Problem

The live-peer semaphore limits simultaneous connection managers, but every
unique address learned from trackers, DHT, PEX, explicit input, or incoming
connections is first inserted into a `DashMap` and an unbounded peer-adder
queue. Retained queued, backoff, dead, and not-needed records do not consume a
live-peer permit. A long-running session can therefore retain an unbounded
number of records even when concurrent peers are capped.

Incoming-only records are especially persistent: after their manager exits,
the current code marks them dead or not needed even though they have no
outgoing address to retry. A changed outgoing address also remains in
`NotNeeded` while it is re-enqueued, so repeated reconnect triggers can enqueue
the same record more than once.

### Design

- Add optional default per-torrent and session-total known-peer limits to
  `SessionOptions`, plus a per-add override in `AddTorrentOptions`. `None`
  preserves current behavior and zero rejects new records.
- Acquire a per-torrent permit and then a shared-session permit before
  inserting a new record. If the shared acquisition fails, RAII immediately
  releases the local permit.
- Store both permits inside the `Peer` record rather than a connection task.
  Capacity is released only when the record leaves the map, matching the
  resource being bounded.
- Apply the same admission path to discovered/explicit outgoing addresses and
  new incoming addresses. Existing records remain eligible for an incoming
  connection without spending another permit.
- Remove incoming-only records when their manager exits. Preserve records that
  already have an outgoing address and may still be useful for reconnect.
- Move every reconnecting `NotNeeded` record to `Queued` before sending its
  address, including the alternate-outgoing-address case, so repeated triggers
  cannot duplicate that queue entry.

The nzbd proposal uses preliminary 1,024-per-torrent and 4,096-session values
for review. On exact rqbit 8.1.1, `Peer` itself measured 296 bytes on macOS
arm64, so 4,096 raw structs are 1,212,416 bytes (1.16 MiB) before map,
allocator, and live-bitfield overhead. That measurement is a sizing input, not
proof that the proposed limits are correct on every target.

### Tests

- the per-torrent ceiling admits exactly two records, refuses the third, and
  reopens after a permit is released;
- two torrents share one three-record session ceiling;
- actual map entries retain capacity until removal;
- a failed shared acquisition returns the already-acquired local slot;
- an alternate outbound address is queued exactly once with the retained
  record handle; and
- formatting, the focused suite, and the rqbit CLI targets compile on exact
  8.1.1 with Rust 1.85 and on the documented current-main source line.

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit --lib known_peer_budget_tests
cargo check -p rqbit --all-targets
```

### AI disclosure

This contribution was prepared with AI assistance. I reviewed and edited the
design, implementation, tests, and this description, and I can explain and
maintain the resulting code. Replace this statement if it does not truthfully
describe the final human review.

## Human review checklist

- Confirm `known_peer_limit` and `known_peer_limit_total` distinguish retained
  records clearly from simultaneous live-peer limits.
- Decide whether zero should reject all new records and whether `None` should
  remain unlimited for backward compatibility.
- Review local-then-shared acquisition ordering and the release behavior when
  the shared ceiling is full.
- Confirm incoming-only records should be removed at manager exit while a
  record with an outgoing retry address should remain.
- Decide whether hitting either limit needs a new metric or public statistic;
  current-main returns a distinct incoming result, while discovered peers are
  ignored like duplicates at the existing boolean boundary.
- Measure realistic record/map overhead on supported targets before accepting
  nzbd's preliminary 1,024/4,096 policy.
- Re-run rqbit's complete contributor gates on a fresh current-main branch,
  not only the focused verifier packaged by nzbd.

## Out of scope

- choosing peers to evict from a full map instead of refusing new records;
- per-IP, subnet, tracker, PEX, or DHT source quotas;
- limiting peer-wire message size or discovery request fan-out;
- dynamically resizing limits after session construction; and
- enabling nzbd's production BitTorrent path before all M0 gates pass.
