# Draft rqbit PR — bound discovery queues from socket to metadata resolver

**Status:** human review required; do not submit verbatim · **Target:** rqbit
main · **Stable evidence:** exact v8.1.1 backport · **Prepared:** 2026-08-07

This draft accompanies
[`0015-bound-discovery-pressure-main.patch`](0015-bound-discovery-pressure-main.patch).
The matching [`0014`](0014-bound-discovery-pressure.patch) backport proves the
DHT and metadata-resolution behavior on rqbit 8.1.1. LSD is a current-main
feature and therefore has no stable backport.

> **Submission stop:** rqbit's `AI_POLICY.md` requires disclosure plus human
> review and editing. Before posting, trace each bounded channel from producer
> to consumer, choose the final limits with the maintainer, edit this text in
> your own words, and be prepared to explain the overload policy without an AI
> tool.

## Suggested title

Bound DHT, metadata-resolution, and LSD discovery pressure

## Suggested PR body

### Problem

The DHT worker accepts outgoing datagrams through an unbounded channel. A DHT
lookup also feeds recursive nodes and discovered peers through unbounded
channels, while its `FuturesUnordered` set can start every queued recursive
request. A slow UDP writer or slow downstream peer consumer can therefore
retain work without a fixed ceiling.

Magnet metadata resolution uses a 128-permit semaphore around connection work,
but creates and retains one future for every initial peer before the semaphore.
It also retains every unique address in `seen`. The semaphore limits active
I/O, not queued futures or retained candidates.

Current rqbit main adds Local Service Discovery with another unbounded peer
result channel. Dropping its result stream removes the registration, but does
not cancel that stream's periodic announce task. A stale stream can also
remove a newer registration for the same info hash.

### Design

- Bound the outgoing DHT datagram queue at 256 records. Locally initiated
  requests await a reserved slot before allocating a transaction ID or
  registering an in-flight request. UDP replies and untracked announces remain
  best-effort and are dropped when the queue is full, because awaiting them in
  the receive path would stop reading the socket.
- Bound each recursive-node queue at 256 records and run at most 32 recursive
  requests per lookup worker. Newly discovered nodes are best-effort under
  saturation; DHT traversal already tolerates packet loss and periodic root
  queries retry from the routing table.
- Bound bucket-refresh and questionable-node ping queues at 256 records and
  their active work at 32 requests per worker. Bound concurrent bootstrap
  hostname attempts at eight. This closes the maintenance and startup paths
  instead of limiting only foreground peer discovery.
- Bound the delivered-peer queue at 256 records and await downstream capacity.
  This applies real backpressure between recursive responses and the consumer
  instead of moving the backlog into another queue.
- Reuse each recursive request's response for callbacks and traversal. The
  previous control flow sent the same request a second time after processing
  the first response's callback, doubling DHT traffic and discarding half the
  returned peer/node data.
- Replace the metadata resolver's semaphore-plus-unbounded-futures shape with
  at most 128 structurally active futures and at most 4,096 retained unique
  candidates. Reaching the candidate ceiling stops polling discovery, finishes
  active attempts, logs the limit, and returns the existing exhausted-input
  result. No public enum variant is added.
- On current main, bound each LSD result stream at 256 peers. A full or closed
  local queue drops that peer result but does not suppress the protocol reply.
  Each stream owns and cancels its periodic-task token, and a monotonic
  registration ID prevents an older stream's destructor from deleting its
  replacement.

These are preliminary fixed limits. They make every affected retained set
finite without adding public configuration before maintainers choose which
budgets, if any, should be caller-controlled.

### Tests

The DHT proof fills the 256-record datagram, peer, recursive-node, and
maintenance queues, verifies the next operation blocks or reports full as
designed, drains one record, and verifies capacity reopens. It also pins both
31/32 active-work boundaries and the eight-bootstrap limit.

The metadata proof admits exactly 4,096 unique peers, preserves duplicate
classification at the limit, rejects the next unique address, and pins the
127/128 active-work boundary.

The current-main LSD proof fills exactly 256 results and rejects the next. It
then proves a stale stream teardown cancels its own task without removing a
newer registration, while current-stream teardown cancels and removes its own
registration.

All affected crate suites and `cargo check --workspace` pass on exact rqbit
8.1.1 and the documented current-main source line.

### Negative controls

The DHT proof was run after intentionally changing the recursive admission
comparison from `< 32` to `<= 32`. It failed at the exact full-window
assertion:

```text
assertion failed: !can_schedule_recursive_request(MAX_CONCURRENT_RECURSIVE_REQUESTS)
```

The same proof was run after independently changing the maintenance admission
comparison from `< 32` to `<= 32`. It failed at its full-window assertion:

```text
assertion failed: !can_schedule_maintenance_request(MAX_CONCURRENT_MAINTENANCE_REQUESTS)
```

The LSD proof was separately run after intentionally removing the registration
ID comparison. The stale-stream check failed because the replacement had been
deleted:

```text
called `Option::unwrap()` on a `None` value
```

Neither mutation is retained.

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit-dht --lib \
  dht::queue_budget_tests::queue_budgets_close_at_exact_boundaries -- --exact
cargo test -p librqbit --lib \
  dht_utils::tests::metadata_peer_budgets_close_at_exact_boundaries -- --exact
cargo test -p librqbit-lsd --lib \
  queue_budget_tests::result_queue_and_registration_lifecycle_are_bounded \
  -- --exact
cargo check --workspace
```

The LSD command applies only to current main.

### AI disclosure

This contribution was prepared with AI assistance. I reviewed and edited the
design, implementation, tests, and this description, and I can explain and
maintain the resulting code. Replace this statement if it does not truthfully
describe the final human review.

## Human review checklist

- Confirm 256 outgoing datagrams, 256 pending nodes, 256 delivered peers, and
  32 active recursive requests are appropriate per lookup/address-family
  budgets. Separately confirm 256 queued and 32 active maintenance requests
  plus eight concurrent bootstrap attempts.
- Confirm best-effort drop is appropriate for UDP replies, untracked announce
  messages, and excess recursive nodes, while locally initiated requests and
  delivered peers should await capacity.
- Confirm reserving worker capacity before in-flight registration is the
  correct cancellation boundary.
- Decide whether the 128 active metadata attempts and 4,096 retained candidate
  ceiling should remain fixed, become options, or derive from another peer
  policy.
- Confirm exhausting the candidate ceiling should preserve the existing public
  exhausted-input result rather than introduce a breaking enum variant.
- On current main, confirm LSD should continue replying when its local result
  queue is saturated and should drop only the local peer observation.
- Verify stream replacement and drop behavior for two simultaneous LSD
  streams with the same info hash.
- Re-run rqbit's complete contributor gates on a fresh current-main branch,
  not only the focused verifier packaged by nzbd.

## Out of scope

- changing DHT traversal depth, routing-table policy, or packet retry rules;
- making fixed discovery limits part of rqbit's public configuration surface;
- deduplicating delivered DHT or LSD peer addresses beyond existing behavior;
- changing live-peer, retained-peer, handshake, tracker, or peer-response
  budgets covered by independent contribution candidates; and
- enabling nzbd's production BitTorrent path before every M0 gate passes.
