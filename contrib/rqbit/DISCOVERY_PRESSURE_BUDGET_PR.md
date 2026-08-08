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
  requests per lookup worker. The production future-window type itself rejects
  a 33rd active request, so deleting a select-loop polling guard cannot reopen
  the retained-work bound. Newly discovered nodes are best-effort under
  saturation; DHT traversal already tolerates packet loss and periodic root
  queries retry from the routing table. A saturated recursive queue uses the
  normal requery interval rather than being mistaken for an empty table and
  sorted once per second.
- Bound bucket-refresh and questionable-node ping queues at 256 records and
  their active work at 32 requests per worker using the same structural
  future-window type. Bootstrap fan-out is intentionally unchanged: the input
  is a finite configured list, while an eight-host `buffer_unordered` window
  can let one hostname's 24-hour retry budget starve every later hostname.
- Bound the delivered-peer queue at 256 records and make delivery best-effort.
  Recursive node traversal is processed first, so a slow metadata consumer
  cannot stall the DHT recursion that could discover replacement peers.
- Reuse each recursive request's response for callbacks and traversal. The
  previous control flow sent the same request a second time after processing
  the first response's callback, doubling DHT traffic and discarding half the
  returned peer/node data.
- Replace the metadata resolver's semaphore-plus-unbounded-futures shape with
  at most 128 structurally active futures plus 256 pending peer addresses.
  Continue polling while pending capacity exists. Retain at most 4,096 unique
  addresses for deduplication, but continue trying later untracked candidates
  after that memory ceiling; reaching it is never reported as an exhausted or
  closed discovery stream.
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
maintenance queues, verifies the next operation reports full, drains one
record, and verifies capacity reopens. It fills the actual recursive and
maintenance future-window types to 32 and proves the 33rd future is rejected.
It also proves a saturated root queue receives the normal requery delay.

The metadata proof fills the production resolver queues to 128 active and 256
pending peers and proves the next retained candidate is rejected. It also
admits exactly 4,096 tracked peers, preserves duplicate classification at the
limit, and classifies the next unique address as untracked rather than
terminal.

The current-main LSD proof fills exactly 256 results and rejects the next. It
then uses the production spawn/lifecycle helper to prove a stale stream
teardown does not remove a newer registration and current-stream teardown both
cancels its spawned task and removes its own registration.

All affected crate suites and the headless workspace check pass on exact rqbit
8.1.1 and the documented current-main source line. The verifier excludes only
`rqbit-desktop`, whose Linux build needs host WebKit/GTK development packages;
the changed libraries, CLI, examples, and tests remain inside the check.

### Negative controls

The DHT proof is run after intentionally bypassing the production future
window. The exact production-type assertion must fail:

```text
assertion failed: recursive.try_push(std::future::pending::<()>()).is_err()
```

The metadata proof is independently run after bypassing the production pending
queue limit. Its exact next-candidate assertion must fail:

```text
assertion failed: !queues.enqueue(...)
```

The LSD proof is separately run after detaching the spawned task from the
stream lifecycle token. The task-cancellation timeout must fail:

```text
announce task must observe stream cancellation: Elapsed(())
```

None of the mutations is retained.

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit-dht --lib \
  dht::queue_budget_tests::queue_budgets_close_at_exact_boundaries -- --exact
cargo test -p librqbit --lib \
  dht_utils::tests::production_metadata_queues_close_at_exact_boundaries -- --exact
cargo test -p librqbit-lsd --lib \
  queue_budget_tests::production_queue_and_announce_lifecycle_are_bounded \
  -- --exact
cargo check --workspace --exclude rqbit-desktop
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
  budgets. Separately confirm 256 queued and 32 active maintenance requests.
- Confirm best-effort drop is appropriate for UDP replies, untracked announce
  messages, excess recursive nodes, and delivered peers, while locally
  initiated DHT requests should await reserved capacity.
- Confirm reserving worker capacity before in-flight registration is the
  correct cancellation boundary.
- Decide whether 128 active metadata attempts, 256 pending candidates, and the
  4,096-entry deduplication ceiling should remain fixed, become options, or
  derive from another peer policy.
- Confirm candidates discovered after the deduplication ceiling should still
  be attempted without being retained, rather than producing a false
  exhausted-input result.
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
