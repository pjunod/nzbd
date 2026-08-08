# Draft rqbit PR — bound remote-triggered peer responses through the writer

**Status:** human review required; do not submit verbatim · **Target:** rqbit
main · **Stable evidence:** exact v8.1.1 backport · **Prepared:** 2026-08-07

This draft accompanies
[`0013-bound-peer-response-backlog-main.patch`](0013-bound-peer-response-backlog-main.patch).
The matching [`0012`](0012-bound-peer-response-backlog.patch) backport proves
the same behavior on rqbit 8.1.1; it is evidence, not the proposed upstream
base.

> **Submission stop:** rqbit's `AI_POLICY.md` requires disclosure plus human
> review and editing. Before posting, read both peer-response paths, choose the
> final backlog limit with the maintainer, edit this text in your own words,
> and be prepared to explain the permit lifetime without an AI tool.

## Suggested title

Bound queued piece and metadata responses per peer

## Suggested PR body

### Problem

One peer can send valid BEP 3 piece requests faster than the rate-limited
upload scheduler and its socket writer can drain them. The scheduler channel
and the per-peer writer channel are both unbounded, and the scheduler can move
requests into the writer while that writer is blocked on a slow remote socket.
A read/write timeout limits one socket operation's lifetime but does not bound
the number of queued response records.

The peer can independently flood valid BEP 9 metadata requests. Those
responses bypass the upload scheduler and enter the same unbounded writer
queue directly. A piece-only scheduler cap would therefore leave another
remote-triggered growth path open.

### Design

- Give each live peer a private 128-permit response budget. The value matches
  rqbit's existing per-peer download request window, and advertise that value
  as BEP 10 `reqq` in the extended handshake.
- Acquire response admission without awaiting after a piece or metadata
  request passes validation. A conforming peer stays inside the advertised
  window. A peer that submits a 129th outstanding response request is
  disconnected immediately, so its socket reader can never be parked behind
  the torrent-global rate-limited upload FIFO.
- Carry the permit with a piece response through the torrent upload scheduler,
  the per-peer writer queue, payload read, and socket write.
- Carry the same kind of permit with a BEP 9 data response from request
  handling through the writer and socket write.
- Release permits automatically when a queue closes, a response errors, or the
  writer completes the send. No counter repair path is required.

The implementation channels remain unbounded for locally generated control
messages. This change bounds the remotely repeatable piece and metadata
responses that can accumulate on those channels; it does not claim that every
possible internal producer shares this budget.

### Tests

The admission proof calls the same production functions as piece request
handling, the upload scheduler, and metadata response handling. It fills all
128 permits in the real scheduler-message shape and verifies the 129th request
is rejected without awaiting. It then forwards one response through the real
writer-message constructor and proves the move does not free capacity.
Dropping that writer item admits one BEP 9 response, which again holds the
final permit until the item is dropped. The same proof checks the advertised
`reqq` value.

A separate writer proof calls the production socket-write helper with an
intentionally blocked socket. The permit remains unavailable while the write
is pending and reopens only when the write future is cancelled.

The same test and formatting gate pass on exact rqbit 8.1.1 and the documented
current-main source line.

### Negative control

The proofs were run in a disposable exact-8.1.1 tree after intentionally
bypassing production admission and, separately, dropping the writer permit
before the socket write. Both named proofs failed at the production boundary;
neither mutation is retained.

```text
assertion failed: enqueue_piece_response(...).is_err()
assertion failed: budget.clone().try_acquire_owned().is_err()
```

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit --lib \
  torrent_state::live::peer_response_budget_tests::production_admission_spans_scheduler_and_writer_queues \
  -- --exact
cargo test -p librqbit --lib \
  peer_connection::peer_response_writer_tests::production_writer_holds_permit_until_socket_write_finishes \
  -- --exact
cargo check --workspace --exclude rqbit-desktop
```

The headless verifier excludes only `rqbit-desktop`, whose Linux build needs
host WebKit/GTK development packages. The changed libraries, CLI, examples,
and tests remain inside the workspace check.

### AI disclosure

This contribution was prepared with AI assistance. I reviewed and edited the
design, implementation, tests, and this description, and I can explain and
maintain the resulting code. Replace this statement if it does not truthfully
describe the final human review.

## Human review checklist

- Confirm 128 is the intended response window, rather than a configurable
  option or a smaller value derived from the peer's advertised request queue.
- Confirm advertising `reqq = 128` and disconnecting a peer that exceeds that
  outstanding-response window is preferable to ever blocking the socket
  reader behind torrent-global upload pressure.
- Verify the permit remains alive through both the scheduler-to-writer handoff
  and the final socket write, including every error and closed-channel path.
- Confirm BEP 9 data responses belong in the same budget as payload piece
  responses; they share the attacker and writer boundary but not rate limits.
- Decide whether cancel-message support should be added separately. This
  candidate preserves rqbit's current behavior of ignoring cancels; the finite
  budget prevents ignored cancels from becoming unbounded memory growth.
- Re-run rqbit's complete contributor gates on a fresh current-main branch,
  not only the focused verifier packaged by nzbd.

## Out of scope

- making every internal writer message use this permit;
- changing upload rate-limit accounting or BEP 3 request validation;
- implementing BEP 3 cancel removal from queued responses;
- changing peer selection, retry, or choking behavior;
- bounding DHT queues, which require a separate algorithm-level audit; and
- enabling nzbd's production BitTorrent path before all M0 gates pass.
