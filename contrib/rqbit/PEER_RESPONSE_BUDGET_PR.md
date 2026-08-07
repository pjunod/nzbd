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
  rqbit's existing per-peer download request window instead of introducing a
  second unexplained pipeline size.
- Await a permit after a piece or metadata request has passed its existing
  validation and before its response enters an asynchronous queue. A full
  window backpressures that peer's reader and lets TCP flow control bound
  additional input; it does not disconnect a conforming peer for temporary
  upload pressure.
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

The exact proof fills all 128 permits in the scheduler queue and verifies the
129th acquisition is pending. It then moves one response into the peer writer
and verifies that the move does not free capacity. Dropping the writer item
admits one BEP 9 response, which again holds the final permit until the writer
item is dropped.

The same test and formatting gate pass on exact rqbit 8.1.1 and the documented
current-main source line.

### Negative control

The proof was also run in a disposable exact-8.1.1 tree after intentionally
bypassing the guard by returning each acquired permit while adding a
replacement permit to the semaphore. The exact test failed immediately at the
full-window assertion:

```text
assertion failed: acquire_peer_response_permit(&budget).now_or_never().is_none()
```

That mutation was not retained. It demonstrates that the named test becomes
red when the admission ceiling no longer closes.

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit --lib \
  torrent_state::live::peer_response_budget_tests::peer_response_permit_spans_scheduler_and_writer_queues \
  -- --exact
```

### AI disclosure

This contribution was prepared with AI assistance. I reviewed and edited the
design, implementation, tests, and this description, and I can explain and
maintain the resulting code. Replace this statement if it does not truthfully
describe the final human review.

## Human review checklist

- Confirm 128 is the intended response window, rather than a configurable
  option or a smaller value derived from the peer's advertised request queue.
- Confirm backpressure is preferable to disconnecting a peer that exceeds the
  local response window.
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
