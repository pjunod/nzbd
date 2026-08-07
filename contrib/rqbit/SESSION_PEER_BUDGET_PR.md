# Draft rqbit PR — bound live peers per torrent and across a session

**Status:** human review required; do not submit verbatim · **Target:** rqbit
main · **Stable evidence:** exact v8.1.1 backport · **Prepared:** 2026-08-07

This draft accompanies [`0008-bound-session-peers-main.patch`](0008-bound-session-peers-main.patch).
The matching [`0007`](0007-bound-session-peers.patch) backport proves the
same behavior on rqbit 8.1.1; it is evidence, not the proposed upstream base.

> **Submission stop:** rqbit's `AI_POLICY.md` requires disclosure plus human
> review and editing. Before posting, read every changed peer lifecycle path,
> choose the final public option name with the maintainer, edit this text in
> your own words, and be prepared to explain the permit ordering and release
> behavior without an AI tool.

## Suggested title

Bound live peers across an entire session

## Suggested PR body

### Problem

`peer_limit` bounds one torrent, but a session managing many torrents has no
corresponding aggregate ceiling. Every torrent owns a separate semaphore, so
the maximum number of live peer tasks grows linearly with the torrent count.
Incoming peers and peers discovered through trackers, PEX, or DHT all need to
participate in the same session budget; limiting only an initial peer list does
not control the runtime resource.

Stable 8.1.1 additionally hard-codes the per-torrent limit to 128. The stable
evidence patch adds the current-main per-session/per-add `peer_limit` shape so
both source lines can exercise the same aggregate design.

### Design

- Add optional `SessionOptions::peer_limit_total`. `None` preserves current
  behavior; a value creates one semaphore shared by every torrent in the
  session.
- Represent one live peer with a private RAII permit that holds both its
  torrent-local permit and, when configured, its session permit until the peer
  manager exits.
- Make incoming peers try both limits without waiting and retain the existing
  `ConcurrencyLimitReached` result when either ceiling is full.
- Make outgoing peers await both limits before spawning their manager. The
  per-torrent permit is acquired first, and the single peer-adder task can hold
  at most one such permit while waiting for a session slot.
- Keep the existing 128-per-torrent fallback when no per-torrent limit is
  configured.

The option is construction-time only. This change does not add live resizing,
change known-peer retention, or claim to bound handshake work before an
incoming connection has been routed to a torrent.

### Tests

- the per-torrent boundary admits exactly two permits, rejects the third, and
  admits again after release;
- two independent torrent pools share one three-permit session pool, reject
  the fourth peer, and admit it immediately after a permit is released;
- an awaited outgoing acquisition remains blocked while the shared permit is
  held and resumes after that combined permit is dropped; and
- the focused tests and formatting pass on exact 8.1.1 with Rust 1.85 and on
  the documented current-main source line.

### Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit --lib peer_semaphore_tests
```

### AI disclosure

This contribution was prepared with AI assistance. I reviewed and edited the
design, implementation, tests, and this description, and I can explain and
maintain the resulting code. Replace this statement if it does not truthfully
describe the final human review.

## Human review checklist

- Confirm `peer_limit_total` is the desired public name and that zero should
  intentionally disable live peers rather than mean “unlimited.”
- Confirm that the session ceiling should count routed live peer managers, not
  pending listener handshakes or retained known-peer addresses.
- Review the per-torrent-then-session acquisition order. One peer-adder task
  per torrent can reserve at most one local permit while waiting globally.
- Verify both incoming and outgoing manager paths hold the combined permit
  until all connection resources are released.
- Decide whether aggregate-limit rejections need a distinct public statistic;
  this candidate preserves the existing limit result and debug message.
- Re-run rqbit's complete contributor gates on a fresh current-main branch,
  not only the focused verifier packaged by nzbd.

## Out of scope

- dynamic peer-limit changes after session construction;
- a cap on known/discovered peer addresses or pending handshake checks;
- per-IP, per-tracker, or protocol-specific peer quotas;
- changing retry/backoff, peer selection, or discovery behavior; and
- enabling nzbd's production BitTorrent path before all M0 gates pass.
