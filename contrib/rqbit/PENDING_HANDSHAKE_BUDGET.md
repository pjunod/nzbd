# rqbit review note — bound incomplete incoming handshakes

**Status:** human review required; do not submit verbatim · **Stable target:**
rqbit 8.1.1 only if its maintainer requests a backport · **Current main:** already
has a configurable 256-check boundary · **Prepared:** 2026-08-07

This note accompanies
[`0009-bound-pending-incoming-handshakes.patch`](0009-bound-pending-incoming-handshakes.patch).
The patch is exact 8.1.1 evidence, not a proposal to replace rqbit main's
newer listener API.

> **Submission stop:** rqbit's `AI_POLICY.md` requires disclosure plus human
> review and editing. Do not post this patch to rqbit main. If the maintainer
> explicitly requests an 8.x backport, first read the complete listener and
> handshake path, edit the change and description in your own words, and be
> prepared to explain the scheduling and resource boundary without an AI
> tool.

## Problem — live-peer limits start too late

rqbit 8.1.1 accepts every incoming TCP socket into a `FuturesUnordered` set
before it knows the torrent or can acquire a live-peer permit. Each incomplete
client can retain a socket, buffers, and a handshake task until the 10-second
read timeout. The listener has no ceiling on that pending set, so neither a
per-torrent peer limit nor a shared session peer semaphore bounds this
pre-routing work.

Current rqbit main already addresses this class of problem with
`ListenerOptions::max_pending_incoming_handshake_checks`, whose default is
256. Its listener stops accepting while its pending set is at that boundary
and resumes as checks finish. The stable patch carries only the equivalent
fixed ceiling into 8.1.1; it does not add a new stable public option.

## Stable backport shape

- Define one exact 256-check ceiling beside the stable session listener.
- Route the production listener through one accept helper that becomes
  pending while the handshake set is full. Completed or failed checks continue
  to be polled, so the next loop iteration can reopen capacity.
- Preserve the existing 10-second handshake read timeout and all post-routing
  peer behavior.
- Use a real loopback socket to prove a queued connection is not accepted at
  256 and is accepted after the pending count returns to 255.

The value is preliminary policy evidence. It becomes an nzbd release boundary
only after human acceptance and inclusion in an accepted stable rqbit release.
The first nzbd BitTorrent release remains TCP/IPv4-only, so the reviewed
budget is one TCP listener. Current main applies its value independently to
TCP and uTP when both are enabled; reviewers must not silently interpret 256
as a combined two-listener total.

## Verification

```text
cargo fmt --all -- --check
cargo test -p librqbit --lib \
  session::tests::pending_handshake_budget_blocks_and_resumes_listener_accepts \
  -- --exact
```

The verifier first requires that exact test name to appear in Cargo's test
list, preventing a renamed or missing test from reporting success with zero
tests. It applies the patch only to exact rqbit 8.1.1. Against the
documented rqbit-main source line it applies no patch, checks the native
default, public option, and guarded listener, and compiles all librqbit
targets.

## Human review checklist

- Decide whether 256 simultaneous incomplete handshakes is an acceptable
  preliminary TCP-listener ceiling for nzbd's measured first-release scope.
- Confirm that pausing `accept()` at the boundary gives the intended kernel
  backlog behavior under load.
- Keep this boundary distinct from the per-torrent and session-total live-peer
  budgets; both are required because they cover different lifecycle stages.
- Confirm that current main's limit is per listener and decide separately
  whether a future TCP+uTP release needs one shared aggregate ceiling.
- Reject zero in any future nzbd mapping to current main's public `usize`
  option. A zero value disables the guarded accept branch and is not nzbd's
  spelling for “unlimited.”
- Re-run rqbit's complete contributor gates on a fresh source tree if an 8.x
  backport is actually requested.

## Out of scope

- per-IP connection quotas, SYN-flood protection, or kernel backlog tuning;
- live resizing of the pending-handshake limit;
- known/discovered peer-address retention and tracker/DHT/PEX fan-out;
- changing the 10-second handshake timeout; and
- enabling an nzbd listener or any production BitTorrent path.
