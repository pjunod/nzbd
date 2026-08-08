# rqbit contribution kit — upstream required APIs and resource budgets without weakening nzbd

**Status:** ready for human review, not submitted · **Upstream base:**
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` · **Verified:** 2026-08-07

Companion to
[BITTORRENT_M0_REPORT.md](../../docs/BITTORRENT_M0_REPORT.md) (why nzbd is
blocked) — this directory is the submission package for the two rqbit APIs,
the pre-allocation magnet-metadata ceiling, tracker request controls,
live/retained-peer budgets, pre-routing handshake boundaries, and established
peer-response plus discovery-pipeline backlog controls that must be accepted
and released before nzbd starts M2.

Read and review this kit in order. Submit the discovery-health design issue
before its implementation, because that patch crosses three rqbit crates and
the maintainer should shape the public contract before reviewing the large
current-main candidate. The smaller authoritative-restore and metadata-size
changes can move independently. No submission makes an nzbd production gate
pass until an accepted stable rqbit release contains the required contracts
and the full M0 path is rerun.

## 1. Human submission boundary — rqbit requires more than disclosure

rqbit's current [`AI_POLICY.md`](https://github.com/ikatson/rqbit/blob/main/AI_POLICY.md)
incorporates Ghostty's policy with project-specific amendments. It requires
all AI assistance to be disclosed, and it requires a human to review and edit
AI-assisted issues, discussions, and code before submission. The human must be
able to explain the contribution without an AI tool.

Before posting any draft:

1. Read the affected rqbit modules and every added test. This is the proof that
   the human owns the behavior, not a ceremonial checkbox.
2. Edit the draft for the final upstream shape. The bolded disclosure note
   deliberately prevents an unreviewed copy-paste.
3. Re-run the matching verifier against the exact upstream head. A clean run
   from an older base is evidence of the design, not evidence that the current
   branch still applies.
4. Demonstrate every new guard's sensitivity once: temporarily remove or
   bypass the guard, run its exact proof, and paste the named failure into the
   human-authored upstream PR before restoring the clean tree. A green test is
   evidence only after the dangerous mutation is known to make it red.
5. Keep independent contributions separate. Restore changes session
   admission; discovery health changes DHT, tracker, and public snapshot
   contracts; the metadata ceiling changes the peer handshake's allocation
   boundary; tracker, live-peer, retained-peer, and pending-handshake budgets
   change distinct runtime resource policies; the peer-response budget bounds
   remote work after a live connection is established; the discovery-pressure
   budget bounds DHT/LSD foreground, maintenance, and bootstrap work plus
   metadata candidates before peer admission.

## 2. Contribution map — stable evidence and current-main submissions

| Contract | Stable 8.1.1 evidence | rqbit-main submission | Review artifact |
|---|---|---|---|
| Skip implicit restore while retaining persistence | [`0001`](0001-allow-persistence-without-auto-restore.patch) | [`0002`](0002-allow-persistence-without-auto-restore-main.patch) | [PR draft](AUTHORITATIVE_RESTORE_PR.md) |
| Credential-safe per-torrent tracker and DHT health | [`0003`](0003-expose-per-torrent-discovery-health.patch) | [`0004`](0004-expose-per-torrent-discovery-health-main.patch) | [design-issue draft](DISCOVERY_HEALTH_ISSUE.md) |
| Bounded tracker requests and announce intervals | [`0005`](0005-bound-tracker-requests.patch) | [`0006`](0006-bound-tracker-requests-main.patch) | [PR draft](TRACKER_REQUEST_BUDGET_PR.md) |
| Per-torrent and shared-session live-peer budgets | [`0007`](0007-bound-session-peers.patch) | [`0008`](0008-bound-session-peers-main.patch) | [PR draft](SESSION_PEER_BUDGET_PR.md) |
| Bounded incomplete incoming handshakes | [`0009`](0009-bound-pending-incoming-handshakes.patch) | Native configurable 256-check boundary; no patch | [review note](PENDING_HANDSHAKE_BUDGET.md) |
| Per-torrent and shared-session retained-peer budgets | [`0010`](0010-bound-known-peer-records.patch) | [`0011`](0011-bound-known-peer-records-main.patch) | [PR draft](KNOWN_PEER_BUDGET_PR.md) |
| Bounded per-peer payload and metadata response backlog | [`0012`](0012-bound-peer-response-backlog.patch) | [`0013`](0013-bound-peer-response-backlog-main.patch) | [PR draft](PEER_RESPONSE_BUDGET_PR.md) |
| Bounded DHT, metadata-resolution, maintenance, bootstrap, and LSD pressure | [`0014`](0014-bound-discovery-pressure.patch) | [`0015`](0015-bound-discovery-pressure-main.patch) | [PR draft](DISCOVERY_PRESSURE_BUDGET_PR.md) |
| Bound BEP 9 metadata before allocation | [`0005`](0005-limit-peer-metadata-before-allocation.patch) | [`0006`](0006-limit-peer-metadata-before-allocation-main.patch) | [PR draft](METADATA_SIZE_LIMIT_PR.md) |

The stable patches preserve the exact experiments behind nzbd's M0 report.
The main patches are the contribution candidates. Do not submit the stable
backports upstream unless the maintainer explicitly asks for an 8.x backport.

## 3. Submission order — ask for the large API shape before sending code

1. Post the [discovery-health design issue](DISCOVERY_HEALTH_ISSUE.md) after
   human review and editing.
2. Submit the independent
   [authoritative-restore PR](AUTHORITATIVE_RESTORE_PR.md). Its default behavior
   is unchanged and its focused test demonstrates the ownership seam while
   reporting only the persisted 16 KiB even though the complete valid payload
   is on disk.
3. Submit the independent
   [metadata-size PR](METADATA_SIZE_LIMIT_PR.md). Its default remains 32 MiB,
   while an embedding caller can reject a smaller configured ceiling before
   rqbit allocates or requests peer metadata.
4. Reconcile maintainer feedback on the discovery-health states, crate
   boundary, and snapshot shape.
5. Rework and submit `0004` only after the design direction is accepted. A
   smaller upstream implementation is preferable if it preserves honest
   per-torrent DHT/tracker state and credential-safe failures.
6. Submit the independent tracker request-budget candidate only after human
   review of its 30-second request deadline, 1 MiB response cap, and 60-second
   minimum unforced announce interval. The floor deliberately delays a
   legitimate 10-second tracker request to 60 seconds; it is a policy tradeoff,
   not just an implementation bound, and does not depend on either public API.
7. Submit the independent session peer-budget candidate only after human
   review of the public option name, aggregate counting boundary, and permit
   acquisition order. It does not depend on the tracker request controls.
8. Review the independent pending-handshake boundary. Do not submit `0009` to
   rqbit main, which already has the configurable equivalent. Use the patch
   only if the maintainer explicitly requests an 8.x backport.
9. Submit the independent retained-peer candidate only after human review of
   its option names, permit lifetime, cleanup behavior, and preliminary
   1,024/4,096 policy.
10. Submit the independent peer-response candidate only after human review of
    its 128-response window, backpressure policy, and permit lifetime across
    both scheduler and writer queues.
11. Submit the discovery-pressure candidate only after human review of its
    DHT/LSD overload policy, foreground and maintenance concurrency, bootstrap
    fan-out, metadata candidate ceiling, and the current-main-only LSD
    lifecycle change.
12. After both required APIs, the allocation ceiling, and all accepted resource
    controls ship, pin that stable release in nzbd and rerun all eleven M0
    gates on native macOS, Linux glibc/musl, and Windows.
13. Run the Linux packet-capture private-mode harness and obtain reviewer
    acceptance of the resource, package, license, and advisory dispositions.
    A gate rerun cannot discharge those review decisions by itself.
14. Only the complete M0 path can authorize M2.

## 4. Reproduction — verify all candidates against rqbit main

The verifier accepts the documented main base or any descendant that still
contains it. On drift, it permits a three-way apply and then runs the affected
tests; a conflict or failing test is a stop, not permission to hand-wave the
patch forward. Pull requests run both stable and current-main legs. Every job
name begins with `blocking:` or `drift:`: stable failures block the PR, while
current-main drift remains visible but non-blocking because upstream can move
independently between nzbd changes. Pushes and the weekly schedule require
every leg.

```bash
rqbit_tree=/tmp/rqbit-upstream-main
git clone https://github.com/ikatson/rqbit.git "$rqbit_tree"          # Get full history for ancestry checks.
git -C "$rqbit_tree" checkout 4e5f94cbcf1d57ec500885c77cf1e24d70232d89

scripts/check-rqbit-authoritative-restore-patch.sh "$rqbit_tree"      # Format + focused librqbit test.
scripts/check-rqbit-discovery-health-patch.sh "$rqbit_tree"          # Format + three affected crate suites.
scripts/check-rqbit-tracker-request-budget-patch.sh "$rqbit_tree"    # Format + tracker response/interval suite.
scripts/check-rqbit-session-peer-budget-patch.sh "$rqbit_tree"       # Format + exact local/shared permit tests.
scripts/check-rqbit-pending-handshake-budget.sh "$rqbit_tree"        # Stable backport or current native boundary.
scripts/check-rqbit-known-peer-budget-patch.sh "$rqbit_tree"         # Format + retained-record admission tests.
scripts/check-rqbit-metadata-size-limit-patch.sh "$rqbit_tree"       # Format + pre-allocation limit test.
scripts/check-rqbit-peer-response-budget-patch.sh "$rqbit_tree"      # Format + scheduler/writer lifetime proof.
scripts/check-rqbit-discovery-pressure-patch.sh "$rqbit_tree"        # Format + DHT/metadata/LSD queue proofs.
```

For submission, apply only the matching main patch to a fresh branch and run
rqbit's own documented gates:

```bash
git -C "$rqbit_tree" switch -c nzbd/authoritative-restore
git -C "$rqbit_tree" apply "$PWD/contrib/rqbit/0002-allow-persistence-without-auto-restore-main.patch"
(
  cd "$rqbit_tree"
  cargo fmt --all -- --check                                           # Required rqbit formatting gate.
  cargo check --workspace                                              # Compile every workspace member.
  cargo clippy --all-targets                                           # Follow rqbit's contributor instructions.
  cargo test --workspace                                               # Run more than the focused nzbd verifier.
)
```

Use separate fresh branches for discovery health, the metadata ceiling, the
tracker request budget, the session peer budget, the retained-peer budget, and
the peer-response and discovery-pressure budgets. Do not stack the upstream
submissions: the contracts solve different problems, and independent history
lets rqbit accept, revise, or reject each one without dragging the others
through review. There is no current-main patch for the
pending-handshake boundary because rqbit main already implements it; keep the
stable-only backport separate from every main submission.

## 5. Non-goals — contribution evidence is not production permission

- Do not point nzbd at a fork or Git dependency. That would trade two explicit
  gates for permanent private-fork maintenance.
- Do not expose raw tracker URLs or error bodies. Paths, queries, user info,
  and passkeys are secrets even when the URL came from public metainfo.
- Do not infer discovery health from peer count. A quiet healthy swarm and a
  failed tracker are different states with different operator actions.
- Do not treat nzbd's admitted tracker-count limit as a response budget. The
  engine must still bound every tracker request, body, and unforced interval.
- Do not treat the explicit/bootstrap peer-input limit as a live-peer budget.
  Incoming connections and later tracker, PEX, and DHT discoveries must share
  the same aggregate session permits.
- Do not treat a live-peer permit as a pending-handshake budget. Stable 8.1.1
  accepts sockets before torrent routing; that earlier work needs its own
  reviewed ceiling.
- Do not treat a live-peer permit as a retained-peer budget. Queued, backoff,
  dead, and not-needed records remain outside the live-manager semaphore and
  need per-torrent plus shared-session ceilings of their own.
- Do not treat upload rate limiting or a socket timeout as a response-backlog
  bound. A remote peer can pipeline piece and metadata requests through the
  scheduler and writer faster than those stages drain.
- Do not treat bounded live or retained peers as bounds on discovery work.
  DHT datagrams, recursive nodes, maintenance/bootstraps, delivered peers,
  metadata candidates, and current-main LSD streams exist before or beside
  those admission permits.
- Do not enable nzbd config, admission, listeners, trackers, DHT, or payload
  I/O from this kit. M2 remains blocked until a stable release passes M0.
