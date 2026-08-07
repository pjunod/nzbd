# rqbit contribution kit — upstream required APIs and network budgets without weakening nzbd

**Status:** ready for human review, not submitted · **Upstream base:**
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` · **Verified:** 2026-08-07

Companion to
[BITTORRENT_M0_REPORT.md](../../docs/BITTORRENT_M0_REPORT.md) (why nzbd is
blocked) — this directory is the submission package for the two rqbit APIs,
tracker request controls, live-peer budgets, and pre-routing handshake
boundaries that must be accepted and released before nzbd starts M2.

Read and review this kit in order. Submit the discovery-health design issue
before its implementation, because that patch crosses three rqbit crates and
the maintainer should shape the public contract before reviewing the large
current-main candidate. The smaller authoritative-restore change can move
independently. Neither submission makes an nzbd production gate pass until an
accepted stable rqbit release contains both contracts.

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
4. Keep independent contributions separate. Restore changes session
   admission; discovery health changes DHT, tracker, and public snapshot
   contracts; tracker, peer, and pending-handshake budgets change distinct
   runtime resource policies.

## 2. Contribution map — stable evidence and current-main submissions

| Contract | Stable 8.1.1 evidence | rqbit-main submission | Review artifact |
|---|---|---|---|
| Skip implicit restore while retaining persistence | [`0001`](0001-allow-persistence-without-auto-restore.patch) | [`0002`](0002-allow-persistence-without-auto-restore-main.patch) | [PR draft](AUTHORITATIVE_RESTORE_PR.md) |
| Credential-safe per-torrent tracker and DHT health | [`0003`](0003-expose-per-torrent-discovery-health.patch) | [`0004`](0004-expose-per-torrent-discovery-health-main.patch) | [design-issue draft](DISCOVERY_HEALTH_ISSUE.md) |
| Bounded tracker requests and announce intervals | [`0005`](0005-bound-tracker-requests.patch) | [`0006`](0006-bound-tracker-requests-main.patch) | [PR draft](TRACKER_REQUEST_BUDGET_PR.md) |
| Per-torrent and shared-session live-peer budgets | [`0007`](0007-bound-session-peers.patch) | [`0008`](0008-bound-session-peers-main.patch) | [PR draft](SESSION_PEER_BUDGET_PR.md) |
| Bounded incomplete incoming handshakes | [`0009`](0009-bound-pending-incoming-handshakes.patch) | Native configurable 256-check boundary; no patch | [review note](PENDING_HANDSHAKE_BUDGET.md) |

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
3. Reconcile maintainer feedback on the discovery-health states, crate
   boundary, and snapshot shape.
4. Rework and submit `0004` only after the design direction is accepted. A
   smaller upstream implementation is preferable if it preserves honest
   per-torrent DHT/tracker state and credential-safe failures.
5. Submit the independent tracker request-budget candidate only after human
   review of its 30-second request deadline, 1 MiB response cap, and 60-second
   minimum unforced announce interval. It does not depend on either public API.
6. Submit the independent session peer-budget candidate only after human
   review of the public option name, aggregate counting boundary, and permit
   acquisition order. It does not depend on the tracker request controls.
7. Review the independent pending-handshake boundary. Do not submit `0009` to
   rqbit main, which already has the configurable equivalent. Use the patch
   only if the maintainer explicitly requests an 8.x backport.
8. After both required APIs and the accepted resource controls ship, pin that
   stable release in nzbd and rerun all eleven
   M0 gates on native macOS, Linux glibc/musl, and Windows.
9. Run the Linux packet-capture private-mode harness and obtain reviewer
   acceptance of the resource, package, license, and advisory dispositions.
   A gate rerun cannot discharge those review decisions by itself.
10. Only the complete M0 path can authorize M2.

## 4. Reproduction — verify the candidates against rqbit main

The verifier accepts the documented main base or any descendant that still
contains it. On drift, it permits a three-way apply and then runs the affected
tests; a conflict or failing test is a stop, not permission to hand-wave the
patch forward. Pull requests run both stable and current-main legs. Stable
failures block the PR; current-main failures remain visible but non-blocking
because upstream can move independently between nzbd changes. Pushes and the
weekly schedule require every leg.

```bash
rqbit_tree=/tmp/rqbit-upstream-main
git clone https://github.com/ikatson/rqbit.git "$rqbit_tree"          # Get full history for ancestry checks.
git -C "$rqbit_tree" checkout 4e5f94cbcf1d57ec500885c77cf1e24d70232d89

scripts/check-rqbit-authoritative-restore-patch.sh "$rqbit_tree"      # Format + focused librqbit test.
scripts/check-rqbit-discovery-health-patch.sh "$rqbit_tree"          # Format + three affected crate suites.
scripts/check-rqbit-tracker-request-budget-patch.sh "$rqbit_tree"    # Format + tracker response/interval suite.
scripts/check-rqbit-session-peer-budget-patch.sh "$rqbit_tree"       # Format + exact local/shared permit tests.
scripts/check-rqbit-pending-handshake-budget.sh "$rqbit_tree"        # Stable backport or current native boundary.
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

Use a separate fresh branch for discovery health. Do not stack it on restore:
the APIs solve different problems, and independent history lets rqbit accept,
revise, or reject either contract without dragging the other through review.
Use a third independent branch for the tracker request budget for the same
reason; it changes resource policy, not either public API. Use a fourth branch
for the session peer budget so rqbit can accept or revise tracker and peer
resource policy independently. There is no fifth current-main patch for the
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
- Do not enable nzbd config, admission, listeners, trackers, DHT, or payload
  I/O from this kit. M2 remains blocked until a stable release passes M0.
