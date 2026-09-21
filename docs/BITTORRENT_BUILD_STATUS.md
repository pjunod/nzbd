# BitTorrent build status — single-node release candidate

**Status:** runtime repair reviewed; qualification and merge tracked in PR #231 · **Updated:** 2026-09-20

Companion to [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md), which owns the
architecture and acceptance contract. This page records execution progress for
the consolidated implementation branch. It is not an operator guide and does
not authorize production use by itself.

## Magnet DHT correction — active 2026-09-20

Trackerless public magnet discovery is being corrected in a separate isolated
clone and batched pull request. The live implementation, review, and test
record is [BITTORRENT_MAGNET_DHT_STATUS.md](BITTORRENT_MAGNET_DHT_STATUS.md).
That record is authoritative for this follow-up: no earlier workflow or public
Ubuntu transfer proves the new list-only DHT resolution, private-result
rejection, timeout cancellation, or durable pending cleanup contracts.

## Runtime repair — Ubuntu transfer incident on nuc3

**Status:** implementation and review fixes complete · **Base:**
`main` `c77edbc` · **Updated:** 2026-09-20

[PR #231](https://github.com/pjunod/nzbd/pull/231) replaces the emergency uncommitted fixes with reviewed
commits. Work for the PR happens in an independent disposable clone. The
previous incident investigation changed local and nuc3 checkouts; those exact
changes will be reconciled after merge without overwriting other work.

| Workstream | State | Next observable result |
|---|---|---|
| Release DHT request dispatch | implemented | Reviewer verifies all four bounded request queues execute without debug assertions |
| Queue progress and rate display | implemented | Torrent bytes, selected files, rates, and aggregate totals agree |
| Startup and resume | implemented | Saved torrents restart; old activity timestamps cannot prevent discovery |
| Adversarial review | addressed | Fixed stalled-retry slot monopoly and missing CI dependency/type scope |
| Fast-lane qualification | [live checks](https://github.com/pjunod/nzbd/pull/231/checks) | Current-head `Main promotion gate` includes focused torrent regressions |
| Merge and cleanup | [live PR status](https://github.com/pjunod/nzbd/pull/231) | PR description records qualification and cleanup completion |

The Ubuntu ISO finished on nuc3: 6,482,409,472 bytes verified, phase `seeding`,
with no reported error. The adversarial review ran without tests. Its P1
finding is addressed by ranking expired automatic torrent retries behind fresh
work in both torrent and NNTP scheduling; retries still use spare capacity.
Startup and explicit resume receive a fresh discovery interval. A deterministic
regression covers repeated yield/resume cycles and shared NNTP ordering. The P2
finding is addressed by selecting torrent qualification for dependency, toolchain,
durable type, state, daemon, configuration, and scope-script changes.

### Decisions and validation boundaries

1. **One PR, focused commits.** Discovery, engine recovery/display, and CI
   qualification belong to one incident repair, so they ship together.
2. **Review before tests.** The incident investigation already ran tests and
   demonstrated 96,090,965 bytes/s with 57 peers on nuc3. Those observations
   are historical evidence, not qualification of this PR. No new tests run
   until the adversarial review and its fixes are complete.
3. **Bounded qualification.** The fast lane adds torrent engine regressions,
   startup/cluster-boundary checks, maintained-patch derivation, and DHT
   dispatch with debug assertions disabled. The full rqbit matrix remains
   available on schedule/manual dispatch; unrelated full-unit failures belong
   to the separate batch process. Earlier broad engine runs left three pool
   connection tests hanging; this PR does not claim to fix those tests.
4. **No new feature gate.** These are fixes to the enabled nzbd runtime. Plurx
   already has a Developer enablement section with advisory readiness; this
   repair does not introduce an additional enablement prerequisite or move
   nzbd configuration into Plurx.
5. **GitHub only.** Per the 2026-09-20 clarification, repository and PR work
   use `pjunod/nzbd` on GitHub. Forgejo is outside this task.

## Earlier implementation evidence

## Current position — reviewed and follow-up fast-lane green

| Workstream | State | Next observable result |
|---|---|---|
| M2e lifecycle and restart convergence | qualified | Reviewed restore behavior is preserved in the consolidated branch |
| M2f seed policy | qualified | Durable ratio/time accounting pauses at exact boundaries and retains payload |
| M2g bandwidth and quota accounting | qualified | Shared ceiling reallocates idle capacity; verified torrent bytes enter quota accounting |
| M2h enforcing disk guard | qualified | Incomplete torrents pause on guard/ENOSPC while completed seeds remain live |
| M2i terminal history | qualified | Confirmed payload outcome is durable before retry-safe mixed-protocol history retirement |
| M2j daemon activation | qualified | One configured session owns admission, recovery, watch ingestion, shutdown, and disabled-mode refusal |
| M3 native web/mobile surface | qualified | The dashboard exposes reviewed magnet, remote `.torrent`, and local `.torrent` intake with green UI harnesses |
| M4 Sonarr/Radarr compatibility | qualified | Web API 2.8.1 routes, Bearer/Basic/SID auth, per-IP login limiting, durable categories, projections, and mutations are wired |
| M5 release evidence | complete | Base evidence remains complete; the web-intake follow-up passed its one requested scoped fast lane |

## Working rules — one consolidated review and qualification pass

- Work happened in an isolated disposable clone; existing user checkouts
  remained untouched.
- Implementation commits are grouped by coherent subsystem. No broad test run
  occurs until the consolidated change is ready for adversarial review.
- After review findings are resolved, run the repository's fast-lane
  qualification once, fix failures, and merge only a green reviewed head.
- The Forgejo token can access `pjunod/ansible` and `noirr/plurx`, but no nzbd
  repository exists there. GitHub remains the source for this isolated clone;
  no Forgejo repository will be created without explicit authorization.

## Decisions applied during implementation

1. **Activation policy.** The explicit instruction to avoid feature gates wins
   over ADR-19's temporary dormant guard. `[torrent].enabled = true` now starts
   the supported single-node runtime; actual incompatibilities such as cluster
   mode remain startup validation errors rather than advisory conditions.
2. **Settings owner.** Runtime configuration remains in nzbd. A separate
   isolated plurx change will expose deployment readiness as advisory status;
   it will not control or override nzbd activation.
3. **Qualification scope.** The consolidated head will receive one adversarial
   review and one fast-lane test pass before merge. Full-suite failures remain
   outside this resource-constrained pass as explicitly requested.

## Activity log

| Date | Change | Evidence |
|---|---|---|
| 2026-09-19 | Created isolated GitHub clone because Forgejo has no nzbd repository | Clean branch `codex/bittorrent-single-node` |
| 2026-09-19 | Based the consolidated branch on reviewed restore head | PR #218 head `e0898c2`; eight remote checks green |
| 2026-09-19 | Implemented seed policy, shared bandwidth/quota accounting, storage holds, and scheduler-owned torrent starts | Source inspection only; qualification intentionally deferred |
| 2026-09-19 | Began ordered torrent terminal history transition | Backend outcome now checkpoints before durable history and queue retirement |
| 2026-09-19 | Activated the single-node daemon runtime and native admission | Workspace and all-target compile checks pass; tests intentionally deferred |
| 2026-09-19 | Added category-root recovery containment and removed the obsolete Cargo feature flag | Persisted payloads restore only under the configured default or category roots |
| 2026-09-19 | Added native web/mobile torrent workflows and observability | Dedicated detail/export/files, metrics, mobile file/magnet admission, and explicit payload deletion are present |
| 2026-09-19 | Added the narrow qBittorrent Web API 2.8.1 projection | Required *arr routes use queue-owner mutations; category overlays persist without rewriting config |
| 2026-09-19 | Completed adversarial review and addressed every finding | Fixed readiness acknowledgement/order, deletion ownership, pending recovery isolation and intent, authoritative category roots, request limits, quota high-water accounting, qBittorrent durability/sentinels, native web/mobile correctness, and stale CI feature invocations |
| 2026-09-19 | Qualified the reviewed release candidate | Workspace format and strict Clippy, focused Rust packages, BitTorrent policy/reproducibility, embedded web smoke, mobile typecheck and 55 Jest tests, plus Plurx syntax and 26 settings contracts are green; the full suite was intentionally not run |
| 2026-09-19 | Added the missing native web intake surface | The queue toolbar now submits magnets and remote `.torrent` URLs as typed JSON and local `.torrent` files as raw metainfo; category, priority, paused intent, bounded feedback, and DOM coverage apply to both wire contracts |
| 2026-09-19 | Addressed the web-intake adversarial review | Magnet admission no longer races a client abort against a durable hidden reservation, the DOM harness pins that distinction from bounded URL fetches, the progress table distinguishes base evidence from follow-up evidence, and closing the form restores keyboard focus |
| 2026-09-19 | Qualified the web-intake follow-up | `cargo fmt --all --check`, the embedded UI boot harness, and all 554 DOM assertions passed; the full workspace suite was intentionally left to the later batched process |
