# BitTorrent build status — single-node release candidate

**Status:** qualified, merge pending · **Base:** reviewed restore head `e0898c2` · **Updated:**
2026-09-19

Companion to [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md), which owns the
architecture and acceptance contract. This page records execution progress for
the consolidated implementation branch. It is not an operator guide and does
not authorize production use by itself.

## Current position — reviewed and fast-lane green

| Workstream | State | Next observable result |
|---|---|---|
| M2e lifecycle and restart convergence | qualified | Reviewed restore behavior is preserved in the consolidated branch |
| M2f seed policy | qualified | Durable ratio/time accounting pauses at exact boundaries and retains payload |
| M2g bandwidth and quota accounting | qualified | Shared ceiling reallocates idle capacity; verified torrent bytes enter quota accounting |
| M2h enforcing disk guard | qualified | Incomplete torrents pause on guard/ENOSPC while completed seeds remain live |
| M2i terminal history | qualified | Confirmed payload outcome is durable before retry-safe mixed-protocol history retirement |
| M2j daemon activation | qualified | One configured session owns admission, recovery, watch ingestion, shutdown, and disabled-mode refusal |
| M3 native web/mobile surface | qualified | Typed add, detail/export/files/metrics, web controls, mobile file/magnet add, and torrent status are wired |
| M4 Sonarr/Radarr compatibility | qualified | Web API 2.8.1 routes, Bearer/Basic/SID auth, per-IP login limiting, durable categories, projections, and mutations are wired |
| M5 release evidence | complete | Adversarial review findings are resolved and the requested fast lane is green |

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
