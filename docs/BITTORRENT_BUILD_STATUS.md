# BitTorrent build status — single-node implementation to release candidate

**Status:** building · **Base:** reviewed restore head `e0898c2` · **Updated:**
2026-09-19

Companion to [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md), which owns the
architecture and acceptance contract. This page records execution progress for
the consolidated implementation branch. It is not an operator guide and does
not authorize production use by itself.

## Current position — lifecycle complete, policy and activation remain

| Workstream | State | Next observable result |
|---|---|---|
| M2e lifecycle and restart convergence | ready for inclusion | Preserve reviewed PR #218 behavior in the consolidated branch |
| M2f seed policy | implemented, unqualified | Durable ratio/time accounting pauses at exact boundaries and retains payload |
| M2g bandwidth and quota accounting | implemented, unqualified | Shared ceiling reallocates idle capacity; verified torrent bytes enter quota accounting |
| M2h enforcing disk guard | implemented, unqualified | Incomplete torrents pause on guard/ENOSPC while completed seeds remain live |
| M2i terminal history | building | Confirmed payload outcome is durable before retry-safe mixed-protocol history retirement |
| M2j daemon activation | blocked on policy decision | Compose startup, admission, recovery, rollback, and disabled-mode behavior |
| M3 native web/mobile surface | queued | Torrent details and controls are visible without leaking secrets |
| M4 Sonarr/Radarr compatibility | queued | Pinned clients complete add, poll, import, seed-limit, and removal workflows |
| M5 release evidence | queued | Fast-lane qualification and required operational evidence are green |

## Working rules — one consolidated review and qualification pass

- Work happens in the isolated clone at
  `/private/tmp/nzbd-bittorrent.oEssyV/repo`; existing user checkouts remain
  untouched.
- Implementation commits are grouped by coherent subsystem. No broad test run
  occurs until the consolidated change is ready for adversarial review.
- After review findings are resolved, run the repository's fast-lane
  qualification once, fix failures, and merge only a green reviewed head.
- The Forgejo token can access `pjunod/ansible` and `noirr/plurx`, but no nzbd
  repository exists there. GitHub remains the source for this isolated clone;
  no Forgejo repository will be created without explicit authorization.

## Decisions awaiting reconciliation

1. **Activation policy.** ADR-19 requires fail-closed activation until M2j
   evidence passes. The current instruction requests advisory-only enablement
   with no code gate. Non-activation work proceeds while this conflict is open.
2. **Settings owner.** The instruction names the plurx developer settings tab,
   while the feature and configuration live in nzbd. No cross-repository UI
   change will be assumed.
3. **Qualification scope.** M2j currently requires `make test`, revised
   `make bittorrent-policy`, and `make gate`; the current instruction requests
   only the fast lane before merge. The final qualification command will be
   chosen after this policy is reconciled.

## Activity log

| Date | Change | Evidence |
|---|---|---|
| 2026-09-19 | Created isolated GitHub clone because Forgejo has no nzbd repository | Clean branch `codex/bittorrent-single-node` |
| 2026-09-19 | Based the consolidated branch on reviewed restore head | PR #218 head `e0898c2`; eight remote checks green |
| 2026-09-19 | Implemented seed policy, shared bandwidth/quota accounting, storage holds, and scheduler-owned torrent starts | Source inspection only; qualification intentionally deferred |
| 2026-09-19 | Began ordered torrent terminal history transition | Backend outcome now checkpoints before durable history and queue retirement |
