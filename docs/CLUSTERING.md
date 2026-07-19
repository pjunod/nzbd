# nzbd — Clustering Design (ADR-13…16)

| | |
|---|---|
| Status | **Accepted** — topology, distribution granularity and HA model chosen by project owner 2026-07-17; mechanism details below are the implementation spec |
| Date | 2026-07-17 |
| Deciders | Paul (owner) |
| Parent | [`ARCHITECTURE.md`](ARCHITECTURE.md) — §15 ADR table entries 13–16 summarize this document |
| Scope | Distribute work across multiple nzbd nodes sharing one work volume (GlusterFS), so a node running par-repair/unpack is not also fighting for CPU/disk with downloads |

---

## 1. Context

A single nzbd node serializes two workloads with very different profiles:
download (network + TLS + decode + sequential disk) and post-processing
(par2 GF(2^16) math, unrar — CPU and random I/O heavy). Running both on one
box degrades both; the operator already runs multiple nodes with a shared
GlusterFS work volume mounted on all of them.

Forces:

- **Product constraints carry over**: single static binary, no external
  runtime dependencies (ARCHITECTURE.md §2), crash-only design (§4.6),
  single-writer queue state (§4.2 / ADR 3).
- **Sonarr/Radarr reach one URL.** Whatever the cluster does internally,
  the *arr ecosystem speaks to a single nzbget-shaped endpoint.
- **The shared volume exists** and is the natural data plane: articles
  decoded on any node are visible to every node.
- **A network filesystem is a treacherous lock service.** POSIX locks over
  FUSE/Gluster are configuration-sensitive; wall clocks across home-lab
  nodes are not trustworthy.
- Phase 2 (post-processing) is not yet implemented — the design must define
  PP work units now so phase 2 lands cluster-native.

## 2. Decisions

| # | Decision | Chosen |
|---|---|---|
| ADR-13 | Coordination topology | **Elected coordinator ("leader") + workers, work leased over HTTP; the shared volume carries data and the election lease, never fine-grained locks** |
| ADR-14 | Work granularity | **Whole-job download leases + stage-level PP leases** (segment-split downloads deferred) |
| ADR-15 | Client reachability & HA | **Automatic failover: leadership is elected; every node serves the API and transparently proxies to the current leader** |
| ADR-16 | State placement | **Queue authority + per-job journals + article data on the shared volume, sharded per job and fenced by lease epoch; SQLite never lives on the network FS** |

### 2.1 Options considered — topology (ADR-13)

| Option | Complexity | Deps | Failure story | Verdict |
|---|---|---|---|---|
| **Leader + workers over HTTP** (chosen) | Medium | none | Single writer preserved; leader loss handled by election (ADR-15) | ✅ Extends §4.2's owner-task pattern across the wire: workers pull job leases exactly like connection tasks pull segment leases |
| Symmetric peers, lease files only | High | none | Every mutation is a distributed-lock problem on a network FS; fencing bugs surface as silent queue corruption | ❌ Hardest to test; Gluster lock semantics vary by version/config |
| External coordinator (etcd/redis/NATS) | Medium | +1 service | Battle-tested primitives | ❌ Breaks the no-runtime-deps ethos for a media-server product |
| Embedded Raft (openraft) | Very high | big crate | Real consensus | ❌ Log replication machinery to protect a download queue; the shared FS already provides shared storage |

### 2.2 Options considered — granularity (ADR-14)

| Option | Wins | Costs | Verdict |
|---|---|---|---|
| PP-only offload | Simplest protocol | Leader still pays TLS+decode+disk while PPing elsewhere | ❌ partial fix |
| **Whole-job downloads + stage-level PP** (chosen) | Node A repairs job 1 while node B downloads job 2; per-node roles | Connection-budget partitioning needed (see §6.3) | ✅ |
| Segment-split downloads across nodes | Aggregate bandwidth when nodes have separate WAN links | Cross-node segment scheduling, per-article fan-in | Deferred to C3 — the job-lease protocol does not preclude it |

### 2.3 Options considered — HA (ADR-15)

| Option | Verdict |
|---|---|
| Workers idle while leader down | ❌ Rejected by owner — cluster must ride through node loss |
| **Election + any-node API proxy** (chosen) | ✅ Leadership is a role, not a node. Election arbitrates via the shared volume — the same resource being protected — so a node that cannot reach Gluster can neither lead nor corrupt state (fate-sharing). Sonarr may point at any node (or an LB across all); non-leaders proxy |
| VIP/keepalived only | Viable operator add-on, but proxying makes it optional rather than required |

## 3. System overview

```
      Sonarr ──▶ http://any-node:6789 ── non-leader proxies ──▶ leader
 ┌────────────────────────────────────────────────────────────────────┐
 │ Gluster volume (shared_dir)                                        │
 │  .nzbd-cluster/leader.json        ← election lease {epoch,node,seq}│
 │  .nzbd-cluster/nodes/<name>.json  ← presence/caps/stats heartbeats │
 │  .nzbd-cluster/queue.json         ← queue authority snapshot       │
 │  .nzbd-cluster/jobs/<id>/journal.<lease>  ← fenced per-job journals│
 │  complete/<job>/<file>[.part]     ← DirectWrite article data       │
 └────────────────────────────────────────────────────────────────────┘
   node A: leader+worker         node B: worker          node C: worker
   ┌───────────────────┐         ┌──────────────┐        ┌──────────────┐
   │ engine (authority)│  grant  │ engine (empty│        │ engine       │
   │ + cluster sched.  │ ───────▶│ queue; leased│        │ (PP exec in  │
   │ + own executors   │ ◀─────── │ jobs only)  │        │  phase C2)   │
   └───────────────────┘  hb/done└──────────────┘        └──────────────┘
```

Every node runs the same binary and the same subsystems; behavior differs
only by which roles are currently active: **leader** (queue authority +
scheduler + API authority), **download executor**, **PP executor** (C2).
The leader is also an executor by default — a 2-node cluster is leader+both
and worker+both.

## 4. Election (ADR-15 mechanism)

State: one file, `<shared>/.nzbd-cluster/leader.json` =
`{epoch, node, api_url, seq}`; all writes are tmp+rename (atomic on
Gluster).

- **Renewal.** The leader rewrites the file every `lease_interval` (5 s)
  with `seq += 1`.
- **Staleness is observed, not computed from wall clocks.** Every node
  polls the file and remembers `(epoch, seq)` plus the *local monotonic*
  time it last saw them change. No change for `takeover_after` (20 s)
  ⇒ the leader is presumed dead. Clock skew between nodes is irrelevant.
- **Candidacy** (nodes with `coordinator = true`): stagger by
  `priority × 2s + jitter`, re-check staleness, then **write–wait–verify**:
  write `{epoch+1, me, seq=1}`, wait `2 × lease_interval`, re-read. If the
  file names someone else with an epoch ≥ ours, stand down. Two racing
  candidates converge in one round; brief dual-claim windows are harmless
  because every state mutation is epoch-fenced (§6.4).
- **Taking office**: load `queue.json`, fold every per-job journal, adopt
  in-flight leases (§6.2), start scheduling, begin renewal.
- **Deposition** (a leader observes a higher epoch): crash-only demotion —
  abort local authority (state is safe in journals/snapshot), rejoin as a
  worker. No graceful state handoff to maintain.
- **Epoch monotonicity**: sourced from `leader.json`; if it is missing or
  corrupt, recover `max(epoch)` over the queue snapshot and node files.

**Operational requirement:** Gluster must be configured for consistency
(server-side quorum / replica 3 or arbiter). A volume that itself
split-brains gives two sides two truths; no application protocol survives
that. Documented in ARCHITECTURE.md §17.

## 5. Node registry

Each node renews `<shared>/.nzbd-cluster/nodes/<name>.json`:
`{name, api_url, roles, download_slots_free, pp_slots_free, busy_pp,
rate_bps, seq, epoch_seen}` every `lease_interval`. Staleness is judged by
observed non-progression, same as the election. The registry feeds the
scheduler and `GET /api/v1/cluster`.

## 6. Work distribution

### 6.1 Protocol

Cluster endpoints share the API port, mounted under `/cluster/v1/*`,
authenticated with a shared `secret` (constant-time compare; TLS or
trusted LAN assumed — provider credentials never cross this channel, §6.5).

| Endpoint | Semantics |
|---|---|
| `POST /cluster/v1/work/poll` | Worker offers `{node, free download slots, pp slots}`; leader replies with 0..n grants. A grant = `{lease_id, epoch, job_spec, server_budgets}` for a download job (C1) or `{lease_id, job, pp_stage_plan}` (C2) |
| `POST /cluster/v1/work/heartbeat` | Worker renews `{lease_ids, per-job progress counters}`; reply may carry `{cancel: [lease_id]}` |
| `POST /cluster/v1/work/complete` | Worker returns the **final `Job` value** (serde) + outcome; leader swaps it into authority state |
| `GET /cluster/v1/leader` | `{epoch, node, api_url}` for proxying and diagnostics |

`lease_id = "L<epoch>-<counter>"` — globally unique, and the fencing
suffix for journals and staging dirs.

### 6.2 Lease lifecycle

Granted → renewed by heartbeat (TTL `worker_ttl` = 30 s, observed
monotonically) → completed | reclaimed | cancelled.

- **Reclaim** (worker presumed dead): leader clears the delegation, folds
  the job's journals (union), and reschedules — locally or to another node.
  Exactly the phase-1 crash-recovery path, applied per job across nodes.
- **Adoption** (leader died, worker lives): workers keep executing through
  an election; the first heartbeat to the new leader lists leases it does
  not know. If the job is unassigned (or assigned to that same node), the
  new leader adopts the lease as-is — no work is thrown away. Otherwise it
  replies `cancel`.
- **Cancellation** (job deleted/paused via API): next heartbeat carries the
  cancel; worker aborts the local execution and confirms.

### 6.3 Connection budgets

Provider `max_connections` is a per-account limit that must hold
**cluster-wide**. Server definitions (and credentials) stay in each node's
local config, keyed by server name; the leader partitions each account's
connection budget across the nodes currently downloading — computed at
grant time: `floor(max_connections / active_download_nodes)`, min 1,
re-issued on membership change. A worker caps its per-server connection
tasks at `min(local config, granted budget)` (engine budget watch, §7).
Nodes may also have *different* providers; the leader only budgets
accounts it has been told are shared (same server name ⇒ same account).

### 6.4 Fencing (why overlapping executors are safe)

Rule: **append-only union for downloads, staging-rename for PP** — no
shared-file locking anywhere.

- Per-job journals are written to
  `jobs/<id>/journal.<lease_id>` — one file per lease, append-only.
  Recovery/fold **unions the Done records across all journal files** of
  that job; duplicates are idempotent (same segment ⇒ same offset/len/crc,
  since article content is immutable). A zombie holder of an expired lease
  appending real completed segments is *contributing*, not corrupting.
- `.part` article writes are positional writes of identical bytes —
  overlapping writers converge on the same content by construction.
- PP (C2) executes in `jobs/<id>/pp.<lease_id>/` staging; commit is
  verify-lease-then-rename, and a superseded lease's staging dir is
  garbage, cleaned by the leader. Double-unpack into one directory can
  never happen.
- Queue-authority writes (snapshot) re-verify `leader.json` (epoch+node)
  immediately before the commit rename; a deposed leader's late write
  requires waking from a >20 s stall inside a millisecond window —
  residual risk accepted and documented (this is practical fencing on a
  shared FS, not linearizable consensus; the union-journal design is what
  makes the residual window harmless for job data).

### 6.5 Security

Cluster calls carry the shared secret (config `secret`/`secret_file`,
required when clustering is enabled). Provider credentials never leave the
node that configured them — job specs reference servers by name only.
The existing API/auth story (phase 3) applies unchanged to client traffic;
`/cluster/v1/*` rejects unauthenticated calls regardless.

## 7. Engine changes (executor-ready core)

The phase-1 engine keeps its architecture; six contained changes make it a
cluster executor:

1. **Per-job fenced journals** — `state/jobs/<id>/journal.<suffix>`
   replaces the global journal; replay unions all files per job.
   Single-node uses suffix `local` (a legacy `segments.journal` is folded
   once at boot and removed by the next snapshot compact).
2. **Optional snapshot persistence** — leader (authority) on; worker
   engines run queue-persistence-off (their truth is the leader + fenced
   journals; a restarted worker starts empty and receives leases anew).
3. **Job import/export** — `add_job_from_spec(Job)` preserving ids +
   journal-fold on import; `export` returns the final `Job` for
   `work/complete`. JobIds are minted only by the authority, so ids never
   collide across engines.
4. **Delegation set** — delegated jobs are skipped by the local scheduler
   and carry `assigned_node` in snapshots/summaries.
5. **Connection-budget watch** — connection task *i* for a server parks
   while `i ≥ budget(server)`; the leader's grants update the watch.
6. **Remote progress mirroring** — heartbeat counters overlay delegated
   jobs' summaries (segment-level truth stays in the shared journals and
   the final export).

## 8. API surface

- Every node serves the full native API + compat shim; non-leaders proxy
  to the leader (streaming reverse proxy; `X-Nzbd-Forwarded: <node>`
  guards loops). Election gaps surface as brief 502/503 — *arr clients
  retry.
- `GET /api/v1/cluster` → `{leader, epoch, self, nodes[], leases[]}`.
- Events gain `LeaderChanged`, `NodeJoined/NodeLost`, `JobAssigned`.

## 9. Configuration

```toml
[cluster]
enabled = true
node_name = "node-a"            # unique, stable
shared_dir = "/mnt/work"        # the Gluster mount
advertise_url = "http://10.0.0.11:6789"   # how peers reach this node
secret_file = "/etc/nzbd/cluster.secret"  # or: secret = "…"
coordinator = true               # eligible for election
priority = 10                    # lower = preferred leader, staggers candidacy
download = true
max_download_jobs = 2            # concurrent download-job leases on this node
post_process = true              # PP executor (effective from phase C2)
pp_slots = 1
lease_interval_secs = 5
takeover_after_secs = 20
worker_ttl_secs = 30
```

With `enabled = true`: queue authority, journals and job data live under
`shared_dir` (`paths.dest_dir` defaults to `<shared_dir>/complete`;
a dest outside the shared volume is a validation warning — remote PP could
not see the files). `enabled = false`: phase-1 behavior, byte-for-byte
(modulo the per-job journal layout, migrated automatically).

## 10. Failure matrix

| Failure | Behavior |
|---|---|
| Worker dies mid-download | TTL expiry → reclaim → journal-union fold → resume elsewhere; journaled segments are never re-fetched |
| Worker dies mid-PP (C2) | Reclaim → stage restarted from its staging dir or scratch; committed stages are idempotent |
| Leader dies | Election in ≈`takeover_after` + one verify round (~30 s default); workers keep executing and are adopted; leader's own in-flight jobs reclaimed via journals |
| Deposed leader wakes | Higher epoch observed → crash-only demotion, rejoin as worker; its stale writes are epoch-shadowed or union-harmless (§6.4) |
| Gluster unreachable from one node | It can neither renew leadership nor journal → it self-demotes / its leases expire; work moves to nodes that still see the volume |
| Gluster down everywhere | Cluster halts (data plane gone); engines idle and retry — crash-only recovery once the volume returns |
| Network partition, volume still shared | Election converges via the volume (sole arbiter); workers that cannot reach the leader over HTTP idle their leases out; no split queue |

## 11. Testing

In-process multi-node harness: N cluster runtimes over one tempdir
"shared volume" + `nzbd-nserv` providers, real loopback HTTP between
nodes; time-compressed lease intervals.

- Exactly-one-leader under concurrent candidacy (loop N rounds).
- Distributed download: jobs added via a *worker's* API (proxy), spread
  across nodes, bit-identical output, budgets respected.
- Worker kill mid-download → reclaim, cross-node resume, zero re-fetch of
  journaled segments (nserv hit counts).
- Leader kill mid-download+mid-delegation → failover within bound, lease
  adoption (no restart of the delegated job), completion, zero re-fetch.
- Fencing units: journal union with overlapping lease files; snapshot
  commit rejected when the lease file changed under the writer.
- Single-node-cluster parity with the phase-1 e2e suite.
- CI runs on local FS; a documented manual soak checklist covers real
  Gluster (quorum on, node reboots, volume heal during downloads).

## 12. Consequences

Easier: PP/download separation (the original goal); rolling restarts;
adding capacity = mount volume + join with a secret; phase-2 PP arrives
cluster-native (a second lease type on an existing protocol).

Harder: two config knobs that must be right (Gluster quorum, shared
secret); per-job journal migration; connection-budget correctness across
nodes; more failure modes to test (mitigated by crash-only + union
fencing).

Revisit later: segment-split downloads (C3); weighted/affinity scheduling
(CPU class, per-node link speed); observed-latency-based provider budget
rebalancing; WAN-separated nodes.

**Filesystem portability.** Nothing here is Gluster-specific. The
protocol needs exactly: atomic same-directory rename, create-exclusive
(`O_EXCL`), and bounded cross-client visibility lag — and it deliberately
avoids everything network filesystems get wrong (no byte-range locks, no
cross-client `O_APPEND`, fencing tokens re-checked at commit so stale
reads can't double-apply work). That holds on NFSv4, CephFS, Lustre,
GPFS, JuiceFS and similar; Gluster is the reference deployment and one
of the *weaker* targets, so anything stronger is strictly easier. What
cannot work is a non-filesystem store (raw object storage has no atomic
rename); supporting that would mean an alternative lease-store backend
(etcd/redis) behind the same lease/fencing protocol — a seam, not a
rewrite.

## 13. Phasing

| Phase | Scope | Exit criteria |
|---|---|---|
| **C1 — foundation** (now) | Election + registry + fenced per-job state + work protocol + distributed whole-job downloads + any-node API proxy + `[cluster]` config | Multi-node harness green: failover mid-download with adoption, worker reclaim without re-fetch, single-leader invariant, single-node parity |
| **C2** (with phase 2) | PP lease type: par-verify/repair/unpack/scripts execute on any node in fenced staging dirs; download-vs-PP anti-affinity scheduling | A job downloaded on node B repairs on node C while node B downloads the next job |
| **C3** (later) | Segment-split downloads, weighted scheduling, cluster dashboards, budget rebalancing | — |
