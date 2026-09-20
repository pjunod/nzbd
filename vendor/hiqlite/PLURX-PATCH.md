# Vendored Hiqlite 0.14.0

This directory is the crates.io `hiqlite` 0.14.0 package, licensed under
Apache-2.0. This copy carries sixteen compatibility patches for clustered
deployments:

- `NodeConfig` selects the local node by `Node::id` and rejects duplicate ids.
  Raft ids are durable identities, so a roster such as `1, 3` is valid when an
  unredeemed join token still holds id 2. Padding that roster with a duplicate
  node creates duplicate connection targets and eventually exhausts file
  descriptors.
- The split-brain probe uses Hiqlite's configured HTTP client and API TLS
  verification policy. Auto-generated certificates are intentionally
  self-signed, so the default Reqwest client otherwise reports
  `UnknownIssuer` on every probe.
- Concurrent embedded-node starts share the first auto-generated TLS key
  without panicking when more than one task reaches the process-wide key's
  one-time publication boundary.
- The authenticated cluster API exposes OpenRaft 0.9's election trigger on a
  selected voter. That release has no dedicated leader-transfer operation;
  Plurx uses the trigger to elect a successor before a leader commits its own
  graceful removal.
- `Client::local_db_raft_metrics` exposes a synchronous local-only wrapper for
  OpenRaft's metrics watch. Remote clients fail immediately instead of falling
  back to management HTTP, and the copied snapshot omits membership, addresses,
  replication maps, and quorum-ack state so passive application metrics cannot
  be mistaken for a quorum-confirmed watermark.
- `Client::db_quorum_watermark` asks the current database leader to run
  OpenRaft's quorum-backed linearizable-read proof and returns only
  `(term, leader_id, committed_index, local_read_protocol_version)`. Followers
  use the existing authenticated leader stream; callers remain responsible for
  a monotonic lease and matching the proof to their local Raft observation. A
  P3a leader's three-column response remains a valid Authority/readiness proof
  but maps to protocol `0`, so bounded local reads stay closed during rollout.
- The SQLite snapshot builder and installer publish process-local, lock-free
  duration histograms through `Client::local_db_snapshot_metrics`. Explicit
  RAII start/finish hooks classify build/install success and every error or
  cancelled exit without polling storage or exposing paths and snapshot ids.
- SQLite and cache snapshot RPC responses preserve peer-side Raft errors as
  `RemoteError` instead of flattening them into `Unreachable`. OpenRaft uses
  that type boundary to recognize `SnapshotMismatch`, reset an interrupted
  chunk stream to offset zero, and let a restarted follower converge. The
  `sqlite_install_snapshot_preserves_mismatch_for_offset_reset` and
  `cache_install_snapshot_preserves_mismatch_for_offset_reset` tests drive the
  production `install_snapshot` methods through a nonzero mismatch and guard
  the remote-error boundary. OpenRaft's retained offset-reset test covers its
  private chunk sender, while Plurx's three-voter learner snapshot contract
  covers convergence; these vendor tests do not claim to drive that private
  sender themselves. Remove this patch only after upstream Hiqlite preserves
  peer snapshot errors on both transports.
- Every Raft, cluster-API, proxy, and authentication WebSocket frame is flushed
  before its writer waits for more work. One 30-second budget covers the write
  and flush together; errors and expiry terminate the writer and wake the
  connection supervisor even while its reader remains live. Graceful Close
  frames use a separate 250-millisecond best-effort allowance, and the server
  challenge/response exchange remains inside the existing five-second
  connection deadline. The real-TLS frame regressions hold the ciphertext tail
  of both client and server writes, including 3 MiB frames, and prove that the
  same socket completes after backpressure clears.
- Raft and cluster-API connection supervisors own and join both split socket
  tasks. Reader EOF, malformed frames, writer errors, task panics, reset,
  shutdown, and leader handoff all wake admission even when a bounded queue is
  full. Queue admission uses an ownership-retaining `try_send` loop, so a
  timeout or handoff cannot claim a mutation was undispatched after a
  cancellable send future already transferred it. Accepted snapshot receive
  operations move to one node-owned executor per Raft group with one running
  and one queued request; disconnecting a socket drops its reply but cannot
  cancel a partial file write. Queued work whose reply owner disappeared is
  skipped before execution. Graceful shutdown keeps Raft and its state-machine
  worker alive until this executor releases accepted work, and cancellation of
  one shutdown waiter cannot detach the retained task. Connections capture
  their originating Tokio runtime so an off-runtime drop still owns cleanup.
- A dropped OpenRaft RPC signals its WebSocket manager through a dedicated
  retained notification instead of best-effort enqueueing `Reset` behind the
  request itself. The old one-slot queue could be full at the hard deadline,
  lose the reset, and leave a leader permanently waiting on the abandoned
  connection after a follower had installed its snapshot. The
  `handler_coordinator_consumes_retained_reset_when_request_queue_is_full`
  regression keeps cancellation independent of request-queue pressure.
- Snapshot build, install, read, and asynchronous cleanup share one
  file-ownership boundary. Completed files use fsync plus atomic rename, and a
  separately fsynced pointer publishes the exact current generation (including
  an explicit empty state). Install uses a durable pending-generation marker;
  publication failures stop further state-machine work and startup completes
  the pending recovery before serving. Read-only inspection leaves generations
  immutable. Startup migrates legacy directories from live database metadata
  or applied Raft order; normal reads and recovery never infer recency from UUID
  ordering, so delayed cleanup and interrupted publication cannot replace or
  delete current state.
- Remote clients created in proxy mode retain only their configured proxy
  endpoints across WebSocket reconnects, metrics discovery, and
  `ForwardToLeader` responses. Connection failures, established-stream closes,
  and leader-forwarding responses advance independent DB/cache cursors through
  that original endpoint set; ambiguous in-flight requests fail without
  replay, and directly advertised voter addresses never replace the boundary.
  A proxy therefore remains an enforceable routing, partition, and trust
  boundary after failover and recovery. Remote shutdown uses out-of-band
  cancellation and joins both stream managers, the rate ticker, and the remote
  listen-notify loop even when every endpoint is unavailable; active and queued
  work receives a stable error instead of a dropped-acknowledgement panic.
- A client receiving `ForwardToLeader(None, None)` treats it as a definitive
  unaccepted request, probes its configured authenticated peers concurrently,
  and reconnects through a detached, bounded discovery and acknowledged stream
  handoff. Database requests make at most three attempts, recovering after no
  more than two definitive forwarding refusals; this covers the interval where
  the first replacement is still learning the election winner without replaying
  an accepted write or allowing raw client calls to hang. Management clients
  never follow redirects, so their custom API-secret header cannot leave the
  configured roster. Proxy-mode clients reconnect through the next configured
  proxy instead of escaping that trust boundary. Replicated backups derive the
  upload owner from the committed log entry's leader rather than a client-side
  cached leader sample, so a handoff cannot acknowledge a backup that no node
  uploads.
- Under `validation-test-helpers` only, the SQLite state machine keeps
  process-local monotonic counters of applied Raft entries by payload kind
  (blank, membership, normal), exposed as
  `validation_applied_payload_counts()`, and attributes applied normal
  entries to caller-registered SQL classes
  (`validation_register_applied_sql_classes` — set-once before the node
  starts; `validation_applied_sql_class_counts` reads them in registration
  order; an entry carrying statement SQL counts toward the first class whose
  needle matches — a `^` prefix anchors the match to the statement start —
  while migration, backup, and RTT payloads stay unclassified and therefore
  fail exact counts loudly). Exact-count drills sample both around their
  write windows so a
  contaminating entry — a blank leader-establishment commit, a membership
  change, or a background writer's SQL — is named rather than merely
  counted. Setting `PLURX_VALIDATION_LOG_APPLIED` in a validation process
  additionally logs each applied entry's index and payload to stderr, which
  is how a contaminating entry is identified down to its SQL. Production
  binaries compile none of it.
- Cryptr's S3 transport is activated by Hiqlite's `s3` feature instead of by
  the unconditional dependency declaration. S3 and backup builds retain the
  same feature through Hiqlite's existing `backup`/`s3` relationship, while
  SQLite-only consumers such as nzbd do not compile the unrelated XML object
  storage client or inherit its advisory surface.

Remove this vendor when an upstream Hiqlite release contains all sixteen patches
and Plurx has upgraded to it. Until then, the sparse-roster regression in
`crates/plurx-core/src/cluster/migration.rs` keeps the first patch load-bearing,
and the snapshot RPC error-boundary plus queue-saturated reset tests above keep
the transport-recovery patches load-bearing.

Cargo records this package as path-sourced, which means cargo-audit skips it.
The weekly `rust-audit.yml` job uses `scripts/vendor-audit-lock` to restore this
exact release's registry source and checksum before a second advisory scan.
That check must remain until this directory is removed.

Source: <https://crates.io/crates/hiqlite/0.14.0>

The `validation-test-helpers` feature adds process-local apply-pause and
transport-partition controls used only by Plurx's separate-process acceptance
harness. Production binaries do not enable or compile those controls.

The README, upstream tests, and static assets remain provenance rather than an
in-place test suite. `Cargo.lock` is the deliberate exception: Plurx regenerates
it to pin OpenRaft 0.9.25 and its resolver-selected transitive dependencies, so
the focused vendor lane exercises the snapshot reset semantics actually
shipped by the application. The workspace excludes this directory
deliberately.
