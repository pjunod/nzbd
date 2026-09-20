//! Replicated cluster-control authority.
//!
//! Hiqlite owns the durable ordering boundary. The shared payload volume is
//! deliberately absent from this module: it stores only identities, exact
//! leases, command receipts, accepted generation references, and migration
//! fingerprints.

use crate::ClusterConfig;
use hiqlite::macros::params;
use hiqlite::{Client, Node, NodeConfig, Row};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTROL_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cluster_meta (
        cluster_id TEXT PRIMARY KEY,
        source_fingerprint TEXT NOT NULL,
        migrated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS control_counters (
        name TEXT PRIMARY KEY,
        value INTEGER NOT NULL CHECK (value >= 0)
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS control_jobs (
        job_id INTEGER PRIMARY KEY,
        incarnation TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        intent_json TEXT NOT NULL,
        result_ref TEXT,
        result_id TEXT UNIQUE,
        deleted INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS work_leases (
        resource TEXT PRIMARY KEY,
        cluster_id TEXT NOT NULL,
        job_id INTEGER NOT NULL,
        job_incarnation TEXT NOT NULL,
        owner_node_id TEXT NOT NULL,
        owner_incarnation TEXT NOT NULL,
        kind TEXT NOT NULL,
        scope_json TEXT NOT NULL,
        fence INTEGER NOT NULL CHECK (fence > 0),
        revision INTEGER NOT NULL CHECK (revision > 0),
        job_revision INTEGER NOT NULL CHECK (job_revision > 0),
        expires_at_ms INTEGER NOT NULL,
        terminal INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS command_receipts (
        command_id TEXT PRIMARY KEY,
        job_id INTEGER NOT NULL,
        expected_revision INTEGER NOT NULL,
        applied_revision INTEGER NOT NULL,
        command_json TEXT NOT NULL,
        applied_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS publication_receipts (
        receipt_id TEXT PRIMARY KEY,
        resource TEXT NOT NULL,
        owner_node_id TEXT NOT NULL,
        owner_incarnation TEXT NOT NULL,
        fence INTEGER NOT NULL,
        lease_revision INTEGER NOT NULL,
        lease_expiry_ms INTEGER NOT NULL,
        expected_job_revision INTEGER NOT NULL,
        result_id TEXT NOT NULL UNIQUE,
        result_ref TEXT NOT NULL,
        accepted_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TRIGGER IF NOT EXISTS publication_validate
     BEFORE INSERT ON publication_receipts
     BEGIN
       SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM work_leases
         WHERE resource = NEW.resource
           AND owner_node_id = NEW.owner_node_id
           AND owner_incarnation = NEW.owner_incarnation
           AND fence = NEW.fence
           AND revision = NEW.lease_revision
           AND expires_at_ms = NEW.lease_expiry_ms
           AND expires_at_ms > NEW.accepted_at_ms
           AND terminal = 0
       ) THEN RAISE(ABORT, 'stale lease token') END;
       SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM control_jobs j JOIN work_leases l ON l.job_id = j.job_id
         WHERE l.resource = NEW.resource
           AND j.incarnation = l.job_incarnation
           AND j.revision = NEW.expected_job_revision
           AND j.deleted = 0
       ) THEN RAISE(ABORT, 'stale job revision') END;
     END",
    "CREATE TRIGGER IF NOT EXISTS publication_apply
     AFTER INSERT ON publication_receipts
     BEGIN
       UPDATE control_jobs SET
         result_ref = NEW.result_ref,
         result_id = NEW.result_id,
         revision = revision + 1,
         updated_at_ms = NEW.accepted_at_ms
       WHERE job_id = (SELECT job_id FROM work_leases WHERE resource = NEW.resource);
       UPDATE work_leases SET terminal = 1, revision = revision + 1,
         updated_at_ms = NEW.accepted_at_ms
       WHERE resource = NEW.resource;
     END",
];

#[derive(Clone)]
pub struct ControlStore {
    client: Client,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseToken {
    pub resource: String,
    pub owner_node_id: String,
    pub owner_incarnation: String,
    pub fence: u64,
    pub revision: u64,
    pub expires_at_unix_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseClaim {
    Acquired(LeaseToken),
    Held {
        owner_node_id: String,
        fence: u64,
        expires_at_unix_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationOutcome {
    Applied { revision: u64 },
    Duplicate { revision: u64 },
    Conflict,
}

impl ControlStore {
    pub async fn start(cfg: &ClusterConfig) -> Result<Self, String> {
        std::fs::create_dir_all(&cfg.control_dir)
            .map_err(|error| format!("create control directory: {error}"))?;
        let local_raft: SocketAddr = cfg
            .control_raft_bind
            .parse()
            .map_err(|error| format!("invalid control_raft_bind: {error}"))?;
        let local_api: SocketAddr = cfg
            .control_api_bind
            .parse()
            .map_err(|error| format!("invalid control_api_bind: {error}"))?;
        let nodes = if cfg.control_peers.is_empty() {
            vec![Node {
                id: cfg.control_node_id,
                addr_raft: cfg.control_raft_bind.clone(),
                addr_api: cfg.control_api_bind.clone(),
            }]
        } else {
            cfg.control_peers
                .iter()
                .map(|peer| Node {
                    id: peer.id,
                    addr_raft: peer.raft_addr.clone(),
                    addr_api: peer.api_addr.clone(),
                })
                .collect()
        };
        let secret = format!("{:x}", Sha256::digest(cfg.secret.as_bytes()));
        let mut raft_config = NodeConfig::default_raft_config(10_000);
        raft_config.heartbeat_interval = 1_000;
        raft_config.election_timeout_min = 2_500;
        raft_config.election_timeout_max = 5_000;
        let node = NodeConfig {
            node_id: cfg.control_node_id,
            nodes,
            listen_addr_api: Cow::Owned(local_api.ip().to_string()),
            listen_addr_raft: Cow::Owned(local_raft.ip().to_string()),
            data_dir: Cow::Owned(cfg.control_dir.to_string_lossy().into_owned()),
            filename_db: Cow::Borrowed("nzbd-control.db"),
            secret_raft: format!("raft-{secret}"),
            secret_api: format!("api-{secret}"),
            health_check_delay_secs: 0,
            wal_size: 8 * 1024 * 1024,
            raft_config,
            ..NodeConfig::default()
        };
        let client = tokio::time::timeout(Duration::from_secs(20), hiqlite::start_node(node))
            .await
            .map_err(|_| "starting replicated control node timed out".to_owned())?
            .map_err(|error| format!("start replicated control node: {error}"))?;
        tokio::time::timeout(Duration::from_secs(20), client.wait_until_healthy_db())
            .await
            .map_err(|_| "replicated control quorum did not become healthy".to_owned())?;
        for statement in CONTROL_SCHEMA {
            client
                .execute(*statement, params!())
                .await
                .map_err(|error| format!("install control schema: {error}"))?;
        }
        Ok(Self { client })
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        self.client
            .shutdown()
            .await
            .map_err(|error| format!("shutdown replicated control node: {error}"))
    }

    /// Import the stopped legacy queue exactly once. The caller owns making a
    /// read-only backup before this transaction begins.
    pub async fn migrate_legacy_snapshot(
        &self,
        cluster_id: &str,
        fingerprint: &str,
        snapshot: &nzbd_state::QueueSnapshotDoc,
    ) -> Result<bool, String> {
        let existing = self
            .client
            .query_consistent_map::<MigrationRow, _>(
                "SELECT cluster_id,source_fingerprint FROM cluster_meta LIMIT 1",
                params!(),
            )
            .await
            .map_err(|error| format!("read control migration: {error}"))?;
        if let Some(row) = existing.into_iter().next() {
            if row.cluster_id == cluster_id && row.source_fingerprint == fingerprint {
                return Ok(false);
            }
            return Err(format!(
                "control store already belongs to cluster {} imported from {}; refusing {} from {}",
                row.cluster_id, row.source_fingerprint, cluster_id, fingerprint
            ));
        }

        let now = unix_ms()?;
        let mut statements: Vec<(String, hiqlite::Params)> = Vec::new();
        for job in &snapshot.jobs {
            let intent_json = serde_json::to_string(job)
                .map_err(|error| format!("encode legacy job {}: {error}", job.id.0))?;
            let incarnation = format!("legacy-{:x}", Sha256::digest(intent_json.as_bytes()));
            statements.push((
                "INSERT INTO control_jobs
                 (job_id,incarnation,revision,intent_json,updated_at_ms)
                 VALUES($1,$2,1,$3,$4)"
                    .into(),
                params!(i64::from(job.id.0), incarnation, intent_json, now),
            ));
        }
        statements.push((
            "INSERT INTO control_counters(name,value) VALUES('next_job_id',$1)".into(),
            params!(i64::from(snapshot.next_job_id)),
        ));
        statements.push((
            "INSERT INTO control_counters(name,value) VALUES('next_file_id',$1)".into(),
            params!(i64::from(snapshot.next_file_id)),
        ));
        statements.push((
            "INSERT INTO cluster_meta(cluster_id,source_fingerprint,migrated_at_ms)
             VALUES($1,$2,$3)"
                .into(),
            params!(cluster_id, fingerprint, now),
        ));
        self.client
            .txn(statements)
            .await
            .map_err(|error| format!("commit legacy control migration: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("legacy control migration statement: {error}"))?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn acquire(
        &self,
        resource: &str,
        cluster_id: &str,
        job_id: u64,
        job_incarnation: &str,
        owner_node_id: &str,
        owner_incarnation: &str,
        kind: &str,
        scope_json: &str,
        job_revision: u64,
        ttl: Duration,
    ) -> Result<LeaseClaim, String> {
        validate_identity(resource, owner_node_id, owner_incarnation)?;
        let now = unix_ms()?;
        let expiry = expiry(now, ttl)?;
        let sql = "INSERT INTO work_leases
            (resource, cluster_id, job_id, job_incarnation, owner_node_id,
             owner_incarnation, kind, scope_json, fence, revision, job_revision,
             expires_at_ms, terminal, updated_at_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,1,$9,$10,0,$11)
            ON CONFLICT(resource) DO UPDATE SET
              cluster_id=excluded.cluster_id, job_id=excluded.job_id,
              job_incarnation=excluded.job_incarnation,
              owner_node_id=excluded.owner_node_id,
              owner_incarnation=excluded.owner_incarnation, kind=excluded.kind,
              scope_json=excluded.scope_json, fence=work_leases.fence+1,
              revision=work_leases.revision+1, job_revision=excluded.job_revision,
              expires_at_ms=excluded.expires_at_ms, terminal=0,
              updated_at_ms=excluded.updated_at_ms
            WHERE work_leases.expires_at_ms <= $11
              AND work_leases.fence < 9223372036854775807
              AND work_leases.revision < 9223372036854775807
            RETURNING resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms";
        let rows = self
            .client
            .execute_returning_map::<_, LeaseRow>(
                sql,
                params!(
                    resource,
                    cluster_id,
                    i64_value("job id", job_id)?,
                    job_incarnation,
                    owner_node_id,
                    owner_incarnation,
                    kind,
                    scope_json,
                    i64_value("job revision", job_revision)?,
                    expiry,
                    now
                ),
            )
            .await
            .map_err(|error| format!("acquire lease: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode acquired lease: {error}"))?;
        if let Some(row) = rows.into_iter().next() {
            return Ok(LeaseClaim::Acquired(row.try_into()?));
        }
        let held = self.current_lease(resource).await?.ok_or_else(|| {
            "lease acquire changed no row and the resource disappeared".to_owned()
        })?;
        Ok(LeaseClaim::Held {
            owner_node_id: held.owner_node_id,
            fence: held.fence,
            expires_at_unix_ms: held.expires_at_unix_ms,
        })
    }

    pub async fn renew(
        &self,
        token: &LeaseToken,
        ttl: Duration,
    ) -> Result<Option<LeaseToken>, String> {
        validate_identity(
            &token.resource,
            &token.owner_node_id,
            &token.owner_incarnation,
        )?;
        let now = unix_ms()?;
        let expiry = expiry(now, ttl)?;
        let rows = self
            .client
            .execute_returning_map::<_, LeaseRow>(
                "UPDATE work_leases SET expires_at_ms=$1,revision=revision+1,updated_at_ms=$2
             WHERE resource=$3 AND owner_node_id=$4 AND owner_incarnation=$5
               AND fence=$6 AND revision=$7 AND expires_at_ms=$8
               AND terminal=0 AND expires_at_ms>$2
             RETURNING resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms",
                params!(
                    expiry,
                    now,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("renew lease: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode renewed lease: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    pub async fn release(&self, token: &LeaseToken) -> Result<bool, String> {
        let now = unix_ms()?;
        Ok(self
            .client
            .execute(
                "UPDATE work_leases SET terminal=1,expires_at_ms=$1,
             revision=revision+1,updated_at_ms=$1
             WHERE resource=$2 AND owner_node_id=$3 AND owner_incarnation=$4
               AND fence=$5 AND revision=$6 AND expires_at_ms=$7 AND terminal=0",
                params!(
                    now,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("release lease: {error}"))?
            == 1)
    }

    pub async fn current_lease(&self, resource: &str) -> Result<Option<LeaseToken>, String> {
        let rows = self
            .client
            .query_consistent_map::<LeaseRow, _>(
                "SELECT resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms
             FROM work_leases WHERE resource=$1 AND terminal=0",
                params!(resource),
            )
            .await
            .map_err(|error| format!("read lease: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    pub async fn seed_job(
        &self,
        job_id: u64,
        incarnation: &str,
        intent_json: &str,
    ) -> Result<(), String> {
        let now = unix_ms()?;
        self.client
            .execute(
                "INSERT INTO control_jobs
             (job_id,incarnation,revision,intent_json,updated_at_ms)
             VALUES($1,$2,1,$3,$4)
             ON CONFLICT(job_id) DO NOTHING",
                params!(i64_value("job id", job_id)?, incarnation, intent_json, now),
            )
            .await
            .map_err(|error| format!("seed control job: {error}"))?;
        Ok(())
    }

    pub async fn job_identity(&self, job_id: u64) -> Result<Option<(String, u64)>, String> {
        let rows = self
            .client
            .query_consistent_map::<JobIdentityRow, _>(
                "SELECT incarnation,revision FROM control_jobs WHERE job_id=$1 AND deleted=0",
                params!(i64_value("job id", job_id)?),
            )
            .await
            .map_err(|error| format!("read replicated job identity: {error}"))?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok((
                    row.incarnation,
                    u64::try_from(row.revision)
                        .map_err(|error| format!("invalid replicated job revision: {error}"))?,
                ))
            })
            .transpose()
    }

    pub async fn load_jobs(&self) -> Result<Vec<(nzbd_types::Job, u64, String)>, String> {
        let rows = self
            .client
            .query_consistent_map::<ControlJobRow, _>(
                "SELECT intent_json,revision,incarnation FROM control_jobs WHERE deleted=0 ORDER BY job_id",
                params!(),
            )
            .await
            .map_err(|error| format!("load replicated jobs: {error}"))?;
        rows.into_iter()
            .map(|row| {
                let job = serde_json::from_str(&row.intent_json)
                    .map_err(|error| format!("decode replicated job: {error}"))?;
                let revision = u64::try_from(row.revision)
                    .map_err(|error| format!("invalid replicated job revision: {error}"))?;
                Ok((job, revision, row.incarnation))
            })
            .collect()
    }

    pub async fn apply_command(
        &self,
        command_id: &str,
        job_id: u64,
        expected_revision: u64,
        command_json: &str,
        next_intent_json: &str,
        deleted: bool,
    ) -> Result<MutationOutcome, String> {
        if let Some(revision) = self.command_receipt(command_id).await? {
            return Ok(MutationOutcome::Duplicate { revision });
        }
        let now = unix_ms()?;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| "job revision exhausted".to_owned())?;
        let changed = self
            .client
            .execute(
                "UPDATE control_jobs SET revision=$1,intent_json=$2,deleted=$3,updated_at_ms=$4
             WHERE job_id=$5 AND revision=$6",
                params!(
                    i64_value("next revision", next_revision)?,
                    next_intent_json,
                    i64::from(deleted),
                    now,
                    i64_value("job id", job_id)?,
                    i64_value("expected revision", expected_revision)?
                ),
            )
            .await
            .map_err(|error| format!("apply command: {error}"))?;
        if changed != 1 {
            return Ok(MutationOutcome::Conflict);
        }
        self.client
            .execute(
                "INSERT INTO command_receipts
             (command_id,job_id,expected_revision,applied_revision,command_json,applied_at_ms)
             VALUES($1,$2,$3,$4,$5,$6)",
                params!(
                    command_id,
                    i64_value("job id", job_id)?,
                    i64_value("expected revision", expected_revision)?,
                    i64_value("applied revision", next_revision)?,
                    command_json,
                    now
                ),
            )
            .await
            .map_err(|error| format!("record command receipt: {error}"))?;
        Ok(MutationOutcome::Applied {
            revision: next_revision,
        })
    }

    pub async fn publish_result(
        &self,
        receipt_id: &str,
        token: &LeaseToken,
        expected_job_revision: u64,
        result_id: &str,
        result_ref: &str,
    ) -> Result<MutationOutcome, String> {
        if let Some(revision) = self.publication_receipt(receipt_id).await? {
            return Ok(MutationOutcome::Duplicate { revision });
        }
        let now = unix_ms()?;
        let inserted = self
            .client
            .execute(
                "INSERT INTO publication_receipts
             (receipt_id,resource,owner_node_id,owner_incarnation,fence,
              lease_revision,lease_expiry_ms,expected_job_revision,result_id,
              result_ref,accepted_at_ms)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                params!(
                    receipt_id,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms,
                    i64_value("expected job revision", expected_job_revision)?,
                    result_id,
                    result_ref,
                    now
                ),
            )
            .await;
        match inserted {
            Ok(1) => Ok(MutationOutcome::Applied {
                revision: expected_job_revision.saturating_add(1),
            }),
            Ok(_) => Ok(MutationOutcome::Conflict),
            Err(error) if error.to_string().contains("stale") => Ok(MutationOutcome::Conflict),
            Err(error) => Err(format!("publish result: {error}")),
        }
    }

    async fn command_receipt(&self, command_id: &str) -> Result<Option<u64>, String> {
        self.scalar_revision(
            "SELECT applied_revision AS revision FROM command_receipts WHERE command_id=$1",
            command_id,
        )
        .await
    }

    async fn publication_receipt(&self, receipt_id: &str) -> Result<Option<u64>, String> {
        self.scalar_revision(
            "SELECT expected_job_revision+1 AS revision FROM publication_receipts WHERE receipt_id=$1",
            receipt_id,
        ).await
    }

    async fn scalar_revision(&self, sql: &'static str, id: &str) -> Result<Option<u64>, String> {
        let rows = self
            .client
            .query_consistent_map::<RevisionRow, _>(sql, params!(id))
            .await
            .map_err(|error| format!("read durable receipt: {error}"))?;
        rows.into_iter()
            .next()
            .map(|row| {
                u64::try_from(row.revision)
                    .map_err(|error| format!("invalid durable revision: {error}"))
            })
            .transpose()
    }
}

struct LeaseRow {
    resource: String,
    owner_node_id: String,
    owner_incarnation: String,
    fence: i64,
    revision: i64,
    expires_at_unix_ms: i64,
}

impl From<&mut Row<'_>> for LeaseRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            owner_node_id: row.get("owner_node_id"),
            owner_incarnation: row.get("owner_incarnation"),
            fence: row.get("fence"),
            revision: row.get("revision"),
            expires_at_unix_ms: row.get("expires_at_ms"),
        }
    }
}

impl TryFrom<LeaseRow> for LeaseToken {
    type Error = String;
    fn try_from(row: LeaseRow) -> Result<Self, Self::Error> {
        Ok(Self {
            resource: row.resource,
            owner_node_id: row.owner_node_id,
            owner_incarnation: row.owner_incarnation,
            fence: u64::try_from(row.fence).map_err(|error| error.to_string())?,
            revision: u64::try_from(row.revision).map_err(|error| error.to_string())?,
            expires_at_unix_ms: row.expires_at_unix_ms,
        })
    }
}

struct RevisionRow {
    revision: i64,
}

struct MigrationRow {
    cluster_id: String,
    source_fingerprint: String,
}
impl From<&mut Row<'_>> for MigrationRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            cluster_id: row.get("cluster_id"),
            source_fingerprint: row.get("source_fingerprint"),
        }
    }
}

struct ControlJobRow {
    intent_json: String,
    revision: i64,
    incarnation: String,
}
impl From<&mut Row<'_>> for ControlJobRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            intent_json: row.get("intent_json"),
            revision: row.get("revision"),
            incarnation: row.get("incarnation"),
        }
    }
}

struct JobIdentityRow {
    incarnation: String,
    revision: i64,
}
impl From<&mut Row<'_>> for JobIdentityRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            incarnation: row.get("incarnation"),
            revision: row.get("revision"),
        }
    }
}
impl From<&mut Row<'_>> for RevisionRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            revision: row.get("revision"),
        }
    }
}

fn validate_identity(resource: &str, node: &str, incarnation: &str) -> Result<(), String> {
    for (label, value, max) in [
        ("resource", resource, 256usize),
        ("owner node", node, 128usize),
        ("owner incarnation", incarnation, 128usize),
    ] {
        if value.is_empty() || value.len() > max {
            return Err(format!("{label} must contain 1..={max} bytes"));
        }
    }
    Ok(())
}

fn i64_value(label: &str, value: u64) -> Result<i64, String> {
    if value == 0 {
        return Err(format!("{label} must be greater than zero"));
    }
    i64::try_from(value).map_err(|error| format!("{label} is too large: {error}"))
}

fn unix_ms() -> Result<i64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes unix epoch: {error}"))?
        .as_millis();
    i64::try_from(millis).map_err(|error| format!("unix millisecond overflow: {error}"))
}

fn expiry(now: i64, ttl: Duration) -> Result<i64, String> {
    let ttl = ttl.clamp(Duration::from_secs(1), Duration::from_secs(300));
    now.checked_add(i64::try_from(ttl.as_millis()).map_err(|error| error.to_string())?)
        .ok_or_else(|| "lease expiry overflow".to_owned())
}
