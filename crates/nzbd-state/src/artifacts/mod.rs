//! Authoritative payload inventory, deliberately independent of History.
//!
//! SQLite owns intent and policy; external sidecars own allocation identity.
//! Nothing in a media payload is a marker or grants permission to remove it.
mod fs;
mod recovery;
pub use recovery::{Receipt, ReceiptFile, Recovery, RecoveryFile};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("inventory database: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("inventory data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Conflict(String),
    #[error("artifact not found")]
    NotFound,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub device: u64,
    pub inode: u64,
    pub bytes: u64,
    pub modified: String,
    pub directory: bool,
}
impl Identity {
    pub fn same_object(&self, other: &Self) -> bool {
        self.device == other.device
            && self.inode == other.inode
            && self.directory == other.directory
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub identity: Identity,
    pub digest: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub generation: String,
    pub revision: u64,
    pub job: Option<u32>,
    pub path: PathBuf,
    pub root: PathBuf,
    pub root_identity: Identity,
    pub identity: Option<Identity>,
    pub state: String,
    pub owned: bool,
    pub keep: bool,
    pub hold: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub retention_seconds: u64,
    pub deadline: Option<i64>,
    pub eligible_seconds: u64,
    pub files: Vec<FileEntry>,
    pub error: Option<String>,
}
impl Artifact {
    pub fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "deleted" | "source_gone")
    }
    fn eligible(&self) -> bool {
        self.owned
            && self.state == "parked_failed"
            && !self.keep
            && self.hold.is_none()
            && self.retention_seconds > 0
    }
    pub fn earliest_expiry(&self, now: i64) -> Option<i64> {
        if !self.eligible() {
            return None;
        }
        Some(self.deadline?.max(
            now.saturating_add(self.retention_seconds.saturating_sub(self.eligible_seconds) as i64),
        ))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub artifact: String,
    pub kind: String,
    pub state: String,
    pub request: String,
    pub created_at: i64,
    pub not_before: i64,
    pub attempts: u32,
    pub next_retry: i64,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub enabled: bool,
    pub failed_retention_days: u32,
    pub recovery_root: PathBuf,
    pub consumer_token: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            failed_retention_days: 7,
            recovery_root: PathBuf::new(),
            consumer_token: String::new(),
        }
    }
}

pub struct Inventory {
    db: Mutex<Connection>,
    state_dir: PathBuf,
    pub installation: String,
    // Serialized mutation coordinator. Reads never wait for filesystem work.
    mutation: Mutex<()>,
    clock: Mutex<Instant>,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn id(db: &Connection) -> Result<String> {
    Ok(db.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}
fn read<T: serde::de::DeserializeOwned>(db: &Connection, table: &str, key: &str) -> Result<T> {
    let value: Option<String> = db
        .query_row(
            &format!("SELECT data FROM {table} WHERE id=?1"),
            [key],
            |r| r.get(0),
        )
        .optional()?;
    serde_json::from_str(&value.ok_or(Error::NotFound)?).map_err(Into::into)
}
fn save_artifact(db: &Connection, a: &Artifact) -> Result<()> {
    db.execute("INSERT INTO artifacts(id,job,path,state,updated_at,data) VALUES(?1,?2,?3,?4,?5,?6)
        ON CONFLICT(id) DO UPDATE SET job=excluded.job,path=excluded.path,state=excluded.state,updated_at=excluded.updated_at,data=excluded.data",
        params![a.id, a.job, a.path.to_string_lossy(), a.state, a.updated_at, serde_json::to_string(a)?])?;
    Ok(())
}
fn save_operation(db: &Connection, op: &Operation) -> Result<()> {
    db.execute(
        "INSERT INTO operations(id,artifact,state,data) VALUES(?1,?2,?3,?4)
       ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",
        params![op.id, op.artifact, op.state, serde_json::to_string(op)?],
    )?;
    Ok(())
}
fn event(db: &Connection, artifact: &str, kind: &str, detail: &str) -> Result<()> {
    db.execute(
        "INSERT INTO events(artifact,at,kind,detail) VALUES(?1,?2,?3,?4)",
        params![artifact, now(), kind, detail],
    )?;
    Ok(())
}

impl Inventory {
    pub fn open(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir)?;
        let db = Connection::open(state_dir.join("artifacts.sqlite"))?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY,job INTEGER,path TEXT NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS artifacts_job ON artifacts(job);
          CREATE INDEX IF NOT EXISTS artifacts_path ON artifacts(path);
          CREATE INDEX IF NOT EXISTS artifacts_state ON artifacts(state,updated_at);
          CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS operations_state ON operations(state);
          CREATE TABLE IF NOT EXISTS recoveries(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,artifact TEXT NOT NULL,at INTEGER NOT NULL,kind TEXT NOT NULL,detail TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS events_artifact ON events(artifact,seq);
          INSERT OR IGNORE INTO meta VALUES('schema','1');
          INSERT OR IGNORE INTO meta VALUES('installation',lower(hex(randomblob(16))));")?;
        let schema: String =
            db.query_row("SELECT value FROM meta WHERE key='schema'", [], |r| {
                r.get(0)
            })?;
        if schema != "1" {
            return Err(Error::Conflict(format!(
                "unsupported inventory schema {schema}"
            )));
        }
        let installation =
            db.query_row("SELECT value FROM meta WHERE key='installation'", [], |r| {
                r.get(0)
            })?;
        File::open(state_dir)?.sync_all()?;
        Ok(Self {
            db: Mutex::new(db),
            state_dir: state_dir.into(),
            installation,
            mutation: Mutex::new(()),
            clock: Mutex::new(Instant::now()),
        })
    }
    pub fn settings(&self) -> Result<Settings> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row("SELECT value FROM meta WHERE key='settings'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(raw
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default())
    }
    /// Advisory prerequisites never prevent accepting the enable switch.
    pub fn set_settings(&self, s: &Settings) -> Result<()> {
        if s.failed_retention_days > 3650 {
            return Err(Error::Conflict(
                "retention must be at most 3650 days".into(),
            ));
        }
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('settings',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(s)?])?;
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Artifact> {
        read(&self.db.lock().unwrap(), "artifacts", key)
    }
    pub fn for_job(&self, job: u32) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE job=?1 ORDER BY updated_at DESC LIMIT 1",
                [job],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn for_path(&self, path: &Path) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE path=?1 ORDER BY updated_at DESC LIMIT 1",
                [path.to_string_lossy()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn list(&self, offset: usize, limit: usize) -> Result<Vec<Artifact>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db
            .prepare("SELECT data FROM artifacts ORDER BY updated_at DESC,id LIMIT ?1 OFFSET ?2")?;
        let rows = stmt.query_map(params![limit.min(200) as i64, offset as i64], |r| {
            r.get::<_, String>(0)
        })?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn events(&self, key: &str, after: i64) -> Result<Vec<serde_json::Value>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT seq,at,kind,detail FROM events WHERE artifact=?1 AND seq>?2 ORDER BY seq LIMIT 100")?;
        let rows = stmt.query_map(params![key,after], |r| Ok(serde_json::json!({"seq":r.get::<_,i64>(0)?,"at":r.get::<_,i64>(1)?,"kind":r.get::<_,String>(2)?,"detail":r.get::<_,String>(3)?})))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
    fn sidecar(&self, a: &Artifact) -> Result<()> {
        let dir = self.state_dir.join("artifact-identities").join(&a.id);
        std::fs::create_dir_all(&dir)?;
        let target = dir.join(format!("{}.json", a.generation));
        let tmp = dir.join(format!("{}.tmp", a.generation));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let result = (|| {
            file.write_all(&serde_json::to_vec(&serde_json::json!({"installation":self.installation,"artifact":a.id,"generation":a.generation,"path":a.path,"root":a.root,"root_identity":a.root_identity,"identity":a.identity}))?)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &target)?;
            File::open(&dir)?.sync_all()?;
            File::open(dir.parent().unwrap())?.sync_all()?;
            File::open(&self.state_dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(tmp);
        }
        result
    }
    /// Commit allocation intent before the directory exists or a writer opens
    /// a file. A directory already on disk must be explicitly adopted instead.
    pub fn allocate(&self, job: u32, root: &Path, path: &Path) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        if let Some(a) = self.for_job(job)? {
            if a.path == path && a.state == "active" {
                self.verify(&a)?;
                return Ok(a);
            }
            return Err(Error::Conflict(
                "job already owns a different or terminal allocation".into(),
            ));
        }
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "allocation must be an immediate child of its root".into(),
            ));
        }
        let root_file = fs::open_dir(root)?;
        let root_identity = fs::identity(&root_file.metadata()?);
        if std::fs::symlink_metadata(path).is_ok() {
            return Err(Error::Conflict(
                "existing payload needs explicit ownership review".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let mut a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: Some(job),
            path: path.into(),
            root: root.into(),
            root_identity,
            identity: None,
            state: "allocating".into(),
            owned: true,
            keep: false,
            hold: None,
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            eligible_seconds: 0,
            files: Vec::new(),
            error: None,
        };
        save_artifact(&db, &a)?;
        drop(db);
        std::fs::create_dir(path)?;
        root_file.sync_all()?;
        a.identity = Some(fs::identity(&fs::open_dir(path)?.metadata()?));
        self.sidecar(&a)?;
        a.state = "active".into();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    /// Upgrade a live queue record without claiming its pre-existing bytes.
    /// It may resume writing; destructive actions still need operator adoption.
    pub fn register_legacy_active(&self, job: u32, root: &Path, path: &Path) -> Result<()> {
        if self.for_job(job)?.is_some() || !path.exists() {
            return Ok(());
        }
        let mut a = self.discover(root, path, true)?;
        let _guard = self.mutation.lock().unwrap();
        a.job = Some(job);
        a.state = "active".into();
        save_artifact(&self.db.lock().unwrap(), &a)
    }

    pub fn begin_transition(&self, job: u32, destination: &Path) -> Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let mut a = self.for_job(job)?.ok_or(Error::NotFound)?;
        if a.hold.as_deref().is_some_and(|h| h != "review")
            || !matches!(a.state.as_str(), "active" | "transitioning")
        {
            return Err(Error::Conflict(
                "payload is not available to post-processing".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let op = Operation {
            id: format!("transition-{}-{}", a.id, a.revision),
            artifact: a.id.clone(),
            kind: "transition".into(),
            state: "running".into(),
            request: destination.to_string_lossy().into(),
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        a.state = "transitioning".into();
        a.revision += 1;
        save_operation(&tx, &op)?;
        save_artifact(&tx, &a)?;
        tx.commit()?;
        Ok(())
    }

    fn verify_root(&self, a: &Artifact) -> Result<File> {
        let root = fs::open_dir(&a.root)?;
        if !a
            .root_identity
            .same_object(&fs::identity(&root.metadata()?))
        {
            return Err(Error::Conflict(
                "configured root identity changed or volume is unavailable".into(),
            ));
        }
        Ok(root)
    }
    fn verify(&self, a: &Artifact) -> Result<File> {
        self.verify_root(a)?;
        let file = fs::open_dir(&a.path)?;
        let observed = fs::identity(&file.metadata()?);
        if !a
            .identity
            .as_ref()
            .is_some_and(|i| i.same_object(&observed))
        {
            return Err(Error::Conflict(
                "payload identity changed; review required".into(),
            ));
        }
        if a.owned {
            let marker = self
                .state_dir
                .join("artifact-identities")
                .join(&a.id)
                .join(format!("{}.json", a.generation));
            let data: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
            if data["installation"] != self.installation
                || data["artifact"] != a.id
                || data["generation"] != a.generation
                || data["path"] != a.path.to_string_lossy().as_ref()
                || data["identity"] != serde_json::to_value(&a.identity)?
            {
                return Err(Error::Conflict(
                    "external allocation identity does not match inventory".into(),
                ));
            }
        }
        Ok(file)
    }
    /// Called at a writer/PP quiescence boundary, before publishing History.
    pub fn finish(&self, job: u32, path: &Path, root: &Path, state: &str) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        let mut a = self.for_job(job)?.ok_or(Error::NotFound)?;
        if !matches!(a.state.as_str(), "active" | "transitioning") {
            return Err(Error::Conflict("allocation is not active".into()));
        }
        let dir = fs::open_dir(path)?;
        let root_file = fs::open_dir(root)?;
        a.path = path.into();
        a.root = root.into();
        a.root_identity = fs::identity(&root_file.metadata()?);
        a.identity = Some(fs::identity(&dir.metadata()?));
        a.files = fs::manifest(&dir, 100_000)?;
        a.state = state.into();
        a.revision += 1;
        a.updated_at = now();
        if state == "parked_failed" && a.owned {
            a.retention_seconds = u64::from(self.settings()?.failed_retention_days) * 86400;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
            a.eligible_seconds = 0;
        }
        self.sidecar(&a)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, &a.id, "finalized", state)?;
        tx.commit()?;
        Ok(a)
    }
    /// Capture an unknown directory without granting deletion authority.
    pub fn discover(&self, root: &Path, path: &Path, active: bool) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "discovery must be an immediate child".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let existing: Option<String> = db.query_row("SELECT data FROM artifacts WHERE path=?1 AND state NOT IN ('deleted','source_gone') LIMIT 1", [path.to_string_lossy()], |r| r.get(0)).optional()?;
        if let Some(raw) = existing {
            return Ok(serde_json::from_str(&raw)?);
        }
        let dir = fs::open_dir(path)?;
        let root_file = fs::open_dir(root)?;
        let a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: None,
            path: path.into(),
            root: root.into(),
            root_identity: fs::identity(&root_file.metadata()?),
            identity: Some(fs::identity(&dir.metadata()?)),
            state: if active {
                "active_unverified"
            } else {
                "unknown"
            }
            .into(),
            owned: false,
            keep: true,
            hold: Some("review".into()),
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            eligible_seconds: 0,
            files: Vec::new(),
            error: None,
        };
        save_artifact(&db, &a)?;
        Ok(a)
    }
    pub fn inspect(&self, key: &str) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        let mut a = self.get(key)?;
        let dir = self.verify(&a)?;
        if a.state == "active" || a.state == "active_unverified" {
            return Err(Error::Conflict(
                "writers must finish before inspection".into(),
            ));
        }
        a.files = fs::manifest(&dir, 100_000)?;
        a.updated_at = now();
        a.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    pub fn adopt(&self, key: &str, revision: u64) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        let mut a = self.get(key)?;
        if a.revision != revision
            || a.owned
            || !matches!(
                a.state.as_str(),
                "unknown" | "retained" | "parked_failed" | "completed"
            )
        {
            return Err(Error::Conflict(
                "stale preview or artifact is not unknown".into(),
            ));
        }
        let dir = self.verify(&a)?;
        let files = fs::manifest(&dir, 100_000)?;
        if files != a.files {
            return Err(Error::Conflict(
                "files changed; inspect before adopting".into(),
            ));
        }
        a.owned = true;
        a.keep = true;
        a.hold = None;
        a.state = "retained".into();
        a.revision += 1;
        self.sidecar(&a)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, key, "adopted", "kept indefinitely")?;
        tx.commit()?;
        Ok(a)
    }
    pub fn retention(
        &self,
        key: &str,
        revision: u64,
        keep: bool,
        seconds: Option<u64>,
    ) -> Result<Artifact> {
        let _guard = self.mutation.lock().unwrap();
        let mut a = self.get(key)?;
        if a.revision != revision || matches!(a.state.as_str(), "deleting" | "deleted") {
            return Err(Error::Conflict(
                "stale revision or deletion already started".into(),
            ));
        }
        a.keep = keep;
        if let Some(seconds) = seconds {
            if seconds > 315_360_000 {
                return Err(Error::Conflict("retention exceeds 3650 days".into()));
            }
            a.retention_seconds = seconds;
        }
        a.eligible_seconds = 0;
        a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        a.revision += 1;
        a.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if keep {
            let mut stmt =
                tx.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='queued'")?;
            let ops = stmt
                .query_map([key], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(stmt);
            for raw in ops {
                let mut op: Operation = serde_json::from_str(&raw)?;
                op.state = "cancelled".into();
                save_operation(&tx, &op)?;
            }
        }
        save_artifact(&tx, &a)?;
        event(
            &tx,
            key,
            "retention",
            if keep {
                "Keep"
            } else {
                "fresh retention period"
            },
        )?;
        tx.commit()?;
        Ok(a)
    }
    pub fn operation(&self, key: &str) -> Result<Operation> {
        read(&self.db.lock().unwrap(), "operations", key)
    }
    pub fn request_delete(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
    ) -> Result<Operation> {
        let _guard = self.mutation.lock().unwrap();
        if request_id.is_empty() || request_id.len() > 128 || undo_seconds > 60 {
            return Err(Error::Conflict(
                "valid idempotency key and undo of 0–60 seconds required".into(),
            ));
        }
        let request = serde_json::to_string(&(key, revision, undo_seconds))?;
        match self.operation(request_id) {
            Ok(op) => {
                return if op.request == request {
                    Ok(op)
                } else {
                    Err(Error::Conflict(
                        "idempotency key reused with different request".into(),
                    ))
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        let a = self.get(key)?;
        if a.revision != revision
            || !a.owned
            || a.keep
            || a.hold.is_some()
            || !matches!(a.state.as_str(), "parked_failed" | "retained" | "completed")
        {
            return Err(Error::Conflict(
                "stale revision, active payload, Keep or recovery/review hold".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let pending:Option<String>=tx.query_row("SELECT data FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry') LIMIT 1",[key],|r|r.get(0)).optional()?;
        if let Some(raw) = pending {
            return Ok(serde_json::from_str(&raw)?);
        }
        let op = Operation {
            id: request_id.into(),
            artifact: key.into(),
            kind: "delete".into(),
            state: "queued".into(),
            request,
            created_at: now(),
            not_before: now() + undo_seconds as i64,
            attempts: 0,
            next_retry: 0,
            error: None,
        };
        save_operation(&tx, &op)?;
        event(&tx, key, "delete_requested", request_id)?;
        tx.commit()?;
        Ok(op)
    }
    pub fn cancel_delete(&self, key: &str) -> Result<Operation> {
        let _guard = self.mutation.lock().unwrap();
        let mut op = self.operation(key)?;
        if op.state != "queued" {
            return Err(Error::Conflict(
                "deletion already started or finished".into(),
            ));
        }
        op.state = "cancelled".into();
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(op)
    }
    pub fn execute_delete(&self, key: &str) -> Result<Operation> {
        let _guard = self.mutation.lock().unwrap();
        let mut op = self.operation(key)?;
        if matches!(op.state.as_str(), "succeeded" | "cancelled" | "review")
            || op.not_before > now()
            || op.next_retry > now()
        {
            return Ok(op);
        }
        let mut a = self.get(&op.artifact)?;
        if a.keep || a.hold.is_some() || !a.owned {
            return Err(Error::Conflict("payload acquired a hold".into()));
        }
        op.state = "running".into();
        op.attempts += 1;
        a.state = "deleting".into();
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &a)?;
            tx.commit()?;
        }
        let result = (|| {
            let root = self.verify_root(&a)?;
            let dir = match self.verify(&a) {
                Ok(dir) => dir,
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::NotFound && op.attempts > 1 =>
                {
                    return Ok(())
                }
                Err(e) => return Err(e),
            };
            let observed = fs::manifest(&dir, 100_000)?;
            // A restarted partial deletion may have fewer entries, never more
            // or different ones. Every surviving regular file is revalidated.
            for f in &observed {
                if !a.files.iter().any(|owned| {
                    owned.path == f.path
                        && owned.identity.same_object(&f.identity)
                        && (f.identity.directory || owned.identity == f.identity)
                }) {
                    return Err(Error::Conflict(format!(
                        "unowned or changed entry: {}",
                        f.path
                    )));
                }
            }
            let mut files = observed;
            files.sort_by_key(|f| std::cmp::Reverse(f.path.matches('/').count()));
            for f in &files {
                if !f.identity.directory {
                    fs::remove_entry(&dir, f)?;
                }
            }
            for f in &files {
                if f.identity.directory {
                    fs::remove_entry(&dir, f)?;
                }
            }
            if !fs::names(&dir)?.is_empty() {
                return Err(Error::Conflict("new files appeared during deletion".into()));
            }
            if a.path.parent() != Some(a.root.as_path()) {
                return Err(Error::Conflict(
                    "payload no longer an immediate child of root".into(),
                ));
            }
            // Recheck the entry itself; unlinkat cannot follow a replacement.
            let current = fs::open_at(&root, Path::new(a.path.file_name().unwrap()), true)?;
            if !fs::identity(&current.metadata()?).same_object(a.identity.as_ref().unwrap()) {
                return Err(Error::Conflict("payload directory replaced".into()));
            }
            fs::unlink(&root, Path::new(a.path.file_name().unwrap()), true)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                op.state = "succeeded".into();
                op.error = None;
                a.state = "deleted".into();
                a.error = None;
            }
            Err(e) => {
                let conflict = matches!(e, Error::Conflict(_));
                op.error = Some(e.to_string());
                a.error = op.error.clone();
                op.state = if conflict { "review" } else { "retry" }.into();
                a.state = "delete_failed".into();
                if conflict {
                    a.hold = Some("review".into());
                }
                op.next_retry =
                    now() + [60, 300, 1800, 21600][(op.attempts.saturating_sub(1) as usize).min(3)];
            }
        }
        a.updated_at = now();
        a.revision += 1;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_operation(&tx, &op)?;
        save_artifact(&tx, &a)?;
        event(
            &tx,
            &a.id,
            &op.state,
            op.error.as_deref().unwrap_or("deletion confirmed"),
        )?;
        tx.commit()?;
        Ok(op)
    }
    /// Count only daemon monotonic uptime observed while eligible. Reset the
    /// checkpoint before the transaction: a failed commit loses time safely.
    pub fn tick(&self) -> Result<()> {
        let elapsed = {
            let mut clock = self.clock.lock().unwrap();
            let elapsed = clock.elapsed().as_secs();
            if elapsed == 0 {
                return Ok(());
            }
            *clock = Instant::now();
            elapsed
        };
        let enabled = self.settings()?.enabled;
        let mut due = Vec::new();
        {
            let _guard = self.mutation.lock().unwrap();
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let raws = {
                let mut stmt =
                    tx.prepare("SELECT data FROM artifacts WHERE state='parked_failed'")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            for raw in raws {
                let mut a: Artifact = serde_json::from_str(&raw)?;
                if enabled && a.eligible() {
                    a.eligible_seconds = a
                        .eligible_seconds
                        .saturating_add(elapsed)
                        .min(a.retention_seconds);
                    save_artifact(&tx, &a)?;
                    if a.eligible_seconds >= a.retention_seconds
                        && a.deadline.is_some_and(|d| d <= now())
                    {
                        due.push((a.id, a.revision));
                    }
                }
            }
            tx.commit()?;
        }
        for (key, revision) in due {
            self.request_delete(&key, revision, &format!("retention-{key}-{revision}"), 0)?;
        }
        let pending = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT id FROM operations WHERE state IN ('queued','running','retry') AND json_extract(data,'$.kind')='delete' LIMIT 25",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for key in pending {
            if let Err(e) = self.execute_delete(&key) {
                tracing::warn!(operation=%key,error=%e,"artifact deletion remains pending");
            }
        }
        Ok(())
    }
    /// Explicit restore quarantine; run before serving requests from a restored
    /// backup. Old retention and deletion intents cannot become active again.
    pub fn quarantine_restore(&self) -> Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            let mut stmt = tx.prepare(
                "SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            a.hold = Some("restored backup: review required".into());
            a.keep = true;
            a.eligible_seconds = 0;
            a.deadline = None;
            a.revision += 1;
            save_artifact(&tx, &a)?;
        }
        let ops = {
            let mut stmt = tx.prepare(
                "SELECT data FROM operations WHERE state IN ('queued','running','retry')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in ops {
            let mut op: Operation = serde_json::from_str(&raw)?;
            op.state = "review".into();
            op.error = Some("restored backup; old deletion invalidated".into());
            save_operation(&tx, &op)?;
        }
        event(
            &tx,
            "installation",
            "restore",
            "all outstanding operations quarantined",
        )?;
        tx.commit()?;
        Ok(())
    }
}
