#![allow(clippy::upper_case_acronyms)]

use crate::helpers::{deserialize, set_path_access};
use crate::migration::Migration;
use crate::query::rows::RowOwned;
use crate::snapshot_metrics::{SnapshotOperation, SnapshotTimer};
use crate::store::state_machine::sqlite::TypeConfigSqlite;
use crate::store::state_machine::sqlite::param::Param;
use crate::store::state_machine::sqlite::snapshot_builder::{
    SQLiteSnapshotBuilder, SnapshotFileState, SnapshotPointer, clear_pending_snapshot,
    load_current_snapshot, load_pending_snapshot, publish_current_snapshot,
    publish_pending_snapshot, snapshots_cleanup, sync_directory, sync_file,
};
use crate::store::state_machine::sqlite::writer::WriterRequest::MetadataRead;
use crate::store::state_machine::sqlite::writer::{
    self, MetaPersistRequest, SqlBatch, SqlTransaction, WriterRequest,
};
use crate::store::{StorageResult, logs};
use crate::{Error, Node, NodeId};
use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, OptionalSend, Snapshot, SnapshotId, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use rusqlite::functions::FunctionFlags;
use rusqlite::{OpenFlags, OptionalExtension, ToSql};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::clone::Clone;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLY_PAUSED: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLY_PAUSE_OBSERVED: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLY_NOTIFY: tokio::sync::Notify = tokio::sync::Notify::const_new();
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLIED_BLANK: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLIED_MEMBERSHIP: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_APPLIED_NORMAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_SQL_CLASSES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
#[cfg(feature = "validation-test-helpers")]
static VALIDATION_SQL_CLASS_COUNTS: std::sync::OnceLock<Vec<std::sync::atomic::AtomicU64>> =
    std::sync::OnceLock::new();

/// Pause this validation process immediately before its next SQLite
/// state-machine apply. The feature is absent from production builds.
#[cfg(feature = "validation-test-helpers")]
pub fn validation_pause_apply() {
    VALIDATION_APPLY_PAUSE_OBSERVED.store(false, Ordering::Release);
    VALIDATION_APPLY_PAUSED.store(true, Ordering::Release);
}

#[cfg(feature = "validation-test-helpers")]
pub fn validation_apply_pause_observed() -> bool {
    VALIDATION_APPLY_PAUSE_OBSERVED.load(Ordering::Acquire)
}

/// Counts of Raft entries this process's SQLite state machine has applied,
/// as `(blank, membership, normal)`. Blank entries are OpenRaft's
/// leader-establishment commits and membership entries are configuration
/// changes; neither runs SQL, so a drill that counts committed indexes cannot
/// tell them apart from acknowledged writes without this breakdown. The
/// counters are process-local, monotonic, and absent from production builds.
#[cfg(feature = "validation-test-helpers")]
pub fn validation_applied_payload_counts() -> (u64, u64, u64) {
    (
        VALIDATION_APPLIED_BLANK.load(Ordering::Acquire),
        VALIDATION_APPLIED_MEMBERSHIP.load(Ordering::Acquire),
        VALIDATION_APPLIED_NORMAL.load(Ordering::Acquire),
    )
}

/// Register the needles that classify applied normal entries by the SQL they
/// carry, in reporting order. Set-once per process, before the node starts,
/// so every counted entry saw the same classes; later calls are ignored and
/// return false. A needle starting with `^` matches a statement whose
/// (trimmed) text begins with the rest of the needle; any other needle
/// matches as a plain substring. An applied normal entry increments the
/// count of the FIRST class matching any of its statements, so classes
/// should be disjoint in practice. Exact-count drills use this to attribute
/// background writers (a lease CAS, a membership heartbeat) by name instead
/// of failing on them or absorbing them in slack.
#[cfg(feature = "validation-test-helpers")]
pub fn validation_register_applied_sql_classes(classes: &[&str]) -> bool {
    let owned: Vec<String> = classes.iter().map(|class| (*class).to_owned()).collect();
    let count = owned.len();
    let registered = VALIDATION_SQL_CLASSES.set(owned).is_ok();
    if registered {
        let counters = (0..count)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        VALIDATION_SQL_CLASS_COUNTS
            .set(counters)
            .ok()
            .expect("sql class counters follow their classes");
    }
    registered
}

/// The per-class applied-entry counts, in registration order. Empty when no
/// classes were registered.
#[cfg(feature = "validation-test-helpers")]
pub fn validation_applied_sql_class_counts() -> Vec<u64> {
    VALIDATION_SQL_CLASS_COUNTS
        .get()
        .map(|counters| {
            counters
                .iter()
                .map(|counter| counter.load(Ordering::Acquire))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(feature = "validation-test-helpers")]
fn validation_classify_applied_sql(payload: &QueryWrite) {
    let Some(classes) = VALIDATION_SQL_CLASSES.get() else {
        return;
    };
    let Some(counters) = VALIDATION_SQL_CLASS_COUNTS.get() else {
        return;
    };
    let statement_matches = |sql: &str, needle: &str| match needle.strip_prefix('^') {
        Some(prefix) => sql.trim_start().starts_with(prefix),
        None => sql.contains(needle),
    };
    let matches = |needle: &str| match payload {
        QueryWrite::Execute(query) | QueryWrite::ExecuteReturning(query) => {
            statement_matches(&query.sql, needle)
        }
        QueryWrite::Transaction(queries) => queries
            .iter()
            .any(|query| statement_matches(&query.sql, needle)),
        QueryWrite::Batch(sql) => statement_matches(sql, needle),
        _ => false,
    };
    if let Some(position) = classes.iter().position(|class| matches(class)) {
        counters[position].fetch_add(1, Ordering::Release);
    }
}

#[cfg(feature = "validation-test-helpers")]
pub fn validation_resume_apply() {
    VALIDATION_APPLY_PAUSED.store(false, Ordering::Release);
    VALIDATION_APPLY_PAUSE_OBSERVED.store(false, Ordering::Release);
    VALIDATION_APPLY_NOTIFY.notify_waiters();
}

#[cfg(feature = "validation-test-helpers")]
async fn validation_wait_for_apply() {
    while VALIDATION_APPLY_PAUSED.load(Ordering::Acquire) {
        // Register the waiter before publishing that the pause was observed.
        // `Notify::notify_waiters()` does not retain a permit, so enabling the
        // future closes the lost-wakeup window between the flag recheck and
        // the await.
        let notified = VALIDATION_APPLY_NOTIFY.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        VALIDATION_APPLY_PAUSE_OBSERVED.store(true, Ordering::Release);
        if !VALIDATION_APPLY_PAUSED.load(Ordering::Acquire) {
            break;
        }
        notified.as_mut().await;
    }
}
use tokio::{fs, task, time};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

type Entry = openraft::Entry<TypeConfigSqlite>;
type SnapshotData = tokio::fs::File;

#[cfg(any(feature = "backup", test))]
fn committed_backup_owner(log_id: &LogId<NodeId>) -> NodeId {
    log_id.leader_id.node_id
}

// TODO uses a `Mutex<_>` inside. We could make this pool a lot
//  faster by building our own lock-free one.
pub type SqlitePool = deadpool::unmanaged::Pool<rusqlite::Connection>;

pub type Params = Vec<Param>;

pub struct PathDb(pub String);
pub struct PathBackups(pub String);
pub struct PathSnapshots(pub String);
pub struct PathLockFile(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryWrite {
    Execute(Query),
    ExecuteReturning(Query),
    Transaction(Vec<Query>),
    Batch(Cow<'static, str>),
    Migration(Vec<Migration>),
    #[cfg(feature = "backup")]
    Backup((NodeId, i64)),
    RTT,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub sql: Cow<'static, str>,
    pub params: Params,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Empty,
    Execute(ResponseExecute),
    ExecuteReturning(ResponseExecuteReturning),
    Transaction(Result<Vec<Result<usize, Error>>, Error>),
    Batch(ResponseBatch),
    Migrate(Result<(), Error>),
    Backup(Result<(), Error>),
    RTT,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseExecute {
    pub result: Result<usize, Error>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseExecuteReturning {
    pub result: Result<Vec<Result<RowOwned, Error>>, Error>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseBatch {
    pub result: Result<Vec<Result<usize, Error>>, Error>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,
    pub path: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, Node>,
    pub last_snapshot_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StateMachineSqlite {
    // pub data: StateMachineData,
    this_node: NodeId,
    path_snapshots: String,
    #[cfg(feature = "backup")]
    path_backups: String,
    path_lock_file: String,
    path_db: String,
    filename_db: String,
    prepared_statement_cache_capacity: usize,
    read_pool_size: usize,
    snapshot_files: Arc<Mutex<SnapshotFileState>>,
    snapshot_recovery_pending: Arc<AtomicBool>,

    #[cfg(feature = "s3")]
    s3_config: Option<Arc<crate::s3::S3Config>>,

    pub(crate) read_pool: SqlitePool,
    pub(crate) write_tx: flume::Sender<WriterRequest>,
}

impl StateMachineSqlite {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        data_dir: &str,
        filename_db: &str,
        this_node: NodeId,
        log_statements: bool,
        prepared_statement_cache_capacity: usize,
        read_pool_size: usize,
        #[cfg(feature = "s3")] s3_config: Option<Arc<crate::s3::S3Config>>,
        do_reset_metadata: bool,
        #[cfg(feature = "backup")] local_backup_keep_days: u16,
    ) -> Result<StateMachineSqlite, StorageError<NodeId>> {
        // IMPORTANT: Do NOT change the order of the db exists check!
        // DB recovery will fail otherwise!
        let mut db_exists = Self::db_exists(data_dir, filename_db).await;
        debug!("db_exists in state_machine::new(): {db_exists}");

        let (
            PathDb(path_db),
            PathBackups(path_backups),
            PathSnapshots(path_snapshots),
            PathLockFile(path_lock_file),
        ) = Self::build_folders(data_dir, true).await;

        Self::check_set_lock_file(&path_lock_file, &path_db, &mut db_exists).await;

        // Always start the writer first! -> creates mandatory tables
        let conn = Self::connect(
            path_db.to_string(),
            filename_db.to_string(),
            false,
            prepared_statement_cache_capacity,
        )
        .await
        .map_err(|err| StorageError::IO {
            source: StorageIOError::write(&err),
        })?;
        let write_tx = writer::spawn_writer(
            conn,
            this_node,
            path_lock_file.clone(),
            log_statements,
            do_reset_metadata,
            #[cfg(feature = "backup")]
            local_backup_keep_days,
        );

        let read_pool = Self::connect_read_pool(
            path_db.as_ref(),
            filename_db,
            prepared_statement_cache_capacity,
            read_pool_size,
        )
        .await
        .map_err(|err| StorageError::IO {
            source: StorageIOError::read(&err),
        })?;

        let mut slf = Self {
            // data: state_machine_data,
            this_node,
            path_snapshots,
            #[cfg(feature = "backup")]
            path_backups,
            path_lock_file,
            path_db: path_db.clone(),
            filename_db: filename_db.to_owned(),
            prepared_statement_cache_capacity,
            read_pool_size,
            snapshot_files: Arc::new(Mutex::new(SnapshotFileState::default())),
            snapshot_recovery_pending: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "s3")]
            s3_config,
            read_pool,
            write_tx,
        };

        slf.initialize_snapshot_pointer(db_exists).await?;
        let recovered_pending = slf.recover_pending_snapshot().await?;

        if !db_exists
            && !recovered_pending
            && let Some(snapshot) = slf.read_current_snapshot().await?
        {
            slf.update_state_machine_(snapshot.path).await?;
        }

        Ok(slf)
    }

    async fn db_exists(data_dir: &str, filename_db: &str) -> bool {
        let path_db = Self::path_db(data_dir);
        let path_db_full = format!("{path_db}/{filename_db}");
        fs::File::open(&path_db_full).await.is_ok()
    }

    pub fn path_base(data_dir: &str) -> String {
        format!("{data_dir}/state_machine")
    }

    fn path_db(data_dir: &str) -> String {
        format!("{}/db", Self::path_base(data_dir))
    }

    pub async fn build_folders(
        data_dir: &str,
        create: bool,
    ) -> (PathDb, PathBackups, PathSnapshots, PathLockFile) {
        let path_base = Self::path_base(data_dir);

        let path_db = Self::path_db(data_dir);
        let path_backups = format!("{path_base}/backups");
        let path_snapshots = format!("{path_base}/snapshots");
        let path_lock_file = format!("{path_base}/lock");

        if create {
            // this may error if we did already re-create it in a lock file recovery before
            let _ = fs::create_dir_all(&path_db).await;
            set_path_access(&path_base, 0o700)
                .await
                .expect("Cannot set access rights for path_base");
            set_path_access(&path_db, 0o700)
                .await
                .expect("Cannot set access rights for path_db");

            fs::create_dir_all(&path_backups)
                .await
                .expect("create state machine folder backups");
            set_path_access(&path_backups, 0o700)
                .await
                .expect("Cannot set access rights for path_backups");

            fs::create_dir_all(&path_snapshots)
                .await
                .expect("create state machine folder snapshots");
            set_path_access(&path_snapshots, 0o700)
                .await
                .expect("Cannot set access rights for path_snapshots");
        }

        (
            PathDb(path_db),
            PathBackups(path_backups),
            PathSnapshots(path_snapshots),
            PathLockFile(path_lock_file),
        )
    }

    async fn check_set_lock_file(path_lock_file: &str, path_db: &str, db_exists: &mut bool) {
        let is_locked = fs::File::open(path_lock_file).await.is_ok();

        if is_locked {
            #[cfg(feature = "auto-heal")]
            {
                warn!(
                    "Lock file already exists: {path_lock_file}\n\
                    Node did not shut down gracefully - auto-rebuilding State Machine"
                );

                // if we can't create the lock file, we will delete the current state machine
                // data so it can be rebuilt.
                // TODO is it enough to delete DB only, or do we need to do a full wipe?
                let _ = fs::remove_dir_all(path_db).await;

                // re-create the DB folder
                if let Err(err) = fs::create_dir_all(path_db).await {
                    panic!("Cannot re-create DB folder {path_db}: {err}");
                }

                *db_exists = false;
            }

            #[cfg(not(feature = "auto-heal"))]
            panic!(
                "Lock file already exists: {}\n\
                Node did not shut down gracefully - needs manual interaction",
                path_lock_file
            );
        } else if let Err(err) = fs::File::create(path_lock_file).await {
            panic!("Error creating lock file {path_lock_file}: {err}");
        }
    }

    pub(crate) fn remove_lock_file(path: &str) {
        let _ = std::fs::remove_file(path);
    }

    pub async fn connect(
        path: String,
        filename_db: String,
        read_only: bool,
        prepared_statement_cache_capacity: usize,
    ) -> Result<rusqlite::Connection, Error> {
        task::spawn_blocking(move || {
            let path_full = format!("{path}/{filename_db}");
            let conn = rusqlite::Connection::open(path_full)?;

            Self::apply_pragmas(&conn, read_only, prepared_statement_cache_capacity)?;
            if !read_only {
                Self::overwrite_non_det_fns(&conn);
            }

            Ok(conn)
        })
        .await?
    }

    async fn connect_read_pool(
        path: &str,
        filename_db: &str,
        prepared_statement_cache_capacity: usize,
        pool_size: usize,
    ) -> Result<SqlitePool, Error> {
        let path_full = format!("{path}/{filename_db}");

        let mut conns = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let mut conn = Self::connect(
                path.to_string(),
                filename_db.to_string(),
                true,
                prepared_statement_cache_capacity,
            )
            .await;
            while conn.is_err() {
                time::sleep(Duration::from_millis(10)).await;
                conn = Self::connect(
                    path.to_string(),
                    filename_db.to_string(),
                    true,
                    prepared_statement_cache_capacity,
                )
                .await;
            }
            conns.push(conn?);
        }

        let pool = deadpool::unmanaged::Pool::from(conns);
        let conn = pool.get().await?;
        task::spawn_blocking(move || {
            let _ = conn.query_row("SELECT 1", (), |row| {
                let res: i64 = row.get(0)?;
                Ok(res)
            })?;
            Ok::<(), Error>(())
        })
        .await?;

        Ok(pool)
    }

    async fn reconnect_read_pool(
        read_pool: &SqlitePool,
        path_db: &str,
        filename_db: &str,
        prepared_statement_cache_capacity: usize,
        read_pool_size: usize,
    ) -> Result<(), StorageError<NodeId>> {
        let mut retired = Vec::with_capacity(read_pool_size);
        for _ in 0..read_pool_size {
            retired.push(read_pool.remove().await.map_err(|error| StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            })?);
        }
        drop(retired);

        for _ in 0..read_pool_size {
            let connection = Self::connect(
                path_db.to_owned(),
                filename_db.to_owned(),
                true,
                prepared_statement_cache_capacity,
            )
            .await
            .map_err(|error| StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            })?;
            if let Err((connection, error)) = read_pool.add(connection).await {
                drop(connection);
                return Err(StorageError::IO {
                    source: StorageIOError::read_state_machine(&error),
                });
            }
        }
        Ok(())
    }

    fn apply_pragmas(
        conn: &rusqlite::Connection,
        read_only: bool,
        prepared_statement_cache_capacity: usize,
    ) -> Result<(), rusqlite::Error> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // synchronous set to OFF is not an issue in our case.
        // If the OS crashes before it could flush any buffers to disk, we will rebuild the DB
        // anyway from the logs store just to be 100% sure that all cluster members are in a
        // consistent state. Setting it to OFF here gives us an ~18% boost compared to NORMAL while
        // not having any disadvantage with the Raft setup.
        conn.pragma_update(None, "synchronous", "OFF")?;

        conn.pragma_update(None, "page_size", 4096)?;
        conn.pragma_update(None, "journal_size_limit", 16384)?;
        conn.pragma_update(None, "wal_autocheckpoint", 4_000)?;

        // setting in-memory temp_store actually slows down SELECTs a little bit
        // conn.pragma_update(None, "temp_store", "memory")?;

        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
        conn.pragma_update(None, "optimize", "0x10002")?;

        // note:
        // in tests, `mmap_size` did not show any performance benefit with the settings above

        // only allow select statements
        if read_only {
            conn.pragma_update(None, "query_only", true)?;
        } else {
            // conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        }

        // TODO make configurable
        conn.set_prepared_statement_cache_capacity(prepared_statement_cache_capacity);

        Ok(())
    }

    fn overwrite_non_det_fns(conn: &rusqlite::Connection) {
        conn.create_scalar_function(
            "date",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `date()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "datetime",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `datetime()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "julianday",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `julianday()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                );
            },
        );
        conn.create_scalar_function(
            "now",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `now()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "random",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `random()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "randomblob",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `randomblob()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "strftime",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `strftime()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "time",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `time()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "timediff",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `timediff()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
        conn.create_scalar_function(
            "unixepoch",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_| -> rusqlite::Result<String> {
                panic!(
                    "forbidden usage of `unixepoch()` - non-deterministic functions must never be \
                    used for writing connections in a Raft cluster"
                )
            },
        );
    }

    async fn update_state_machine_(
        &mut self,
        snapshot_path: String,
    ) -> Result<(), StorageError<NodeId>> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send_async(WriterRequest::SnapshotApply((snapshot_path, tx)))
            .await
            .expect("SQLite Writer rx to always be listening");

        rx.await
            .expect("snapshot writer to return an apply result")?;
        Self::reconnect_read_pool(
            &self.read_pool,
            &self.path_db,
            &self.filename_db,
            self.prepared_statement_cache_capacity,
            self.read_pool_size,
        )
        .await
    }

    async fn initialize_snapshot_pointer(&mut self, db_exists: bool) -> StorageResult<()> {
        let snapshot_files = self.snapshot_files.clone();
        let mut snapshot_files_guard = snapshot_files.lock().await;
        let snapshot_id = match load_current_snapshot(&self.path_snapshots).await? {
            SnapshotPointer::Snapshot(snapshot_id) => Some(snapshot_id),
            SnapshotPointer::Empty => None,
            SnapshotPointer::Missing => {
                let snapshot_id = if db_exists {
                    match self.read_authoritative_snapshot_id().await? {
                        Some(snapshot_id) => {
                            match self.read_snapshot_metadata(&snapshot_id).await {
                                Ok(_) => Some(snapshot_id),
                                Err(error) => {
                                    warn!(
                                        "Ignoring invalid snapshot named by live database metadata: {error}"
                                    );
                                    self.find_legacy_current_snapshot_id(false).await?
                                }
                            }
                        }
                        None => None,
                    }
                } else {
                    self.find_legacy_current_snapshot_id(true).await?
                };
                if let Some(snapshot_id) = &snapshot_id {
                    self.read_snapshot_metadata(snapshot_id).await?;
                }
                publish_current_snapshot(&self.path_snapshots, snapshot_id.as_deref()).await?;
                snapshot_id
            }
        };
        if let Some(snapshot_id) = &snapshot_id {
            self.read_snapshot_metadata(snapshot_id).await?;
        }
        snapshot_files_guard.current_id = snapshot_id;
        Ok(())
    }

    async fn recover_pending_snapshot(&mut self) -> StorageResult<bool> {
        let snapshot_id = match load_pending_snapshot(&self.path_snapshots).await? {
            SnapshotPointer::Missing => return Ok(false),
            SnapshotPointer::Snapshot(snapshot_id) => snapshot_id,
            SnapshotPointer::Empty => unreachable!("pending loader rejects empty pointers"),
        };

        self.snapshot_recovery_pending
            .store(true, Ordering::Release);
        let snapshot_files = self.snapshot_files.clone();
        let mut snapshot_files_guard = snapshot_files.lock().await;
        snapshot_files_guard.pending_id = Some(snapshot_id.clone());
        self.read_snapshot_metadata(&snapshot_id).await?;
        let path = format!("{}/{}", self.path_snapshots, snapshot_id);
        self.update_state_machine_(path).await?;
        publish_current_snapshot(&self.path_snapshots, Some(&snapshot_id)).await?;
        snapshot_files_guard.current_id = Some(snapshot_id);
        clear_pending_snapshot(&self.path_snapshots).await?;
        snapshot_files_guard.pending_id = None;
        self.snapshot_recovery_pending
            .store(false, Ordering::Release);
        Ok(true)
    }

    async fn read_authoritative_snapshot_id(&self) -> StorageResult<Option<String>> {
        let (ack, rx) = oneshot::channel();
        self.write_tx
            .send_async(WriterRequest::MetadataRead(ack))
            .await
            .map_err(|error| StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            })?;
        let metadata = rx.await.map_err(|error| StorageError::IO {
            source: StorageIOError::read_state_machine(&error),
        })?;
        match metadata.last_snapshot_id {
            Some(snapshot_id) => Uuid::parse_str(&snapshot_id)
                .map(|id| Some(id.to_string()))
                .map_err(|error| StorageError::IO {
                    source: StorageIOError::read_state_machine(&error),
                }),
            None => Ok(None),
        }
    }

    async fn read_current_snapshot(&mut self) -> StorageResult<Option<StoredSnapshot>> {
        let snapshot_files = self.snapshot_files.clone();
        let mut snapshot_files_guard = snapshot_files.lock().await;
        self.read_current_snapshot_locked(&mut snapshot_files_guard)
            .await
    }

    async fn resolve_current_snapshot_id(
        &self,
        snapshot_files: &mut SnapshotFileState,
    ) -> StorageResult<Option<String>> {
        let snapshot_id = match load_current_snapshot(&self.path_snapshots).await? {
            SnapshotPointer::Snapshot(snapshot_id) => Some(snapshot_id),
            SnapshotPointer::Empty => None,
            SnapshotPointer::Missing => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "current snapshot pointer disappeared after initialization",
                );
                return Err(StorageError::IO {
                    source: StorageIOError::read_state_machine(&error),
                });
            }
        };

        if let Some(snapshot_id) = &snapshot_id {
            self.validate_snapshot_file(snapshot_id).await?;
        }

        snapshot_files.current_id.clone_from(&snapshot_id);
        Ok(snapshot_id)
    }

    async fn find_legacy_current_snapshot_id(
        &self,
        require_valid_candidate: bool,
    ) -> StorageResult<Option<String>> {
        let mut list = tokio::fs::read_dir(&self.path_snapshots)
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::read(&err),
            })?;

        let mut current: Option<(Option<LogId<NodeId>>, Uuid)> = None;
        let mut first_candidate_error = None;
        loop {
            let Some(entry) = list.next_entry().await.map_err(|error| StorageError::IO {
                source: StorageIOError::read(&error),
            })?
            else {
                break;
            };
            let file_name = entry.file_name();
            let name = file_name.to_str().unwrap_or("UNKNOWN");
            let id = match Uuid::parse_str(name) {
                Ok(uuid) => uuid,
                Err(_) => {
                    debug!("Non-UUID in snapshots folder");
                    continue;
                }
            };

            let meta = entry.metadata().await.map_err(|err| StorageError::IO {
                source: StorageIOError::read(&err),
            })?;
            if !meta.is_file() {
                warn!("Invalid folder in snapshots dir: {}", name);
                continue;
            }

            let snapshot_id = id.to_string();
            match self.read_snapshot_metadata(&snapshot_id).await {
                Ok(metadata) => {
                    let candidate = (metadata.last_applied_log_id, id);
                    if current.is_none_or(|best| candidate > best) {
                        current = Some(candidate);
                    }
                }
                Err(error) => {
                    warn!("Ignoring invalid legacy snapshot {snapshot_id}: {error}");
                    if first_candidate_error.is_none() {
                        first_candidate_error = Some(error);
                    }
                }
            }
        }

        match current {
            Some((_, id)) => Ok(Some(id.to_string())),
            None => match (require_valid_candidate, first_candidate_error) {
                (true, Some(error)) => Err(error),
                _ => Ok(None),
            },
        }
    }

    async fn validate_snapshot_file(&self, snapshot_id: &str) -> StorageResult<()> {
        let path = format!("{}/{}", self.path_snapshots, snapshot_id);
        let metadata = fs::metadata(&path)
            .await
            .map_err(|error| StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            })?;
        if metadata.is_file() {
            Ok(())
        } else {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("current snapshot is not a file: {path}"),
            );
            Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            })
        }
    }

    async fn read_snapshot_metadata(&self, snapshot_id: &str) -> StorageResult<StateMachineData> {
        self.validate_snapshot_file(snapshot_id).await?;
        let path_snapshot = format!("{}/{}", self.path_snapshots, snapshot_id);
        let expected_id = snapshot_id.to_owned();
        let path_debug = path_snapshot.clone();
        let metadata = task::spawn_blocking(move || {
            // Snapshot generations are immutable after fsync + rename. Inspect
            // them through a genuinely read-only connection and do not run the
            // live database's write-capable pragma setup on these files.
            let conn = rusqlite::Connection::open_with_flags(
                &path_debug,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|error| {
                Error::Sqlite(
                    format!("Error opening snapshot '{path_debug}' read-only: {error}").into(),
                )
            })?;
            let mut stmt = conn
                .prepare("SELECT data FROM _metadata WHERE key = 'meta'")
                .map_err(|error| {
                    Error::Sqlite(
                        format!(
                            "Error preparing metadata read from snapshot '{path_debug}': {error}"
                        )
                        .into(),
                    )
                })?;
            let meta_bytes =
                stmt.query_row((), |row| row.get::<_, Vec<u8>>(0))
                    .map_err(|error| {
                        Error::Sqlite(
                            format!("Error reading metadata from snapshot '{path_debug}': {error}")
                                .into(),
                        )
                    })?;
            let metadata: StateMachineData = deserialize(&meta_bytes).map_err(Error::from)?;
            if metadata.last_snapshot_id.as_deref() != Some(expected_id.as_str()) {
                return Err(Error::Sqlite(
                    format!(
                        "Snapshot '{path_debug}' metadata names {:?}, expected {expected_id}",
                        metadata.last_snapshot_id
                    )
                    .into(),
                ));
            }
            Ok::<StateMachineData, Error>(metadata)
        })
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::read_state_machine(&error),
        })?;

        metadata.map_err(|error| StorageError::IO {
            source: StorageIOError::read_state_machine(&error),
        })
    }

    async fn read_current_snapshot_locked(
        &mut self,
        snapshot_files: &mut SnapshotFileState,
    ) -> StorageResult<Option<StoredSnapshot>> {
        let Some(snapshot_id) = self.resolve_current_snapshot_id(snapshot_files).await? else {
            return Ok(None);
        };

        let path_snapshot = format!("{}/{}", self.path_snapshots, snapshot_id);
        let metadata = self.read_snapshot_metadata(&snapshot_id).await?;

        let meta = SnapshotMeta {
            last_log_id: metadata.last_applied_log_id,
            last_membership: metadata.last_membership,
            snapshot_id,
        };
        let snapshot = StoredSnapshot {
            meta,
            path: path_snapshot,
        };

        Ok(Some(snapshot))
    }
}

impl RaftStateMachine<TypeConfigSqlite> for StateMachineSqlite {
    type SnapshotBuilder = SQLiteSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "applied state unavailable until pending snapshot recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
        let (ack, rx) = oneshot::channel();
        self.write_tx
            .send_async(WriterRequest::MetadataRead(ack))
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::read(&err),
            })?;
        let data = rx.await.expect("To always get Metadata from DB");

        debug!("applied_state: {:?}", data);

        Ok((data.last_applied_log_id, data.last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        #[cfg(feature = "validation-test-helpers")]
        validation_wait_for_apply().await;
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "state-machine apply refused until pending snapshot recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        let entries = entries.into_iter();

        let (bound_lower, bound_upper) = entries.size_hint();
        let entries_len = bound_upper
            .expect("We always expect an upper bound to entries in apply()")
            - bound_lower
            + 1;
        let mut replies = Vec::with_capacity(entries_len);

        for entry in entries {
            #[cfg(feature = "backup")]
            let backup_owner = committed_backup_owner(&entry.log_id);
            let last_applied_log_id = Some(entry.log_id);

            #[cfg(feature = "validation-test-helpers")]
            {
                match &entry.payload {
                    EntryPayload::Blank => &VALIDATION_APPLIED_BLANK,
                    EntryPayload::Membership(_) => &VALIDATION_APPLIED_MEMBERSHIP,
                    EntryPayload::Normal(_) => &VALIDATION_APPLIED_NORMAL,
                }
                .fetch_add(1, Ordering::Release);
                if let EntryPayload::Normal(payload) = &entry.payload {
                    validation_classify_applied_sql(payload);
                }
                // Env-gated apply log: one line per applied entry with its
                // index and payload, so an exact-count investigation can name
                // the precise entry that contaminated a window. Off unless
                // PLURX_VALIDATION_LOG_APPLIED is set at process start.
                static VALIDATION_LOG_APPLIED: std::sync::OnceLock<bool> =
                    std::sync::OnceLock::new();
                if *VALIDATION_LOG_APPLIED
                    .get_or_init(|| std::env::var_os("PLURX_VALIDATION_LOG_APPLIED").is_some())
                {
                    let mut payload = match &entry.payload {
                        EntryPayload::Blank => "blank".to_owned(),
                        EntryPayload::Membership(_) => "membership".to_owned(),
                        EntryPayload::Normal(query) => format!("{query:?}"),
                    };
                    if payload.len() > 600 {
                        let mut cut = 600;
                        while !payload.is_char_boundary(cut) {
                            cut -= 1;
                        }
                        payload.truncate(cut);
                        payload.push('…');
                    }
                    eprintln!("validation-applied {} {payload}", entry.log_id.index);
                }
            }

            // TODO if we always collect 1 in-flight req in a temp var to always have 1 req prepared
            // before we await the rx before, we could probably improve the throughput here a bit
            // in exchange for a more complicated logic -> test!

            let resp = match entry.payload {
                // TODO we probably need to update the log id in writer in case of ::Empty?
                EntryPayload::Blank => Response::Empty,

                EntryPayload::Normal(QueryWrite::Execute(Query { sql, params })) => {
                    let (tx, rx) = oneshot::channel();
                    let query = writer::Query::Execute(writer::SqlExecute {
                        sql,
                        params,
                        last_applied_log_id,
                        tx,
                    });

                    self.write_tx
                        .send_async(WriterRequest::Query(query))
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::Execute(ResponseExecute { result })
                }

                EntryPayload::Normal(QueryWrite::ExecuteReturning(Query { sql, params })) => {
                    let (tx, rx) = oneshot::channel();
                    let query = writer::Query::ExecuteReturning(writer::SqlExecuteReturning {
                        sql,
                        params,
                        last_applied_log_id,
                        tx,
                    });

                    self.write_tx
                        .send_async(WriterRequest::Query(query))
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::ExecuteReturning(ResponseExecuteReturning { result })
                }

                EntryPayload::Normal(QueryWrite::Transaction(queries)) => {
                    let (tx, rx) = oneshot::channel();
                    let req = WriterRequest::Query(writer::Query::Transaction(SqlTransaction {
                        queries,
                        last_applied_log_id,
                        tx,
                    }));

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::Transaction(result)
                }

                EntryPayload::Normal(QueryWrite::Batch(sql)) => {
                    let (tx, rx) = oneshot::channel();
                    let req = WriterRequest::Query(writer::Query::Batch(SqlBatch {
                        sql,
                        last_applied_log_id,
                        tx,
                    }));

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::Batch(ResponseBatch { result })
                }

                #[cfg(feature = "backup")]
                EntryPayload::Normal(QueryWrite::Backup((_requested_node_id, ts))) => {
                    let (ack, rx) = oneshot::channel();
                    let req = WriterRequest::Backup(writer::BackupRequest {
                        node_id: backup_owner,
                        target_folder: self.path_backups.clone(),
                        ts,
                        #[cfg(feature = "s3")]
                        s3_config: self.s3_config.clone(),
                        last_applied_log_id,
                        ack,
                    });

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::Backup(result)
                }

                EntryPayload::Normal(QueryWrite::Migration(migrations)) => {
                    let (tx, rx) = oneshot::channel();
                    let req = WriterRequest::Migrate(writer::Migrate {
                        migrations,
                        last_applied_log_id,
                        tx,
                    });

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    let result = rx.await.expect("to always get a response from sql writer");
                    Response::Migrate(result)
                }

                EntryPayload::Normal(QueryWrite::RTT) => {
                    let (ack, rx) = oneshot::channel();
                    let req = WriterRequest::RTT(writer::RTTRequest {
                        last_applied_log_id,
                        ack,
                    });

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    rx.await.expect("to always get a response from sql writer");
                    Response::RTT
                }

                EntryPayload::Membership(mem) => {
                    let (ack, rx) = oneshot::channel();
                    let req = WriterRequest::MetadataMembership(writer::MetaMembershipRequest {
                        last_membership: StoredMembership::new(Some(entry.log_id), mem),
                        last_applied_log_id,
                        ack,
                    });

                    self.write_tx
                        .send_async(req)
                        .await
                        .expect("sql writer to always be listening");

                    rx.await.expect("to always get a response from sql writer");

                    Response::Empty
                }
            };

            replies.push(resp);
        }

        Ok(replies)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // TODO clean up possibly existing restore files inside snapshot builder upon success

        SQLiteSnapshotBuilder {
            #[cfg(feature = "backup")]
            path_backups: self.path_backups.clone(),
            path_snapshots: self.path_snapshots.clone(),
            write_tx: self.write_tx.clone(),
            snapshot_files: self.snapshot_files.clone(),
            snapshot_recovery_pending: self.snapshot_recovery_pending.clone(),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<fs::File>, StorageError<NodeId>> {
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "snapshot receive refused until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        let snapshot_files = self.snapshot_files.clone();
        let _snapshot_files_guard = snapshot_files.lock().await;
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "snapshot receive refused until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        let path = format!("{}/temp", self.path_snapshots);

        // clean up possible existing old data
        let _ = fs::remove_file(&path).await;

        match fs::File::create(path).await {
            Ok(file) => Ok(Box::new(file)),
            Err(err) => Err(StorageError::IO {
                source: StorageIOError::write(&err),
            }),
        }
    }

    #[tracing::instrument(level = "trace", skip(self, _snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        _snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let timer = SnapshotTimer::start(SnapshotOperation::Install);
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "snapshot install refused until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        let snapshot_files = self.snapshot_files.clone();
        let snapshot_files_guard = snapshot_files.clone().lock_owned().await;
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "snapshot install refused until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        let snapshot_id = Uuid::parse_str(&meta.snapshot_id)
            .map_err(|error| StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            })?
            .to_string();
        let src = format!("{}/temp", self.path_snapshots);
        let dest = format!("{}/{}", self.path_snapshots, snapshot_id);
        let staged = format!("{dest}.installing");
        let _ = fs::remove_file(&staged).await;
        fs::copy(&src, &staged)
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::write(&err),
            })?;
        sync_file(&staged).await?;
        fs::rename(&staged, &dest)
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::write_state_machine(&err),
            })?;
        sync_directory(&self.path_snapshots).await?;
        let candidate_metadata = self.read_snapshot_metadata(&snapshot_id).await?;
        if candidate_metadata.last_applied_log_id != meta.last_log_id
            || candidate_metadata.last_membership != meta.last_membership
        {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "received snapshot metadata does not match its Raft envelope",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
        drop(_snapshot);

        // Once the durable snapshot file exists, finish validating, applying,
        // and publishing it even if OpenRaft cancels the caller's future. The
        // ownership lock prevents readers and cleanup from observing an
        // intermediate state, and the pointer advances only after restore has
        // accepted the candidate.
        let write_tx = self.write_tx.clone();
        let read_pool = self.read_pool.clone();
        let path_db = self.path_db.clone();
        let filename_db = self.filename_db.clone();
        let prepared_statement_cache_capacity = self.prepared_statement_cache_capacity;
        let read_pool_size = self.read_pool_size;
        let path_snapshots = self.path_snapshots.clone();
        let snapshot_recovery_pending = self.snapshot_recovery_pending.clone();
        #[cfg(feature = "backup")]
        let path_backups = self.path_backups.clone();
        snapshot_recovery_pending.store(true, Ordering::Release);
        task::spawn(async move {
            let mut snapshot_files_guard = snapshot_files_guard;
            if let Err(publish_error) =
                publish_pending_snapshot(&path_snapshots, &snapshot_id).await
            {
                match load_pending_snapshot(&path_snapshots).await {
                    Ok(SnapshotPointer::Snapshot(durable_id)) if durable_id == snapshot_id => {
                        warn!(
                            "Pending snapshot pointer rename completed before durability error; continuing guarded recovery: {publish_error}"
                        );
                    }
                    _ => {
                        snapshot_recovery_pending.store(false, Ordering::Release);
                        return Err(publish_error);
                    }
                }
            }
            snapshot_files_guard.pending_id = Some(snapshot_id.clone());
            let (tx, rx) = oneshot::channel();
            write_tx
                .send_async(WriterRequest::SnapshotApply((dest, tx)))
                .await
                .expect("SQLite Writer rx to always be listening");
            rx.await
                .expect("snapshot writer to return an apply result")?;
            StateMachineSqlite::reconnect_read_pool(
                &read_pool,
                &path_db,
                &filename_db,
                prepared_statement_cache_capacity,
                read_pool_size,
            )
            .await?;
            publish_current_snapshot(&path_snapshots, Some(&snapshot_id)).await?;
            snapshot_files_guard.current_id = Some(snapshot_id);
            clear_pending_snapshot(&path_snapshots).await?;
            let _ = fs::remove_file(src).await;
            snapshot_files_guard.pending_id = None;
            snapshot_recovery_pending.store(false, Ordering::Release);
            drop(snapshot_files_guard);
            task::spawn(snapshots_cleanup(
                path_snapshots,
                #[cfg(feature = "backup")]
                path_backups,
                snapshot_files,
            ));
            Ok::<(), StorageError<NodeId>>(())
        })
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })??;
        timer.success();
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfigSqlite>>, StorageError<NodeId>> {
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "current snapshot unavailable until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
        let snapshot_files = self.snapshot_files.clone();
        let mut snapshot_files_guard = snapshot_files.lock().await;
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "current snapshot unavailable until pending recovery completes",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
        match self
            .read_current_snapshot_locked(&mut snapshot_files_guard)
            .await?
        {
            None => Ok(None),
            Some(snap) => {
                let file = fs::File::open(&snap.path)
                    .await
                    .map_err(|err| StorageError::IO {
                        source: StorageIOError::read(&err),
                    })?;

                Ok(Some(Snapshot {
                    meta: snap.meta,
                    snapshot: Box::new(file),
                }))
            }
        }
    }
}

#[cfg(test)]
mod backup_owner_contracts {
    use super::committed_backup_owner;
    use openraft::{CommittedLeaderId, LogId};

    #[test]
    fn backup_owner_follows_the_accepting_leader_after_a_client_handoff() {
        let stale_client_sample = 1;
        let accepted = LogId::new(CommittedLeaderId::new(7, 2), 41);

        assert_eq!(committed_backup_owner(&accepted), 2);
        assert_ne!(committed_backup_owner(&accepted), stale_client_sample);
    }
}

#[cfg(test)]
mod snapshot_metrics_contracts {
    use super::*;
    use crate::LocalDbSnapshotMetrics;
    use crate::helpers::serialize;
    use crate::store::state_machine::sqlite::snapshot_builder::{
        CURRENT_PUBLICATION_RENAMED, RELEASE_CURRENT_PUBLICATION,
        inject_current_publication_failure,
    };
    use openraft::{CommittedLeaderId, RaftSnapshotBuilder};
    use tokio::io::AsyncWriteExt;

    async fn new_test_state(
        root: &str,
        filename: &str,
    ) -> Result<StateMachineSqlite, StorageError<NodeId>> {
        StateMachineSqlite::new(
            root,
            filename,
            1,
            false,
            16,
            1,
            #[cfg(feature = "s3")]
            None,
            false,
            #[cfg(feature = "backup")]
            30,
        )
        .await
    }

    async fn write_snapshot_fixture(path: String, snapshot_id: &str, applied_index: u64) {
        let snapshot_id = snapshot_id.to_owned();
        task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(path).expect("open snapshot fixture");
            conn.execute(
                "CREATE TABLE _metadata (key TEXT PRIMARY KEY, data BLOB NOT NULL)",
                (),
            )
            .expect("create snapshot metadata table");
            let metadata = StateMachineData {
                last_applied_log_id: Some(LogId::new(CommittedLeaderId::new(3, 1), applied_index)),
                last_membership: StoredMembership::default(),
                last_snapshot_id: Some(snapshot_id),
            };
            let bytes = serialize(&metadata).expect("serialize snapshot metadata");
            conn.execute(
                "INSERT INTO _metadata (key, data) VALUES ('meta', ?1)",
                [bytes],
            )
            .expect("insert snapshot metadata");
        })
        .await
        .expect("write snapshot fixture task");
    }

    async fn shutdown_state(state: &StateMachineSqlite) {
        let (shutdown, shutdown_ack) = oneshot::channel();
        state
            .write_tx
            .send_async(WriterRequest::Shutdown(shutdown))
            .await
            .expect("request writer shutdown");
        shutdown_ack.await.expect("writer shutdown ack");
    }

    #[cfg(feature = "validation-test-helpers")]
    #[tokio::test]
    async fn validation_apply_resume_cannot_miss_the_registered_waiter() {
        validation_pause_apply();
        let waiter = tokio::spawn(validation_wait_for_apply());

        while !validation_apply_pause_observed() {
            tokio::task::yield_now().await;
        }
        validation_resume_apply();

        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("registered apply waiter must observe resume")
            .expect("apply waiter task must complete");
    }

    #[tokio::test]
    async fn snapshot_metrics_real_build_install_outcomes() {
        let root =
            std::env::temp_dir().join(format!("hiqlite-snapshot-metrics-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).await.expect("create test root");
        let root = root.to_str().expect("UTF-8 temp path").to_owned();
        let mut state = new_test_state(&root, "metrics.db")
            .await
            .expect("create SQLite state machine");
        let handle = LocalDbSnapshotMetrics::new();
        let before = handle.snapshot();

        let mut builder = state.get_snapshot_builder().await;
        let snapshot = builder
            .build_snapshot()
            .await
            .expect("build real SQLite snapshot");
        let snapshot_path = format!("{}/{}", state.path_snapshots, snapshot.meta.snapshot_id);
        let receive_path = format!("{}/temp", state.path_snapshots);
        fs::copy(&snapshot_path, &receive_path)
            .await
            .expect("stage real snapshot install");
        let receive = fs::File::open(&receive_path)
            .await
            .expect("open staged snapshot");
        state
            .install_snapshot(&snapshot.meta, Box::new(receive))
            .await
            .expect("install real SQLite snapshot");

        // A canceled build/install can leave a fully renamed UUID file. It is
        // not current until the durable pointer names it, even when its UUID
        // sorts after the installed snapshot.
        let newer_orphan_id = "ffffffff-ffff-7fff-bfff-ffffffffffff";
        let newer_orphan_path = format!("{}/{}", state.path_snapshots, newer_orphan_id);
        fs::copy(&snapshot_path, &newer_orphan_path)
            .await
            .expect("create newer interrupted publication");
        let current = state
            .get_current_snapshot()
            .await
            .expect("read current snapshot")
            .expect("current snapshot exists");
        assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);
        drop(current);

        let mut failing_builder = state.get_snapshot_builder().await;
        failing_builder.path_snapshots = format!("{root}/missing/snapshots");
        assert!(failing_builder.build_snapshot().await.is_err());

        let rejected_id = "aaaaaaaa-aaaa-7aaa-aaaa-aaaaaaaaaaaa";
        fs::write(&receive_path, b"not a SQLite snapshot")
            .await
            .expect("stage corrupt snapshot install");
        let placeholder = fs::File::open(&receive_path)
            .await
            .expect("open corrupt snapshot placeholder");
        let missing_meta = SnapshotMeta {
            last_log_id: snapshot.meta.last_log_id,
            last_membership: snapshot.meta.last_membership.clone(),
            snapshot_id: rejected_id.to_owned(),
        };
        assert!(
            state
                .install_snapshot(&missing_meta, Box::new(placeholder))
                .await
                .is_err()
        );
        assert_eq!(
            load_current_snapshot(&state.path_snapshots)
                .await
                .expect("load pointer after rejected restore"),
            SnapshotPointer::Snapshot(snapshot.meta.snapshot_id.clone())
        );
        let current = state
            .get_current_snapshot()
            .await
            .expect("read current snapshot after rejected restore")
            .expect("current snapshot survives rejected restore");
        assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);
        drop(current);

        let after = handle.snapshot();
        assert_eq!(after.build_ok.count, before.build_ok.count + 1);
        assert_eq!(after.install_ok.count, before.install_ok.count + 1);
        assert_eq!(after.build_error.count, before.build_error.count + 1);
        assert_eq!(after.install_error.count, before.install_error.count + 1);

        shutdown_state(&state).await;

        drop(state);
        fs::write(format!("{root}/state_machine/lock"), b"simulate crash")
            .await
            .expect("create crash-recovery lock");
        let mut restarted = new_test_state(&root, "metrics.db")
            .await
            .expect("restart SQLite state machine");
        let current = restarted
            .get_current_snapshot()
            .await
            .expect("read current snapshot after restart")
            .expect("current snapshot exists after restart");
        assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);
        drop(current);
        shutdown_state(&restarted).await;
        fs::remove_dir_all(&root).await.expect("remove test root");
    }

    #[tokio::test]
    async fn snapshot_metrics_legacy_pointer_uses_applied_index_and_survives_auto_heal() {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-snapshot-legacy-recovery-{}",
            Uuid::now_v7()
        ));
        let snapshots = root.join("state_machine/snapshots");
        fs::create_dir_all(&snapshots)
            .await
            .expect("create legacy snapshot directory");
        let installed_id = "018f0000-0000-7000-8000-000000000001";
        let newer_local_id = "019f0000-0000-7000-8000-000000000002";
        write_snapshot_fixture(
            snapshots.join(installed_id).display().to_string(),
            installed_id,
            200,
        )
        .await;
        write_snapshot_fixture(
            snapshots.join(newer_local_id).display().to_string(),
            newer_local_id,
            100,
        )
        .await;
        let installed_bytes_before = fs::read(snapshots.join(installed_id))
            .await
            .expect("read immutable installed snapshot before migration");

        let root = root.to_str().expect("UTF-8 legacy root").to_owned();
        let mut state = new_test_state(&root, "legacy.db")
            .await
            .expect("recover legacy snapshots");
        let current = state
            .get_current_snapshot()
            .await
            .expect("read migrated snapshot")
            .expect("migrated snapshot exists");
        assert_eq!(current.meta.snapshot_id, installed_id);
        assert_eq!(current.meta.last_log_id.map(|id| id.index), Some(200));
        drop(current);
        assert_eq!(
            fs::read(format!("{}/{}", state.path_snapshots, installed_id))
                .await
                .expect("read immutable installed snapshot after migration"),
            installed_bytes_before
        );
        assert!(!snapshots.join(format!("{installed_id}-wal")).exists());
        assert!(!snapshots.join(format!("{installed_id}-shm")).exists());
        shutdown_state(&state).await;
        drop(state);

        fs::write(format!("{root}/state_machine/lock"), b"simulate crash")
            .await
            .expect("create legacy auto-heal lock");
        let mut restarted = new_test_state(&root, "legacy.db")
            .await
            .expect("auto-heal migrated snapshot");
        let current = restarted
            .get_current_snapshot()
            .await
            .expect("read auto-healed snapshot")
            .expect("auto-healed snapshot exists");
        assert_eq!(current.meta.snapshot_id, installed_id);
        assert_eq!(current.meta.last_log_id.map(|id| id.index), Some(200));
        drop(current);
        shutdown_state(&restarted).await;
        fs::remove_dir_all(&root)
            .await
            .expect("remove legacy recovery root");
    }

    #[tokio::test]
    async fn snapshot_metrics_legacy_pointer_prefers_live_database_metadata() {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-snapshot-legacy-live-db-{}",
            Uuid::now_v7()
        ));
        let snapshots = root.join("state_machine/snapshots");
        let database = root.join("state_machine/db");
        fs::create_dir_all(&snapshots)
            .await
            .expect("create legacy snapshot directory");
        fs::create_dir_all(&database)
            .await
            .expect("create legacy database directory");
        let live_id = "018f0000-0000-7000-8000-000000000001";
        let higher_index_orphan_id = "019f0000-0000-7000-8000-000000000002";
        write_snapshot_fixture(snapshots.join(live_id).display().to_string(), live_id, 100).await;
        write_snapshot_fixture(
            snapshots.join(higher_index_orphan_id).display().to_string(),
            higher_index_orphan_id,
            200,
        )
        .await;
        fs::copy(snapshots.join(live_id), database.join("legacy.db"))
            .await
            .expect("create live legacy database");

        let root = root.to_str().expect("UTF-8 legacy root").to_owned();
        let mut state = new_test_state(&root, "legacy.db")
            .await
            .expect("migrate live database snapshot pointer");
        let current = state
            .get_current_snapshot()
            .await
            .expect("read live database snapshot")
            .expect("live database snapshot exists");
        assert_eq!(current.meta.snapshot_id, live_id);
        assert_eq!(
            load_current_snapshot(&state.path_snapshots)
                .await
                .expect("load migrated live pointer"),
            SnapshotPointer::Snapshot(live_id.to_owned())
        );
        drop(current);
        shutdown_state(&state).await;
        fs::remove_dir_all(&root)
            .await
            .expect("remove live legacy root");
    }

    #[tokio::test]
    async fn snapshot_metrics_legacy_failed_build_falls_back_before_publication() {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-snapshot-legacy-failed-build-{}",
            Uuid::now_v7()
        ));
        let snapshots = root.join("state_machine/snapshots");
        let database = root.join("state_machine/db");
        fs::create_dir_all(&snapshots)
            .await
            .expect("create failed-build snapshot directory");
        fs::create_dir_all(&database)
            .await
            .expect("create failed-build database directory");
        let fallback_id = "018f0000-0000-7000-8000-000000000001";
        let missing_failed_id = "019f0000-0000-7000-8000-000000000002";
        write_snapshot_fixture(
            snapshots.join(fallback_id).display().to_string(),
            fallback_id,
            100,
        )
        .await;
        write_snapshot_fixture(
            database.join("legacy.db").display().to_string(),
            missing_failed_id,
            200,
        )
        .await;

        let root = root.to_str().expect("UTF-8 failed-build root").to_owned();
        let mut state = new_test_state(&root, "legacy.db")
            .await
            .expect("migrate failed legacy build");
        assert_eq!(
            load_current_snapshot(&state.path_snapshots)
                .await
                .expect("load failed-build fallback pointer"),
            SnapshotPointer::Snapshot(fallback_id.to_owned())
        );
        let current = state
            .get_current_snapshot()
            .await
            .expect("read failed-build fallback")
            .expect("failed-build fallback exists");
        assert_eq!(current.meta.snapshot_id, fallback_id);
        drop(current);
        shutdown_state(&state).await;
        fs::remove_dir_all(&root)
            .await
            .expect("remove failed-build root");
    }

    #[tokio::test]
    async fn snapshot_metrics_pending_generation_recovers_after_cancelled_promotion() {
        let root = std::env::temp_dir().join(format!(
            "hiqlite-snapshot-pending-recovery-{}",
            Uuid::now_v7()
        ));
        fs::create_dir_all(&root)
            .await
            .expect("create pending root");
        let root = root.to_str().expect("UTF-8 pending root").to_owned();
        let mut state = new_test_state(&root, "pending.db")
            .await
            .expect("create pending state machine");
        let candidate_id = "028f0000-0000-7000-8000-000000000001";
        let receive_path = format!("{}/temp", state.path_snapshots);
        write_snapshot_fixture(receive_path.clone(), candidate_id, 300).await;
        let receive = fs::File::open(&receive_path)
            .await
            .expect("open pending candidate");
        let candidate_meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 1), 300)),
            last_membership: StoredMembership::default(),
            snapshot_id: candidate_id.to_owned(),
        };
        let mut receiving_state = state.clone();

        let renamed = CURRENT_PUBLICATION_RENAMED.notified();
        tokio::pin!(renamed);
        inject_current_publication_failure(&state.path_snapshots);
        let install = task::spawn(async move {
            let result = state
                .install_snapshot(&candidate_meta, Box::new(receive))
                .await;
            (state, result)
        });
        renamed.await;
        assert!(
            receiving_state.begin_receiving_snapshot().await.is_err(),
            "replacement receive must fail closed during pending recovery"
        );
        drop(receiving_state);
        install.abort();
        RELEASE_CURRENT_PUBLICATION.notify_one();
        assert!(
            install
                .await
                .expect_err("cancel install caller")
                .is_cancelled()
        );

        let lock_path = format!("{root}/state_machine/lock");
        time::timeout(Duration::from_secs(5), async {
            while fs::metadata(&lock_path).await.is_ok() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached publication exits and writer releases lock");

        let mut restarted = new_test_state(&root, "pending.db")
            .await
            .expect("recover durable pending generation");
        let current = restarted
            .get_current_snapshot()
            .await
            .expect("read recovered pending snapshot")
            .expect("recovered pending snapshot exists");
        assert_eq!(current.meta.snapshot_id, candidate_id);
        assert_eq!(current.meta.last_log_id.map(|id| id.index), Some(300));
        drop(current);
        assert_eq!(
            load_pending_snapshot(&restarted.path_snapshots)
                .await
                .expect("pending pointer cleared after recovery"),
            SnapshotPointer::Missing
        );
        let mut replacement = restarted
            .begin_receiving_snapshot()
            .await
            .expect("begin replacement receive after recovery");
        replacement
            .write_all(b"replacement snapshot bytes")
            .await
            .expect("write replacement receive bytes");
        replacement
            .sync_all()
            .await
            .expect("sync replacement receive bytes");
        drop(replacement);
        assert_eq!(
            fs::read(format!("{}/temp", restarted.path_snapshots))
                .await
                .expect("read replacement receive after recovery"),
            b"replacement snapshot bytes"
        );
        shutdown_state(&restarted).await;
        fs::remove_dir_all(&root)
            .await
            .expect("remove pending recovery root");
    }
}
