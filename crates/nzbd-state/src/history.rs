//! Job history (ARCHITECTURE.md §8.6 / ADR-16).
//!
//! Two layers: **append-only JSONL** files on the (possibly shared)
//! volume — the crash-safe, mergeable source of truth — and a **local
//! SQLite index** (rusqlite bundled) rebuilt from the JSONL when empty.
//! SQLite never lives on a network filesystem (ADR-16). In cluster mode
//! every node appends to its OWN `history.<node>.jsonl` (cross-client
//! O_APPEND interleaving on Gluster is not trustworthy); readers union
//! all `history*.jsonl` files, deduped by (job, completed_at).

use crate::{fsx, HistoryEntry, StateError};
use rusqlite::Connection;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Every column a [`HistoryEntry`] is read from, in the order the row
/// mapper expects. `id` (the cursor `seq`) is appended last so adding it
/// did not renumber the existing indices.
const COLUMNS: &str = "job_id, name, category, final_dir, status, size, health, params,
                       dupe_key, dupe_score, completed_at, hidden, first_seen, last_seen,
                       seen_count, removed_at, picked_up_by, id, stages";

/// How much history to keep. `0` disables that bound; whichever bound
/// bites first wins. See [`nzbd_config::HistorySection`] for why there are
/// two of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub keep_max: u32,
    pub keep_days: u32,
}

impl Retention {
    pub const UNLIMITED: Retention = Retention {
        keep_max: 0,
        keep_days: 0,
    };

    pub fn is_unlimited(&self) -> bool {
        self.keep_max == 0 && self.keep_days == 0
    }

    /// Entries completed before this instant are out of the age window.
    fn age_cutoff(&self, now: i64) -> Option<i64> {
        (self.keep_days > 0).then(|| now - (self.keep_days as i64) * 86_400)
    }
}

impl Default for Retention {
    fn default() -> Self {
        Retention::UNLIMITED
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub struct HistoryDb {
    conn: Mutex<Connection>,
    jsonl: Option<PathBuf>,
    /// Local spool for the regenerated NZBs of deleted jobs (`nzbs/<job>.nzb`
    /// beside the SQLite index). Local, not shared: it is a convenience for
    /// undoing a delete on the node that served the click, not cluster state.
    spool: Option<PathBuf>,
    last_refresh: Mutex<Option<Instant>>,
    /// Serializes appends against compaction. The JSONL has exactly one
    /// appending *process* (its own node), but several threads inside it;
    /// a tmp+rename landing between a thread's `open_append` and its write
    /// would drop that line into the unlinked old inode.
    jsonl_write: Mutex<()>,
    retention: Mutex<Retention>,
    last_prune: Mutex<Option<Instant>>,
    /// Cached listing of the parked-NZB spool, so deriving `can_requeue`
    /// for a page of history costs ONE `read_dir` instead of one `stat`
    /// per row. See [`HistoryDb::spooled_ids`].
    spool_cache: Mutex<Option<(Instant, std::collections::HashSet<u32>)>>,
}

impl HistoryDb {
    /// `db_path` = local SQLite file; `jsonl_dir` = directory for the
    /// authoritative `history.jsonl` (pass the shared volume in cluster
    /// mode, or the local state dir single-node).
    pub fn open(db_path: &Path, jsonl_dir: Option<&Path>) -> Result<HistoryDb, StateError> {
        Self::open_tagged(db_path, jsonl_dir, None)
    }

    /// Cluster form: this node appends to `history.<tag>.jsonl`, so
    /// concurrent PP executors never share an append fd. Reads union every
    /// `history*.jsonl` in the directory.
    pub fn open_tagged(
        db_path: &Path,
        jsonl_dir: Option<&Path>,
        tag: Option<&str>,
    ) -> Result<HistoryDb, StateError> {
        if let Some(parent) = db_path.parent() {
            fsx::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)
            .map_err(|e| StateError::Corrupt(format!("sqlite open {}: {e}", db_path.display())))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS history (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 job_id INTEGER NOT NULL,
                 name TEXT NOT NULL,
                 category TEXT,
                 final_dir TEXT,
                 status TEXT NOT NULL,
                 size INTEGER NOT NULL,
                 health INTEGER NOT NULL DEFAULT 1000,
                 params TEXT NOT NULL DEFAULT '[]',
                 dupe_key TEXT NOT NULL DEFAULT '',
                 dupe_score INTEGER NOT NULL DEFAULT 0,
                 completed_at INTEGER NOT NULL,
                 UNIQUE(job_id, completed_at)
             );",
        )
        .map_err(|e| StateError::Corrupt(format!("sqlite schema: {e}")))?;
        // Older index files: add the params column in place (ignore "dup
        // column" — the JSONL stays authoritative either way).
        let _ = conn.execute(
            "ALTER TABLE history ADD COLUMN params TEXT NOT NULL DEFAULT '[]'",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE history ADD COLUMN dupe_key TEXT NOT NULL DEFAULT ''",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE history ADD COLUMN dupe_score INTEGER NOT NULL DEFAULT 0",
            [],
        );
        for ddl in [
            "ALTER TABLE history ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE history ADD COLUMN first_seen INTEGER",
            "ALTER TABLE history ADD COLUMN last_seen INTEGER",
            "ALTER TABLE history ADD COLUMN seen_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE history ADD COLUMN removed_at INTEGER",
            "ALTER TABLE history ADD COLUMN picked_up_by TEXT",
            "ALTER TABLE history ADD COLUMN stages TEXT NOT NULL DEFAULT '[]'",
        ] {
            let _ = conn.execute(ddl, []);
        }
        // Node-local scalars that are NOT part of the portable log. The
        // retention floor lives here: see `retention_floor`.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);",
        )
        .map_err(|e| StateError::Corrupt(format!("sqlite meta schema: {e}")))?;

        let jsonl = jsonl_dir.map(|d| {
            let file = match tag {
                Some(t) => format!("history.{t}.jsonl"),
                None => "history.jsonl".into(),
            };
            d.join(file)
        });
        let db = HistoryDb {
            conn: Mutex::new(conn),
            jsonl,
            spool: db_path.parent().map(|d| d.join("nzbs")),
            last_refresh: Mutex::new(None),
            jsonl_write: Mutex::new(()),
            retention: Mutex::new(Retention::UNLIMITED),
            last_prune: Mutex::new(None),
            spool_cache: Mutex::new(None),
        };
        db.rebuild_from_jsonl(false)?;
        db.sweep_spool();
        Ok(db)
    }

    /// Apply retention bounds and trim immediately, reporting how many
    /// entries went. Called once at boot with the configured `[history]`
    /// section; `record` re-trims on a throttle after that.
    ///
    /// Retention is deliberately NOT a constructor argument. `open` runs
    /// the first JSONL rebuild before anyone has said what the bounds are,
    /// so the honest sequence is "load everything the log holds, then trim
    /// it once we're told" — which is also what makes lowering the bound
    /// on an existing install take effect on the next boot rather than
    /// waiting for the next completed job.
    pub fn set_retention(&self, retention: Retention) -> Result<usize, StateError> {
        *self.retention.lock().unwrap() = retention;
        self.prune(unix_now())
    }

    fn retention(&self) -> Retention {
        *self.retention.lock().unwrap()
    }

    /// Pull in rows other nodes appended since open (cluster: call before
    /// serving history reads). Throttled — at most one JSONL re-union per
    /// 5 s no matter how often clients poll.
    pub fn refresh(&self) -> Result<(), StateError> {
        {
            let mut last = self.last_refresh.lock().unwrap();
            if last.is_some_and(|t| t.elapsed() < Duration::from_secs(5)) {
                return Ok(());
            }
            *last = Some(Instant::now());
        }
        self.rebuild_from_jsonl(true)
    }

    /// Import any JSONL rows the index doesn't have (fresh index after a
    /// leader failover, a wiped local disk, or another node's appends).
    /// Unions every `history*.jsonl` in the directory — duplicates are
    /// dropped by the (job, completed_at) unique index.
    ///
    /// `quiet` marks the routine poll-path refresh: the upsert reports
    /// conflict-updates as affected rows, so counting THOSE made every
    /// 5 s history poll log "index rebuilt imported=57" forever (field
    /// report 2026-07-25). New-row counts come from a real before/after
    /// row count; the poll path logs at debug even then.
    ///
    /// **Ingest honours the retention floor**, which is what makes a prune
    /// stick. Without it the rebuild is a machine for undoing retention:
    /// this node's own file is compacted, but another node's is not, and
    /// every poll would re-import what the last prune just dropped —
    /// exactly the resurrect-with-a-new-cursor shape that
    /// `docs/DEFECT_HISTORY_DELETE.md` documents for `delete`. The floor
    /// only ever rises, so it can't flap.
    fn rebuild_from_jsonl(&self, quiet: bool) -> Result<(), StateError> {
        let Some(own) = &self.jsonl else {
            return Ok(());
        };
        let Some(dir) = own.parent() else {
            return Ok(());
        };
        let mut files: Vec<PathBuf> = match fsx::read_dir(dir) {
            Ok(entries) => entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    let n = p.file_name().unwrap_or_default().to_string_lossy();
                    n.starts_with("history") && n.ends_with(".jsonl")
                })
                .collect(),
            Err(e) if e.is_not_found() => return Ok(()),
            Err(e) => return Err(e),
        };
        files.sort();
        let floor = self.ingest_floor()?;
        let before = self.row_count()?;
        let mut dropped = 0usize;
        for path in files {
            let Ok(file) = fsx::open(&path) else {
                continue;
            };
            for line in BufReader::new(file).split(b'\n') {
                let line = fsx::ctx(line, "read", &path)?;
                if line.is_empty() {
                    continue;
                }
                let Ok(entry) = serde_json::from_slice::<HistoryEntry>(&line) else {
                    continue; // torn tail / old format
                };
                if entry.completed_at_unix < floor {
                    dropped += 1;
                    continue; // outside retention — a prune already dropped it
                }
                self.insert(&entry, false)?;
            }
        }
        let imported = self.row_count()?.saturating_sub(before);
        if imported > 0 {
            if quiet {
                tracing::debug!(imported, "history index picked up new JSONL rows");
            } else {
                tracing::info!(imported, "history index rebuilt from JSONL");
            }
        }
        if dropped > 0 {
            tracing::debug!(dropped, floor, "history ingest skipped pre-retention rows");
        }
        Ok(())
    }

    /// The oldest `completed_at` ingest will accept: the higher of the
    /// stored prune watermark and the live age bound. Zero means "accept
    /// everything", which is what an install with retention off gets.
    fn ingest_floor(&self) -> Result<i64, StateError> {
        let stored = self.meta_get("retention_floor")?.unwrap_or(0);
        let age = self.retention().age_cutoff(unix_now()).unwrap_or(0);
        Ok(stored.max(age))
    }

    fn meta_get(&self, key: &str) -> Result<Option<i64>, StateError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0))
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(StateError::Corrupt(format!("sqlite meta get: {e}"))),
            })
    }

    /// Raise a monotone watermark. A floor that could fall would let the
    /// next rebuild re-import what the last prune dropped.
    fn meta_raise(&self, key: &str, value: i64) -> Result<(), StateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = MAX(meta.value, excluded.value)",
            rusqlite::params![key, value],
        )
        .map_err(|e| StateError::Corrupt(format!("sqlite meta set: {e}")))?;
        Ok(())
    }

    fn row_count(&self) -> Result<u64, StateError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM history", [], |r| r.get::<_, u64>(0))
            .map_err(|e| StateError::Corrupt(format!("sqlite count: {e}")))
    }

    /// Visible row count — what the pager divides into pages.
    pub fn count_filtered(&self, include_hidden: bool) -> Result<u64, StateError> {
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT COUNT(*) FROM history {}",
            if include_hidden {
                ""
            } else {
                "WHERE hidden = 0"
            }
        );
        conn.query_row(&sql, [], |r| r.get::<_, u64>(0))
            .map_err(|e| StateError::Corrupt(format!("sqlite count: {e}")))
    }

    // -----------------------------------------------------------------
    // Retention
    // -----------------------------------------------------------------

    /// Trim history to the configured bounds, returning how many entries
    /// were dropped.
    ///
    /// Three things move together, and skipping any one of them leaves the
    /// entry half-deleted:
    ///
    /// 1. **The index rows go**, by whichever bound bites — age first
    ///    (cheap, indexed), then count.
    /// 2. **This node's JSONL is compacted** to the surviving keys. Without
    ///    this the log grows forever and the *log's length*, not the row
    ///    count, is what makes a history read slow: every refresh re-reads
    ///    it end to end. 179 rows cost 3.1 s on nuc3 because of this.
    /// 3. **The watermark rises**, so ingest refuses to re-import what just
    ///    went (see `rebuild_from_jsonl`).
    ///
    /// `docs/DEFECT_HISTORY_DELETE.md` rejects "delete the JSONL line" as a
    /// fix for `delete`, and rightly: rewriting an append-only log on a
    /// shared volume *under other nodes' concurrent appends* to service a
    /// UI click. Compaction is not that. A node rewrites only the file it
    /// alone appends to (`history.<node>.jsonl` in a cluster, `history.jsonl`
    /// single-node), never a peer's; the swap is tmp+rename, so a concurrent
    /// reader on another node sees the whole old file or the whole new one;
    /// and `jsonl_write` serializes it against this process's own appends.
    /// The semantic question that defect is blocked on — does "forget" mean
    /// here or everywhere — is not answered here and not touched.
    pub fn prune(&self, now: i64) -> Result<usize, StateError> {
        let r = self.retention();
        if r.is_unlimited() {
            return Ok(0);
        }
        let removed_jobs: Vec<u32> = {
            let conn = self.conn.lock().unwrap();
            let mut gone: Vec<u32> = Vec::new();
            if let Some(cutoff) = r.age_cutoff(now) {
                let mut stmt = conn
                    .prepare("SELECT job_id FROM history WHERE completed_at < ?1")
                    .map_err(|e| StateError::Corrupt(e.to_string()))?;
                let ids = stmt
                    .query_map([cutoff], |row| row.get::<_, u32>(0))
                    .map_err(|e| StateError::Corrupt(e.to_string()))?;
                for id in ids {
                    gone.push(id.map_err(|e| StateError::Corrupt(e.to_string()))?);
                }
                drop(stmt);
                conn.execute("DELETE FROM history WHERE completed_at < ?1", [cutoff])
                    .map_err(|e| StateError::Corrupt(e.to_string()))?;
            }
            if r.keep_max > 0 {
                // Rank by the same order the UI lists in, so "the newest
                // keep_max" means the first keep_max rows of page one.
                let sql = "SELECT job_id FROM history WHERE id NOT IN
                             (SELECT id FROM history
                               ORDER BY completed_at DESC, id DESC LIMIT ?1)";
                let mut stmt = conn
                    .prepare(sql)
                    .map_err(|e| StateError::Corrupt(e.to_string()))?;
                let ids = stmt
                    .query_map([r.keep_max as i64], |row| row.get::<_, u32>(0))
                    .map_err(|e| StateError::Corrupt(e.to_string()))?;
                for id in ids {
                    gone.push(id.map_err(|e| StateError::Corrupt(e.to_string()))?);
                }
                drop(stmt);
                conn.execute(
                    "DELETE FROM history WHERE id NOT IN
                       (SELECT id FROM history ORDER BY completed_at DESC, id DESC LIMIT ?1)",
                    [r.keep_max as i64],
                )
                .map_err(|e| StateError::Corrupt(e.to_string()))?;
            }
            gone
        };
        *self.last_prune.lock().unwrap() = Some(Instant::now());
        if removed_jobs.is_empty() {
            return Ok(0);
        }
        // The parked NZB dies with the entry, same rule `delete` follows.
        for job in &removed_jobs {
            self.drop_spool(crate::JobId(*job));
        }
        // Raise the watermark to the oldest survivor. With no survivors
        // left, everything the log holds is out of bounds, so the age
        // cutoff is the honest floor.
        let floor = {
            let conn = self.conn.lock().unwrap();
            conn.query_row("SELECT MIN(completed_at) FROM history", [], |r| {
                r.get::<_, Option<i64>>(0)
            })
            .unwrap_or(None)
        }
        .or_else(|| r.age_cutoff(now));
        if let Some(floor) = floor {
            self.meta_raise("retention_floor", floor)?;
        }
        let compacted = self.compact_jsonl()?;
        tracing::info!(
            dropped = removed_jobs.len(),
            keep_max = r.keep_max,
            keep_days = r.keep_days,
            lines_dropped = compacted,
            "history trimmed to its retention bounds"
        );
        Ok(removed_jobs.len())
    }

    /// Prune on a throttle — `record` calls this, so a burst of finished
    /// jobs costs one trim, not one per job.
    fn maybe_prune(&self) {
        if self.retention().is_unlimited() {
            return;
        }
        {
            let last = self.last_prune.lock().unwrap();
            if last.is_some_and(|t| t.elapsed() < Duration::from_secs(60)) {
                return;
            }
        }
        // A failed trim is not worth failing the job that finished.
        if let Err(e) = self.prune(unix_now()) {
            tracing::warn!(error = %e, "history retention trim failed");
        }
    }

    /// Rewrite this node's own JSONL with only the lines whose entry still
    /// exists in the index. Returns how many lines were dropped.
    ///
    /// Every surviving line is kept, not just the newest per key. The
    /// reader is not purely last-line-wins — `removed_at` and
    /// `picked_up_by` merge with `COALESCE`, so collapsing a key's history
    /// to its final line would silently change what a rebuild reconstructs.
    /// Compaction is a size optimisation and must not be a semantic one.
    fn compact_jsonl(&self) -> Result<usize, StateError> {
        let Some(path) = &self.jsonl else {
            return Ok(0);
        };
        let live: std::collections::HashSet<(u32, i64)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT job_id, completed_at FROM history")
                .map_err(|e| StateError::Corrupt(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, u32>(0)?, r.get::<_, i64>(1)?)))
                .map_err(|e| StateError::Corrupt(e.to_string()))?;
            rows.filter_map(Result::ok).collect()
        };

        let _guard = self.jsonl_write.lock().unwrap();
        let file = match fsx::open(path) {
            Ok(f) => f,
            Err(e) if e.is_not_found() => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut kept: Vec<u8> = Vec::new();
        let mut dropped = 0usize;
        for line in BufReader::new(file).split(b'\n') {
            let line = fsx::ctx(line, "read", path)?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<HistoryEntry>(&line) {
                Ok(e) if live.contains(&(e.job.0, e.completed_at_unix)) => {
                    kept.extend_from_slice(&line);
                    kept.push(b'\n');
                }
                Ok(_) => dropped += 1,
                // A torn tail is not evidence to delete by. Keep it: the
                // reader already skips what it cannot parse.
                Err(_) => {
                    kept.extend_from_slice(&line);
                    kept.push(b'\n');
                }
            }
        }
        if dropped == 0 {
            return Ok(0);
        }
        // tmp+rename in the same directory: a reader (this node or a peer)
        // sees the whole old file or the whole new one, never a torn one.
        let tmp = path.with_extension("jsonl.compact");
        let mut f = fsx::create(&tmp)?;
        fsx::write_whole(&mut f, &kept, &tmp)?;
        fsx::sync_data(&f, &tmp)?;
        drop(f);
        fsx::rename(&tmp, path)?;
        Ok(dropped)
    }

    fn insert(&self, entry: &HistoryEntry, and_jsonl: bool) -> Result<bool, StateError> {
        self.insert_seq(entry, and_jsonl).map(|(new, _)| new)
    }

    /// `insert`, also reporting the row's cursor value — `(was_new, seq)`.
    /// The rowid is read back rather than taken from `last_insert_rowid`:
    /// after an upsert that *updated*, that function reports the last
    /// insert on the connection, which would be some other row. A consumer
    /// handed a wrong cursor silently skips history.
    fn insert_seq(&self, entry: &HistoryEntry, and_jsonl: bool) -> Result<(bool, i64), StateError> {
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute(
                "INSERT INTO history
                 (job_id, name, category, final_dir, status, size, health, params,
                  dupe_key, dupe_score, completed_at, hidden, removed_at, picked_up_by,
                  stages)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                 ON CONFLICT(job_id, completed_at) DO UPDATE SET
                   hidden = excluded.hidden,
                   removed_at = COALESCE(excluded.removed_at, history.removed_at),
                   picked_up_by = COALESCE(excluded.picked_up_by, history.picked_up_by)",
                rusqlite::params![
                    entry.job.0,
                    entry.name,
                    entry.category,
                    entry.final_dir,
                    entry.status,
                    entry.size as i64,
                    entry.health as i64,
                    serde_json::to_string(&entry.params).unwrap_or_else(|_| "[]".into()),
                    entry.dupe_key,
                    entry.dupe_score,
                    entry.completed_at_unix,
                    entry.hidden as i64,
                    entry.removed_at_unix,
                    entry.picked_up_by,
                    serde_json::to_string(&entry.stages).unwrap_or_else(|_| "[]".into()),
                ],
            )
            .map_err(|e| StateError::Corrupt(format!("sqlite insert: {e}")))?;
        let seq = conn
            .query_row(
                "SELECT id FROM history WHERE job_id = ?1 AND completed_at = ?2",
                rusqlite::params![entry.job.0, entry.completed_at_unix],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0);
        drop(conn);
        if n > 0 && and_jsonl {
            self.append_jsonl(entry)?;
        }
        Ok((n > 0, seq))
    }

    pub fn record(&self, entry: &HistoryEntry) -> Result<(), StateError> {
        self.insert(entry, true)?;
        self.maybe_prune();
        Ok(())
    }

    /// `record`, returning the entry's cursor value (`seq`) so a caller
    /// can publish "the row you want is at N" in the same breath as the
    /// event announcing it. This is what makes the `job_pp_finished`
    /// ordering guarantee usable: the consumer gets the event *and* the
    /// exact cursor, and never has to guess how far to page back.
    pub fn record_seq(&self, entry: &HistoryEntry) -> Result<i64, StateError> {
        let seq = self.insert_seq(entry, true).map(|(_, seq)| seq)?;
        // After the seq is in hand: a trim must never be able to change
        // the cursor value this call is about to publish.
        self.maybe_prune();
        Ok(seq)
    }

    /// Visible (non-hidden) entries — what NZBGet-compat clients see.
    pub fn list(&self, limit: usize) -> Result<Vec<HistoryEntry>, StateError> {
        self.list_filtered(limit, false)
    }

    pub fn list_filtered(
        &self,
        limit: usize,
        include_hidden: bool,
    ) -> Result<Vec<HistoryEntry>, StateError> {
        self.list_page(limit, 0, include_hidden)
    }

    /// One page of the newest-first view: `limit` entries starting at
    /// `offset`.
    ///
    /// Paging is server-side because the cost this bounds is server-side.
    /// The browser fetching 200 rows was never the expensive part — the
    /// expensive part is what the daemon does per row before it can answer
    /// (a spool lookup each, on a network state mount) and per request
    /// (re-union the JSONL). A page of 20 does 20 rows' worth of that; a
    /// client-side slice of a 200-row fetch does 200.
    ///
    /// The order is `completed_at DESC, id DESC` — the same order
    /// [`prune`](Self::prune) ranks by, so "kept by retention" and "on the
    /// first pages" mean the same thing.
    pub fn list_page(
        &self,
        limit: usize,
        offset: usize,
        include_hidden: bool,
    ) -> Result<Vec<HistoryEntry>, StateError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM history {} ORDER BY completed_at DESC, id DESC
             LIMIT ?1 OFFSET ?2",
            if include_hidden {
                ""
            } else {
                "WHERE hidden = 0"
            }
        );
        self.query(&sql, rusqlite::params![limit as i64, offset as i64])
    }

    /// The cursor form: every entry newer than `since_seq`, oldest first.
    ///
    /// Ascending and hidden-inclusive, both deliberately. Ascending
    /// because a consumer walking forward must be able to take the last
    /// row's `seq` as its next cursor — descending would make the cursor
    /// meaningless mid-page. Hidden-inclusive because "hidden" means some
    /// *other* client removed the entry after its own import; a second
    /// consumer's catch-up must still see that the job finished. This is
    /// the path that makes SSE loss harmless: the stream can drop
    /// anything, and `since_seq` still reconstructs it.
    ///
    /// **Known defect — `docs/DEFECT_HISTORY_DELETE.md`.** `delete` removes
    /// the index row but not the authoritative JSONL line, so the next
    /// `refresh` re-imports it with a *new, higher* rowid and a consumer
    /// paging forward is handed the same entry a second time under a later
    /// cursor. The duplicate is byte-identical, so **dedupe on
    /// `(job, completed_at)`** — the key this index is already unique on.
    /// Pinned by `delete_resurrects_the_entry_with_a_new_cursor`, which
    /// documents the wrong behavior on purpose; that doc has the two fix
    /// options and the semantic decision they wait on.
    pub fn list_since(
        &self,
        since_seq: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEntry>, StateError> {
        let sql = format!("SELECT {COLUMNS} FROM history WHERE id > ?1 ORDER BY id ASC LIMIT ?2");
        self.query(&sql, rusqlite::params![since_seq, limit as i64])
    }

    fn query(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<HistoryEntry>, StateError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        let rows = stmt
            .query_map(params, |r| {
                let params: String = r.get(7)?;
                Ok(HistoryEntry {
                    job: crate::JobId(r.get::<_, i64>(0)? as u32),
                    name: r.get(1)?,
                    category: r.get(2)?,
                    final_dir: r.get(3)?,
                    status: r.get(4)?,
                    size: r.get::<_, i64>(5)? as u64,
                    health: r.get::<_, i64>(6)? as u16,
                    params: serde_json::from_str(&params).unwrap_or_default(),
                    dupe_key: r.get(8)?,
                    dupe_score: r.get::<_, i64>(9)? as i32,
                    completed_at_unix: r.get(10)?,
                    hidden: r.get::<_, i64>(11)? != 0,
                    first_seen_at_unix: r.get(12)?,
                    last_seen_at_unix: r.get(13)?,
                    seen_count: r.get::<_, i64>(14)? as u32,
                    removed_at_unix: r.get(15)?,
                    picked_up_by: r.get(16)?,
                    seq: r.get(17)?,
                    // A row written before the column existed reads back
                    // as an empty timeline, never as a failed query.
                    stages: r
                        .get::<_, Option<String>>(18)?
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default(),
                })
            })
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| StateError::Corrupt(e.to_string()))?);
        }
        Ok(out)
    }

    /// A compat client's history poll listed these entries: record the
    /// observation (index-local; cheap, no JSONL churn).
    pub fn mark_seen(
        &self,
        jobs: &[crate::JobId],
        client: Option<&str>,
        now_unix: i64,
    ) -> Result<(), StateError> {
        let conn = self.conn.lock().unwrap();
        for job in jobs {
            conn.execute(
                "UPDATE history SET
                   first_seen = COALESCE(first_seen, ?2),
                   last_seen = ?2,
                   seen_count = seen_count + 1,
                   picked_up_by = COALESCE(?3, picked_up_by)
                 WHERE job_id = ?1",
                rusqlite::params![job.0, now_unix, client],
            )
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        }
        Ok(())
    }

    /// Hide an entry (NZBGet HistoryDelete semantics). When a client did
    /// it right after import, this IS the "imported" signal — stamp who.
    pub fn hide(
        &self,
        job: crate::JobId,
        by_client: Option<&str>,
        now_unix: i64,
    ) -> Result<bool, StateError> {
        self.set_hidden(job, true, by_client, Some(now_unix))
    }

    /// Un-hide: the entry reappears in compat history, so a connected
    /// *arr will see it again and re-import.
    pub fn restore(&self, job: crate::JobId) -> Result<bool, StateError> {
        self.set_hidden(job, false, None, None)
    }

    fn set_hidden(
        &self,
        job: crate::JobId,
        hidden: bool,
        by_client: Option<&str>,
        removed_at: Option<i64>,
    ) -> Result<bool, StateError> {
        let changed = {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE history SET hidden = ?2,
                   removed_at = CASE WHEN ?2 = 1 THEN ?3 ELSE NULL END,
                   picked_up_by = COALESCE(?4, picked_up_by)
                 WHERE job_id = ?1",
                rusqlite::params![job.0, hidden as i64, removed_at, by_client],
            )
            .map_err(|e| StateError::Corrupt(e.to_string()))?
                > 0
        };
        if changed {
            // Re-append the updated entry so the hidden state survives an
            // index rebuild (the upsert makes the last JSONL line win).
            if let Some(entry) = self
                .list_filtered(10_000, true)?
                .into_iter()
                .find(|e| e.job == job)
            {
                let _ = self.append_jsonl(&entry);
            }
        }
        Ok(changed)
    }

    fn append_jsonl(&self, entry: &HistoryEntry) -> Result<(), StateError> {
        if let Some(path) = &self.jsonl {
            if let Some(parent) = path.parent() {
                fsx::create_dir_all(parent)?;
            }
            // Held across open+write+fsync: a compaction's rename landing
            // mid-append would write this line into the unlinked old inode.
            let _guard = self.jsonl_write.lock().unwrap();
            let mut f = fsx::open_append(path)?;
            // The JSONL is the portable, mergeable source of truth and is
            // re-imported into indices that assign their own rowids —
            // writing this node's `seq` into it would persist a number
            // that is wrong everywhere else. Zero it on the way out; the
            // read path fills it in from the row it actually came from.
            let mut portable = entry.clone();
            portable.seq = 0;
            let mut line = serde_json::to_vec(&portable)?;
            line.push(b'\n');
            fsx::write_all(&mut f, &line, path)?;
            fsx::sync_data(&f, path)?;
        }
        Ok(())
    }

    /// Forget an entry.
    ///
    /// **Known defect — `docs/DEFECT_HISTORY_DELETE.md`.** This removes the
    /// index row only. The JSONL line survives, and the next `refresh`
    /// puts the entry back with a fresh rowid, so the delete does not
    /// stick — the UI's "forget" is undone within one history poll and
    /// `list_since` reports the entry twice. `hide` re-appends its change
    /// to the log for exactly this reason; `delete` has no equivalent, and
    /// giving it one is a cross-node semantic decision rather than a
    /// mechanical fix. Read that doc before changing this function.
    pub fn delete(&self, job: crate::JobId) -> Result<bool, StateError> {
        let n = {
            let conn = self.conn.lock().unwrap();
            conn.execute("DELETE FROM history WHERE job_id = ?1", [job.0])
                .map_err(|e| StateError::Corrupt(e.to_string()))?
        };
        // The record and its spooled NZB live and die together: an entry
        // nobody can see must not leave a file behind.
        self.drop_spool(job);
        Ok(n > 0)
    }

    // -----------------------------------------------------------------
    // NZB spool: deleting a queued job parks its regenerated NZB here so
    // the delete can be undone (ADR: a misclick on a 60 GiB job must not
    // cost a full re-download). Single-digit MB each and reaped with their
    // history entry, so no quota is needed.
    // -----------------------------------------------------------------

    fn spool_path(&self, job: crate::JobId) -> Option<PathBuf> {
        self.spool
            .as_ref()
            .map(|d| d.join(format!("{}.nzb", job.0)))
    }

    /// Park `bytes` as this job's requeue source. Overwrites any previous
    /// spool for the same id (job ids are reused only after a restart, and
    /// the newer job is the one worth keeping).
    pub fn spool_nzb(&self, job: crate::JobId, bytes: &[u8]) -> Result<(), StateError> {
        let Some(path) = self.spool_path(job) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fsx::create_dir_all(parent)?;
        }
        fsx::write(&path, bytes)?;
        self.invalidate_spool_cache();
        Ok(())
    }

    /// The spooled NZB for a parked job, if it is still on this node.
    pub fn read_spool(&self, job: crate::JobId) -> Option<Vec<u8>> {
        let path = self.spool_path(job)?;
        fsx::read(&path).ok()
    }

    /// Can this entry be put back in the queue from local state?
    ///
    /// One `stat`. Correct for the one-off check the requeue path makes;
    /// use [`spooled_ids`](Self::spooled_ids) to decorate a list, or this
    /// becomes one syscall per row (measured: ~250 ms for 179 rows on a
    /// network state mount, nuc3 2026-07-29).
    pub fn has_spool(&self, job: crate::JobId) -> bool {
        self.spool_path(job).is_some_and(|p| p.is_file())
    }

    /// Every job id with a parked NZB, from ONE directory listing, cached
    /// for a few seconds.
    ///
    /// This is the list-decoration form of [`has_spool`](Self::has_spool).
    /// The set is a display snapshot — `can_requeue` on a history row is
    /// telling the operator whether the Undo button is worth showing — so
    /// a few seconds of staleness is free, while a stale *answer* to an
    /// actual requeue is not: that path still stats the file, and the
    /// requeue itself fails honestly if the spool went in between.
    /// Invalidated on every spool write and drop, so it is only ever stale
    /// with respect to another node, which cannot write this node's spool
    /// anyway.
    pub fn spooled_ids(&self) -> std::collections::HashSet<u32> {
        const TTL: Duration = Duration::from_secs(5);
        let mut cache = self.spool_cache.lock().unwrap();
        if let Some((at, ids)) = cache.as_ref() {
            if at.elapsed() < TTL {
                return ids.clone();
            }
        }
        let ids: std::collections::HashSet<u32> = self
            .spool
            .as_ref()
            .and_then(|d| fsx::read_dir(d).ok())
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        let p = e.path();
                        (p.extension().and_then(|s| s.to_str()) == Some("nzb"))
                            .then(|| p.file_stem()?.to_str()?.parse::<u32>().ok())
                            .flatten()
                    })
                    .collect()
            })
            .unwrap_or_default();
        *cache = Some((Instant::now(), ids.clone()));
        ids
    }

    fn invalidate_spool_cache(&self) {
        *self.spool_cache.lock().unwrap() = None;
    }

    pub fn drop_spool(&self, job: crate::JobId) {
        if let Some(path) = self.spool_path(job) {
            let _ = fsx::remove_file(&path);
        }
        self.invalidate_spool_cache();
    }

    /// Reap spooled NZBs whose history entry is gone — an index wiped out
    /// from under us, or a crash between the two writes.
    fn sweep_spool(&self) {
        let Some(dir) = &self.spool else { return };
        let Ok(entries) = fsx::read_dir(dir) else {
            return; // nothing spooled yet
        };
        let known: std::collections::HashSet<u32> = self
            .list_filtered(100_000, true)
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.job.0)
            .collect();
        let mut reaped = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u32>().ok());
            match id {
                Some(id) if known.contains(&id) => {}
                _ => {
                    if fsx::remove_file(&path).is_ok() {
                        reaped += 1;
                    }
                }
            }
        }
        if reaped > 0 {
            tracing::info!(reaped, "history: reaped orphaned parked NZBs");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(job: u32, at: i64) -> HistoryEntry {
        HistoryEntry {
            job: crate::JobId(job),
            name: format!("job-{job}"),
            category: Some("tv".into()),
            final_dir: Some("/dest/x".into()),
            status: "SUCCESS".into(),
            size: 1000,
            health: 1000,
            params: vec![("drone".into(), "abc123".into())],
            dupe_key: String::new(),
            dupe_score: 0,
            completed_at_unix: at,
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            stages: Vec::new(),
            seq: 0,
        }
    }

    /// The stage timeline survives the round-trip through SQLite. That is
    /// the whole point of persisting it: "why did that one take forty
    /// minutes" is a question you ask the next morning, about a job that
    /// left the queue hours ago.
    #[test]
    fn the_stage_timeline_survives_a_round_trip() {
        use nzbd_types::{PostStage, StageSpan};
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), None).unwrap();
        let mut e = entry(1, 1_000);
        e.stages = vec![
            StageSpan {
                stage: PostStage::ParVerify,
                started_at_unix: 900,
                ms: Some(12_000),
            },
            StageSpan {
                stage: PostStage::ParRepair,
                started_at_unix: 912,
                ms: Some(2_400_000),
            },
            StageSpan {
                stage: PostStage::Unpack,
                started_at_unix: 3_312,
                ms: Some(63_000),
            },
        ];
        db.record(&e).unwrap();

        let got = db.list(10).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].stages.len(), 3);
        assert_eq!(got[0].stages[1].stage, PostStage::ParRepair);
        assert_eq!(got[0].stages[1].ms, Some(2_400_000));
        // The forty minutes were the repair, and the row now says so.
        let slowest = got[0]
            .stages
            .iter()
            .max_by_key(|s| s.ms.unwrap_or(0))
            .unwrap();
        assert_eq!(slowest.stage, PostStage::ParRepair);
    }

    /// An index written by an older nzbd upgrades in place and its rows
    /// still read — with an empty timeline, not an error.
    ///
    /// This builds the genuine pre-migration schema and a row in it, then
    /// opens `HistoryDb` over the top so the real `ALTER TABLE` runs. A
    /// test that instead forced `stages = NULL` would be testing a state
    /// SQLite will not produce: `ADD COLUMN … NOT NULL DEFAULT '[]'`
    /// backfills existing rows with the default and then refuses NULL. The
    /// row mapper still reads the column as optional, which costs nothing
    /// and covers an index rebuilt by something other than this code.
    #[test]
    fn an_index_from_before_the_column_upgrades_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("history.sqlite");
        {
            // The schema exactly as it shipped before `stages` existed.
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE history (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     job_id INTEGER NOT NULL,
                     name TEXT NOT NULL,
                     category TEXT,
                     final_dir TEXT,
                     status TEXT NOT NULL,
                     size INTEGER NOT NULL,
                     health INTEGER NOT NULL DEFAULT 1000,
                     params TEXT NOT NULL DEFAULT '[]',
                     dupe_key TEXT NOT NULL DEFAULT '',
                     dupe_score INTEGER NOT NULL DEFAULT 0,
                     completed_at INTEGER NOT NULL,
                     hidden INTEGER NOT NULL DEFAULT 0,
                     first_seen INTEGER,
                     last_seen INTEGER,
                     seen_count INTEGER NOT NULL DEFAULT 0,
                     removed_at INTEGER,
                     picked_up_by TEXT,
                     UNIQUE(job_id, completed_at)
                 );
                 INSERT INTO history (job_id, name, status, size, completed_at)
                 VALUES (7, 'job-7', 'SUCCESS', 1000, 2000);",
            )
            .unwrap();
        }
        let db = HistoryDb::open(&path, None).unwrap();
        let got = db.list(10).unwrap();
        assert_eq!(got.len(), 1, "the pre-upgrade row is still readable");
        assert_eq!(got[0].name, "job-7");
        assert!(
            got[0].stages.is_empty(),
            "no timeline was recorded, and none is invented"
        );

        // And the upgraded index accepts a timeline on the next write.
        let mut fresh = entry(8, 3_000);
        fresh.stages = vec![nzbd_types::StageSpan {
            stage: nzbd_types::PostStage::Unpack,
            started_at_unix: 2_900,
            ms: Some(1_500),
        }];
        db.record(&fresh).unwrap();
        let got = db.list(10).unwrap();
        let eight = got.iter().find(|e| e.job.0 == 8).unwrap();
        assert_eq!(eight.stages.len(), 1);
    }

    #[test]
    fn record_list_delete_and_jsonl_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("local/history.sqlite");
        let shared = tmp.path().join("shared");

        let db = HistoryDb::open(&db_path, Some(&shared)).unwrap();
        db.record(&entry(1, 100)).unwrap();
        db.record(&entry(2, 200)).unwrap();
        db.record(&entry(2, 200)).unwrap(); // duplicate ignored
        let list = db.list(10).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].job.0, 2, "newest first");
        assert!(db.delete(crate::JobId(1)).unwrap());
        drop(db);

        // New authority, fresh local index: rebuilt from the shared JSONL.
        let db2 = HistoryDb::open(&tmp.path().join("other/history.sqlite"), Some(&shared)).unwrap();
        let list = db2.list(10).unwrap();
        assert_eq!(
            list.len(),
            2,
            "rebuilt from JSONL (incl. deleted-locally row)"
        );
    }

    /// The cursor a consumer walks forward on. Two properties matter and
    /// both are asserted here: paging from any point returns exactly the
    /// later rows in ascending order (so the last row's seq is a valid
    /// next cursor), and the pre-existing `?limit=` path is untouched.
    #[test]
    fn since_seq_pages_forward_without_disturbing_the_newest_first_view() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&tmp.path().join("local/history.sqlite"), None).unwrap();
        for n in 1..=5 {
            db.record(&entry(n, 100 * n as i64)).unwrap();
        }

        let all = db.list_since(0, 100).unwrap();
        assert_eq!(
            all.iter().map(|e| e.job.0).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "oldest first — a consumer reads forward"
        );
        let seqs: Vec<i64> = all.iter().map(|e| e.seq).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seq is monotone");

        // From every midpoint: exactly the rows after it.
        for (i, cursor) in seqs.iter().enumerate() {
            let rest = db.list_since(*cursor, 100).unwrap();
            assert_eq!(
                rest.iter().map(|e| e.job.0).collect::<Vec<_>>(),
                (i as u32 + 2..=5).collect::<Vec<_>>(),
                "paging from seq {cursor} must return exactly what follows it"
            );
        }
        assert!(db
            .list_since(*seqs.last().unwrap(), 100)
            .unwrap()
            .is_empty());
        assert_eq!(db.list_since(0, 2).unwrap().len(), 2, "limit applies");

        // A row another client hid is still news to a second consumer
        // catching up: hidden means "someone else imported it", not
        // "this never happened".
        assert!(db.hide(crate::JobId(3), Some("sonarr"), 999).unwrap());
        assert!(
            db.list_since(0, 100).unwrap().iter().any(|e| e.job.0 == 3),
            "the cursor path must not hide rows from a catching-up consumer"
        );

        // And the UI's view is unchanged: newest first, hidden included.
        let newest = db.list_filtered(10, true).unwrap();
        assert_eq!(
            newest.iter().map(|e| e.job.0).collect::<Vec<_>>(),
            vec![5, 4, 3, 2, 1]
        );
    }

    /// `record_seq` is what lets a completion event name its own history
    /// row. If it reported the wrong rowid, consumers would page from the
    /// wrong place and silently skip entries.
    #[test]
    fn record_seq_names_the_row_it_just_wrote() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&tmp.path().join("local/history.sqlite"), None).unwrap();
        let a = db.record_seq(&entry(1, 100)).unwrap();
        let b = db.record_seq(&entry(2, 200)).unwrap();
        assert!(a > 0 && b > a);
        assert_eq!(
            db.list_since(a - 1, 10).unwrap().first().map(|e| e.job.0),
            Some(1),
            "reading from seq-1 must find the row the write reported"
        );
        // Re-recording the same entry is an upsert, not a new row — and it
        // must still report that row, not the last insert on the handle.
        assert_eq!(db.record_seq(&entry(1, 100)).unwrap(), a);
    }

    /// The JSONL is portable and gets re-imported into indices that assign
    /// their own rowids; persisting one node's seq into it would write a
    /// number that is wrong everywhere else.
    #[test]
    fn the_portable_log_does_not_carry_a_local_rowid() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = tmp.path().join("shared");
        let db = HistoryDb::open(&tmp.path().join("local/history.sqlite"), Some(&shared)).unwrap();
        db.record(&entry(1, 100)).unwrap();
        drop(db);

        let line = std::fs::read_to_string(shared.join("history.jsonl")).unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(
            v["seq"], 0,
            "a rowid is index-local and must not be published: {line}"
        );

        // A fresh index still assigns a usable cursor on rebuild.
        let db2 = HistoryDb::open(&tmp.path().join("other/history.sqlite"), Some(&shared)).unwrap();
        assert!(db2.list_since(0, 10).unwrap()[0].seq > 0);
    }

    /// **Pins a known defect on purpose** (`docs/DEFECT_HISTORY_DELETE.md`).
    ///
    /// A deleted entry comes back on the next refresh with a new rowid, so
    /// "forget" does not stick and a cursor consumer sees the entry twice.
    /// This test asserts the WRONG behavior so that a fix cannot land
    /// silently: when someone implements tombstones, this test fails, and
    /// its replacement is item 1 and item 3 of that doc's acceptance list.
    #[test]
    fn delete_resurrects_the_entry_with_a_new_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = tmp.path().join("shared");
        let db = HistoryDb::open(&tmp.path().join("local/history.sqlite"), Some(&shared)).unwrap();
        for n in 1..=3 {
            db.record(&entry(n, 100 * n as i64)).unwrap();
        }
        let before = db.list_since(0, 50).unwrap();
        let cursor = before.last().unwrap().seq;
        let gone_at = before.iter().find(|e| e.job.0 == 2).unwrap().seq;

        assert!(db.delete(crate::JobId(2)).unwrap());
        assert!(
            !db.list_since(0, 50).unwrap().iter().any(|e| e.job.0 == 2),
            "the delete does take effect in the index"
        );

        // …until the first read rebuilds the index from the log, which is
        // every history poll. `open` seeds without arming the throttle, so
        // this first `refresh` is not skipped.
        db.refresh().unwrap();
        let back = db
            .list_since(0, 50)
            .unwrap()
            .into_iter()
            .find(|e| e.job.0 == 2)
            .expect("DEFECT: the deleted entry is back");
        assert!(
            back.seq > gone_at,
            "and it is back as a NEW row (seq {} > {gone_at}), which is why \
             a consumer past cursor {cursor} is handed it a second time",
            back.seq
        );
        assert!(
            db.list_since(cursor, 50)
                .unwrap()
                .iter()
                .any(|e| e.job.0 == 2),
            "DEFECT: duplicate delivery on the cursor"
        );
    }

    #[test]
    fn parked_nzb_lives_and_dies_with_its_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("local/history.sqlite");
        let db = HistoryDb::open(&db_path, None).unwrap();

        assert!(!db.has_spool(crate::JobId(1)), "nothing parked yet");
        db.spool_nzb(crate::JobId(1), b"<nzb/>").unwrap();
        db.record(&entry(1, 100)).unwrap();
        assert!(db.has_spool(crate::JobId(1)));
        assert_eq!(
            db.read_spool(crate::JobId(1)).as_deref(),
            Some(&b"<nzb/>"[..])
        );

        // Forgetting the entry takes the spool with it — a file nobody can
        // reach through the UI must not survive on disk.
        assert!(db.delete(crate::JobId(1)).unwrap());
        assert!(
            !db.has_spool(crate::JobId(1)),
            "spool reaped with the record"
        );
        assert!(db.read_spool(crate::JobId(1)).is_none());
    }

    #[test]
    fn orphaned_spools_are_swept_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("local/history.sqlite");
        {
            let db = HistoryDb::open(&db_path, None).unwrap();
            db.record(&entry(1, 100)).unwrap();
            db.spool_nzb(crate::JobId(1), b"keep").unwrap();
            db.spool_nzb(crate::JobId(2), b"orphan").unwrap(); // no entry
        }
        // A crash between spool and record, or an index wiped from under
        // us, must not leak NZBs forever.
        let db = HistoryDb::open(&db_path, None).unwrap();
        assert!(
            db.has_spool(crate::JobId(1)),
            "the parked entry keeps its NZB"
        );
        assert!(!db.has_spool(crate::JobId(2)), "the orphan is reaped");
    }

    // -----------------------------------------------------------------
    // Retention
    // -----------------------------------------------------------------

    const DAY: i64 = 86_400;

    /// The count bound keeps the NEWEST `keep_max` and drops the rest —
    /// ranked in the same order the UI's first page shows, so "kept" and
    /// "near the top" never disagree.
    #[test]
    fn the_count_bound_keeps_the_newest_and_drops_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(
            &tmp.path().join("history.sqlite"),
            Some(&tmp.path().join("h")),
        )
        .unwrap();
        let now = 1_800_000_000;
        for i in 0..25u32 {
            db.record(&entry(i, now - (25 - i as i64) * 60)).unwrap();
        }
        assert_eq!(db.count_filtered(true).unwrap(), 25);

        let dropped = db
            .set_retention(Retention {
                keep_max: 10,
                keep_days: 0,
            })
            .unwrap();
        assert_eq!(dropped, 15);
        let kept = db.list_filtered(100, true).unwrap();
        assert_eq!(kept.len(), 10);
        assert_eq!(kept[0].job.0, 24, "newest first, and it survived");
        assert_eq!(kept[9].job.0, 15, "the tenth-newest is the oldest kept");
    }

    /// The age bound answers a different question and has to hold on its
    /// own: a quiet daemon never reaches a count bound, and a year-old row
    /// is still a year old.
    #[test]
    fn the_age_bound_drops_what_is_older_than_the_window() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(
            &tmp.path().join("history.sqlite"),
            Some(&tmp.path().join("h")),
        )
        .unwrap();
        let now = 1_800_000_000;
        db.record(&entry(1, now - 200 * DAY)).unwrap();
        db.record(&entry(2, now - 100 * DAY)).unwrap();
        db.record(&entry(3, now - 10 * DAY)).unwrap();
        db.record(&entry(4, now - 1)).unwrap();

        *db.retention.lock().unwrap() = Retention {
            keep_max: 0,
            keep_days: 90,
        };
        assert_eq!(db.prune(now).unwrap(), 2, "the 200- and 100-day rows go");
        let kept: Vec<u32> = db
            .list_filtered(100, true)
            .unwrap()
            .into_iter()
            .map(|e| e.job.0)
            .collect();
        assert_eq!(kept, vec![4, 3]);

        // Whichever bound bites first wins: tightening the count bound
        // trims further, and the age bound alone would not have.
        *db.retention.lock().unwrap() = Retention {
            keep_max: 1,
            keep_days: 90,
        };
        assert_eq!(db.prune(now).unwrap(), 1);
        assert_eq!(db.count_filtered(true).unwrap(), 1);
    }

    /// The one that makes retention *mean* something. Trimming the index
    /// alone leaves the JSONL — the file every read re-unions — growing
    /// forever, and the next refresh imports the dropped rows straight
    /// back. Compaction plus the ingest floor is what makes a trim stick.
    #[test]
    fn a_trim_shrinks_the_log_and_survives_every_later_refresh() {
        let tmp = tempfile::tempdir().unwrap();
        let jsonl_dir = tmp.path().join("h");
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&jsonl_dir)).unwrap();
        let now = 1_800_000_000;
        for i in 0..20u32 {
            db.record(&entry(i, now - (20 - i as i64) * DAY)).unwrap();
        }
        let log = jsonl_dir.join("history.jsonl");
        let lines = |p: &Path| std::fs::read_to_string(p).unwrap().lines().count();
        assert_eq!(lines(&log), 20);

        db.set_retention(Retention {
            keep_max: 5,
            keep_days: 0,
        })
        .unwrap();
        assert_eq!(lines(&log), 5, "the log itself shrank, not just the index");

        // Three refresh cycles: the throttle is bypassed by calling the
        // rebuild directly, which is what refresh does once it fires.
        for _ in 0..3 {
            db.rebuild_from_jsonl(true).unwrap();
            assert_eq!(db.count_filtered(true).unwrap(), 5, "nothing came back");
        }

        // And a peer's file, still holding the old rows, cannot resurrect
        // them either — the ingest floor is what stops it.
        let peer = jsonl_dir.join("history.peer.jsonl");
        let mut buf = String::new();
        for i in 0..20u32 {
            let mut e = entry(i, now - (20 - i as i64) * DAY);
            e.seq = 0;
            buf.push_str(&serde_json::to_string(&e).unwrap());
            buf.push('\n');
        }
        std::fs::write(&peer, buf).unwrap();
        db.rebuild_from_jsonl(true).unwrap();
        assert_eq!(
            db.count_filtered(true).unwrap(),
            5,
            "a peer's uncompacted log does not undo this node's trim"
        );
    }

    /// Compaction keeps EVERY surviving line, not the newest per key. The
    /// reader merges `removed_at`/`picked_up_by` with COALESCE, so
    /// collapsing a key's lines would quietly change what a rebuild
    /// reconstructs — a size optimisation must not be a semantic one.
    #[test]
    fn compaction_preserves_the_full_history_of_a_surviving_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let jsonl_dir = tmp.path().join("h");
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&jsonl_dir)).unwrap();
        let now = 1_800_000_000;
        db.record(&entry(1, now - 10 * DAY)).unwrap();
        db.record(&entry(2, now - 1)).unwrap();
        // `hide` re-appends, so job 2 now owns two lines in the log.
        db.hide(crate::JobId(2), Some("sonarr"), now).unwrap();
        let log = jsonl_dir.join("history.jsonl");
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 3);

        db.set_retention(Retention {
            keep_max: 1,
            keep_days: 0,
        })
        .unwrap();
        let text = std::fs::read_to_string(&log).unwrap();
        assert_eq!(
            text.lines().count(),
            2,
            "job 1's line went, job 2's both stayed"
        );
        assert!(!text.contains("job-1"));

        // Rebuild from the compacted log alone: the hidden state and the
        // consumer that set it are still reconstructible.
        let db2 = HistoryDb::open(&tmp.path().join("h2.sqlite"), Some(&jsonl_dir)).unwrap();
        let rows = db2.list_filtered(10, true).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].hidden);
        assert_eq!(rows[0].picked_up_by.as_deref(), Some("sonarr"));
    }

    /// Retention off is retention off: an install that never sets bounds
    /// keeps behaving exactly as it did before this existed.
    #[test]
    fn unlimited_retention_touches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let jsonl_dir = tmp.path().join("h");
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&jsonl_dir)).unwrap();
        let now = 1_800_000_000;
        for i in 0..10u32 {
            db.record(&entry(i, now - (500 - i as i64) * DAY)).unwrap();
        }
        assert_eq!(db.set_retention(Retention::UNLIMITED).unwrap(), 0);
        assert_eq!(db.count_filtered(true).unwrap(), 10);
        assert_eq!(
            std::fs::read_to_string(jsonl_dir.join("history.jsonl"))
                .unwrap()
                .lines()
                .count(),
            10
        );
    }

    /// A trim must take the parked NZB with it. The spool is reaped by
    /// `sweep_spool` only at open; an entry pruned at runtime would
    /// otherwise leave a file nobody can reach from the UI.
    #[test]
    fn a_pruned_entry_takes_its_parked_nzb_with_it() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(
            &tmp.path().join("history.sqlite"),
            Some(&tmp.path().join("h")),
        )
        .unwrap();
        let now = 1_800_000_000;
        db.record(&entry(1, now - 300 * DAY)).unwrap();
        db.record(&entry(2, now - 1)).unwrap();
        db.spool_nzb(crate::JobId(1), b"<nzb/>").unwrap();
        db.spool_nzb(crate::JobId(2), b"<nzb/>").unwrap();

        db.set_retention(Retention {
            keep_max: 0,
            keep_days: 90,
        })
        .unwrap();
        assert!(!db.has_spool(crate::JobId(1)), "pruned entry's NZB is gone");
        assert!(db.has_spool(crate::JobId(2)), "the survivor keeps its NZB");
    }

    /// `spooled_ids` is the list-decoration form of `has_spool`: one
    /// directory read for a whole page instead of one stat per row. It has
    /// to agree with `has_spool` exactly, or `can_requeue` starts lying.
    #[test]
    fn one_listing_answers_can_requeue_for_a_whole_page() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), None).unwrap();
        for i in [3u32, 7, 11] {
            db.spool_nzb(crate::JobId(i), b"<nzb/>").unwrap();
        }
        let ids = db.spooled_ids();
        assert_eq!(ids.len(), 3);
        for i in 0..15u32 {
            assert_eq!(
                ids.contains(&i),
                db.has_spool(crate::JobId(i)),
                "job {i}: the batched answer must match the per-row one"
            );
        }
        // A write invalidates the cache rather than serving a stale set.
        db.drop_spool(crate::JobId(7));
        assert!(!db.spooled_ids().contains(&7));
        db.spool_nzb(crate::JobId(9), b"<nzb/>").unwrap();
        assert!(db.spooled_ids().contains(&9));
    }

    /// Paging is over the same newest-first order the unpaged list used,
    /// so page 2 continues exactly where page 1 stopped — no overlap, no
    /// gap.
    #[test]
    fn pages_tile_the_newest_first_view_without_overlap_or_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&tmp.path().join("history.sqlite"), None).unwrap();
        let now = 1_800_000_000;
        for i in 0..25u32 {
            db.record(&entry(i, now - (25 - i as i64) * 60)).unwrap();
        }
        let all: Vec<u32> = db
            .list_filtered(100, true)
            .unwrap()
            .into_iter()
            .map(|e| e.job.0)
            .collect();
        assert_eq!(db.count_filtered(true).unwrap() as usize, all.len());

        let mut tiled: Vec<u32> = Vec::new();
        for page in 0..3 {
            tiled.extend(
                db.list_page(10, page * 10, true)
                    .unwrap()
                    .into_iter()
                    .map(|e| e.job.0),
            );
        }
        assert_eq!(tiled, all, "the pages reassemble the whole list, in order");
        assert_eq!(
            db.list_page(10, 20, true).unwrap().len(),
            5,
            "short last page"
        );
        assert!(
            db.list_page(10, 100, true).unwrap().is_empty(),
            "past the end"
        );

        // Hidden rows are excluded from both the page and its total, or
        // the pager would show pages that render empty.
        db.hide(crate::JobId(24), Some("sonarr"), now).unwrap();
        assert_eq!(db.count_filtered(false).unwrap(), 24);
        assert_eq!(db.list_page(10, 0, false).unwrap()[0].job.0, 23);
    }
}
