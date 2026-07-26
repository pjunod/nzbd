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
                       seen_count, removed_at, picked_up_by, id";

pub struct HistoryDb {
    conn: Mutex<Connection>,
    jsonl: Option<PathBuf>,
    /// Local spool for the regenerated NZBs of deleted jobs (`nzbs/<job>.nzb`
    /// beside the SQLite index). Local, not shared: it is a convenience for
    /// undoing a delete on the node that served the click, not cluster state.
    spool: Option<PathBuf>,
    last_refresh: Mutex<Option<Instant>>,
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
        ] {
            let _ = conn.execute(ddl, []);
        }

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
        };
        db.rebuild_from_jsonl(false)?;
        db.sweep_spool();
        Ok(db)
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
        let before = self.row_count()?;
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
        Ok(())
    }

    fn row_count(&self) -> Result<u64, StateError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM history", [], |r| r.get::<_, u64>(0))
            .map_err(|e| StateError::Corrupt(format!("sqlite count: {e}")))
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
                  dupe_key, dupe_score, completed_at, hidden, removed_at, picked_up_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
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
        Ok(())
    }

    /// `record`, returning the entry's cursor value (`seq`) so a caller
    /// can publish "the row you want is at N" in the same breath as the
    /// event announcing it. This is what makes the `job_pp_finished`
    /// ordering guarantee usable: the consumer gets the event *and* the
    /// exact cursor, and never has to guess how far to page back.
    pub fn record_seq(&self, entry: &HistoryEntry) -> Result<i64, StateError> {
        self.insert_seq(entry, true).map(|(_, seq)| seq)
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
        let sql = format!(
            "SELECT {COLUMNS} FROM history {} ORDER BY completed_at DESC, id DESC LIMIT ?1",
            if include_hidden {
                ""
            } else {
                "WHERE hidden = 0"
            }
        );
        self.query(&sql, rusqlite::params![limit as i64])
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
    /// **Known wart, inherited from `delete`.** `delete` removes the index
    /// row but not the JSONL line — deliberately, because the JSONL is the
    /// authoritative cross-node log and a delete is index-local (see
    /// `record_list_delete_and_jsonl_rebuild`). The next `refresh`
    /// re-imports that line and SQLite assigns it a *new, higher* rowid,
    /// so a consumer paging forward can be handed the same entry a second
    /// time under a later cursor. The duplicate is byte-identical, so
    /// **dedupe on `(job, completed_at)`** — the same key the index is
    /// already unique on. Fixing it properly means giving deletes a
    /// durable tombstone, which changes cross-node semantics and deserves
    /// its own change rather than riding in as a side effect of adding a
    /// cursor.
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
        Ok(())
    }

    /// The spooled NZB for a parked job, if it is still on this node.
    pub fn read_spool(&self, job: crate::JobId) -> Option<Vec<u8>> {
        let path = self.spool_path(job)?;
        fsx::read(&path).ok()
    }

    /// Can this entry be put back in the queue from local state?
    pub fn has_spool(&self, job: crate::JobId) -> bool {
        self.spool_path(job).is_some_and(|p| p.is_file())
    }

    pub fn drop_spool(&self, job: crate::JobId) {
        if let Some(path) = self.spool_path(job) {
            let _ = fsx::remove_file(&path);
        }
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
            seq: 0,
        }
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
}
