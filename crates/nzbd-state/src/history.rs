//! Job history (ARCHITECTURE.md §8.6 / ADR-16).
//!
//! Two layers: **append-only JSONL** files on the (possibly shared)
//! volume — the crash-safe, mergeable source of truth — and a **local
//! SQLite index** (rusqlite bundled) rebuilt from the JSONL when empty.
//! SQLite never lives on a network filesystem (ADR-16). In cluster mode
//! every node appends to its OWN `history.<node>.jsonl` (cross-client
//! O_APPEND interleaving on Gluster is not trustworthy); readers union
//! all `history*.jsonl` files, deduped by (job, completed_at).

use crate::{HistoryEntry, StateError};
use rusqlite::Connection;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct HistoryDb {
    conn: Mutex<Connection>,
    jsonl: Option<PathBuf>,
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
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)
            .map_err(|e| StateError::Corrupt(format!("sqlite open: {e}")))?;
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
            last_refresh: Mutex::new(None),
        };
        db.rebuild_from_jsonl()?;
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
        self.rebuild_from_jsonl()
    }

    /// Import any JSONL rows the index doesn't have (fresh index after a
    /// leader failover, a wiped local disk, or another node's appends).
    /// Unions every `history*.jsonl` in the directory — duplicates are
    /// dropped by the (job, completed_at) unique index.
    fn rebuild_from_jsonl(&self) -> Result<(), StateError> {
        let Some(own) = &self.jsonl else {
            return Ok(());
        };
        let Some(dir) = own.parent() else {
            return Ok(());
        };
        let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
            Ok(entries) => entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    let n = p.file_name().unwrap_or_default().to_string_lossy();
                    n.starts_with("history") && n.ends_with(".jsonl")
                })
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        files.sort();
        let mut imported = 0usize;
        for path in files {
            let Ok(file) = std::fs::File::open(&path) else {
                continue;
            };
            for line in BufReader::new(file).split(b'\n') {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                let Ok(entry) = serde_json::from_slice::<HistoryEntry>(&line) else {
                    continue; // torn tail / old format
                };
                if self.insert(&entry, false)? {
                    imported += 1;
                }
            }
        }
        if imported > 0 {
            tracing::info!(imported, "history index rebuilt from JSONL");
        }
        Ok(())
    }

    fn insert(&self, entry: &HistoryEntry, and_jsonl: bool) -> Result<bool, StateError> {
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
        drop(conn);
        if n > 0 && and_jsonl {
            if let Some(path) = &self.jsonl {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut f = OpenOptions::new().create(true).append(true).open(path)?;
                let mut line = serde_json::to_vec(entry)?;
                line.push(b'\n');
                f.write_all(&line)?;
                f.sync_data()?;
            }
        }
        Ok(n > 0)
    }

    pub fn record(&self, entry: &HistoryEntry) -> Result<(), StateError> {
        self.insert(entry, true)?;
        Ok(())
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
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT job_id, name, category, final_dir, status, size, health, params,
                    dupe_key, dupe_score, completed_at, hidden, first_seen, last_seen,
                    seen_count, removed_at, picked_up_by
             FROM history {} ORDER BY completed_at DESC, id DESC LIMIT ?1",
            if include_hidden {
                ""
            } else {
                "WHERE hidden = 0"
            }
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        let rows = stmt
            .query_map([limit as i64], |r| {
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
                std::fs::create_dir_all(parent)?;
            }
            let mut f = OpenOptions::new().create(true).append(true).open(path)?;
            let mut line = serde_json::to_vec(entry)?;
            line.push(b'\n');
            f.write_all(&line)?;
            f.sync_data()?;
        }
        Ok(())
    }

    pub fn delete(&self, job: crate::JobId) -> Result<bool, StateError> {
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute("DELETE FROM history WHERE job_id = ?1", [job.0])
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        Ok(n > 0)
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
}
