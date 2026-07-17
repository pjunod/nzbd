//! Job history (ARCHITECTURE.md §8.6 / ADR-16).
//!
//! Two layers: an **append-only JSONL** file on the (possibly shared)
//! volume — the crash-safe, mergeable source of truth — and a **local
//! SQLite index** (rusqlite bundled) rebuilt from the JSONL when empty.
//! SQLite never lives on a network filesystem (ADR-16): in cluster mode
//! the JSONL sits on the shared volume, the index on the local disk of
//! whichever node is the authority.

use crate::{HistoryEntry, StateError};
use rusqlite::Connection;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct HistoryDb {
    conn: Mutex<Connection>,
    jsonl: Option<PathBuf>,
}

impl HistoryDb {
    /// `db_path` = local SQLite file; `jsonl_dir` = directory for the
    /// authoritative `history.jsonl` (pass the shared volume in cluster
    /// mode, or the local state dir single-node).
    pub fn open(db_path: &Path, jsonl_dir: Option<&Path>) -> Result<HistoryDb, StateError> {
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
                 completed_at INTEGER NOT NULL,
                 UNIQUE(job_id, completed_at)
             );",
        )
        .map_err(|e| StateError::Corrupt(format!("sqlite schema: {e}")))?;

        let jsonl = jsonl_dir.map(|d| d.join("history.jsonl"));
        let db = HistoryDb {
            conn: Mutex::new(conn),
            jsonl,
        };
        db.rebuild_from_jsonl()?;
        Ok(db)
    }

    /// Import any JSONL rows the index doesn't have (fresh index after a
    /// leader failover, or a wiped local disk).
    fn rebuild_from_jsonl(&self) -> Result<(), StateError> {
        let Some(path) = &self.jsonl else {
            return Ok(());
        };
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut imported = 0usize;
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
        if imported > 0 {
            tracing::info!(imported, "history index rebuilt from JSONL");
        }
        Ok(())
    }

    fn insert(&self, entry: &HistoryEntry, and_jsonl: bool) -> Result<bool, StateError> {
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute(
                "INSERT OR IGNORE INTO history
                 (job_id, name, category, final_dir, status, size, health, completed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    entry.job.0,
                    entry.name,
                    entry.category,
                    entry.final_dir,
                    entry.status,
                    entry.size as i64,
                    entry.health as i64,
                    entry.completed_at_unix,
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

    pub fn list(&self, limit: usize) -> Result<Vec<HistoryEntry>, StateError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT job_id, name, category, final_dir, status, size, health, completed_at
                 FROM history ORDER BY completed_at DESC, id DESC LIMIT ?1",
            )
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        let rows = stmt
            .query_map([limit as i64], |r| {
                Ok(HistoryEntry {
                    job: crate::JobId(r.get::<_, i64>(0)? as u32),
                    name: r.get(1)?,
                    category: r.get(2)?,
                    final_dir: r.get(3)?,
                    status: r.get(4)?,
                    size: r.get::<_, i64>(5)? as u64,
                    health: r.get::<_, i64>(6)? as u16,
                    completed_at_unix: r.get(7)?,
                })
            })
            .map_err(|e| StateError::Corrupt(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| StateError::Corrupt(e.to_string()))?);
        }
        Ok(out)
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
            completed_at_unix: at,
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
