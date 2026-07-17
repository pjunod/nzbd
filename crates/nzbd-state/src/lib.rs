//! Persistence for the download queue. See ARCHITECTURE.md §8.6.
//!
//! Two artifacts in the state directory, plus a marker:
//!
//! 1. **Queue snapshot** (`queue.json`): all jobs/files/segments sans
//!    transient lease state. Written atomically (tmp + rename, fsync'd),
//!    debounced on structural change.
//! 2. **Segment journal** (`segments.journal`): append-only records of
//!    completed segments — one JSON line each, fsync'd on a short interval
//!    by the engine tick, compacted (truncated) whenever a fresh snapshot
//!    has folded them in. Recovery = load snapshot, replay journal,
//!    re-lease everything else.
//! 3. **`unclean` marker**: present while the daemon runs; removed on
//!    graceful shutdown. Its presence at startup signals a crash.
//!
//! Everything here is deliberately synchronous std I/O: the owner task calls
//! appends (page-cache writes, microseconds) inline and defers fsync to its
//! 1 Hz tick — same policy as NZBGet's DiskState, minus the bespoke format.
//!
//! **History** in SQLite arrives in phase 2 (the trait is defined below).

use nzbd_types::{FileId, Job, JobId};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("corrupt state: {0}")]
    Corrupt(String),
}

// ---------------------------------------------------------------------------
// Segment journal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub job: JobId,
    pub file: FileId,
    pub segment_number: u32,
    /// Position of the decoded part in the output file.
    pub offset: u64,
    pub len: u32,
    pub crc32: u32,
    /// Total output-file size from the yEnc header — lets recovery finalize
    /// files whose remaining segments all failed before the crash.
    pub file_size: u64,
}

/// Append-only segment journal backed by a single file of JSON lines.
/// Replay tolerates a torn trailing line (crash mid-append).
pub struct FsJournal {
    path: PathBuf,
    file: File,
    dirty: bool,
}

impl FsJournal {
    pub fn open(dir: &Path) -> Result<FsJournal, StateError> {
        fs::create_dir_all(dir)?;
        let path = dir.join("segments.journal");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(FsJournal {
            path,
            file,
            dirty: false,
        })
    }

    pub fn append(&mut self, rec: &JournalRecord) -> Result<(), StateError> {
        let mut line = serde_json::to_vec(rec)?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.dirty = true;
        Ok(())
    }

    /// fsync if anything was appended since the last sync.
    pub fn sync(&mut self) -> Result<(), StateError> {
        if self.dirty {
            self.file.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Read every intact record. Stops (without erroring) at the first
    /// corrupt or torn line — everything after it is unusable anyway.
    pub fn replay(&self) -> Result<Vec<JournalRecord>, StateError> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for line in BufReader::new(file).split(b'\n') {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<JournalRecord>(&line) {
                Ok(rec) => out.push(rec),
                Err(_) => break, // torn tail
            }
        }
        Ok(out)
    }

    /// Truncate after the records have been folded into a snapshot.
    pub fn compact(&mut self) -> Result<(), StateError> {
        self.file.set_len(0)?;
        self.file.sync_data()?;
        self.dirty = false;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-job fenced journals (CLUSTERING.md §6.4 / ADR-16)
//
// One journal file per (job, writer-lease): `jobs/<id>/journal.<suffix>`.
// Replay UNIONS the Done records across every journal file of a job —
// records are idempotent (immutable article content ⇒ same offset/len/crc),
// so an expired lease holder appending late is harmless. This is what makes
// cross-node reclaim safe without any shared-file locking.
// ---------------------------------------------------------------------------

pub struct JobJournals {
    jobs_dir: PathBuf,
    /// Fencing suffix — the work-lease id, or "local" for single-node.
    suffix: String,
    open: std::collections::HashMap<u32, File>,
    dirty: bool,
}

impl JobJournals {
    pub fn open(state_dir: &Path, suffix: &str) -> Result<JobJournals, StateError> {
        let jobs_dir = state_dir.join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        Ok(JobJournals {
            jobs_dir,
            suffix: sanitize_suffix(suffix),
            open: Default::default(),
            dirty: false,
        })
    }

    pub fn jobs_dir(&self) -> &Path {
        &self.jobs_dir
    }

    fn file_for(&mut self, job: JobId) -> Result<&mut File, StateError> {
        if !self.open.contains_key(&job.0) {
            let dir = self.jobs_dir.join(job.0.to_string());
            fs::create_dir_all(&dir)?;
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(format!("journal.{}", self.suffix)))?;
            self.open.insert(job.0, f);
        }
        Ok(self.open.get_mut(&job.0).unwrap())
    }

    pub fn append(&mut self, rec: &JournalRecord) -> Result<(), StateError> {
        let mut line = serde_json::to_vec(rec)?;
        line.push(b'\n');
        self.file_for(rec.job)?.write_all(&line)?;
        self.dirty = true;
        Ok(())
    }

    /// fsync every journal touched since the last sync.
    pub fn sync(&mut self) -> Result<(), StateError> {
        if self.dirty {
            for f in self.open.values() {
                f.sync_data()?;
            }
            self.dirty = false;
        }
        Ok(())
    }

    /// Union replay of ALL journal files (any suffix) for one job.
    pub fn replay_job(state_dir: &Path, job: JobId) -> Result<Vec<JournalRecord>, StateError> {
        let dir = state_dir.join("jobs").join(job.0.to_string());
        replay_dir(&dir)
    }

    /// Union replay across every job directory.
    pub fn replay_all(state_dir: &Path) -> Result<Vec<JournalRecord>, StateError> {
        let jobs_dir = state_dir.join("jobs");
        let mut out = Vec::new();
        let entries = match fs::read_dir(&jobs_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                out.extend(replay_dir(&entry.path())?);
            }
        }
        Ok(out)
    }

    /// Remove every journal file of a job — call ONLY as the queue
    /// authority, immediately after a snapshot that folds the job's state,
    /// and never while the job is delegated (a live foreign writer).
    pub fn remove_job(&mut self, job: JobId) -> Result<(), StateError> {
        self.open.remove(&job.0);
        let dir = self.jobs_dir.join(job.0.to_string());
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn replay_dir(dir: &Path) -> Result<Vec<JournalRecord>, StateError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("journal.") {
            continue;
        }
        let file = match File::open(entry.path()) {
            Ok(f) => f,
            Err(_) => continue,
        };
        for line in BufReader::new(file).split(b'\n') {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<JournalRecord>(&line) {
                Ok(rec) => out.push(rec),
                Err(_) => break, // torn tail of this file; others still count
            }
        }
    }
    Ok(out)
}

fn sanitize_suffix(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "local".into()
    } else {
        cleaned
    }
}

// ---------------------------------------------------------------------------
// Queue snapshot
// ---------------------------------------------------------------------------

/// Everything needed to reconstruct the queue after a restart.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueSnapshotDoc {
    pub jobs: Vec<Job>,
    pub next_job_id: u32,
    pub next_file_id: u32,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
}

pub struct SnapshotStore {
    path: PathBuf,
    tmp: PathBuf,
    dir: PathBuf,
}

impl SnapshotStore {
    pub fn open(dir: &Path) -> Result<SnapshotStore, StateError> {
        fs::create_dir_all(dir)?;
        Ok(SnapshotStore {
            path: dir.join("queue.json"),
            tmp: dir.join("queue.json.tmp"),
            dir: dir.to_path_buf(),
        })
    }

    /// Atomic write: tmp + fsync + rename + fsync(dir).
    pub fn save(&self, doc: &QueueSnapshotDoc) -> Result<(), StateError> {
        self.save_guarded(doc, &|| true)
    }

    /// Atomic write with a fencing guard checked immediately before the
    /// commit rename (CLUSTERING.md §6.4: a deposed leader must not clobber
    /// its successor's snapshot). Returns `Corrupt("fenced")` when the
    /// guard rejects.
    pub fn save_guarded(
        &self,
        doc: &QueueSnapshotDoc,
        guard: &dyn Fn() -> bool,
    ) -> Result<(), StateError> {
        let mut f = File::create(&self.tmp)?;
        serde_json::to_writer(&mut f, doc)?;
        f.sync_data()?;
        drop(f);
        if !guard() {
            return Err(StateError::Corrupt(
                "fenced: no longer the authority".into(),
            ));
        }
        fs::rename(&self.tmp, &self.path)?;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all(); // best-effort directory fsync
        }
        Ok(())
    }

    /// `None` if no snapshot exists yet. A corrupt snapshot is an error —
    /// the operator should decide, not silently lose a queue.
    pub fn load(&self) -> Result<Option<QueueSnapshotDoc>, StateError> {
        let bytes = match fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let doc = serde_json::from_slice(&bytes)
            .map_err(|e| StateError::Corrupt(format!("queue.json: {e}")))?;
        Ok(Some(doc))
    }
}

// ---------------------------------------------------------------------------
// Unclean-shutdown marker
// ---------------------------------------------------------------------------

pub struct UncleanMarker {
    path: PathBuf,
}

impl UncleanMarker {
    /// `tag` scopes the marker: in cluster mode every node shares one state
    /// directory, so each arms its own `unclean.<node>` file.
    pub fn new(dir: &Path, tag: &str) -> UncleanMarker {
        UncleanMarker {
            path: dir.join(format!("unclean.{}", sanitize_suffix(tag))),
        }
    }

    /// Returns whether the previous run ended uncleanly, then (re)arms the
    /// marker for this run.
    pub fn check_and_arm(&self) -> Result<bool, StateError> {
        let was_unclean = self.path.exists();
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, b"")?;
        Ok(was_unclean)
    }

    /// Graceful shutdown: state on disk is consistent.
    pub fn disarm(&self) -> Result<(), StateError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// History (phase 2: SQLite implementation)
// ---------------------------------------------------------------------------

/// Terminal record of a job for the history store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub job: JobId,
    pub name: String,
    pub category: Option<String>,
    pub final_dir: Option<String>,
    pub status: String,
    pub size: u64,
    pub completed_at_unix: i64,
}

pub trait HistoryStore {
    fn record(&mut self, entry: &HistoryEntry) -> Result<(), StateError>;
    fn list(&self, limit: usize) -> Result<Vec<HistoryEntry>, StateError>;
    fn delete(&mut self, job: JobId) -> Result<bool, StateError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seg: u32) -> JournalRecord {
        JournalRecord {
            job: JobId(1),
            file: FileId(2),
            segment_number: seg,
            offset: seg as u64 * 1000,
            len: 1000,
            crc32: 0xDEAD_0000 + seg,
            file_size: 5000,
        }
    }

    #[test]
    fn journal_roundtrip_and_compact() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = FsJournal::open(dir.path()).unwrap();
        for i in 0..5 {
            j.append(&rec(i)).unwrap();
        }
        j.sync().unwrap();
        assert_eq!(j.replay().unwrap().len(), 5);
        assert_eq!(j.replay().unwrap()[3], rec(3));

        j.compact().unwrap();
        assert!(j.replay().unwrap().is_empty());

        // append still works post-compact
        j.append(&rec(9)).unwrap();
        j.sync().unwrap();
        assert_eq!(j.replay().unwrap(), vec![rec(9)]);
    }

    #[test]
    fn journal_tolerates_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = FsJournal::open(dir.path()).unwrap();
        j.append(&rec(0)).unwrap();
        j.append(&rec(1)).unwrap();
        j.sync().unwrap();
        // simulate a crash mid-append
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.path().join("segments.journal"))
            .unwrap();
        f.write_all(b"{\"job\":1,\"file\":2,\"segment_nu").unwrap();
        drop(f);

        let j2 = FsJournal::open(dir.path()).unwrap();
        let recs = j2.replay().unwrap();
        assert_eq!(recs.len(), 2, "torn line must be dropped, prior kept");
    }

    #[test]
    fn snapshot_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        assert!(store.load().unwrap().is_none());

        let doc = QueueSnapshotDoc {
            jobs: vec![],
            next_job_id: 7,
            next_file_id: 42,
            download_paused: true,
            speed_limit_bps: Some(1_000_000),
        };
        store.save(&doc).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.next_job_id, 7);
        assert_eq!(loaded.next_file_id, 42);
        assert!(loaded.download_paused);
        assert_eq!(loaded.speed_limit_bps, Some(1_000_000));
        assert!(!dir.path().join("queue.json.tmp").exists());
    }

    #[test]
    fn job_journals_union_across_lease_files() {
        let dir = tempfile::tempdir().unwrap();

        // Two writers (two lease suffixes) journal different segments of
        // the same job — e.g. a worker died and the job was reclaimed.
        let mut a = JobJournals::open(dir.path(), "L1-1").unwrap();
        a.append(&rec(0)).unwrap();
        a.append(&rec(1)).unwrap();
        a.sync().unwrap();
        let mut b = JobJournals::open(dir.path(), "L2-9").unwrap();
        b.append(&rec(2)).unwrap();
        b.append(&rec(1)).unwrap(); // duplicate: union is idempotent upstream
        b.sync().unwrap();

        let recs = JobJournals::replay_job(dir.path(), JobId(1)).unwrap();
        assert_eq!(recs.len(), 4, "union of both lease files");
        let segs: std::collections::BTreeSet<u32> = recs.iter().map(|r| r.segment_number).collect();
        assert_eq!(segs.into_iter().collect::<Vec<_>>(), vec![0, 1, 2]);

        assert_eq!(JobJournals::replay_all(dir.path()).unwrap().len(), 4);

        // Authority folds + removes.
        a.remove_job(JobId(1)).unwrap();
        assert!(JobJournals::replay_job(dir.path(), JobId(1))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn snapshot_guard_fences_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        store
            .save(&QueueSnapshotDoc {
                next_job_id: 1,
                ..Default::default()
            })
            .unwrap();

        let err = store
            .save_guarded(
                &QueueSnapshotDoc {
                    next_job_id: 99,
                    ..Default::default()
                },
                &|| false, // deposed between write and commit
            )
            .unwrap_err();
        assert!(matches!(err, StateError::Corrupt(_)));
        // The old snapshot survives untouched.
        assert_eq!(store.load().unwrap().unwrap().next_job_id, 1);
    }

    #[test]
    fn unclean_marker_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let m = UncleanMarker::new(dir.path(), "local");
        assert!(!m.check_and_arm().unwrap(), "first run is clean");
        assert!(
            m.check_and_arm().unwrap(),
            "second arm without disarm = unclean"
        );
        m.disarm().unwrap();
        assert!(!m.check_and_arm().unwrap(), "after disarm = clean");
        m.disarm().unwrap();
        m.disarm().unwrap(); // idempotent
    }
}
