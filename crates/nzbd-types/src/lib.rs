//! Domain model for nzbd. No I/O lives here.
//!
//! Naming maps to the NZBGet reference implementation where behavior is
//! carried over (see docs/ARCHITECTURE.md §3): `Job` ≈ NzbInfo,
//! `FileEntry` ≈ FileInfo, `Segment` ≈ ArticleInfo.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServerId(pub u32);

// ---------------------------------------------------------------------------
// Priorities (NZBGet-compatible scale; 900 == Force, ignores pause/quota)
// ---------------------------------------------------------------------------

pub const PRIORITY_FORCE: i32 = 900;

// ---------------------------------------------------------------------------
// Jobs, files, segments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Nzb,
    Url,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DupeMode {
    Score,
    All,
    Force,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DupeInfo {
    pub key: String,
    pub score: i32,
    pub mode: Option<DupeMode>,
}

/// Byte/article accounting for a job. All sizes in bytes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct JobTotals {
    pub size: u64,
    pub par_size: u64,
    pub failed_size: u64,
    pub failed_par_size: u64,
    pub success_size: u64,
    pub total_articles: u32,
    pub success_articles: u32,
    pub failed_articles: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentState {
    Pending,
    Leased {
        server: ServerId,
    },
    /// Decoded and written. `offset`/`len` are positions in the output file
    /// (yEnc `begin - 1` / part length); `crc` is the decoded part CRC32.
    Done {
        offset: u64,
        len: u32,
        crc: u32,
    },
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub message_id: Box<str>,
    pub number: u32,
    /// Size from the NZB `<segment bytes=..>` attribute (encoded size, advisory).
    pub size: u32,
    pub state: SegmentState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub id: FileId,
    pub subject: String,
    pub filename: String,
    pub filename_confirmed: bool,
    pub is_par2: bool,
    pub paused: bool,
    pub groups: Vec<String>,
    /// Post date (unix) from the NZB — drives retention pre-fail and
    /// `PropagationDelay`.
    #[serde(default)]
    pub date: Option<i64>,
    pub segments: Vec<Segment>,
    /// Combined CRC32 of the decoded file, available once all segments are done.
    pub crc32: Option<u32>,
    /// Output file assembled and atomically renamed into place.
    #[serde(default)]
    pub finalized: bool,
}

impl FileEntry {
    /// All segments in a terminal state (done or failed).
    pub fn is_terminal(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s.state, SegmentState::Done { .. } | SegmentState::Failed))
    }

    pub fn done_segments(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| matches!(s.state, SegmentState::Done { .. }))
            .count()
    }

    pub fn has_any_done(&self) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s.state, SegmentState::Done { .. }))
    }
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    None,
    Tls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertLevel {
    None,
    Minimal,
    Strict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerDef {
    pub id: ServerId,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: TlsMode,
    pub username: Option<String>,
    pub password: Option<String>,
    pub active: bool,
    /// Normalized failover tier (NZBGet "Level"): 0 = main, 1 = first backup…
    pub tier: u8,
    /// Servers with the same (tier, group>0) are interchangeable: a
    /// per-article failure on one skips the whole group.
    pub group: u8,
    /// Fill server (NZBGet "Optional"): when blocked, never stalls progress —
    /// selection falls through to the next tier instead of waiting.
    pub fill: bool,
    pub max_connections: u16,
    /// NNTP command pipelining depth (first-class, unlike NZBGet). 1 = off.
    pub pipeline_depth: u8,
    /// 0 = unlimited. Articles older than this are pre-failed on this server.
    pub retention_days: u32,
    pub cert_verification: CertLevel,
}

// ---------------------------------------------------------------------------
// Health (per-mille), formulas carried exactly from NZBGet
// (daemon/queue/DownloadInfo.cpp — CalcHealth / CalcCriticalHealth)
// ---------------------------------------------------------------------------

/// Health of a download in per-mille (0..=1000). 1000 = 100.0%.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Health(pub u16);

impl Health {
    pub const PERFECT: Health = Health(1000);

    /// Fraction of *non-par* data successfully downloaded, per-mille.
    ///
    /// `health = (size − parSize − (failed − parFailed)) × 1000 / (size − parSize)`,
    /// clamped to 999 if any non-par bytes failed, 1000 iff nothing failed.
    pub fn calc(t: &JobTotals) -> Health {
        let non_par_failed = t.failed_size.saturating_sub(t.failed_par_size);
        if non_par_failed == 0 {
            return Health::PERFECT;
        }
        let denom = t.size.saturating_sub(t.par_size);
        if denom == 0 {
            return Health(0);
        }
        let raw = denom.saturating_sub(non_par_failed).saturating_mul(1000) / denom;
        Health(raw.min(999) as u16)
    }

    /// The health threshold below which par-repair is hopeless.
    ///
    /// `goodPar = parSize − parFailed`;
    /// 0 when par data ≥ half of total (repair always feasible);
    /// else `(size − 2·goodPar) × 1000 / (size − goodPar)`;
    /// 850 as an empirical fallback when the result is 1000 and estimation is
    /// allowed (guards against renamed/undetected par files).
    pub fn calc_critical(t: &JobTotals, allow_estimation: bool) -> Health {
        let good_par = t.par_size.saturating_sub(t.failed_par_size);
        if t.size == 0 || good_par.saturating_mul(2) >= t.size {
            return Health(0);
        }
        let denom = t.size - good_par; // > 0 because good_par*2 < size
        let raw = (t.size - 2 * good_par).saturating_mul(1000) / denom;
        let raw = raw.min(1000) as u16;
        if raw == 1000 && allow_estimation {
            Health(850)
        } else {
            Health(raw)
        }
    }
}

// ---------------------------------------------------------------------------
// Job status (native vocabulary; compat strings live in nzbd-compat)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostStage {
    ParRename,
    ParVerify,
    ParRepair,
    RarRename,
    Unpack,
    Cleanup,
    Move,
    PostUnpackRename,
    Script,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Downloading,
    Paused,
    Fetching, // URL fetch
    PostQueued,
    Post { stage: PostStage },
    Completed,
    Failed,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub kind: JobKind,
    pub name: String,
    pub category: Option<String>,
    pub priority: i32,
    pub dupe: DupeInfo,
    /// Post-processing parameters, including e.g. Sonarr's `drone` tracking id.
    pub params: Vec<(String, String)>,
    pub files: Vec<FileEntry>,
    pub totals: JobTotals,
    pub status: JobStatus,
}

impl Job {
    pub fn force_priority(&self) -> bool {
        self.priority >= PRIORITY_FORCE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(size: u64, par: u64, failed: u64, par_failed: u64) -> JobTotals {
        JobTotals {
            size,
            par_size: par,
            failed_size: failed,
            failed_par_size: par_failed,
            ..Default::default()
        }
    }

    #[test]
    fn health_perfect_when_nothing_failed() {
        assert_eq!(Health::calc(&totals(1000, 200, 0, 0)), Health(1000));
        // par failures alone don't reduce health (non-par data is intact)
        assert_eq!(Health::calc(&totals(1000, 200, 50, 50)), Health(1000));
    }

    #[test]
    fn health_clamps_to_999_on_any_nonpar_failure() {
        // 1 byte of non-par failure out of 800 non-par bytes: ratio rounds to
        // 998 (floor division), and must never report 1000.
        let h = Health::calc(&totals(1000, 200, 1, 0));
        assert!(h < Health(1000) && h >= Health(998), "got {h:?}");
    }

    #[test]
    fn health_proportional() {
        // non-par = 800, non-par failed = 400 -> 500 per-mille
        assert_eq!(Health::calc(&totals(1000, 200, 400, 0)), Health(500));
        // everything non-par failed -> 0
        assert_eq!(Health::calc(&totals(1000, 200, 800, 0)), Health(0));
    }

    #[test]
    fn critical_health_zero_when_par_covers_half() {
        // good par (500) * 2 >= size (1000): repair always feasible
        assert_eq!(
            Health::calc_critical(&totals(1000, 500, 0, 0), false),
            Health(0)
        );
    }

    #[test]
    fn critical_health_formula() {
        // size=1000, goodPar=200 -> (1000-400)*1000/(1000-200) = 750
        assert_eq!(
            Health::calc_critical(&totals(1000, 200, 0, 0), false),
            Health(750)
        );
        // failed par shrinks goodPar: par=200 with 100 failed -> goodPar=100
        // (1000-200)*1000/(1000-100) = 888
        assert_eq!(
            Health::calc_critical(&totals(1000, 200, 100, 100), false),
            Health(888)
        );
    }

    #[test]
    fn critical_health_estimation_fallback() {
        // No par at all -> raw = 1000; with estimation allowed -> 850
        assert_eq!(
            Health::calc_critical(&totals(1000, 0, 0, 0), true),
            Health(850)
        );
        assert_eq!(
            Health::calc_critical(&totals(1000, 0, 0, 0), false),
            Health(1000)
        );
    }

    #[test]
    fn force_priority_threshold() {
        let mut job = Job {
            id: JobId(1),
            kind: JobKind::Nzb,
            name: "x".into(),
            category: None,
            priority: 100,
            dupe: DupeInfo::default(),
            params: vec![],
            files: vec![],
            totals: JobTotals::default(),
            status: JobStatus::Queued,
        };
        assert!(!job.force_priority());
        job.priority = PRIORITY_FORCE;
        assert!(job.force_priority());
    }
}
