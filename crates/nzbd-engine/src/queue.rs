//! Queue state: admission, priority selection, totals/health accounting.
//!
//! Selection order preserves NZBGet semantics (ARCHITECTURE.md §8.2):
//! highest-priority schedulable job → first incomplete non-paused file →
//! next pending segment; force priority (≥900) bypasses every pause;
//! `PropagationDelay` filters too-young files; the failover ladder decides
//! which servers may take the segment at its current tier.

use crate::failover::{Candidates, Ladder, SegmentAttempt};
use nzbd_nzb::ParsedNzb;
use nzbd_state::QueueSnapshotDoc;
use nzbd_types::{
    DupeInfo, FileEntry, FileId, Health, Job, JobId, JobKind, JobStatus, Segment, SegmentState,
    ServerDef, ServerId,
};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct QueueState {
    pub jobs: Vec<Job>,
    pub next_job_id: u32,
    pub next_file_id: u32,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
}

/// Coordinates of one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegRef {
    pub job: JobId,
    pub file: FileId,
    pub seg_number: u32,
}

impl QueueState {
    // -- persistence ---------------------------------------------------------

    pub fn from_doc(doc: QueueSnapshotDoc) -> QueueState {
        let mut state = QueueState {
            jobs: doc.jobs,
            next_job_id: doc.next_job_id,
            next_file_id: doc.next_file_id,
            download_paused: doc.download_paused,
            speed_limit_bps: doc.speed_limit_bps,
        };
        // Leases are transient; anything in flight at the crash re-leases.
        for job in &mut state.jobs {
            for file in &mut job.files {
                for seg in &mut file.segments {
                    if matches!(seg.state, SegmentState::Leased { .. }) {
                        seg.state = SegmentState::Pending;
                    }
                }
            }
            if matches!(job.status, JobStatus::Downloading) {
                job.status = JobStatus::Queued;
            }
        }
        state
    }

    pub fn to_doc(&self) -> QueueSnapshotDoc {
        QueueSnapshotDoc {
            jobs: self.jobs.clone(),
            next_job_id: self.next_job_id,
            next_file_id: self.next_file_id,
            download_paused: self.download_paused,
            speed_limit_bps: self.speed_limit_bps,
        }
    }

    // -- lookup --------------------------------------------------------------

    pub fn job(&self, id: JobId) -> Option<&Job> {
        self.jobs.iter().find(|j| j.id == id)
    }

    pub fn job_mut(&mut self, id: JobId) -> Option<&mut Job> {
        self.jobs.iter_mut().find(|j| j.id == id)
    }

    pub fn file_mut(&mut self, job: JobId, file: FileId) -> Option<&mut FileEntry> {
        self.job_mut(job)?.files.iter_mut().find(|f| f.id == file)
    }

    pub fn segment_mut(&mut self, r: SegRef) -> Option<&mut Segment> {
        self.file_mut(r.job, r.file)?
            .segments
            .iter_mut()
            .find(|s| s.number == r.seg_number)
    }

    // -- admission -----------------------------------------------------------

    /// Add a parsed NZB as a job. `pause_extra_pars` queues `*.volNNN+MM.par2`
    /// files paused (delayed-par download, §3.2 — unpaused by the repair
    /// path in phase 2).
    pub fn admit_nzb(
        &mut self,
        name: String,
        parsed: &ParsedNzb,
        category: Option<String>,
        priority: i32,
        pause_extra_pars: bool,
    ) -> JobId {
        self.next_job_id += 1;
        let job_id = JobId(self.next_job_id);
        let category = category.or_else(|| parsed.meta.category.clone());
        let files = self.build_files(parsed, pause_extra_pars);

        let mut job = Job {
            id: job_id,
            kind: JobKind::Nzb,
            name,
            category,
            priority,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files,
            totals: Default::default(),
            status: JobStatus::Queued,
        };
        recompute_job_totals(&mut job);
        self.jobs.push(job);
        job_id
    }

    /// Register a URL job: no files yet, `Fetching` until the NZB arrives
    /// (then [`QueueState::complete_url_fetch`] fills it in).
    pub fn admit_url(
        &mut self,
        name: String,
        url: &str,
        category: Option<String>,
        priority: i32,
    ) -> JobId {
        self.next_job_id += 1;
        let job_id = JobId(self.next_job_id);
        self.jobs.push(Job {
            id: job_id,
            kind: JobKind::Url,
            name,
            category,
            priority,
            dupe: DupeInfo::default(),
            params: vec![("*URL".into(), url.to_string())],
            files: Vec::new(),
            totals: Default::default(),
            status: JobStatus::Fetching,
        });
        job_id
    }

    /// The fetched NZB for a URL job: populate files and queue it.
    pub fn complete_url_fetch(
        &mut self,
        job_id: JobId,
        parsed: &ParsedNzb,
        pause_extra_pars: bool,
    ) -> bool {
        let files = self.build_files(parsed, pause_extra_pars);
        let meta_category = parsed.meta.category.clone();
        let Some(job) = self.job_mut(job_id) else {
            return false;
        };
        if !matches!(job.status, JobStatus::Fetching) {
            return false;
        }
        if job.category.is_none() {
            job.category = meta_category;
        }
        job.files = files;
        job.status = JobStatus::Queued;
        recompute_job_totals(job);
        true
    }

    fn build_files(&mut self, parsed: &ParsedNzb, pause_extra_pars: bool) -> Vec<FileEntry> {
        let mut files = Vec::with_capacity(parsed.files.len());
        let mut seen_names: HashMap<String, u32> = HashMap::new();
        for pf in &parsed.files {
            self.next_file_id += 1;
            let file_id = FileId(self.next_file_id);
            let mut filename = sanitize_name(&pf.filename_hint());
            // Disambiguate duplicate names inside one NZB (would clobber on disk).
            let n = seen_names.entry(filename.to_lowercase()).or_insert(0);
            *n += 1;
            if *n > 1 {
                filename = format!("{}.dup{}", filename, *n - 1);
            }
            let lower = filename.to_lowercase();
            let is_par2 = lower.ends_with(".par2");
            let is_extra_par = is_par2 && lower.contains(".vol");

            files.push(FileEntry {
                id: file_id,
                subject: pf.subject.clone(),
                filename,
                filename_confirmed: false,
                is_par2,
                paused: pause_extra_pars && is_extra_par,
                groups: pf.groups.clone(),
                date: pf.date,
                segments: pf
                    .segments
                    .iter()
                    .map(|s| Segment {
                        message_id: s.message_id.clone().into_boxed_str(),
                        number: s.number,
                        size: s.bytes.min(u32::MAX as u64) as u32,
                        state: SegmentState::Pending,
                    })
                    .collect(),
                crc32: None,
                finalized: false,
            });
        }
        files
    }

    // -- accounting ----------------------------------------------------------

    pub fn recompute_all_totals(&mut self) {
        for job in &mut self.jobs {
            recompute_job_totals(job);
        }
    }

    /// Bytes still to fetch (pending + leased, non-paused files, active jobs).
    pub fn remaining_bytes(&self) -> u64 {
        self.jobs
            .iter()
            .filter(|j| {
                matches!(
                    j.status,
                    JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
                )
            })
            .map(|j| {
                j.files
                    .iter()
                    .filter(|f| !f.paused)
                    .flat_map(|f| &f.segments)
                    .filter(|s| {
                        matches!(s.state, SegmentState::Pending | SegmentState::Leased { .. })
                    })
                    .map(|s| s.size as u64)
                    .sum::<u64>()
            })
            .sum()
    }
}

pub fn recompute_job_totals(job: &mut Job) {
    let mut t = nzbd_types::JobTotals::default();
    for f in &job.files {
        let par = f.is_par2;
        for s in &f.segments {
            let size = s.size as u64;
            t.size += size;
            t.total_articles += 1;
            if par {
                t.par_size += size;
            }
            match s.state {
                SegmentState::Done { .. } => {
                    t.success_size += size;
                    t.success_articles += 1;
                }
                SegmentState::Failed => {
                    t.failed_size += size;
                    t.failed_articles += 1;
                    if par {
                        t.failed_par_size += size;
                    }
                }
                _ => {}
            }
        }
    }
    job.totals = t;
}

/// Filesystem-safe job/file names (path separators and control chars out).
pub fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    let out = if trimmed.is_empty() {
        "unnamed"
    } else {
        trimmed
    };
    // Keep well under PATH_MAX with room for ".part"/dup suffixes.
    out.chars().take(200).collect()
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

pub struct SelectionCtx<'a> {
    pub ladder: &'a Ladder<'a>,
    pub attempts: &'a mut HashMap<SegRef, SegmentAttempt>,
    pub is_blocked: &'a dyn Fn(ServerId) -> bool,
    /// Jobs executing on another node — invisible to local selection.
    pub delegated: &'a HashMap<JobId, String>,
    pub article_retries: u8,
    pub now_unix: i64,
    pub propagation_delay_secs: i64,
    /// Quota reached (or another soft hold): only force-priority jobs run.
    pub soft_hold: bool,
}

pub struct SelectionResult {
    pub lease: Option<SegRef>,
    /// Segments discovered unrecoverable during the scan (all tiers
    /// exhausted) — the owner fails them through the common path.
    pub exhausted: Vec<SegRef>,
}

/// Find the next pending segment `server` may take, in queue priority order.
/// Does not mutate segment states (the owner applies the lease); does
/// escalate per-segment attempt tiers as a side effect of candidate
/// computation (that is the ladder's contract).
pub fn next_for_server(
    state: &QueueState,
    server: &ServerDef,
    ctx: &mut SelectionCtx<'_>,
) -> SelectionResult {
    let mut exhausted = Vec::new();

    let mut order: Vec<&Job> = state
        .jobs
        .iter()
        .filter(|j| !ctx.delegated.contains_key(&j.id))
        .filter(|j| job_schedulable(j, state.download_paused || ctx.soft_hold))
        .collect();
    order.sort_by(|a, b| b.priority.cmp(&a.priority).then(a.id.cmp(&b.id)));

    for job in order {
        for file in &job.files {
            if file.paused || file.is_terminal() {
                continue;
            }
            if ctx.propagation_delay_secs > 0 {
                if let Some(date) = file.date {
                    if date + ctx.propagation_delay_secs > ctx.now_unix {
                        continue; // too young: not yet propagated everywhere
                    }
                }
            }
            let age_days = file
                .date
                .map(|d| ((ctx.now_unix - d).max(0) / 86_400) as u32);
            for seg in &file.segments {
                if !matches!(seg.state, SegmentState::Pending) {
                    continue;
                }
                let r = SegRef {
                    job: job.id,
                    file: file.id,
                    seg_number: seg.number,
                };
                let att = ctx
                    .attempts
                    .entry(r)
                    .or_insert_with(|| SegmentAttempt::new(ctx.article_retries));
                match ctx.ladder.current_candidates(att, ctx.is_blocked, age_days) {
                    Candidates::Servers(ids) if ids.contains(&server.id) => {
                        return SelectionResult {
                            lease: Some(r),
                            exhausted,
                        };
                    }
                    Candidates::Servers(_) | Candidates::WaitForBlocked => {}
                    Candidates::Exhausted => exhausted.push(r),
                }
            }
        }
    }
    SelectionResult {
        lease: None,
        exhausted,
    }
}

fn job_schedulable(job: &Job, download_paused: bool) -> bool {
    match job.status {
        JobStatus::Queued | JobStatus::Downloading => job.force_priority() || !download_paused,
        JobStatus::Paused => job.force_priority(),
        _ => false,
    }
}

/// Parse the recovery-block count from a `*.volXX+NN.par2` filename.
pub fn vol_par_blocks(filename: &str) -> Option<u32> {
    let lower = filename.to_ascii_lowercase();
    let vol = lower.rfind(".vol")?;
    let rest = &lower[vol + 4..];
    let plus = rest.find('+')?;
    let end = rest[plus + 1..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| plus + 1 + i)
        .unwrap_or(rest.len());
    rest[plus + 1..end].parse().ok()
}

/// Choose the smallest set of paused par files covering `needed` recovery
/// blocks (NZBGet's delayed-par selection, simplified): prefer the smallest
/// single file that covers it; otherwise accumulate largest-first.
pub fn pick_par_files(candidates: &[(FileId, u32)], needed: u32) -> Vec<FileId> {
    let mut sorted: Vec<_> = candidates.to_vec();
    sorted.sort_by_key(|(_, blocks)| *blocks);
    if let Some((id, _)) = sorted.iter().find(|(_, b)| *b >= needed) {
        return vec![*id];
    }
    let mut out = Vec::new();
    let mut have = 0u32;
    for (id, blocks) in sorted.iter().rev() {
        if have >= needed {
            break;
        }
        out.push(*id);
        have += blocks;
    }
    out
}

/// Health verdict for a finished job: below critical health the download is
/// beyond repair (would be failed/parked by the health check; phase 2 adds
/// the par-aware paths).
pub fn final_status(job: &Job) -> (JobStatus, Health) {
    let health = Health::calc(&job.totals);
    let critical = Health::calc_critical(&job.totals, true);
    if health < critical {
        (JobStatus::Failed, health)
    } else {
        (JobStatus::Completed, health)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{CertLevel, TlsMode};

    fn server(id: u32, tier: u8) -> ServerDef {
        ServerDef {
            id: ServerId(id),
            name: format!("s{id}"),
            host: "h".into(),
            port: 119,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier,
            group: 0,
            fill: false,
            max_connections: 4,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }
    }

    fn sample_nzb(files: &[(&str, u32)]) -> ParsedNzb {
        let mut xml = String::from(r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#);
        for (name, segs) in files {
            xml.push_str(&format!(
                r#"<file poster="p" date="1700000000" subject="&quot;{name}&quot; yEnc (1/{segs})"><groups><group>a.b</group></groups><segments>"#
            ));
            for n in 1..=*segs {
                xml.push_str(&format!(
                    r#"<segment bytes="1000" number="{n}">{name}.{n}@x</segment>"#
                ));
            }
            xml.push_str("</segments></file>");
        }
        xml.push_str("</nzb>");
        nzbd_nzb::parse(xml.as_bytes()).unwrap()
    }

    #[test]
    fn admission_pauses_extra_pars_and_counts() {
        let mut q = QueueState::default();
        let parsed = sample_nzb(&[("data.rar", 3), ("data.par2", 1), ("data.vol00+01.par2", 2)]);
        let id = q.admit_nzb("job".into(), &parsed, None, 0, true);
        let job = q.job(id).unwrap();
        assert_eq!(job.files.len(), 3);
        assert!(!job.files[0].paused);
        assert!(!job.files[1].paused, "main par2 stays active");
        assert!(job.files[1].is_par2);
        assert!(job.files[2].paused, "vol par is delayed");
        assert_eq!(job.totals.total_articles, 6);
        assert_eq!(job.totals.size, 6000);
        assert_eq!(job.totals.par_size, 3000);
    }

    #[test]
    fn selection_respects_priority_pause_and_force() {
        let mut q = QueueState::default();
        let low = q.admit_nzb("low".into(), &sample_nzb(&[("a.bin", 2)]), None, 0, true);
        let high = q.admit_nzb("high".into(), &sample_nzb(&[("b.bin", 2)]), None, 100, true);

        let servers = vec![server(1, 0)];
        let ladder = Ladder::new(&servers);
        let mut attempts = HashMap::new();
        let not_blocked = |_: ServerId| false;
        let no_delegation: HashMap<JobId, String> = HashMap::new();
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
        };

        let r = next_for_server(&q, &servers[0], &mut ctx);
        assert_eq!(r.lease.unwrap().job, high, "higher priority first");

        // Global pause blocks everything…
        q.download_paused = true;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
        };
        assert!(next_for_server(&q, &servers[0], &mut ctx).lease.is_none());

        // …except force priority.
        q.job_mut(low).unwrap().priority = nzbd_types::PRIORITY_FORCE;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
        };
        let r = next_for_server(&q, &servers[0], &mut ctx);
        assert_eq!(r.lease.unwrap().job, low, "force ignores global pause");
    }

    #[test]
    fn selection_skips_paused_files_and_finds_tiered_server() {
        let mut q = QueueState::default();
        let id = q.admit_nzb(
            "j".into(),
            &sample_nzb(&[("x.vol00+01.par2", 1), ("x.rar", 1)]),
            None,
            0,
            true,
        );
        let servers = vec![server(1, 0), server(2, 1)];
        let ladder = Ladder::new(&servers);
        let mut attempts = HashMap::new();
        let not_blocked = |_: ServerId| false;
        let no_delegation: HashMap<JobId, String> = HashMap::new();

        // Tier-1 server gets nothing while tier 0 is viable.
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
        };
        assert!(next_for_server(&q, &servers[1], &mut ctx).lease.is_none());

        // Tier-0 server gets the rar (vol-par is paused).
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
        };
        let r = next_for_server(&q, &servers[0], &mut ctx).lease.unwrap();
        assert_eq!(r.job, id);
        let file = q
            .job(id)
            .unwrap()
            .files
            .iter()
            .find(|f| f.id == r.file)
            .unwrap();
        assert_eq!(file.filename, "x.rar");
    }

    #[test]
    fn vol_block_parsing_and_selection() {
        assert_eq!(vol_par_blocks("x.vol00+01.par2"), Some(1));
        assert_eq!(vol_par_blocks("Show.S01.vol127+64.PAR2"), Some(64));
        assert_eq!(vol_par_blocks("x.par2"), None);
        assert_eq!(vol_par_blocks("x.vol7.par2"), None);

        let c = [
            (FileId(1), 1),
            (FileId(2), 2),
            (FileId(3), 8),
            (FileId(4), 16),
        ];
        assert_eq!(
            pick_par_files(&c, 2),
            vec![FileId(2)],
            "smallest single cover"
        );
        assert_eq!(pick_par_files(&c, 5), vec![FileId(3)]);
        assert_eq!(
            pick_par_files(&c, 20),
            vec![FileId(4), FileId(3)],
            "accumulate largest-first"
        );
        assert_eq!(pick_par_files(&c, 100).len(), 4, "take everything if short");
    }

    #[test]
    fn sanitize_names() {
        assert_eq!(sanitize_name("a/b\\c:d"), "a_b_c_d");
        assert_eq!(sanitize_name("  .hidden.  "), "hidden");
        assert_eq!(sanitize_name(""), "unnamed");
    }

    #[test]
    fn final_status_uses_health_gate() {
        let mut q = QueueState::default();
        let id = q.admit_nzb("j".into(), &sample_nzb(&[("a.bin", 10)]), None, 0, true);
        let job = q.job_mut(id).unwrap();
        // 9 done, 1 failed: health 900 ≥ critical 850 → completed
        for (i, s) in job.files[0].segments.iter_mut().enumerate() {
            s.state = if i == 0 {
                SegmentState::Failed
            } else {
                SegmentState::Done {
                    offset: 0,
                    len: 1000,
                    crc: 0,
                }
            };
        }
        recompute_job_totals(job);
        let (status, health) = final_status(job);
        assert_eq!(status, JobStatus::Completed);
        assert_eq!(health.0, 900);

        // 8 failed → health 200 < critical 850 → failed
        for s in job.files[0].segments.iter_mut().take(8) {
            s.state = SegmentState::Failed;
        }
        recompute_job_totals(job);
        let (status, health) = final_status(job);
        assert_eq!(status, JobStatus::Failed);
        assert_eq!(health.0, 200);
    }
}
