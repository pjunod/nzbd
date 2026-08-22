//! Dormant BitTorrent runtime ownership and restore reconciliation.
//!
//! This module intentionally knows `JobId` but has no engine dependency. The
//! maintained adapter receives only [`RestoreRequest`] values and returns
//! engine identities, keeping raw rqbit handles and queue identities apart.

use crate::backend::{BackendFact, SafeError, StopReason, TransferProgress};
use nzbd_types::{Job, JobId, JobKind, JobStatus, TorrentPhase};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

pub const MAX_RESTORE_DIAGNOSTICS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EngineIdentity {
    pub id: usize,
    pub info_hash_v1: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub job: JobId,
    pub info_hash_v1: String,
    pub metadata_file: PathBuf,
    pub selected_files: Vec<usize>,
    pub preferred_engine_id: Option<usize>,
    pub start_paused: bool,
    pub resume_after_restore: bool,
    pub force_recheck: bool,
    pub trusted_downloaded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreDiagnostic {
    MissingTorrentRecord,
    InvalidInfoHash,
    UnsafeMetadataPath,
    DuplicateInfoHash,
    DuplicatePreferredIdentity,
    DeletedRecord,
    UnsafePayloadRoot,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RestorePlan {
    pub requests: Vec<RestoreRequest>,
    pub diagnostics: Vec<RestoreDiagnostic>,
}

/// Build the only set the engine may restore. Diagnostics are categorical,
/// bounded, and contain no names, paths, magnets, tracker URLs, or passkeys.
pub fn plan_restore(
    jobs: &[Job],
    observed: &HashMap<String, ObservedResumeState>,
    torrent_root: &Path,
    scheduler_allowed: &HashSet<JobId>,
) -> RestorePlan {
    let mut plan = RestorePlan::default();
    let mut hashes = HashSet::new();
    let mut preferred_ids = HashSet::new();
    for job in jobs.iter().filter(|job| job.kind == JobKind::Torrent) {
        let Some(record) = &job.torrent else {
            push_diagnostic(&mut plan, RestoreDiagnostic::MissingTorrentRecord);
            continue;
        };
        if job.status == JobStatus::Deleted {
            push_diagnostic(&mut plan, RestoreDiagnostic::DeletedRecord);
            continue;
        }
        if !valid_hash(&record.info_hash_v1) {
            push_diagnostic(&mut plan, RestoreDiagnostic::InvalidInfoHash);
            continue;
        }
        if !safe_relative_path(&record.metadata_file) {
            push_diagnostic(&mut plan, RestoreDiagnostic::UnsafeMetadataPath);
            continue;
        }
        if record
            .content_path
            .as_ref()
            .is_some_and(|path| !payload_is_within_root(path, torrent_root))
        {
            push_diagnostic(&mut plan, RestoreDiagnostic::UnsafePayloadRoot);
            continue;
        }
        if !hashes.insert(record.info_hash_v1.clone()) {
            push_diagnostic(&mut plan, RestoreDiagnostic::DuplicateInfoHash);
            continue;
        }
        let state = observed.get(&record.info_hash_v1);
        let preferred_engine_id = state.map(|state| state.engine_id);
        if preferred_engine_id.is_some_and(|id| !preferred_ids.insert(id)) {
            push_diagnostic(&mut plan, RestoreDiagnostic::DuplicatePreferredIdentity);
            continue;
        }
        let readiness_disagrees = state.is_some_and(|state| {
            state.finished != record.ready_at_unix.is_some()
                || state.verified_bytes != record.downloaded_bytes
        });
        let resume_after_restore = scheduler_allowed.contains(&job.id)
            && matches!(job.status, JobStatus::Queued | JobStatus::Downloading)
            && !matches!(
                record.phase,
                TorrentPhase::PausedDownload | TorrentPhase::PausedSeed | TorrentPhase::Failed
            );
        plan.requests.push(RestoreRequest {
            job: job.id,
            info_hash_v1: record.info_hash_v1.clone(),
            metadata_file: record.metadata_file.clone(),
            selected_files: record
                .files
                .iter()
                .enumerate()
                .filter_map(|(index, file)| file.selected.then_some(index))
                .collect(),
            preferred_engine_id,
            start_paused: true,
            resume_after_restore,
            force_recheck: readiness_disagrees,
            trusted_downloaded_bytes: record.downloaded_bytes,
        });
    }
    plan
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedResumeState {
    pub engine_id: usize,
    pub verified_bytes: u64,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisabledWithLiveTorrents {
    pub count: usize,
}

pub fn refuse_disabled_with_live_torrents(jobs: &[Job]) -> Result<(), DisabledWithLiveTorrents> {
    let count = jobs
        .iter()
        .filter(|job| job.kind == JobKind::Torrent && job.status != JobStatus::Deleted)
        .count();
    if count == 0 {
        Ok(())
    } else {
        Err(DisabledWithLiveTorrents { count })
    }
}

#[derive(Debug, Default)]
pub struct RuntimeAssociations {
    jobs: HashMap<JobId, EngineIdentity>,
    hashes: HashMap<String, JobId>,
    ids: HashMap<usize, JobId>,
}

impl RuntimeAssociations {
    pub fn associate(
        &mut self,
        job: JobId,
        identity: EngineIdentity,
    ) -> Result<(), AssociationError> {
        if self.jobs.contains_key(&job)
            || self.hashes.contains_key(&identity.info_hash_v1)
            || self.ids.contains_key(&identity.id)
        {
            return Err(AssociationError::Duplicate);
        }
        self.hashes.insert(identity.info_hash_v1.clone(), job);
        self.ids.insert(identity.id, job);
        self.jobs.insert(job, identity);
        Ok(())
    }

    pub fn engine_for_job(&self, job: JobId) -> Option<&EngineIdentity> {
        self.jobs.get(&job)
    }

    pub fn job_for_hash(&self, hash: &str) -> Option<JobId> {
        self.hashes.get(hash).copied()
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn translate_fact(
        &self,
        identity: &EngineIdentity,
        fact: EngineStructuralFact,
    ) -> Option<BackendFact> {
        let job = *self.ids.get(&identity.id)?;
        if self.hashes.get(&identity.info_hash_v1) != Some(&job) {
            return None;
        }
        Some(match fact {
            EngineStructuralFact::Ready { content_path } => {
                BackendFact::Ready { job, content_path }
            }
            EngineStructuralFact::Stopped { reason } => BackendFact::Stopped { job, reason },
            EngineStructuralFact::StorageFull => BackendFact::Stopped {
                job,
                reason: StopReason::StorageFull,
            },
            EngineStructuralFact::MissingContent => BackendFact::Stopped {
                job,
                reason: StopReason::MissingContent,
            },
            EngineStructuralFact::Transient => BackendFact::Stopped {
                job,
                reason: StopReason::Transient,
            },
            EngineStructuralFact::Unrecoverable { error } => BackendFact::Failed { job, error },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineStructuralFact {
    Ready {
        content_path: PathBuf,
    },
    Stopped {
        reason: StopReason,
    },
    StorageFull,
    MissingContent,
    Transient,
    /// The adapter must explicitly classify an error as unrecoverable before
    /// it is allowed to cross the backend boundary as `Failed`.
    Unrecoverable {
        error: SafeError,
    },
}

/// Queue-owner side effects produced while folding backend traffic. The
/// caller persists `durable_changed` at its normal snapshot cadence and
/// latches the shared disk guard when `storage_hold` is set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub durable_changed: bool,
    pub storage_hold: bool,
}

/// Fold a replaceable progress sample into durable queue state. Volatile
/// rates and peer counts deliberately remain outside [`nzbd_types::Job`].
pub fn reconcile_progress(job: &mut Job, progress: &TransferProgress) -> ReconcileOutcome {
    let Some(torrent) = torrent_mut(job) else {
        return ReconcileOutcome::default();
    };
    let downloaded = progress.downloaded_bytes.min(torrent.selected_bytes);
    let uploaded = torrent.uploaded_bytes.max(progress.uploaded_bytes);
    let activity = match (torrent.last_activity_unix, progress.last_activity_unix) {
        (Some(old), Some(new)) => Some(old.max(new)),
        (old, new) => old.or(new),
    };
    let changed = torrent.downloaded_bytes != downloaded
        || torrent.uploaded_bytes != uploaded
        || torrent.last_activity_unix != activity;
    torrent.downloaded_bytes = downloaded;
    torrent.uploaded_bytes = uploaded;
    torrent.last_activity_unix = activity;
    ReconcileOutcome {
        durable_changed: changed,
        storage_hold: false,
    }
}

/// Fold one reliable structural fact. Readiness is accepted only when the
/// latest engine sample proves every selected byte hash-verified; a bare
/// engine "finished" phase can therefore never make queue state ready.
pub fn reconcile_fact(
    job: &mut Job,
    fact: BackendFact,
    latest: Option<&TransferProgress>,
    now_unix: i64,
) -> ReconcileOutcome {
    if fact_job(&fact) != job.id {
        return ReconcileOutcome::default();
    }
    if job.kind != JobKind::Torrent || job.torrent.is_none() {
        return ReconcileOutcome::default();
    }
    match fact {
        BackendFact::MetadataReady {
            torrent: metadata, ..
        } => {
            let name_changed = job.name != metadata.name || job.name_provisional;
            let torrent = job.torrent.as_mut().unwrap();
            let changed = torrent.info_hash_v1 != metadata.info_hash_v1
                || torrent.total_bytes != metadata.total_bytes
                || torrent.selected_bytes != metadata.selected_bytes
                || torrent.phase != TorrentPhase::Queued
                || name_changed;
            torrent.info_hash_v1 = metadata.info_hash_v1;
            torrent.total_bytes = metadata.total_bytes;
            torrent.selected_bytes = metadata.selected_bytes;
            torrent.phase = TorrentPhase::Queued;
            job.name = metadata.name;
            job.name_provisional = false;
            ReconcileOutcome {
                durable_changed: changed,
                storage_hold: false,
            }
        }
        BackendFact::Ready { content_path, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            let verified = latest.map_or(0, |progress| progress.verified_bytes);
            if verified < torrent.selected_bytes || torrent.selected_bytes == 0 {
                return ReconcileOutcome::default();
            }
            torrent.downloaded_bytes = torrent.selected_bytes;
            torrent.ready_at_unix.get_or_insert(now_unix);
            torrent.content_path = Some(content_path);
            torrent.phase = TorrentPhase::Seeding;
            torrent.last_error = None;
            job.status = JobStatus::Downloading;
            ReconcileOutcome {
                durable_changed: true,
                storage_hold: false,
            }
        }
        BackendFact::Stopped { reason, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            let storage_hold = reason == StopReason::StorageFull;
            match reason {
                StopReason::Paused | StopReason::StorageFull => {
                    torrent.phase = if torrent.ready_at_unix.is_some() {
                        TorrentPhase::PausedSeed
                    } else {
                        TorrentPhase::PausedDownload
                    };
                    job.status = JobStatus::Paused;
                }
                StopReason::MissingContent => {
                    torrent.phase = TorrentPhase::MissingFiles;
                    job.status = JobStatus::Paused;
                }
                StopReason::Transient => {
                    torrent.phase = TorrentPhase::Downloading;
                    job.status = JobStatus::Downloading;
                }
                StopReason::SeedPolicyReached => {
                    torrent.phase = TorrentPhase::PausedSeed;
                    job.status = JobStatus::Paused;
                }
                StopReason::Removed | StopReason::Shutdown => {}
            }
            ReconcileOutcome {
                durable_changed: true,
                storage_hold,
            }
        }
        BackendFact::Failed { error, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            torrent.phase = TorrentPhase::Failed;
            torrent.last_error = Some(error.as_str().to_owned());
            job.status = JobStatus::Failed;
            ReconcileOutcome {
                durable_changed: true,
                storage_hold: false,
            }
        }
    }
}

fn torrent_mut(job: &mut Job) -> Option<&mut nzbd_types::TorrentRecord> {
    (job.kind == JobKind::Torrent).then_some(())?;
    job.torrent.as_mut()
}

fn fact_job(fact: &BackendFact) -> JobId {
    match fact {
        BackendFact::MetadataReady { job, .. }
        | BackendFact::Ready { job, .. }
        | BackendFact::Stopped { job, .. }
        | BackendFact::Failed { job, .. } => *job,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssociationError {
    Duplicate,
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 40
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Resolve the existing portion of a payload path so a symlink cannot make a
/// lexically contained path escape. The leaf may not exist yet during restore,
/// so canonicalization deliberately stops at its nearest existing ancestor.
fn payload_is_within_root(path: &Path, root: &Path) -> bool {
    let Some(path) = normalize_absolute(path) else {
        return false;
    };
    let Some(root) = normalize_absolute(root) else {
        return false;
    };
    if !path.starts_with(&root) {
        return false;
    }

    let Ok(canonical_root) = root.canonicalize() else {
        return false;
    };
    let mut existing = path.as_path();
    loop {
        match existing.canonicalize() {
            Ok(canonical_existing) => return canonical_existing.starts_with(&canonical_root),
            Err(_) => {
                let Some(parent) = existing.parent() else {
                    return false;
                };
                existing = parent;
            }
        }
    }
}

fn normalize_absolute(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
        }
    }
    Some(normalized)
}

fn push_diagnostic(plan: &mut RestorePlan, diagnostic: RestoreDiagnostic) {
    if plan.diagnostics.len() < MAX_RESTORE_DIAGNOSTICS {
        plan.diagnostics.push(diagnostic);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{
        DupeInfo, JobTotals, SeedPolicy, TorrentFileRecord, TorrentRecord, TorrentSource,
    };

    fn job(id: u32, hash: &str, status: JobStatus) -> Job {
        Job {
            id: JobId(id),
            kind: JobKind::Torrent,
            name: "not logged".into(),
            dir_name: "torrent".into(),
            name_provisional: false,
            queued_at_unix: 1,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: JobTotals::default(),
            status,
            torrent: Some(TorrentRecord {
                info_hash_v1: hash.into(),
                source: TorrentSource::Metainfo,
                metadata_file: PathBuf::from("meta/selected.torrent"),
                phase: TorrentPhase::Downloading,
                files: vec![
                    TorrentFileRecord {
                        path: "one".into(),
                        length: 1,
                        selected: true,
                    },
                    TorrentFileRecord {
                        path: "two".into(),
                        length: 1,
                        selected: false,
                    },
                ],
                total_bytes: 2,
                selected_bytes: 1,
                downloaded_bytes: 1,
                uploaded_bytes: 0,
                seeding_seconds: 0,
                ready_at_unix: None,
                content_path: None,
                seed_policy: SeedPolicy::default(),
                last_activity_unix: None,
                last_error: None,
            }),
            stages: Vec::new(),
        }
    }

    #[test]
    fn durable_queue_selects_one_paused_restore_and_queue_control_resumes_it() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let unknown = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let observed = HashMap::from([
            (
                hash.to_string(),
                ObservedResumeState {
                    engine_id: 7,
                    verified_bytes: 1,
                    finished: false,
                },
            ),
            (
                unknown,
                ObservedResumeState {
                    engine_id: 8,
                    verified_bytes: 99,
                    finished: true,
                },
            ),
        ]);
        let plan = plan_restore(
            &[job(10, hash, JobStatus::Queued)],
            &observed,
            Path::new("/torrents"),
            &HashSet::from([JobId(10)]),
        );
        assert_eq!(plan.requests.len(), 1);
        let request = &plan.requests[0];
        assert!(request.start_paused);
        assert!(request.resume_after_restore);
        assert_eq!(request.preferred_engine_id, Some(7));
        assert_eq!(request.selected_files, vec![0]);
        assert!(plan.diagnostics.is_empty());
    }

    #[test]
    fn readiness_or_checkpoint_disagreement_forces_recheck_before_ready_claim() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let observed = HashMap::from([(
            hash.to_string(),
            ObservedResumeState {
                engine_id: 7,
                verified_bytes: 2,
                finished: true,
            },
        )]);
        let plan = plan_restore(
            &[job(10, hash, JobStatus::Paused)],
            &observed,
            Path::new("/torrents"),
            &HashSet::new(),
        );
        assert!(plan.requests[0].force_recheck);
        assert!(!plan.requests[0].resume_after_restore);
        assert_eq!(plan.requests[0].trusted_downloaded_bytes, 1);
    }

    #[test]
    fn association_owner_rejects_duplicate_engine_handles() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut associations = RuntimeAssociations::default();
        associations
            .associate(
                JobId(10),
                EngineIdentity {
                    id: 7,
                    info_hash_v1: hash.into(),
                },
            )
            .unwrap();
        assert_eq!(associations.len(), 1);
        assert_eq!(associations.job_for_hash(hash), Some(JobId(10)));
        assert!(matches!(
            associations.translate_fact(
                associations.engine_for_job(JobId(10)).unwrap(),
                EngineStructuralFact::Stopped {
                    reason: crate::backend::StopReason::Paused
                }
            ),
            Some(crate::backend::BackendFact::Stopped { job: JobId(10), .. })
        ));
        assert_eq!(
            associations.associate(
                JobId(11),
                EngineIdentity {
                    id: 7,
                    info_hash_v1: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
                }
            ),
            Err(AssociationError::Duplicate)
        );
    }

    #[test]
    fn completion_requires_all_selected_bytes_verified_and_ready_seed_stays_live() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut job = job(10, hash, JobStatus::Downloading);
        let content_path = PathBuf::from("/torrents/example");
        let incomplete = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 0,
            ..Default::default()
        };

        assert_eq!(
            reconcile_fact(
                &mut job,
                BackendFact::Ready {
                    job: JobId(10),
                    content_path: content_path.clone(),
                },
                Some(&incomplete),
                100,
            ),
            ReconcileOutcome::default(),
            "an engine completion phase is not verification evidence"
        );
        assert!(!job.ready());

        let verified = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 1,
            ..Default::default()
        };
        assert!(
            reconcile_fact(
                &mut job,
                BackendFact::Ready {
                    job: JobId(10),
                    content_path: content_path.clone(),
                },
                Some(&verified),
                101,
            )
            .durable_changed
        );
        assert!(job.ready());
        assert_eq!(job.status, JobStatus::Downloading);
        let torrent = job.torrent.as_ref().unwrap();
        assert_eq!(torrent.phase, TorrentPhase::Seeding);
        assert_eq!(torrent.ready_at_unix, Some(101));
        assert_eq!(torrent.content_path.as_ref(), Some(&content_path));

        let persisted = serde_json::to_vec(&job).unwrap();
        let restored: Job = serde_json::from_slice(&persisted).unwrap();
        assert!(
            restored.ready(),
            "the next durable snapshot keeps readiness"
        );
        assert_eq!(restored.torrent.unwrap().phase, TorrentPhase::Seeding);
    }

    #[test]
    fn progress_persists_only_coalescible_counters_and_activity() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut job = job(10, hash, JobStatus::Downloading);
        let progress = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 1,
            uploaded_bytes: 7,
            download_bps: 900,
            upload_bps: 800,
            useful_peers: 12,
            last_activity_unix: Some(55),
        };
        assert!(reconcile_progress(&mut job, &progress).durable_changed);
        let torrent = job.torrent.unwrap();
        assert_eq!(torrent.downloaded_bytes, 1);
        assert_eq!(torrent.uploaded_bytes, 7);
        assert_eq!(torrent.last_activity_unix, Some(55));
        // TorrentRecord intentionally has no rate or peer fields: those
        // volatile values cannot force full-list structural publication.
    }

    #[test]
    fn recoverable_engine_conditions_never_translate_to_failed() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let identity = EngineIdentity {
            id: 7,
            info_hash_v1: hash.into(),
        };
        let mut associations = RuntimeAssociations::default();
        associations.associate(JobId(10), identity.clone()).unwrap();

        for (engine, expected) in [
            (EngineStructuralFact::StorageFull, StopReason::StorageFull),
            (
                EngineStructuralFact::MissingContent,
                StopReason::MissingContent,
            ),
            (EngineStructuralFact::Transient, StopReason::Transient),
        ] {
            assert_eq!(
                associations.translate_fact(&identity, engine),
                Some(BackendFact::Stopped {
                    job: JobId(10),
                    reason: expected,
                })
            );
        }

        let error = SafeError::from_redacted("named unrecoverable corruption");
        assert_eq!(
            associations.translate_fact(
                &identity,
                EngineStructuralFact::Unrecoverable {
                    error: error.clone()
                }
            ),
            Some(BackendFact::Failed {
                job: JobId(10),
                error,
            })
        );
    }

    #[test]
    fn storage_full_latches_hold_and_missing_content_remains_recoverable() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut storage = job(10, hash, JobStatus::Downloading);
        let outcome = reconcile_fact(
            &mut storage,
            BackendFact::Stopped {
                job: JobId(10),
                reason: StopReason::StorageFull,
            },
            None,
            100,
        );
        assert!(outcome.storage_hold);
        assert_eq!(storage.status, JobStatus::Paused);
        assert_ne!(storage.torrent.unwrap().phase, TorrentPhase::Failed);

        let mut missing = job(10, hash, JobStatus::Downloading);
        let outcome = reconcile_fact(
            &mut missing,
            BackendFact::Stopped {
                job: JobId(10),
                reason: StopReason::MissingContent,
            },
            None,
            100,
        );
        assert!(!outcome.storage_hold);
        assert_eq!(missing.status, JobStatus::Paused);
        assert_eq!(missing.torrent.unwrap().phase, TorrentPhase::MissingFiles);
    }

    #[test]
    fn disabled_runtime_names_live_row_count() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(
            refuse_disabled_with_live_torrents(&[job(10, hash, JobStatus::Paused)]),
            Err(DisabledWithLiveTorrents { count: 1 })
        );
    }

    #[test]
    fn restore_rejects_parent_escape_from_payload_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        std::fs::create_dir(&root).unwrap();
        let mut escaped = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Paused,
        );
        escaped.torrent.as_mut().unwrap().content_path =
            Some(root.join("category").join("..").join("..").join("outside"));

        let plan = plan_restore(&[escaped], &HashMap::new(), &root, &HashSet::new());

        assert!(plan.requests.is_empty());
        assert_eq!(plan.diagnostics, vec![RestoreDiagnostic::UnsafePayloadRoot]);
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_symlink_escape_from_payload_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, root.join("redirect")).unwrap();
        let mut escaped = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Paused,
        );
        escaped.torrent.as_mut().unwrap().content_path =
            Some(root.join("redirect").join("not-created-yet"));

        let plan = plan_restore(&[escaped], &HashMap::new(), &root, &HashSet::new());

        assert!(plan.requests.is_empty());
        assert_eq!(plan.diagnostics, vec![RestoreDiagnostic::UnsafePayloadRoot]);
    }
}
