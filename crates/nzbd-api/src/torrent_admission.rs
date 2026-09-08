//! Dormant native BitTorrent admission. Production mounting belongs to #163.
use axum::extract::State;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use nzbd_engine::backend::{
    BackendAdapterPort, BackendCommand, BackendFact, SafeError, StopReason,
};
use nzbd_engine::{AddOpts, EngineHandle};
use nzbd_state::torrent_sources::PendingSourceStore;
use nzbd_torrent::{
    inspect_metainfo, TorrentAddConfig, TorrentRegistry, TorrentSession, TorrentSourceFetchLimits,
};
use nzbd_types::{
    JobId, SeedPolicy, TorrentFileRecord, TorrentPhase, TorrentRecord, TorrentSource,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct TorrentAdmissionService {
    engine: EngineHandle,
    session: TorrentSession,
    registry: Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
    state_dir: PathBuf,
    proxy_enabled: bool,
    dht_enabled: bool,
    #[cfg(test)]
    before_managed_add: Option<Arc<dyn Fn() -> Result<(), AdmissionError> + Send + Sync>>,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("{0}")]
    Torrent(#[from] nzbd_torrent::TorrentError),
    #[error("{0}")]
    Engine(#[from] nzbd_engine::EngineError),
    #[error("{0}")]
    State(#[from] nzbd_state::StateError),
    #[error("invalid torrent source encoding")]
    Encoding,
    #[error("pending torrent admission disappeared")]
    MissingPending,
    #[error("torrent backend adapter has already been taken")]
    MissingBackendAdapter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionResult {
    pub id: JobId,
    pub created: bool,
    pub info_hash: String,
}

#[derive(Clone)]
struct Association {
    identity: nzbd_torrent::EngineIdentity,
    /// `None` means that the engine's initial paused state has not yet been
    /// reconciled with a queue-owner control. Managed admission deliberately
    /// starts paused, while the durable queue starts runnable.
    applied_pause: Option<bool>,
}

impl TorrentAdmissionService {
    pub fn new(
        engine: EngineHandle,
        session: TorrentSession,
        state_dir: PathBuf,
        proxy_enabled: bool,
        dht_enabled: bool,
    ) -> Self {
        Self {
            engine,
            registry: Arc::new(tokio::sync::Mutex::new(TorrentRegistry::new(
                session.clone(),
            ))),
            associations: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            session,
            state_dir,
            proxy_enabled,
            dht_enabled,
            #[cfg(test)]
            before_managed_add: None,
        }
    }

    pub async fn admit_raw(
        &self,
        bytes: Vec<u8>,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        self.finish(None, bytes, TorrentSource::Metainfo, opts)
            .await
    }

    pub async fn admit_source(
        &self,
        source: TorrentSource,
        secret: String,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        match source {
            TorrentSource::Magnet => {
                nzbd_torrent::validate_magnet_source(&secret, self.proxy_enabled)?
            }
            TorrentSource::Url => {
                let url = url::Url::parse(&secret).map_err(|_| AdmissionError::Encoding)?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(AdmissionError::Encoding);
                }
            }
            TorrentSource::Metainfo => return Err(AdmissionError::Encoding),
        }
        let job = self
            .engine
            .reserve_torrent_admission(source, secret.as_bytes().to_vec())
            .await?;
        let bytes = match source {
            TorrentSource::Magnet => self.session.resolve_magnet_metadata(secret).await?,
            TorrentSource::Url => {
                nzbd_torrent::fetch_torrent_source(
                    &secret,
                    TorrentSourceFetchLimits::default(),
                    self.proxy_enabled,
                )
                .await?
            }
            TorrentSource::Metainfo => return Err(AdmissionError::Encoding),
        };
        self.finish(Some(job), bytes, source, opts).await
    }

    /// Reconcile queue-authorized descriptors and pending secret sidecars.
    /// Orphans are removed; linked sources remain until `finish` has made the
    /// descriptor and structural replacement durable.
    pub async fn recover(&self) -> Result<Vec<AdmissionResult>, AdmissionError> {
        let snapshot = nzbd_state::SnapshotStore::open(&self.state_dir)?
            .load()?
            .unwrap_or_default();
        let source_store = PendingSourceStore::open(&self.state_dir)?;
        let linked = snapshot
            .pending_admissions
            .iter()
            .map(|pending| pending.job_id)
            .collect::<std::collections::HashSet<_>>();
        for orphan in source_store
            .inventory()?
            .into_iter()
            .filter(|job| !linked.contains(job))
        {
            source_store.remove(orphan)?;
        }

        let mut restored = Vec::new();
        for job in &snapshot.jobs {
            let Some(torrent) = &job.torrent else {
                continue;
            };
            let bytes =
                std::fs::read(self.state_dir.join(&torrent.metadata_file)).map_err(|error| {
                    nzbd_state::StateError::Io {
                        op: "read torrent descriptor",
                        path: self.state_dir.join(&torrent.metadata_file),
                        source: error,
                    }
                })?;
            let identities = self
                .registry
                .lock()
                .await
                .restore_selected([nzbd_torrent::RestoreDescriptor {
                    metainfo: bytes,
                    expected_info_hash_v1: torrent.info_hash_v1.clone(),
                    preferred_id: None,
                    selected_files: Some(
                        torrent
                            .files
                            .iter()
                            .enumerate()
                            .filter_map(|(index, file)| file.selected.then_some(index))
                            .collect(),
                    ),
                }])
                .await?;
            if let Some(identity) = identities.into_iter().next() {
                self.associations.lock().await.insert(
                    job.id,
                    Association {
                        identity,
                        applied_pause: None,
                    },
                );
            }
        }

        for pending in snapshot.pending_admissions {
            let source_bytes = source_store.read(pending.job_id)?;
            let bytes = match pending.source {
                TorrentSource::Magnet => {
                    self.session
                        .resolve_magnet_metadata(
                            String::from_utf8(source_bytes)
                                .map_err(|_| AdmissionError::Encoding)?,
                        )
                        .await?
                }
                TorrentSource::Url => {
                    nzbd_torrent::fetch_torrent_source(
                        &String::from_utf8(source_bytes).map_err(|_| AdmissionError::Encoding)?,
                        TorrentSourceFetchLimits::default(),
                        self.proxy_enabled,
                    )
                    .await?
                }
                TorrentSource::Metainfo => source_bytes,
            };
            restored.push(
                self.finish(
                    Some(pending.job_id),
                    bytes,
                    pending.source,
                    AddOpts::default(),
                )
                .await?,
            );
        }
        Ok(restored)
    }

    /// Start the single consumer of queue-owner backend controls. The daemon
    /// still decides when this feature-gated service is mounted; once it is,
    /// controls remain ordered on the existing backend FIFO.
    pub fn spawn_backend_executor(&self) -> Result<tokio::task::JoinHandle<()>, AdmissionError> {
        let adapter = self
            .engine
            .take_backend_adapter()
            .ok_or(AdmissionError::MissingBackendAdapter)?;
        let registry = self.registry.clone();
        let associations = self.associations.clone();
        Ok(tokio::spawn(async move {
            run_backend_executor(adapter, registry, associations).await;
        }))
    }

    async fn finish(
        &self,
        pending: Option<JobId>,
        bytes: Vec<u8>,
        source: TorrentSource,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        // Parser diagnostics come from the embedded engine and are not an API
        // contract (and may echo hostile bytes). At this boundary they are a
        // generic client-input failure; named policy errors stay named.
        let descriptor =
            inspect_metainfo(&bytes, self.proxy_enabled, self.dht_enabled).map_err(|error| {
                match error {
                    nzbd_torrent::TorrentError::Engine(_) => AdmissionError::Encoding,
                    other => AdmissionError::Torrent(other),
                }
            })?;
        let job = match pending {
            Some(job) => job,
            None => {
                self.engine
                    .reserve_torrent_admission(TorrentSource::Metainfo, bytes.clone())
                    .await?
            }
        };
        let relative = PathBuf::from(format!(
            "torrents/sources/{}.torrent",
            descriptor.info_hash_v1
        ));
        persist_descriptor(&self.state_dir.join(&relative), &bytes)?;
        let record = TorrentRecord {
            info_hash_v1: descriptor.info_hash_v1.clone(),
            source,
            metadata_file: relative,
            phase: TorrentPhase::Queued,
            control_intent: nzbd_types::TorrentControlIntent::Running,
            files: descriptor
                .files
                .iter()
                .map(|(path, length)| TorrentFileRecord {
                    path: path.clone(),
                    length: *length,
                    selected: true,
                })
                .collect(),
            total_bytes: descriptor.total_bytes,
            selected_bytes: descriptor.total_bytes,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: SeedPolicy::default(),
            last_activity_unix: None,
            last_error: None,
        };
        let committed = self
            .engine
            .commit_torrent_admission(job, descriptor.name, opts, record)
            .await?
            .ok_or(AdmissionError::MissingPending)?;
        match committed {
            Err(existing) => Ok(AdmissionResult {
                id: existing,
                created: false,
                info_hash: descriptor.info_hash_v1,
            }),
            Ok(id) => {
                #[cfg(test)]
                if let Some(hook) = &self.before_managed_add {
                    hook()?;
                }
                let identity = self
                    .registry
                    .lock()
                    .await
                    .add_committed(
                        bytes,
                        TorrentAddConfig {
                            paused: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                self.associations.lock().await.insert(
                    id,
                    Association {
                        identity: identity.clone(),
                        applied_pause: None,
                    },
                );
                Ok(AdmissionResult {
                    id,
                    created: true,
                    info_hash: identity.info_hash_v1,
                })
            }
        }
    }

    pub async fn scan_watch_once(
        &self,
        dir: &Path,
    ) -> Result<Vec<AdmissionResult>, AdmissionError> {
        let mut entries = std::fs::read_dir(dir)
            .map_err(|e| nzbd_state::StateError::Io {
                op: "read torrent watch directory",
                path: dir.to_path_buf(),
                source: e,
            })?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        let mut results = Vec::new();
        for entry in entries {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("torrent") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|e| nzbd_state::StateError::Io {
                op: "read torrent watch source",
                path: path.clone(),
                source: e,
            })?;
            let result = match self.admit_raw(bytes, AddOpts::default()).await {
                Ok(result) => result,
                Err(error) if error.is_input_error() => {
                    std::fs::rename(&path, path.with_extension("torrent.rejected")).map_err(
                        |e| nzbd_state::StateError::Io {
                            op: "rename rejected torrent watch source",
                            path: path.clone(),
                            source: e,
                        },
                    )?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let suffix = if result.created {
                "processed"
            } else {
                "duplicate"
            };
            std::fs::rename(&path, path.with_extension(format!("torrent.{suffix}"))).map_err(
                |e| nzbd_state::StateError::Io {
                    op: "rename torrent watch source",
                    path: path.clone(),
                    source: e,
                },
            )?;
            results.push(result);
        }
        Ok(results)
    }
}

async fn run_backend_executor(
    mut adapter: BackendAdapterPort,
    registry: Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
) {
    while let Some(command) = adapter.next_command().await {
        let fact = match command {
            BackendCommand::Pause { job } => {
                apply_pause_resume(job, true, &registry, &associations).await
            }
            BackendCommand::Resume { job } => {
                apply_pause_resume(job, false, &registry, &associations).await
            }
            // M2e-3 owns removal and the remaining commands have their own
            // executor slices. The service is feature-gated and unmounted
            // until those ordered children complete.
            BackendCommand::Start { .. }
            | BackendCommand::Remove { .. }
            | BackendCommand::SetPriority { .. }
            | BackendCommand::SetDownloadLimit { .. } => None,
        };
        if let Some(fact) = fact {
            if adapter.structural(fact).await.is_err() {
                break;
            }
        }
    }
}

async fn apply_pause_resume(
    job: JobId,
    pause: bool,
    registry: &Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: &Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
) -> Option<BackendFact> {
    let Some(association) = associations.lock().await.get(&job).cloned() else {
        return Some(control_failure(job));
    };
    if association.applied_pause == Some(pause) {
        return None;
    }
    let result = {
        let registry = registry.lock().await;
        if pause {
            registry.pause(&association.identity).await
        } else {
            registry.resume(&association.identity).await
        }
    };
    match result {
        Ok(()) => {
            if let Some(association) = associations.lock().await.get_mut(&job) {
                association.applied_pause = Some(pause);
            }
            if pause {
                Some(BackendFact::Stopped {
                    job,
                    reason: StopReason::Paused,
                })
            } else {
                Some(BackendFact::Resumed { job })
            }
        }
        // Never send an engine diagnostic across the owner boundary: it may
        // contain a passkey, query, peer address, or untrusted path.
        Err(_) => Some(control_failure(job)),
    }
}

fn control_failure(job: JobId) -> BackendFact {
    BackendFact::Failed {
        job,
        error: SafeError::from_redacted("torrent control target is unavailable"),
    }
}

#[derive(Deserialize)]
struct TypedRequest {
    source: TypedSource,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    params: std::collections::BTreeMap<String, String>,
}
#[derive(Deserialize)]
struct TypedSource {
    #[serde(rename = "type")]
    kind: String,
    uri: String,
}

pub fn router(service: TorrentAdmissionService) -> Router {
    Router::new()
        .route("/api/v1/jobs", axum::routing::post(post_job))
        .with_state(service)
}

async fn post_job(
    State(service): State<TorrentAdmissionService>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .unwrap_or("");
    let result = match content_type {
        "application/x-bittorrent" => service.admit_raw(body.to_vec(), AddOpts::default()).await,
        "application/json" => match serde_json::from_slice::<TypedRequest>(&body) {
            Ok(request) => {
                let source = match request.source.kind.as_str() {
                    "magnet" => TorrentSource::Magnet,
                    "torrent_url" => TorrentSource::Url,
                    _ => {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json(json!({"error":"unsupported torrent source type"})),
                        )
                            .into_response()
                    }
                };
                service
                    .admit_source(
                        source,
                        request.source.uri,
                        AddOpts {
                            category: request.category,
                            priority: request.priority,
                            paused: request.paused,
                            params: request.params.into_iter().collect(),
                            ..Default::default()
                        },
                    )
                    .await
            }
            Err(_) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"error":"invalid typed torrent request"})),
                )
                    .into_response()
            }
        },
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error":"unsupported torrent content type"})),
            )
                .into_response()
        }
    };
    match result {
        Ok(result) if result.created => (
            StatusCode::CREATED,
            Json(json!({"id":result.id,"info_hash":result.info_hash})),
        )
            .into_response(),
        Ok(result) => (
            StatusCode::OK,
            Json(json!({"id":result.id,"info_hash":result.info_hash,"created":false})),
        )
            .into_response(),
        Err(error) if error.is_input_error() => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"torrent admission failed"})),
        )
            .into_response(),
    }
}

impl AdmissionError {
    fn is_input_error(&self) -> bool {
        matches!(self, Self::Encoding)
            || matches!(self, Self::Torrent(error) if !matches!(error, nzbd_torrent::TorrentError::Engine(_)))
    }
}

fn persist_descriptor(path: &Path, bytes: &[u8]) -> Result<(), nzbd_state::StateError> {
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).map_err(|e| nzbd_state::StateError::Io {
        op: "create torrent descriptor directory",
        path: parent.to_path_buf(),
        source: e,
    })?;
    let tmp = path.with_extension("torrent.tmp");
    let mut file = std::fs::File::create(&tmp).map_err(|e| nzbd_state::StateError::Io {
        op: "create torrent descriptor",
        path: tmp.clone(),
        source: e,
    })?;
    use std::io::Write;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| nzbd_state::StateError::Io {
            op: "persist torrent descriptor",
            path: tmp.clone(),
            source: e,
        })?;
    std::fs::rename(&tmp, path).map_err(|e| nzbd_state::StateError::Io {
        op: "rename torrent descriptor",
        path: path.to_path_buf(),
        source: e,
    })?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| nzbd_state::StateError::Io {
            op: "fsync torrent descriptor directory",
            path: parent.to_path_buf(),
            source: e,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use nzbd_engine::backend::backend_channel;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_torrent::TorrentSessionConfig;
    use tower::ServiceExt;

    fn metainfo(name: &[u8]) -> Vec<u8> {
        fn bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(value.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(value);
        }
        let mut torrent = b"d4:infod6:lengthi1e4:name".to_vec();
        bytes(&mut torrent, name);
        torrent.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
        torrent.extend_from_slice(&[0; 20]);
        torrent.extend_from_slice(b"ee");
        torrent
    }

    async fn service(tmp: &tempfile::TempDir) -> (TorrentAdmissionService, EngineHandle) {
        let state = tmp.path().join("state");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state.clone(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let session =
            TorrentSession::start(tmp.path().join("payload"), TorrentSessionConfig::default())
                .await
                .unwrap();
        (
            TorrentAdmissionService::new(engine.clone(), session, state, false, false),
            engine,
        )
    }

    async fn next_fact(owner: &mut nzbd_engine::backend::BackendOwnerPort) -> BackendFact {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(fact) = owner.try_structural() {
                    return fact;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backend executor did not emit a structural fact")
    }

    #[tokio::test]
    async fn backend_executor_pauses_and_resumes_once_per_state_change() {
        let tmp = tempfile::tempdir().unwrap();
        let session =
            TorrentSession::start(tmp.path().join("payload"), TorrentSessionConfig::default())
                .await
                .unwrap();
        let mut registry = TorrentRegistry::new(session.clone());
        let identity = registry
            .add_committed(
                metainfo(b"executor.bin"),
                TorrentAddConfig {
                    paused: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let registry = Arc::new(tokio::sync::Mutex::new(registry));
        let associations = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
            JobId(7),
            Association {
                identity: identity.clone(),
                applied_pause: None,
            },
        )])));
        let (mut owner, adapter) = backend_channel(8, 8);
        let executor = tokio::spawn(run_backend_executor(
            adapter,
            registry.clone(),
            associations,
        ));

        owner
            .try_command(BackendCommand::Pause { job: JobId(7) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Stopped {
                job: JobId(7),
                reason: StopReason::Paused,
            }
        );
        assert_eq!(registry.lock().await.is_paused(&identity), Some(true));

        owner
            .try_command(BackendCommand::Pause { job: JobId(7) })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), next_fact(&mut owner))
                .await
                .is_err()
        );

        owner
            .try_command(BackendCommand::Resume { job: JobId(7) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Resumed { job: JobId(7) }
        );
        assert_eq!(registry.lock().await.is_paused(&identity), Some(false));

        owner
            .try_command(BackendCommand::Resume { job: JobId(7) })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), next_fact(&mut owner))
                .await
                .is_err()
        );

        owner
            .try_command(BackendCommand::Pause { job: JobId(8) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Failed {
                job: JobId(8),
                error: SafeError::from_redacted("torrent control target is unavailable"),
            }
        );

        drop(owner);
        executor.await.unwrap();
        session.stop().await;
    }

    #[tokio::test]
    async fn raw_route_commits_descriptor_before_managed_add_and_deduplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let body = metainfo(b"payload.bin");

        let response = router(service.clone())
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let first: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let torrent = persisted.jobs[0].torrent.as_ref().unwrap();
        assert!(tmp
            .path()
            .join("state")
            .join(&torrent.metadata_file)
            .exists());
        assert!(persisted.pending_admissions.is_empty());

        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(duplicate["id"], first["id"]);
        assert_eq!(duplicate["created"], false);
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn managed_add_failure_observes_descriptor_and_queue_already_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut service, engine) = service(&tmp).await;
        let state = tmp.path().join("state");
        let observed_state = state.clone();
        service.before_managed_add = Some(Arc::new(move || {
            let persisted = nzbd_state::SnapshotStore::open(&observed_state)?
                .load()?
                .expect("queue commit must precede managed add");
            let torrent = persisted.jobs[0]
                .torrent
                .as_ref()
                .expect("torrent row must precede managed add");
            assert!(observed_state.join(&torrent.metadata_file).exists());
            Err(AdmissionError::MissingPending)
        }));

        let error = service
            .admit_raw(metainfo(b"ordered.bin"), AddOpts::default())
            .await
            .unwrap_err();
        assert!(matches!(error, AdmissionError::MissingPending));
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_input_is_422_without_a_live_or_pending_job() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from("not bencode"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(engine.snapshot().jobs.is_empty());
        assert!(nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap_or_default()
            .pending_admissions
            .is_empty());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn typed_http_source_uses_the_same_durable_admission_path() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tmp = tempfile::tempdir().unwrap();
        let body = metainfo(b"fetched.bin");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
        });
        let (service, engine) = service(&tmp).await;
        let request = serde_json::json!({
            "source": {"type": "torrent_url", "uri": format!("http://{address}/source?passkey=secret")},
            "category": "test"
        });
        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        server.await.unwrap();
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(persisted.jobs.len(), 1);
        assert_eq!(persisted.jobs[0].category.as_deref(), Some("test"));
        assert!(persisted.pending_admissions.is_empty());
        let serialized = serde_json::to_string(&persisted).unwrap();
        assert!(!serialized.contains("passkey"));
        assert!(
            nzbd_state::torrent_sources::PendingSourceStore::open(&tmp.path().join("state"))
                .unwrap()
                .inventory()
                .unwrap()
                .is_empty()
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn recover_resumes_a_durable_http_intent_and_reaps_orphans() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tmp = tempfile::tempdir().unwrap();
        let body = metainfo(b"recovered.bin");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
        });
        let (service, engine) = service(&tmp).await;
        let state = tmp.path().join("state");
        let secret = format!("http://{address}/source?passkey=restart-secret");
        let job = engine
            .reserve_torrent_admission(TorrentSource::Url, secret.into_bytes())
            .await
            .unwrap();
        let store = PendingSourceStore::open(&state).unwrap();
        store.write(JobId(999), b"orphan-secret").unwrap();

        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state.join("queue.json")).unwrap()).unwrap();
        assert_eq!(raw["schema_version"], 4);
        assert_eq!(raw["pending_admissions"][0]["job_id"], job.0);
        assert_eq!(raw["pending_admissions"][0]["source"], "url");
        assert_eq!(
            raw["pending_admissions"][0]["secret_ref"],
            format!("torrents/pending/{}.source", job.0)
        );
        assert!(!raw.to_string().contains("restart-secret"));

        let recovered = service.recover().await.unwrap();
        server.await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, job);
        assert!(store.inventory().unwrap().is_empty());
        let persisted = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(persisted.pending_admissions.is_empty());
        assert_eq!(persisted.jobs[0].id, job);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn watch_rejects_a_bad_entry_and_continues_to_the_next() {
        let tmp = tempfile::tempdir().unwrap();
        let watch = tmp.path().join("watch");
        std::fs::create_dir(&watch).unwrap();
        std::fs::write(watch.join("a.torrent"), b"bad").unwrap();
        std::fs::write(watch.join("b.torrent"), metainfo(b"watch.bin")).unwrap();
        let (service, engine) = service(&tmp).await;

        let results = service.scan_watch_once(&watch).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(watch.join("a.torrent.rejected").exists());
        assert!(watch.join("b.torrent.processed").exists());
        assert!(service.scan_watch_once(&watch).await.unwrap().is_empty());
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn watch_leaves_valid_input_in_place_after_an_internal_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let watch = tmp.path().join("watch");
        std::fs::create_dir(&watch).unwrap();
        let source = watch.join("retry.torrent");
        std::fs::write(&source, metainfo(b"retry.bin")).unwrap();
        let (service, engine) = service(&tmp).await;
        engine.shutdown().await;

        let error = service.scan_watch_once(&watch).await.unwrap_err();
        assert!(matches!(error, AdmissionError::Engine(_)));
        assert!(source.exists());
        assert!(!watch.join("retry.torrent.rejected").exists());
    }

    #[test]
    fn internal_errors_are_opaque_server_failures() {
        assert!(!AdmissionError::MissingPending.is_input_error());
        assert!(!AdmissionError::Torrent(nzbd_torrent::TorrentError::Engine(
            "magnet:?xt=secret".into()
        ))
        .is_input_error());
        assert!(AdmissionError::Encoding.is_input_error());
    }
}
