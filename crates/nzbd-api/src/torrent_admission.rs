//! Dormant native BitTorrent admission. Production mounting belongs to #163.
use axum::extract::State;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
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
    associations: Arc<tokio::sync::Mutex<HashMap<JobId, nzbd_torrent::EngineIdentity>>>,
    state_dir: PathBuf,
    proxy_enabled: bool,
    dht_enabled: bool,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionResult {
    pub id: JobId,
    pub created: bool,
    pub info_hash: String,
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
                self.associations.lock().await.insert(job.id, identity);
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

    async fn finish(
        &self,
        pending: Option<JobId>,
        bytes: Vec<u8>,
        source: TorrentSource,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        let descriptor = inspect_metainfo(&bytes, self.proxy_enabled, self.dht_enabled)?;
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
                self.associations.lock().await.insert(id, identity.clone());
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
            let result = self.admit_raw(bytes, AddOpts::default()).await?;
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
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
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
