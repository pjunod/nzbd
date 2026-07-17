//! NZBGet compatibility shim.
//!
//! Speaks NZBGet's JSON-RPC 1.1 dialect on `/jsonrpc`: no `"jsonrpc":"2.0"`
//! member, positional params, `{"version":"1.1","id":…,"result":…}` envelope.
//! XML-RPC (`/xmlrpc`), JSON-P (`/jsonprpc`), GET-form safe methods, the
//! three auth tiers and the full C1 method table (`append`, `listgroups`,
//! `history`, `config`, `editqueue`) are phase 3 (ARCHITECTURE.md §10.2).
//! Phase 1 answers `version`, `status` and a minimal `listgroups` with live
//! engine data so a client pointed at the daemon gets well-shaped answers.
//!
//! Field-shape rules (do not "fix" them — clients parse by name):
//! - 64-bit sizes are split into `…Lo` / `…Hi` / `…MB` triplets.
//! - Deprecated aliases (`FirstID`, `LastID`, …) are preserved.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use nzbd_engine::{EngineHandle, JobSummary};
use nzbd_types::JobStatus;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CompatConfig {
    /// Version string reported to clients (Sonarr gates on >=12 / >=16).
    pub version: String,
}

impl Default for CompatConfig {
    fn default() -> Self {
        CompatConfig {
            version: "26.2".into(),
        }
    }
}

#[derive(Clone)]
pub struct CompatState {
    pub config: Arc<CompatConfig>,
    pub engine: EngineHandle,
}

/// Split a u64 into NZBGet's `(Lo, Hi, MB)` wire triplet.
pub fn split64(v: u64) -> (u32, u32, u64) {
    (v as u32, (v >> 32) as u32, v / 1024 / 1024)
}

/// Build the JSON-RPC 1.1-flavored response envelope NZBGet clients expect.
pub fn envelope(id: Value, result: Result<Value, (i64, &str)>) -> Value {
    match result {
        Ok(result) => json!({ "version": "1.1", "id": id, "result": result }),
        Err((code, msg)) => json!({
            "version": "1.1",
            "id": id,
            "error": { "code": code, "message": msg }
        }),
    }
}

/// NZBGet queue status string for a job (the full vocabulary lands with
/// the phase-3 shim: PP_QUEUED, LOADING_PARS, …).
fn group_status(j: &JobSummary) -> &'static str {
    match j.status {
        JobStatus::Queued => "QUEUED",
        JobStatus::Downloading => "DOWNLOADING",
        JobStatus::Paused => "PAUSED",
        JobStatus::Fetching => "FETCHING",
        JobStatus::PostQueued | JobStatus::Post { .. } => "PP_QUEUED",
        JobStatus::Completed => "SUCCESS",
        JobStatus::Failed => "FAILURE",
        JobStatus::Deleted => "DELETED",
    }
}

pub fn dispatch(
    state: &CompatState,
    method: &str,
    _params: &Value,
) -> Result<Value, (i64, &'static str)> {
    match method {
        "version" => Ok(Value::String(state.config.version.clone())),
        "status" => {
            let snap = state.engine.snapshot();
            let (rlo, rhi, rmb) = split64(snap.remaining_bytes);
            let (dlo, dhi, dmb) = split64(snap.session_downloaded_bytes);
            let uptime = (nowish() - snap.up_since_unix).max(0);
            Ok(json!({
                "RemainingSizeLo": rlo,
                "RemainingSizeHi": rhi,
                "RemainingSizeMB": rmb,
                "DownloadedSizeLo": dlo,
                "DownloadedSizeHi": dhi,
                "DownloadedSizeMB": dmb,
                "DownloadRate": snap.download_rate_bps.min(u32::MAX as u64) as u32,
                "AverageDownloadRate": 0,
                "DownloadLimit": snap.speed_limit_bps.unwrap_or(0).min(u32::MAX as u64) as u32,
                "DownloadPaused": snap.download_paused,
                "Download2Paused": snap.download_paused, // deprecated alias
                "PostPaused": false,
                "ScanPaused": false,
                "ServerStandBy": snap.download_rate_bps == 0,
                "UpTimeSec": uptime,
                "DownloadTimeSec": uptime,
                "ThreadCount": 1,
                "PostJobCount": 0,
                "UrlCount": 0,
                "QuotaReached": false,
                "NewsServers": [],
            }))
        }
        "listgroups" => {
            let snap = state.engine.snapshot();
            let groups: Vec<Value> = snap
                .jobs
                .iter()
                .filter(|j| {
                    !matches!(
                        j.status,
                        JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                    )
                })
                .map(|j| {
                    let (flo, fhi, fmb) = split64(j.size_bytes);
                    let remaining = j.remaining_bytes;
                    let (rlo, rhi, rmb) = split64(remaining);
                    let (dlo, dhi, dmb) = split64(j.downloaded_bytes);
                    json!({
                        "NZBID": j.id,
                        "FirstID": j.id, // deprecated alias
                        "LastID": j.id,  // deprecated alias
                        "NZBName": j.name,
                        "NZBNicename": j.name, // deprecated alias
                        "Kind": "NZB",
                        "Status": group_status(j),
                        "Category": j.category.clone().unwrap_or_default(),
                        "Priority": j.priority,
                        "FileSizeLo": flo,
                        "FileSizeHi": fhi,
                        "FileSizeMB": fmb,
                        "RemainingSizeLo": rlo,
                        "RemainingSizeHi": rhi,
                        "RemainingSizeMB": rmb,
                        "DownloadedSizeLo": dlo,
                        "DownloadedSizeHi": dhi,
                        "DownloadedSizeMB": dmb,
                        "PausedSizeLo": 0, "PausedSizeHi": 0, "PausedSizeMB": 0,
                        "FileCount": j.files_total,
                        "RemainingFileCount": j.files_total - j.files_done,
                        "RemainingParCount": 0,
                        "Health": j.health,
                        "CriticalHealth": j.critical_health,
                        "DupeKey": "",
                        "DupeScore": 0,
                        "DupeMode": "SCORE",
                        "Parameters": [],
                        "ScriptStatuses": [],
                        "ServerStats": [],
                        "PostInfoText": "NONE",
                        "PostStageProgress": 0,
                        "PostTotalTimeSec": 0,
                        "PostStageTimeSec": 0,
                    })
                })
                .collect();
            Ok(Value::Array(groups))
        }
        _ => Err((1, "Invalid procedure")),
    }
}

fn nowish() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn jsonrpc(State(state): State<CompatState>, body: String) -> Json<Value> {
    let req: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return Json(envelope(Value::Null, Err((4, "Parse error")))),
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(Value::as_str).unwrap_or_default();
    let params = req.get("params").cloned().unwrap_or(Value::Array(vec![]));
    Json(envelope(id, dispatch(&state, method, &params)))
}

pub fn router(state: CompatState) -> Router {
    Router::new()
        .route("/jsonrpc", post(jsonrpc))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_engine::{Engine, EngineConfig, Tuning};

    async fn test_state(tmp: &tempfile::TempDir) -> CompatState {
        let engine = Engine::spawn(EngineConfig {
            servers: vec![],
            state_dir: tmp.path().join("state"),
            dest_dir: tmp.path().join("dest"),
            tuning: Tuning::default(),
            speed_limit_bps: None,
        })
        .await
        .unwrap();
        CompatState {
            config: Arc::new(CompatConfig::default()),
            engine,
        }
    }

    const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="4194304" number="1">m1@x</segment></segments>
</file></nzb>"#;

    #[test]
    fn split64_matches_nzbget_wire_format() {
        let v = (7u64 << 32) | 123;
        let (lo, hi, mb) = split64(v);
        assert_eq!(lo, 123);
        assert_eq!(hi, 7);
        assert_eq!(mb, v / 1024 / 1024);
        assert_eq!(((hi as u64) << 32) | lo as u64, v);
    }

    #[tokio::test]
    async fn version_envelope_and_status_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;

        let result = dispatch(&state, "version", &Value::Null).unwrap();
        assert_eq!(result, Value::String("26.2".into()));
        let env = envelope(json!(7), Ok(result));
        assert_eq!(env["version"], "1.1"); // JSON-RPC 1.1 dialect, not 2.0
        assert_eq!(env["id"], 7);
        assert_eq!(env["result"], "26.2");
        assert!(env.get("jsonrpc").is_none());

        let status = dispatch(&state, "status", &Value::Null).unwrap();
        for key in [
            "RemainingSizeLo",
            "RemainingSizeHi",
            "RemainingSizeMB",
            "DownloadRate",
            "DownloadPaused",
            "Download2Paused",
            "ServerStandBy",
            "UpTimeSec",
        ] {
            assert!(status.get(key).is_some(), "missing {key}");
        }
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn listgroups_reflects_live_queue_with_triplets() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let id = state
            .engine
            .add_nzb("My Show S01E01", NZB.as_bytes(), Some("tv".into()), 0)
            .await
            .unwrap();

        let groups = dispatch(&state, "listgroups", &Value::Null).unwrap();
        let g = &groups[0];
        assert_eq!(g["NZBID"], id.0);
        assert_eq!(g["FirstID"], id.0, "deprecated alias preserved");
        assert_eq!(g["NZBName"], "My Show S01E01");
        assert_eq!(g["Status"], "QUEUED");
        assert_eq!(g["Category"], "tv");
        assert_eq!(g["FileSizeMB"], 4);
        assert_eq!(g["FileSizeLo"], 4_194_304u32);
        assert_eq!(g["Health"], 1000);

        // status remaining reflects the queued article bytes
        let status = dispatch(&state, "status", &Value::Null).unwrap();
        assert_eq!(status["RemainingSizeLo"], 4_194_304u32);
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_method_is_error_1() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        assert_eq!(
            dispatch(&state, "nope", &Value::Null),
            Err((1, "Invalid procedure"))
        );
        state.engine.shutdown().await;
    }
}
