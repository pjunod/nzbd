//! NZBGet compatibility shim.
//!
//! Speaks NZBGet's JSON-RPC 1.1 dialect on `/jsonrpc`: no `"jsonrpc":"2.0"`
//! member, positional params, `{"version":"1.1","id":…,"result":…}` envelope.
//! XML-RPC (`/xmlrpc`), JSON-P (`/jsonprpc`), GET-form safe methods, the
//! three auth tiers and the full method table are phase 3 (ARCHITECTURE.md
//! §10.2). Phase 0 answers `version` and a minimal `status` so a client can
//! be pointed at the daemon and get well-shaped answers.
//!
//! Field-shape rules (do not "fix" them — clients parse by name):
//! - 64-bit sizes are split into `…Lo` / `…Hi` / `…MB` triplets.
//! - Deprecated aliases (`FirstID`, `LastID`, …) are preserved.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use nzbd_api::SharedSnapshot;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CompatConfig {
    /// Version string reported to clients (Sonarr gates on >=12 / >=16).
    pub version: String,
}

impl Default for CompatConfig {
    fn default() -> Self {
        CompatConfig { version: "26.2".into() }
    }
}

#[derive(Clone)]
pub struct CompatState {
    pub config: Arc<CompatConfig>,
    pub snapshot: SharedSnapshot,
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

pub fn dispatch(
    state: &CompatState,
    method: &str,
    _params: &Value,
) -> Result<Value, (i64, &'static str)> {
    match method {
        "version" => Ok(Value::String(state.config.version.clone())),
        "status" => {
            let snap = state.snapshot.load();
            let (rlo, rhi, rmb) = split64(snap.remaining_bytes);
            let (dlo, dhi, _) = split64(snap.download_rate_bps);
            Ok(json!({
                "RemainingSizeLo": rlo,
                "RemainingSizeHi": rhi,
                "RemainingSizeMB": rmb,
                "DownloadRate": snap.download_rate_bps as u32, // deprecated 32-bit field
                "DownloadRateLo": dlo,
                "DownloadRateHi": dhi,
                "DownloadPaused": snap.download_paused,
                "Download2Paused": snap.download_paused, // deprecated alias
                "PostPaused": snap.post_paused,
                "ServerStandBy": snap.download_rate_bps == 0,
                "UpTimeSec": 0,
                "ThreadCount": 0,
                "PostJobCount": 0,
                "UrlCount": 0,
                "NewsServers": [],
            }))
        }
        _ => Err((1, "Invalid procedure")),
    }
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

    fn test_state() -> CompatState {
        CompatState {
            config: Arc::new(CompatConfig::default()),
            snapshot: nzbd_api::new_shared_snapshot(),
        }
    }

    #[test]
    fn split64_matches_nzbget_wire_format() {
        let v = (7u64 << 32) | 123; // 30,064,771,195 bytes
        let (lo, hi, mb) = split64(v);
        assert_eq!(lo, 123);
        assert_eq!(hi, 7);
        assert_eq!(mb, v / 1024 / 1024);
        assert_eq!(((hi as u64) << 32) | lo as u64, v);
    }

    #[test]
    fn version_method_and_envelope() {
        let state = test_state();
        let result = dispatch(&state, "version", &Value::Null).unwrap();
        assert_eq!(result, Value::String("26.2".into()));

        let env = envelope(json!(7), Ok(result));
        assert_eq!(env["version"], "1.1"); // JSON-RPC 1.1 dialect, not 2.0
        assert_eq!(env["id"], 7);
        assert_eq!(env["result"], "26.2");
        assert!(env.get("jsonrpc").is_none());
    }

    #[test]
    fn status_has_lo_hi_triplets() {
        let state = test_state();
        state.snapshot.store(Arc::new(nzbd_api::Snapshot {
            remaining_bytes: (1u64 << 32) + 5,
            ..Default::default()
        }));
        let result = dispatch(&state, "status", &Value::Null).unwrap();
        assert_eq!(result["RemainingSizeLo"], 5);
        assert_eq!(result["RemainingSizeHi"], 1);
        assert!(result.get("Download2Paused").is_some(), "deprecated aliases preserved");
    }

    #[test]
    fn unknown_method_is_error_1() {
        let state = test_state();
        assert_eq!(dispatch(&state, "nope", &Value::Null), Err((1, "Invalid procedure")));
    }
}
