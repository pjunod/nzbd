//! Golden structural tests: the exact field sets (names + casing) of every
//! compat response are locked here. A missing or renamed field is a wire
//! break for Sonarr/Radarr-class clients — these tests make that a compile
//! -time-loud event instead of a silent regression.

use nzbd_compat::{dispatch, CompatConfig, CompatState};
use nzbd_engine::{Engine, EngineConfig, Tuning};
use nzbd_state::history::HistoryDb;
use nzbd_state::HistoryEntry;
use nzbd_types::JobId;
use serde_json::{json, Value};
use std::sync::Arc;

const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="4194304" number="1">m1@x</segment></segments>
</file></nzb>"#;

async fn state(tmp: &tempfile::TempDir) -> CompatState {
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    let history = HistoryDb::open(&tmp.path().join("h.sqlite"), None).unwrap();
    let mut st = CompatState::new(CompatConfig::default(), engine);
    st.history = Some(Arc::new(history));
    st
}

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

fn assert_keys(v: &Value, golden: &[&str], what: &str) {
    let mut want: Vec<String> = golden.iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(keys(v), want, "{what}: wire field set changed");
}

#[tokio::test]
async fn golden_status_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp).await;
    let v = dispatch(&st, "status", &Value::Null).await.unwrap();
    assert_keys(
        &v,
        &[
            "RemainingSizeLo",
            "RemainingSizeHi",
            "RemainingSizeMB",
            "DownloadedSizeLo",
            "DownloadedSizeHi",
            "DownloadedSizeMB",
            "DownloadRate",
            "AverageDownloadRate",
            "DownloadLimit",
            "DownloadPaused",
            "Download2Paused",
            "PostPaused",
            "ScanPaused",
            "ServerStandBy",
            "UpTimeSec",
            "DownloadTimeSec",
            "ThreadCount",
            "PostJobCount",
            "UrlCount",
            "QuotaReached",
            "NewsServers",
        ],
        "status",
    );
    st.engine.shutdown().await;
}

#[tokio::test]
async fn golden_listgroups_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp).await;
    st.engine
        .add_nzb("g", NZB.as_bytes(), None, 0)
        .await
        .unwrap();
    let v = dispatch(&st, "listgroups", &Value::Null).await.unwrap();
    assert_keys(
        &v[0],
        &[
            "NZBID",
            "FirstID",
            "LastID",
            "NZBName",
            "NZBNicename",
            "Kind",
            "Status",
            "Category",
            "Priority",
            "FileSizeLo",
            "FileSizeHi",
            "FileSizeMB",
            "RemainingSizeLo",
            "RemainingSizeHi",
            "RemainingSizeMB",
            "DownloadedSizeLo",
            "DownloadedSizeHi",
            "DownloadedSizeMB",
            "PausedSizeLo",
            "PausedSizeHi",
            "PausedSizeMB",
            "FileCount",
            "RemainingFileCount",
            "RemainingParCount",
            "Health",
            "CriticalHealth",
            "DupeKey",
            "DupeScore",
            "DupeMode",
            "Parameters",
            "ScriptStatuses",
            "ServerStats",
            "PostInfoText",
            "PostStageProgress",
            "PostTotalTimeSec",
            "PostStageTimeSec",
        ],
        "listgroups[0]",
    );
    st.engine.shutdown().await;
}

#[tokio::test]
async fn golden_history_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp).await;
    st.history
        .as_ref()
        .unwrap()
        .record(&HistoryEntry {
            job: JobId(1),
            name: "done".into(),
            category: Some("tv".into()),
            final_dir: Some("/dest/done".into()),
            status: "SUCCESS".into(),
            size: 1000,
            health: 1000,
            params: vec![("drone".into(), "x".into())],
            dupe_key: String::new(),
            dupe_score: 0,
            completed_at_unix: 10,
        })
        .unwrap();
    let v = dispatch(&st, "history", &json!([false])).await.unwrap();
    assert_keys(
        &v[0],
        &[
            "NZBID",
            "ID",
            "Kind",
            "Name",
            "NZBName",
            "NZBNicename",
            "RemoteName",
            "Status",
            "TotalStatus",
            "Category",
            "FileSizeLo",
            "FileSizeHi",
            "FileSizeMB",
            "DestDir",
            "FinalDir",
            "HistoryTime",
            "Health",
            "CriticalHealth",
            "ParStatus",
            "UnpackStatus",
            "MoveStatus",
            "ScriptStatus",
            "DeleteStatus",
            "MarkStatus",
            "UrlStatus",
            "Parameters",
            "ScriptStatuses",
            "ServerStats",
            "Deleted",
            "DownloadedSizeLo",
            "DownloadedSizeHi",
            "DownloadedSizeMB",
            "DownloadTimeSec",
            "PostTotalTimeSec",
            "ParTimeSec",
            "RepairTimeSec",
            "UnpackTimeSec",
            "DupeKey",
            "DupeScore",
            "DupeMode",
            "RetryData",
        ],
        "history[0]",
    );
    st.engine.shutdown().await;
}

#[tokio::test]
async fn golden_listfiles_and_log_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let mut st = state(&tmp).await;
    st.log = Some(nzbd_api::LogBuffer::new(16));
    st.log.as_ref().unwrap().push("INFO", "hello".into());
    let id = st
        .engine
        .add_nzb("g", NZB.as_bytes(), None, 0)
        .await
        .unwrap();

    let v = dispatch(&st, "listfiles", &json!([0, 0, id.0]))
        .await
        .unwrap();
    assert_keys(
        &v[0],
        &[
            "ID",
            "NZBID",
            "Filename",
            "Subject",
            "FileSizeLo",
            "FileSizeHi",
            "RemainingSizeLo",
            "RemainingSizeHi",
            "Paused",
            "PostTime",
            "FilenameConfirmed",
            "ActiveDownloads",
            "CompletedArticles",
            "TotalArticles",
        ],
        "listfiles[0]",
    );

    let v = dispatch(&st, "log", &json!([0, 10])).await.unwrap();
    assert_keys(&v[0], &["ID", "Kind", "Time", "Text"], "log[0]");
    st.engine.shutdown().await;
}

/// The full JSON-RPC envelope shape (1.1 dialect, no "jsonrpc" member).
#[tokio::test]
async fn golden_envelope_shape() {
    let env = nzbd_compat::envelope(json!(9), Ok(json!("x")));
    assert_keys(&env, &["version", "id", "result"], "ok envelope");
    let env = nzbd_compat::envelope(json!(9), Err((1, "Invalid procedure")));
    assert_keys(&env, &["version", "id", "error"], "error envelope");
    assert_keys(&env["error"], &["code", "message"], "error body");
}
