//! RSS/Atom feed engine (the NZBGet `Feed1.*` subsystem): per-feed pollers
//! fetch indexer feeds, run the filter language over new items, and queue
//! accepted ones as URL jobs. Seen-item state persists as JSON (put it on
//! the shared volume in cluster mode so failover never re-downloads a
//! feed's history); polling is gated on an authority check the same way
//! the watch-dir scanner is.

pub mod filter;
pub mod parse;

use filter::Filter;
use nzbd_engine::{AddOpts, EngineHandle};
use parse::FeedItem;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// One configured feed.
#[derive(Debug, Clone)]
pub struct FeedDef {
    /// 1-based id (compat `viewfeed`/`fetchfeeds` address feeds by id).
    pub id: u32,
    pub name: String,
    pub url: String,
    pub interval: Duration,
    /// Filter script (see [`filter`]); empty = accept everything.
    pub filter: String,
    /// Defaults applied unless an Accept rule overrides them.
    pub category: Option<String>,
    pub priority: i32,
    pub pause: bool,
}

/// Seen-item ledger, persisted as `feeds.json`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct SeenDoc {
    /// feed name → guid → first-seen unix.
    feeds: HashMap<String, HashMap<String, i64>>,
}

const SEEN_RETENTION_SECS: i64 = 90 * 86_400;

/// A fetched item + its filter verdict (served by compat `viewfeed`).
#[derive(Debug, Clone)]
pub struct PreviewItem {
    pub item: FeedItem,
    pub accepted: bool,
    pub new: bool,
}

/// Shared handle: on-demand fetch + last-poll cache for `viewfeed`.
#[derive(Clone)]
pub struct FeedsHandle {
    pub feeds: Arc<Vec<FeedDef>>,
    notify: Arc<tokio::sync::Notify>,
    cache: Arc<Mutex<HashMap<u32, Vec<PreviewItem>>>>,
}

impl FeedsHandle {
    /// Nudge every poller to fetch now (compat `fetchfeeds`).
    pub fn fetch_now(&self) {
        self.notify.notify_waiters();
    }

    /// The last poll's items for a feed (compat `viewfeed`).
    pub fn preview(&self, feed_id: u32) -> Vec<PreviewItem> {
        self.cache
            .lock()
            .unwrap()
            .get(&feed_id)
            .cloned()
            .unwrap_or_default()
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn load_seen(path: &std::path::Path) -> SeenDoc {
    std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn save_seen(path: &std::path::Path, doc: &SeenDoc) {
    if let Ok(bytes) = serde_json::to_vec(doc) {
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, &bytes).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }
}

/// Spawn one poller task per feed. `state_dir` holds `feeds.json`;
/// `is_authority` gates polling (always-true single node, leader-only in
/// cluster mode).
pub fn spawn_feeds(
    engine: EngineHandle,
    feeds: Vec<FeedDef>,
    state_dir: PathBuf,
    is_authority: Arc<dyn Fn() -> bool + Send + Sync>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) -> FeedsHandle {
    let handle = FeedsHandle {
        feeds: Arc::new(feeds),
        notify: Arc::new(tokio::sync::Notify::new()),
        cache: Arc::new(Mutex::new(HashMap::new())),
    };
    let seen_path = state_dir.join("feeds.json");
    let _ = std::fs::create_dir_all(&state_dir);

    for feed in handle.feeds.iter().cloned() {
        let engine = engine.clone();
        let notify = handle.notify.clone();
        let cache = handle.cache.clone();
        let seen_path = seen_path.clone();
        let is_authority = is_authority.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(feed.interval) => {}
                    _ = notify.notified() => {}
                }
                if cancel.is_cancelled() {
                    break;
                }
                if !is_authority() {
                    continue;
                }
                poll_feed(&engine, &feed, &seen_path, &cache).await;
            }
        });
    }
    handle
}

/// One poll: fetch → parse → filter → queue new accepted items.
pub async fn poll_feed(
    engine: &EngineHandle,
    feed: &FeedDef,
    seen_path: &std::path::Path,
    cache: &Arc<Mutex<HashMap<u32, Vec<PreviewItem>>>>,
) {
    let body = match nzbd_engine::fetch::http_get(&feed.url).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(feed = %feed.name, error = %e, "feed fetch failed");
            return;
        }
    };
    let xml = String::from_utf8_lossy(&body);
    let now = now_unix();
    let items = parse::parse_feed(&xml, now);
    let flt = Filter::parse(&feed.filter);

    // Seen-ledger read-modify-write is confined to this call; pollers are
    // per-feed but share the file, so re-read every poll.
    let mut doc = load_seen(seen_path);
    let seen = doc.feeds.entry(feed.name.clone()).or_default();
    seen.retain(|_, first| now - *first < SEEN_RETENTION_SECS);

    let mut previews = Vec::with_capacity(items.len());
    let mut queued = 0u32;
    for item in items {
        let verdict = flt.evaluate(&item);
        let is_new = !seen.contains_key(&item.guid);
        if let (Some(opts), true) = (&verdict, is_new) {
            let add = AddOpts {
                category: opts.category.clone().or_else(|| feed.category.clone()),
                priority: opts.priority.unwrap_or(feed.priority),
                paused: opts.pause.unwrap_or(feed.pause),
                dupe: opts.dupekey.as_ref().map(|k| nzbd_types::DupeInfo {
                    key: k.clone(),
                    score: opts.dupescore.unwrap_or(0),
                    mode: Some(nzbd_types::DupeMode::Score),
                }),
            };
            match engine.add_url(&item.title, &item.url, add).await {
                Ok(id) => {
                    tracing::info!(feed = %feed.name, job = id.0, title = %item.title, "feed item queued");
                    queued += 1;
                }
                Err(e) => {
                    tracing::warn!(feed = %feed.name, title = %item.title, error = %e, "feed add failed")
                }
            }
        }
        if is_new {
            seen.insert(item.guid.clone(), now);
        }
        previews.push(PreviewItem {
            accepted: verdict.is_some(),
            new: is_new,
            item,
        });
    }
    save_seen(seen_path, &doc);
    cache.lock().unwrap().insert(feed.id, previews);
    if queued > 0 {
        tracing::info!(feed = %feed.name, queued, "feed poll queued items");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use std::io::{Read as _, Write as _};

    /// Serve fixed HTTP responses; returns (port, hit counter).
    fn tiny_http(responses: Vec<(String, String)>) -> (u16, Arc<Mutex<u32>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(Mutex::new(0u32));
        let h2 = hits.clone();
        std::thread::spawn(move || {
            for (mut s, _) in listener.incoming().flatten().map(|s| (s, ())) {
                let mut buf = [0u8; 4096];
                let n = s.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
                *h2.lock().unwrap() += 1;
                let body = responses
                    .iter()
                    .find(|(p, _)| *p == path)
                    .map(|(_, b)| b.clone())
                    .unwrap_or_default();
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            }
        });
        (port, hits)
    }

    const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="1000" number="1">m1@x</segment></segments>
</file></nzb>"#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poll_queues_new_accepted_items_once() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();

        // The NZB lives on the same tiny server the feed points at.
        let (port, _) = tiny_http(vec![("/get/1.nzb".into(), NZB.into())]);
        let feed_xml = format!(
            r#"<rss><channel>
              <item><title>Wanted.Show.1080p</title>
                <guid>g-1</guid>
                <enclosure url="http://127.0.0.1:{port}/get/1.nzb" length="1000"/></item>
              <item><title>Unwanted.720p</title>
                <guid>g-2</guid>
                <enclosure url="http://127.0.0.1:{port}/get/2.nzb" length="1000"/></item>
            </channel></rss>"#
        );
        let (feed_port, _) = tiny_http(vec![("/rss".into(), feed_xml)]);

        let feed = FeedDef {
            id: 1,
            name: "idx".into(),
            url: format!("http://127.0.0.1:{feed_port}/rss"),
            interval: Duration::from_secs(3600),
            filter: "Accept(category:tv): *1080p*".into(),
            category: None,
            priority: 0,
            pause: false,
        };
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let seen = tmp.path().join("feeds.json");

        poll_feed(&engine, &feed, &seen, &cache).await;

        // Exactly the matching item queued, with the rule's category.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let snap = engine.snapshot();
            let done = snap.jobs.len() == 1
                && snap.jobs[0].name == "Wanted.Show.1080p"
                && !matches!(snap.jobs[0].status, nzbd_types::JobStatus::Fetching);
            if done {
                assert_eq!(snap.jobs[0].category.as_deref(), Some("tv"));
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "job never queued");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Preview cache captured both items with verdicts.
        let previews = cache.lock().unwrap().get(&1).unwrap().clone();
        assert_eq!(previews.len(), 2);
        assert!(previews.iter().any(|p| p.accepted && p.item.guid == "g-1"));
        assert!(previews.iter().any(|p| !p.accepted && p.item.guid == "g-2"));

        // Second poll: nothing new, nothing re-queued.
        poll_feed(&engine, &feed, &seen, &cache).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(engine.snapshot().jobs.len(), 1, "dedup by guid");
        let previews = cache.lock().unwrap().get(&1).unwrap().clone();
        assert!(previews.iter().all(|p| !p.new));

        engine.shutdown().await;
    }
}
