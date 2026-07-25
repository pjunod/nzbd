//! In-memory log ring buffer: a `tracing` layer feeds it, the native API
//! and the compat shim's `log`/`writelog` methods read it (NZBGet keeps
//! its recent log in RAM the same way).
//!
//! Every record carries a **scope** — `system`, `job`, or `file` — and
//! the job id when one is present, derived from the tracing fields at
//! capture time. Per-file lines ("file finished job=59 file=862 …") are
//! two orders of magnitude noisier than anything else and were drowning
//! the log view (field report 2026-07-25); scoping lets the UI default
//! them off, and `?job=` powers per-job log tails.

use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_CAPACITY: usize = 1000;

/// What a log line is about, coarsely: the daemon itself, one job, or one
/// file inside a job. Derived from tracing fields (`job=`, `file=`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogScope {
    System,
    Job,
    File,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogRecord {
    pub id: u64,
    /// INFO / WARNING / ERROR / DETAIL (NZBGet vocabulary).
    pub kind: &'static str,
    pub time_unix: i64,
    pub text: String,
    pub scope: LogScope,
    /// The job this line is about, when the event carried a `job` field.
    pub job: Option<u32>,
}

#[derive(Debug)]
pub struct LogBuffer {
    next_id: AtomicU64,
    ring: Mutex<VecDeque<LogRecord>>,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Arc<LogBuffer> {
        Arc::new(LogBuffer {
            next_id: AtomicU64::new(1),
            ring: Mutex::new(VecDeque::with_capacity(capacity.min(4096))),
            capacity: capacity.max(16),
        })
    }

    pub fn push(&self, kind: &'static str, text: String) {
        self.push_scoped(kind, text, LogScope::System, None);
    }

    pub fn push_scoped(&self, kind: &'static str, text: String, scope: LogScope, job: Option<u32>) {
        let rec = LogRecord {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            kind,
            time_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            text,
            scope,
            job,
        };
        let mut ring = self.ring.lock().unwrap();
        if ring.len() >= self.capacity {
            ring.pop_front();
        }
        ring.push_back(rec);
    }

    /// Entries with id > `after`, oldest first, at most `limit`.
    pub fn since(&self, after: u64, limit: usize) -> Vec<LogRecord> {
        let ring = self.ring.lock().unwrap();
        ring.iter()
            .filter(|r| r.id > after)
            .take(limit.max(1))
            .cloned()
            .collect()
    }

    /// The NEWEST `limit` entries with id > `after`, oldest first, plus how
    /// many older ones the cap skipped.
    ///
    /// A live tail that falls behind should show the most RECENT lines and
    /// say how much it missed. Handing out the oldest `limit` instead would
    /// leave the view permanently stuck in the past during a burst — the
    /// failure mode a naive `take(limit)` produces.
    pub fn since_capped(&self, after: u64, limit: usize) -> (Vec<LogRecord>, u32) {
        let ring = self.ring.lock().unwrap();
        let limit = limit.max(1);
        let total = ring.iter().filter(|r| r.id > after).count();
        let skipped = total.saturating_sub(limit);
        let out = ring
            .iter()
            .filter(|r| r.id > after)
            .skip(skipped)
            .cloned()
            .collect();
        (out, skipped as u32)
    }

    /// Highest id currently in the ring (0 when empty). A new SSE stream
    /// starts from here so it tails from "now" instead of replaying the
    /// whole buffer; the client backfills through the REST endpoint when
    /// the Logs tab is opened.
    pub fn newest_id(&self) -> u64 {
        self.ring.lock().unwrap().back().map(|r| r.id).unwrap_or(0)
    }

    /// The newest `limit` entries, oldest first (NZBGet `log(0, N)`).
    pub fn tail(&self, limit: usize) -> Vec<LogRecord> {
        self.tail_filtered(limit, None, None)
    }

    /// The newest `limit` entries matching the filters, oldest first.
    /// `scopes` = allowed scopes (None = all); `job` = only lines about
    /// that job (its `job`- and `file`-scoped records).
    pub fn tail_filtered(
        &self,
        limit: usize,
        scopes: Option<&[LogScope]>,
        job: Option<u32>,
    ) -> Vec<LogRecord> {
        let ring = self.ring.lock().unwrap();
        let matches = |r: &LogRecord| {
            if let Some(j) = job {
                if r.job != Some(j) {
                    return false;
                }
            }
            match scopes {
                Some(s) => s.contains(&r.scope),
                None => true,
            }
        };
        let mut out: Vec<LogRecord> = ring
            .iter()
            .rev()
            .filter(|r| matches(r))
            .take(limit.max(1))
            .cloned()
            .collect();
        out.reverse();
        out
    }
}

/// `tracing` layer that mirrors events into a [`LogBuffer`].
pub struct LogBufferLayer(pub Arc<LogBuffer>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogBufferLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        use tracing::Level;
        let kind = match *event.metadata().level() {
            Level::ERROR => "ERROR",
            Level::WARN => "WARNING",
            Level::INFO => "INFO",
            Level::DEBUG => "DETAIL",
            Level::TRACE => return, // too chatty for the ring
        };
        struct Visitor {
            text: String,
            job: Option<u32>,
            has_file: bool,
        }
        impl tracing::field::Visit for Visitor {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                match field.name() {
                    "job" => self.job = u32::try_from(value).ok(),
                    "file" => self.has_file = true,
                    _ => {}
                }
                self.record_debug(field, &value);
            }
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                match field.name() {
                    "job" => self.job = u32::try_from(value).ok(),
                    "file" => self.has_file = true,
                    _ => {}
                }
                self.record_debug(field, &value);
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    if !self.text.is_empty() {
                        self.text.push(' ');
                    }
                    self.text.push_str(format!("{value:?}").trim_matches('"'));
                } else {
                    if !self.text.is_empty() {
                        self.text.push(' ');
                    }
                    self.text.push_str(&format!("{}={:?}", field.name(), value));
                }
            }
        }
        let mut v = Visitor {
            text: String::new(),
            job: None,
            has_file: false,
        };
        event.record(&mut v);
        let scope = match (v.job, v.has_file) {
            (Some(_), true) => LogScope::File,
            (Some(_), false) => LogScope::Job,
            (None, _) => LogScope::System,
        };
        self.0.push_scoped(kind, v.text, scope, v.job);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_rolls_and_queries() {
        let buf = LogBuffer::new(16);
        for i in 0..40 {
            buf.push("INFO", format!("line {i}"));
        }
        let tail = buf.tail(5);
        assert_eq!(tail.len(), 5);
        assert_eq!(tail[4].text, "line 39");
        assert_eq!(tail[0].text, "line 35");
        assert!(tail[0].id < tail[4].id);

        // Ring capacity held.
        assert_eq!(buf.tail(1000).len(), 16);

        // since() pages forward.
        let newest = tail[4].id;
        assert!(buf.since(newest, 10).is_empty());
        let page = buf.since(newest - 3, 10);
        assert_eq!(page.len(), 3);
    }

    /// Per-file noise must be separable: scope filters and per-job tails
    /// (the flood of "file finished" lines was burying everything else).
    #[test]
    fn scope_and_job_filters() {
        let buf = LogBuffer::new(64);
        buf.push_scoped("INFO", "boot".into(), LogScope::System, None);
        for f in 0..10 {
            buf.push_scoped(
                "INFO",
                format!("file finished job=7 file={f}"),
                LogScope::File,
                Some(7),
            );
        }
        buf.push_scoped("INFO", "job added job=7".into(), LogScope::Job, Some(7));
        buf.push_scoped("INFO", "job added job=9".into(), LogScope::Job, Some(9));

        // Files hidden: the 10 noisy lines vanish, the 3 real ones stay.
        let quiet = buf.tail_filtered(100, Some(&[LogScope::System, LogScope::Job]), None);
        assert_eq!(quiet.len(), 3);
        assert!(quiet.iter().all(|r| r.scope != LogScope::File));

        // Per-job tail: everything about job 7, nothing else.
        let j7 = buf.tail_filtered(100, None, Some(7));
        assert_eq!(j7.len(), 11);
        assert!(j7.iter().all(|r| r.job == Some(7)));

        // Limit applies to the FILTERED stream (newest kept).
        let last2 = buf.tail_filtered(2, None, Some(7));
        assert_eq!(last2.len(), 2);
        assert!(last2[1].text.contains("job added"));
    }
}
