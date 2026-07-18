//! In-memory log ring buffer: a `tracing` layer feeds it, the native API
//! and the compat shim's `log`/`writelog` methods read it (NZBGet keeps
//! its recent log in RAM the same way).

use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_CAPACITY: usize = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct LogRecord {
    pub id: u64,
    /// INFO / WARNING / ERROR / DETAIL / DEBUG (NZBGet vocabulary).
    pub kind: &'static str,
    pub time_unix: i64,
    pub text: String,
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
        let rec = LogRecord {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            kind,
            time_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            text,
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

    /// The newest `limit` entries, oldest first (NZBGet `log(0, N)`).
    pub fn tail(&self, limit: usize) -> Vec<LogRecord> {
        let ring = self.ring.lock().unwrap();
        let skip = ring.len().saturating_sub(limit.max(1));
        ring.iter().skip(skip).cloned().collect()
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
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    if !self.0.is_empty() {
                        self.0.push(' ');
                    }
                    self.0.push_str(format!("{value:?}").trim_matches('"'));
                } else {
                    if !self.0.is_empty() {
                        self.0.push(' ');
                    }
                    self.0.push_str(&format!("{}={:?}", field.name(), value));
                }
            }
        }
        let mut v = Visitor(String::new());
        event.record(&mut v);
        self.0.push(kind, v.0);
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
}
