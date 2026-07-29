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
//!
//! **Scoping the view was only half the fix, and the missing half is why
//! the log "rolls over" wrong.** A single FIFO shared by every scope means
//! the noisiest scope decides how far back *all* of them reach: measured
//! on nuc3 2026-07-29, an IDLE daemon's 1000-line ring held 662 per-file
//! lines, 289 job lines and 49 system lines. Under load the per-file
//! stream is faster still, so the boot banner, the server connections and
//! every job transition are evicted within minutes by lines the UI hides
//! by default. Asking "what happened to this job an hour ago?" then has no
//! answer, and the buffer meant to hold it spent its whole budget on
//! `file finished … ok=true`.
//!
//! So the ring is **two rings with independent budgets**: per-file records
//! roll against their own capacity and can never evict a system or job
//! line. Reads merge the two by id, which restores exactly the single-FIFO
//! view callers already expect — ids are globally allocated and both rings
//! stay ascending, so the merge is a two-way zip, not a sort. Writes stay
//! O(1); the merge cost lands on reads, which are thousands of times
//! rarer than pushes.

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

/// The two independent budgets. `file` holds per-file records; `main`
/// holds everything else (system + job). Both stay ascending by id.
#[derive(Debug)]
struct Rings {
    main: VecDeque<LogRecord>,
    file: VecDeque<LogRecord>,
}

impl Rings {
    /// Every record from both budgets, oldest first — a two-way merge on
    /// `id`, which is a global counter, so the result is exactly the order
    /// a single FIFO would have produced.
    fn merged(&self) -> Vec<&LogRecord> {
        let (mut a, mut b) = (self.main.iter().peekable(), self.file.iter().peekable());
        let mut out = Vec::with_capacity(self.main.len() + self.file.len());
        loop {
            let take_main = match (a.peek(), b.peek()) {
                (Some(x), Some(y)) => x.id <= y.id,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            out.push(if take_main {
                a.next().unwrap()
            } else {
                b.next().unwrap()
            });
        }
        out
    }
}

#[derive(Debug)]
pub struct LogBuffer {
    next_id: AtomicU64,
    rings: Mutex<Rings>,
    /// Budget for system + job records.
    main_capacity: usize,
    /// Budget for per-file records, spent independently of `main_capacity`.
    file_capacity: usize,
}

impl LogBuffer {
    /// `capacity` is the budget **per class**, not the total: system+job
    /// lines get `capacity` slots and per-file lines get their own
    /// `capacity` slots. That is the whole point — a per-file flood must
    /// not be able to shorten the window the other scopes reach back
    /// through (see the module docs). Worst-case memory is therefore twice
    /// what a single ring of the same number would use, which at the
    /// default of 1000 is a few hundred KiB.
    pub fn new(capacity: usize) -> Arc<LogBuffer> {
        Self::with_budgets(capacity, capacity)
    }

    pub fn with_budgets(main_capacity: usize, file_capacity: usize) -> Arc<LogBuffer> {
        let main_capacity = main_capacity.max(16);
        let file_capacity = file_capacity.max(16);
        Arc::new(LogBuffer {
            next_id: AtomicU64::new(1),
            rings: Mutex::new(Rings {
                main: VecDeque::with_capacity(main_capacity.min(4096)),
                file: VecDeque::with_capacity(file_capacity.min(4096)),
            }),
            main_capacity,
            file_capacity,
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
        let mut rings = self.rings.lock().unwrap();
        let (ring, cap) = match scope {
            LogScope::File => (&mut rings.file, self.file_capacity),
            _ => (&mut rings.main, self.main_capacity),
        };
        if ring.len() >= cap {
            ring.pop_front();
        }
        ring.push_back(rec);
    }

    /// Entries with id > `after`, oldest first, at most `limit`.
    pub fn since(&self, after: u64, limit: usize) -> Vec<LogRecord> {
        let rings = self.rings.lock().unwrap();
        rings
            .merged()
            .into_iter()
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
        let rings = self.rings.lock().unwrap();
        let limit = limit.max(1);
        let merged = rings.merged();
        let total = merged.iter().filter(|r| r.id > after).count();
        let skipped = total.saturating_sub(limit);
        let out = merged
            .into_iter()
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
        let rings = self.rings.lock().unwrap();
        let newest = |q: &VecDeque<LogRecord>| q.back().map(|r| r.id).unwrap_or(0);
        newest(&rings.main).max(newest(&rings.file))
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
        let rings = self.rings.lock().unwrap();
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
        let mut out: Vec<LogRecord> = rings
            .merged()
            .into_iter()
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

    /// The defect this file's second paragraph describes: with one shared
    /// FIFO, a per-file flood evicts every system and job line, so the
    /// answer to "when did this daemon boot / when did job 7 finish?" is
    /// gone — destroyed by lines the UI hides by default. Measured on
    /// nuc3: 662 of 1000 slots spent on per-file lines while IDLE.
    ///
    /// With independent budgets the flood spends only its own.
    #[test]
    fn a_per_file_flood_cannot_evict_the_lines_that_matter() {
        let buf = LogBuffer::with_budgets(32, 32);
        buf.push_scoped("INFO", "nzbd starting".into(), LogScope::System, None);
        buf.push_scoped("INFO", "job finished job=7".into(), LogScope::Job, Some(7));

        // Ten times the per-file budget, which under one shared ring would
        // have pushed both lines above out long ago.
        for f in 0..320 {
            buf.push_scoped(
                "INFO",
                format!("file finished job=7 file={f} ok=true"),
                LogScope::File,
                Some(7),
            );
        }

        let quiet = buf.tail_filtered(1000, Some(&[LogScope::System, LogScope::Job]), None);
        assert_eq!(quiet.len(), 2, "the boot banner and the job line survived");
        assert_eq!(quiet[0].text, "nzbd starting");
        assert_eq!(quiet[1].text, "job finished job=7");

        // The file ring rolled against its OWN budget, keeping the newest.
        let files = buf.tail_filtered(1000, Some(&[LogScope::File]), None);
        assert_eq!(files.len(), 32);
        assert!(files[31].text.contains("file=319"));
        assert!(files[0].text.contains("file=288"));
    }

    /// Reads merge the two budgets back into one ascending stream, so
    /// every existing caller (`since`, the SSE tail, the compat `log`
    /// method) sees exactly the order a single FIFO produced.
    #[test]
    fn reads_merge_the_budgets_back_into_one_ordered_stream() {
        let buf = LogBuffer::with_budgets(64, 64);
        for i in 0..12 {
            let scope = if i % 3 == 0 {
                LogScope::File
            } else {
                LogScope::Job
            };
            buf.push_scoped("INFO", format!("line {i}"), scope, Some(1));
        }
        let all = buf.tail(100);
        assert_eq!(all.len(), 12);
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.text, format!("line {i}"), "interleaved order preserved");
        }
        assert!(all.windows(2).all(|w| w[0].id < w[1].id));

        // `since` walks the merged stream too, and newest_id is the real
        // newest whichever budget it landed in.
        assert_eq!(buf.since(all[8].id, 100).len(), 3);
        assert_eq!(buf.newest_id(), all[11].id);

        // A capped tail reports the skip against the merged total.
        let (page, skipped) = buf.since_capped(0, 5);
        assert_eq!(page.len(), 5);
        assert_eq!(skipped, 7);
        assert_eq!(page[4].text, "line 11", "a lagging tail gets the NEWEST");
    }
}
