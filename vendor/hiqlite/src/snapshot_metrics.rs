use arc_swap::ArcSwapOption;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Fixed upper bounds for database snapshot duration histograms.
///
/// Nanoseconds keep the vendored hook integer-only. The application renders
/// the public Prometheus family in seconds with matching fixed labels.
pub const DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS: [u64; 16] = [
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
    120_000_000_000,
    300_000_000_000,
];

/// One coherent monotonic snapshot of a cumulative duration histogram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbSnapshotHistogram {
    pub count: u64,
    pub sum_nanos: u64,
    pub cumulative_buckets: [u64; DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS.len()],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbSnapshotLastOutcome {
    pub ok: bool,
    pub observed_at_unix_ms: u64,
    pub elapsed_nanos: u64,
}

/// Fixed operation/outcome projection of the database snapshot hooks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbSnapshotMetricsSnapshot {
    pub build_ok: DbSnapshotHistogram,
    pub build_error: DbSnapshotHistogram,
    pub install_ok: DbSnapshotHistogram,
    pub install_error: DbSnapshotHistogram,
    pub last_build: Option<DbSnapshotLastOutcome>,
    pub last_install: Option<DbSnapshotLastOutcome>,
}

const ZERO_HISTOGRAM: DbSnapshotHistogram = DbSnapshotHistogram {
    count: 0,
    sum_nanos: 0,
    cumulative_buckets: [0; DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS.len()],
};

const ZERO_SNAPSHOT: DbSnapshotMetricsSnapshot = DbSnapshotMetricsSnapshot {
    build_ok: ZERO_HISTOGRAM,
    build_error: ZERO_HISTOGRAM,
    install_ok: ZERO_HISTOGRAM,
    install_error: ZERO_HISTOGRAM,
    last_build: None,
    last_install: None,
};

/// Local-only handle with a coherent, wait-free scrape path.
#[derive(Clone, Copy, Debug)]
pub struct LocalDbSnapshotMetrics {
    _private: (),
}

impl LocalDbSnapshotMetrics {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }

    /// Copy the most recently completed metrics publication. This does not
    /// acquire the writer mutex or lazily initialize any shared state.
    #[must_use]
    pub fn snapshot(self) -> DbSnapshotMetricsSnapshot {
        PUBLISHED
            .load_full()
            .as_deref()
            .copied()
            .unwrap_or(ZERO_SNAPSHOT)
    }
}

static WRITER_STATE: Mutex<DbSnapshotMetricsSnapshot> = Mutex::new(ZERO_SNAPSHOT);
static PUBLISHED: ArcSwapOption<DbSnapshotMetricsSnapshot> = ArcSwapOption::const_empty();

fn record(operation: SnapshotOperation, ok: bool, elapsed_nanos: u64) {
    let mut snapshot = WRITER_STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let last = DbSnapshotLastOutcome {
        ok,
        observed_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .unwrap_or_default(),
        elapsed_nanos,
    };
    match operation {
        SnapshotOperation::Build => snapshot.last_build = Some(last),
        SnapshotOperation::Install => snapshot.last_install = Some(last),
    }
    let histogram = match (operation, ok) {
        (SnapshotOperation::Build, true) => &mut snapshot.build_ok,
        (SnapshotOperation::Build, false) => &mut snapshot.build_error,
        (SnapshotOperation::Install, true) => &mut snapshot.install_ok,
        (SnapshotOperation::Install, false) => &mut snapshot.install_error,
    };
    histogram.count = histogram.count.saturating_add(1);
    histogram.sum_nanos = histogram.sum_nanos.saturating_add(elapsed_nanos);
    for (bound, bucket) in DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS
        .iter()
        .zip(&mut histogram.cumulative_buckets)
    {
        if elapsed_nanos <= *bound {
            *bucket = bucket.saturating_add(1);
        }
    }
    PUBLISHED.store(Some(Arc::new(*snapshot)));
}

#[derive(Clone, Copy)]
pub(crate) enum SnapshotOperation {
    Build,
    Install,
}

/// RAII start-and-finish hook. Every exit after construction records exactly
/// one fixed outcome; cancellation, panic, and an early `?` are errors.
pub(crate) struct SnapshotTimer {
    operation: SnapshotOperation,
    started_at: Instant,
    ok: bool,
}

impl SnapshotTimer {
    pub(crate) fn start(operation: SnapshotOperation) -> Self {
        Self {
            operation,
            started_at: Instant::now(),
            ok: false,
        }
    }

    pub(crate) fn success(mut self) {
        self.ok = true;
    }
}

impl Drop for SnapshotTimer {
    fn drop(&mut self) {
        let elapsed_nanos = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        record(self.operation, self.ok, elapsed_nanos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn timer_records_success_error_and_cancellation_once() {
        let before = LocalDbSnapshotMetrics::new().snapshot();
        SnapshotTimer::start(SnapshotOperation::Build).success();
        drop(SnapshotTimer::start(SnapshotOperation::Install));
        let after = LocalDbSnapshotMetrics::new().snapshot();
        assert_eq!(after.build_ok.count, before.build_ok.count + 1);
        assert_eq!(after.install_error.count, before.install_error.count + 1);
        assert_eq!(after.build_error.count, before.build_error.count);
        assert_eq!(after.install_ok.count, before.install_ok.count);
        assert_eq!(after.last_build.map(|outcome| outcome.ok), Some(true));
        assert_eq!(after.last_install.map(|outcome| outcome.ok), Some(false));
        assert!(
            after
                .last_build
                .is_some_and(|outcome| outcome.observed_at_unix_ms > 0)
        );
        assert!(
            after
                .last_install
                .is_some_and(|outcome| outcome.observed_at_unix_ms > 0)
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_never_exceed_count() {
        let before = LocalDbSnapshotMetrics::new().snapshot();
        record(SnapshotOperation::Build, true, 1_000_000);
        record(SnapshotOperation::Build, true, 500_000_000);
        let after = LocalDbSnapshotMetrics::new().snapshot();
        assert_eq!(after.build_ok.count, before.build_ok.count + 2);
        assert_eq!(
            after.build_ok.cumulative_buckets[0],
            before.build_ok.cumulative_buckets[0] + 1
        );
        assert_eq!(
            after.build_ok.cumulative_buckets[8],
            before.build_ok.cumulative_buckets[8] + 2
        );
        assert!(
            after
                .build_ok
                .cumulative_buckets
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
        );
        assert!(
            after
                .build_ok
                .cumulative_buckets
                .iter()
                .all(|bucket| *bucket <= after.build_ok.count)
        );
    }

    #[test]
    fn concurrent_snapshot_metrics_writes_and_scrapes_are_coherent() {
        const WRITERS: usize = 4;
        const RECORDS_PER_WRITER: usize = 10_000;
        let before = LocalDbSnapshotMetrics::new().snapshot();
        let barrier = Arc::new(Barrier::new(WRITERS + 1));
        let active = Arc::new(AtomicUsize::new(WRITERS));
        let writers = (0..WRITERS)
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                let active = Arc::clone(&active);
                thread::spawn(move || {
                    barrier.wait();
                    for sample in 0..RECORDS_PER_WRITER {
                        let elapsed = DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS
                            [(writer + sample) % DB_SNAPSHOT_HISTOGRAM_BOUNDS_NANOS.len()];
                        record(SnapshotOperation::Build, true, elapsed);
                    }
                    active.fetch_sub(1, Ordering::Release);
                })
            })
            .collect::<Vec<_>>();

        barrier.wait();
        while active.load(Ordering::Acquire) != 0 {
            let view = LocalDbSnapshotMetrics::new().snapshot().build_ok;
            assert!(
                view.cumulative_buckets
                    .windows(2)
                    .all(|pair| pair[0] <= pair[1])
            );
            assert!(
                view.cumulative_buckets
                    .iter()
                    .all(|bucket| *bucket <= view.count)
            );
        }
        for writer in writers {
            writer.join().expect("snapshot metrics writer panicked");
        }

        let after = LocalDbSnapshotMetrics::new().snapshot();
        assert_eq!(
            after.build_ok.count,
            before.build_ok.count
                + u64::try_from(WRITERS * RECORDS_PER_WRITER).expect("test count fits u64")
        );
        assert_eq!(
            after.build_ok.cumulative_buckets.last(),
            Some(&after.build_ok.count)
        );
    }

    #[test]
    fn local_handle_has_a_private_constructor_and_no_default_escape_hatch() {
        let source = include_str!("snapshot_metrics.rs");
        let declaration = ["pub struct LocalDbSnapshot", "Metrics {\n    _private: (),"].concat();
        let constructor = ["pub(crate) fn ", "new() -> Self"].concat();
        let default_impl = ["impl Default for LocalDb", "SnapshotMetrics"].concat();
        let default_derive = ["derive(Clone, Copy, Debug, ", "Default)"].concat();
        let lazy_global = ["Once", "Lock"].concat();
        assert!(source.contains(&declaration));
        assert!(source.contains(&constructor));
        assert!(source.contains("ArcSwapOption::const_empty()"));
        assert!(source.contains("load_full()"));
        assert!(!source.contains(&default_impl));
        assert!(!source.contains(&default_derive));
        assert!(!source.contains(&lazy_global));
    }
}
