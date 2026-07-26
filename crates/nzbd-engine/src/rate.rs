//! Global download-rate control and speed metering (ARCHITECTURE.md §8.5).
//!
//! The limiter is a token bucket with debt: connection tasks `debit(n)`
//! *after* each socket read; going negative delays the next read for
//! exactly the overdraft. This replaces NZBGet's cooperative
//! `Sleep(10ms)`-loop throttling — changing the limit takes effect on the
//! next read, and fairness falls out of per-task sleeping.
//!
//! Uses `tokio::time::Instant` throughout so tests can pause time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::time::{Duration, Instant};

/// Burst allowance: how far ahead of the steady rate a refill may run.
const BURST_SECS: f64 = 0.25;
const MIN_BURST: f64 = 64.0 * 1024.0;

pub struct RateLimiter {
    /// bytes/sec; 0 = unlimited.
    rate: AtomicU64,
    bucket: Mutex<Bucket>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(rate_bps: Option<u64>) -> RateLimiter {
        RateLimiter {
            rate: AtomicU64::new(rate_bps.unwrap_or(0)),
            bucket: Mutex::new(Bucket {
                tokens: 0.0,
                last: Instant::now(),
            }),
        }
    }

    pub fn set(&self, rate_bps: Option<u64>) {
        self.rate.store(rate_bps.unwrap_or(0), Ordering::Relaxed);
        let mut b = self.bucket.lock().unwrap();
        b.tokens = 0.0; // clean slate: new limit applies immediately
        b.last = Instant::now();
    }

    pub fn get(&self) -> Option<u64> {
        match self.rate.load(Ordering::Relaxed) {
            0 => None,
            r => Some(r),
        }
    }

    /// Charge `n` bytes just read; sleeps off any overdraft. Never blocks
    /// when unlimited.
    pub async fn debit(&self, n: usize) {
        let rate = self.rate.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        let rate_f = rate as f64;
        let wait = {
            let mut b = self.bucket.lock().unwrap();
            let now = Instant::now();
            let burst = (rate_f * BURST_SECS).max(MIN_BURST);
            b.tokens = (b.tokens + now.duration_since(b.last).as_secs_f64() * rate_f).min(burst);
            b.last = now;
            b.tokens -= n as f64;
            if b.tokens < 0.0 {
                Duration::from_secs_f64(-b.tokens / rate_f)
            } else {
                Duration::ZERO
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Speed meter: ONE wire measurement, attributed twice, normalized by time
// ---------------------------------------------------------------------------
//
// Field report 2026-07-26: the header tile said 24.8 MiB/s while the row
// and the provider chip said 9.9 — all three claimed to be the same
// measurement. Two causes, both fixed here:
//
//   * The header used a 30 × 1 s ring average while rows/chips used a 5 s
//     EMA — two windows over the same counters routinely disagree around
//     bursts. There is now exactly one derivation: per-entity EMAs, and
//     the header is their sum.
//   * Every consumer assumed the owner tick ran at exactly 1 Hz and read
//     each drain as "one second of bytes". The tick loop does journal
//     fsync and snapshot saves; on a slow state volume a 2.5 s-late tick
//     counted 2.5 s of bytes as one second — every rate inflated by the
//     stall factor. Drains now carry their measured wall time and rates
//     are bytes ÷ that, so a delayed tick cannot lie.

/// One take of the attributed wire counters, stamped with the wall time
/// they accumulated over.
pub struct Drained {
    /// Measured seconds since the previous drain (never 0).
    pub secs: f64,
    /// Wire bytes per job over that window.
    pub per_job: std::collections::HashMap<u32, u64>,
    /// The same bytes, sliced per news server.
    pub per_server: std::collections::HashMap<u32, u64>,
}

pub struct SpeedMeter {
    total: AtomicU64,
    /// When the counters were last drained (tokio clock: test-pausable).
    last_drain: Mutex<Instant>,
    /// Wire bytes per job since the last drain. Fed at the one read site
    /// alongside the per-server slice, so the two attributions can never
    /// drift apart — they are literally the same bytes counted twice.
    per_job: Mutex<std::collections::HashMap<u32, u64>>,
    /// The same wire bytes, sliced per SERVER instead of per job.
    per_server: Mutex<std::collections::HashMap<u32, u64>>,
}

impl Default for SpeedMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedMeter {
    pub fn new() -> SpeedMeter {
        SpeedMeter {
            total: AtomicU64::new(0),
            last_drain: Mutex::new(Instant::now()),
            per_job: Mutex::new(std::collections::HashMap::new()),
            per_server: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn add(&self, n: u64) {
        self.total.fetch_add(n, Ordering::Relaxed);
    }

    /// [`SpeedMeter::add`] plus per-job and per-server attribution. The
    /// connection task knows both, so both are recorded at the one read
    /// site — there is no way for the numbers to drift apart.
    pub fn add_for(&self, job: u32, server: u32, n: u64) {
        self.add(n);
        *self.per_job.lock().unwrap().entry(job).or_insert(0) += n;
        *self.per_server.lock().unwrap().entry(server).or_insert(0) += n;
    }

    /// Take and reset the attributed counters, stamped with the wall time
    /// they cover. Called by the owner tick — nominally 1 Hz, but the
    /// stamp, not the nominal cadence, is what rates divide by.
    pub fn drain(&self) -> Drained {
        let per_job = std::mem::take(&mut *self.per_job.lock().unwrap());
        let per_server = std::mem::take(&mut *self.per_server.lock().unwrap());
        let mut last = self.last_drain.lock().unwrap();
        let now = Instant::now();
        let secs = now.duration_since(*last).as_secs_f64().max(1e-3);
        *last = now;
        Drained {
            secs,
            per_job,
            per_server,
        }
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// Fold one drain of attributed wire bytes into a per-entity EMA map
/// (values in B/s). `secs` is the wall time the drain covers — the
/// instantaneous rate is bytes ÷ secs, so a delayed tick folds the same
/// rate a punctual one would have. Entities absent from the drain decay
/// toward zero instead of freezing at their last value.
pub fn fold_wire_ema(
    ema: &mut std::collections::HashMap<u32, f64>,
    drained: &std::collections::HashMap<u32, u64>,
    secs: f64,
) {
    const ALPHA: f64 = 1.0 / 5.0;
    let secs = secs.max(1e-3);
    for (id, e) in ema.iter_mut() {
        let inst = drained.get(id).copied().unwrap_or(0) as f64 / secs;
        *e += ALPHA * (inst - *e);
    }
    for (id, bytes) in drained {
        ema.entry(*id)
            .or_insert_with(|| *bytes as f64 / secs * ALPHA);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn limiter_paces_to_the_configured_rate() {
        let lim = RateLimiter::new(Some(1000)); // 1000 B/s
        let start = Instant::now();
        for _ in 0..4 {
            lim.debit(1000).await;
        }
        let elapsed = start.elapsed().as_secs_f64();
        assert!(
            (3.5..=4.6).contains(&elapsed),
            "4×1000B at 1000B/s should take ~4 s, took {elapsed:.2}s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unlimited_never_waits() {
        let lim = RateLimiter::new(None);
        let start = Instant::now();
        for _ in 0..100 {
            lim.debit(1 << 20).await;
        }
        assert!(start.elapsed() < Duration::from_millis(1));
    }

    #[tokio::test(start_paused = true)]
    async fn rate_change_applies_immediately() {
        let lim = RateLimiter::new(Some(10)); // crawl
        lim.debit(10_000).await; // builds debt, sleeps it off virtually
        lim.set(None);
        let start = Instant::now();
        lim.debit(1 << 20).await;
        assert!(start.elapsed() < Duration::from_millis(1));
    }

    #[tokio::test(start_paused = true)]
    async fn drain_stamps_measured_time_so_a_late_tick_cannot_inflate() {
        let m = SpeedMeter::new();
        tokio::time::advance(Duration::from_secs(1)).await;
        m.add_for(1, 0, 1000);
        let d = m.drain();
        assert!((d.secs - 1.0).abs() < 0.05, "punctual drain: ~1 s");
        assert_eq!(d.per_job[&1], 1000);

        // The stalled-owner case: 2.5 s pass before the next drain and
        // 2500 bytes land. Normalized, that is still 1000 B/s — the old
        // code read it as 2500 B/s (the header showing 24.8 MiB/s while
        // the wire moved at 9.9).
        tokio::time::advance(Duration::from_millis(2500)).await;
        m.add_for(1, 0, 2500);
        let d = m.drain();
        assert!((d.secs - 2.5).abs() < 0.05, "late drain: ~2.5 s");
        let bps = d.per_job[&1] as f64 / d.secs;
        assert!(
            (bps - 1000.0).abs() < 5.0,
            "normalized rate stays true: {bps}"
        );
    }

    #[test]
    fn attribution_partitions_the_same_bytes() {
        let m = SpeedMeter::new();
        m.add_for(1, 10, 700);
        m.add_for(1, 11, 300);
        m.add_for(2, 10, 500);
        let d = m.drain();
        let jobs: u64 = d.per_job.values().sum();
        let servers: u64 = d.per_server.values().sum();
        assert_eq!(jobs, 1500, "per-job slices cover every byte");
        assert_eq!(servers, 1500, "per-server slices cover the same bytes");
        assert_eq!(m.total(), 1500, "session total agrees");
        assert_eq!(d.per_server[&10], 1200);
        assert_eq!(d.per_server[&11], 300);
    }

    #[test]
    fn ema_converges_to_the_true_rate_at_any_cadence() {
        use std::collections::HashMap;
        // 1000 B/s arriving as punctual 1 s drains…
        let mut punctual = HashMap::new();
        let d1 = HashMap::from([(1u32, 1000u64)]);
        for _ in 0..60 {
            fold_wire_ema(&mut punctual, &d1, 1.0);
        }
        // …and the same 1000 B/s arriving as delayed 2.5 s drains.
        let mut late = HashMap::new();
        let d2 = HashMap::from([(1u32, 2500u64)]);
        for _ in 0..60 {
            fold_wire_ema(&mut late, &d2, 2.5);
        }
        assert!((punctual[&1] - 1000.0).abs() < 1.0, "{}", punctual[&1]);
        assert!((late[&1] - 1000.0).abs() < 1.0, "{}", late[&1]);
    }

    #[test]
    fn ema_decays_when_an_entity_goes_quiet() {
        use std::collections::HashMap;
        let mut ema = HashMap::from([(1u32, 1000.0f64)]);
        let empty = HashMap::new();
        for _ in 0..30 {
            fold_wire_ema(&mut ema, &empty, 1.0);
        }
        assert!(
            ema[&1] < 2.0,
            "a quiet job decays toward 0, not a frozen rate"
        );
    }
}
