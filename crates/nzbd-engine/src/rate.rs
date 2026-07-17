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
// Speed meter: NZBGet-parity 30 × 1 s window
// ---------------------------------------------------------------------------

pub struct SpeedMeter {
    current_second: AtomicU64,
    total: AtomicU64,
    ring: Mutex<Ring>,
}

struct Ring {
    slots: [u64; 30],
    next: usize,
    filled: usize,
}

impl Default for SpeedMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedMeter {
    pub fn new() -> SpeedMeter {
        SpeedMeter {
            current_second: AtomicU64::new(0),
            total: AtomicU64::new(0),
            ring: Mutex::new(Ring {
                slots: [0; 30],
                next: 0,
                filled: 0,
            }),
        }
    }

    pub fn add(&self, n: u64) {
        self.current_second.fetch_add(n, Ordering::Relaxed);
        self.total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Called at 1 Hz by the owner tick; returns the windowed rate in B/s.
    pub fn tick(&self) -> u64 {
        let this_second = self.current_second.swap(0, Ordering::Relaxed);
        let mut r = self.ring.lock().unwrap();
        let idx = r.next;
        r.slots[idx] = this_second;
        r.next = (r.next + 1) % 30;
        r.filled = (r.filled + 1).min(30);
        let sum: u64 = r.slots.iter().sum();
        sum / r.filled.max(1) as u64
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

    #[test]
    fn meter_windows_and_totals() {
        let m = SpeedMeter::new();
        m.add(500);
        m.add(500);
        assert_eq!(m.tick(), 1000); // one filled slot: 1000/1
        m.add(2000);
        assert_eq!(m.tick(), 1500); // (1000+2000)/2
        assert_eq!(m.total(), 3000);
        for _ in 0..28 {
            assert!(m.tick() > 0);
        }
        // window full; two more empty ticks push the data out
        m.tick();
        m.tick();
        assert_eq!(m.tick(), 0);
    }
}
