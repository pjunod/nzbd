//! Per-server download volume accounting + quotas (NZBGet `DailyQuota` /
//! `MonthlyQuota` / `QuotaStartDay`, per-server volume counters for the
//! `servervolumes` RPC).
//!
//! Windows roll on UTC civil dates; the monthly window starts on
//! `quota_start_day` of each month. Counters persist per node
//! (`volumes.<suffix>.json`) so cluster peers on the shared volume can be
//! summed for an account-wide quota decision without write contention.

use nzbd_types::ServerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Days → (year, month, day) — Howard Hinnant's civil-from-days.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The monthly-quota period key for a unix timestamp: periods begin on
/// `start_day` of each month (clamped to 28 for short months).
fn month_key(unix: i64, start_day: u32) -> i64 {
    let days = unix.div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let start = start_day.clamp(1, 28);
    let (py, pm) = if d >= start {
        (y, m)
    } else if m == 1 {
        (y - 1, 12)
    } else {
        (y, m - 1)
    };
    py * 12 + pm as i64
}

fn day_key(unix: i64) -> i64 {
    unix.div_euclid(86_400)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeWindow {
    pub total_bytes: u64,
    pub day_key: i64,
    pub day_bytes: u64,
    pub month_key: i64,
    pub month_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeDoc {
    /// server id → window
    pub servers: HashMap<u32, VolumeWindow>,
}

impl VolumeDoc {
    pub fn day_total(&self, now_unix: i64) -> u64 {
        let key = day_key(now_unix);
        self.servers
            .values()
            .filter(|w| w.day_key == key)
            .map(|w| w.day_bytes)
            .sum()
    }

    pub fn month_total(&self, now_unix: i64, start_day: u32) -> u64 {
        let key = month_key(now_unix, start_day);
        self.servers
            .values()
            .filter(|w| w.month_key == key)
            .map(|w| w.month_bytes)
            .sum()
    }
}

/// This node's live counter book.
pub struct VolumeBook {
    doc: VolumeDoc,
    path: PathBuf,
    dir: PathBuf,
    dirty: bool,
}

impl VolumeBook {
    pub fn load(state_dir: &Path, suffix: &str) -> VolumeBook {
        let dir = state_dir.to_path_buf();
        let path = dir.join(format!("volumes.{suffix}.json"));
        let doc = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        VolumeBook {
            doc,
            path,
            dir,
            dirty: false,
        }
    }

    pub fn add(&mut self, server: ServerId, bytes: u64, now_unix: i64, start_day: u32) {
        let w = self.doc.servers.entry(server.0).or_default();
        let dk = day_key(now_unix);
        let mk = month_key(now_unix, start_day);
        if w.day_key != dk {
            w.day_key = dk;
            w.day_bytes = 0;
        }
        if w.month_key != mk {
            w.month_key = mk;
            w.month_bytes = 0;
        }
        w.total_bytes += bytes;
        w.day_bytes += bytes;
        w.month_bytes += bytes;
        self.dirty = true;
    }

    pub fn doc(&self) -> &VolumeDoc {
        &self.doc
    }

    /// Cluster-aware totals: this node's counters plus every peer's
    /// `volumes.*.json` in the same state dir.
    pub fn cluster_totals(&self, now_unix: i64, start_day: u32) -> (u64, u64) {
        let mut day = self.doc.day_total(now_unix);
        let mut month = self.doc.month_total(now_unix, start_day);
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p == self.path {
                    continue;
                }
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                if name.starts_with("volumes.") && name.ends_with(".json") {
                    if let Some(doc) = std::fs::read(&p)
                        .ok()
                        .and_then(|b| serde_json::from_slice::<VolumeDoc>(&b).ok())
                    {
                        day += doc.day_total(now_unix);
                        month += doc.month_total(now_unix, start_day);
                    }
                }
            }
        }
        (day, month)
    }

    pub fn save_if_dirty(&mut self) {
        if !self.dirty {
            return;
        }
        if let Ok(bytes) = serde_json::to_vec(&self.doc) {
            let tmp = self.path.with_extension("tmp");
            if std::fs::write(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, &self.path).is_ok() {
                self.dirty = false;
            }
        }
    }
}

/// Free bytes on the filesystem holding `path` (0 if it can't be measured).
pub fn free_space(path: &Path) -> u64 {
    use std::os::unix::ffi::OsStrExt;
    let Ok(cstr) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return u64::MAX;
    };
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cstr.as_ptr(), &mut st) };
    if rc != 0 {
        return u64::MAX; // can't measure → don't false-trip the guard
    }
    (st.f_bavail as u64).saturating_mul(st.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1)); // leap-adjacent
        assert_eq!(civil_from_days(20_651), (2026, 7, 17));
    }

    #[test]
    fn month_key_honors_start_day() {
        // 2026-07-17, start day 1 → July period.
        let jul17 = 20_651 * 86_400;
        assert_eq!(month_key(jul17, 1), 2026 * 12 + 7);
        // Start day 20: the 17th belongs to the JUNE period.
        assert_eq!(month_key(jul17, 20), 2026 * 12 + 6);
        // Start day 20 on the 20th → July period begins.
        let jul20 = (20_651 + 3) * 86_400;
        assert_eq!(month_key(jul20, 20), 2026 * 12 + 7);
    }

    #[test]
    fn windows_roll_and_persist() {
        let tmp = tempfile::tempdir().unwrap();
        let mut book = VolumeBook::load(tmp.path(), "a");
        let day1 = 20_651 * 86_400;
        book.add(ServerId(1), 100, day1, 1);
        book.add(ServerId(1), 50, day1, 1);
        book.add(ServerId(2), 10, day1, 1);
        assert_eq!(book.doc().day_total(day1), 160);
        assert_eq!(book.doc().month_total(day1, 1), 160);

        // Next day: daily rolls, monthly accumulates.
        let day2 = day1 + 86_400;
        book.add(ServerId(1), 5, day2, 1);
        assert_eq!(book.doc().day_total(day2), 5);
        assert_eq!(book.doc().month_total(day2, 1), 165);
        assert_eq!(book.doc().servers[&1].total_bytes, 155);

        // Persist + reload.
        book.save_if_dirty();
        let book2 = VolumeBook::load(tmp.path(), "a");
        assert_eq!(book2.doc().month_total(day2, 1), 165);

        // A peer file is summed into cluster totals.
        let peer = VolumeDoc {
            servers: HashMap::from([(
                1,
                VolumeWindow {
                    total_bytes: 40,
                    day_key: day_key(day2),
                    day_bytes: 40,
                    month_key: month_key(day2, 1),
                    month_bytes: 40,
                },
            )]),
        };
        std::fs::write(
            tmp.path().join("volumes.b.json"),
            serde_json::to_vec(&peer).unwrap(),
        )
        .unwrap();
        let (day, month) = book2.cluster_totals(day2, 1);
        assert_eq!(day, 45);
        assert_eq!(month, 205);
    }

    #[test]
    fn free_space_measures_something() {
        let free = free_space(Path::new("/"));
        assert!(free > 0);
    }
}
