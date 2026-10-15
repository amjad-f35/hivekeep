//! Memory limits without the OOM killer.
//!
//! When the worker's container scope kills the whole worker on any OOM kill
//! beneath it, a sandbox must never reach `memory.max`. Its leaf gets
//! `memory.high` at the Actor's limit instead, which throttles without
//! invoking the OOM killer, and a [`MemoryWatch`] decides when a throttled
//! sandbox is stuck there and should be killed with `cgroup.kill`.

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::cgroup_stats::{read_keyed_uint, read_uint};

/// How far back stalls are counted.
const STALL_WINDOW: Duration = Duration::from_secs(2);
/// The share of the window a cgroup may spend stalled before it is stuck.
const STALL_KILL: f64 = 0.25;
/// The least a cgroup is watched before it can be stuck.
const STALL_MIN_WINDOW: Duration = Duration::from_secs(1);
/// The most a cgroup may go over its limit at all.
pub const OVERSHOOT_MAX: u64 = 16 << 20;

#[derive(Debug, Clone, Copy)]
struct StallSample {
    at: Instant,
    /// `memory.pressure` "some" total, microseconds.
    stall: u64,
    /// `memory.events.local` high.
    high: u64,
}

/// Tracks one cgroup's recent memory stalls. Check it on a timer.
#[derive(Debug, Default, Clone)]
pub struct MemoryWatch {
    samples: Vec<StallSample>,
}

fn not_found(path: &Path, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no {what} in {}", path.display()),
    )
}

fn keyed(path: &Path, key: &str) -> io::Result<u64> {
    read_keyed_uint(path, key)?.ok_or_else(|| not_found(path, key))
}

fn memory_stall(dir: &Path) -> io::Result<u64> {
    let path = dir.join("memory.pressure");
    let s = std::fs::read_to_string(&path)?;
    for line in s.lines() {
        let mut f = line.split_whitespace();
        if f.next() != Some("some") {
            continue;
        }
        for kv in f {
            if let Some(v) = kv.strip_prefix("total=") {
                return v
                    .parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e}")));
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("no some total in {}", path.display()),
    ))
}

impl MemoryWatch {
    /// Whether the cgroup at `dir` is stuck at `limit`, with its
    /// `memory.current`: stalled for at least a quarter of the recent window
    /// while hitting its own `memory.high` with little reclaimable cache, or
    /// over the limit by more than a sixteenth of it (capped at
    /// [`OVERSHOOT_MAX`]).
    pub fn check(&mut self, dir: &Path, limit: u64, now: Instant) -> io::Result<(bool, u64)> {
        let current_path = dir.join("memory.current");
        let current =
            read_uint(&current_path)?.ok_or_else(|| not_found(&current_path, "memory.current"))?;
        if current > limit + (limit / 16).min(OVERSHOOT_MAX) {
            return Ok((true, current));
        }
        let floor = limit - limit / 16;
        if current < floor {
            self.samples.clear();
            return Ok((false, current));
        }
        let stat = dir.join("memory.stat");
        let cache = keyed(&stat, "active_file")? + keyed(&stat, "inactive_file")?;
        if current - cache.min(current) < floor {
            self.samples.clear();
            return Ok((false, current));
        }
        let stall = memory_stall(dir)?;
        let high = keyed(&dir.join("memory.events.local"), "high")?;
        self.samples.push(StallSample {
            at: now,
            stall,
            high,
        });
        while self.samples.len() > 1 && now.duration_since(self.samples[1].at) >= STALL_WINDOW {
            self.samples.remove(0);
        }
        let first = self.samples[0];
        let span = now.duration_since(first.at);
        if span < STALL_MIN_WINDOW {
            return Ok((false, current));
        }
        let stalled = Duration::from_micros(stall.saturating_sub(first.stall));
        Ok((
            high > first.high && stalled.as_secs_f64() >= STALL_KILL * span.as_secs_f64(),
            current,
        ))
    }
}

/// Whether the kernel tracks memory pressure for cgroups under `root`, which
/// the watch needs to tell a stuck sandbox from a busy one.
pub fn psi_enabled(root: &Path) -> bool {
    root.join("memory.pressure").exists()
}

/// Replaces the leaf's `memory.max` with a `memory.high` at the same limit,
/// returning the limit, or 0 when the leaf has none. `memory.high` is written
/// first so the leaf is never unlimited.
pub fn swap_memory_max_for_high(dir: &Path) -> io::Result<u64> {
    let v = match std::fs::read_to_string(dir.join("memory.max")) {
        Ok(v) => v.trim().to_string(),
        // The memory controller is not delegated.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if v == "max" {
        return Ok(0);
    }
    let limit: u64 = v.parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parsing memory.max {v:?}: {e}"),
        )
    })?;
    std::fs::write(dir.join("memory.high"), &v)?;
    std::fs::write(dir.join("memory.max"), "max")?;
    Ok(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: u64 = 256 << 20;

    fn write(dir: &Path, current: u64, cache: u64, stall_ms: u64, high: u64) {
        let files = [
            ("memory.current", current.to_string()),
            (
                "memory.stat",
                format!("anon 1\ninactive_file {cache}\nactive_file 0\n"),
            ),
            (
                "memory.pressure",
                format!(
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total={}\nfull avg10=0.00 total=0\n",
                    stall_ms * 1000
                ),
            ),
            ("memory.events.local", format!("low 0\nhigh {high}\n")),
        ];
        for (n, c) in files {
            std::fs::write(dir.join(n), c).unwrap();
        }
    }

    #[test]
    fn stuck_at_the_limit() {
        let d = tempfile::tempdir().unwrap();
        let t0 = Instant::now();
        let mut w = MemoryWatch::default();
        for (after, stall, want) in [(0, 0, false), (500, 400, false), (1000, 900, true)] {
            write(d.path(), LIMIT + (4 << 20), 1 << 20, stall, stall);
            let (stuck, _) = w
                .check(d.path(), LIMIT, t0 + Duration::from_millis(after))
                .unwrap();
            assert_eq!(stuck, want, "at {after}ms");
        }
    }

    #[test]
    fn busy_but_not_stalled() {
        let d = tempfile::tempdir().unwrap();
        let t0 = Instant::now();
        let mut w = MemoryWatch::default();
        for (after, stall) in [(0, 0), (1000, 50), (1500, 100)] {
            write(d.path(), LIMIT, 0, stall, stall);
            assert!(
                !w.check(d.path(), LIMIT, t0 + Duration::from_millis(after))
                    .unwrap()
                    .0
            );
        }
    }

    #[test]
    fn reclaimable_cache_is_not_stuck() {
        let d = tempfile::tempdir().unwrap();
        let t0 = Instant::now();
        let mut w = MemoryWatch::default();
        for (after, stall) in [(0, 0), (1000, 900), (2000, 1900)] {
            write(d.path(), LIMIT, LIMIT / 4, stall, stall);
            assert!(
                !w.check(d.path(), LIMIT, t0 + Duration::from_millis(after))
                    .unwrap()
                    .0
            );
        }
    }

    #[test]
    fn caps_overshoot() {
        let limit: u64 = 8 << 30;
        for (current, want) in [
            (limit + OVERSHOOT_MAX, false),
            (limit + OVERSHOOT_MAX + 1, true),
        ] {
            let d = tempfile::tempdir().unwrap();
            write(d.path(), current, 0, 0, 0);
            let mut w = MemoryWatch::default();
            assert_eq!(w.check(d.path(), limit, Instant::now()).unwrap().0, want);
        }
    }

    #[test]
    fn missing_cgroup_errors() {
        let d = tempfile::tempdir().unwrap();
        assert!(MemoryWatch::default()
            .check(&d.path().join("gone"), 1, Instant::now())
            .is_err());
    }

    #[test]
    fn swaps_max_for_high() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("memory.max"), "268435456\n").unwrap();
        std::fs::write(d.path().join("memory.high"), "max\n").unwrap();
        assert_eq!(swap_memory_max_for_high(d.path()).unwrap(), 268435456);
        assert_eq!(
            std::fs::read_to_string(d.path().join("memory.high"))
                .unwrap()
                .trim(),
            "268435456"
        );
        assert_eq!(
            std::fs::read_to_string(d.path().join("memory.max"))
                .unwrap()
                .trim(),
            "max"
        );
    }

    #[test]
    fn no_limit_to_swap() {
        let unlimited = tempfile::tempdir().unwrap();
        std::fs::write(unlimited.path().join("memory.max"), "max\n").unwrap();
        assert_eq!(swap_memory_max_for_high(unlimited.path()).unwrap(), 0);
        let none = tempfile::tempdir().unwrap();
        assert_eq!(swap_memory_max_for_high(none.path()).unwrap(), 0);
        assert!(!none.path().join("memory.high").exists());
    }
}
