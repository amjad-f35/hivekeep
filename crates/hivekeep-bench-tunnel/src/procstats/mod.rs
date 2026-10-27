//! What a benchmark process costs: CPU time, resident memory, heap in use,
//! async tasks and threads, and inside a cgroup v2 container the cgroup's
//! own CPU and memory accounting. Every benchmark process serves its
//! snapshot as JSON so the load generator can read it per rung.

pub mod alloc;
pub mod rusage;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A process's resource counters at one instant.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub time: DateTime<Utc>,
    /// Identifies the process: a reader seeing it change knows the process
    /// was replaced, as a restarted container is.
    pub started_at: DateTime<Utc>,
    /// User plus system CPU since the process started.
    pub cpu_seconds: f64,
    /// Resident set size: current on Linux, elsewhere the peak so far, which
    /// `rss_is_peak` says.
    pub rss_bytes: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rss_is_peak: bool,
    /// Heap bytes allocated and not freed, when the process counts them.
    pub heap_in_use_bytes: u64,
    /// Async tasks alive on the process's runtime.
    pub tasks: usize,
    /// OS threads, where the platform reports them.
    pub threads: usize,
    /// Cores the process may run on.
    pub cpus: usize,
    /// The cgroup's accounting, when the process runs in a cgroup v2
    /// hierarchy other than the root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup: Option<Cgroup>,
}

/// A cgroup v2 container's accounting.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cgroup {
    pub cpu_seconds: f64,
    pub memory_current_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub memory_peak_bytes: u64,
    /// `cpu.max` as cores; zero is unlimited.
    #[serde(default, skip_serializing_if = "is_zero_f")]
    pub cpu_limit_cores: f64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}
fn is_zero_f(v: &f64) -> bool {
    *v == 0.0
}

fn started_at() -> DateTime<Utc> {
    static STARTED: OnceLock<DateTime<Utc>> = OnceLock::new();
    *STARTED.get_or_init(Utc::now)
}

/// Fixes the process's start time; call early in `main`.
pub fn mark_start() {
    let _ = started_at();
}

/// A snapshot of this process.
pub fn read() -> Snapshot {
    let (rss_bytes, rss_is_peak) = rusage::rss();
    Snapshot {
        time: Utc::now(),
        started_at: started_at(),
        cpu_seconds: rusage::cpu_seconds(),
        rss_bytes,
        rss_is_peak,
        heap_in_use_bytes: alloc::heap_in_use(),
        tasks: tokio::runtime::Handle::try_current()
            .map(|h| h.metrics().num_alive_tasks())
            .unwrap_or(0),
        threads: rusage::threads(),
        cpus: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        cgroup: read_cgroup(&own_cgroup()),
    }
}

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// This process's cgroup v2 directory, from `/proc/self/cgroup`, so a
/// process moved into a cgroup of its own reports that one rather than its
/// container's.
pub fn own_cgroup() -> PathBuf {
    let Ok(text) = std::fs::read_to_string("/proc/self/cgroup") else {
        return PathBuf::from(CGROUP_ROOT);
    };
    text.trim()
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| PathBuf::from(format!("{CGROUP_ROOT}{}", p.trim_end_matches('/'))))
        .unwrap_or_else(|| PathBuf::from(CGROUP_ROOT))
}

fn read_u64(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// cgroup v2 accounting in `dir`. The root of a hierarchy has no
/// `memory.current`, so a process in a host's root cgroup reports `None`.
pub fn read_cgroup(dir: &Path) -> Option<Cgroup> {
    let stat = std::fs::read_to_string(dir.join("cpu.stat")).ok()?;
    let mut c = Cgroup {
        memory_current_bytes: read_u64(&dir.join("memory.current"))?,
        ..Default::default()
    };
    for line in stat.lines() {
        if let Some(v) = line.strip_prefix("usage_usec ") {
            c.cpu_seconds = v.trim().parse::<u64>().unwrap_or(0) as f64 / 1e6;
        }
    }
    c.memory_peak_bytes = read_u64(&dir.join("memory.peak")).unwrap_or(0);
    if let Ok(max) = std::fs::read_to_string(dir.join("cpu.max")) {
        let f: Vec<&str> = max.split_whitespace().collect();
        if f.len() == 2 && f[0] != "max" {
            let quota: f64 = f[0].parse().unwrap_or(0.0);
            let period: f64 = f[1].parse().unwrap_or(0.0);
            if period > 0.0 {
                c.cpu_limit_cores = quota / period;
            }
        }
    }
    Some(c)
}

/// The `/stats` body: this process's snapshot under `process`, merged with
/// `extra`'s fields.
pub fn stats_body(extra: serde_json::Map<String, serde_json::Value>) -> Vec<u8> {
    let mut body = serde_json::Map::new();
    body.insert(
        "process".into(),
        serde_json::to_value(read()).unwrap_or_default(),
    );
    body.extend(extra);
    let mut out = serde_json::to_vec(&body).unwrap_or_default();
    out.push(b'\n');
    out
}

#[cfg(test)]
#[global_allocator]
static COUNTING: alloc::CountingAllocator = alloc::CountingAllocator;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_has_runtime_figures() {
        let _keep = vec![0u8; 4096];
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(rx);
        let s = read();
        task.abort();
        assert!(
            s.cpus > 0 && s.tasks > 0 && s.heap_in_use_bytes > 0,
            "snapshot missing runtime figures: {s:?}"
        );
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(
                s.cpu_seconds > 0.0 && s.rss_bytes > 0,
                "snapshot missing process figures: {s:?}"
            );
        }
    }

    #[test]
    fn read_cgroup_files() {
        let dir = tempfile::tempdir().unwrap();
        let write =
            |name: &str, content: &str| std::fs::write(dir.path().join(name), content).unwrap();
        assert_eq!(read_cgroup(dir.path()), None, "empty directory");
        write(
            "cpu.stat",
            "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n",
        );
        // The root cgroup has cpu.stat but no memory.current: not a container.
        assert_eq!(read_cgroup(dir.path()), None, "no memory.current");
        write("memory.current", "104857600\n");
        write("memory.peak", "209715200\n");
        write("cpu.max", "150000 100000\n");
        let want = Cgroup {
            cpu_seconds: 2.5,
            memory_current_bytes: 100 << 20,
            memory_peak_bytes: 200 << 20,
            cpu_limit_cores: 1.5,
        };
        assert_eq!(read_cgroup(dir.path()), Some(want));
        write("cpu.max", "max 100000\n");
        assert_eq!(
            read_cgroup(dir.path()).unwrap().cpu_limit_cores,
            0.0,
            "unlimited cpu.max"
        );
    }
}
