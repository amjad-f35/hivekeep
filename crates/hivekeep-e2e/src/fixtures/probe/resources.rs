//! The compute envelope the Actor observes from inside its sandbox.
//!
//! - `num_cpu` is the CPU count available to this process: the vCPU count of
//!   a micro-VM guest, or what the gVisor sentry was provisioned with from
//!   the CPU quota, so it equals `ceil(limits.cpu)`.
//! - `mem_total_bytes` is `MemTotal` from `/proc/meminfo`: the memory the
//!   sandbox believes it has, bounded by `limits.memory`.
//! - `cpu_max` / `memory_max` are the raw cgroup v2 files, best effort.

use serde_json::{Map, Value};

/// Parses `MemTotal` (reported in kB) from `/proc/meminfo` text.
pub fn parse_mem_total(meminfo: &str) -> Option<i64> {
    let line = meminfo.lines().find_map(|l| l.strip_prefix("MemTotal:"))?;
    let kb: i64 = line.split_whitespace().next()?.parse().ok()?;
    Some(kb * 1024)
}

/// The `/resources` reply.
pub fn resources() -> Value {
    let mut resp = Map::new();
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    resp.insert("num_cpu".into(), cpus.into());
    match std::fs::read_to_string("/proc/meminfo") {
        Ok(text) => match parse_mem_total(&text) {
            Some(v) => {
                resp.insert("mem_total_bytes".into(), v.into());
            }
            None => {
                resp.insert(
                    "mem_total_error".into(),
                    "MemTotal not found in /proc/meminfo".into(),
                );
            }
        },
        Err(e) => {
            resp.insert("mem_total_error".into(), e.to_string().into());
        }
    }
    for (key, path) in [
        ("cpu_max", "/sys/fs/cgroup/cpu.max"),
        ("memory_max", "/sys/fs/cgroup/memory.max"),
    ] {
        if let Ok(v) = std::fs::read_to_string(path) {
            resp.insert(key.into(), v.trim().into());
        }
    }
    Value::Object(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mem_total_reads_kilobytes() {
        assert_eq!(
            parse_mem_total("MemFree: 1 kB\nMemTotal:       524288 kB\n"),
            Some(524288 * 1024)
        );
        assert_eq!(parse_mem_total("MemFree: 1 kB\n"), None);
        assert_eq!(parse_mem_total("MemTotal:\n"), None);
    }

    #[test]
    fn resources_reports_cpus() {
        assert!(resources()["num_cpu"].as_u64().is_some_and(|n| n >= 1));
    }
}
