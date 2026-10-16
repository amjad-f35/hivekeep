//! A container's usage as the guest kernel accounts it, read out by the guest
//! agent. The same four numbers the host-cgroup runtime reports, because both
//! feed the same fields of a workload sample.

use crate::agentpb::grpc::CgroupStats;

/// The `memory.stat` keys of reclaimable file pages: cgroup v2's, and v1's
/// hierarchical total, which matches what v2's means.
const INACTIVE_FILE_KEYS: [&str; 2] = ["inactive_file", "total_inactive_file"];

/// One container's reading, or the sum of several. Anything the guest did not
/// report reads as zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sample {
    /// What the container's guest cgroup is charged, page cache included.
    pub memory_current_bytes: u64,
    /// The high-water mark of the above; zero when the guest kernel exposes
    /// none. Summed across containers it is an upper bound.
    pub memory_peak_bytes: u64,
    /// Current less reclaimable page cache, floored at zero: the figure to
    /// compare against a limit.
    pub memory_working_set_bytes: u64,
    /// Cumulative CPU time.
    pub cpu_usage_usec: u64,
}

impl Sample {
    /// Converts one container's accounting. It never fails: missing fields
    /// read as zero, and `None` (no accounting, normal for an exited
    /// container) is all zeros.
    pub fn from_cgroup_stats(cs: Option<&CgroupStats>) -> Sample {
        let mem = cs.and_then(|c| c.memory_stats.as_ref());
        let usage = mem.and_then(|m| m.usage.as_ref());
        let current = usage.map_or(0, |u| u.usage);
        // Saturating: usage and memory.stat are read a moment apart, so the
        // reclaimable figure can exceed the usage beside it.
        let inactive = mem.and_then(|m| {
            INACTIVE_FILE_KEYS
                .iter()
                .find_map(|k| m.stats.get(*k).copied())
        });
        Sample {
            memory_current_bytes: current,
            memory_peak_bytes: usage.map_or(0, |u| u.max_usage),
            memory_working_set_bytes: inactive.map_or(current, |i| current.saturating_sub(i)),
            // Nanoseconds on the wire; truncation loses under a microsecond
            // per reading of a cumulative counter.
            cpu_usage_usec: cs
                .and_then(|c| c.cpu_stats.as_ref())
                .and_then(|c| c.cpu_usage.as_ref())
                .map_or(0, |u| u.total_usage / 1000),
        }
    }

    /// The saturating sum of two samples.
    pub fn plus(self, o: Sample) -> Sample {
        Sample {
            memory_current_bytes: self
                .memory_current_bytes
                .saturating_add(o.memory_current_bytes),
            memory_peak_bytes: self.memory_peak_bytes.saturating_add(o.memory_peak_bytes),
            memory_working_set_bytes: self
                .memory_working_set_bytes
                .saturating_add(o.memory_working_set_bytes),
            cpu_usage_usec: self.cpu_usage_usec.saturating_add(o.cpu_usage_usec),
        }
    }
}

#[cfg(test)]
pub(crate) fn cgroup_stats(
    usage: u64,
    max_usage: u64,
    stats: &[(&str, u64)],
    cpu_nanos: u64,
) -> CgroupStats {
    use crate::agentpb::grpc as apb;
    CgroupStats {
        memory_stats: Some(apb::MemoryStats {
            usage: Some(apb::MemoryData {
                usage,
                max_usage,
                ..Default::default()
            }),
            stats: stats.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            ..Default::default()
        }),
        cpu_stats: Some(apb::CpuStats {
            cpu_usage: Some(apb::CpuUsage {
                total_usage: cpu_nanos,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agentpb::grpc as apb;

    fn s(c: u64, p: u64, w: u64, cpu: u64) -> Sample {
        Sample {
            memory_current_bytes: c,
            memory_peak_bytes: p,
            memory_working_set_bytes: w,
            cpu_usage_usec: cpu,
        }
    }

    #[test]
    fn converts_cgroup_stats() {
        let mem_only = CgroupStats {
            memory_stats: Some(apb::MemoryStats {
                usage: Some(apb::MemoryData {
                    usage: 4096,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cpu_only = CgroupStats {
            cpu_stats: Some(apb::CpuStats {
                cpu_usage: Some(apb::CpuUsage {
                    total_usage: 2000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cache_no_usage = CgroupStats {
            memory_stats: Some(apb::MemoryStats {
                stats: [("inactive_file".to_string(), 100)].into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cases: Vec<(&str, Option<CgroupStats>, Sample)> = vec![
            (
                "cgroup v2 guest",
                Some(cgroup_stats(
                    157286400,
                    209715200,
                    &[("inactive_file", 20971520)],
                    1234567000,
                )),
                s(157286400, 209715200, 136314880, 1234567),
            ),
            (
                "cgroup v1 guest reports total_inactive_file",
                Some(cgroup_stats(1000, 2000, &[("total_inactive_file", 400)], 0)),
                s(1000, 2000, 600, 0),
            ),
            (
                "both keys present prefers the v2 name",
                Some(cgroup_stats(
                    1000,
                    0,
                    &[("inactive_file", 100), ("total_inactive_file", 400)],
                    0,
                )),
                s(1000, 0, 900, 0),
            ),
            (
                "reclaimable cache above usage floors at zero",
                Some(cgroup_stats(1000, 0, &[("inactive_file", 4000)], 0)),
                s(1000, 0, 0, 0),
            ),
            (
                "equal usage and reclaimable cache floors at zero",
                Some(cgroup_stats(1000, 0, &[("inactive_file", 1000)], 0)),
                s(1000, 0, 0, 0),
            ),
            (
                "no memory.stat entries",
                Some(cgroup_stats(1000, 2000, &[], 0)),
                s(1000, 2000, 1000, 0),
            ),
            (
                "memory.stat without a reclaimable-cache entry",
                Some(cgroup_stats(1000, 0, &[("anon", 900)], 0)),
                s(1000, 0, 1000, 0),
            ),
            (
                "no peak reported",
                Some(cgroup_stats(1000, 0, &[("inactive_file", 100)], 5000)),
                s(1000, 0, 900, 5),
            ),
            (
                "sub-microsecond cpu time truncates",
                Some(cgroup_stats(0, 0, &[], 999)),
                Sample::default(),
            ),
            (
                "cpu time rounds down to whole microseconds",
                Some(cgroup_stats(0, 0, &[], 1999)),
                s(0, 0, 0, 1),
            ),
            (
                "memory reported without cpu",
                Some(mem_only),
                s(4096, 0, 4096, 0),
            ),
            ("cpu reported without memory", Some(cpu_only), s(0, 0, 0, 2)),
            ("nil cgroup stats", None, Sample::default()),
            (
                "empty cgroup stats",
                Some(CgroupStats::default()),
                Sample::default(),
            ),
            (
                "reclaimable cache without a usage message",
                Some(cache_no_usage),
                Sample::default(),
            ),
        ];
        for (name, cs, want) in cases {
            assert_eq!(Sample::from_cgroup_stats(cs.as_ref()), want, "{name}");
        }
    }

    #[test]
    fn plus_saturates() {
        let m = u64::MAX;
        assert_eq!(s(100, 200, 90, 10).plus(s(1, 2, 3, 4)), s(101, 202, 93, 14));
        assert_eq!(Sample::default().plus(s(7, 8, 9, 10)), s(7, 8, 9, 10));
        assert_eq!(s(m, m, m, m).plus(s(1, 2, 3, 4)), s(m, m, m, m));
    }
}
