//! What a benchmark run measured, and how it is printed.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::durfmt;
use crate::Result;

/// A latency distribution, in milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Percentiles {
    #[serde(rename = "p50_ms")]
    pub p50: f64,
    #[serde(rename = "p90_ms")]
    pub p90: f64,
    #[serde(rename = "p99_ms")]
    pub p99: f64,
    #[serde(rename = "max_ms")]
    pub max: f64,
    #[serde(rename = "mean_ms")]
    pub mean: f64,
}

impl Percentiles {
    /// The worse of two distributions, field by field.
    pub fn worst(self, o: Percentiles) -> Percentiles {
        Percentiles {
            p50: self.p50.max(o.p50),
            p90: self.p90.max(o.p90),
            p99: self.p99.max(o.p99),
            max: self.max.max(o.max),
            mean: self.mean.max(o.mean),
        }
    }
}

/// What the generator saw during one rung.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Load {
    /// The target rate per second, or for a connection scenario the number
    /// of connections to hold.
    pub offered: f64,
    /// The window after warmup that everything covers.
    pub measured_seconds: f64,
    pub scheduled: u64,
    /// Units that never left the generator: its queue was full.
    pub skipped: u64,
    pub completed: u64,
    pub errors: u64,
    /// Completed units per measured second.
    pub achieved: f64,
    pub latency: Percentiles,
    /// How late units left the generator.
    pub send_lag: Percentiles,
    pub bytes: u64,
    pub mbytes_per_sec: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_samples: Vec<String>,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_zero_usize(v: &usize) -> bool {
    *v == 0
}
fn is_false(v: &bool) -> bool {
    !*v
}

/// One process's cost over the measured window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_seconds: f64,
    pub cores: f64,
    /// CPU spread over the completed units.
    #[serde(rename = "cpu_us_per_unit")]
    pub micros_per_unit: f64,
    /// Cores the process may run on when it has no cgroup CPU limit.
    pub cpus: usize,

    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub rss_end_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub rss_max_bytes: u64,
    /// The platform reports only the process's lifetime peak RSS.
    #[serde(default, skip_serializing_if = "is_false")]
    pub rss_is_peak: bool,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub heap_end_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub heap_max_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub tasks_max: usize,
    /// Heap and tasks read while a connection scenario holds its
    /// connections.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub heap_live_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub tasks_live: usize,

    /// Set when the process runs in a cgroup v2 container.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub cgroup_cores: f64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub cgroup_mem_end_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub cgroup_mem_max_bytes: u64,
    #[serde(
        rename = "cgroup_cpu_limit_cores",
        default,
        skip_serializing_if = "is_zero_f64"
    )]
    pub cgroup_cpu_limit_core: f64,
}

/// How a rung went.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    #[default]
    #[serde(rename = "OK")]
    Ok,
    /// The generator or sink, not the tunnel, set the pace; the tunnel's
    /// numbers at that rung are a floor.
    #[serde(rename = "RIG-LIMITED")]
    RigLimited,
    #[serde(rename = "ERRORS")]
    Errors,
    /// The tunnel used all the CPU it may: the rung is past its capacity.
    #[serde(rename = "SATURATED")]
    Saturated,
    /// The tunnel stopped answering during the rung, as when it is killed
    /// for going over its memory limit.
    #[serde(rename = "DOWN")]
    Down,
    /// The tunnel fell behind the offered load, by completing fewer units
    /// or backing up the generator's queue, without errors and below its
    /// CPU limit.
    #[serde(rename = "SHORTFALL")]
    Shortfall,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Verdict::Ok => "OK",
            Verdict::RigLimited => "RIG-LIMITED",
            Verdict::Errors => "ERRORS",
            Verdict::Saturated => "SATURATED",
            Verdict::Down => "DOWN",
            Verdict::Shortfall => "SHORTFALL",
        })
    }
}

/// One offered rate of a scenario.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Rung {
    pub load: Load,
    /// The tunnel's own cost: process B.
    pub tunnel: Resources,
    /// Processes A and C, reported so a saturated one is visible.
    pub generator: Resources,
    pub sink: Resources,
    pub verdict: Verdict,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

mod nanos {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_nanos() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_nanos(u64::deserialize(d)?))
    }
}

/// One scenario across its rungs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub scenario: String,
    pub description: String,
    pub unit: String,
    #[serde(rename = "step_ns", with = "nanos")]
    pub step: Duration,
    #[serde(rename = "warmup_ns", with = "nanos")]
    pub warmup: Duration,
    pub rungs: Vec<Rung>,
    /// The ingress actors' and gateway's counters over the scenario, for
    /// checking against the generator.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sink_counters: BTreeMap<String, u64>,
}

fn ms_string(v: f64) -> String {
    if v >= 100.0 {
        format!("{v:.0}ms")
    } else {
        format!("{v:.2}ms")
    }
}

fn rss_string(r: &Resources) -> String {
    let mut s = mib(r.rss_max_bytes);
    if r.rss_is_peak {
        s.push('*');
    }
    s
}

/// Bytes as mebibytes.
pub fn mib(b: u64) -> String {
    format!("{:.1}MiB", b as f64 / (1u64 << 20) as f64)
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut b = String::new();
        let _ = writeln!(b, "=== {}: {}", self.scenario, self.description);
        let _ = writeln!(
            b,
            "    unit: {}; {} per rung, first {} not measured",
            self.unit,
            durfmt::format(self.step),
            durfmt::format(self.warmup)
        );
        let cgroup = self.rungs.iter().any(|g| g.tunnel.cgroup_mem_end_bytes > 0);
        let mut header = format!(
            "{:>9} {:>9} {:>8} {:>8} {:>8} {:>6} | {:>6} {:>7} {:>9} {:>9} {:>6}",
            "offered",
            "achieved",
            "p50",
            "p99",
            "max",
            "errs",
            "cores",
            "µs/unit",
            "rss-max",
            "heap-max",
            "tasks"
        );
        if cgroup {
            let _ = write!(header, " {:>8} {:>9}", "cg-cores", "cg-mem");
        }
        let _ = write!(header, " | {:>7} {:>7}  verdict", "A cores", "C cores");
        let _ = writeln!(b, "    {header}");
        for g in &self.rungs {
            let (l, t) = (&g.load, &g.tunnel);
            let offered = if l.offered > 0.0 {
                format!("{:.0}", l.offered)
            } else {
                "-".into()
            };
            let mut line = format!(
                "{:>9} {:>9.0} {:>8} {:>8} {:>8} {:>6} | {:>6.2} {:>7.0} {:>9} {:>9} {:>6}",
                offered,
                l.achieved,
                ms_string(l.latency.p50),
                ms_string(l.latency.p99),
                ms_string(l.latency.max),
                l.errors,
                t.cores,
                t.micros_per_unit,
                rss_string(t),
                mib(t.heap_max_bytes),
                t.tasks_max
            );
            if cgroup {
                let _ = write!(
                    line,
                    " {:>8.2} {:>9}",
                    t.cgroup_cores,
                    mib(t.cgroup_mem_max_bytes)
                );
            }
            let _ = write!(
                line,
                " | {:>7.2} {:>7.2}  {}",
                g.generator.cores, g.sink.cores, g.verdict
            );
            if !g.reason.is_empty() {
                let _ = write!(line, " ({})", g.reason);
            }
            let _ = writeln!(b, "    {line}");
            if l.mbytes_per_sec > 0.0 && self.unit == "write" {
                let _ = writeln!(b, "    {:>9} throughput {:.0} MB/s", "", l.mbytes_per_sec);
            }
            if t.heap_live_bytes > 0 && l.completed > 0 {
                let _ = writeln!(
                    b,
                    "    {:>9} holding {}: live heap {}, {:.1} KiB and {:.1} tasks per connection",
                    "",
                    l.completed,
                    mib(t.heap_live_bytes),
                    t.heap_live_bytes as f64 / l.completed as f64 / 1024.0,
                    t.tasks_live as f64 / l.completed as f64
                );
            }
        }
        let columns = if cgroup {
            "cores..cg-mem"
        } else {
            "cores..tasks"
        };
        let _ = writeln!(
            b,
            "    {columns} are the tunnel alone (process B); A and C are the traffic generator and the far end"
        );
        if self.rungs.first().is_some_and(|r| r.tunnel.rss_is_peak) {
            let _ = writeln!(
                b,
                "    rss-max*: this platform reports only the tunnel's lifetime peak RSS"
            );
        }
        if !self.sink_counters.is_empty() {
            let counters = serde_json::to_string(&self.sink_counters).unwrap_or_default();
            let _ = writeln!(b, "    sink counters: {counters}");
        }
        f.write_str(&b)
    }
}

/// Saves `reports` to `path` as indented JSON.
pub fn write(path: &Path, reports: &[Report]) -> Result<()> {
    let data = serde_json::to_vec_pretty(reports)?;
    std::fs::write(path, data)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Report {
        Report {
            scenario: "EgressIdleConns".into(),
            description: "idle".into(),
            unit: "held connection".into(),
            step: Duration::from_secs(10),
            rungs: vec![
                Rung {
                    load: Load {
                        offered: 1000.0,
                        achieved: 1000.0,
                        completed: 1000,
                        latency: Percentiles {
                            p50: 2.1,
                            p99: 6.5,
                            max: 150.0,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    tunnel: Resources {
                        cores: 0.12,
                        micros_per_unit: 1238.0,
                        heap_max_bytes: 156 << 20,
                        heap_live_bytes: 155 << 20,
                        tasks_live: 5100,
                        cgroup_mem_end_bytes: 223 << 20,
                        cgroup_mem_max_bytes: 223 << 20,
                        ..Default::default()
                    },
                    verdict: Verdict::Ok,
                    ..Default::default()
                },
                Rung {
                    load: Load {
                        offered: 8000.0,
                        ..Default::default()
                    },
                    verdict: Verdict::Down,
                    reason: "the tunnel stopped answering".into(),
                    ..Default::default()
                },
            ],
            sink_counters: BTreeMap::from([("gateway_tunnels".to_string(), 1000)]),
            ..Default::default()
        }
    }

    #[test]
    fn string() {
        let out = sample().to_string();
        for want in [
            "=== EgressIdleConns: idle",
            "cg-cores", // shown because a rung has cgroup figures
            "2.10ms",
            "150ms",
            "holding 1000: live heap 155.0MiB, 158.7 KiB and 5.1 tasks per connection",
            "DOWN (the tunnel stopped answering)",
            r#"sink counters: {"gateway_tunnels":1000}"#,
        ] {
            assert!(out.contains(want), "report lacks {want:?}:\n{out}");
        }
    }

    #[test]
    fn write_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("results.json");
        write(&path, &[sample()]).unwrap();
        let got: Vec<Report> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].rungs.len(), 2);
        assert_eq!(got[0].rungs[1].verdict, Verdict::Down);
        assert_eq!(got[0], sample());
    }
}
