//! Reading the benchmark processes' `/stats`, and watching one over a
//! measured window.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use http::StatusCode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::procstats::Snapshot;
use crate::report::Resources;
use crate::sink::{GatewayStats, IngressActorStats};
use crate::{admin, Error, Result};

/// What a benchmark process's `/stats` serves. Each process fills its own
/// fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stats {
    #[serde(default)]
    pub process: Snapshot,
    /// Set by the tunnel process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<TunnelInfo>,
    /// Set by the ingress actors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_actor: Option<IngressActorStats>,
    /// Set by the sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<GatewayStats>,
}

/// The part of the tunnel's hosted Actors the generator uses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TunnelInfo {
    #[serde(default)]
    pub sandboxes: Vec<SandboxRef>,
    #[serde(default)]
    pub actor_port: u16,
}

/// One hosted Actor, as the generator addresses it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxRef {
    /// `hive/name`, the value of the routing header.
    pub actor: String,
}

/// A benchmark process's admin endpoint.
#[derive(Debug, Clone)]
pub struct Admin(pub admin::Client);

impl Admin {
    /// The endpoint at `http://host:port`.
    pub fn http(base: &str) -> Result<Admin> {
        Ok(Admin(admin::Client::http(base)?))
    }

    /// The endpoint served on a Unix socket.
    pub fn unix(socket: impl Into<std::path::PathBuf>) -> Admin {
        Admin(admin::Client::unix(socket))
    }

    /// `/stats`.
    pub async fn stats(&self) -> Result<Stats> {
        self.get("/stats").await
    }

    /// `/stats` while a connection scenario holds its connections.
    pub async fn live_stats(&self) -> Result<Stats> {
        self.get("/stats?gc=1").await
    }

    async fn get(&self, path: &str) -> Result<Stats> {
        let (status, body) = self.0.get(path).await?;
        if status != StatusCode::OK {
            return Err(Error::msg(format!(
                "{}/stats: HTTP {}",
                self.0.base,
                status.as_u16()
            )));
        }
        Ok(serde_json::from_slice(&body)?)
    }

    /// Polls `/healthz` until it answers or `timeout` passes.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        self.0
            .wait_ready(tokio::time::Instant::now() + timeout)
            .await
    }
}

/// Reads one process snapshot.
pub type ReadFn = Arc<dyn Fn() -> BoxFuture<'static, Result<Snapshot>> + Send + Sync>;

/// How many reads in a row must fail before a watched process counts as
/// gone.
pub const FAILURES_TO_GIVE_UP: u32 = 2;

#[derive(Default)]
struct WatchState {
    peak: Snapshot,
    err: Option<String>,
}

/// Samples a process over a measured window: a snapshot at the start, at
/// the end, and every interval between, for peaks.
pub struct Watch {
    read: ReadFn,
    start: Snapshot,
    state: Arc<Mutex<WatchState>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

fn restarted(s: &Snapshot) -> String {
    format!("process restarted at {}", s.started_at.format("%H:%M:%S"))
}

fn observe(peak: &mut Snapshot, s: &Snapshot) {
    peak.rss_bytes = peak.rss_bytes.max(s.rss_bytes);
    peak.heap_in_use_bytes = peak.heap_in_use_bytes.max(s.heap_in_use_bytes);
    peak.tasks = peak.tasks.max(s.tasks);
    if let Some(c) = &s.cgroup {
        let p = peak.cgroup.get_or_insert_with(|| c.clone());
        p.memory_current_bytes = p.memory_current_bytes.max(c.memory_current_bytes);
    }
}

impl Watch {
    /// Takes the first snapshot and samples every `interval` after. When
    /// the process stops answering, cancels `on_gone`, if set.
    pub async fn start(
        read: ReadFn,
        interval: Duration,
        on_gone: Option<CancellationToken>,
    ) -> Watch {
        let first = read().await;
        let state = Arc::new(Mutex::new(WatchState::default()));
        let start = match first {
            Ok(s) => s,
            Err(e) => {
                state.lock().err = Some(e.to_string());
                Snapshot::default()
            }
        };
        state.lock().peak = start.clone();
        let stop = CancellationToken::new();
        let task = {
            let (read, state, stop, started) =
                (read.clone(), state.clone(), stop.clone(), start.started_at);
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
                let mut failures = 0;
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = ticker.tick() => {}
                    }
                    let mut r = read().await.map_err(|e| e.to_string());
                    if let Ok(s) = &r {
                        if s.started_at != started {
                            r = Err(restarted(s));
                            failures = FAILURES_TO_GIVE_UP - 1;
                        }
                    }
                    {
                        let mut st = state.lock();
                        match r {
                            Ok(s) => {
                                observe(&mut st.peak, &s);
                                failures = 0;
                            }
                            Err(e) => {
                                st.err = Some(e);
                                failures += 1;
                            }
                        }
                    }
                    if failures == FAILURES_TO_GIVE_UP {
                        if let Some(g) = &on_gone {
                            g.cancel();
                        }
                    }
                }
            })
        };
        Watch {
            read,
            start,
            state,
            stop,
            task,
        }
    }

    /// Ends the window and returns the process's cost over it, with CPU
    /// spread over `units`.
    pub async fn finish(self, units: u64) -> Result<Resources> {
        self.stop.cancel();
        let _ = self.task.await;
        let mut end = (self.read)().await;
        if let Ok(s) = &end {
            if s.started_at != self.start.started_at {
                end = Err(Error::msg(restarted(s)));
            }
        }
        let end = end?;
        let mut st = self.state.lock();
        if let Some(e) = &st.err {
            return Err(Error::msg(e.clone()));
        }
        observe(&mut st.peak, &end);
        let elapsed = (end.time - self.start.time).num_microseconds().unwrap_or(0) as f64 / 1e6;
        let mut res = Resources {
            cpu_seconds: end.cpu_seconds - self.start.cpu_seconds,
            cpus: end.cpus,
            rss_end_bytes: end.rss_bytes,
            rss_max_bytes: st.peak.rss_bytes,
            rss_is_peak: end.rss_is_peak,
            heap_end_bytes: end.heap_in_use_bytes,
            heap_max_bytes: st.peak.heap_in_use_bytes,
            tasks_max: st.peak.tasks,
            ..Default::default()
        };
        if elapsed > 0.0 {
            res.cores = res.cpu_seconds / elapsed;
        }
        if units > 0 {
            res.micros_per_unit = res.cpu_seconds / units as f64 * 1e6;
        }
        if let (Some(e), Some(s)) = (&end.cgroup, &self.start.cgroup) {
            if elapsed > 0.0 {
                res.cgroup_cores = (e.cpu_seconds - s.cpu_seconds) / elapsed;
            }
            res.cgroup_mem_end_bytes = e.memory_current_bytes;
            res.cgroup_mem_max_bytes = st
                .peak
                .cgroup
                .as_ref()
                .map_or(0, |c| c.memory_current_bytes);
            res.cgroup_cpu_limit_core = e.cpu_limit_cores;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::procstats::Cgroup;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn snap(t: i64, cpu: f64, rss: u64, mem: u64) -> Snapshot {
        let base = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        Snapshot {
            time: base + chrono::Duration::seconds(t),
            started_at: base,
            cpu_seconds: cpu,
            rss_bytes: rss,
            cpus: 2,
            cgroup: Some(Cgroup {
                cpu_seconds: cpu,
                memory_current_bytes: mem,
                cpu_limit_cores: 1.0,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn watch_reports_cost_and_peaks() {
        let n = Arc::new(AtomicU64::new(0));
        let read: ReadFn = Arc::new(move || {
            let i = n.fetch_add(1, Ordering::SeqCst) as i64;
            // Start, one peak sample, then the end.
            let s = match i {
                0 => snap(0, 1.0, 10, 100),
                1 => snap(1, 1.5, 50, 500),
                _ => snap(2, 2.0, 20, 200),
            };
            Box::pin(async move { Ok(s) })
        });
        let w = Watch::start(read, Duration::from_millis(20), None).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let r = w.finish(1000).await.unwrap();
        assert_eq!(
            (r.cpu_seconds, r.cores, r.micros_per_unit),
            (1.0, 0.5, 1000.0)
        );
        assert_eq!(
            (
                r.rss_end_bytes,
                r.rss_max_bytes,
                r.cgroup_mem_max_bytes,
                r.cgroup_cpu_limit_core
            ),
            (20, 50, 500, 1.0)
        );
    }

    #[tokio::test]
    async fn watch_notices_a_replaced_process() {
        let gone = CancellationToken::new();
        let n = Arc::new(AtomicU64::new(0));
        let read: ReadFn = Arc::new(move || {
            let mut s = snap(0, 0.0, 0, 0);
            if n.fetch_add(1, Ordering::SeqCst) > 0 {
                s.started_at += chrono::Duration::seconds(5);
            }
            Box::pin(async move { Ok(s) })
        });
        let w = Watch::start(read, Duration::from_millis(10), Some(gone.clone())).await;
        tokio::time::timeout(Duration::from_secs(2), gone.cancelled())
            .await
            .expect("on_gone not called");
        assert!(w
            .finish(1)
            .await
            .unwrap_err()
            .to_string()
            .contains("restarted"));
    }
}
