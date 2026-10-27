//! The ladder: a scenario run at each rate in turn against a running tunnel
//! and sink, with the tunnel's own CPU and memory read per rung.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::egress_actor;
use super::modes::{self, Hook, Mode, Options, Targets};
use super::scenarios::{Scenario, IDLE_OPEN_RATE};
use super::stats::{Admin, ReadFn, Stats, Watch};
use crate::report::{Load, Report, Resources, Rung, Verdict};
use crate::{procstats, Error, Result};

/// How often the tunnel is polled for peaks during a rung.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

/// Shares of a process's CPU past which it counts as saturated: the
/// tunnel's, and the generator's or sink's, which then set the pace
/// instead of the tunnel.
pub const TUNNEL_SATURATED: f64 = 0.95;
pub const RIG_SATURATED: f64 = 0.85;

/// Runs scenarios against a running tunnel and sink.
pub struct Runner {
    pub targets: Targets,
    /// The tunnel's admin base URL.
    pub tunnel_admin: String,
    /// The sink's admin base URL.
    pub sink_admin: String,
    pub step: Duration,
    pub warmup: Duration,
    /// Caps units in flight.
    pub workers: usize,
    /// Shared with the worker container: the Unix sockets of the egress
    /// actors, which run egress scenarios inside the sandboxes, and of the
    /// ingress actors, which serve their counters.
    pub shared_dir: Option<PathBuf>,
    tunnel: Option<Admin>,
    sink: Option<Admin>,
}

impl Runner {
    /// A runner; call [`Runner::prepare`] before [`Runner::run`].
    pub fn new(
        targets: Targets,
        tunnel_admin: String,
        sink_admin: String,
        step: Duration,
        warmup: Duration,
        workers: usize,
        shared_dir: Option<PathBuf>,
    ) -> Self {
        Runner {
            targets,
            tunnel_admin,
            sink_admin,
            step,
            warmup,
            workers,
            shared_dir,
            tunnel: None,
            sink: None,
        }
    }

    fn tunnel(&self) -> Result<&Admin> {
        self.tunnel
            .as_ref()
            .ok_or_else(|| Error::msg("runner not prepared"))
    }

    fn sink(&self) -> Result<&Admin> {
        self.sink
            .as_ref()
            .ok_or_else(|| Error::msg("runner not prepared"))
    }

    /// Waits for both processes, learns the hosted Actors and their port
    /// from the tunnel, and sends a little traffic down every path, so a
    /// misconfiguration fails here rather than as a rung full of errors.
    pub async fn prepare(&mut self, cancel: &CancellationToken) -> Result<()> {
        let (tunnel, sink) = (
            Admin::http(&self.tunnel_admin)?,
            Admin::http(&self.sink_admin)?,
        );
        let deadline = Duration::from_secs(60);
        tunnel.wait_ready(deadline).await?;
        sink.wait_ready(deadline).await?;
        let st = tunnel.stats().await?;
        let info = st.tunnel.filter(|t| !t.sandboxes.is_empty()).ok_or_else(|| {
            Error::msg(format!("{}/stats does not describe a tunnel with hosted Actors; is it `hivekeep-tunnel-bench tunnel`?", self.tunnel_admin))
        })?;
        self.targets.actors = info.sandboxes.iter().map(|s| s.actor.clone()).collect();
        self.targets.actor_port = info.actor_port;
        self.tunnel = Some(tunnel);
        self.sink = Some(sink);
        // The worker places the egress and ingress actors in the sandboxes
        // once the tunnel hosts them, so they come up a moment after it.
        self.wait_for_sandbox_sockets(info.sandboxes.len(), deadline)
            .await?;

        // The stream check goes first so its bytes have drained through the
        // gateway before the first scenario reads the sink's counters.
        let checks = [
            Options {
                connections: 1,
                payload_bytes: 1024,
                ..Options::new(Mode::EgressStream)
            },
            Options {
                rate: 20.0,
                ..Options::new(Mode::IngressH1)
            },
            Options {
                rate: 20.0,
                ..Options::new(Mode::IngressH2)
            },
            Options {
                rate: 20.0,
                ..Options::new(Mode::Connect)
            },
            Options {
                rate: 20.0,
                ..Options::new(Mode::EgressHttp)
            },
        ];
        for mut check in checks {
            check.duration = Duration::from_millis(300);
            check.workers = 4;
            let l = self.execute(&check, cancel).await?;
            if l.errors > 0 || l.completed == 0 {
                return Err(Error::msg(format!(
                    "check of {} failed: {} completed, {} errors: {}",
                    check.mode.map_or("?", Mode::as_str),
                    l.completed,
                    l.errors,
                    l.error_samples.join("; ")
                )));
            }
        }
        Ok(())
    }

    fn socket(&self, kind: &str, i: usize) -> Option<PathBuf> {
        self.shared_dir
            .as_ref()
            .map(|d| d.join(format!("{kind}-actor-{i}.sock")))
    }

    /// The socket of the egress actor in sandbox `i`.
    pub fn egress_actor_socket(&self, i: usize) -> Option<PathBuf> {
        self.socket("egress", i)
    }

    fn ingress_actor_sockets(&self) -> Vec<PathBuf> {
        let Some(dir) = &self.shared_dir else {
            return Vec::new();
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut out: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("ingress-actor-") && n.ends_with(".sock"))
            })
            .collect();
        out.sort();
        out
    }

    /// Waits until every sandbox's ingress and egress actors answer on
    /// their sockets in the shared directory.
    async fn wait_for_sandbox_sockets(&self, sandboxes: usize, timeout: Duration) -> Result<()> {
        if self.shared_dir.is_none() {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + timeout;
        for i in 0..sandboxes {
            for socket in [self.socket("ingress", i), self.socket("egress", i)]
                .into_iter()
                .flatten()
            {
                loop {
                    match tokio::net::UnixStream::connect(&socket).await {
                        Ok(_) => break,
                        Err(e) if tokio::time::Instant::now() >= deadline => {
                            return Err(Error::msg(format!(
                                "waiting for {}: {e}",
                                socket.display()
                            )));
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                    }
                }
            }
        }
        Ok(())
    }

    /// Runs `s` at each rung and reports it. A rung where the tunnel stops
    /// answering ends the scenario; the runner then waits for the tunnel to
    /// come back, as a replaced one does, and fails with the report so far
    /// if it does not.
    pub async fn run(
        &self,
        s: &Scenario,
        rungs: &[f64],
        cancel: &CancellationToken,
    ) -> (Option<Report>, Result<()>) {
        let mut rep = Report {
            scenario: s.name.into(),
            description: s.description.into(),
            unit: s.unit.into(),
            step: self.step,
            warmup: if s.holds_connections {
                Duration::ZERO
            } else {
                self.warmup
            },
            ..Default::default()
        };
        // Bandwidth has no rate to offer; one rung is the measurement.
        let rungs = if s.streams > 0 {
            &rungs[..rungs.len().min(1)]
        } else {
            rungs
        };
        let before = match self.sink_stats().await {
            Ok(b) => b,
            Err(e) => return (None, Err(e)),
        };
        for &target in rungs {
            let rung = match self.rung(s, target, cancel).await {
                Ok(r) => r,
                Err(e) => {
                    return (
                        None,
                        Err(Error::msg(format!("{} at {target:.0}: {e}", s.name))),
                    )
                }
            };
            let down = rung.verdict == Verdict::Down;
            rep.rungs.push(rung);
            if down {
                let back = async {
                    self.tunnel()?.wait_ready(Duration::from_secs(120)).await?;
                    // The replacement's sandboxes get their own egress and
                    // ingress actors a moment after it is ready.
                    self.wait_for_sandbox_sockets(
                        self.targets.actors.len(),
                        Duration::from_secs(120),
                    )
                    .await
                };
                if let Err(e) = back.await {
                    let err = Error::msg(format!(
                        "the tunnel did not come back after {} at {target:.0}: {e}",
                        s.name
                    ));
                    return (Some(rep), Err(err));
                }
                break;
            }
        }
        match self.settle(cancel).await {
            Ok(after) => {
                rep.sink_counters = sink_delta(&before, &after);
                (Some(rep), Ok(()))
            }
            Err(e) => (None, Err(e)),
        }
    }

    /// Waits up to 10 s until every tunnel the sink saw has closed, so
    /// bytes still draining are counted in this scenario rather than the
    /// next, and returns the sink's counters.
    async fn settle(&self, cancel: &CancellationToken) -> Result<Stats> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let st = self.sink_stats().await?;
            if st.gateway.is_none_or(|g| g.tunnels_open == 0)
                || tokio::time::Instant::now() > deadline
            {
                return Ok(st);
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(Error::msg("canceled")),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    async fn rung(&self, s: &Scenario, target: f64, cancel: &CancellationToken) -> Result<Rung> {
        let mut opts = Options {
            rate: target,
            duration: self.step,
            warmup: self.warmup,
            workers: self.workers,
            new_connection_per_request: s.new_connection_per_request,
            ..Options::new(s.mode)
        };
        let live: Arc<Mutex<Option<Result<Stats>>>> = Arc::default();
        if s.holds_connections {
            // Open at a steady rate, stretching the rung if the count needs
            // it, then hold for at least half a step. Every open is measured.
            opts.connections = target as usize;
            opts.rate = IDLE_OPEN_RATE;
            opts.warmup = Duration::ZERO;
            opts.duration = self
                .step
                .max(Duration::from_secs_f64(target / IDLE_OPEN_RATE) + self.step / 2);
            let (tunnel, live) = (self.tunnel()?.clone(), live.clone());
            let hook: Hook = Arc::new(move || {
                let (tunnel, live) = (tunnel.clone(), live.clone());
                Box::pin(async move {
                    let r = tunnel.live_stats().await;
                    *live.lock() = Some(r);
                })
            });
            opts.while_holding = Some(hook);
        } else if s.streams > 0 {
            opts.connections = s.streams;
            opts.payload_bytes = 64 << 10;
            opts.rate = 0.0;
        }

        // The load stops early if the tunnel dies: against a replacement it
        // would only measure, or kill, that one.
        let stop_load = cancel.child_token();
        let load = {
            let (ex, opts, stop) = (self.executor(), opts.clone(), stop_load.clone());
            tokio::spawn(async move { ex.execute(&opts, &stop).await })
        };

        // The window opens when warmup ends.
        tokio::select! {
            _ = cancel.cancelled() => return Err(Error::msg("canceled")),
            _ = tokio::time::sleep(opts.warmup) => {}
        }
        let tunnel_watch = Watch::start(
            process_stats(self.tunnel()?.clone()),
            SAMPLE_INTERVAL,
            Some(stop_load.clone()),
        )
        .await;
        let sink_watch = Watch::start(self.sink_process_stats()?, SAMPLE_INTERVAL, None).await;
        let generator: ReadFn = match (
            opts.mode.is_some_and(Mode::is_egress),
            self.egress_actor_socket(0),
        ) {
            // Egress is generated by the egress actor in the sandbox.
            (true, Some(socket)) => process_stats(Admin::unix(socket)),
            _ => Arc::new(|| Box::pin(async { Ok(procstats::read()) })),
        };
        let self_watch = Watch::start(generator, SAMPLE_INTERVAL, None).await;

        let res = load
            .await
            .map_err(|e| Error::msg(format!("load task: {e}")))?;
        let completed = res.as_ref().map_or(0, |l| l.completed);
        let tunnel_res = tunnel_watch.finish(completed).await;
        let load = match res {
            Ok(l) => l,
            Err(e) if cancel.is_cancelled() || tunnel_res.is_ok() => return Err(e),
            // The load failed because the tunnel died: stopped by the watch,
            // or cut off when the worker replaced the egress actor with it.
            Err(_) => Load {
                offered: opts.rate,
                ..Default::default()
            },
        };
        let mut rung = Rung {
            load,
            ..Default::default()
        };
        if s.holds_connections {
            // The rung offers connections, and every open is a unit.
            rung.load.offered = target;
            rung.load.achieved = rung.load.completed as f64;
        }
        let sink_res = sink_watch.finish(rung.load.completed).await;
        let generator_res = self_watch.finish(rung.load.completed).await;
        // When the tunnel dies the worker replaces the egress and ingress
        // actors with it, so their readings fail too; only the tunnel's
        // failure is the result.
        let mut tunnel_err = tunnel_res.as_ref().err().map(ToString::to_string);
        if tunnel_err.is_none() {
            rung.sink = sink_res.map_err(|e| Error::msg(format!("reading sink stats: {e}")))?;
            rung.generator =
                generator_res.map_err(|e| Error::msg(format!("reading generator stats: {e}")))?;
        }
        let live = live.lock().take();
        if s.holds_connections && tunnel_err.is_none() {
            match &live {
                Some(Err(e)) => tunnel_err = Some(e.to_string()),
                None => tunnel_err = Some("no reading while the connections were held".into()),
                Some(Ok(_)) => {}
            }
        }
        if let Some(e) = tunnel_err {
            // The tunnel died or hung during the rung, as when it is killed
            // for going over its memory limit. That is a result.
            rung.sink = Resources::default();
            rung.generator = Resources::default();
            rung.verdict = Verdict::Down;
            rung.reason = format!("the tunnel stopped answering: {e}");
            return Ok(rung);
        }
        rung.tunnel = tunnel_res?;
        if let Some(Ok(live)) = live {
            rung.tunnel.heap_live_bytes = live.process.heap_in_use_bytes;
            rung.tunnel.tasks_live = live.process.tasks;
        }
        let (v, reason) = verdict(s, &rung);
        rung.verdict = v;
        rung.reason = reason;
        Ok(rung)
    }

    /// Runs `opts` here, or on the egress actors for egress modes.
    pub async fn execute(&self, opts: &Options, cancel: &CancellationToken) -> Result<Load> {
        self.executor().execute(opts, cancel).await
    }

    fn executor(&self) -> Executor {
        Executor {
            targets: self.targets.clone(),
            shared_dir: self.shared_dir.clone(),
        }
    }

    /// The gateway's counters and the ingress actors', summed.
    async fn sink_stats(&self) -> Result<Stats> {
        let mut st = self.sink()?.stats().await?;
        if self.shared_dir.is_none() {
            return Ok(st);
        }
        let mut total = crate::sink::IngressActorStats::default();
        for socket in self.ingress_actor_sockets() {
            // An ingress actor of a replaced tunnel; its successor's are read
            // under the same names.
            let Ok(Stats {
                ingress_actor: Some(w),
                ..
            }) = Admin::unix(socket).stats().await
            else {
                continue;
            };
            total.requests += w.requests;
            total.connections_seen += w.connections_seen;
            total.connections_open += w.connections_open;
        }
        st.ingress_actor = Some(total);
        Ok(st)
    }

    /// The CPU and memory of everything at the far end of the tunnel: the
    /// fake gateway and the ingress actors, summed.
    fn sink_process_stats(&self) -> Result<ReadFn> {
        let sink = self.sink()?.clone();
        let sockets = self.ingress_actor_sockets();
        Ok(Arc::new(move || {
            let (sink, sockets) = (sink.clone(), sockets.clone());
            Box::pin(async move {
                let mut sum = sink.stats().await?.process;
                for socket in sockets {
                    let Ok(w) = Admin::unix(socket).stats().await else {
                        continue;
                    };
                    sum.cpu_seconds += w.process.cpu_seconds;
                    sum.rss_bytes += w.process.rss_bytes;
                    sum.heap_in_use_bytes += w.process.heap_in_use_bytes;
                    sum.tasks += w.process.tasks;
                }
                Ok(sum)
            })
        }))
    }
}

fn process_stats(a: Admin) -> ReadFn {
    Arc::new(move || {
        let a = a.clone();
        Box::pin(async move { Ok(a.stats().await?.process) })
    })
}

/// What a run of a mode needs, detached from the runner so it can run as
/// its own task.
#[derive(Clone)]
struct Executor {
    targets: Targets,
    shared_dir: Option<PathBuf>,
}

impl Executor {
    fn egress_actor_socket(&self, i: usize) -> Option<PathBuf> {
        self.shared_dir
            .as_ref()
            .map(|d| d.join(format!("egress-actor-{i}.sock")))
    }

    async fn execute(&self, opts: &Options, cancel: &CancellationToken) -> Result<Load> {
        let mode = opts.mode.ok_or_else(|| Error::msg("no mode"))?;
        if !mode.is_egress() {
            return modes::execute(&self.targets, opts, cancel).await;
        }
        if self.shared_dir.is_none() {
            return Err(Error::msg(format!(
                "{} needs the egress actor in the shared directory",
                mode.as_str()
            )));
        }
        if mode == Mode::EgressIdle && self.targets.actors.len() > 1 {
            let sockets: Vec<PathBuf> = (0..self.targets.actors.len())
                .filter_map(|i| self.egress_actor_socket(i))
                .collect();
            return egress_actor::execute_on_all(&sockets, opts, cancel).await;
        }
        let socket = self
            .egress_actor_socket(0)
            .ok_or_else(|| Error::msg("no shared directory"))?;
        egress_actor::execute_on(&socket, opts, cancel).await
    }
}

/// The cores a process may use: its cgroup's limit when it has one, else
/// the CPUs it may run on.
pub fn cpu_limit(r: &Resources) -> f64 {
    if r.cgroup_cpu_limit_core > 0.0 {
        r.cgroup_cpu_limit_core
    } else {
        r.cpus.max(1) as f64
    }
}

/// Classifies a rung.
pub fn verdict(s: &Scenario, g: &Rung) -> (Verdict, String) {
    let l = &g.load;
    if l.errors > 0 {
        return (
            Verdict::Errors,
            format!("{} errors: {}", l.errors, l.error_samples.join("; ")),
        );
    }
    let limit = cpu_limit(&g.tunnel);
    if g.tunnel.cores >= TUNNEL_SATURATED * limit {
        return (
            Verdict::Saturated,
            format!("the tunnel at {:.2} of {limit} cores", g.tunnel.cores),
        );
    }
    for (name, res) in [("generator", &g.generator), ("sink", &g.sink)] {
        let limit = cpu_limit(res);
        if res.cores > RIG_SATURATED * limit {
            return (
                Verdict::RigLimited,
                format!("{name} at {:.1} of {limit} cores", res.cores),
            );
        }
    }
    // With the generator and sink idle enough, a backed-up queue means the
    // generator's workers were all waiting on the tunnel.
    if l.skipped > 0 {
        return (
            Verdict::Shortfall,
            format!("{} units found every worker busy", l.skipped),
        );
    }
    if l.send_lag.p99 > 10.0 {
        return (
            Verdict::Shortfall,
            format!("units waited for a worker, p99 {:.1}ms", l.send_lag.p99),
        );
    }
    if !s.holds_connections && s.streams == 0 && l.achieved < 0.97 * l.offered {
        return (
            Verdict::Shortfall,
            format!("achieved {:.0} of {:.0}", l.achieved, l.offered),
        );
    }
    (Verdict::Ok, String::new())
}

/// How far the sink's counters moved over a scenario. A counter that went
/// down was reset, by a replaced ingress actor, and is left out.
pub fn sink_delta(before: &Stats, after: &Stats) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let mut delta = |name: &str, b: u64, a: u64| {
        if a >= b {
            out.insert(name.to_string(), a - b);
        }
    };
    if let (Some(b), Some(a)) = (&before.ingress_actor, &after.ingress_actor) {
        delta("ingress_actor_requests", b.requests, a.requests);
        delta(
            "ingress_actor_connections",
            b.connections_seen,
            a.connections_seen,
        );
    }
    if let (Some(b), Some(a)) = (&before.gateway, &after.gateway) {
        delta("gateway_connections", b.connections, a.connections);
        delta("gateway_tunnels", b.tunnels, a.tunnels);
        delta("gateway_tunnels_h2", b.tunnels_h2, a.tunnels_h2);
        delta("gateway_requests", b.requests, a.requests);
        delta("gateway_bytes_in", b.bytes_in, a.bytes_in);
        delta("gateway_rejected", b.rejected, a.rejected);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::Percentiles;
    use crate::sink::GatewayStats;

    fn rung(load: Load, tunnel_cores: f64) -> Rung {
        Rung {
            load,
            tunnel: Resources {
                cores: tunnel_cores,
                cgroup_cpu_limit_core: 1.0,
                ..Default::default()
            },
            generator: Resources {
                cpus: 8,
                ..Default::default()
            },
            sink: Resources {
                cpus: 8,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn verdicts() {
        let s = crate::load::select("IngressH1").unwrap().remove(0);
        let ok = Load {
            offered: 100.0,
            achieved: 100.0,
            completed: 100,
            ..Default::default()
        };
        let cases = [
            (rung(ok.clone(), 0.5), Verdict::Ok),
            (
                rung(
                    Load {
                        errors: 1,
                        ..ok.clone()
                    },
                    0.5,
                ),
                Verdict::Errors,
            ),
            (rung(ok.clone(), 0.96), Verdict::Saturated),
            (
                rung(
                    Load {
                        skipped: 3,
                        ..ok.clone()
                    },
                    0.5,
                ),
                Verdict::Shortfall,
            ),
            (
                rung(
                    Load {
                        send_lag: Percentiles {
                            p99: 11.0,
                            ..Default::default()
                        },
                        ..ok.clone()
                    },
                    0.5,
                ),
                Verdict::Shortfall,
            ),
            (
                rung(
                    Load {
                        achieved: 90.0,
                        ..ok.clone()
                    },
                    0.5,
                ),
                Verdict::Shortfall,
            ),
        ];
        for (r, want) in cases {
            assert_eq!(verdict(&s, &r).0, want, "{r:?}");
        }
        let mut busy = rung(ok, 0.5);
        busy.generator.cores = 7.0;
        assert_eq!(verdict(&s, &busy).0, Verdict::RigLimited);
        // Connection scenarios do not compare achieved with offered.
        let idle = crate::load::select("EgressIdleConns").unwrap().remove(0);
        assert_eq!(
            verdict(
                &idle,
                &rung(
                    Load {
                        offered: 100.0,
                        achieved: 1.0,
                        ..Default::default()
                    },
                    0.1
                )
            )
            .0,
            Verdict::Ok
        );
    }

    #[test]
    fn cpu_limit_prefers_the_cgroup() {
        assert_eq!(
            cpu_limit(&Resources {
                cgroup_cpu_limit_core: 1.5,
                cpus: 8,
                ..Default::default()
            }),
            1.5
        );
        assert_eq!(
            cpu_limit(&Resources {
                cpus: 8,
                ..Default::default()
            }),
            8.0
        );
        assert_eq!(cpu_limit(&Resources::default()), 1.0);
    }

    #[test]
    fn sink_delta_skips_reset_counters() {
        let before = Stats {
            gateway: Some(GatewayStats {
                tunnels: 5,
                requests: 10,
                ..Default::default()
            }),
            ingress_actor: Some(crate::sink::IngressActorStats {
                requests: 50,
                ..Default::default()
            }),
            ..Default::default()
        };
        let after = Stats {
            gateway: Some(GatewayStats {
                tunnels: 8,
                requests: 30,
                ..Default::default()
            }),
            ingress_actor: Some(crate::sink::IngressActorStats {
                requests: 7,
                ..Default::default()
            }),
            ..Default::default()
        };
        let d = sink_delta(&before, &after);
        assert_eq!(d.get("gateway_tunnels"), Some(&3));
        assert_eq!(d.get("gateway_requests"), Some(&20));
        assert_eq!(
            d.get("ingress_actor_requests"),
            None,
            "a reset counter is left out"
        );
        assert_eq!(d.get("ingress_actor_connections"), Some(&0));
    }
}
