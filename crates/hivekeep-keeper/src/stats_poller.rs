//! Samples the running actors' resource usage from every local worker and
//! publishes it aggregated by template, never by actor.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::worker::v1 as wpb;
use parking_lot::Mutex;
use prometheus::{CounterVec, IntGaugeVec, Opts, Registry};

use crate::context::Ctx;
use crate::sandbox_syncer::ClusterView;
use crate::worker_dialer::WorkerConnector;

/// The shortest sampling interval accepted.
pub const MIN_POLL_INTERVAL: Duration = Duration::from_secs(50);
/// Bounds one worker's stats call.
const STATS_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// Workers sampled at once.
const SWEEP_CONCURRENCY: usize = 8;

/// Clamps a positive interval up to the floor; zero disables sampling.
pub fn clamp_poll_interval(configured: Duration) -> Duration {
    if !configured.is_zero() && configured < MIN_POLL_INTERVAL {
        tracing::warn!(
            configured_secs = configured.as_secs(),
            clamped_secs = MIN_POLL_INTERVAL.as_secs(),
            "actor-stats-poll-interval below the floor; clamping"
        );
        return MIN_POLL_INTERVAL;
    }
    configured
}

/// A worker pool, by namespace and name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct PoolRef {
    /// The namespace.
    pub namespace: String,
    /// The name.
    pub name: String,
}

/// The dimensions one aggregate is kept by.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TemplateKey {
    /// The template's hive.
    pub template_hive: String,
    /// The template's name.
    pub template_name: String,
    /// The bounded sandbox class.
    pub sandbox_class: String,
    /// Where the sample came from.
    pub source: String,
    /// The worker's pool, when known.
    pub pool: PoolRef,
}

/// One template's totals in a sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Aggregate {
    /// Actors sampled.
    pub sampled_actors: i64,
    /// Current memory, page cache included.
    pub memory_current_bytes: i64,
    /// Memory working set.
    pub memory_working_set_bytes: i64,
    /// CPU time since the previous sweep.
    pub cpu_delta_usec: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Baseline {
    usec: u64,
    pod_uid: String,
}

/// Adds `v` to `agg`, saturating.
pub fn add_sat(agg: i64, v: u64) -> i64 {
    agg.saturating_add(v.min(i64::MAX as u64) as i64)
}

fn source_label(s: i32) -> &'static str {
    match wpb::StatsSource::try_from(s) {
        Ok(wpb::StatsSource::Cgroup) => "cgroup",
        Ok(wpb::StatsSource::GuestAgent) => "guest-agent",
        _ => "unspecified",
    }
}

/// The poller's instruments.
#[derive(Clone)]
pub struct StatsInstruments {
    sampled: IntGaugeVec,
    memory: IntGaugeVec,
    working_set: IntGaugeVec,
    cpu: CounterVec,
}

const STATS_LABELS: &[&str] = &[
    "hk_template_hive",
    "hk_template_name",
    "hk_sandbox_class",
    "stats_source",
    "worker_pool_namespace",
    "worker_pool_name",
];

impl StatsInstruments {
    /// Registers the instruments on `registry`.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let g = |n: &str, h: &str| IntGaugeVec::new(Opts::new(n, h), STATS_LABELS);
        let sampled = g(
            "hivekeep_actor_stats_sampled_actors",
            "Running actors with a current resource usage measurement.",
        )?;
        let memory = g(
            "hivekeep_actor_stats_memory_usage_bytes",
            "Current memory of running actors, page cache included.",
        )?;
        let working_set = g(
            "hivekeep_actor_stats_memory_working_set_bytes",
            "Memory working set of running actors.",
        )?;
        let cpu = CounterVec::new(
            Opts::new(
                "hivekeep_actor_stats_cpu_time_seconds_total",
                "CPU time consumed by running actors.",
            ),
            STATS_LABELS,
        )?;
        registry.register(Box::new(sampled.clone()))?;
        registry.register(Box::new(memory.clone()))?;
        registry.register(Box::new(working_set.clone()))?;
        registry.register(Box::new(cpu.clone()))?;
        Ok(StatsInstruments {
            sampled,
            memory,
            working_set,
            cpu,
        })
    }

    fn publish(&self, aggs: &HashMap<TemplateKey, Aggregate>) {
        self.sampled.reset();
        self.memory.reset();
        self.working_set.reset();
        for (k, a) in aggs {
            let l = [
                k.template_hive.as_str(),
                k.template_name.as_str(),
                k.sandbox_class.as_str(),
                k.source.as_str(),
                k.pool.namespace.as_str(),
                k.pool.name.as_str(),
            ];
            self.sampled.with_label_values(&l).set(a.sampled_actors);
            self.memory
                .with_label_values(&l)
                .set(a.memory_current_bytes);
            self.working_set
                .with_label_values(&l)
                .set(a.memory_working_set_bytes);
            self.cpu
                .with_label_values(&l)
                .inc_by(a.cpu_delta_usec as f64 / 1e6);
        }
    }
}

/// The poller.
pub struct StatsPoller {
    workers_dir: PathBuf,
    workers: Arc<dyn WorkerConnector>,
    cluster: Option<Arc<dyn ClusterView>>,
    inst: Option<StatsInstruments>,
    state: Mutex<PollState>,
    started_at: i64,
}

#[derive(Default)]
struct PollState {
    cached_pools: HashMap<String, PoolRef>,
    last_cpu: HashMap<(String, i64), Baseline>,
}

impl StatsPoller {
    /// A poller over the workers under `workers_dir`, labeling by pool from
    /// `cluster` when given.
    pub fn new(
        workers_dir: PathBuf,
        workers: Arc<dyn WorkerConnector>,
        cluster: Option<Arc<dyn ClusterView>>,
        inst: Option<StatsInstruments>,
    ) -> Self {
        StatsPoller {
            workers_dir,
            workers,
            cluster,
            inst,
            state: Mutex::new(PollState::default()),
            started_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        }
    }

    /// Sweeps every `interval` until `ctx` ends.
    pub async fn run(self: Arc<Self>, ctx: Ctx, interval: Duration) {
        tracing::info!(
            interval_secs = interval.as_secs(),
            "Actor stats poller starting"
        );
        loop {
            let aggs = self.collect(&ctx).await;
            if let Some(i) = &self.inst {
                i.publish(&aggs);
            }
            if ctx.sleep(interval).await.is_err() {
                return;
            }
        }
    }

    async fn resolve_pools(&self, pod_uids: &[String]) -> HashMap<String, PoolRef> {
        let Some(cluster) = &self.cluster else {
            return HashMap::new();
        };
        if pod_uids.is_empty() {
            self.state.lock().cached_pools.clear();
            return HashMap::new();
        }
        // A failed list answers from what was resolved before.
        let fresh: HashMap<String, PoolRef> = match cluster.worker_pods().await {
            Ok(pods) => pods
                .into_iter()
                .filter_map(|p| {
                    let name = p.pool.filter(|n| !n.is_empty())?;
                    Some((
                        p.uid,
                        PoolRef {
                            namespace: p.namespace,
                            name,
                        },
                    ))
                })
                .collect(),
            Err(err) => {
                tracing::debug!(%err, "Actor stats sweep: worker pool list failed; answering from cached resolutions");
                HashMap::new()
            }
        };
        let mut st = self.state.lock();
        let merged: HashMap<String, PoolRef> = pod_uids
            .iter()
            .filter_map(|u| {
                fresh
                    .get(u)
                    .or_else(|| st.cached_pools.get(u))
                    .map(|p| (u.clone(), p.clone()))
            })
            .collect();
        st.cached_pools = merged.clone();
        merged
    }

    /// One sweep: every worker's samples, aggregated by template. CPU is
    /// reported as the delta since an actor's previous sample in the same
    /// epoch; an actor first seen after the poller started counts from
    /// zero. A worker that did not answer keeps its actors' baselines.
    pub async fn collect(&self, ctx: &Ctx) -> HashMap<TemplateKey, Aggregate> {
        let Ok(rd) = std::fs::read_dir(&self.workers_dir) else {
            return HashMap::new();
        };
        let pod_uids: Vec<String> = rd
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        let present: HashSet<String> = pod_uids.iter().cloned().collect();
        let pools = self.resolve_pools(&pod_uids).await;
        let responses: Vec<(String, Option<wpb::GetActiveWorkloadStatsResponse>)> =
            futures::stream::iter(pod_uids.clone())
                .map(|uid| async move {
                    let call_ctx = ctx.with_timeout(STATS_RPC_TIMEOUT);
                    let resp = match self.workers.connect(&uid).await {
                        Ok(w) => w.get_active_workload_stats(&call_ctx).await.ok(),
                        Err(_) => None,
                    };
                    (uid, resp)
                })
                .buffer_unordered(SWEEP_CONCURRENCY)
                .collect()
                .await;

        let mut st = self.state.lock();
        let mut aggs: HashMap<TemplateKey, Aggregate> = HashMap::new();
        let mut seen: HashMap<(String, i64), Baseline> = HashMap::new();
        let mut answered = HashSet::new();
        for (pod, resp) in responses {
            let Some(resp) = resp else { continue };
            answered.insert(pod.clone());
            for s in resp.samples {
                let key = (s.actor_uid.clone(), s.epoch_unix_nano);
                if s.source == wpb::StatsSource::Unspecified as i32 {
                    // No measurement this time: keep the baseline.
                    if let Some(b) = st.last_cpu.get(&key) {
                        seen.insert(key, b.clone());
                    }
                    continue;
                }
                let tk = TemplateKey {
                    template_hive: s.actor_template_hive.clone(),
                    template_name: s.actor_template_name.clone(),
                    sandbox_class: hivekeep_core::attr::normalize_sandbox_class(&s.sandbox_class)
                        .to_string(),
                    source: source_label(s.source).into(),
                    pool: pools.get(&pod).cloned().unwrap_or_default(),
                };
                let agg = aggs.entry(tk).or_default();
                agg.sampled_actors += 1;
                agg.memory_current_bytes =
                    add_sat(agg.memory_current_bytes, s.memory_current_bytes);
                agg.memory_working_set_bytes =
                    add_sat(agg.memory_working_set_bytes, s.memory_working_set_bytes);
                let cpu = s.cpu_usage_usec;
                match st.last_cpu.get(&key) {
                    Some(last) if cpu > last.usec => {
                        agg.cpu_delta_usec = add_sat(agg.cpu_delta_usec, cpu - last.usec)
                    }
                    Some(_) => {}
                    None if key.1 > self.started_at => {
                        agg.cpu_delta_usec = add_sat(agg.cpu_delta_usec, cpu)
                    }
                    None => {}
                }
                seen.insert(
                    key,
                    Baseline {
                        usec: cpu,
                        pod_uid: pod.clone(),
                    },
                );
            }
        }
        for (k, b) in st.last_cpu.iter() {
            if !seen.contains_key(k)
                && present.contains(&b.pod_uid)
                && !answered.contains(&b.pod_uid)
            {
                seen.insert(k.clone(), b.clone());
            }
        }
        st.last_cpu = seen;
        aggs
    }
}

use futures::StreamExt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_dialer::WorkerApi;
    use async_trait::async_trait;
    use tonic::Status;

    struct Worker(Mutex<Vec<wpb::WorkloadStatsSample>>);

    #[async_trait]
    impl WorkerApi for Worker {
        async fn run_workload(
            &self,
            _: &Ctx,
            _: wpb::RunWorkloadRequest,
        ) -> Result<wpb::RunWorkloadResponse, Status> {
            unimplemented!()
        }
        async fn checkpoint_workload(
            &self,
            _: &Ctx,
            _: wpb::CheckpointWorkloadRequest,
        ) -> Result<wpb::CheckpointWorkloadResponse, Status> {
            unimplemented!()
        }
        async fn restore_workload(
            &self,
            _: &Ctx,
            _: wpb::RestoreWorkloadRequest,
        ) -> Result<wpb::RestoreWorkloadResponse, Status> {
            unimplemented!()
        }
        async fn terminate_workload(
            &self,
            _: &Ctx,
            _: wpb::TerminateWorkloadRequest,
        ) -> Result<(), Status> {
            unimplemented!()
        }
        async fn get_active_workload_stats(
            &self,
            _: &Ctx,
        ) -> Result<wpb::GetActiveWorkloadStatsResponse, Status> {
            Ok(wpb::GetActiveWorkloadStatsResponse {
                samples: self.0.lock().clone(),
            })
        }
    }

    struct Conn(Arc<Worker>);

    #[async_trait]
    impl WorkerConnector for Conn {
        async fn connect(&self, _: &str) -> Result<Arc<dyn WorkerApi>, Status> {
            Ok(self.0.clone())
        }
        fn socket_path(&self, _: &str) -> PathBuf {
            PathBuf::new()
        }
    }

    fn sample(uid: &str, epoch: i64, cpu: u64) -> wpb::WorkloadStatsSample {
        wpb::WorkloadStatsSample {
            actor_uid: uid.into(),
            actor_template_hive: "h".into(),
            actor_template_name: "t".into(),
            sandbox_class: "gvisor".into(),
            source: wpb::StatsSource::Cgroup as i32,
            memory_current_bytes: 100,
            memory_working_set_bytes: 60,
            cpu_usage_usec: cpu,
            epoch_unix_nano: epoch,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn aggregates_by_template_and_reports_cpu_deltas() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("pod-1")).unwrap();
        let worker = Arc::new(Worker(Mutex::new(vec![
            sample("a", 1, 1_000),
            sample("b", 1, 500),
        ])));
        let reg = Registry::new();
        let p = StatsPoller::new(
            d.path().to_path_buf(),
            Arc::new(Conn(worker.clone())),
            None,
            Some(StatsInstruments::new(&reg).unwrap()),
        );
        let ctx = Ctx::background();
        let aggs = p.collect(&ctx).await;
        assert_eq!(aggs.len(), 1);
        let a = aggs.values().next().unwrap();
        assert_eq!(a.sampled_actors, 2);
        assert_eq!(a.memory_current_bytes, 200);
        assert_eq!(
            a.cpu_delta_usec, 0,
            "epochs from before the poller started count no CPU"
        );
        *worker.0.lock() = vec![sample("a", 1, 3_000), sample("b", 1, 400)];
        let aggs = p.collect(&ctx).await;
        assert_eq!(
            aggs.values().next().unwrap().cpu_delta_usec,
            2_000,
            "a counter reset adds nothing"
        );
        p.inst.as_ref().unwrap().publish(&aggs);
        assert!(!reg.gather().is_empty());
    }

    #[test]
    fn helpers() {
        assert_eq!(add_sat(i64::MAX - 1, 5), i64::MAX);
        assert_eq!(add_sat(1, u64::MAX), i64::MAX);
        assert_eq!(
            clamp_poll_interval(Duration::from_secs(1)),
            MIN_POLL_INTERVAL
        );
        assert_eq!(clamp_poll_interval(Duration::ZERO), Duration::ZERO);
        assert_eq!(
            clamp_poll_interval(Duration::from_secs(120)),
            Duration::from_secs(120)
        );
    }
}

#[cfg(test)]
mod behavior_tests;
