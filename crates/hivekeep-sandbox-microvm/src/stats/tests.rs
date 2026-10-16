use super::*;
use std::sync::atomic::{AtomicI32, Ordering};

use async_trait::async_trait;
use hivekeep_core::resource::ActorRef;
use parking_lot::Mutex;

use crate::activation::Activation;
use crate::agent_stats::cgroup_stats;
use crate::agentpb::grpc::CgroupStats;
use crate::kata::agent::{AgentError, ContainerStatsReader};
use crate::service::ServiceConfig;
use crate::ttrpc::TtrpcError;
use crate::usage::{source, RecordingSink};

pub(crate) const OTHER_ACTOR_UID: &str = "bbbbbbbb-0000-4000-8000-000000000002";

pub(crate) fn test_actor() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("space-a", "actor-a"),
        uid: "aaaaaaaa-0000-4000-8000-000000000001".into(),
        template_hive: "ns-a".into(),
        template_name: "template-a".into(),
    }
}

pub(crate) type Hook = Arc<dyn Fn() + Send + Sync>;

/// A guest agent answering StatsContainer from a table.
#[derive(Default)]
pub(crate) struct FakeAgent {
    pub stats: Mutex<HashMap<String, Option<CgroupStats>>>,
    pub errs: Mutex<HashMap<String, String>>,
    pub on_call: Mutex<Option<Hook>>,
    pub calls: Mutex<Vec<String>>,
    /// Ids whose call never returns.
    pub block: Mutex<Vec<String>>,
}

impl FakeAgent {
    pub fn with(stats: &[(&str, Option<CgroupStats>)]) -> Arc<FakeAgent> {
        let a = FakeAgent::default();
        *a.stats.lock() = stats
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        Arc::new(a)
    }

    pub fn set(&self, id: &str, cs: CgroupStats) {
        self.stats.lock().insert(id.into(), Some(cs));
    }

    pub fn calls(&self) -> usize {
        self.calls.lock().len()
    }
}

#[async_trait]
impl ContainerStatsReader for FakeAgent {
    async fn stats_container(
        &self,
        id: &str,
    ) -> std::result::Result<Option<CgroupStats>, AgentError> {
        // Called without the lock held, so hooks can block concurrently.
        let hook = self.on_call.lock().clone();
        if let Some(f) = hook {
            f();
        }
        self.calls.lock().push(id.into());
        if self.block.lock().iter().any(|b| b == id) {
            futures::future::pending::<()>().await;
        }
        if let Some(m) = self.errs.lock().get(id) {
            return Err(AgentError {
                method: "StatsContainer",
                source: TtrpcError::Codec(m.clone()),
            });
        }
        Ok(self.stats.lock().get(id).cloned().flatten())
    }
}

pub(crate) fn stats(usage: u64, peak: u64, inactive: u64, cpu_nanos: u64) -> Option<CgroupStats> {
    Some(cgroup_stats(
        usage,
        peak,
        &[("inactive_file", inactive)],
        cpu_nanos,
    ))
}

/// An activation that has passed its initial reading.
pub(crate) fn running(a: Activation) -> Activation {
    a.initial(None, |_| {});
    a
}

pub(crate) fn test_activation() -> Activation {
    running(Activation::new(SystemTime::UNIX_EPOCH, false))
}

pub(crate) fn target(uid: &str, agent: &Arc<FakeAgent>, ids: &[&str]) -> Arc<GuestStatsTarget> {
    Arc::new(GuestStatsTarget {
        actor_uid: uid.into(),
        agent: agent.clone(),
        workload_ids: ids.iter().map(|s| s.to_string()).collect(),
    })
}

pub(crate) fn stats_service(
    agent: &Arc<FakeAgent>,
    ids: &[&str],
) -> (Arc<WorkerService>, Arc<RecordingSink>) {
    let rec = Arc::new(RecordingSink::default());
    let s = WorkerService::new(ServiceConfig {
        usage: Some(rec.clone()),
        ..Default::default()
    });
    s.host_for_test(
        test_actor(),
        Some(target(&test_actor().uid, agent, ids)),
        test_activation(),
    );
    (s, rec)
}

/// Re-hosts the test Actor with `a`, keeping its stats target.
pub(crate) fn set_activation(s: &WorkerService, a: Activation) -> Arc<HostedActor> {
    let g = s
        .lookup_actor(&test_actor().uid)
        .and_then(|h| s.guest_of(&h));
    s.host_for_test(test_actor(), g, a)
}

fn req(uid: &str) -> pb::GetWorkloadStatsRequest {
    pb::GetWorkloadStatsRequest {
        actor_uid: uid.into(),
    }
}

async fn sweep_and_list(s: &Arc<WorkerService>) -> pb::GetActiveWorkloadStatsResponse {
    s.sweep_usage().await;
    s.get_active_workload_stats()
}

fn pending_for(a: &ActorAttribution) -> pb::WorkloadStatsSample {
    pb::WorkloadStatsSample {
        hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        actor_template_hive: a.template_hive.clone(),
        actor_template_name: a.template_name.clone(),
        sandbox_class: "microvm".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_deadline_mid_read_fails_the_sum() {
    let agent = FakeAgent::with(&[("a", stats(1000, 2000, 100, 5000))]);
    agent.block.lock().push("b".into());
    let t = target("", &agent, &["a", "b"]);
    let err = sum_container_stats(&t, Some(Instant::now() + Duration::from_millis(20)))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DeadlineExceeded), "{err}");
}

#[tokio::test]
async fn reads_the_guest() {
    let agent = FakeAgent::with(&[("app_ovl", stats(157286400, 209715200, 20971520, 1234567000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let before = unix_nanos(SystemTime::now());
    let mut got = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap()
        .sample
        .unwrap();
    let after = unix_nanos(SystemTime::now());
    assert!(got.observed_at_unix_nano >= before && got.observed_at_unix_nano <= after);
    got.observed_at_unix_nano = 0;
    let a = test_actor();
    assert_eq!(
        got,
        pb::WorkloadStatsSample {
            source: pb::StatsSource::GuestAgent as i32,
            memory_current_bytes: 157286400,
            memory_peak_bytes: 209715200,
            memory_working_set_bytes: 136314880,
            cpu_usage_usec: 1234567,
            ..pending_for(&a)
        }
    );
    assert_eq!(agent.calls(), 1);
}

#[tokio::test]
async fn sums_the_containers() {
    let agent = FakeAgent::with(&[
        ("app_ovl", stats(1000, 4000, 100, 7000)),
        ("sidecar_ovl", stats(500, 800, 200, 3000)),
    ]);
    let (s, _) = stats_service(&agent, &["app_ovl", "sidecar_ovl"]);
    let got = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap()
        .sample
        .unwrap();
    assert_eq!(
        (
            got.memory_current_bytes,
            got.memory_peak_bytes,
            got.memory_working_set_bytes,
            got.cpu_usage_usec
        ),
        (1500, 4800, 1200, 10)
    );
    assert_eq!(*agent.calls.lock(), ["app_ovl", "sidecar_ovl"]);
}

#[tokio::test]
async fn skips_an_unreadable_container() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    agent
        .errs
        .lock()
        .insert("exited_ovl".into(), "no such container".into());
    let (s, _) = stats_service(&agent, &["app_ovl", "exited_ovl"]);
    let got = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap()
        .sample
        .unwrap();
    assert_eq!((got.memory_current_bytes, got.cpu_usage_usec), (1000, 5));
}

#[tokio::test]
async fn counts_a_container_answering_without_stats() {
    let agent = FakeAgent::with(&[("app_ovl", None)]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let got = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap()
        .sample
        .unwrap();
    assert_eq!((got.memory_current_bytes, got.cpu_usage_usec), (0, 0));
}

#[tokio::test]
async fn error_codes() {
    let healthy = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let tu = test_actor().uid;
    type Make = Box<dyn Fn() -> Arc<WorkerService>>;
    let h1 = healthy.clone();
    let h2 = healthy.clone();
    let h3 = healthy.clone();
    let h4 = healthy.clone();
    let cases: Vec<(&str, Make, String, tonic::Code)> = vec![
        (
            "empty actor_uid",
            Box::new(move || stats_service(&h1, &["app_ovl"]).0),
            String::new(),
            tonic::Code::InvalidArgument,
        ),
        (
            "malformed actor_uid",
            Box::new(move || stats_service(&h2, &["app_ovl"]).0),
            "not-a-uuid".into(),
            tonic::Code::InvalidArgument,
        ),
        (
            "worker is available",
            Box::new(|| WorkerService::new(ServiceConfig::default())),
            tu.clone(),
            tonic::Code::NotFound,
        ),
        (
            "actor_uid does not match the executing workload",
            Box::new(move || stats_service(&h3, &["app_ovl"]).0),
            OTHER_ACTOR_UID.into(),
            tonic::Code::NotFound,
        ),
        (
            "no guest agent connection yet",
            Box::new(|| {
                let s = WorkerService::new(ServiceConfig::default());
                s.host_for_test(test_actor(), None, test_activation());
                s
            }),
            tu.clone(),
            tonic::Code::FailedPrecondition,
        ),
        (
            "guest agent connection belongs to another actor",
            Box::new(move || {
                let s = WorkerService::new(ServiceConfig::default());
                s.host_for_test(
                    test_actor(),
                    Some(target(OTHER_ACTOR_UID, &h4, &["app_ovl"])),
                    test_activation(),
                );
                s
            }),
            tu.clone(),
            tonic::Code::Internal,
        ),
        (
            "no container answers",
            Box::new(|| {
                let a = FakeAgent::with(&[]);
                a.errs
                    .lock()
                    .insert("app_ovl".into(), "ttrpc: closed".into());
                a.errs
                    .lock()
                    .insert("sidecar_ovl".into(), "ttrpc: closed".into());
                stats_service(&a, &["app_ovl", "sidecar_ovl"]).0
            }),
            tu.clone(),
            tonic::Code::FailedPrecondition,
        ),
        (
            "no containers to measure",
            Box::new(move || stats_service(&healthy, &[]).0),
            tu.clone(),
            tonic::Code::FailedPrecondition,
        ),
    ];
    for (name, make, uid, want) in cases {
        let err = make().get_workload_stats(req(&uid)).await.unwrap_err();
        assert_eq!(err.code(), want, "{name}: {err}");
    }
}

#[tokio::test]
async fn does_not_take_the_actor_lock() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let _g = s.locks.lock(&test_actor().uid).await;
    s.get_workload_stats(req(&test_actor().uid)).await.unwrap();
}

#[tokio::test]
async fn discovery_serves_what_the_sweep_read() {
    let agent = FakeAgent::with(&[("app_ovl", stats(157286400, 209715200, 20971520, 1234567000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), 1);
    let mut want = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap()
        .sample
        .unwrap();
    let mut sample = got.samples[0].clone();
    sample.observed_at_unix_nano = 0;
    want.observed_at_unix_nano = 0;
    // The keyed read is one more reading of the same activation; the
    // discovery sample is the one before it, with the same CPU.
    assert_eq!(sample, want);
}

#[tokio::test]
async fn an_empty_worker_lists_nothing() {
    let s = WorkerService::new(ServiceConfig::default());
    assert!(sweep_and_list(&s).await.samples.is_empty());
}

#[tokio::test]
async fn a_booting_actor_is_pending() {
    let s = WorkerService::new(ServiceConfig::default());
    s.host_for_test(test_actor(), None, test_activation());
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), 1);
    let mut e = got.samples[0].clone();
    assert_ne!(e.observed_at_unix_nano, 0);
    e.observed_at_unix_nano = 0;
    assert_eq!(e, pending_for(&test_actor()));
}

#[tokio::test]
async fn lists_several_actors() {
    let mut second = test_actor();
    second.uid = OTHER_ACTOR_UID.into();
    second.actor = ActorRef::new("space-a", "actor-b");
    let agent = FakeAgent::with(&[
        ("app_ovl", stats(1000, 2000, 100, 5000)),
        ("b_ovl", stats(3000, 4000, 200, 7000)),
    ]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    s.host_for_test(
        second.clone(),
        Some(target(&second.uid, &agent, &["b_ovl"])),
        test_activation(),
    );
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), 2);
    let by: HashMap<_, _> = got
        .samples
        .iter()
        .map(|x| (x.actor_uid.clone(), x.clone()))
        .collect();
    assert!(by.contains_key(&test_actor().uid));
    assert_eq!(by[&second.uid].memory_current_bytes, 3000);
}

#[tokio::test]
async fn one_booting_among_running() {
    let mut booting = test_actor();
    booting.uid = OTHER_ACTOR_UID.into();
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    s.host_for_test(booting, None, test_activation());
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), 2);
    for x in &got.samples {
        let measured = x.source != pb::StatsSource::Unspecified as i32;
        assert_eq!(measured, x.actor_uid == test_actor().uid, "{}", x.actor_uid);
    }
}

#[tokio::test]
async fn a_transition_mid_read_is_not_found() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let weak = Arc::downgrade(&s);
    *agent.on_call.lock() = Some(Arc::new(move || {
        if let Some(s) = weak.upgrade() {
            s.unhost_for_test(&test_actor().uid);
        }
    }));
    let err = s
        .get_workload_stats(req(&test_actor().uid))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn a_stale_target_is_pending_in_the_list() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let s = WorkerService::new(ServiceConfig::default());
    s.host_for_test(
        test_actor(),
        Some(target(OTHER_ACTOR_UID, &agent, &["app_ovl"])),
        test_activation(),
    );
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), 1);
    assert_eq!(got.samples[0].source, pb::StatsSource::Unspecified as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn samples_guests_concurrently() {
    const ACTORS: i32 = 4;
    let in_flight = Arc::new(AtomicI32::new(0));
    let peak = Arc::new(AtomicI32::new(0));
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
    let (f, p) = (in_flight.clone(), peak.clone());
    *agent.on_call.lock() = Some(Arc::new(move || {
        let n = f.fetch_add(1, Ordering::SeqCst) + 1;
        p.fetch_max(n, Ordering::SeqCst);
        // Hold the call until every guest is in, or a second passes.
        let start = std::time::Instant::now();
        while f.load(Ordering::SeqCst) < ACTORS && start.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(1));
        }
        f.fetch_sub(1, Ordering::SeqCst);
    }));
    let s = WorkerService::new(ServiceConfig::default());
    for i in 0..ACTORS {
        let mut a = test_actor();
        a.uid = format!("uid-{i}");
        s.host_for_test(
            a.clone(),
            Some(target(&a.uid, &agent, &["app_ovl"])),
            test_activation(),
        );
    }
    let got = sweep_and_list(&s).await;
    assert_eq!(got.samples.len(), ACTORS as usize);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        ACTORS,
        "guests were not asked at once"
    );
}

#[tokio::test]
async fn transitions_during_the_sweep() {
    let mut other_actor = test_actor();
    other_actor.uid = OTHER_ACTOR_UID.into();
    let mut other_template = test_actor();
    other_template.template_name = "template-b".into();
    type During = Box<dyn Fn(&WorkerService) + Send + Sync>;
    let (ot, oa) = (other_template.clone(), other_actor.clone());
    let cases: Vec<(&str, During, Vec<pb::WorkloadStatsSample>)> = vec![
        (
            "re-hosted on another template",
            Box::new(move |s| {
                s.host_for_test(ot.clone(), None, test_activation());
            }),
            vec![pending_for(&other_template)],
        ),
        (
            "to another actor",
            Box::new(move |s| {
                s.unhost_for_test(&test_actor().uid);
                s.host_for_test(oa.clone(), None, test_activation());
            }),
            vec![],
        ),
        (
            "to available",
            Box::new(|s| s.unhost_for_test(&test_actor().uid)),
            vec![],
        ),
    ];
    for (name, during, want) in cases {
        let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5000))]);
        let (s, _) = stats_service(&agent, &["app_ovl"]);
        let weak = Arc::downgrade(&s);
        let once = std::sync::Once::new();
        let during = Arc::new(during);
        *agent.on_call.lock() = Some(Arc::new(move || {
            if let Some(s) = weak.upgrade() {
                once.call_once(|| during(&s));
            }
        }));
        let got = sweep_and_list(&s).await;
        let samples: Vec<_> = got
            .samples
            .into_iter()
            .filter(|x| x.actor_uid != other_actor.uid)
            .map(|mut x| {
                x.observed_at_unix_nano = 0;
                x
            })
            .collect();
        assert_eq!(samples, want, "{name}");
    }
}

// ---- usage records

#[tokio::test]
async fn the_sweep_stamps_the_epoch_and_discovery_serves_it() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let epoch = SystemTime::UNIX_EPOCH + Duration::from_secs(1700);
    set_activation(&s, running(Activation::new(epoch, false)));
    let before = s.get_active_workload_stats();
    assert_eq!(
        before.samples[0].source,
        pb::StatsSource::Unspecified as i32
    );
    assert_eq!(before.samples[0].epoch_unix_nano, unix_nanos(epoch));
    assert_eq!(agent.calls(), 0, "the discovery read called the guest");
    s.sweep_usage().await;
    let after = s.get_active_workload_stats();
    assert_eq!(
        (
            after.samples[0].epoch_unix_nano,
            after.samples[0].cpu_usage_usec
        ),
        (unix_nanos(epoch), 5000)
    );
}

#[tokio::test]
async fn a_resumed_guests_cpu_is_activation_relative() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 9_000_000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, running(Activation::new(SystemTime::now(), true)));
    s.sweep_usage().await;
    assert_eq!(h.usage.latest().unwrap().cpu_usage_usec, 0);
    agent.set("app_ovl", stats(1000, 2000, 100, 9_400_000).unwrap());
    s.sweep_usage().await;
    assert_eq!(h.usage.latest().unwrap().cpu_usage_usec, 400);
}

#[tokio::test]
async fn initial_and_final_records() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.record_initial(&h).await;
    assert!(
        h.usage.latest().is_some(),
        "the initial sample was not stored"
    );
    let calls = agent.calls();
    s.record_final(&h);
    assert_eq!(agent.calls(), calls, "the final record read the guest");
    assert_eq!(rec.kinds(), ["initial", "final"]);
}

#[tokio::test]
async fn final_only_once_unhosted() {
    let agent = FakeAgent::with(&[]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.record_final_if_ended(Some(&h));
    assert!(rec.kinds().is_empty());
    s.unhost_for_test(&test_actor().uid);
    s.record_final_if_ended(Some(&h));
    assert_eq!(rec.kinds(), ["final"]);
}

#[tokio::test]
async fn read_final_keeps_the_last_measured() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.record_initial(&h).await;
    agent.set("app_ovl", stats(1000, 2000, 100, 9_000_000).unwrap());
    s.read_final(&h).await;
    assert_eq!(h.usage.latest().unwrap().cpu_usage_usec, 9000);
    agent
        .errs
        .lock()
        .insert("app_ovl".into(), "guest gone".into());
    s.read_final(&h).await;
    assert_eq!(h.usage.latest().unwrap().cpu_usage_usec, 9000);
}

#[tokio::test]
async fn ending_the_previous_activation_writes_its_final() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let old = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.end_previous_activation(&test_actor().uid, None)
        .await
        .unwrap();
    s.record_initial(&old).await;
    assert_eq!(rec.kinds(), ["final"]);
}

#[tokio::test]
async fn a_final_with_no_sample_is_pending() {
    let agent = FakeAgent::with(&[]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.record_final(&h);
    assert_eq!(rec.sources(), [source::UNSPECIFIED]);
}

#[tokio::test]
async fn no_initial_after_the_final() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    let (weak, hh) = (Arc::downgrade(&s), h.clone());
    *agent.on_call.lock() = Some(Arc::new(move || {
        if let Some(s) = weak.upgrade() {
            s.record_final(&hh);
        }
    }));
    s.record_initial(&h).await;
    assert_eq!(rec.kinds(), ["final"]);
}

#[tokio::test]
async fn the_initial_reading_survives_a_panic() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    *agent.on_call.lock() = Some(Arc::new(|| panic!("guest read")));
    s.record_initial(&h).await;
    assert!(rec.kinds().is_empty());
    assert!(h.usage.sampling(), "not sampling after the panic");
}

#[tokio::test]
async fn the_sweep_survives_a_panic() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    set_activation(&s, running(Activation::new(SystemTime::now(), false)));
    *agent.on_call.lock() = Some(Arc::new(|| panic!("guest read")));
    s.sweep_usage().await;
    assert!(rec.kinds().is_empty());
}

#[tokio::test]
async fn the_initial_reading_takes_a_slot() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    let _all = s
        .guest_slots
        .clone()
        .acquire_many_owned(crate::service::STATS_FAN_OUT as u32)
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(50), s.record_initial(&h)).await;
    assert_eq!(agent.calls(), 0, "read the guest with every slot taken");
}

#[tokio::test]
async fn a_rehost_mid_initial_leaves_the_new_activation_alone() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, _) = stats_service(&agent, &["app_ovl"]);
    let old = set_activation(&s, Activation::new(SystemTime::now(), false));
    let t = s.guest_of(&old);
    let weak = Arc::downgrade(&s);
    *agent.on_call.lock() = Some(Arc::new(move || {
        if let Some(s) = weak.upgrade() {
            s.host_for_test(
                test_actor(),
                t.clone(),
                Activation::new(SystemTime::now(), false),
            );
        }
    }));
    s.record_initial(&old).await;
    let l = old.usage.latest().unwrap();
    assert_eq!(l.epoch_unix_nano, old.usage.epoch());
    assert!(
        s.lookup_actor(&test_actor().uid)
            .unwrap()
            .usage
            .latest()
            .is_none(),
        "the new activation holds the old one's sample"
    );
}

#[tokio::test]
async fn a_partial_read_does_not_double_charge() {
    let agent = FakeAgent::with(&[
        ("a_ovl", stats(1000, 2000, 100, 600_000_000)),
        ("b_ovl", stats(1000, 2000, 100, 400_000_000)),
    ]);
    let (s, _) = stats_service(&agent, &["a_ovl", "b_ovl"]);
    let h = set_activation(&s, running(Activation::new(SystemTime::now(), true)));
    s.sweep_usage().await;
    agent.set("a_ovl", stats(1000, 2000, 100, 605_000_000).unwrap());
    agent
        .errs
        .lock()
        .insert("b_ovl".into(), "deadline exceeded".into());
    s.sweep_usage().await;
    assert_eq!(h.usage.latest().unwrap().cpu_usage_usec, 5000);
    agent.set("a_ovl", stats(1000, 2000, 100, 606_000_000).unwrap());
    agent.set("b_ovl", stats(1000, 2000, 100, 404_000_000).unwrap());
    agent.errs.lock().clear();
    s.sweep_usage().await;
    assert_eq!(
        h.usage.latest().unwrap().cpu_usage_usec,
        10000,
        "b's counter charged twice"
    );
}

#[tokio::test]
async fn the_sweep_waits_for_the_initial_and_stops_at_the_final() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let h = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.sweep_usage().await;
    assert_eq!(agent.calls(), 0);
    s.record_initial(&h).await;
    s.sweep_usage().await;
    s.record_final(&h);
    let calls = agent.calls();
    s.sweep_usage().await;
    assert_eq!(agent.calls(), calls, "swept after the final record");
    assert_eq!(rec.kinds(), ["initial", "periodic", "final"]);
}

#[tokio::test]
async fn the_initial_reads_its_own_guest() {
    let old_agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let new_agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 9_000_000))]);
    let (s, _) = stats_service(&old_agent, &["app_ovl"]);
    let old = set_activation(&s, Activation::new(SystemTime::now(), false));
    let fresh = s.host_for_test(
        test_actor(),
        Some(target(&test_actor().uid, &new_agent, &["app_ovl"])),
        Activation::new(SystemTime::now(), false),
    );
    s.record_initial(&old).await;
    assert_eq!(old.usage.latest().unwrap().cpu_usage_usec, 5000);
    assert_eq!(new_agent.calls(), 0);
    assert!(!fresh.usage.sampling() && fresh.usage.latest().is_none());
}

#[tokio::test]
async fn a_late_initial_leaves_the_new_activation_alone() {
    let agent = FakeAgent::with(&[("app_ovl", stats(1000, 2000, 100, 5_000_000))]);
    let (s, rec) = stats_service(&agent, &["app_ovl"]);
    let old = set_activation(&s, Activation::new(SystemTime::now(), false));
    s.record_final(&old);
    let fresh = s.host_for_test(
        test_actor(),
        None,
        Activation::new(SystemTime::now(), false),
    );
    s.record_initial(&old).await;
    assert_eq!(rec.kinds(), ["final"]);
    assert!(!fresh.usage.sampling());
}
