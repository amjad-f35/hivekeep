use std::time::Duration;

use super::*;
use crate::glutton::fake;
use crate::loadgen::classes::glutton::knobs::GluttonKnobs;
use crate::loadgen::classes::glutton::test_support::{config, count};
use crate::loadgen::dynconfig::Holder;
use crate::loadgen::fake_control::FakeControl;
use crate::loadgen::lifecycle::{
    CONCURRENT_UPDATE_MSG, CONFLICT_RETRY_ATTEMPTS, CONFLICT_RETRY_BACKOFF,
};

async fn actor_with(srv: &fake::Server, knobs: GluttonKnobs) -> GluttonActor {
    let cfg = config(FakeControl::new(), &srv.url(), Holder::fixed(knobs));
    GluttonActor::new(Arc::new(cfg), "memactor".into())
}

fn ram(target: &str, churn: &str, read: &str) -> GluttonKnobs {
    GluttonKnobs {
        mem_target: target.into(),
        mem_churn: churn.into(),
        mem_read: read.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn ensure_ram_filled_requests_target() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("2Gi", "", "")).await;
    u.ensure_ram_filled().await;
    assert!(u.ram_filled);
    assert_eq!(
        srv.recorded().ram_write_sizes,
        vec!["2Gi"],
        "passed through verbatim"
    );
    // The working set persists in the Actor: a second call does nothing.
    u.ensure_ram_filled().await;
    assert_eq!(srv.recorded().ram_write_sizes.len(), 1);
}

#[tokio::test]
async fn ensure_ram_filled_disabled_by_default() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, GluttonKnobs::default()).await;
    u.ensure_ram_filled().await;
    assert!(u.ram_filled, "disabled counts as done");
    assert!(srv.recorded().ram_write_sizes.is_empty());
}

#[tokio::test]
async fn churn_ram_overwrites_each_cycle() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("1Gi", "64Mi", "")).await;
    // Nothing to overwrite before the fill.
    u.churn_ram().await;
    assert!(srv.recorded().ram_write_sizes.is_empty());
    u.ensure_ram_filled().await;
    u.churn_ram().await;
    u.churn_ram().await;
    let r = srv.recorded();
    assert_eq!(r.ram_write_sizes, vec!["1Gi", "64Mi", "64Mi"]);
    assert_eq!(
        r.ram_write_modes,
        vec![
            pb::WriteMode::Truncate,
            pb::WriteMode::OverwriteRotate,
            pb::WriteMode::OverwriteRotate
        ]
    );
}

#[tokio::test]
async fn churn_ram_disabled_by_default() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("1Gi", "", "")).await;
    u.ensure_ram_filled().await;
    u.churn_ram().await;
    assert_eq!(srv.recorded().ram_write_sizes.len(), 1, "fill only");
}

#[tokio::test]
async fn read_ram_walks_after_fill() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("1Gi", "", "all")).await;
    u.read_ram().await;
    assert!(
        srv.recorded().ram_read_sizes.is_empty(),
        "nothing to walk before the fill"
    );
    u.ensure_ram_filled().await;
    u.read_ram().await;
    u.read_ram().await;
    // "all" is an empty size: the whole array.
    assert_eq!(srv.recorded().ram_read_sizes, vec!["", ""]);
}

#[tokio::test]
async fn read_ram_passes_size_verbatim() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("1Gi", "", "512Mi")).await;
    u.ensure_ram_filled().await;
    u.read_ram().await;
    assert_eq!(srv.recorded().ram_read_sizes, vec!["512Mi"]);
}

#[tokio::test]
async fn read_ram_disabled_by_default() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, ram("1Gi", "", "")).await;
    u.ensure_ram_filled().await;
    u.read_ram().await;
    assert!(srv.recorded().ram_read_sizes.is_empty());
}

#[tokio::test]
async fn ensure_ram_filled_retries_after_failure() {
    let srv = fake::Server::start(fake::Config {
        status: Some(503),
        ..Default::default()
    })
    .await;
    let mut u = actor_with(&srv, ram("1Mi", "", "")).await;
    u.ensure_ram_filled().await;
    assert!(!u.ram_filled, "a failed fill must be retried");
    srv.update(|c| c.status = None);
    u.ensure_ram_filled().await;
    assert!(u.ram_filled);
}

fn cpu(cores: i64, duty: f64) -> GluttonKnobs {
    GluttonKnobs {
        cpu_cores: cores,
        cpu_duty_cycle: duty,
        ..Default::default()
    }
}

#[tokio::test]
async fn ensure_cpu_load_requests_configured_load() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, cpu(2, 0.1)).await;
    u.ensure_cpu_load().await;
    assert!(u.cpu_loaded);
    let reqs = srv.recorded().cpu_requests;
    assert_eq!(reqs.len(), 1);
    assert_eq!((reqs[0].num_cores, reqs[0].duty_cycle), (2, 0.1));
    u.ensure_cpu_load().await;
    assert_eq!(
        srv.recorded().cpu_requests.len(),
        1,
        "the load persists in the Actor"
    );
}

#[tokio::test]
async fn ensure_cpu_load_disabled_by_default() {
    let srv = fake::Server::start(Default::default()).await;
    let mut u = actor_with(&srv, GluttonKnobs::default()).await;
    u.ensure_cpu_load().await;
    assert!(u.cpu_loaded, "disabled counts as done");
    assert!(srv.recorded().cpu_requests.is_empty());
}

#[tokio::test]
async fn ensure_cpu_load_retries_after_failure() {
    let srv = fake::Server::start(fake::Config {
        status: Some(503),
        ..Default::default()
    })
    .await;
    let mut u = actor_with(&srv, cpu(1, 0.5)).await;
    u.ensure_cpu_load().await;
    assert!(!u.cpu_loaded);
    srv.update(|c| c.status = None);
    u.ensure_cpu_load().await;
    assert!(u.cpu_loaded);
    assert_eq!(
        srv.recorded().cpu_requests.len(),
        1,
        "the failed call is refused before it is recorded"
    );
}

fn conflict() -> Status {
    Status::aborted(CONCURRENT_UPDATE_MSG)
}

async fn resume_actor(errs: Vec<Status>) -> (GluttonActor, Arc<FakeControl>, fake::Server) {
    let srv = fake::Server::start(Default::default()).await;
    let api = FakeControl::new();
    api.with(|s| s.resume_errs.extend(errs));
    let cfg = config(
        api.clone(),
        &srv.url(),
        Holder::fixed(GluttonKnobs::default()),
    );
    (
        GluttonActor::new(Arc::new(cfg), "resumeactor".into()),
        api,
        srv,
    )
}

#[tokio::test]
async fn resume_retries_concurrent_update_conflict() {
    let (mut u, api, _srv) = resume_actor(vec![conflict(), conflict()]).await;
    let start = std::time::Instant::now();
    u.resume().await.expect("conflicts clear");
    assert_eq!(
        count(&api.calls(), "ResumeActor"),
        3,
        "two conflicts, then success"
    );
    assert!(
        start.elapsed() >= CONFLICT_RETRY_BACKOFF,
        "the second retry backs off"
    );
    assert!(!u.crashed);
}

#[tokio::test]
async fn resume_does_not_retry_other_errors() {
    let (mut u, api, _srv) = resume_actor(vec![Status::unavailable("down")]).await;
    assert!(u.resume().await.is_err());
    assert_eq!(count(&api.calls(), "ResumeActor"), 1);
}

#[tokio::test]
async fn resume_gives_up_after_max_attempts() {
    let (mut u, api, _srv) = resume_actor(vec![conflict(); CONFLICT_RETRY_ATTEMPTS + 1]).await;
    assert!(u.resume().await.is_err());
    assert_eq!(count(&api.calls(), "ResumeActor"), CONFLICT_RETRY_ATTEMPTS);
}

/// A caller giving up mid-retry: the first retry is immediate, so exactly
/// two attempts happen before the backoff is cut short.
#[tokio::test]
async fn resume_retry_honors_cancellation() {
    let (mut u, api, _srv) = resume_actor(vec![conflict(), conflict(), conflict()]).await;
    let res = tokio::time::timeout(Duration::from_millis(10), u.resume()).await;
    assert!(res.is_err(), "the resume must be cut short");
    assert_eq!(count(&api.calls(), "ResumeActor"), 2);
}

#[test]
fn uniform_wait_stays_in_range() {
    use crate::loadgen::dynconfig::uniform;
    let (lo, hi) = (Duration::from_millis(200), Duration::from_secs(1));
    for _ in 0..1000 {
        let got = uniform(lo, hi);
        assert!(got >= lo && got <= hi, "{got:?}");
    }
}

#[test]
fn uniform_wait_degenerate_ranges() {
    use crate::loadgen::dynconfig::uniform;
    assert_eq!(uniform(Duration::ZERO, Duration::ZERO), Duration::ZERO);
    assert_eq!(
        uniform(Duration::from_secs(3), Duration::from_secs(3)),
        Duration::from_secs(3)
    );
    // An inverted range yields the lower bound rather than a negative wait.
    assert_eq!(
        uniform(Duration::from_secs(5), Duration::from_secs(1)),
        Duration::from_secs(5)
    );
}
