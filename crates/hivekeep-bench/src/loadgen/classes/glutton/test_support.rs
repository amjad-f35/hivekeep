//! Shared fixtures for the glutton-family class tests.

use std::sync::Arc;
use std::time::Duration;

use crate::loadgen::dynconfig::Holder;
use crate::loadgen::fake_control::FakeControl;
use crate::loadgen::trace::Tracer;
use crate::loadgen::userclass::Config;

/// A config talking to `api` and to an Actor at `router_url`, with a fixed
/// class config `knobs`.
pub fn config(api: Arc<FakeControl>, router_url: &str, knobs: Holder) -> Config {
    Config {
        api,
        http: reqwest::Client::new(),
        router_url: router_url.to_string(),
        hive: String::new(),
        dyn_config: Arc::new(knobs),
        tracer: Tracer::never("test"),
        actors_per_user: 1,
        total_actors: 1,
        spawn_concurrency: 1,
        actor_deadline: Duration::from_secs(5),
    }
}

/// How often `name` appears in `calls`.
pub fn count(calls: &[String], name: &str) -> usize {
    calls.iter().filter(|c| c.as_str() == name).count()
}

/// The `locust_requests_total` value for request `name`, `status` and
/// `user_class`; 0 when the series does not exist yet.
pub fn requests_total(name: &str, status: &str, user_class: &str) -> f64 {
    let want = [
        format!("name=\"{name}\""),
        format!("status=\"{status}\""),
        format!("user_class=\"{user_class}\""),
    ];
    crate::loadgen::metrics::lines_of("locust_requests_total")
        .lines()
        .filter(|l| !l.starts_with('#') && want.iter().all(|w| l.contains(w.as_str())))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}
