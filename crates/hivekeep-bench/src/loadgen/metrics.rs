//! The load generator's Prometheus surface (`locust_*` series, by method,
//! name, status and user class) and the record calls that feed both it and
//! the locust master's request stats.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{
    exponential_buckets, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
};

use super::locust::stats;

/// The registry every load-generator metric lives in; user classes
/// register their own series here too.
pub fn registry() -> &'static Registry {
    &METRICS.registry
}

struct Metrics {
    registry: Registry,
    requests_total: CounterVec,
    request_duration: HistogramVec,
    server_duration: HistogramVec,
    active_users: GaugeVec,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let registry = Registry::new();
    let requests_total = CounterVec::new(
        Opts::new(
            "locust_requests_total",
            "Total number of requests, by method/name/status/user_class.",
        ),
        &["method", "name", "status", "user_class"],
    )
    .unwrap();
    let request_duration = HistogramVec::new(
        HistogramOpts::new(
            "locust_request_duration_milliseconds",
            "Request latency in milliseconds, by method/name/status/user_class.",
        )
        .buckets(exponential_buckets(1.0, 2.0, 16).unwrap()),
        &["method", "name", "status", "user_class"],
    )
    .unwrap();
    let server_duration = HistogramVec::new(
        HistogramOpts::new(
            "locust_server_duration_milliseconds",
            "Server-side latency in milliseconds, read from the response trailer or header, by method/name/user_class.",
        )
        .buckets(exponential_buckets(0.1, 2.0, 20).unwrap()),
        &["method", "name", "user_class"],
    )
    .unwrap();
    let active_users = GaugeVec::new(
        Opts::new("locust_users", "Active locust users, by user_class."),
        &["user_class"],
    )
    .unwrap();
    for c in [
        Box::new(requests_total.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(request_duration.clone()),
        Box::new(server_duration.clone()),
        Box::new(active_users.clone()),
    ] {
        registry.register(c).unwrap();
    }
    Metrics {
        registry,
        requests_total,
        request_duration,
        server_duration,
        active_users,
    }
});

/// Registers a class's own collector in [`registry`], tolerating a second
/// registration of the same series (tests build classes many times).
pub fn register(c: Box<dyn prometheus::core::Collector>) {
    match registry().register(c) {
        Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
        Err(e) => tracing::warn!(err = %e, "registering a load-generator metric failed"),
    }
}

/// Renders [`registry`] in the Prometheus text format.
pub fn render() -> String {
    crate::boot::render_metrics(registry())
}

/// The lines of the rendered registry that mention `name`.
pub fn lines_of(name: &str) -> String {
    render()
        .lines()
        .filter(|l| l.contains(name))
        .map(|l| format!("{l}\n"))
        .collect()
}

fn millis(latency: Duration) -> i64 {
    latency.as_millis().min(i64::MAX as u128) as i64
}

/// Reports a successful request to the master and to Prometheus.
pub fn record_success(
    method: &str,
    name: &str,
    user_class: &str,
    latency: Duration,
    response_bytes: i64,
) {
    stats::record_success(method, name, millis(latency), response_bytes);
    record_prom_success(method, name, user_class, latency);
}

/// Reports a failed request to the master and to Prometheus.
pub fn record_failure(method: &str, name: &str, user_class: &str, latency: Duration, err: &str) {
    stats::record_failure(method, name, millis(latency), err);
    record_prom_failure(method, name, user_class, latency);
}

/// Reports a successful request to Prometheus only (for requests made
/// after the run, such as shutdown clean-up).
pub fn record_prom_success(method: &str, name: &str, user_class: &str, latency: Duration) {
    let m = &*METRICS;
    let ms = millis(latency) as f64;
    m.requests_total
        .with_label_values(&[method, name, "success", user_class])
        .inc();
    m.request_duration
        .with_label_values(&[method, name, "success", user_class])
        .observe(ms);
}

/// Reports a failed request to Prometheus only.
pub fn record_prom_failure(method: &str, name: &str, user_class: &str, latency: Duration) {
    let m = &*METRICS;
    let ms = millis(latency) as f64;
    m.requests_total
        .with_label_values(&[method, name, "failure", user_class])
        .inc();
    m.request_duration
        .with_label_values(&[method, name, "failure", user_class])
        .observe(ms);
}

/// Reports the server's own handling time for a request, to Prometheus
/// only: the master keeps one latency per request, the client's.
pub fn record_server_latency(method: &str, name: &str, user_class: &str, latency: Duration) {
    METRICS
        .server_duration
        .with_label_values(&[method, name, user_class])
        .observe(latency.as_secs_f64() * 1e3);
}

/// Shifts the active-users gauge of a class by `delta`.
pub fn update_users(user_class: &str, delta: f64) {
    METRICS
        .active_users
        .with_label_values(&[user_class])
        .add(delta);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The server histogram resolves a fast RPC's sub-millisecond time,
    /// which a first bucket at 1 ms would hide.
    #[test]
    fn record_server_latency_keeps_sub_millisecond() {
        record_server_latency("grpc", "SubMs", "TestUser", Duration::from_micros(300));
        let got = lines_of("locust_server_duration_milliseconds");
        for line in [
            r#"locust_server_duration_milliseconds_bucket{method="grpc",name="SubMs",user_class="TestUser",le="0.2"} 0"#,
            r#"locust_server_duration_milliseconds_bucket{method="grpc",name="SubMs",user_class="TestUser",le="0.4"} 1"#,
            r#"locust_server_duration_milliseconds_sum{method="grpc",name="SubMs",user_class="TestUser"} 0.3"#,
        ] {
            assert!(got.contains(line), "missing line {line:?} in:\n{got}");
        }
    }

    #[test]
    fn record_server_latency_sums_and_counts() {
        record_server_latency("grpc", "Srv", "TestUser", Duration::from_micros(1500));
        record_server_latency("grpc", "Srv", "TestUser", Duration::from_micros(2500));
        let got = lines_of("locust_server_duration_milliseconds");
        assert!(
            got.contains(r#"locust_server_duration_milliseconds_sum{method="grpc",name="Srv",user_class="TestUser"} 4"#),
            "sum is not 4 ms:\n{got}"
        );
        assert!(
            got.contains(r#"locust_server_duration_milliseconds_count{method="grpc",name="Srv",user_class="TestUser"} 2"#),
            "count is not 2:\n{got}"
        );
    }

    #[test]
    fn success_and_failure_feed_the_request_series() {
        record_success("grpc", "RecOk", "RecUser", Duration::from_millis(3), 10);
        record_failure("grpc", "RecOk", "RecUser", Duration::from_millis(5), "boom");
        update_users("RecUser", 2.0);
        update_users("RecUser", -1.0);
        let got = lines_of("RecUser");
        assert!(got.contains(r#"locust_requests_total{method="grpc",name="RecOk",status="success",user_class="RecUser"} 1"#), "{got}");
        assert!(got.contains(r#"locust_requests_total{method="grpc",name="RecOk",status="failure",user_class="RecUser"} 1"#), "{got}");
        assert!(
            got.contains(r#"locust_users{user_class="RecUser"} 1"#),
            "{got}"
        );
    }
}
