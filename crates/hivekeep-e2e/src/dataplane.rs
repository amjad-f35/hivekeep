//! The observable behaviour of the gate's dataplane. Suites call these
//! operations instead of branching on the selected implementation.

use crate::collector_metrics::{
    missing_platform_metrics, scrape_egress_envoy_metrics, ROUTE_DURATION_PREFIX,
};
use crate::statusz::StatuszClient;

/// Delta of egress CONNECT connections and requests between two scrapes;
/// `-1` when the dataplane does not report the counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressConnectStats {
    /// New HTTP/2 CONNECT connections.
    pub connections: i64,
    /// New 2xx CONNECT requests.
    pub requests: i64,
}

/// A gate dataplane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dataplane {
    /// The Envoy dataplane.
    Envoy,
}

/// Waits on a dataplane's active parked-request gauge.
pub enum ParkingObserver {
    /// Through the router's status page.
    Statusz(StatuszClient),
}

impl ParkingObserver {
    /// Polls the gauge until `cond` holds (4 s budget), returning the last
    /// value seen.
    pub async fn wait_for_count(&self, cond: impl Fn(i64) -> bool) -> Result<i64, String> {
        match self {
            ParkingObserver::Statusz(c) => c.wait_for_count(cond).await,
        }
    }
}

fn is_egress_policy_denied(status: u16, body: &str) -> bool {
    (status == 403 && body.contains("egress denied"))
        || (status == 502 && body.contains("request failed"))
}

impl Dataplane {
    /// The dataplane selected for this process.
    pub fn current() -> Self {
        Dataplane::Envoy
    }

    /// An observer of the parked-request gauge.
    pub async fn new_parking_observer(
        self,
        kube: &kube::Client,
    ) -> Result<ParkingObserver, String> {
        match self {
            Dataplane::Envoy => Ok(ParkingObserver::Statusz(StatuszClient::new(kube).await?)),
        }
    }

    /// Whether a response means the parking budget ran out and the caller
    /// may retry.
    pub fn is_retryable_parking_budget_exhaustion(self, status: u16, body: &str) -> bool {
        status == 503 && body.contains("no worker has room for the actor")
    }

    /// The status a request gets when the parking budget is exhausted.
    pub fn parking_budget_status(self) -> u16 {
        503
    }

    /// Whether a response is the gate refusing by egress policy.
    pub fn is_egress_policy_denied(self, status: u16, body: &str) -> bool {
        is_egress_policy_denied(status, body)
    }

    /// The platform prefixes this dataplane exports through the collector.
    pub fn platform_metric_prefixes<'a>(self, prefixes: &[&'a str]) -> Vec<&'a str> {
        match self {
            Dataplane::Envoy => prefixes.to_vec(),
        }
    }

    /// Whether the router's route-duration metric has been recorded.
    pub async fn route_duration_seen(
        self,
        _kube: &kube::Client,
        collector_scrape: &str,
    ) -> Result<bool, String> {
        match self {
            Dataplane::Envoy => {
                Ok(missing_platform_metrics(collector_scrape, &[ROUTE_DURATION_PREFIX]).is_empty())
            }
        }
    }

    /// Scrapes the dataplane's own metrics.
    pub async fn scrape_metrics(self, kube: &kube::Client) -> Result<String, String> {
        match self {
            Dataplane::Envoy => scrape_egress_envoy_metrics(kube).await,
        }
    }

    /// The egress CONNECT delta between two scrapes.
    pub fn egress_connect_stats(self, before: &str, after: &str) -> EgressConnectStats {
        match self {
            Dataplane::Envoy => {
                let (b, a) = (
                    parse_envoy_egress_connect(before),
                    parse_envoy_egress_connect(after),
                );
                EgressConnectStats {
                    connections: a.connections - b.connections,
                    requests: a.requests - b.requests,
                }
            }
        }
    }
}

fn parse_envoy_egress_connect(scrape: &str) -> EgressConnectStats {
    let mut stats = EgressConnectStats {
        connections: 0,
        requests: 0,
    };
    for line in scrape.split('\n') {
        let line = line.trim();
        if line.is_empty()
            || line.starts_with('#')
            || !line.contains(r#"envoy_http_conn_manager_prefix="egress_connect""#)
        {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let Ok(val) = fields[fields.len() - 1].parse::<f64>() else {
            continue;
        };
        if line.starts_with("envoy_http_downstream_cx_http2_total{") {
            stats.connections += val as i64;
        } else if line.starts_with("envoy_http_downstream_rq_xx{")
            && line.contains(r#"envoy_response_code_class="2""#)
        {
            stats.requests += val as i64;
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn egress_policy_denial() {
        let envoy = Dataplane::current();
        assert_eq!(envoy, Dataplane::Envoy);
        assert!(
            envoy.is_egress_policy_denied(502, "request failed"),
            "Envoy CONNECT refusal"
        );
        assert!(
            envoy.is_egress_policy_denied(403, "egress denied for \"h\"/\"a\": blocked"),
            "router denial"
        );
        assert!(
            !envoy.is_egress_policy_denied(403, "forbidden"),
            "unrelated 403"
        );
    }

    #[test]
    fn parking_budget() {
        let d = Dataplane::Envoy;
        assert_eq!(d.parking_budget_status(), 503);
        assert!(d.is_retryable_parking_budget_exhaustion(503, "no worker has room for the actor"));
        assert!(!d.is_retryable_parking_budget_exhaustion(504, "request timed out"));
    }

    #[test]
    fn egress_connect_stats() {
        let before = "# TYPE envoy_http_downstream_cx_http2_total counter\n\
envoy_http_downstream_cx_http2_total{envoy_http_conn_manager_prefix=\"egress_connect\"} 1\n\
envoy_http_downstream_rq_xx{envoy_response_code_class=\"2\",envoy_http_conn_manager_prefix=\"egress_connect\"} 2\n";
        let after = "# TYPE envoy_http_downstream_cx_http2_total counter\n\
envoy_http_downstream_cx_http2_total{envoy_http_conn_manager_prefix=\"egress_connect\"} 4\n\
envoy_http_downstream_rq_xx{envoy_response_code_class=\"2\",envoy_http_conn_manager_prefix=\"egress_connect\"} 252\n";
        assert_eq!(
            Dataplane::Envoy.egress_connect_stats(before, after),
            EgressConnectStats {
                connections: 3,
                requests: 250
            }
        );
    }

    #[test]
    fn platform_metric_prefixes() {
        let all = ["a", ROUTE_DURATION_PREFIX];
        assert_eq!(Dataplane::Envoy.platform_metric_prefixes(&all), all);
    }
}
