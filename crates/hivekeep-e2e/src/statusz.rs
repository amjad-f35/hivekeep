//! The gate router's `/statusz` page, read over a port-forward, for suites
//! that assert on router-internal state (the request-parking gauge)
//! without going through the metrics pipeline.

use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::collector_metrics::reqwest_like;
use crate::env::{resource_name, system_namespace};
use crate::install;
use crate::portforward::{self, Forward};

/// The `parking` section of `/statusz?format=json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ParkingStatusz {
    /// Whether parking is on.
    #[serde(default)]
    pub enabled: bool,
    /// Requests parked right now.
    #[serde(default)]
    pub active: i64,
    /// The parking limit.
    #[serde(default)]
    pub max_parked: i64,
    /// The longest a request may wait.
    #[serde(default)]
    pub max_wait: String,
}

/// Reads the router's status page. The status port is loopback-only in the
/// pod and not published by the Service, so the forward targets a router
/// pod directly.
pub struct StatuszClient {
    forward: Forward,
}

impl StatuszClient {
    /// Opens a forward to the router's status port.
    pub async fn new(kube: &kube::Client) -> Result<Self, String> {
        let forward = portforward::service_pod_port_forward(
            kube,
            &system_namespace(),
            &resource_name(install::ROUTER_SERVICE),
            install::ROUTER_STATUS_PORT,
        )
        .await?;
        Ok(StatuszClient { forward })
    }

    /// The current request-parking snapshot.
    pub async fn parking(&self) -> Result<ParkingStatusz, String> {
        let (status, body) = reqwest_like::Client::new(Duration::from_secs(10))
            .get(&format!(
                "http://{}/statusz?format=json",
                self.forward.addr()
            ))
            .await?;
        if status != 200 {
            return Err(format!("statusz returned {status}"));
        }
        parse_parking(&body)
    }

    /// Polls the parking gauge until `cond` holds (4 s budget).
    pub async fn wait_for_count(&self, cond: impl Fn(i64) -> bool) -> Result<i64, String> {
        let deadline = Instant::now() + Duration::from_secs(4);
        while Instant::now() < deadline {
            if let Ok(p) = self.parking().await {
                if cond(p.active) {
                    return Ok(p.active);
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Err("timed out waiting for the parking gauge to satisfy the condition".into())
    }
}

fn parse_parking(body: &str) -> Result<ParkingStatusz, String> {
    #[derive(Deserialize)]
    struct Dashboard {
        #[serde(default)]
        parking: ParkingStatusz,
    }
    serde_json::from_str::<Dashboard>(body)
        .map(|d| d.parking)
        .map_err(|e| format!("decoding statusz JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_parking_section() {
        let got = parse_parking(
            r#"{"parking":{"enabled":true,"active":2,"max_parked":10,"max_wait":"30s"},"other":1}"#,
        )
        .unwrap();
        assert_eq!(
            got,
            ParkingStatusz {
                enabled: true,
                active: 2,
                max_parked: 10,
                max_wait: "30s".into()
            }
        );
        assert!(parse_parking("not json").is_err());
    }
}
