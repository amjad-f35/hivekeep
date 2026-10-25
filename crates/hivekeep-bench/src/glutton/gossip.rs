//! Gossip: periodic Ping traffic to peer gluttons, one loop per peer.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::metrics::Metrics;
use super::service::Service;
use crate::pb::glutton::v1 as pb;
use crate::pb::glutton::v1::glutton_client::GluttonClient;

/// One running peer loop.
pub(crate) struct PeerGossip {
    pub(crate) delay_ms: i32,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl PeerGossip {
    /// Asks the loop to stop without waiting.
    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Stops the loop and waits for it to exit.
    pub(crate) async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

impl Service {
    /// Makes the set of peer loops match `req`: loops for peers no longer
    /// listed, or listed with a new delay, are stopped; new peers get a loop
    /// sending one Ping every `delay_ms`.
    pub async fn gossip(&self, req: &pb::GossipRequest) -> Result<pb::GossipResponse, Status> {
        let mut want: HashMap<String, i32> = HashMap::with_capacity(req.peers.len());
        for p in &req.peers {
            if p.host.is_empty() {
                return Err(Status::invalid_argument("peer host is required"));
            }
            if p.delay_ms <= 0 {
                return Err(Status::invalid_argument(format!(
                    "peer {:?} delay_ms must be positive",
                    p.host
                )));
            }
            want.insert(p.host.clone(), p.delay_ms);
        }

        let mut to_stop = Vec::new();
        let mut to_start = Vec::new();
        {
            let mut peers = self.peers.lock();
            let gone: Vec<String> = peers
                .iter()
                .filter(|(host, existing)| want.get(*host) != Some(&existing.delay_ms))
                .map(|(host, _)| host.clone())
                .collect();
            for host in gone {
                if let Some(p) = peers.remove(&host) {
                    self.metrics.gossip_delay.remove_label_values(&[&host]).ok();
                    to_stop.push(p);
                }
            }
            for (host, delay) in &want {
                if !peers.contains_key(host) {
                    to_start.push((host.clone(), *delay));
                }
            }
        }
        for p in to_stop {
            p.stop().await;
        }
        for (host, delay_ms) in to_start {
            let cancel = CancellationToken::new();
            let task = tokio::spawn(run_gossip(
                self.metrics.clone(),
                host.clone(),
                delay_ms,
                cancel.clone(),
            ));
            self.metrics
                .gossip_delay
                .with_label_values(&[&host])
                .set(delay_ms as i64);
            self.peers.lock().insert(
                host,
                PeerGossip {
                    delay_ms,
                    cancel,
                    task,
                },
            );
        }
        Ok(pb::GossipResponse {})
    }
}

/// Pings `host` every `delay_ms` until cancelled. The channel connects
/// lazily, so a peer need not be reachable when gossip starts; each failed
/// Ping is logged and counted.
async fn run_gossip(metrics: Metrics, host: String, delay_ms: i32, cancel: CancellationToken) {
    let endpoint = match tonic::transport::Endpoint::from_shared(format!("http://{host}")) {
        Ok(e) => e,
        Err(err) => {
            tracing::error!(%host, %err, "Failed to dial gossip peer");
            return;
        }
    };
    let mut client = GluttonClient::new(endpoint.connect_lazy());
    let period = Duration::from_millis(delay_ms as u64);
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let msg = uuid::Uuid::new_v4().to_string();
        let start = Instant::now();
        let result = tokio::select! {
            r = client.ping(pb::PingRequest { message: msg.clone() }) => Some(r),
            _ = cancel.cancelled() => None,
        };
        let latency = start.elapsed().as_secs_f64();
        let outcome = match &result {
            None => "cancelled",
            Some(Err(_)) => "error",
            Some(Ok(_)) => "ok",
        };
        metrics
            .gossip_sent
            .with_label_values(&[&host, outcome])
            .inc();
        metrics.gossip_latency_for(&host, outcome).observe(latency);
        match result {
            None => return,
            Some(Err(err)) => tracing::warn!(%host, %err, "Gossip ping failed"),
            Some(Ok(resp)) => {
                let got = resp.into_inner().message;
                if got != msg {
                    tracing::warn!(%host, sent = %msg, received = %got, "Gossip ping returned unexpected message");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glutton::service::tests::new_service;

    fn peer(host: &str, delay_ms: i32) -> pb::Peer {
        pb::Peer {
            host: host.into(),
            delay_ms,
        }
    }

    #[tokio::test]
    async fn gossip_validates_and_reconciles_peers() {
        let (svc, _d) = new_service();
        let bad = svc
            .gossip(&pb::GossipRequest {
                peers: vec![peer("", 10)],
            })
            .await
            .unwrap_err();
        assert_eq!(bad.code(), tonic::Code::InvalidArgument);
        let bad = svc
            .gossip(&pb::GossipRequest {
                peers: vec![peer("a:1", 0)],
            })
            .await
            .unwrap_err();
        assert_eq!(bad.code(), tonic::Code::InvalidArgument);

        svc.gossip(&pb::GossipRequest {
            peers: vec![peer("127.0.0.1:1", 1000), peer("127.0.0.1:2", 1000)],
        })
        .await
        .unwrap();
        assert_eq!(svc.peers.lock().len(), 2);
        svc.gossip(&pb::GossipRequest {
            peers: vec![peer("127.0.0.1:1", 500)],
        })
        .await
        .unwrap();
        {
            let peers = svc.peers.lock();
            assert_eq!(peers.len(), 1);
            assert_eq!(peers["127.0.0.1:1"].delay_ms, 500);
        }
        svc.close().await;
        assert!(svc.peers.lock().is_empty());
    }
}
