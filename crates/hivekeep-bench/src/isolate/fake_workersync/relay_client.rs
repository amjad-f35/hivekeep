//! Sends capacity reports to the fake-keeper relay on a Worker's node, as a
//! worker sends them to the keeper on its own node. A rejected report
//! carries hivekeep-api's status unchanged.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use parking_lot::Mutex;
use rustls::ClientConfig;
use tonic::transport::Channel;
use tonic::Status;

use super::ports::{CapacityRelay, WorkerReport};
use crate::tls::client::{client_config, lazy_channel, PeerCheck};
use crate::tls::TlsError;

/// Bounds one capacity report, relay hop included.
pub const REPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// The gRPC capacity relay client.
pub struct GrpcRelay {
    tls: Arc<ClientConfig>,
    /// One channel per relay address. There is one relay per fake node, so
    /// the map stays small.
    pub(crate) conns: Mutex<HashMap<String, Channel>>,
}

impl GrpcRelay {
    /// Presents this pod's certificate and accepts only a relay whose
    /// certificate chains to the pod-identity trust bundle and carries the
    /// keeper's SPIFFE ID. The relay is dialed by pod IP and its certificate
    /// has no DNS or IP SAN, so the chain is verified here rather than by
    /// name. The trust bundle is read at each handshake.
    pub fn new(
        client_bundle: &str,
        trust_bundle: &str,
        keeper_id: &str,
    ) -> Result<GrpcRelay, TlsError> {
        Ok(GrpcRelay {
            tls: client_config(
                client_bundle,
                trust_bundle,
                PeerCheck::SpiffeId(keeper_id.to_string()),
            )?,
            conns: Mutex::new(HashMap::new()),
        })
    }

    fn conn(&self, addr: &str) -> Result<Channel, Status> {
        let mut conns = self.conns.lock();
        if let Some(c) = conns.get(addr) {
            return Ok(c.clone());
        }
        let c = lazy_channel(addr, self.tls.clone())
            .map_err(|e| Status::internal(format!("dialing relay {addr}: {e}")))?;
        conns.insert(addr.to_string(), c.clone());
        Ok(c)
    }

    /// Drops every relay connection.
    pub fn close(&self) {
        self.conns.lock().clear();
    }

    async fn send(&self, addr: &str, report: WorkerReport) -> Result<(), Status> {
        let mut client = WorkerServiceClient::new(self.conn(addr)?);
        let worker = Some(api::ObjectRef {
            hive: String::new(),
            name: report.name.clone(),
        });
        let mut epoch = report.epoch;
        if epoch == 0 {
            epoch = client
                .acquire_worker_epoch(api::AcquireWorkerEpochRequest {
                    worker: worker.clone(),
                })
                .await?
                .into_inner()
                .epoch;
        }
        client
            .register_worker(api::RegisterWorkerRequest {
                worker: worker.clone(),
                epoch,
                ips: report.ips,
                capacity: report.capacity,
            })
            .await?;
        client
            .set_worker_sandbox_runtimes(api::SetWorkerSandboxRuntimesRequest {
                worker,
                epoch,
                sandbox_classes: report.sandbox_classes,
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl CapacityRelay for GrpcRelay {
    async fn report(&self, addr: &str, report: WorkerReport) -> Result<(), Status> {
        match tokio::time::timeout(REPORT_TIMEOUT, self.send(addr, report)).await {
            Ok(r) => r,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "capacity report via {addr} timed out"
            ))),
        }
    }

    fn prune(&self, live: &HashMap<String, String>) {
        let keep: HashSet<&String> = live.values().collect();
        self.conns.lock().retain(|addr, _| keep.contains(addr));
    }
}

#[cfg(test)]
#[path = "relay_client_tests.rs"]
mod tests;
