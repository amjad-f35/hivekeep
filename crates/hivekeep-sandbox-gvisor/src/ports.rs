//! What the worker needs from the rest of the node, as traits: the tunnel
//! and sandbox networking, crash reporting, usage records and registration.
//!
//! The defaults here keep a standalone worker functional (direct dialing, log
//! output); deployments plug in the node agent's implementations.

use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use tokio::net::TcpStream;

use crate::error::{status, Result};
use crate::phaselog::actor_fields;

/// Opens a connection into an Actor's sandbox.
pub type Dialer = Arc<dyn Fn(String) -> BoxFuture<'static, io::Result<TcpStream>> + Send + Sync>;

/// A dialer that connects straight to the address.
pub fn direct_dialer() -> Dialer {
    Arc::new(|addr| Box::pin(async move { TcpStream::connect(addr).await }))
}

/// An Actor's sandbox network, from setup until it is closed.
#[async_trait]
pub trait SandboxNetwork: Send + Sync {
    /// Opens a connection into the sandbox.
    fn dialer(&self) -> Dialer;
    /// Tears the network down.
    async fn close(&self) -> Result<()>;
}

/// An egress activation prepared ahead of the sandbox starting.
#[derive(Debug, Clone, Default)]
pub struct Egress {
    /// The gateway the Actor's egress is tunnelled to, if any.
    pub gateway: Option<String>,
}

/// The tunnel and per-Actor networking. Egress is activated before the
/// sandbox starts, ingress once its wakeup probes pass.
#[async_trait]
pub trait Tunnel: Send + Sync {
    /// Stops all traffic for the Actor. Repeated calls are fine.
    async fn deactivate(&self, a: &ActorAttribution) -> Result<()>;
    /// Prepares the Actor's egress through `gateway`.
    async fn prepare_egress(
        &self,
        a: &ActorAttribution,
        gateway: Option<&pb::EgressGateway>,
    ) -> Result<Egress>;
    /// Activates prepared egress.
    fn activate_egress(&self, a: &ActorAttribution, egress: Egress) -> Result<()>;
    /// Routes ingress to the Actor through `dialer`.
    fn activate_ingress(&self, a: &ActorAttribution, dialer: Dialer) -> Result<()>;
    /// Sets up the Actor's sandbox network.
    async fn serve_sandbox(&self, a: &ActorAttribution) -> Result<Arc<dyn SandboxNetwork>>;
}

/// The network of a worker without a tunnel: sandboxes are dialed directly.
#[derive(Debug, Default)]
pub struct DirectNetwork;

#[async_trait]
impl SandboxNetwork for DirectNetwork {
    fn dialer(&self) -> Dialer {
        direct_dialer()
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// A tunnel that routes nothing and dials sandboxes directly.
#[derive(Debug, Default)]
pub struct NoTunnel;

#[async_trait]
impl Tunnel for NoTunnel {
    async fn deactivate(&self, _: &ActorAttribution) -> Result<()> {
        Ok(())
    }

    async fn prepare_egress(
        &self,
        _: &ActorAttribution,
        gateway: Option<&pb::EgressGateway>,
    ) -> Result<Egress> {
        Ok(Egress {
            gateway: gateway.map(|g| g.address.clone()),
        })
    }

    fn activate_egress(&self, _: &ActorAttribution, _: Egress) -> Result<()> {
        Ok(())
    }

    fn activate_ingress(&self, _: &ActorAttribution, _: Dialer) -> Result<()> {
        Ok(())
    }

    async fn serve_sandbox(&self, _: &ActorAttribution) -> Result<Arc<dyn SandboxNetwork>> {
        Ok(Arc::new(DirectNetwork))
    }
}

/// Reports Actors whose sandbox died on its own.
#[async_trait]
pub trait CrashReporter: Send + Sync {
    /// Reports the Actor crashed, with what was observed.
    async fn report_crashed(&self, a: &ActorAttribution, message: &str) -> Result<()>;
}

/// Which usage record a sample is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageKind {
    /// The first reading of an activation.
    Initial,
    /// A reading on the sampling timer.
    Periodic,
    /// The last reading, once the activation ended.
    Final,
}

impl UsageKind {
    /// The record's kind label.
    pub fn as_str(self) -> &'static str {
        match self {
            UsageKind::Initial => "initial",
            UsageKind::Periodic => "periodic",
            UsageKind::Final => "final",
        }
    }
}

/// Where usage records go.
pub trait UsageSink: Send + Sync {
    /// Writes one record.
    fn emit(&self, kind: UsageKind, sample: &pb::WorkloadStatsSample);
}

/// Writes usage records as structured log lines.
#[derive(Debug, Default, Clone)]
pub struct LogUsageSink {
    /// The worker pool's namespace, when known.
    pub pool_namespace: String,
    /// The worker pool's name, when known.
    pub pool_name: String,
}

impl UsageSink for LogUsageSink {
    fn emit(&self, kind: UsageKind, s: &pb::WorkloadStatsSample) {
        tracing::info!(
            target: "hivekeep::usage",
            kind = kind.as_str(),
            pool_namespace = %self.pool_namespace,
            pool = %self.pool_name,
            hive = %s.hive,
            actor = %s.actor_name,
            actor_uid = %s.actor_uid,
            source = s.source,
            memory_current_bytes = s.memory_current_bytes,
            memory_peak_bytes = s.memory_peak_bytes,
            memory_working_set_bytes = s.memory_working_set_bytes,
            cpu_usage_usec = s.cpu_usage_usec,
            observed_at_unix_nano = s.observed_at_unix_nano,
            epoch_unix_nano = s.epoch_unix_nano,
            "hk.actor.usage_sampled"
        );
    }
}

/// The epoch this worker process registered with. No workload starts before
/// registration: the control plane could not tell which process ran it.
#[derive(Debug, Default)]
pub struct Epoch(AtomicI64);

impl Epoch {
    /// Records the registered epoch.
    pub fn store(&self, epoch: i64) {
        self.0.store(epoch, Ordering::SeqCst);
    }

    /// Whether registration finished.
    pub fn registered(&self) -> bool {
        self.0.load(Ordering::SeqCst) > 0
    }

    /// The epoch, or UNAVAILABLE so the caller retries after registration.
    pub fn require(&self) -> Result<i64, tonic::Status> {
        match self.0.load(Ordering::SeqCst) {
            e if e > 0 => Ok(e),
            _ => Err(status::unavailable("worker has not registered yet")),
        }
    }
}

/// Logs an Actor lifecycle event.
pub fn lifecycle_log(msg: &str, a: &ActorAttribution) {
    let fields = serde_json::Value::Object(actor_fields(a));
    tracing::info!(target: "hivekeep::actor", actor = %fields, "{msg}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_gates_startups() {
        let e = Epoch::default();
        assert_eq!(e.require().unwrap_err().code(), tonic::Code::Unavailable);
        assert!(!e.registered());
        e.store(7);
        assert_eq!(e.require().unwrap(), 7);
    }
}
