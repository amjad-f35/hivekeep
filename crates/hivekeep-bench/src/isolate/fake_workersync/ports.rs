//! What the controller talks to: hivekeep-api's Worker registry, the
//! capacity relays on the fake keepers, and Kubernetes.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use hivekeep_k8s::crd::WorkerPool;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::control_client::ControlClient;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use tonic::transport::Channel;
use tonic::{Response, Status};

/// The part of the Control API the controller uses.
#[async_trait]
pub trait WorkerClient: Send + Sync + 'static {
    /// CreateWorker.
    async fn create_worker(&self, req: api::CreateWorkerRequest) -> Result<api::Worker, Status>;
    /// DeleteWorker.
    async fn delete_worker(&self, req: api::DeleteWorkerRequest) -> Result<api::Worker, Status>;
    /// DrainWorker.
    async fn drain_worker(
        &self,
        req: api::DrainWorkerRequest,
    ) -> Result<api::DrainWorkerResponse, Status>;
    /// MarkWorkerAvailable.
    async fn mark_worker_available(
        &self,
        req: api::MarkWorkerAvailableRequest,
    ) -> Result<api::MarkWorkerAvailableResponse, Status>;
    /// GetWorker.
    async fn get_worker(&self, req: api::GetWorkerRequest) -> Result<api::Worker, Status>;
    /// UpdateWorker.
    async fn update_worker(&self, req: api::UpdateWorkerRequest) -> Result<api::Worker, Status>;
    /// ListWorkers.
    async fn list_workers(
        &self,
        req: api::ListWorkersRequest,
    ) -> Result<api::ListWorkersResponse, Status>;
    /// ListWorkerActorAssignments.
    async fn list_worker_actor_assignments(
        &self,
        req: api::ListWorkerActorAssignmentsRequest,
    ) -> Result<api::ListWorkerActorAssignmentsResponse, Status>;
}

#[async_trait]
impl WorkerClient for ControlClient<Channel> {
    async fn create_worker(&self, req: api::CreateWorkerRequest) -> Result<api::Worker, Status> {
        ControlClient::create_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn delete_worker(&self, req: api::DeleteWorkerRequest) -> Result<api::Worker, Status> {
        ControlClient::delete_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn drain_worker(
        &self,
        req: api::DrainWorkerRequest,
    ) -> Result<api::DrainWorkerResponse, Status> {
        ControlClient::drain_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn mark_worker_available(
        &self,
        req: api::MarkWorkerAvailableRequest,
    ) -> Result<api::MarkWorkerAvailableResponse, Status> {
        ControlClient::mark_worker_available(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn get_worker(&self, req: api::GetWorkerRequest) -> Result<api::Worker, Status> {
        ControlClient::get_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn update_worker(&self, req: api::UpdateWorkerRequest) -> Result<api::Worker, Status> {
        ControlClient::update_worker(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn list_workers(
        &self,
        req: api::ListWorkersRequest,
    ) -> Result<api::ListWorkersResponse, Status> {
        ControlClient::list_workers(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
    async fn list_worker_actor_assignments(
        &self,
        req: api::ListWorkerActorAssignmentsRequest,
    ) -> Result<api::ListWorkerActorAssignmentsResponse, Status> {
        ControlClient::list_worker_actor_assignments(&mut self.clone(), req)
            .await
            .map(Response::into_inner)
    }
}

/// What a fake worker process registers and reports.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkerReport {
    /// The Worker's name.
    pub name: String,
    /// The Worker's registered epoch to register again, or 0 to acquire a
    /// new one. Reusing it keeps a restarted fake-workersync from raising
    /// the epoch, which would make hivekeep-api crash every Actor on the
    /// Worker.
    pub epoch: i64,
    /// The addresses it registers.
    pub ips: Vec<String>,
    /// Its capacity.
    pub capacity: Option<api::WorkerResources>,
    /// Its sandbox runtimes.
    pub sandbox_classes: Vec<api::WorkerSandboxClass>,
}

/// Registers a fake worker process through the fake-keeper relay at an
/// address: acquires an epoch if needed, registers, and reports runtimes.
#[async_trait]
pub trait CapacityRelay: Send + Sync + 'static {
    /// Reports `report` through the relay at `addr` (`host:port`).
    async fn report(&self, addr: &str, report: WorkerReport) -> Result<(), Status>;
    /// Lets go of every relay not in `live`, the relay addresses by node.
    fn prune(&self, live: &HashMap<String, String>);
}

/// A fake-keeper node and what it can allocate.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Node {
    /// Node name.
    pub name: String,
    /// Its allocatable resources.
    pub allocatable: BTreeMap<String, Quantity>,
}

/// What the controller reads from and writes to Kubernetes.
#[async_trait]
pub trait Cluster: Send + Sync + 'static {
    /// Every WorkerPool.
    async fn pools(&self) -> Result<Vec<WorkerPool>, String>;
    /// The nodes that run a fake-keeper pod, sorted by name.
    async fn nodes(&self) -> Result<Vec<Node>, String>;
    /// The fake-keeper relay address on each node that has one.
    async fn relays(&self) -> Result<HashMap<String, String>, String>;
    /// Writes a WorkerPool's status.
    async fn update_status(&self, wp: &WorkerPool) -> Result<(), String>;
}
