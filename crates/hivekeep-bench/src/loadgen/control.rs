//! The slice of the Control API the user classes call, as a trait so tests
//! can stand in for hivekeep-api.

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use hivekeep_proto::api::v1::control_client::ControlClient;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// The Control API calls a load class makes. Requests and responses keep
/// their metadata, so callers can attach trace context and read the
/// server-elapsed trailer.
#[async_trait]
pub trait ControlApi: Send + Sync + 'static {
    /// Creates a Hive.
    async fn create_hive(
        &self,
        req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status>;
    /// Creates an Actor.
    async fn create_actor(
        &self,
        req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status>;
    /// Resumes an Actor.
    async fn resume_actor(
        &self,
        req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status>;
    /// Suspends an Actor.
    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status>;
    /// Deletes an Actor.
    async fn delete_actor(
        &self,
        req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status>;
    /// Reads an ActorTemplate.
    async fn get_actor_template(
        &self,
        req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status>;
}

#[async_trait]
impl ControlApi for ControlClient<Channel> {
    async fn create_hive(
        &self,
        req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status> {
        self.clone().create_hive(req).await
    }
    async fn create_actor(
        &self,
        req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        self.clone().create_actor(req).await
    }
    async fn resume_actor(
        &self,
        req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status> {
        self.clone().resume_actor(req).await
    }
    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status> {
        self.clone().suspend_actor(req).await
    }
    async fn delete_actor(
        &self,
        req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        self.clone().delete_actor(req).await
    }
    async fn get_actor_template(
        &self,
        req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status> {
        self.clone().get_actor_template(req).await
    }
}
