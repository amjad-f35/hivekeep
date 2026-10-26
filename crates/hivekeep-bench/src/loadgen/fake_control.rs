//! An in-memory stand-in for hivekeep-api's Control API, for user-class
//! tests. Every call is recorded by name; each method can be scripted to
//! fail with a queue of statuses, drained one per call.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use super::control::ControlApi;
use crate::wire::SERVER_ELAPSED_HEADER;

/// Decides the outcome of a CreateActor for the named Actor.
pub type CreateActorFn = Arc<dyn Fn(&str) -> Result<(), Status> + Send + Sync>;

/// The recorded calls and scripted failures.
#[derive(Default)]
pub struct State {
    /// Call names in order: `CreateHive`, `CreateActor`, `ResumeActor`,
    /// `SuspendActor` or `SuspendActor(async)`, `DeleteActor`,
    /// `GetActorTemplate`.
    pub calls: Vec<String>,
    /// CreateActor requests.
    pub create_reqs: Vec<api::CreateActorRequest>,
    /// ResumeActor requests.
    pub resume_reqs: Vec<api::ResumeActorRequest>,
    /// SuspendActor requests.
    pub suspend_reqs: Vec<api::SuspendActorRequest>,
    /// DeleteActor requests.
    pub delete_reqs: Vec<api::DeleteActorRequest>,
    /// Metadata of every request, in call order.
    pub metadata: Vec<tonic::metadata::MetadataMap>,
    /// Failures for successive CreateHive calls.
    pub create_hive_errs: VecDeque<Status>,
    /// Failures for successive CreateActor calls.
    pub create_errs: VecDeque<Status>,
    /// Failures for successive ResumeActor calls.
    pub resume_errs: VecDeque<Status>,
    /// Failures for successive SuspendActor calls.
    pub suspend_errs: VecDeque<Status>,
    /// Failures for successive DeleteActor calls.
    pub delete_errs: VecDeque<Status>,
    /// Failures for successive GetActorTemplate calls.
    pub template_errs: VecDeque<Status>,
    /// Called with each CreateActor's Actor name, after `create_errs`.
    pub create_actor_fn: Option<CreateActorFn>,
    /// What GetActorTemplate returns.
    pub template: Option<api::ActorTemplate>,
    /// When set, every answer carries this server-elapsed value.
    pub elapsed_us: Option<String>,
}

/// The fake. Clone the `Arc` to share it between a test and a class.
#[derive(Default)]
pub struct FakeControl {
    /// Everything recorded and scripted.
    pub state: Mutex<State>,
}

impl FakeControl {
    /// A fake whose every call succeeds.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The call names so far.
    pub fn calls(&self) -> Vec<String> {
        self.state.lock().calls.clone()
    }

    /// Runs `f` on the state.
    pub fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.state.lock())
    }

    fn answer<T>(&self, v: T) -> Response<T> {
        let mut r = Response::new(v);
        if let Some(us) = self.state.lock().elapsed_us.clone() {
            r.metadata_mut()
                .insert(SERVER_ELAPSED_HEADER, us.parse().unwrap());
        }
        r
    }
}

/// The name a SuspendActor is recorded under: async told apart from sync.
pub fn suspend_call_name(req: &api::SuspendActorRequest) -> &'static str {
    let mode = req.options.as_ref().map(|o| o.mode()).unwrap_or_default();
    if mode == api::DurabilityMode::Asynchronous {
        "SuspendActor(async)"
    } else {
        "SuspendActor"
    }
}

#[async_trait]
impl ControlApi for FakeControl {
    async fn create_hive(
        &self,
        req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status> {
        let err = {
            let mut s = self.state.lock();
            s.calls.push("CreateHive".into());
            s.metadata.push(req.metadata().clone());
            s.create_hive_errs.pop_front()
        };
        match err {
            Some(e) => Err(e),
            None => Ok(self.answer(api::Hive::default())),
        }
    }

    async fn create_actor(
        &self,
        req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let (err, hook, name) = {
            let mut s = self.state.lock();
            s.calls.push("CreateActor".into());
            s.metadata.push(req.metadata().clone());
            let inner = req.get_ref().clone();
            let name = inner
                .actor
                .as_ref()
                .and_then(|a| a.metadata.as_ref())
                .map(|m| m.name.clone())
                .unwrap_or_default();
            s.create_reqs.push(inner);
            (s.create_errs.pop_front(), s.create_actor_fn.clone(), name)
        };
        if let Some(e) = err {
            return Err(e);
        }
        if let Some(f) = hook {
            f(&name)?;
        }
        Ok(self.answer(api::Actor::default()))
    }

    async fn resume_actor(
        &self,
        req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status> {
        let err = {
            let mut s = self.state.lock();
            s.calls.push("ResumeActor".into());
            s.metadata.push(req.metadata().clone());
            s.resume_reqs.push(req.get_ref().clone());
            s.resume_errs.pop_front()
        };
        match err {
            Some(e) => Err(e),
            None => Ok(self.answer(api::ResumeActorResponse::default())),
        }
    }

    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status> {
        let err = {
            let mut s = self.state.lock();
            s.calls.push(suspend_call_name(req.get_ref()).into());
            s.metadata.push(req.metadata().clone());
            s.suspend_reqs.push(req.get_ref().clone());
            s.suspend_errs.pop_front()
        };
        match err {
            Some(e) => Err(e),
            None => Ok(self.answer(api::SuspendActorResponse::default())),
        }
    }

    async fn delete_actor(
        &self,
        req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let err = {
            let mut s = self.state.lock();
            s.calls.push("DeleteActor".into());
            s.metadata.push(req.metadata().clone());
            s.delete_reqs.push(req.get_ref().clone());
            s.delete_errs.pop_front()
        };
        match err {
            Some(e) => Err(e),
            None => Ok(self.answer(api::Actor::default())),
        }
    }

    async fn get_actor_template(
        &self,
        req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status> {
        let (err, t) = {
            let mut s = self.state.lock();
            s.calls.push("GetActorTemplate".into());
            s.metadata.push(req.metadata().clone());
            (s.template_errs.pop_front(), s.template.clone())
        };
        match (err, t) {
            (Some(e), _) => Err(e),
            (None, Some(t)) => Ok(self.answer(t)),
            (None, None) => Err(Status::not_found("ActorTemplate not found")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_and_scripts() {
        let f = FakeControl::new();
        f.with(|s| {
            s.resume_errs.push_back(Status::unavailable("down"));
            s.elapsed_us = Some("12".into());
        });
        assert!(f
            .resume_actor(Request::new(Default::default()))
            .await
            .is_err());
        let ok = f
            .resume_actor(Request::new(Default::default()))
            .await
            .unwrap();
        assert_eq!(ok.metadata().get(SERVER_ELAPSED_HEADER).unwrap(), "12");
        let async_req = api::SuspendActorRequest {
            options: Some(api::SuspendOptions {
                mode: api::DurabilityMode::Asynchronous as i32,
            }),
            ..Default::default()
        };
        f.suspend_actor(Request::new(async_req)).await.unwrap();
        f.suspend_actor(Request::new(Default::default()))
            .await
            .unwrap();
        assert!(f
            .get_actor_template(Request::new(Default::default()))
            .await
            .is_err());
        assert_eq!(
            f.calls(),
            vec![
                "ResumeActor",
                "ResumeActor",
                "SuspendActor(async)",
                "SuspendActor",
                "GetActorTemplate"
            ]
        );
    }
}
