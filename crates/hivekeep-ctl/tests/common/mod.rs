//! An in-memory Control server for exercising hivectl end to end.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use clap::Parser;
use hivekeep_ctl::app;
use hivekeep_ctl::cli::Cli;
use hivekeep_ctl::error::CliError;
use hivekeep_ctl::io::Io;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_server::{Control, ControlServer};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

type Key = (String, String);

fn key(r: &Option<pb::ObjectRef>) -> Key {
    let r = r.clone().unwrap_or_default();
    (r.hive, r.name)
}

fn meta_key(m: &Option<pb::ResourceMetadata>) -> Key {
    let m = m.clone().unwrap_or_default();
    (m.hive, m.name)
}

/// A recorded call.
#[derive(Debug, Clone)]
pub struct Call {
    /// RPC name, e.g. `GetActor`.
    pub method: String,
    /// The request's metadata, as (key, value) text pairs.
    pub metadata: HashMap<String, String>,
    /// The request, as Debug text.
    pub request: String,
}

/// The server's state, which tests seed and inspect.
#[derive(Default)]
pub struct State {
    pub actors: BTreeMap<Key, pb::Actor>,
    pub templates: BTreeMap<Key, pb::ActorTemplate>,
    pub tags: BTreeMap<Key, pb::Tag>,
    pub hives: BTreeMap<String, pb::Hive>,
    pub workers: BTreeMap<String, pb::Worker>,
    pub assignments: BTreeMap<String, Vec<pb::ActorAssignment>>,
    pub egress: BTreeMap<Key, pb::EgressPolicy>,
    pub hive_policies: BTreeMap<String, pb::AccessPolicy>,
    pub global_policy: Option<pb::AccessPolicy>,
    /// Errors to return, by RPC name, once each.
    pub errors: HashMap<String, Vec<Status>>,
    /// Every call, in order.
    pub calls: Vec<Call>,
    /// Items per List page; 0 means all at once.
    pub page_size: usize,
    /// Canned replies to return, by RPC name, once each, ahead of the
    /// server's own logic.
    pub replies: HashMap<String, Vec<Vec<u8>>>,
}

impl State {
    pub fn fail(&mut self, method: &str, status: Status) {
        self.errors
            .entry(method.to_string())
            .or_default()
            .push(status);
    }

    /// Queues `msg` as the next reply to `method`.
    pub fn reply<M: prost::Message>(&mut self, method: &str, msg: &M) {
        self.replies
            .entry(method.to_string())
            .or_default()
            .push(msg.encode_to_vec());
    }

    fn take_reply<M: prost::Message + Default>(&mut self, method: &str) -> Option<M> {
        let queue = self.replies.get_mut(method)?;
        if queue.is_empty() {
            return None;
        }
        Some(M::decode(queue.remove(0).as_slice()).expect("canned reply decodes"))
    }

    pub fn calls_to(&self, method: &str) -> Vec<Call> {
        self.calls
            .iter()
            .filter(|c| c.method == method)
            .cloned()
            .collect()
    }
}

/// The fake server.
#[derive(Clone, Default)]
pub struct Fake(pub Arc<Mutex<State>>);

impl Fake {
    pub fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }

    fn begin<T: std::fmt::Debug>(
        &self,
        method: &str,
        req: &Request<T>,
    ) -> Result<MutexGuard<'_, State>, Status> {
        let mut s = self.state();
        let metadata = req
            .metadata()
            .iter()
            .filter_map(|kv| match kv {
                tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                    Some((k.to_string(), v.to_str().unwrap_or_default().to_string()))
                }
                _ => None,
            })
            .collect();
        s.calls.push(Call {
            method: method.to_string(),
            metadata,
            request: format!("{:?}", req.get_ref()),
        });
        if let Some(errs) = s.errors.get_mut(method) {
            if !errs.is_empty() {
                return Err(errs.remove(0));
            }
        }
        Ok(s)
    }
}

fn page<T: Clone>(items: Vec<T>, token: &str, size: usize) -> (Vec<T>, String) {
    if size == 0 {
        return (items, String::new());
    }
    let start: usize = token.parse().unwrap_or(0);
    let end = (start + size).min(items.len());
    let next = if end < items.len() {
        end.to_string()
    } else {
        String::new()
    };
    (items[start.min(items.len())..end].to_vec(), next)
}

fn token(o: &Option<pb::ListOptions>) -> String {
    o.clone().unwrap_or_default().page_token
}

fn not_found(what: &str) -> Status {
    Status::not_found(format!("{what} not found"))
}

fn bump(m: &mut Option<pb::ResourceMetadata>) {
    let m = m.get_or_insert_with(Default::default);
    m.version += 1;
    if m.uid.is_empty() {
        m.uid = "00000000-0000-4000-8000-000000000001".into();
    }
}

type R<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl Control for Fake {
    async fn get_actor(&self, req: Request<pb::GetActorRequest>) -> R<pb::Actor> {
        let mut s = self.begin("GetActor", &req)?;
        if let Some(r) = s.take_reply::<pb::Actor>("GetActor") {
            return Ok(Response::new(r));
        }
        s.actors
            .get(&key(&req.get_ref().actor))
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("actor"))
    }
    async fn create_actor(&self, req: Request<pb::CreateActorRequest>) -> R<pb::Actor> {
        let mut s = self.begin("CreateActor", &req)?;
        let mut a = req.into_inner().actor.unwrap_or_default();
        bump(&mut a.metadata);
        a.status = Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            ..Default::default()
        });
        s.actors.insert(meta_key(&a.metadata), a.clone());
        Ok(Response::new(a))
    }
    async fn update_actor(&self, req: Request<pb::UpdateActorRequest>) -> R<pb::Actor> {
        let mut s = self.begin("UpdateActor", &req)?;
        if let Some(r) = s.take_reply::<pb::Actor>("UpdateActor") {
            return Ok(Response::new(r));
        }
        let mut a = req.into_inner().actor.unwrap_or_default();
        let k = meta_key(&a.metadata);
        if !s.actors.contains_key(&k) {
            return Err(not_found("actor"));
        }
        bump(&mut a.metadata);
        s.actors.insert(k, a.clone());
        Ok(Response::new(a))
    }
    async fn suspend_actor(
        &self,
        req: Request<pb::SuspendActorRequest>,
    ) -> R<pb::SuspendActorResponse> {
        let mut s = self.begin("SuspendActor", &req)?;
        let a = s
            .actors
            .get_mut(&key(&req.get_ref().actor))
            .ok_or_else(|| not_found("actor"))?;
        a.status = Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            ..Default::default()
        });
        Ok(Response::new(pb::SuspendActorResponse {
            actor: Some(a.clone()),
        }))
    }
    async fn resume_actor(
        &self,
        req: Request<pb::ResumeActorRequest>,
    ) -> R<pb::ResumeActorResponse> {
        let mut s = self.begin("ResumeActor", &req)?;
        let a = s
            .actors
            .get_mut(&key(&req.get_ref().actor))
            .ok_or_else(|| not_found("actor"))?;
        a.status.get_or_insert_with(Default::default).state = pb::ActorState::Running as i32;
        Ok(Response::new(pb::ResumeActorResponse {
            actor: Some(a.clone()),
            resumed: true,
        }))
    }
    async fn revert_actor(
        &self,
        req: Request<pb::RevertActorRequest>,
    ) -> R<pb::RevertActorResponse> {
        let mut s = self.begin("RevertActor", &req)?;
        let a = s
            .actors
            .get_mut(&key(&req.get_ref().actor))
            .ok_or_else(|| not_found("actor"))?;
        a.status.get_or_insert_with(Default::default).state = pb::ActorState::Suspended as i32;
        Ok(Response::new(pb::RevertActorResponse {
            actor: Some(a.clone()),
        }))
    }
    async fn delete_actor(&self, req: Request<pb::DeleteActorRequest>) -> R<pb::Actor> {
        let mut s = self.begin("DeleteActor", &req)?;
        s.actors
            .remove(&key(&req.get_ref().actor))
            .map(Response::new)
            .ok_or_else(|| not_found("actor"))
    }
    async fn get_actor_egress_policy(
        &self,
        req: Request<pb::GetActorEgressPolicyRequest>,
    ) -> R<pb::EgressPolicy> {
        let mut s = self.begin("GetActorEgressPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::EgressPolicy>("GetActorEgressPolicy") {
            return Ok(Response::new(r));
        }
        s.egress
            .get(&key(&req.get_ref().actor))
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("egress policy"))
    }
    async fn create_actor_egress_policy(
        &self,
        req: Request<pb::CreateActorEgressPolicyRequest>,
    ) -> R<pb::EgressPolicy> {
        let mut s = self.begin("CreateActorEgressPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::EgressPolicy>("CreateActorEgressPolicy") {
            return Ok(Response::new(r));
        }
        let r = req.into_inner();
        let k = key(&r.actor);
        if !s.actors.contains_key(&k) {
            return Err(not_found("actor"));
        }
        let mut p = r.egress_policy.unwrap_or_default();
        bump(&mut p.metadata);
        s.egress.insert(k, p.clone());
        Ok(Response::new(p))
    }
    async fn update_actor_egress_policy(
        &self,
        req: Request<pb::UpdateActorEgressPolicyRequest>,
    ) -> R<pb::EgressPolicy> {
        let mut s = self.begin("UpdateActorEgressPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::EgressPolicy>("UpdateActorEgressPolicy") {
            return Ok(Response::new(r));
        }
        let r = req.into_inner();
        let k = key(&r.actor);
        let stored = s.egress.get(&k).ok_or_else(|| not_found("egress policy"))?;
        let mut p = r.egress_policy.unwrap_or_default();
        if p.metadata.as_ref().map(|m| m.version) != stored.metadata.as_ref().map(|m| m.version) {
            return Err(Status::aborted("version mismatch"));
        }
        bump(&mut p.metadata);
        s.egress.insert(k, p.clone());
        Ok(Response::new(p))
    }
    async fn delete_actor_egress_policy(
        &self,
        req: Request<pb::DeleteActorEgressPolicyRequest>,
    ) -> R<pb::EgressPolicy> {
        let mut s = self.begin("DeleteActorEgressPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::EgressPolicy>("DeleteActorEgressPolicy") {
            return Ok(Response::new(r));
        }
        s.egress
            .remove(&key(&req.get_ref().actor))
            .map(Response::new)
            .ok_or_else(|| not_found("egress policy"))
    }
    async fn mint_actor_jwt(
        &self,
        _: Request<pb::MintActorJwtRequest>,
    ) -> R<pb::MintActorJwtResponse> {
        Err(Status::unimplemented("MintActorJwt"))
    }
    async fn mint_actor_certificate(
        &self,
        _: Request<pb::MintActorCertificateRequest>,
    ) -> R<pb::MintActorCertificateResponse> {
        Err(Status::unimplemented("MintActorCertificate"))
    }
    async fn create_tag(&self, req: Request<pb::CreateTagRequest>) -> R<pb::Tag> {
        let mut s = self.begin("CreateTag", &req)?;
        let mut t = req.into_inner().tag.unwrap_or_default();
        bump(&mut t.metadata);
        s.tags.insert(meta_key(&t.metadata), t.clone());
        Ok(Response::new(t))
    }
    async fn get_tag(&self, req: Request<pb::GetTagRequest>) -> R<pb::Tag> {
        let mut s = self.begin("GetTag", &req)?;
        if let Some(r) = s.take_reply::<pb::Tag>("GetTag") {
            return Ok(Response::new(r));
        }
        s.tags
            .get(&key(&req.get_ref().tag))
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("tag"))
    }
    async fn list_tags(&self, req: Request<pb::ListTagsRequest>) -> R<pb::ListTagsResponse> {
        let s = self.begin("ListTags", &req)?;
        let r = req.get_ref();
        let items = s
            .tags
            .iter()
            .filter(|(k, _)| r.hive.is_empty() || k.0 == r.hive)
            .map(|(_, v)| v.clone())
            .collect();
        let (tags, next_page_token) = page(items, &token(&r.options), s.page_size);
        Ok(Response::new(pb::ListTagsResponse {
            tags,
            next_page_token,
        }))
    }
    async fn update_tag(&self, req: Request<pb::UpdateTagRequest>) -> R<pb::Tag> {
        let mut s = self.begin("UpdateTag", &req)?;
        if let Some(r) = s.take_reply::<pb::Tag>("UpdateTag") {
            return Ok(Response::new(r));
        }
        let mut t = req.into_inner().tag.unwrap_or_default();
        bump(&mut t.metadata);
        s.tags.insert(meta_key(&t.metadata), t.clone());
        Ok(Response::new(t))
    }
    async fn delete_tag(&self, req: Request<pb::DeleteTagRequest>) -> R<pb::Tag> {
        let mut s = self.begin("DeleteTag", &req)?;
        s.tags
            .remove(&key(&req.get_ref().tag))
            .map(Response::new)
            .ok_or_else(|| not_found("tag"))
    }
    async fn list_workers(
        &self,
        req: Request<pb::ListWorkersRequest>,
    ) -> R<pb::ListWorkersResponse> {
        let s = self.begin("ListWorkers", &req)?;
        let (workers, next_page_token) = page(
            s.workers.values().cloned().collect(),
            &token(&req.get_ref().options),
            s.page_size,
        );
        Ok(Response::new(pb::ListWorkersResponse {
            workers,
            next_page_token,
        }))
    }
    async fn get_worker(&self, req: Request<pb::GetWorkerRequest>) -> R<pb::Worker> {
        let s = self.begin("GetWorker", &req)?;
        s.workers
            .get(&key(&req.get_ref().worker).1)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("worker"))
    }
    async fn create_worker(&self, _: Request<pb::CreateWorkerRequest>) -> R<pb::Worker> {
        Err(Status::unimplemented("CreateWorker"))
    }
    async fn update_worker(&self, _: Request<pb::UpdateWorkerRequest>) -> R<pb::Worker> {
        Err(Status::unimplemented("UpdateWorker"))
    }
    async fn delete_worker(&self, _: Request<pb::DeleteWorkerRequest>) -> R<pb::Worker> {
        Err(Status::unimplemented("DeleteWorker"))
    }
    async fn drain_worker(
        &self,
        req: Request<pb::DrainWorkerRequest>,
    ) -> R<pb::DrainWorkerResponse> {
        let mut s = self.begin("DrainWorker", &req)?;
        let w = set_worker_state(&mut s, &req.get_ref().worker, pb::WorkerState::Draining)?;
        Ok(Response::new(pb::DrainWorkerResponse { worker: Some(w) }))
    }
    async fn mark_worker_available(
        &self,
        req: Request<pb::MarkWorkerAvailableRequest>,
    ) -> R<pb::MarkWorkerAvailableResponse> {
        let mut s = self.begin("MarkWorkerAvailable", &req)?;
        let w = set_worker_state(&mut s, &req.get_ref().worker, pb::WorkerState::Active)?;
        Ok(Response::new(pb::MarkWorkerAvailableResponse {
            worker: Some(w),
        }))
    }
    async fn mark_worker_unavailable(
        &self,
        req: Request<pb::MarkWorkerUnavailableRequest>,
    ) -> R<pb::MarkWorkerUnavailableResponse> {
        let mut s = self.begin("MarkWorkerUnavailable", &req)?;
        let w = set_worker_state(&mut s, &req.get_ref().worker, pb::WorkerState::Unavailable)?;
        Ok(Response::new(pb::MarkWorkerUnavailableResponse {
            worker: Some(w),
        }))
    }
    async fn list_worker_actor_assignments(
        &self,
        req: Request<pb::ListWorkerActorAssignmentsRequest>,
    ) -> R<pb::ListWorkerActorAssignmentsResponse> {
        let s = self.begin("ListWorkerActorAssignments", &req)?;
        let r = req.get_ref();
        let name = key(&r.worker).1;
        if !s.workers.contains_key(&name) {
            return Err(not_found("worker"));
        }
        let items = s.assignments.get(&name).cloned().unwrap_or_default();
        let (actor_assignments, next_page_token) = page(items, &token(&r.options), s.page_size);
        Ok(Response::new(pb::ListWorkerActorAssignmentsResponse {
            actor_assignments,
            next_page_token,
        }))
    }
    async fn list_actors(&self, req: Request<pb::ListActorsRequest>) -> R<pb::ListActorsResponse> {
        let s = self.begin("ListActors", &req)?;
        let r = req.get_ref();
        let items = s
            .actors
            .iter()
            .filter(|(k, _)| r.hive.is_empty() || k.0 == r.hive)
            .map(|(_, v)| v.clone())
            .collect();
        let (actors, next_page_token) = page(items, &token(&r.options), s.page_size);
        Ok(Response::new(pb::ListActorsResponse {
            actors,
            next_page_token,
        }))
    }
    async fn create_hive(&self, req: Request<pb::CreateHiveRequest>) -> R<pb::Hive> {
        let mut s = self.begin("CreateHive", &req)?;
        let mut h = req.into_inner().hive.unwrap_or_default();
        bump(&mut h.metadata);
        let name = meta_key(&h.metadata).1;
        if s.hives.contains_key(&name) {
            return Err(Status::already_exists("hive exists"));
        }
        s.hives.insert(name, h.clone());
        Ok(Response::new(h))
    }
    async fn get_hive(&self, req: Request<pb::GetHiveRequest>) -> R<pb::Hive> {
        let s = self.begin("GetHive", &req)?;
        s.hives
            .get(&key(&req.get_ref().hive).1)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("hive"))
    }
    async fn list_hives(&self, req: Request<pb::ListHivesRequest>) -> R<pb::ListHivesResponse> {
        let s = self.begin("ListHives", &req)?;
        let (hives, next_page_token) = page(
            s.hives.values().cloned().collect(),
            &token(&req.get_ref().options),
            s.page_size,
        );
        Ok(Response::new(pb::ListHivesResponse {
            hives,
            next_page_token,
        }))
    }
    async fn delete_hive(&self, req: Request<pb::DeleteHiveRequest>) -> R<pb::Hive> {
        let mut s = self.begin("DeleteHive", &req)?;
        s.hives
            .remove(&key(&req.get_ref().hive).1)
            .map(Response::new)
            .ok_or_else(|| not_found("hive"))
    }
    async fn create_actor_template(
        &self,
        req: Request<pb::CreateActorTemplateRequest>,
    ) -> R<pb::ActorTemplate> {
        let mut s = self.begin("CreateActorTemplate", &req)?;
        let mut t = req.into_inner().actor_template.unwrap_or_default();
        bump(&mut t.metadata);
        s.templates.insert(meta_key(&t.metadata), t.clone());
        Ok(Response::new(t))
    }
    async fn get_actor_template(
        &self,
        req: Request<pb::GetActorTemplateRequest>,
    ) -> R<pb::ActorTemplate> {
        let s = self.begin("GetActorTemplate", &req)?;
        s.templates
            .get(&key(&req.get_ref().actor_template))
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("actor template"))
    }
    async fn list_actor_templates(
        &self,
        req: Request<pb::ListActorTemplatesRequest>,
    ) -> R<pb::ListActorTemplatesResponse> {
        let s = self.begin("ListActorTemplates", &req)?;
        let r = req.get_ref();
        let items = s
            .templates
            .iter()
            .filter(|(k, _)| r.hive.is_empty() || k.0 == r.hive)
            .map(|(_, v)| v.clone())
            .collect();
        let (actor_templates, next_page_token) = page(items, &token(&r.options), s.page_size);
        Ok(Response::new(pb::ListActorTemplatesResponse {
            actor_templates,
            next_page_token,
        }))
    }
    async fn delete_actor_template(
        &self,
        req: Request<pb::DeleteActorTemplateRequest>,
    ) -> R<pb::ActorTemplate> {
        let mut s = self.begin("DeleteActorTemplate", &req)?;
        s.templates
            .remove(&key(&req.get_ref().actor_template))
            .map(Response::new)
            .ok_or_else(|| not_found("actor template"))
    }
    async fn get_global_access_policy(
        &self,
        req: Request<pb::GetGlobalAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("GetGlobalAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("GetGlobalAccessPolicy") {
            return Ok(Response::new(r));
        }
        s.global_policy
            .clone()
            .map(Response::new)
            .ok_or_else(|| not_found("access policy"))
    }
    async fn create_global_access_policy(
        &self,
        req: Request<pb::CreateGlobalAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("CreateGlobalAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("CreateGlobalAccessPolicy") {
            return Ok(Response::new(r));
        }
        if s.global_policy.is_some() {
            return Err(Status::already_exists("exists"));
        }
        let mut p = req.into_inner().access_policy.unwrap_or_default();
        bump(&mut p.metadata);
        s.global_policy = Some(p.clone());
        Ok(Response::new(p))
    }
    async fn update_global_access_policy(
        &self,
        req: Request<pb::UpdateGlobalAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("UpdateGlobalAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("UpdateGlobalAccessPolicy") {
            return Ok(Response::new(r));
        }
        if s.global_policy.is_none() {
            return Err(not_found("access policy"));
        }
        let mut p = req.into_inner().access_policy.unwrap_or_default();
        bump(&mut p.metadata);
        s.global_policy = Some(p.clone());
        Ok(Response::new(p))
    }
    async fn get_hive_access_policy(
        &self,
        req: Request<pb::GetHiveAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("GetHiveAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("GetHiveAccessPolicy") {
            return Ok(Response::new(r));
        }
        s.hive_policies
            .get(&key(&req.get_ref().hive).1)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("access policy"))
    }
    async fn create_hive_access_policy(
        &self,
        req: Request<pb::CreateHiveAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("CreateHiveAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("CreateHiveAccessPolicy") {
            return Ok(Response::new(r));
        }
        let r = req.into_inner();
        let hive = key(&r.hive).1;
        if !s.hives.contains_key(&hive) {
            return Err(not_found("hive"));
        }
        if s.hive_policies.contains_key(&hive) {
            return Err(Status::already_exists("exists"));
        }
        let mut p = r.access_policy.unwrap_or_default();
        bump(&mut p.metadata);
        s.hive_policies.insert(hive, p.clone());
        Ok(Response::new(p))
    }
    async fn update_hive_access_policy(
        &self,
        req: Request<pb::UpdateHiveAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("UpdateHiveAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("UpdateHiveAccessPolicy") {
            return Ok(Response::new(r));
        }
        let r = req.into_inner();
        let hive = key(&r.hive).1;
        if !s.hive_policies.contains_key(&hive) {
            return Err(not_found("access policy"));
        }
        let mut p = r.access_policy.unwrap_or_default();
        bump(&mut p.metadata);
        s.hive_policies.insert(hive, p.clone());
        Ok(Response::new(p))
    }
    async fn delete_hive_access_policy(
        &self,
        req: Request<pb::DeleteHiveAccessPolicyRequest>,
    ) -> R<pb::AccessPolicy> {
        let mut s = self.begin("DeleteHiveAccessPolicy", &req)?;
        if let Some(r) = s.take_reply::<pb::AccessPolicy>("DeleteHiveAccessPolicy") {
            return Ok(Response::new(r));
        }
        s.hive_policies
            .remove(&key(&req.get_ref().hive).1)
            .map(Response::new)
            .ok_or_else(|| not_found("access policy"))
    }
}

fn set_worker_state(
    s: &mut State,
    r: &Option<pb::ObjectRef>,
    state: pb::WorkerState,
) -> Result<pb::Worker, Status> {
    let w = s
        .workers
        .get_mut(&key(r).1)
        .ok_or_else(|| not_found("worker"))?;
    w.status.get_or_insert_with(Default::default).state = state as i32;
    Ok(w.clone())
}

/// Starts the fake on a free local port.
pub async fn serve(fake: Fake) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ControlServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    addr
}

/// What a hivectl run produced.
pub struct Run {
    pub result: Result<(), CliError>,
    pub out: String,
    pub err: String,
}

impl Run {
    pub fn ok(&self) -> &str {
        if let Err(e) = &self.result {
            panic!("command failed: {e}\nstderr: {}", self.err);
        }
        &self.out
    }

    pub fn error(&self) -> &CliError {
        self.result.as_ref().expect_err("command succeeded")
    }
}

/// Runs hivectl against `addr` with `args`, feeding `stdin`.
pub async fn hivectl_with_stdin(addr: SocketAddr, args: &[&str], stdin: &str) -> Run {
    let endpoint = addr.to_string();
    let argv: Vec<&str> = ["hivectl", "--endpoint", &endpoint, "--plaintext"]
        .into_iter()
        .chain(args.iter().copied())
        .collect();
    let cli = Cli::try_parse_from(argv).unwrap_or_else(|e| panic!("{e}"));
    let (io, out, err) = Io::captured_with_stdin(stdin.as_bytes().to_vec());
    let result = app::run(cli, io).await;
    Run {
        result,
        out: out.contents(),
        err: err.contents(),
    }
}

/// Runs hivectl against `addr` with `args`.
pub async fn hivectl(addr: SocketAddr, args: &[&str]) -> Run {
    hivectl_with_stdin(addr, args, "").await
}

// Fixtures.

pub fn actor(hive: &str, name: &str, state: pb::ActorState) -> pb::Actor {
    pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: hive.into(),
            name: name.into(),
            version: 1,
            uid: format!("uid-{name}"),
            ..Default::default()
        }),
        actor_template: Some(pb::ObjectRef {
            hive: hive.into(),
            name: "tpl".into(),
        }),
        status: Some(pb::ActorStatus {
            state: state as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn running_on(mut a: pb::Actor, worker: &str, ns: &str, pod: &str) -> pb::Actor {
    let st = a.status.get_or_insert_with(Default::default);
    st.state = pb::ActorState::Running as i32;
    st.worker_assignment = Some(pb::WorkerAssignment {
        worker: Some(pb::ObjectRef {
            hive: String::new(),
            name: worker.into(),
        }),
        worker_namespace: ns.into(),
        worker_pod: pod.into(),
        ..Default::default()
    });
    a
}

pub fn worker(
    name: &str,
    ns: &str,
    pool: &str,
    labels: &[(&str, &str)],
    class: &str,
) -> pb::Worker {
    pb::Worker {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            ..Default::default()
        }),
        worker_namespace: ns.into(),
        worker_pool: pool.into(),
        worker_pod: format!("{name}-pod"),
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        status: Some(pb::WorkerStatus {
            state: pb::WorkerState::Active as i32,
            sandbox_classes: if class.is_empty() {
                vec![]
            } else {
                vec![pb::WorkerSandboxClass {
                    sandbox_class: class.into(),
                    ..Default::default()
                }]
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn hive(name: &str) -> pb::Hive {
    pb::Hive {
        metadata: Some(pb::ResourceMetadata {
            name: name.into(),
            ..Default::default()
        }),
    }
}
