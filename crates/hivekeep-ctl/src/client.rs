//! The Control API client, with every call logged by verbosity: the method
//! at `-vv`, failures and slow calls at `-v`, payloads at `-vvvvv`.

use std::future::Future;
use std::time::Instant;

use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_client::ControlClient;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::connection::RequestDecorator;
use crate::error::status_text;
use crate::protojson::{to_compact_string, ProtoMessage};
use crate::verbosity::{round_ms, Logger, BASIC, FLOW, TRACE};

/// The generated client, with request metadata attached.
pub type RawClient = ControlClient<InterceptedService<Channel, RequestDecorator>>;

/// Page size used when listing.
pub const PAGE_SIZE: i32 = 1000;

/// A connected Control client.
#[derive(Clone)]
pub struct Api {
    raw: RawClient,
    log: Logger,
}

/// The full gRPC method name of a Control RPC.
pub fn full_method(method: &str) -> String {
    format!("/hivekeep.api.v1.Control/{method}")
}

impl Api {
    /// A client over `channel`.
    pub fn new(channel: Channel, decorator: RequestDecorator, log: Logger) -> Self {
        Api {
            raw: ControlClient::with_interceptor(channel, decorator),
            log,
        }
    }

    /// The logger calls report to.
    pub fn logger(&self) -> &Logger {
        &self.log
    }

    async fn call<Req, Resp, F, Fut>(&self, method: &str, req: Req, f: F) -> Result<Resp, Status>
    where
        Req: ProtoMessage,
        Resp: ProtoMessage,
        F: FnOnce(RawClient, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Resp>, Status>>,
    {
        let method = full_method(method);
        if self.log.enabled(TRACE) {
            self.log.log(
                TRACE,
                format!("gRPC {method} request: {}", to_compact_string(&req)),
            );
        } else {
            self.log.log(FLOW, format!("gRPC {method}"));
        }
        let start = Instant::now();
        let result = f(self.raw.clone(), Request::new(req)).await;
        let elapsed = start.elapsed();
        match result {
            Err(status) => {
                self.log.log(
                    BASIC,
                    format!(
                        "gRPC {method} failed after {}: {}",
                        round_ms(elapsed),
                        status_text(&status)
                    ),
                );
                Err(status)
            }
            Ok(resp) => {
                let resp = resp.into_inner();
                if self.log.enabled(TRACE) {
                    self.log.log(
                        TRACE,
                        format!(
                            "gRPC {method} response ({}): {}",
                            round_ms(elapsed),
                            to_compact_string(&resp)
                        ),
                    );
                } else {
                    self.log.completed(
                        elapsed,
                        format!("gRPC {method} completed in {}", round_ms(elapsed)),
                    );
                }
                Ok(resp)
            }
        }
    }
}

macro_rules! rpcs {
    ($($fn:ident ( $req:ident ) -> $resp:ident = $name:literal;)*) => {
        impl Api {
            $(
                #[doc = concat!("Calls `", $name, "`.")]
                pub async fn $fn(&self, req: pb::$req) -> Result<pb::$resp, Status> {
                    self.call($name, req, |mut c, r| async move { c.$fn(r).await }).await
                }
            )*
        }
    };
}

rpcs! {
    get_actor(GetActorRequest) -> Actor = "GetActor";
    create_actor(CreateActorRequest) -> Actor = "CreateActor";
    update_actor(UpdateActorRequest) -> Actor = "UpdateActor";
    suspend_actor(SuspendActorRequest) -> SuspendActorResponse = "SuspendActor";
    resume_actor(ResumeActorRequest) -> ResumeActorResponse = "ResumeActor";
    revert_actor(RevertActorRequest) -> RevertActorResponse = "RevertActor";
    delete_actor(DeleteActorRequest) -> Actor = "DeleteActor";
    list_actors(ListActorsRequest) -> ListActorsResponse = "ListActors";

    get_actor_egress_policy(GetActorEgressPolicyRequest) -> EgressPolicy = "GetActorEgressPolicy";
    create_actor_egress_policy(CreateActorEgressPolicyRequest) -> EgressPolicy = "CreateActorEgressPolicy";
    update_actor_egress_policy(UpdateActorEgressPolicyRequest) -> EgressPolicy = "UpdateActorEgressPolicy";
    delete_actor_egress_policy(DeleteActorEgressPolicyRequest) -> EgressPolicy = "DeleteActorEgressPolicy";

    create_tag(CreateTagRequest) -> Tag = "CreateTag";
    get_tag(GetTagRequest) -> Tag = "GetTag";
    list_tags(ListTagsRequest) -> ListTagsResponse = "ListTags";
    update_tag(UpdateTagRequest) -> Tag = "UpdateTag";
    delete_tag(DeleteTagRequest) -> Tag = "DeleteTag";

    list_workers(ListWorkersRequest) -> ListWorkersResponse = "ListWorkers";
    get_worker(GetWorkerRequest) -> Worker = "GetWorker";
    drain_worker(DrainWorkerRequest) -> DrainWorkerResponse = "DrainWorker";
    mark_worker_available(MarkWorkerAvailableRequest) -> MarkWorkerAvailableResponse = "MarkWorkerAvailable";
    mark_worker_unavailable(MarkWorkerUnavailableRequest) -> MarkWorkerUnavailableResponse = "MarkWorkerUnavailable";
    list_worker_actor_assignments(ListWorkerActorAssignmentsRequest) -> ListWorkerActorAssignmentsResponse = "ListWorkerActorAssignments";

    create_hive(CreateHiveRequest) -> Hive = "CreateHive";
    get_hive(GetHiveRequest) -> Hive = "GetHive";
    list_hives(ListHivesRequest) -> ListHivesResponse = "ListHives";
    delete_hive(DeleteHiveRequest) -> Hive = "DeleteHive";

    create_actor_template(CreateActorTemplateRequest) -> ActorTemplate = "CreateActorTemplate";
    get_actor_template(GetActorTemplateRequest) -> ActorTemplate = "GetActorTemplate";
    list_actor_templates(ListActorTemplatesRequest) -> ListActorTemplatesResponse = "ListActorTemplates";
    delete_actor_template(DeleteActorTemplateRequest) -> ActorTemplate = "DeleteActorTemplate";

    get_global_access_policy(GetGlobalAccessPolicyRequest) -> AccessPolicy = "GetGlobalAccessPolicy";
    create_global_access_policy(CreateGlobalAccessPolicyRequest) -> AccessPolicy = "CreateGlobalAccessPolicy";
    update_global_access_policy(UpdateGlobalAccessPolicyRequest) -> AccessPolicy = "UpdateGlobalAccessPolicy";
    get_hive_access_policy(GetHiveAccessPolicyRequest) -> AccessPolicy = "GetHiveAccessPolicy";
    create_hive_access_policy(CreateHiveAccessPolicyRequest) -> AccessPolicy = "CreateHiveAccessPolicy";
    update_hive_access_policy(UpdateHiveAccessPolicyRequest) -> AccessPolicy = "UpdateHiveAccessPolicy";
    delete_hive_access_policy(DeleteHiveAccessPolicyRequest) -> AccessPolicy = "DeleteHiveAccessPolicy";
}

fn list_options(page_token: String) -> Option<pb::ListOptions> {
    Some(pb::ListOptions {
        page_size: PAGE_SIZE,
        page_token,
    })
}

/// Pages through a List RPC, collecting every item.
macro_rules! paged {
    ($self:ident, $call:ident, $field:ident, |$token:ident| $req:expr) => {{
        let mut items = Vec::new();
        let mut $token = String::new();
        loop {
            let resp = $self.$call($req).await?;
            items.extend(resp.$field);
            $token = resp.next_page_token;
            if $token.is_empty() {
                break Ok(items);
            }
        }
    }};
}

impl Api {
    /// Every Actor in `hive` (every hive when empty).
    pub async fn list_all_actors(&self, hive: &str) -> Result<Vec<pb::Actor>, Status> {
        paged!(self, list_actors, actors, |token| pb::ListActorsRequest {
            hive: hive.to_string(),
            options: list_options(token.clone()),
        })
    }

    /// Every template in `hive` (every hive when empty).
    pub async fn list_all_actor_templates(
        &self,
        hive: &str,
    ) -> Result<Vec<pb::ActorTemplate>, Status> {
        paged!(self, list_actor_templates, actor_templates, |token| {
            pb::ListActorTemplatesRequest {
                hive: hive.to_string(),
                options: list_options(token.clone()),
            }
        })
    }

    /// Every Tag in `hive` (every hive when empty).
    pub async fn list_all_tags(&self, hive: &str) -> Result<Vec<pb::Tag>, Status> {
        paged!(self, list_tags, tags, |token| pb::ListTagsRequest {
            hive: hive.to_string(),
            options: list_options(token.clone()),
        })
    }

    /// Every Hive.
    pub async fn list_all_hives(&self) -> Result<Vec<pb::Hive>, Status> {
        paged!(self, list_hives, hives, |token| pb::ListHivesRequest {
            options: list_options(token.clone()),
        })
    }

    /// Every Worker.
    pub async fn list_all_workers(&self) -> Result<Vec<pb::Worker>, Status> {
        paged!(self, list_workers, workers, |token| {
            pb::ListWorkersRequest {
                options: list_options(token.clone()),
            }
        })
    }

    /// Every Actor assignment of `worker`.
    pub async fn list_all_worker_assignments(
        &self,
        worker: &str,
    ) -> Result<Vec<pb::ActorAssignment>, Status> {
        paged!(
            self,
            list_worker_actor_assignments,
            actor_assignments,
            |token| {
                pb::ListWorkerActorAssignmentsRequest {
                    worker: Some(crate::refs::global_ref(worker)),
                    options: list_options(token.clone()),
                }
            }
        )
    }
}
