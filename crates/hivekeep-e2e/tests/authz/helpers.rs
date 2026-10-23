//! Principals, the shared fixture, and the polled call table the authz
//! checks are written in.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_e2e::clients::{ApiAuth, Clients, ControlApi};
use hivekeep_e2e::env::{resource_name, system_namespace};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::kube_util::{is_not_found, live_signer_roots};
use hivekeep_e2e::{install, portforward, preview, probe};
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_client::ControlClient;
use hivekeep_proto::api::v1::worker_service_client::WorkerServiceClient;
use k8s_openapi::api::authentication::v1::{TokenRequest, TokenRequestSpec};
use k8s_openapi::api::core::v1::ServiceAccount;
use kube::api::{Api, PostParams};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Status};

/// The one Actor the fixture creates.
pub const ACTOR_NAME: &str = "authz-actor";
/// How long a policy change may take to reach every API replica's checks.
pub const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

/// The WorkerService client type.
pub type WorkerApi = WorkerServiceClient<InterceptedService<Channel, ApiAuth>>;

/// Both API surfaces, authenticated as one principal.
#[derive(Clone)]
pub struct Apis {
    pub control: ControlApi,
    pub workers: WorkerApi,
}

/// An identity the tests make calls as.
#[derive(Clone)]
pub struct Principal {
    pub name: String,
    /// AccessPolicy member: `user:` plus the token subject.
    pub member: String,
    pub apis: Apis,
}

/// What every check shares: a hive holding the probe template and one
/// never-resumed Actor from it, all created by the bootstrap owner.
pub struct Fixture {
    pub clients: Arc<Clients>,
    pub bootstrap: Principal,
    pub hive: String,
    pub template: pb::ObjectRef,
    pub actor: pb::ObjectRef,
}

/// The status codes a call may return to pass.
#[derive(Clone)]
pub struct Outcome {
    pub name: &'static str,
    pub ok: fn(Code) -> bool,
}

pub const ALLOWED: Outcome = Outcome {
    name: "OK",
    ok: |c| c == Code::Ok,
};
pub const DENIED: Outcome = Outcome {
    name: "PermissionDenied",
    ok: |c| c == Code::PermissionDenied,
};
/// Anything but a permission or authentication failure. The permission check
/// runs before the handler validates or looks anything up, so a request the
/// handler then rejects shows the caller got past the check without changing
/// anything. Every request expecting this is paired with a caller expecting
/// DENIED on the same request: were its identifiers malformed, that caller
/// would get InvalidArgument and fail.
pub const PASSED_CHECK: Outcome = Outcome {
    name: "anything but PermissionDenied or Unauthenticated",
    ok: |c| c != Code::PermissionDenied && c != Code::Unauthenticated,
};
/// A caller refused for who it is, by the permission check or by a handler
/// that authenticates the caller itself.
pub const REJECTED: Outcome = Outcome {
    name: "PermissionDenied or Unauthenticated",
    ok: |c| c == Code::PermissionDenied || c == Code::Unauthenticated,
};

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;
/// One request, made with whichever principal a call supplies.
pub type Rpc = Arc<dyn Fn(Apis) -> BoxFut + Send + Sync>;

/// Wraps a closure as an [`Rpc`].
pub fn rpc<F, Fut>(f: F) -> Rpc
where
    F: Fn(Apis) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Status>> + Send + 'static,
{
    Arc::new(move |a| Box::pin(f(a)))
}

/// An rpc made as `who`, and the outcome it must have.
pub struct Call {
    pub name: String,
    pub who: Principal,
    pub rpc: Rpc,
    pub want: Outcome,
}

/// Shorthand for a [`Call`].
pub fn call(name: &str, who: &Principal, rpc: Rpc, want: Outcome) -> Call {
    Call {
        name: name.to_string(),
        who: who.clone(),
        rpc,
        want,
    }
}

fn code_of(r: &Result<(), Status>) -> Code {
    match r {
        Ok(()) => Code::Ok,
        Err(s) => s.code(),
    }
}

/// Polls `calls` until each has its outcome: a policy change is not
/// guaranteed to reach every replica at once. The calls share one deadline,
/// so a step costs at most [`PROPAGATION_TIMEOUT`].
pub async fn expect_all(t: &T, calls: Vec<Call>) {
    let deadline = Instant::now() + PROPAGATION_TIMEOUT;
    let mut pending = calls;
    loop {
        let mut still = Vec::new();
        let mut last = Vec::new();
        for c in pending {
            let r = (c.rpc)(c.who.apis.clone()).await;
            if !(c.want.ok)(code_of(&r)) {
                last.push(r.err());
                still.push(c);
            }
        }
        if still.is_empty() {
            return;
        }
        if Instant::now() > deadline {
            for (c, err) in still.iter().zip(last) {
                let got = err
                    .map(|s| format!("{}: {}", s.code(), s.message()))
                    .unwrap_or_else(|| "OK".into());
                t.error(format!(
                    "{} {}: got {got}, want {}",
                    c.who.name, c.name, c.want.name
                ));
            }
            return;
        }
        pending = still;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Connects both API surfaces as the holder of `token`, over a forward to
/// the API Service of its own, closed when `t` ends.
pub async fn apis_with_token(t: &T, clients: &Clients, token: &str) -> Apis {
    let kube = clients.kube();
    let ns = system_namespace();
    let forward = portforward::service_port_forward(
        &kube,
        &ns,
        &resource_name(install::API_SERVICE),
        install::API_SERVICE_PORT,
    )
    .await
    .unwrap_or_else(|e| t.fatal(format!("port-forwarding to hivekeep-api: {e}")));
    let roots = live_signer_roots(&kube, install::SERVICE_DNS_SIGNER)
        .await
        .unwrap_or_else(|e| t.fatal(e));
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(roots))
        .domain_name(install::api_server_name(&ns));
    let channel = Endpoint::from_shared(format!("https://{}", forward.addr()))
        .unwrap_or_else(|e| t.fatal(e.to_string()))
        .tls_config(tls)
        .unwrap_or_else(|e| t.fatal(format!("API TLS config: {e}")))
        .connect_timeout(Duration::from_secs(30))
        .connect()
        .await
        .unwrap_or_else(|e| t.fatal(format!("dialing hivekeep-api: {e}")));
    t.cleanup(move |_| async move { drop(forward) });
    let auth = ApiAuth::new(Some(token), preview::any_enabled()).unwrap_or_else(|e| t.fatal(e));
    Apis {
        control: ControlClient::with_interceptor(channel.clone(), auth.clone()),
        workers: WorkerServiceClient::with_interceptor(channel, auth),
    }
}

/// Creates a ServiceAccount in `namespace` and returns a principal that
/// authenticates with its token.
pub async fn new_principal(t: &T, clients: &Clients, namespace: &str, name: &str) -> Principal {
    let api: Api<ServiceAccount> = Api::namespaced(clients.kube(), namespace);
    let mut sa = ServiceAccount::default();
    sa.metadata.name = Some(name.into());
    sa.metadata.namespace = Some(namespace.into());
    api.create(&PostParams::default(), &sa)
        .await
        .unwrap_or_else(|e| t.fatal(format!("creating ServiceAccount {namespace}/{name}: {e}")));
    principal_for(t, clients, namespace, name).await
}

/// A principal authenticating with a token minted for the existing
/// ServiceAccount `namespace/name`.
pub async fn principal_for(t: &T, clients: &Clients, namespace: &str, name: &str) -> Principal {
    let api: Api<ServiceAccount> = Api::namespaced(clients.kube(), namespace);
    let request = TokenRequest {
        spec: Some(TokenRequestSpec {
            audiences: Some(vec![install::api_server_name(&system_namespace())]),
            expiration_seconds: Some(3600),
            ..Default::default()
        }),
        ..Default::default()
    };
    let issued = api
        .create_token_request(name, &PostParams::default(), &request)
        .await
        .unwrap_or_else(|e| t.fatal(format!("minting a token for {namespace}/{name}: {e}")));
    let token = issued.status.and_then(|s| s.token).unwrap_or_default();
    Principal {
        name: name.to_string(),
        member: format!("user:system:serviceaccount:{namespace}:{name}"),
        apis: apis_with_token(t, clients, &token).await,
    }
}

/// Whether ServiceAccount `namespace/name` exists, failing on other errors.
pub async fn service_account_exists(t: &T, clients: &Clients, namespace: &str, name: &str) -> bool {
    match Api::<ServiceAccount>::namespaced(clients.kube(), namespace)
        .get(name)
        .await
    {
        Ok(_) => true,
        Err(e) if is_not_found(&e) => false,
        Err(e) => t.fatal(format!("getting ServiceAccount {namespace}/{name}: {e}")),
    }
}

pub fn binding(role: &str, ps: &[&Principal]) -> pb::Binding {
    pb::Binding {
        role: role.into(),
        members: ps.iter().map(|p| p.member.clone()).collect(),
    }
}

fn hive_ref(hive: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: String::new(),
        name: hive.into(),
    }
}

/// Replaces `hive`'s AccessPolicy bindings, creating the policy if needed.
/// The caller deletes it with [`cleanup_hive_policy`].
pub async fn set_hive_policy(t: &T, clients: &Clients, hive: &str, bindings: Vec<pb::Binding>) {
    let mut api = clients.api();
    let mut policy = pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: SINGLETON_NAME.into(),
            ..Default::default()
        }),
        bindings,
    };
    let current = api
        .get_hive_access_policy(pb::GetHiveAccessPolicyRequest {
            hive: Some(hive_ref(hive)),
            name: SINGLETON_NAME.into(),
        })
        .await;
    let result = match current {
        Err(e) if e.code() == Code::NotFound => api
            .create_hive_access_policy(pb::CreateHiveAccessPolicyRequest {
                hive: Some(hive_ref(hive)),
                access_policy: Some(policy),
            })
            .await
            .map(|_| ()),
        Ok(cur) => {
            policy.metadata = cur.into_inner().metadata;
            api.update_hive_access_policy(pb::UpdateHiveAccessPolicyRequest {
                hive: Some(hive_ref(hive)),
                access_policy: Some(policy),
            })
            .await
            .map(|_| ())
        }
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        t.fatal(format!("setting the AccessPolicy of hive {hive:?}: {e}"));
    }
}

/// Deletes `hive`'s AccessPolicy when `t` ends.
pub fn cleanup_hive_policy(t: &T, clients: &Arc<Clients>, hive: &str) {
    let clients = clients.clone();
    let hive = hive.to_string();
    t.cleanup(move |t| async move {
        if let Err(e) = delete_hive_policy(&clients, &hive).await {
            t.log(format!(
                "cleanup: deleting the AccessPolicy of hive {hive:?}: {e}"
            ));
        }
    });
}

/// Deletes `hive`'s AccessPolicy; one already gone is not an error.
pub async fn delete_hive_policy(clients: &Clients, hive: &str) -> Result<(), Status> {
    match clients
        .api()
        .delete_hive_access_policy(pb::DeleteHiveAccessPolicyRequest {
            hive: Some(hive_ref(hive)),
            name: SINGLETON_NAME.into(),
            ..Default::default()
        })
        .await
    {
        Err(e) if e.code() != Code::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Creates an empty hive as the bootstrap owner, deleted (with its
/// AccessPolicy) when `t` ends.
pub async fn create_hive(t: &T, clients: &Arc<Clients>, name: &str) {
    let req = pb::CreateHiveRequest {
        hive: Some(pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                ..Default::default()
            }),
        }),
    };
    if let Err(e) = clients.api().create_hive(req).await {
        t.fatal(format!("CreateHive {name:?}: {e}"));
    }
    let clients = clients.clone();
    let name = name.to_string();
    t.cleanup(move |t| async move {
        if let Err(e) = delete_hive(&clients, &name).await {
            t.log(format!("cleanup: DeleteHive {name:?}: {e}"));
        }
    });
}

/// Deletes a hive; one already gone is not an error.
pub async fn delete_hive(clients: &Clients, name: &str) -> Result<(), Status> {
    match clients
        .api()
        .delete_hive(pb::DeleteHiveRequest {
            hive: Some(hive_ref(name)),
            ..Default::default()
        })
        .await
    {
        Err(e) if e.code() != Code::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Deploys the probe template into a new hive and creates one Actor from
/// it, all as the bootstrap owner. The Actor is never resumed: the checks
/// only need one that exists.
pub async fn deploy_fixture(t: &T, bucket: &str) -> Fixture {
    let clients = hivekeep_e2e::suite::clients();
    let (hive, _) =
        probe::deploy_probe(t, &clients, bucket, "authz", probe::ProbeOptions::default()).await;
    let actor = pb::ObjectRef {
        hive: hive.clone(),
        name: ACTOR_NAME.into(),
    };
    let template = pb::ObjectRef {
        hive: hive.clone(),
        name: probe::PROBE_NAME.into(),
    };
    let req = pb::CreateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.clone(),
                name: ACTOR_NAME.into(),
                ..Default::default()
            }),
            actor_template: Some(template.clone()),
            ..Default::default()
        }),
    };
    if let Err(e) = clients.api().create_actor(req).await {
        t.fatal(format!("CreateActor {hive}/{ACTOR_NAME}: {e}"));
    }
    let mut api = clients.api();
    let a = actor.clone();
    t.cleanup(move |t| async move {
        if let Err(e) = api
            .delete_actor(pb::DeleteActorRequest {
                actor: Some(a.clone()),
                ..Default::default()
            })
            .await
        {
            t.log(format!("cleanup: DeleteActor {}/{}: {e}", a.hive, a.name));
        }
    });
    // The bootstrap owner is the client ServiceAccount the shared clients
    // authenticate as; its own connection also reaches the WorkerService.
    let mut bootstrap = principal_for(
        t,
        &clients,
        &system_namespace(),
        &hivekeep_e2e::clients::client_service_account(),
    )
    .await;
    bootstrap.name = "bootstrap-owner".into();
    bootstrap.apis.control = clients.api();
    Fixture {
        clients,
        bootstrap,
        hive,
        template,
        actor,
    }
}

/// Polls `actor`, as the bootstrap owner, until it is in `want`.
pub async fn wait_for_actor_state(
    t: &T,
    clients: &Clients,
    actor: &pb::ObjectRef,
    want: pb::ActorState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(resp) = clients
            .api()
            .get_actor(pb::GetActorRequest {
                actor: Some(actor.clone()),
            })
            .await
        {
            if resp.into_inner().status.map(|s| s.state) == Some(want as i32) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    t.fatal(format!(
        "timed out waiting for actor {}/{} to reach {want:?}",
        actor.hive, actor.name
    ))
}

// The requests the checks make, shared so each check pairs callers on the
// same request.

pub fn get_hive(name: &str) -> Rpc {
    let name = name.to_string();
    rpc(move |mut a| {
        let r = hive_ref(&name);
        async move {
            a.control
                .get_hive(pb::GetHiveRequest { hive: Some(r) })
                .await
                .map(|_| ())
        }
    })
}

pub fn get_actor(r: &pb::ObjectRef) -> Rpc {
    let r = r.clone();
    rpc(move |mut a| {
        let r = r.clone();
        async move {
            a.control
                .get_actor(pb::GetActorRequest { actor: Some(r) })
                .await
                .map(|_| ())
        }
    })
}

pub fn list_actors(hive: &str) -> Rpc {
    let hive = hive.to_string();
    rpc(move |mut a| {
        let hive = hive.clone();
        async move {
            a.control
                .list_actors(pb::ListActorsRequest {
                    hive,
                    ..Default::default()
                })
                .await
                .map(|_| ())
        }
    })
}

pub fn suspend_actor(r: &pb::ObjectRef) -> Rpc {
    let r = r.clone();
    rpc(move |mut a| {
        let r = r.clone();
        async move {
            a.control
                .suspend_actor(pb::SuspendActorRequest {
                    actor: Some(r),
                    ..Default::default()
                })
                .await
                .map(|_| ())
        }
    })
}

/// Asks for an Actor with no name: the check reads only its hive and
/// template, and validation then rejects it, so nothing is created.
pub fn create_actor_unnamed(hive: &str, template: &pb::ObjectRef) -> Rpc {
    let (hive, template) = (hive.to_string(), template.clone());
    rpc(move |mut a| {
        let req = pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: hive.clone(),
                    ..Default::default()
                }),
                actor_template: Some(template.clone()),
                ..Default::default()
            }),
        };
        async move { a.control.create_actor(req).await.map(|_| ()) }
    })
}
