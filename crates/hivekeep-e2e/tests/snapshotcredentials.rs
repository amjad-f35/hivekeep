//! Against real GCS, the credential exchange hands a node a token that
//! reaches exactly the Actor's own snapshot prefixes. It runs only on a GCS
//! install: on an S3-compatible store the exchange answers UNIMPLEMENTED,
//! so the suite skips there, or fails when `HK_E2E_REQUIRE_GCS` says a GCS
//! lane was meant.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::preview::Gate;
use hivekeep_e2e::actors;
use hivekeep_e2e::backend::require_gcs_backend;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::env::{check_env, resource_name, system_namespace};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::portforward::{service_port_forward, Forward};
use hivekeep_e2e::preview::require_preview;
use hivekeep_e2e::probe::{deploy_probe, ProbeOptions};
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::{register_template_cleanup, wait_for_template_ready};
use hivekeep_e2e::trustbundle::service_dns_trust_bundle;
use hivekeep_identity::jwt::authority::JwtPool;
use hivekeep_identity::jwt::claims::{ActorClaims, Claims, TokenKind};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::api::Api;
use reqwest::Method;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Code;

const PROBE_TEMPLATE: &str = "probe";

/// The credential exchange's port on the hivekeep-api Service.
const EXCHANGE_PORT: u16 = 8445;
/// The audience the exchange accepts worker-for-actor tokens for.
const EXCHANGE_AUDIENCE: &str = "objectstore.hivekeep.io";
/// The hivekeep-api sidecar that holds the object-storage settings.
const OBJECTSTORE_SIDECAR: &str = "objectstore-plugin";
/// The sidecar's backend selector.
const STORAGE_BACKEND_ENV: &str = "HK_STORAGE_BACKEND";
/// The Secret holding the Actor JWT signing pool, and its key.
const JWT_POOL_SECRET: &str = "actor-id-jwt-pool";
/// The ConfigMap carrying the JWT issuer, and its key.
const API_ENV_CONFIG_MAP: &str = "hivekeep-api-envvars";
const ACTOR_JWT_ISSUER_KEY: &str = "HK_API_ACTOR_JWT_ISSUER";
/// The public GCS XML API.
const GCS_ENDPOINT: &str = "https://storage.googleapis.com/";

/// The exchange's wire messages.
mod exchange {
    /// Trades an Actor JWT for an object-store token.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExchangeActorJwtRequest {
        #[prost(string, tag = "1")]
        pub actor_jwt: String,
        #[prost(bool, tag = "2")]
        pub force: bool,
    }

    /// The token and its expiry.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExchangeActorJwtResponse {
        #[prost(string, tag = "1")]
        pub access_token: String,
        #[prost(message, optional, tag = "2")]
        pub expire_time: Option<prost_types::Timestamp>,
    }

    pub const PATH: &str = "/hivekeep.objectstore.exchange.v1.CredentialExchange/ExchangeActorJwt";
}

/// A client of the credential exchange over a port-forward, verified the
/// way the node plugin verifies it: the service-DNS bundle and the
/// Service's DNS name.
struct Exchange {
    channel: Channel,
    _forward: Forward,
}

impl Exchange {
    async fn call(
        &self,
        actor_jwt: &str,
        force: bool,
    ) -> Result<exchange::ExchangeActorJwtResponse, tonic::Status> {
        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
        let req = exchange::ExchangeActorJwtRequest {
            actor_jwt: actor_jwt.into(),
            force,
        };
        let codec = tonic_prost::ProstCodec::<
            exchange::ExchangeActorJwtRequest,
            exchange::ExchangeActorJwtResponse,
        >::default();
        grpc.unary(
            tonic::Request::new(req),
            http::uri::PathAndQuery::from_static(exchange::PATH),
            codec,
        )
        .await
        .map(|r| r.into_inner())
    }
}

async fn dial_exchange(t: &T, clients: &Clients) -> Exchange {
    let ns = system_namespace();
    let api = resource_name(hivekeep_e2e::install::API_SERVICE);
    let forward = service_port_forward(&clients.kube(), &ns, &api, EXCHANGE_PORT)
        .await
        .unwrap_or_else(|e| t.fatal(format!("port-forwarding the exchange: {e}")));
    let roots = service_dns_trust_bundle(t, clients).await;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(roots))
        .domain_name(format!("{api}.{ns}.svc"));
    let channel = Endpoint::from_shared(format!("https://{}", forward.addr()))
        .unwrap_or_else(|e| t.fatal(e.to_string()))
        .tls_config(tls)
        .unwrap_or_else(|e| t.fatal(format!("exchange TLS config: {e}")))
        .connect()
        .await
        .unwrap_or_else(|e| t.fatal(format!("dialing the exchange: {e}")));
    Exchange {
        channel,
        _forward: forward,
    }
}

/// Skips (or fails under `HK_E2E_REQUIRE_GCS`) unless the install stores
/// snapshots in GCS, as the object-store sidecar's backend setting says.
async fn require_gcs(t: &T, clients: &Clients) {
    let deploy = Api::<Deployment>::namespaced(clients.kube(), &system_namespace())
        .get(&resource_name("hivekeep-api"))
        .await
        .unwrap_or_else(|e| t.fatal(format!("getting the hivekeep-api Deployment: {e}")));
    let mut backend = "gcs".to_string();
    let init = deploy
        .spec
        .and_then(|s| s.template.spec)
        .and_then(|s| s.init_containers)
        .unwrap_or_default();
    for c in init.iter().filter(|c| c.name == OBJECTSTORE_SIDECAR) {
        for env in c.env.iter().flatten() {
            if env.name == STORAGE_BACKEND_ENV {
                if let Some(v) = env.value.as_ref().filter(|v| !v.is_empty()) {
                    backend = v.clone();
                }
            }
        }
    }
    require_gcs_backend(t, &backend);
}

/// Mints worker-for-actor JWTs with the install's own pool, as hivekeep-api
/// would for the keeper. The harness cannot call MintWorkerActorJwt, which
/// needs the keeper's identity, so it reads the pool Secret with its admin
/// client and signs with the same keys.
struct Signer {
    pool: JwtPool,
    issuer: String,
}

async fn new_signer(t: &T, clients: &Clients) -> Signer {
    let ns = system_namespace();
    let secret = Api::<Secret>::namespaced(clients.kube(), &ns)
        .get(&resource_name(JWT_POOL_SECRET))
        .await
        .unwrap_or_else(|e| t.fatal(format!("getting the actor JWT pool Secret: {e}")));
    let raw = secret
        .data
        .and_then(|d| d.get("pool").map(|b| b.0.clone()))
        .unwrap_or_default();
    let pool = JwtPool::from_json(&raw)
        .unwrap_or_else(|e| t.fatal(format!("decoding the actor JWT pool: {e}")));
    let cm = Api::<ConfigMap>::namespaced(clients.kube(), &ns)
        .get(&resource_name(API_ENV_CONFIG_MAP))
        .await
        .unwrap_or_else(|e| t.fatal(format!("getting the hivekeep-api ConfigMap: {e}")));
    let issuer = cm
        .data
        .and_then(|d| d.get(ACTOR_JWT_ISSUER_KEY).cloned())
        .unwrap_or_default();
    if issuer.is_empty() {
        t.fatal(format!(
            "{ACTOR_JWT_ISSUER_KEY} is empty in the hivekeep-api ConfigMap"
        ));
    }
    Signer { pool, issuer }
}

#[derive(Clone)]
struct TokenClaims {
    kind: TokenKind,
    audience: String,
    actor: pb::Actor,
    uid: String,
    node: String,
}

/// The token hivekeep-api would mint for the node hosting `actor`.
fn for_actor(actor: &pb::Actor) -> TokenClaims {
    TokenClaims {
        kind: TokenKind::WorkerForActor,
        audience: EXCHANGE_AUDIENCE.into(),
        actor: actor.clone(),
        uid: actor.metadata.clone().unwrap_or_default().uid,
        node: actor.status.clone().unwrap_or_default().assigned_node,
    }
}

fn with_audience(mut c: TokenClaims, audience: &str) -> TokenClaims {
    c.audience = audience.into();
    c
}
fn with_uid(mut c: TokenClaims, uid: &str) -> TokenClaims {
    c.uid = uid.into();
    c
}
fn with_node(mut c: TokenClaims, node: &str) -> TokenClaims {
    c.node = node.into();
    c
}

impl Signer {
    fn jwt(&self, t: &T, c: &TokenClaims) -> String {
        let now = SystemTime::now();
        let meta = c.actor.metadata.clone().unwrap_or_default();
        let claims = Claims {
            kind: c.kind,
            issuer: self.issuer.clone(),
            subject: c.kind.subject(&meta.hive, &meta.name),
            audiences: vec![c.audience.clone()],
            expiration: now + Duration::from_secs(3600),
            not_before: now - Duration::from_secs(60),
            issued_at: now,
            jti: uuid::Uuid::new_v4().to_string(),
            actor: ActorClaims {
                hive: meta.hive,
                actor_name: meta.name,
                actor_uid: c.uid.clone(),
                node: c.node.clone(),
            },
        };
        self.pool
            .active()
            .and_then(|a| a.sign(&claims))
            .unwrap_or_else(|e| t.fatal(format!("signing a {:?} token: {e}", c.kind)))
    }
}

/// An HTTP client for GCS trusting the platform roots.
fn gcs_client() -> reqwest::Client {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    reqwest::Client::builder()
        .tls_backend_preconfigured(config)
        .timeout(Duration::from_secs(60))
        .build()
        .expect("HTTP client")
}

/// Sends one request to GCS's XML API with `token` and returns the status.
/// `uri` is `gs://bucket/object`.
async fn gcs(t: &T, token: &str, method: Method, uri: &str, body: &str) -> u16 {
    let url = format!("{GCS_ENDPOINT}{}", uri.trim_start_matches("gs://"));
    let resp = gcs_client()
        .request(method.clone(), &url)
        .bearer_auth(token)
        .body(body.to_string())
        .send()
        .await
        .unwrap_or_else(|e| t.fatal(format!("{method} {uri}: {e}")));
    let status = resp.status().as_u16();
    let _ = resp.bytes().await;
    status
}

async fn expect_gcs(
    t: &T,
    what: &str,
    token: &str,
    method: Method,
    uri: &str,
    body: &str,
    want: u16,
) {
    let got = gcs(t, token, method.clone(), uri, body).await;
    if got != want {
        t.error(format!("{what}: {method} {uri} = {got}, want {want}"));
    }
}

/// The prefix of the Actor's generation owned by `owner`, which must be
/// COMPLETED in object storage.
fn durable_uri(t: &T, actor: &pb::Actor, owner: pb::SnapshotOwner) -> String {
    let status = actor.status.clone().unwrap_or_default();
    for snap in &status.snapshots {
        if let Some(d) = &snap.durable_snapshot {
            if snap.owner == owner as i32 && d.state == pb::SnapshotStorageState::Completed as i32 {
                return d.object.clone().unwrap_or_default().snapshot_uri;
            }
        }
    }
    t.fatal(format!(
        "actor {} has no completed {owner:?} snapshot: {:?}",
        actor.metadata.clone().unwrap_or_default().name,
        status.snapshots
    ))
}

fn obj(hive: &str, name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    }
}

/// Suspends the Actor and waits for its snapshot to be durable.
async fn suspend_sync(t: &T, clients: &Clients, hive: &str, name: &str) -> pb::Actor {
    let req = pb::SuspendActorRequest {
        actor: Some(obj(hive, name)),
        options: Some(pb::SuspendOptions {
            mode: pb::DurabilityMode::Synchronous as i32,
        }),
    };
    if let Err(e) = clients.api().suspend_actor(req).await {
        t.fatal(format!("SuspendActor {name}: {e}"));
    }
    wait_for_actor_state(t, clients, hive, name, pb::ActorState::Suspended).await
}

async fn resume(t: &T, clients: &Clients, hive: &str, name: &str, when: &str) -> pb::Actor {
    if let Err(e) = resume_actor_await_capacity(
        t,
        clients,
        pb::ResumeActorRequest {
            actor: Some(obj(hive, name)),
        },
    )
    .await
    {
        t.fatal(format!("ResumeActor {name}{when}: {e}"));
    }
    wait_for_actor_state(t, clients, hive, name, pb::ActorState::Running).await
}

/// The storage location a snapshot URI lives under: the part before its
/// owner's `hives` segment.
fn location_of(snapshot_uri: &str) -> String {
    snapshot_uri
        .split_once("/hives/")
        .map(|(l, _)| l)
        .unwrap_or(snapshot_uri)
        .to_string()
}

/// The Actor's own prefix at the location its golden lives in, the
/// read-write grant the exchange derives before the Actor has a snapshot of
/// its own: `<location>/hives/<hive>/actors/<uid>`.
fn own_prefix(golden_uri: &str, actor: &pb::Actor) -> String {
    own_prefix_at(&location_of(golden_uri), actor)
}

/// [`own_prefix`] for a location given directly.
fn own_prefix_at(location: &str, actor: &pb::Actor) -> String {
    let m = actor.metadata.clone().unwrap_or_default();
    format!(
        "{}/hives/{}/actors/{}",
        location.trim_end_matches('/'),
        m.hive,
        m.uid
    )
}

fn stamp() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

fn remaining(resp: &exchange::ExchangeActorJwtResponse) -> Duration {
    let ts = resp.expire_time.unwrap_or_default();
    let expiry =
        SystemTime::UNIX_EPOCH + Duration::new(ts.seconds.max(0) as u64, ts.nanos.max(0) as u32);
    expiry.duration_since(SystemTime::now()).unwrap_or_default()
}

/// The exchange and the boundary it grants through one Actor's life: fresh
/// from a golden it may read the golden and write only under its own
/// prefix; once it and a sibling have their own snapshots, it reads its own
/// and not the sibling's. Tokens the exchange must refuse are checked
/// alongside.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn exchange_scopes_the_token_to_the_actor() {
    e2e_test("TestExchangeScopesTheTokenToTheActor", |t| async move {
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = clients();
        let budget = tokio::time::timeout(Duration::from_secs(15 * 60), async {
            require_gcs(&t, &clients).await;
            let (hive, _) = deploy_probe(
                &t,
                &clients,
                &env["HK_E2E_BUCKET_NAME"],
                "snapcreds",
                ProbeOptions::default(),
            )
            .await;
            let sign = new_signer(&t, &clients).await;
            let xchg = dial_exchange(&t, &clients).await;
            let stamp = stamp();

            // Both Actors start from the golden, wherever the scheduler puts them.
            let alpha = create_and_resume(&t, &clients, &hive, "creds-alpha").await;
            let beta = create_and_resume(&t, &clients, &hive, "creds-beta").await;
            let golden = durable_uri(&t, &alpha, pb::SnapshotOwner::Tag);

            // A token for alpha, as its node would present it.
            let resp = xchg
                .call(&sign.jwt(&t, &for_actor(&alpha)), false)
                .await
                .unwrap_or_else(|e| t.fatal(format!("exchange for a running actor: {e}")));
            let token = resp.access_token.clone();
            if token.is_empty() || remaining(&resp).is_zero() {
                t.fatal(format!(
                    "exchange returned token {token:?} expiring in {:?}",
                    remaining(&resp)
                ));
            }
            // A forced exchange mints from the current source token, which is
            // replaced once under 40 minutes are left; a minute is allowed for
            // clock skew with the provider.
            let fresh = xchg
                .call(&sign.jwt(&t, &for_actor(&alpha)), true)
                .await
                .unwrap_or_else(|e| t.fatal(format!("forced exchange for a running actor: {e}")));
            if remaining(&fresh) < Duration::from_secs(39 * 60) {
                t.error(format!(
                    "a freshly minted token expires in {:?}, want at least 39m",
                    remaining(&fresh)
                ));
            }
            // The golden reads but does not write; the Actor's own (empty)
            // prefix writes.
            expect_gcs(
                &t,
                "golden manifest read",
                &token,
                Method::GET,
                &format!("{golden}/manifest.json"),
                "",
                200,
            )
            .await;
            expect_gcs(
                &t,
                "golden write refused",
                &token,
                Method::PUT,
                &format!("{golden}/e2e-{stamp}"),
                "x",
                403,
            )
            .await;
            let own = own_prefix(&golden, &alpha);
            expect_gcs(
                &t,
                "own prefix write",
                &token,
                Method::PUT,
                &format!("{own}/e2e-{stamp}"),
                "hello",
                200,
            )
            .await;
            expect_gcs(
                &t,
                "own prefix read back",
                &token,
                Method::GET,
                &format!("{own}/e2e-{stamp}"),
                "",
                200,
            )
            .await;
            expect_gcs(
                &t,
                "own prefix delete",
                &token,
                Method::DELETE,
                &format!("{own}/e2e-{stamp}"),
                "",
                204,
            )
            .await;
            let sibling = own_prefix(&golden, &beta);
            expect_gcs(
                &t,
                "sibling actor's prefix refused",
                &token,
                Method::PUT,
                &format!("{sibling}/e2e-{stamp}"),
                "x",
                403,
            )
            .await;
            // 403 outside the boundary, 404 for a missing object inside it:
            // the pair tells a denied read from an absent object.
            expect_gcs(
                &t,
                "sibling actor's prefix read refused",
                &token,
                Method::GET,
                &format!("{sibling}/e2e-{stamp}"),
                "",
                403,
            )
            .await;
            expect_gcs(
                &t,
                "own prefix read of a missing object",
                &token,
                Method::GET,
                &format!("{own}/e2e-missing-{stamp}"),
                "",
                404,
            )
            .await;

            // Tokens the exchange refuses, with the code the node plugin sees.
            let own_token = TokenClaims {
                kind: TokenKind::Actor,
                audience: EXCHANGE_AUDIENCE.into(),
                actor: alpha.clone(),
                uid: alpha.metadata.clone().unwrap_or_default().uid,
                node: String::new(),
            };
            let refusals = [
                (
                    "the actor's own token",
                    sign.jwt(&t, &own_token),
                    Code::PermissionDenied,
                ),
                (
                    "another audience",
                    sign.jwt(&t, &with_audience(for_actor(&alpha), "other")),
                    Code::Unauthenticated,
                ),
                (
                    "a recreated actor's old uid",
                    sign.jwt(
                        &t,
                        &with_uid(for_actor(&alpha), "00000000-0000-4000-8000-000000000000"),
                    ),
                    Code::NotFound,
                ),
                (
                    "a node the actor is not on",
                    sign.jwt(&t, &with_node(for_actor(&alpha), "not-the-node")),
                    Code::PermissionDenied,
                ),
                ("an empty token", String::new(), Code::InvalidArgument),
            ];
            for (name, jwt, want) in refusals {
                let got = xchg
                    .call(&jwt, false)
                    .await
                    .err()
                    .map(|s| s.code())
                    .unwrap_or(Code::Ok);
                if got != want {
                    t.error(format!("exchange of {name} = {got:?}, want {want:?}"));
                }
            }

            // Each Actor gets its own snapshot. Alpha comes back; beta stays
            // suspended with no node, so no node may act for it.
            suspend_sync(&t, &clients, &hive, "creds-alpha").await;
            let beta = suspend_sync(&t, &clients, &hive, "creds-beta").await;
            let got = xchg
                .call(
                    &sign.jwt(&t, &with_node(for_actor(&beta), "any-node")),
                    false,
                )
                .await;
            if got.as_ref().err().map(|s| s.code()) != Some(Code::PermissionDenied) {
                t.error(format!(
                    "exchange for a suspended actor held by no node = {:?}, want PermissionDenied",
                    got.err()
                ));
            }
            let alpha = resume(&t, &clients, &hive, "creds-alpha", "").await;

            // The forced exchange re-derives the grants: the golden is gone
            // from alpha's status, its own snapshot is in.
            let resp = xchg
                .call(&sign.jwt(&t, &for_actor(&alpha)), true)
                .await
                .unwrap_or_else(|e| {
                    t.fatal(format!("forced exchange after the first suspend: {e}"))
                });
            let token = resp.access_token;
            expect_gcs(
                &t,
                "own snapshot manifest read",
                &token,
                Method::GET,
                &format!(
                    "{}/manifest.json",
                    durable_uri(&t, &alpha, pb::SnapshotOwner::Actor)
                ),
                "",
                200,
            )
            .await;
            expect_gcs(
                &t,
                "sibling snapshot manifest refused",
                &token,
                Method::GET,
                &format!(
                    "{}/manifest.json",
                    durable_uri(&t, &beta, pb::SnapshotOwner::Actor)
                ),
                "",
                403,
            )
            .await;
            expect_gcs(
                &t,
                "golden read dropped with the tag generation",
                &token,
                Method::GET,
                &format!("{golden}/manifest.json"),
                "",
                403,
            )
            .await;
        })
        .await;
        if budget.is_err() {
            t.fatal("test exceeded its 15 minute budget");
        }
    });
}

/// Repoints an Actor to a template whose snapshots live under another
/// location and checks the exchange follows. Grants come from the URIs in
/// the Actor's status, so nothing names the new location until the first
/// suspend records a pending snapshot there: until then the exchange keeps
/// answering for the old location, the suspend's upload is the first
/// request the new location sees, and it goes through on the node plugin's
/// forced re-exchange. Afterwards an unforced exchange mints for the new
/// location and the old token reaches nothing the Actor owns. Per-template
/// locations are a preview feature.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn repoint_moves_the_grant() {
    e2e_test("TestRepointMovesTheGrant", |t| async move {
        require_preview(&t, Gate::PerTemplateSnapshotLocation);
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let bucket = env["HK_E2E_BUCKET_NAME"].clone();
        let clients = clients();
        let budget = tokio::time::timeout(Duration::from_secs(20 * 60), async {
            require_gcs(&t, &clients).await;
            let (hive, probe) = deploy_probe(&t, &clients, &bucket, "snapcreds-repoint", ProbeOptions::default()).await;
            let sign = new_signer(&t, &clients).await;
            let xchg = dial_exchange(&t, &clients).await;
            let stamp = stamp();

            // Template B is the probe template under another location in the
            // same bucket; the Actor's next snapshot goes there.
            let name_b = "probe-repoint";
            let location_b = format!("gs://{bucket}/hivekeep-e2e-repoint-{stamp}/");
            let mut template_b = probe.clone();
            template_b.metadata = Some(pb::ResourceMetadata { hive: hive.clone(), name: name_b.into(), ..Default::default() });
            template_b.status = None;
            let mut snap = probe.snapshot_config.clone().unwrap_or_default();
            snap.storage_location = location_b.clone();
            template_b.snapshot_config = Some(snap);
            if let Err(e) = clients.api().create_actor_template(pb::CreateActorTemplateRequest { actor_template: Some(template_b) }).await {
                t.fatal(format!("CreateActorTemplate {name_b}: {e}"));
            }
            register_template_cleanup(&t, &clients, &hive, name_b);
            wait_for_template_ready(&t, &*clients, &hive, name_b).await;

            // Running from template A, the token writes under A's location only.
            let name = "creds-repoint";
            let actor = create_and_resume(&t, &clients, &hive, name).await;
            let location_a = location_of(&durable_uri(&t, &actor, pb::SnapshotOwner::Tag));
            let first = xchg.call(&sign.jwt(&t, &for_actor(&actor)), false).await.unwrap_or_else(|e| t.fatal(format!("exchange before the repoint: {e}")));
            let a_obj = format!("{}/e2e-{stamp}", own_prefix_at(&location_a, &actor));
            let b_obj = format!("{}/e2e-{stamp}", own_prefix_at(&location_b, &actor));
            expect_gcs(&t, "old location write before the repoint", &first.access_token, Method::PUT, &a_obj, "a", 200).await;
            expect_gcs(&t, "old location delete before the repoint", &first.access_token, Method::DELETE, &a_obj, "", 204).await;
            expect_gcs(&t, "new location write before the repoint", &first.access_token, Method::PUT, &b_obj, "b", 403).await;

            // A repoint needs a suspended Actor that owns no snapshot, so it
            // is reverted to its golden rather than suspended.
            if let Err(e) = clients.api().revert_actor(pb::RevertActorRequest { actor: Some(obj(&hive, name)) }).await {
                t.fatal(format!("RevertActor {name}: {e}"));
            }
            let mut actor = wait_for_actor_state(&t, &clients, &hive, name, pb::ActorState::Suspended).await;
            actor.actor_template = Some(obj(&hive, name_b));
            if let Err(e) = clients.api().update_actor(pb::UpdateActorRequest { actor: Some(actor) }).await {
                t.fatal(format!("UpdateActor {name} to template {name_b}: {e}"));
            }
            let actor = resume(&t, &clients, &hive, name, " after the repoint").await;

            // The status still names only A's location, so the exchange still
            // grants A and not B, whatever it serves from its cache.
            let between = xchg.call(&sign.jwt(&t, &for_actor(&actor)), false).await.unwrap_or_else(|e| t.fatal(format!("exchange after the repoint: {e}")));
            expect_gcs(&t, "new location write after the repoint, before any snapshot there", &between.access_token, Method::PUT, &b_obj, "b", 403).await;
            expect_gcs(&t, "old location write after the repoint", &between.access_token, Method::PUT, &a_obj, "a", 200).await;
            expect_gcs(&t, "old location delete after the repoint", &between.access_token, Method::DELETE, &a_obj, "", 204).await;

            // The real path: the suspend records a pending snapshot under B,
            // the node's cached token is refused there, and one forced
            // re-exchange, now granting B, carries the upload.
            let actor = suspend_sync(&t, &clients, &hive, name).await;
            let snapshot = durable_uri(&t, &actor, pb::SnapshotOwner::Actor);
            if !snapshot.starts_with(&own_prefix_at(&location_b, &actor)) {
                t.fatal(format!("snapshot after the repoint is at {snapshot}, want it under {}", own_prefix_at(&location_b, &actor)));
            }

            // A suspended Actor is on no node, so the exchange answers again
            // once it runs. Now the status names B, so an unforced exchange
            // must not serve the old token.
            let actor = resume(&t, &clients, &hive, name, " after the suspend").await;
            let after = xchg.call(&sign.jwt(&t, &for_actor(&actor)), false).await.unwrap_or_else(|e| t.fatal(format!("exchange after the suspend: {e}")));
            if after.access_token == first.access_token {
                t.error("the exchange served the old token after the suspend, its grants no longer cover the actor");
            }
            expect_gcs(&t, "snapshot read with the new token", &after.access_token, Method::GET, &format!("{snapshot}/manifest.json"), "", 200).await;
            expect_gcs(&t, "snapshot read with the old token", &first.access_token, Method::GET, &format!("{snapshot}/manifest.json"), "", 403).await;

            // The suspend pruned the generation at A, so A is no longer
            // granted. Only a forced exchange shows it: an unforced one may
            // serve the upload's token, which covered both.
            let fresh = xchg.call(&sign.jwt(&t, &for_actor(&actor)), true).await.unwrap_or_else(|e| t.fatal(format!("forced exchange after the suspend: {e}")));
            expect_gcs(&t, "old location write after the suspend", &fresh.access_token, Method::PUT, &a_obj, "a", 403).await;
        })
        .await;
        if budget.is_err() {
            t.fatal("test exceeded its 20 minute budget");
        }
    });
}

/// Polls until the Actor reaches `want`, for up to two minutes: snapshot
/// operations on this suite's lane are slower than the default.
async fn wait_for_actor_state(
    t: &T,
    clients: &Clients,
    hive: &str,
    name: &str,
    want: pb::ActorState,
) -> pb::Actor {
    actors::wait_for_actor_state_within(t, clients, hive, name, want, Duration::from_secs(120))
        .await
}

/// Creates an Actor from the probe template (clearing a leftover; records
/// outlive the fixture namespace) and resumes it from the golden snapshot.
async fn create_and_resume(t: &T, clients: &Arc<Clients>, hive: &str, name: &str) -> pb::Actor {
    actors::create_and_resume_actor(t, clients, hive, name, PROBE_TEMPLATE).await;
    wait_for_actor_state(t, clients, hive, name, pb::ActorState::Running).await
}
