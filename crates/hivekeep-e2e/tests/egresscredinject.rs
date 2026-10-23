//! Egress credential injection: an https (or http) rule with a
//! `replace_headers` effect makes the egress gate resolve the credential
//! through the Kubernetes Secrets credential provider and replace the
//! Actor's placeholder header with it before re-originating upstream. The
//! same rule also injects an Actor JWT minted by hivekeep-api.
//!
//! Needs an install with the bundled credential provider and a gate that
//! trusts the service-DNS signer (the echo origin's certificate issuer).

use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::actors::{clear_leftover_actor, wait_for_actor_state};
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::credprovider::{
    configure_credential_provider, CREDENTIAL_INJECTION_TOKEN, CREDENTIAL_INJECTION_URI,
    CREDENTIAL_SECRETS_NAMESPACE,
};
use hivekeep_e2e::egresspolicy::{
    egress_inject_header, egress_inject_header_http, ensure_egress_policy,
};
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::probe::{deploy_probe, ProbeOptions};
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::serverpod::{deploy_server_pod, service_dns_tls, ServerPod};
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_identity::jwt::claims::{ActorClaims, WireClaims};
use hivekeep_proto::api::v1 as pb;
use serde::Deserialize;

const PROBE_TEMPLATE: &str = "probe";

// The hosts of the fail-closed cases.
const UNFETCHABLE_HOST: &str = "unfetchable.egresscredinject.invalid";
const UNSERVED_HOST: &str = "unserved.egresscredinject.invalid";
const UNAUTHORIZED_HOST: &str = "unauthorized.egresscredinject.invalid";

/// The value the Actor sends in each injected header: replace_headers only
/// replaces a header the request carries.
const PLACEHOLDER: &str = "Bearer actor-placeholder";

// The echo rules also replace this header with an Actor JWT.
const ACTOR_JWT_HEADER: &str = "X-Actor-Token";
const ACTOR_JWT_AUDIENCE: &str = "https://egresscredinject.invalid";
const ACTOR_JWT_LIFETIME: i64 = 600;

const ECHO_RETRY_WINDOW: Duration = Duration::from_secs(120);

fn escape(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

fn with_placeholder() -> Vec<String> {
    vec![
        format!("header={}", escape(&format!("Authorization:{PLACEHOLDER}"))),
        format!(
            "header={}",
            escape(&format!("{ACTOR_JWT_HEADER}:{PLACEHOLDER}"))
        ),
    ]
}

/// The origin the injection fetches reach, over HTTPS and HTTP alike:
/// `/echoheaders` returns the headers it received.
fn echo_server() -> ServerPod {
    service_dns_tls(ServerPod {
        name: "echo".into(),
        args: vec!["http".into()],
        ..Default::default()
    })
}

/// The injected credential reaches the upstream, and every way injection can
/// go wrong lands on the documented side of fail-open vs fail-closed:
///
/// - replaced: a fetch carrying the placeholder echoes the injected
///   `Authorization: Bearer <token>` (the on-the-wire proof) and not the
///   placeholder, plus an Actor JWT naming this Actor;
/// - no header: without the placeholders neither header is echoed (only a
///   header the request carries is replaced, none is added);
/// - cleartext: the same over plain HTTP, through an http rule;
/// - fail closed: a credential the provider will not or cannot produce
///   denies the request (403 for an unfetchable secret or an unauthorized
///   namespace, 403 or 500 for an unserved provider), over both schemes.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_egress_credential_injection() {
    e2e_test("TestActorEgressCredentialInjection", |t| async move {
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = clients();

        let echo = deploy_server_pod(&t, &echo_server()).await;
        // The name the service-DNS signer certifies.
        let echo_host = echo.svc_name();

        configure_credential_provider(&t);

        let (hive, _) = deploy_probe(
            &t,
            &clients,
            &env["HK_E2E_BUCKET_NAME"],
            "egresscredinject",
            ProbeOptions::default().with_trust_bundle(),
        )
        .await;

        let id = "probe-credinject";
        create_and_resume_actor(&t, &clients, &hive, id, &echo_host).await;
        wait_for_actor_state(&t, &clients, &hive, id, pb::ActorState::Running).await;
        let actor = clients
            .api()
            .get_actor(pb::GetActorRequest {
                actor: Some(pb::ObjectRef {
                    hive: hive.clone(),
                    name: id.into(),
                }),
            })
            .await
            .unwrap_or_else(|e| t.fatal(format!("GetActor {id:?}: {e}")))
            .into_inner();
        let meta = actor.metadata.unwrap_or_default();

        let rc = RouterClient::new(&t, &clients)
            .await
            .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));

        // HTTPS is injected on the gate's intercepting leg and HTTP on its
        // cleartext leg, each by a rule with the same effect.
        let legs = [("https", echo.tls_address()), ("http", echo.address())];
        for (scheme, _) in &legs {
            let (rc, hive, meta, echo_host) = (&rc, &hive, &meta, &echo_host);
            t.run(scheme, |t| async move {
                let origin = format!("{scheme}://{echo_host}/echoheaders");
                let want = format!("Bearer {CREDENTIAL_INJECTION_TOKEN}");
                let replaced = assert_echoed_headers(&t, "injection fetch", fetch_echo(&t, rc, hive, id, &origin, &with_placeholder()).await);
                if replaced.get("Authorization") != Some(&want) {
                    t.error(format!("upstream received Authorization {:?}, want the injected {want:?}", replaced.get("Authorization")));
                }
                check_actor_jwt(&t, replaced.get(ACTOR_JWT_HEADER).map(String::as_str).unwrap_or(""), meta);

                // No header is added when the Actor did not send one.
                let unasked = assert_echoed_headers(&t, "fetch without the headers", fetch_echo(&t, rc, hive, id, &origin, &[]).await);
                for h in ["Authorization", ACTOR_JWT_HEADER] {
                    if let Some(got) = unasked.get(h) {
                        t.error(format!("upstream received {h} {got:?} on a request that did not carry it, want none"));
                    }
                }
            })
            .await;
        }

        // Fail closed: each rule names a credential that cannot be
        // injected; the denial must be the mapped status, not a request that
        // went out without it. No retries: the gate answers these itself.
        let cases: [(&str, &str, &[&str]); 3] = [
            ("unfetchable secret", UNFETCHABLE_HOST, &["403"]),
            ("unserved provider", UNSERVED_HOST, &["500"]),
            ("unauthorized namespace", UNAUTHORIZED_HOST, &["403"]),
        ];
        for (name, host, want) in cases {
            for (scheme, dial) in &legs {
                let (rc, hive) = (&rc, &hive);
                t.run(&format!("{name}/{scheme}"), |t| async move {
                    let origin = format!("{scheme}://{host}/");
                    let mut params = with_placeholder();
                    params.push(format!("dial={}", escape(dial)));
                    let got = probe_fetch(&t, rc, hive, id, &origin, &params).await;
                    if !got.error.is_empty() {
                        t.fatal(format!("fetch of {origin} failed at the transport ({}), want an HTTP denial {want:?} from the gate", got.error));
                    }
                    if !want.contains(&got.status.as_str()) {
                        t.error(format!("fetch of {origin} returned status {}, want one of {want:?}", got.status));
                    }
                })
                .await;
            }
        }
    });
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FetchResponse {
    status: String,
    error: String,
    body: String,
}

/// Whether [`fetch_echo`] retries: a certificate error until the gate's
/// signing pool propagates, or 503 while the gate reconnects to a
/// redeployed provider.
fn transient_echo_failure(resp: &FetchResponse) -> bool {
    if !resp.error.is_empty() {
        return resp.error.contains("certificate") || resp.error.contains("x509");
    }
    resp.status == "503"
}

/// [`probe_fetch`] for echo fetches that should return 200.
async fn fetch_echo(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
    origin: &str,
    extra: &[String],
) -> FetchResponse {
    let deadline = Instant::now() + ECHO_RETRY_WINDOW;
    loop {
        let resp = probe_fetch(t, rc, hive, id, origin, extra).await;
        if !transient_echo_failure(&resp) || Instant::now() > deadline {
            return resp;
        }
        t.log(format!(
            "fetch of {origin}: transient failure (status {:?}, error {:?}), retrying",
            resp.status, resp.error
        ));
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Fails on any transport- or HTTP-level failure of an echo fetch and
/// returns the headers the upstream received.
fn assert_echoed_headers(
    t: &T,
    step: &str,
    resp: FetchResponse,
) -> std::collections::BTreeMap<String, String> {
    if !resp.error.is_empty() {
        t.fatal(format!(
            "{step}: TLS through the MITM egress gate failed: {}",
            resp.error
        ));
    }
    if resp.status != "200" {
        t.fatal(format!(
            "{step}: status {}, want 200 (an injection failure would deny with 403/500/503; is the credential provider deployed and the gate pointed at it, and does the gate trust the echo origin's certificate?) body {:?}",
            resp.status, resp.body
        ));
    }
    #[derive(Deserialize)]
    struct Echoed {
        #[serde(default)]
        headers: std::collections::BTreeMap<String, String>,
    }
    serde_json::from_str::<Echoed>(&resp.body)
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "{step}: decoding the echo origin's body: {e} (body {:?})",
                resp.body
            ))
        })
        .headers
}

/// Checks an injected Actor JWT's claims against the Actor and the rule's
/// actor_jwt source (not its signature).
fn check_actor_jwt(t: &T, jwt: &str, actor: &pb::ResourceMetadata) {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() != 3 {
        t.error(format!(
            "upstream received {ACTOR_JWT_HEADER} {jwt:?}, want an actor JWT"
        ));
        return;
    }
    let payload = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(p) => p,
        Err(e) => {
            t.error(format!("decoding the actor JWT payload: {e}"));
            return;
        }
    };
    let shown = String::from_utf8_lossy(&payload).into_owned();
    let claims: WireClaims = match serde_json::from_slice(&payload) {
        Ok(c) => c,
        Err(e) => {
            t.error(format!(
                "decoding the actor JWT claims: {e} (payload {shown})"
            ));
            return;
        }
    };
    let aud: Vec<String> = match &claims.audiences {
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        Some(v) => serde_json::from_value(v.clone()).unwrap_or_else(|e| {
            t.error(format!(
                "decoding the actor JWT aud claim: {e} (payload {shown})"
            ));
            Vec::new()
        }),
        None => Vec::new(),
    };
    let want_sub = format!("actor/{}/{}", actor.hive, actor.name);
    if claims.subject != want_sub {
        t.error(format!(
            "actor JWT sub is {:?}, want {want_sub:?}",
            claims.subject
        ));
    }
    if aud != [ACTOR_JWT_AUDIENCE] {
        t.error(format!(
            "actor JWT aud is {aud:?}, want [{ACTOR_JWT_AUDIENCE:?}]"
        ));
    }
    let lifetime = claims.expiration - claims.issued_at;
    if lifetime != ACTOR_JWT_LIFETIME as f64 {
        t.error(format!(
            "actor JWT lives {lifetime}s (exp - iat), want {ACTOR_JWT_LIFETIME}s"
        ));
    }
    let want = ActorClaims {
        hive: actor.hive.clone(),
        actor_name: actor.name.clone(),
        actor_uid: actor.uid.clone(),
        node: String::new(),
    };
    let got = claims
        .actor
        .clone()
        .and_then(|v| serde_json::from_value::<ActorClaims>(v).ok());
    if claims.worker_for_actor.is_some() || got.as_ref() != Some(&want) {
        t.error(format!(
            "actor JWT payload is {shown}, want only an actor.hivekeep.io claim of {want:?}"
        ));
    }
}

/// Asks the probe to fetch `origin` with the projected bundle; `extra` are
/// pre-encoded query parameters. Router-level failures are retried for 30 s;
/// probe-level TLS failures are results.
async fn probe_fetch(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
    origin: &str,
    extra: &[String],
) -> FetchResponse {
    let mut path = format!("/fetch?roots=bundle&url={}", escape(origin));
    for p in extra {
        path.push('&');
        path.push_str(p);
    }
    let actor = ActorRef::new(hive, id);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let resp = rc
            .get(&actor, &path)
            .await
            .unwrap_or_else(|e| t.fatal(format!("GET {path} for {id:?}: {e}")));
        if resp.status == 200 {
            return serde_json::from_slice(&resp.body).unwrap_or_else(|e| {
                t.fatal(format!(
                    "decoding {path} response for {id:?}: {e} (body {:?})",
                    resp.text()
                ))
            });
        }
        if Instant::now() > deadline {
            t.fatal(format!(
                "GET {path} for {id:?}: status {} after {attempt} attempts, body {:?}",
                resp.status,
                resp.text()
            ));
        }
        t.log(format!(
            "GET {path} for {id:?}: attempt {attempt}: status {}, body {:?}; retrying",
            resp.status,
            resp.text()
        ));
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn actor_jwt_injection() -> pb::CredentialHeader {
    pb::CredentialHeader {
        header: ACTOR_JWT_HEADER.into(),
        actor_jwt: Some(pb::ActorJwtSource {
            audiences: vec![ACTOR_JWT_AUDIENCE.into()],
            expiration_seconds: ACTOR_JWT_LIFETIME,
        }),
        ..Default::default()
    }
}

/// Creates (after clearing a leftover; records outlive the fixture
/// namespace) and resumes the probe Actor with rules covering each
/// credential outcome over HTTP and HTTPS.
async fn create_and_resume_actor(
    t: &T,
    clients: &Arc<Clients>,
    hive: &str,
    id: &str,
    echo_host: &str,
) {
    let r = pb::ObjectRef {
        hive: hive.into(),
        name: id.into(),
    };
    clear_leftover_actor(t, clients, &r).await;
    let actor = pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: hive.into(),
            name: id.into(),
            ..Default::default()
        }),
        actor_template: Some(pb::ObjectRef {
            hive: hive.into(),
            name: PROBE_TEMPLATE.into(),
        }),
        ..Default::default()
    };
    if let Err(e) = clients
        .api()
        .create_actor(pb::CreateActorRequest { actor: Some(actor) })
        .await
    {
        t.fatal(format!("CreateActor {id:?}: {e}"));
    }
    let (c, rr, idc, h) = (clients.clone(), r.clone(), id.to_string(), hive.to_string());
    t.cleanup(move |t| async move {
        if let Err(e) = c.api().suspend_actor(pb::SuspendActorRequest { actor: Some(rr.clone()), ..Default::default() }).await {
            t.log(format!("cleanup: SuspendActor {idc:?}: {e}"));
        }
        if let Err(e) = c.api().delete_actor(pb::DeleteActorRequest { actor: Some(rr), ..Default::default() }).await {
            t.log(format!("cleanup: DeleteActor {idc:?} failed, actor leaked (remove with: hivectl delete actor {h}/{idc}): {e}"));
        }
    });

    let fq = format!("{echo_host}.cluster.local");
    let mut https_rule = egress_inject_header(
        "Authorization",
        "Bearer ",
        CREDENTIAL_INJECTION_URI,
        &[echo_host, &fq],
    );
    if let Some(effects) = https_rule.https.as_mut().and_then(|h| h.effects.as_mut()) {
        effects.replace_headers.push(actor_jwt_injection());
    }
    let mut http_rule = egress_inject_header_http(
        "Authorization",
        "Bearer ",
        CREDENTIAL_INJECTION_URI,
        &[echo_host, &fq],
    );
    if let Some(effects) = http_rule.http.as_mut().and_then(|h| h.effects.as_mut()) {
        effects.replace_headers.push(actor_jwt_injection());
    }
    let mut rules = vec![https_rule, http_rule];
    let failing = [
        (
            UNFETCHABLE_HOST,
            format!(
                "hk-secret://k8s.io/default/{CREDENTIAL_SECRETS_NAMESPACE}/no-such-secret/token"
            ),
        ),
        (
            UNSERVED_HOST,
            format!("hk-secret://other.io/default/{CREDENTIAL_SECRETS_NAMESPACE}/api-token/token"),
        ),
        (
            UNAUTHORIZED_HOST,
            "hk-secret://k8s.io/default/kube-system/api-token/token".to_string(),
        ),
    ];
    for (host, uri) in &failing {
        rules.push(egress_inject_header(
            "Authorization",
            "Bearer ",
            uri,
            &[host],
        ));
        rules.push(egress_inject_header_http(
            "Authorization",
            "Bearer ",
            uri,
            &[host],
        ));
    }
    ensure_egress_policy(t, clients, &r, rules).await;

    if let Err(e) = clients
        .api()
        .resume_actor(pb::ResumeActorRequest { actor: Some(r) })
        .await
    {
        t.fatal(format!("ResumeActor {id:?}: {e}"));
    }
}
