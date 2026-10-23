//! What both egress MITM tests share: the origin servers, the probe's
//! `/fetch` and `/whoami` calls through the router, and Actor lifecycle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::egresspolicy::{
    egress_allow_http, egress_allow_https, egress_allow_passthrough,
    egress_allow_passthrough_on_ports, ensure_egress_policy,
};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::serverpod::{service_dns_tls, Server, ServerPod};
use hivekeep_proto::api::v1 as pb;
use serde::Deserialize;

/// The probe fixture's template.
pub const PROBE_TEMPLATE: &str = "probe";

/// In the reserved 240/4 block: a gate that dialed it instead of resolving
/// the SNI would reach nothing.
pub const UNREACHABLE_ADDRESS: &str = "240.0.0.1:443";
/// Common name of a WebPKI root the keeper image ships.
pub const SYSTEM_ROOT: &str = "ISRG Root X1";

/// Bounds the retries of a fetch that must return 200.
pub const REACH_WINDOW: Duration = Duration::from_secs(120);

/// An origin answering `/healthz` over HTTPS and HTTP alike, with a
/// service-DNS certificate.
pub fn origin_spec(name: &str) -> ServerPod {
    service_dns_tls(ServerPod {
        name: name.into(),
        args: vec!["http".into()],
        ..Default::default()
    })
}

/// Query-escapes `s` the way URL query values are encoded.
pub fn escape(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// The probe's `/fetch` result.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct FetchResponse {
    pub status: String,
    pub error: String,
    pub body: String,
    pub tls_handshake: String,
}

/// Whether [`fetch_until_reached`] retries `resp`: any transport error (a
/// stale gate signing pool, a route still propagating) and any 5xx.
pub fn transient_failure(resp: &FetchResponse) -> bool {
    !resp.error.is_empty() || resp.status.starts_with('5')
}

/// [`probe_fetch`] for a fetch that must return 200: transient failures are
/// retried for up to [`REACH_WINDOW`], then the last result is returned for
/// the caller to fail on. It never skips: reaching the origins is part of
/// what the lane proves.
pub async fn fetch_until_reached(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
    origin: &str,
    roots: &str,
    extra: &[String],
) -> FetchResponse {
    let deadline = Instant::now() + REACH_WINDOW;
    loop {
        let resp = probe_fetch(t, rc, hive, id, origin, roots, extra).await;
        if !transient_failure(&resp) || Instant::now() > deadline {
            return resp;
        }
        t.log(format!(
            "fetch of {origin}: transient failure (status {:?}, error {:?}), retrying",
            resp.status, resp.error
        ));
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Asks the probe to fetch `origin` with the given roots mode; `extra` are
/// further pre-encoded query parameters. Router-level failures are retried
/// for 30 s (a resume can return before the route reaches the router);
/// probe-level TLS failures are results for the caller to assert on.
pub async fn probe_fetch(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
    origin: &str,
    roots: &str,
    extra: &[String],
) -> FetchResponse {
    let mut path = format!("/fetch?roots={roots}&url={}", escape(origin));
    for p in extra {
        path.push('&');
        path.push_str(p);
    }
    let actor = ActorRef::new(hive, id);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
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
                "GET {path} for {id:?}: status {}, body {:?}",
                resp.status,
                resp.text()
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// The trust bundle the probe's `/whoami` reads from its projected file.
pub async fn probe_trust(t: &T, rc: &RouterClient, hive: &str, id: &str) -> String {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Who {
        trust: String,
        error: String,
    }
    let resp = rc
        .get(&ActorRef::new(hive, id), "/whoami")
        .await
        .unwrap_or_else(|e| t.fatal(format!("GET /whoami for {id:?}: {e}")));
    let who: Who = serde_json::from_slice(&resp.body).unwrap_or_else(|e| {
        t.fatal(format!(
            "decoding /whoami for {id:?} (status {}): {e}",
            resp.status
        ))
    });
    if who.trust.is_empty() {
        t.fatal(format!(
            "/whoami for {id:?} returned no trust bundle; error: {}",
            who.error
        ));
    }
    who.trust
}

/// Whether `bundle` holds a certificate whose subject common name is
/// `common_name`.
pub fn holds_certificate(t: &T, bundle: &str, common_name: &str) -> bool {
    let ders = hivekeep_identity::pem::decode_certificates(bundle.as_bytes())
        .unwrap_or_else(|e| t.fatal(format!("parsing the trust bundle: {e}")));
    ders.iter().any(|der| {
        let (_, cert) = x509_parser::parse_x509_certificate(der)
            .unwrap_or_else(|e| t.fatal(format!("parsing a certificate in the trust bundle: {e}")));
        let matches = cert
            .subject()
            .iter_common_name()
            .any(|cn| cn.as_str().ok() == Some(common_name));
        matches
    })
}

/// Polls the Actor until it reaches `want` (60 s).
pub async fn wait_for_actor_state(
    t: &T,
    clients: &Clients,
    hive: &str,
    name: &str,
    want: pb::ActorState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let got = clients
            .api()
            .get_actor(pb::GetActorRequest {
                actor: Some(pb::ObjectRef {
                    hive: hive.into(),
                    name: name.into(),
                }),
            })
            .await;
        if let Ok(a) = got {
            if a.into_inner().status.map(|s| s.state) == Some(want as i32) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    t.fatal(format!(
        "timed out waiting for actor {name:?} to reach state {want:?}"
    ))
}

/// Suspends then deletes `r`, ignoring errors.
pub async fn remove_actor(clients: &Clients, r: &pb::ObjectRef) -> Result<(), tonic::Status> {
    let _ = clients
        .api()
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(r.clone()),
            ..Default::default()
        })
        .await;
    clients
        .api()
        .delete_actor(pb::DeleteActorRequest {
            actor: Some(r.clone()),
            ..Default::default()
        })
        .await
        .map(|_| ())
}

/// Creates (after clearing any leftover: Actor records outlive the fixture
/// namespace) and resumes the probe Actor `id`, whose policy names only the
/// origins so the same Actor can show a denial too. The passthrough
/// origin's fully qualified alias is allowed on 8443 only, while the probe
/// dials 443.
pub async fn create_and_resume_actor(
    t: &T,
    clients: &Arc<Clients>,
    hive: &str,
    id: &str,
    mitm: &Server,
    passthrough: &Server,
) {
    let r = pb::ObjectRef {
        hive: hive.into(),
        name: id.into(),
    };
    let _ = remove_actor(clients, &r).await;
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
    let (mitm_name, pt_name) = (mitm.svc_name(), passthrough.svc_name());
    let mitm_fq = format!("{mitm_name}.cluster.local");
    let pt_fq = format!("{pt_name}.cluster.local");
    ensure_egress_policy(
        t,
        clients,
        &r,
        vec![
            egress_allow_http(&[&mitm_name, &mitm_fq]),
            egress_allow_https(&[&mitm_name]),
            egress_allow_passthrough(&[&pt_name]),
            egress_allow_passthrough_on_ports(&[8443], &[&pt_fq]),
        ],
    )
    .await;
    let (c, rr, idc, h) = (clients.clone(), r.clone(), id.to_string(), hive.to_string());
    t.cleanup(move |t| async move {
        if let Err(e) = remove_actor(&c, &rr).await {
            t.log(format!("cleanup: DeleteActor {idc:?} failed, actor leaked (remove with: hivectl delete actor {h}/{idc}): {e}"));
        }
    });
    if let Err(e) = clients
        .api()
        .resume_actor(pb::ResumeActorRequest { actor: Some(r) })
        .await
    {
        t.fatal(format!("ResumeActor {id:?}: {e}"));
    }
}
