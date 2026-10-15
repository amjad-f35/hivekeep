use std::time::SystemTime;

use envoy_types::pb::google::protobuf::value::Kind;
use parking_lot::Mutex;

use super::*;
use crate::egress_policy::SniMode;
use crate::router::egress::test_support::*;
use crate::test_pki::TestCa;

type Res = Result<HandlerResult, RouteError>;

fn handler_with(api: Arc<MockApi>, roots: Option<RootsLoader>, ttl: Duration) -> Handler {
    Handler::new(Arc::new(Api(api)), roots, ttl, None, "")
}

fn egress_handler(
    ca: &Arc<Mutex<TestCa>>,
    actor: Option<pb::Actor>,
    err: Option<tonic::Status>,
) -> Handler {
    let api = MockApi::with(actor, Some(allow_all_policy()));
    *api.err.lock() = err;
    handler_with(
        api,
        Some(loader(ca)),
        super::super::DEFAULT_POLICY_CACHE_TTL,
    )
}

async fn run(h: &Handler, md: &RequestMetadata) -> Res {
    h.handle(md, &CancellationToken::new()).await
}

fn want_status(r: &Res, want: u16) {
    match r {
        Ok(_) => panic!("allowed, want {want}"),
        Err(e) => assert_eq!(e.http_status(), want, "{e}"),
    }
}

fn ns_field(res: &HandlerResult, ns: &str, key: &str) -> String {
    let md = res.dynamic_metadata.as_ref().unwrap();
    let Some(Kind::StructValue(inner)) = &md.fields[ns].kind else {
        panic!("no {ns}")
    };
    match inner.fields.get(key).and_then(|v| v.kind.as_ref()) {
        Some(Kind::StringValue(s)) => s.clone(),
        _ => String::new(),
    }
}

fn dialed_port_of(res: &HandlerResult) -> String {
    ns_field(res, EGRESS_METADATA_NAMESPACE, EGRESS_DIALED_PORT_KEY)
}

fn sni_rules_of(res: &HandlerResult) -> Vec<SniRule> {
    let json = ns_field(
        res,
        EGRESS_POLICY_METADATA_NAMESPACE,
        EGRESS_POLICY_JSON_KEY,
    );
    let v: serde_json::Value = serde_json::from_str(&json).expect("policy JSON");
    v[EGRESS_SNI_RULES_KEY]
        .as_array()
        .expect("rules list")
        .iter()
        .map(|r| SniRule {
            pattern: r[EGRESS_SNI_RULE_PATTERN_KEY].as_str().unwrap().to_string(),
            mode: if r[EGRESS_SNI_RULE_MODE_KEY] == "mitm" {
                SniMode::Mitm
            } else {
                SniMode::Passthrough
            },
        })
        .collect()
}

fn new_ca() -> Arc<Mutex<TestCa>> {
    Arc::new(Mutex::new(TestCa::new()))
}

#[tokio::test]
async fn allows_verified_actor() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let h = egress_handler(&ca, Some(running_actor()), None);
    let res = run(&h, &egress_metadata(&xfcc_header(&[&leaf])))
        .await
        .unwrap();
    assert!(res.resume.is_empty() && res.target.is_empty());
    assert_eq!(dialed_port_of(&res), "80");
}

#[tokio::test]
async fn connect_leg_opens_for_any_rules() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let mitm = |p: &[&str]| {
        p.iter()
            .map(|p| SniRule {
                pattern: p.to_string(),
                mode: SniMode::Mitm,
            })
            .collect::<Vec<_>>()
    };
    let pass = |p: &[&str]| {
        p.iter()
            .map(|p| SniRule {
                pattern: p.to_string(),
                mode: SniMode::Passthrough,
            })
            .collect::<Vec<_>>()
    };
    let r = |p: &str, m| SniRule {
        pattern: p.into(),
        mode: m,
    };
    let cases: Vec<(&str, pb::EgressPolicy, &str, Vec<SniRule>)> = vec![
        ("http", http_policy(&["api.example.com"]), "", vec![]),
        (
            "https",
            https_policy(&["api.example.com"]),
            "",
            mitm(&["api.example.com"]),
        ),
        (
            "tls passthrough",
            passthrough_policy(ports(&[443]), &["*"]),
            "",
            pass(&["*"]),
        ),
        ("allow all", allow_all_policy(), "", mitm(&["*"])),
        (
            "https and tls passthrough rules, most specific first",
            combined(vec![
                https_policy(&["*.example.org", "api.example.com"]),
                http_policy(&["plain.example.com"]),
                passthrough_policy(ports(&[443]), &["foo.bar.com"]),
            ]),
            "",
            vec![
                r("api.example.com", SniMode::Mitm),
                r("foo.bar.com", SniMode::Passthrough),
                r("*.example.org", SniMode::Mitm),
            ],
        ),
        (
            "https rule on another port",
            https_policy(&["api.example.com"]),
            "93.184.216.34:8443",
            vec![],
        ),
        (
            "https rule on the dialed port",
            https_policy_on(ports(&[8443]), &["api.example.com"]),
            "93.184.216.34:8443",
            mitm(&["api.example.com"]),
        ),
    ];
    for (name, policy, dialed, want) in cases {
        let h = handler_with(
            MockApi::with(Some(running_actor()), Some(policy)),
            Some(loader(&ca)),
            Duration::ZERO,
        );
        let mut md = egress_metadata(&xfcc_header(&[&leaf]));
        md.host = if dialed.is_empty() {
            "93.184.216.34:443".into()
        } else {
            dialed.into()
        };
        md.headers.insert(":authority".into(), md.host.clone());
        let res = run(&h, &md).await.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            dialed_port_of(&res),
            md.host.rsplit_once(':').unwrap().1,
            "{name}"
        );
        assert_eq!(sni_rules_of(&res), want, "{name}");
    }
}

fn certificate_attribute_metadata(cert_pem: &str) -> RequestMetadata {
    RequestMetadata::new(
        &[
            raw(":method", "CONNECT"),
            raw(":authority", "93.184.216.34:80"),
        ],
        attrs(&[(
            super::super::identity::CLIENT_CERTIFICATE_ATTRIBUTE,
            cert_pem,
        )]),
    )
}

#[tokio::test]
async fn connect_leg_without_request_legs() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let pem = hivekeep_identity::pem::encode(hivekeep_identity::pem::CERTIFICATE, &leaf);
    let h = handler_with(
        MockApi::with(
            Some(running_actor()),
            Some(http_policy(&["api.example.com"])),
        ),
        Some(loader(&ca)),
        Duration::ZERO,
    );
    assert_eq!(
        dialed_port_of(
            &run(&h, &certificate_attribute_metadata(&pem))
                .await
                .unwrap()
        ),
        "80"
    );
    let h = handler_with(
        MockApi::with(Some(running_actor()), None),
        Some(loader(&ca)),
        Duration::ZERO,
    );
    want_status(&run(&h, &certificate_attribute_metadata(&pem)).await, 403);
}

#[tokio::test]
async fn allows_certificate_attribute() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let pem = hivekeep_identity::pem::encode(hivekeep_identity::pem::CERTIFICATE, &leaf);
    run(
        &egress_handler(&ca, Some(running_actor()), None),
        &certificate_attribute_metadata(&pem),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn rejects_bad_certificates() {
    let ca = new_ca();
    let other = TestCa::new();
    let now = SystemTime::now();
    let hour = Duration::from_secs(3600);
    let td = "hivekeep-actor.local";
    let c = TestCa::clone_of(&ca.lock());
    let with_uris = |uris: Vec<String>| {
        actor_leaf(&c, |o| {
            o.uri = None;
            o.uris = uris;
        })
    };
    let cases: Vec<(&str, String)> = vec![
        ("no client certificate at all", String::new()),
        (
            "signed by an unknown CA",
            xfcc_header(&[&actor_leaf(&other, |_| {})]),
        ),
        (
            "expired",
            xfcc_header(&[&actor_leaf(&c, |o| {
                o.validity = Some((now - 2 * hour, now - hour))
            })]),
        ),
        (
            "not yet valid",
            xfcc_header(&[&actor_leaf(&c, |o| {
                o.validity = Some((now + hour, now + 2 * hour))
            })]),
        ),
        (
            "no ClientAuth EKU",
            xfcc_header(&[&actor_leaf(&c, |o| {
                o.client = false;
                o.server = true;
            })]),
        ),
        (
            "is a CA certificate",
            xfcc_header(&[&actor_leaf(&c, |o| o.is_ca = true)]),
        ),
        ("no URI SANs", xfcc_header(&[&with_uris(vec![])])),
        (
            "multiple URI SANs",
            xfcc_header(&[&with_uris(vec![
                WORKER_FOR_ACTOR_ID.into(),
                format!("spiffe://{td}/worker-for-actor/default/other-actor"),
            ])]),
        ),
        (
            "non-spiffe URI scheme",
            xfcc_header(&[&with_uris(vec![format!(
                "https://{td}/worker-for-actor/default/my-actor"
            )])]),
        ),
        (
            "empty trust domain",
            xfcc_header(&[&with_uris(vec![
                "spiffe:///worker-for-actor/default/my-actor".into(),
            ])]),
        ),
        (
            "non-worker actor SPIFFE URI",
            xfcc_header(&[&with_uris(vec![format!(
                "spiffe://{td}/actor/default/my-actor"
            )])]),
        ),
        (
            "malformed path (too few segments)",
            xfcc_header(&[&with_uris(vec![format!(
                "spiffe://{td}/worker-for-actor/default"
            )])]),
        ),
        (
            "malformed path (extra segment)",
            xfcc_header(&[&with_uris(vec![format!(
                "spiffe://{td}/worker-for-actor/default/my-actor/extra"
            )])]),
        ),
        (
            "invalid hive resource name",
            xfcc_header(&[&with_uris(vec![format!(
                "spiffe://{td}/worker-for-actor/INVALID_HIVE/my-actor"
            )])]),
        ),
        (
            "invalid actor resource name",
            xfcc_header(&[&with_uris(vec![format!(
                "spiffe://{td}/worker-for-actor/default/INVALID_ACTOR"
            )])]),
        ),
        ("two XFCC elements", {
            let l = actor_leaf(&c, |_| {});
            format!("{},{}", xfcc_header(&[&l]), xfcc_header(&[&l]))
        }),
        (
            "XFCC without a Chain value",
            "By=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-egress;Hash=abc123"
                .into(),
        ),
        (
            "XFCC Chain that is not a certificate",
            format!(
                "Chain=\"{}\"",
                path_escape(
                    "-----BEGIN CERTIFICATE-----\nbm90LWEtY2VydA==\n-----END CERTIFICATE-----\n"
                )
            ),
        ),
    ];
    for (name, xfcc) in cases {
        let h = egress_handler(&ca, Some(running_actor()), None);
        want_status(&run(&h, &egress_metadata(&xfcc)).await, 403);
        let _ = name;
    }
}

#[tokio::test]
async fn authorization() {
    let ca = new_ca();
    let mut suspended = running_actor();
    suspended.status.as_mut().unwrap().state = pb::ActorState::Suspended as i32;
    for (name, actor, err, want) in [
        ("actor is suspended", Some(suspended), None, 403),
        (
            "actor no longer exists",
            None,
            Some(tonic::Status::not_found("no such actor")),
            403,
        ),
        (
            "control plane unreachable",
            None,
            Some(tonic::Status::unavailable("hivekeep-api is down")),
            503,
        ),
    ] {
        let leaf = actor_leaf(&ca.lock(), |_| {});
        let h = egress_handler(&ca, actor, err);
        let r = run(&h, &egress_metadata(&xfcc_header(&[&leaf]))).await;
        assert_eq!(r.err().map(|e| e.http_status()), Some(want), "{name}");
    }
}

#[tokio::test]
async fn actor_state() {
    let ca = new_ca();
    for state in [
        pb::ActorState::Resuming,
        pb::ActorState::Running,
        pb::ActorState::Suspending,
        pb::ActorState::Suspended,
        pb::ActorState::Crashed,
        pb::ActorState::Deleting,
        pb::ActorState::Reverting,
    ] {
        let mut a = running_actor();
        a.status.as_mut().unwrap().state = state as i32;
        let leaf = actor_leaf(&ca.lock(), |_| {});
        let r = run(
            &egress_handler(&ca, Some(a), None),
            &egress_metadata(&xfcc_header(&[&leaf])),
        )
        .await;
        if matches!(state, pb::ActorState::Resuming | pb::ActorState::Running) {
            r.unwrap_or_else(|e| panic!("{state:?}: {e}"));
        } else {
            want_status(&r, 403);
        }
    }
}

#[tokio::test]
async fn without_usable_ca() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let failing: RootsLoader = Arc::new(|| Err("trust bundle contains no certificates".into()));
    for roots in [None, Some(failing)] {
        let h = handler_with(
            MockApi::with(Some(running_actor()), Some(allow_all_policy())),
            roots,
            Duration::ZERO,
        );
        want_status(
            &run(&h, &egress_metadata(&xfcc_header(&[&leaf]))).await,
            503,
        );
    }
}

#[tokio::test]
async fn picks_up_rotated_ca() {
    let ca = new_ca();
    let rotated = TestCa::new();
    let leaf = actor_leaf(&rotated, |_| {});
    let h = egress_handler(&ca, Some(running_actor()), None);
    want_status(
        &run(&h, &egress_metadata(&xfcc_header(&[&leaf]))).await,
        403,
    );
    *ca.lock() = rotated;
    run(&h, &egress_metadata(&xfcc_header(&[&leaf])))
        .await
        .unwrap();
}

#[tokio::test]
async fn rejects_non_address_authority() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let h = egress_handler(&ca, Some(running_actor()), None);
    for authority in ["example.com:443", "93.184.216.34", ""] {
        let mut md = egress_metadata(&xfcc_header(&[&leaf]));
        md.host = authority.into();
        md.headers.insert(":authority".into(), authority.into());
        want_status(&run(&h, &md).await, 403);
    }
}

#[tokio::test]
async fn rejects_non_connect() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    let h = egress_handler(&ca, Some(running_actor()), None);
    let mut md = egress_metadata(&xfcc_header(&[&leaf]));
    md.method = "GET".into();
    md.headers.insert(":method".into(), "GET".into());
    want_status(&run(&h, &md).await, 405);
}

#[test]
fn parse_xfcc_chain_preserves_plus_in_pem() {
    let ca = TestCa::new();
    let leaf = (0..50)
        .map(|_| actor_leaf(&ca, |_| {}))
        .find(|l| {
            hivekeep_identity::pem::encode(hivekeep_identity::pem::CERTIFICATE, l).contains('+')
        })
        .expect("a certificate with '+' in its PEM");
    let chain = super::super::identity::parse_xfcc_chain(&xfcc_header(&[&leaf])).unwrap();
    assert_eq!(chain, vec![leaf]);
}

#[test]
fn parse_xfcc_chain_includes_intermediates() {
    let ca = TestCa::new();
    let leaf = actor_leaf(&ca, |_| {});
    let chain = super::super::identity::parse_xfcc_chain(&xfcc_header(&[&leaf, &ca.der])).unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0], leaf, "leaf first");
}

// ---- Request legs ----

fn test_dialed(leg: &str) -> &'static str {
    if leg == EGRESS_TLS_MITM_FILTER_CHAIN_NAME {
        "93.184.216.34:443"
    } else {
        "93.184.216.34:80"
    }
}

const TEST_SNI: &str = "api.example.com";

fn inner_metadata(
    leg: &str,
    method: &str,
    authority: &str,
    overrides: &[(&str, &str)],
) -> RequestMetadata {
    let mut fields: Vec<(String, String)> = vec![
        (FILTER_CHAIN_NAME_ATTRIBUTE.into(), leg.into()),
        (
            ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE.into(),
            WORKER_FOR_ACTOR_ID.into(),
        ),
        (
            CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE.into(),
            test_dialed(leg).into(),
        ),
    ];
    if leg == EGRESS_TLS_MITM_FILTER_CHAIN_NAME {
        fields.push((REQUESTED_SERVER_NAME_ATTRIBUTE.into(), TEST_SNI.into()));
    }
    for (k, v) in overrides {
        fields.retain(|(fk, _)| fk != k);
        if !v.is_empty() {
            fields.push((k.to_string(), v.to_string()));
        }
    }
    let kv: Vec<(&str, &str)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    RequestMetadata::new(
        &[
            raw(":method", method),
            raw(":authority", authority),
            raw(":path", "/v1/things?secret=1"),
        ],
        attrs(&kv),
    )
}

fn request_metadata(authority: &str) -> RequestMetadata {
    inner_metadata(EGRESS_CLEARTEXT_FILTER_CHAIN_NAME, "GET", authority, &[])
}

fn policy_handler(policy: Option<pb::EgressPolicy>) -> Handler {
    handler_with(
        MockApi::with(Some(running_actor()), policy),
        None,
        Duration::ZERO,
    )
}

fn want_dial(r: &Res) -> &HandlerResult {
    let res = r.as_ref().unwrap_or_else(|e| panic!("denied: {e}"));
    let common = res.response.response.as_ref().unwrap();
    assert!(
        common.clear_route_cache,
        "allowed without clearing the route cache"
    );
    assert!(
        common.header_mutation.is_some(),
        "allowed without a header mutation"
    );
    assert_eq!(
        ns_field(res, EGRESS_METADATA_NAMESPACE, EGRESS_DIAL_KEY),
        EGRESS_DIAL_NAME
    );
    assert!(
        !ns_field(res, EGRESS_METADATA_NAMESPACE, EGRESS_DIAL_HOST_KEY).is_empty(),
        "dial=name without a host"
    );
    res
}

#[tokio::test]
async fn refuses_unknown_filter_chain() {
    want_status(
        &run(
            &policy_handler(Some(allow_all_policy())),
            &inner_metadata("some_other_chain", "GET", "example.com", &[]),
        )
        .await,
        404,
    );
}

#[tokio::test]
async fn request_leg_answers_the_name_to_dial() {
    let h = policy_handler(Some(combined(vec![
        http_policy(&["*"]),
        https_policy(&["*"]),
    ])));
    let (c, m) = (
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
    );
    for (name, leg, authority, want) in [
        ("hostname", c, "api.example.com", "api.example.com"),
        (
            "hostname as matched",
            c,
            "API.Example.com.",
            "api.example.com",
        ),
        (
            "port in the host",
            c,
            "api.example.com:8080",
            "api.example.com",
        ),
        (
            "port in the host on the MITM leg",
            m,
            "api.example.com:8443",
            "api.example.com",
        ),
        (
            "ip literal with a port",
            c,
            "203.0.113.9:8080",
            "203.0.113.9",
        ),
        (
            "ipv4-mapped literal",
            c,
            "[::ffff:203.0.113.9]",
            "203.0.113.9",
        ),
        (
            "ipv6 literal with a port",
            c,
            "[2001:db8::7]:8080",
            "[2001:db8::7]",
        ),
    ] {
        let r = run(&h, &inner_metadata(leg, "GET", authority, &[])).await;
        assert_eq!(
            ns_field(
                want_dial(&r),
                EGRESS_METADATA_NAMESPACE,
                EGRESS_DIAL_HOST_KEY
            ),
            want,
            "{name}"
        );
    }
}

#[tokio::test]
async fn request_leg_decides_host_and_dialed_port() {
    let ca_attr = CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE;
    let cleartext_injection = pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: vec!["api.example.com".into()],
                ports: None,
                effects: Some(sample_effects()),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let cases: Vec<(&str, pb::EgressPolicy, &str, Vec<(&str, &str)>, Option<u16>)> = vec![
        (
            "exact hostname",
            http_policy(&["api.example.com"]),
            "api.example.com",
            vec![],
            None,
        ),
        (
            "hostname case folded",
            http_policy(&["api.example.com"]),
            "API.Example.com",
            vec![],
            None,
        ),
        (
            "hostname with trailing dot",
            http_policy(&["api.example.com"]),
            "api.example.com.",
            vec![],
            None,
        ),
        (
            "wildcard hostname",
            http_policy(&["*.example.com"]),
            "api.example.com",
            vec![],
            None,
        ),
        (
            "star hostname",
            http_policy(&["*"]),
            "anything.example",
            vec![],
            None,
        ),
        (
            "star matches an ip literal",
            http_policy(&["*"]),
            "93.184.216.34",
            vec![],
            None,
        ),
        (
            "star matches an ip literal with a port",
            http_policy_on(ports(&[8080]), &["*"]),
            "203.0.113.9:8080",
            vec![(ca_attr, "203.0.113.9:8080")],
            None,
        ),
        (
            "star matches an ipv6 literal",
            http_policy(&["*"]),
            "[2001:db8::7]",
            vec![],
            None,
        ),
        (
            "allow-all policy",
            allow_all_policy(),
            "anything.example",
            vec![],
            None,
        ),
        (
            "host port is ignored, dialed port matches",
            http_policy(&["api.example.com"]),
            "api.example.com:8443",
            vec![],
            None,
        ),
        (
            "host port is ignored, dialed port does not match",
            http_policy(&["api.example.com"]),
            "api.example.com:80",
            vec![(ca_attr, "93.184.216.34:8080")],
            Some(403),
        ),
        (
            "dialed port in the http rule",
            http_policy_on(ports(&[8080]), &["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "93.184.216.34:8080")],
            None,
        ),
        (
            "dialed port outside the http rule",
            http_policy(&["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "93.184.216.34:8080")],
            Some(403),
        ),
        (
            "any port in the http rule",
            http_policy_on(all_ports(), &["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "93.184.216.34:8080")],
            None,
        ),
        (
            "no CONNECT authority leaves ports unenforced",
            http_policy_on(ports(&[8080]), &["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "")],
            None,
        ),
        (
            "unparseable CONNECT authority",
            http_policy(&["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "not an address:80")],
            Some(403),
        ),
        (
            "name in the CONNECT authority",
            http_policy(&["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "example.com:80")],
            Some(403),
        ),
        (
            "CONNECT authority without a port",
            http_policy(&["api.example.com"]),
            "api.example.com",
            vec![(ca_attr, "93.184.216.34")],
            Some(403),
        ),
        (
            "other hostname",
            http_policy(&["api.example.com"]),
            "evil.example",
            vec![],
            Some(403),
        ),
        (
            "wildcard does not match the apex",
            http_policy(&["*.example.com"]),
            "example.com",
            vec![],
            Some(403),
        ),
        (
            "wildcard does not match two labels",
            http_policy(&["*.example.com"]),
            "a.b.example.com",
            vec![],
            Some(403),
        ),
        (
            "ip literal host with a named http rule",
            http_policy(&["api.example.com"]),
            "93.184.216.34",
            vec![],
            Some(403),
        ),
        (
            "https rule does not decide the cleartext leg",
            https_policy(&["api.example.com"]),
            "api.example.com",
            vec![],
            Some(403),
        ),
        (
            "passthrough rule does not decide the cleartext leg",
            passthrough_policy(all_ports(), &["*"]),
            "api.example.com",
            vec![],
            Some(403),
        ),
        (
            "unparseable host",
            allow_all_policy(),
            "exa mple.com",
            vec![],
            Some(403),
        ),
        ("empty host", allow_all_policy(), "", vec![], Some(403)),
        (
            "cleartext injection rule without the header passes through",
            cleartext_injection,
            "api.example.com",
            vec![],
            None,
        ),
    ];
    for (name, policy, authority, over, want) in cases {
        let r = run(
            &policy_handler(Some(policy)),
            &inner_metadata(EGRESS_CLEARTEXT_FILTER_CHAIN_NAME, "GET", authority, &over),
        )
        .await;
        match want {
            None => {
                want_dial(&r);
            }
            Some(code) => assert_eq!(r.err().map(|e| e.http_status()), Some(code), "{name}"),
        }
    }
}

#[tokio::test]
async fn mitm_leg_decides_sni_then_authority() {
    let two = || {
        combined(vec![
            https_policy(&["api.example.com"]),
            https_policy(&["cdn.example.com"]),
        ])
    };
    let cases: Vec<(&str, pb::EgressPolicy, &str, &str, bool)> = vec![
        (
            "sni and authority under the rule",
            https_policy(&["api.example.com"]),
            "api.example.com",
            "api.example.com",
            true,
        ),
        (
            "authority under another https rule",
            two(),
            "api.example.com",
            "cdn.example.com",
            true,
        ),
        (
            "star sni",
            https_policy(&["*"]),
            "anything.example",
            "anything.example",
            true,
        ),
        (
            "sni under no rule",
            https_policy(&["api.example.com"]),
            "evil.example",
            "api.example.com",
            false,
        ),
        (
            "authority under no rule",
            https_policy(&["api.example.com"]),
            "api.example.com",
            "evil.example",
            false,
        ),
        ("no sni", https_policy(&["*"]), "", "api.example.com", false),
        (
            "sni under an http rule only",
            combined(vec![
                http_policy(&["api.example.com"]),
                https_policy(&["cdn.example.com"]),
            ]),
            "api.example.com",
            "cdn.example.com",
            false,
        ),
        (
            "passthrough rule does not decide",
            passthrough_policy(ports(&[443]), &["*"]),
            "api.example.com",
            "api.example.com",
            false,
        ),
    ];
    for (name, policy, sni, authority, allowed) in cases {
        let md = inner_metadata(
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            "GET",
            authority,
            &[(REQUESTED_SERVER_NAME_ATTRIBUTE, sni)],
        );
        let r = run(&policy_handler(Some(policy)), &md).await;
        if allowed {
            want_dial(&r);
        } else {
            assert_eq!(r.err().map(|e| e.http_status()), Some(403), "{name}");
        }
    }
}

#[tokio::test]
async fn request_leg_serves_both_decrypted_chains() {
    let h = policy_handler(Some(combined(vec![
        http_policy(&["api.example.com"]),
        https_policy(&["api.example.com"]),
    ])));
    for leg in [
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
    ] {
        want_dial(&run(&h, &inner_metadata(leg, "POST", "api.example.com", &[])).await);
        want_status(
            &run(&h, &inner_metadata(leg, "POST", "other.example", &[])).await,
            403,
        );
    }
}

#[tokio::test]
async fn request_leg_picks_rules_by_leg() {
    let (c, m) = (
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
    );
    for (policy, leg, allowed) in [
        (http_policy(&["api.example.com"]), c, true),
        (http_policy(&["api.example.com"]), m, false),
        (https_policy(&["api.example.com"]), m, true),
        (https_policy(&["api.example.com"]), c, false),
    ] {
        let r = run(
            &policy_handler(Some(policy)),
            &inner_metadata(leg, "GET", "api.example.com", &[]),
        )
        .await;
        if allowed {
            want_dial(&r);
        } else {
            want_status(&r, 403);
        }
    }
}

#[tokio::test]
async fn request_leg_denies_http1_websocket_upgrade() {
    let h = policy_handler(Some(allow_all_policy()));
    for leg in [
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
    ] {
        let mut md = inner_metadata(leg, "GET", "api.example.com", &[]);
        md.set("connection", "keep-alive, UpGrAdE");
        md.set("upgrade", "WebSocket");
        let r = run(&h, &md).await;
        want_status(&r, 403);
        assert_eq!(r.unwrap_err().to_string(), WEBSOCKET_DENIED_BODY);
    }
}

#[tokio::test]
async fn request_leg_denies_connect() {
    for policy in [Some(allow_all_policy()), None] {
        for leg in [
            EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        ] {
            for method in ["CONNECT", "connect"] {
                let r = run(
                    &policy_handler(policy.clone()),
                    &inner_metadata(leg, method, "api.example.com:443", &[]),
                )
                .await;
                want_status(&r, 403);
                assert_eq!(r.unwrap_err().to_string(), CONNECT_DENIED_BODY);
            }
        }
    }
}

#[tokio::test]
async fn request_leg_allows_ordinary_https_request() {
    let h = policy_handler(Some(allow_all_policy()));
    for (name, method, connection, upgrade) in [
        ("ordinary GET", "GET", "", ""),
        ("other upgrade", "GET", "Upgrade", "h2c"),
        (
            "upgrade without connection token",
            "GET",
            "keep-alive",
            "websocket",
        ),
        ("non-GET", "POST", "Upgrade", "websocket"),
    ] {
        let mut md = inner_metadata(
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            method,
            "api.example.com",
            &[],
        );
        md.set("connection", connection);
        md.set("upgrade", upgrade);
        let r = run(&h, &md).await;
        assert!(r.is_ok(), "{name}");
        want_dial(&r);
    }
}

#[tokio::test]
async fn request_leg_requires_identity() {
    let h = policy_handler(Some(allow_all_policy()));
    for id in [
        "",
        "api.example.com",
        "spiffe://cluster.local/ns/default/sa/thing",
        "spiffe://hivekeep-actor.local/hive/default",
    ] {
        let md = inner_metadata(
            EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
            "GET",
            "api.example.com",
            &[(ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE, id)],
        );
        want_status(&run(&h, &md).await, 403);
    }
}

#[tokio::test]
async fn request_leg_policy_lookup() {
    let empty = pb::EgressPolicy::default();
    for (name, policy, err, want) in [
        ("no policy", None, None, 403),
        ("policy with no rules", Some(empty), None, 403),
        (
            "control plane unavailable",
            None,
            Some(tonic::Status::unavailable("down")),
            503,
        ),
        (
            "control plane refuses",
            None,
            Some(tonic::Status::permission_denied("no")),
            503,
        ),
    ] {
        let api = MockApi::with(None, policy);
        *api.policy_err.lock() = err;
        let r = run(
            &handler_with(api, None, Duration::ZERO),
            &request_metadata("api.example.com"),
        )
        .await;
        assert_eq!(r.err().map(|e| e.http_status()), Some(want), "{name}");
    }
}

#[tokio::test]
async fn connect_leg_requires_a_policy() {
    let ca = new_ca();
    let leaf = actor_leaf(&ca.lock(), |_| {});
    for (name, policy, err, want) in [
        ("policy present", Some(allow_all_policy()), None, None),
        (
            "http-only policy still opens the tunnel",
            Some(http_policy(&["api.example.com"])),
            None,
            None,
        ),
        ("no policy", None, None, Some(403)),
        (
            "policy with no rules",
            Some(pb::EgressPolicy::default()),
            None,
            Some(403),
        ),
        (
            "control plane unavailable",
            None,
            Some(tonic::Status::unavailable("down")),
            Some(503),
        ),
    ] {
        let api = MockApi::with(Some(running_actor()), policy);
        *api.policy_err.lock() = err;
        let h = handler_with(
            api.clone(),
            Some(loader(&ca)),
            super::super::DEFAULT_POLICY_CACHE_TTL,
        );
        let r = run(&h, &egress_metadata(&xfcc_header(&[&leaf]))).await;
        match want {
            None => {
                r.unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(
                    api.policy_calls.load(std::sync::atomic::Ordering::SeqCst),
                    1
                );
                run(&h, &request_metadata("api.example.com")).await.unwrap();
                assert_eq!(
                    api.policy_calls.load(std::sync::atomic::Ordering::SeqCst),
                    1,
                    "{name}: the request leg reuses the CONNECT's lookup"
                );
            }
            Some(code) => assert_eq!(r.err().map(|e| e.http_status()), Some(code), "{name}"),
        }
    }
}

#[tokio::test]
async fn request_leg_refuses_authority_host_mismatch() {
    let h = policy_handler(Some(http_policy(&["api.example.com"])));
    let mut md = request_metadata("api.example.com");
    md.set("host", "evil.example");
    want_status(&run(&h, &md).await, 403);
    let mut md = request_metadata("api.example.com");
    md.set("host", "API.example.com.");
    run(&h, &md).await.unwrap();
}

#[tokio::test]
async fn denial_body_is_uniform() {
    let h = policy_handler(Some(http_policy(&["api.example.com"])));
    for md in [
        request_metadata("evil.example"),
        request_metadata("exa mple.com"),
        inner_metadata(
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            "GET",
            "evil.example",
            &[],
        ),
    ] {
        assert_eq!(run(&h, &md).await.unwrap_err().to_string(), DENIED_BODY);
    }
}

#[tokio::test]
async fn canceled_caller_is_not_a_policy_failure() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let api = Arc::new(MockApi {
        policy: Mutex::new(Some(allow_all_policy())),
        policy_gate: Some(gate.clone()),
        ..Default::default()
    });
    let h = Arc::new(handler_with(api.clone(), None, Duration::ZERO));
    let cancel = CancellationToken::new();
    let (h2, c2) = (h.clone(), cancel.clone());
    let done = tokio::spawn(async move { h2.handle(&request_metadata("example.com"), &c2).await });
    while api.policy_calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    cancel.cancel();
    let r = done.await.unwrap();
    gate.notify_waiters();
    want_status(&r, 408);
}

// ---- Credential injection ----

const PROVIDER: &str = "k8s";

fn sample_effects() -> pb::HttpRuleEffects {
    pb::HttpRuleEffects {
        replace_headers: vec![pb::CredentialHeader {
            header: "authorization".into(),
            prefix: "Bearer ".into(),
            credential_uri: "hk-secret://k8s/default/token".into(),
            actor_jwt: None,
        }],
    }
}

fn effects_policy(leg: &str, effects: pb::HttpRuleEffects) -> pb::EgressPolicy {
    let hosts = vec!["api.example.com".to_string()];
    let rule = if leg == EGRESS_CLEARTEXT_FILTER_CHAIN_NAME {
        pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: hosts,
                ports: None,
                effects: Some(effects),
            }),
            ..Default::default()
        }
    } else {
        pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: hosts,
                ports: None,
                effects: Some(effects),
            }),
            ..Default::default()
        }
    };
    pb::EgressPolicy {
        rules: vec![rule],
        ..Default::default()
    }
}

#[derive(Default)]
struct FakeProvider {
    resp: Option<Vec<u8>>,
    err: Option<tonic::Status>,
    hang: bool,
    got: Mutex<Option<crate::credprovider::v1::FetchSecretRequest>>,
}

impl CredentialProvider for Arc<FakeProvider> {
    fn fetch_secret(
        &self,
        req: crate::credprovider::v1::FetchSecretRequest,
    ) -> futures::future::BoxFuture<
        'static,
        Result<crate::credprovider::v1::FetchSecretResponse, tonic::Status>,
    > {
        let this = self.clone();
        Box::pin(async move {
            *this.got.lock() = Some(req);
            if this.hang {
                std::future::pending::<()>().await;
            }
            match (&this.err, &this.resp) {
                (Some(e), _) => Err(e.clone()),
                (None, r) => Ok(crate::credprovider::v1::FetchSecretResponse {
                    opaque_bytes: r.clone().unwrap_or_default(),
                }),
            }
        })
    }
}

fn injection_handler(
    policy: pb::EgressPolicy,
    provider: Option<Arc<FakeProvider>>,
    name: &str,
) -> Handler {
    Handler::new(
        Arc::new(Api(MockApi::with(Some(running_actor()), Some(policy)))),
        None,
        Duration::ZERO,
        provider.map(|p| Arc::new(p) as Arc<dyn CredentialProvider>),
        name,
    )
}

fn with_authorization(mut md: RequestMetadata) -> RequestMetadata {
    md.set("authorization", "Bearer placeholder");
    md
}

fn set_headers(r: &Res) -> Vec<HeaderValueOption> {
    r.as_ref()
        .unwrap()
        .response
        .response
        .as_ref()
        .unwrap()
        .header_mutation
        .as_ref()
        .unwrap()
        .set_headers
        .clone()
}

#[tokio::test]
async fn injection_on_request_legs() {
    for leg in [
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ] {
        let p = Arc::new(FakeProvider {
            resp: Some(b"s3cr3t\n".to_vec()),
            ..Default::default()
        });
        let h = injection_handler(
            effects_policy(leg, sample_effects()),
            Some(p.clone()),
            PROVIDER,
        );
        let r = run(
            &h,
            &with_authorization(inner_metadata(leg, "GET", "api.example.com", &[])),
        )
        .await;
        let sh = set_headers(&r);
        assert_eq!(sh.len(), 1, "{leg}");
        let hd = sh[0].header.as_ref().unwrap();
        assert_eq!(hd.key, "authorization");
        assert_eq!(hd.raw_value, b"Bearer s3cr3t");
        assert_eq!(
            sh[0].append_action,
            HeaderAppendAction::OverwriteIfExistsOrAdd as i32
        );
        let got = p.got.lock().clone().unwrap();
        assert_eq!(got.uri, "hk-secret://k8s/default/token");
        assert_eq!(
            got.actor_spiffe_id,
            "spiffe://hivekeep-actor.local/actor/default/my-actor"
        );
    }
}

#[tokio::test]
async fn injection_skipped_without_header() {
    let jwt = pb::HttpRuleEffects {
        replace_headers: vec![pb::CredentialHeader {
            header: "authorization".into(),
            actor_jwt: Some(pb::ActorJwtSource {
                audiences: vec!["https://api.example.com".into()],
                expiration_seconds: 0,
            }),
            ..Default::default()
        }],
    };
    let leg = EGRESS_TLS_MITM_FILTER_CHAIN_NAME;
    for (name, effects, provider) in [
        (
            "provider configured",
            sample_effects(),
            Some(Arc::new(FakeProvider {
                resp: Some(b"s3cr3t".to_vec()),
                ..Default::default()
            })),
        ),
        ("no provider configured", sample_effects(), None),
        (
            "actor JWT",
            jwt,
            Some(Arc::new(FakeProvider {
                resp: Some(b"s3cr3t".to_vec()),
                ..Default::default()
            })),
        ),
    ] {
        let h = injection_handler(effects_policy(leg, effects), provider.clone(), PROVIDER);
        let r = run(&h, &inner_metadata(leg, "GET", "api.example.com", &[])).await;
        assert!(set_headers(&r).is_empty(), "{name}");
        if let Some(p) = provider {
            assert!(p.got.lock().is_none(), "{name}: provider asked");
        }
    }
}

#[tokio::test]
async fn injection_replaces_only_carried_headers() {
    let effects = pb::HttpRuleEffects {
        replace_headers: vec![
            pb::CredentialHeader {
                header: "authorization".into(),
                prefix: "Bearer ".into(),
                credential_uri: "hk-secret://k8s/default/token".into(),
                actor_jwt: None,
            },
            pb::CredentialHeader {
                header: "X-Api-Key".into(),
                credential_uri: "hk-secret://k8s/default/api-key".into(),
                ..Default::default()
            },
        ],
    };
    let p = Arc::new(FakeProvider {
        resp: Some(b"s3cr3t".to_vec()),
        ..Default::default()
    });
    let h = injection_handler(
        effects_policy(EGRESS_TLS_MITM_FILTER_CHAIN_NAME, effects),
        Some(p.clone()),
        PROVIDER,
    );
    let mut md = inner_metadata(
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        "GET",
        "api.example.com",
        &[],
    );
    md.set("x-api-key", "placeholder");
    let sh = set_headers(&run(&h, &md).await);
    assert_eq!(sh.len(), 1);
    assert_eq!(sh[0].header.as_ref().unwrap().key, "X-Api-Key");
    assert_eq!(
        p.got.lock().clone().unwrap().uri,
        "hk-secret://k8s/default/api-key"
    );
}

#[tokio::test]
async fn injection_denials() {
    for leg in [
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ] {
        let cases: Vec<(&str, Option<Arc<FakeProvider>>, &str, u16)> = vec![
            ("no provider configured is refused", None, PROVIDER, 500),
            (
                "credential URI for another provider is refused",
                Some(Arc::new(FakeProvider {
                    resp: Some(b"s3cr3t".to_vec()),
                    ..Default::default()
                })),
                "vault",
                500,
            ),
            (
                "provider unavailable fails closed as retryable",
                Some(Arc::new(FakeProvider {
                    err: Some(tonic::Status::unavailable("provider down")),
                    ..Default::default()
                })),
                PROVIDER,
                503,
            ),
            (
                "secret not found denies as non-retryable",
                Some(Arc::new(FakeProvider {
                    err: Some(tonic::Status::not_found("no such secret")),
                    ..Default::default()
                })),
                PROVIDER,
                403,
            ),
            (
                "provider refuses the actor denies as non-retryable",
                Some(Arc::new(FakeProvider {
                    err: Some(tonic::Status::permission_denied("hive not allowed")),
                    ..Default::default()
                })),
                PROVIDER,
                403,
            ),
            (
                "unexpected provider error denies",
                Some(Arc::new(FakeProvider {
                    err: Some(tonic::Status::unknown("provider down")),
                    ..Default::default()
                })),
                PROVIDER,
                403,
            ),
            (
                "empty secret fails closed",
                Some(Arc::new(FakeProvider {
                    resp: Some(vec![]),
                    ..Default::default()
                })),
                PROVIDER,
                503,
            ),
        ];
        for (name, provider, pname, want) in cases {
            let h = injection_handler(effects_policy(leg, sample_effects()), provider, pname);
            let r = run(
                &h,
                &with_authorization(inner_metadata(leg, "GET", "api.example.com", &[])),
            )
            .await;
            assert_eq!(r.err().map(|e| e.http_status()), Some(want), "{leg}/{name}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn injection_provider_hangs() {
    let p = Arc::new(FakeProvider {
        hang: true,
        ..Default::default()
    });
    let h = injection_handler(
        effects_policy(EGRESS_TLS_MITM_FILTER_CHAIN_NAME, sample_effects()),
        Some(p),
        PROVIDER,
    );
    let start = tokio::time::Instant::now();
    let r = run(
        &h,
        &with_authorization(inner_metadata(
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            "GET",
            "api.example.com",
            &[],
        )),
    )
    .await;
    want_status(&r, 503);
    assert!(start.elapsed() < Duration::from_secs(5));
}

fn jwt_header(header: &str) -> pb::CredentialHeader {
    pb::CredentialHeader {
        header: header.into(),
        prefix: "Bearer ".into(),
        actor_jwt: Some(pb::ActorJwtSource {
            audiences: vec!["https://api.example.com".into()],
            expiration_seconds: 900,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn actor_jwt_injection() {
    for leg in [
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ] {
        let effects = pb::HttpRuleEffects {
            replace_headers: vec![
                jwt_header("authorization"),
                pb::CredentialHeader {
                    header: "x-api-key".into(),
                    credential_uri: "hk-secret://k8s/default/token".into(),
                    ..Default::default()
                },
            ],
        };
        let api = MockApi::with(Some(running_actor()), Some(effects_policy(leg, effects)));
        let h = handler_with(api.clone(), None, Duration::ZERO);
        let sh = set_headers(
            &run(
                &h,
                &with_authorization(inner_metadata(leg, "GET", "api.example.com", &[])),
            )
            .await,
        );
        assert_eq!(sh.len(), 1);
        assert_eq!(sh[0].header.as_ref().unwrap().key, "authorization");
        assert_eq!(sh[0].header.as_ref().unwrap().raw_value, b"Bearer jwt-1");
        let want = pb::MintActorJwtRequest {
            actor: Some(pb::ObjectRef {
                hive: HIVE.into(),
                name: ACTOR.into(),
            }),
            audiences: vec!["https://api.example.com".into()],
            expiration_seconds: 900,
        };
        assert_eq!(api.last_mint.lock().clone().unwrap(), want);
    }
}

#[tokio::test]
async fn actor_jwt_injection_skipped_without_header() {
    let api = MockApi::with(
        Some(running_actor()),
        Some(effects_policy(
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            pb::HttpRuleEffects {
                replace_headers: vec![jwt_header("authorization")],
            },
        )),
    );
    let h = handler_with(api.clone(), None, Duration::ZERO);
    assert!(set_headers(
        &run(
            &h,
            &inner_metadata(
                EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
                "GET",
                "api.example.com",
                &[]
            )
        )
        .await
    )
    .is_empty());
    assert_eq!(api.mint_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actor_jwt_injection_denials() {
    for leg in [
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    ] {
        for (name, header, err, want) in [
            (
                "actor deleted",
                "authorization",
                Some(tonic::Status::not_found("actor not found")),
                403,
            ),
            (
                "hivekeep-api unavailable",
                "authorization",
                Some(tonic::Status::unavailable("down")),
                503,
            ),
            (
                "mint timed out",
                "authorization",
                Some(tonic::Status::deadline_exceeded("deadline exceeded")),
                503,
            ),
            (
                "gateway may not mint",
                "authorization",
                Some(tonic::Status::permission_denied("denied")),
                500,
            ),
            (
                "mint request rejected",
                "authorization",
                Some(tonic::Status::invalid_argument("bad lifetime")),
                500,
            ),
            ("system header", ":path", None, 500),
        ] {
            let api = MockApi::with(
                Some(running_actor()),
                Some(effects_policy(
                    leg,
                    pb::HttpRuleEffects {
                        replace_headers: vec![jwt_header(header)],
                    },
                )),
            );
            *api.mint_err.lock() = err;
            let h = handler_with(api, None, Duration::ZERO);
            let mut md = inner_metadata(leg, "GET", "api.example.com", &[]);
            md.set(header, "placeholder");
            assert_eq!(
                run(&h, &md).await.err().map(|e| e.http_status()),
                Some(want),
                "{leg}/{name}"
            );
        }
    }
}

#[tokio::test]
async fn process_returns_websocket_denial_as_immediate_response() {
    use envoy_types::pb::envoy::config::core::v3::HeaderMap;
    use envoy_types::pb::envoy::service::ext_proc::v3::{
        processing_request, processing_response, HttpHeaders, ProcessingRequest,
    };
    for leg in [
        EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
    ] {
        let req = ProcessingRequest {
            request: Some(processing_request::Request::RequestHeaders(HttpHeaders {
                headers: Some(HeaderMap {
                    headers: vec![
                        raw(":method", "GET"),
                        raw(":authority", "api.example.com"),
                        raw("connection", "keep-alive, Upgrade"),
                        raw("upgrade", "websocket"),
                    ],
                }),
                ..Default::default()
            })),
            attributes: attrs(&[(FILTER_CHAIN_NAME_ATTRIBUTE, leg)]),
            ..Default::default()
        };
        let h: Arc<dyn ExtprocHandler> = Arc::new(handler_with(
            MockApi::with(None, None),
            None,
            Duration::ZERO,
        ));
        let server = crate::router::extproc::Server::new(None, [(Direction::Egress, h)].into());
        let Some(processing_request::Request::RequestHeaders(hh)) = &req.request else {
            unreachable!()
        };
        let resp = server.process_request_headers(&req, hh).await;
        let Some(processing_response::Response::ImmediateResponse(ir)) = resp.response else {
            panic!("want an immediate response")
        };
        assert_eq!(ir.status.unwrap().code, 403);
        assert_eq!(String::from_utf8(ir.body).unwrap(), WEBSOCKET_DENIED_BODY);
    }
}
