//! Actor EgressPolicy request validation and the rule set of a policy.

mod common;

use common::*;
use hivekeep_core::defaults;
use hivekeep_core::field::{ErrorList, Path};
use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

const HIVE: &str = "test-hive";

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn oref(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

fn secret_header(header: &str, uri: &str) -> pb::CredentialHeader {
    pb::CredentialHeader {
        header: header.into(),
        credential_uri: uri.into(),
        ..Default::default()
    }
}

fn valid_policy() -> pb::EgressPolicy {
    pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: HIVE.into(),
            name: "default".into(),
            ..Default::default()
        }),
        rules: vec![pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: vec!["api.example.com".into()],
                effects: Some(pb::HttpRuleEffects {
                    replace_headers: vec![pb::CredentialHeader {
                        prefix: "Bearer ".into(),
                        ..secret_header(
                            "Authorization",
                            "hk-secret://kubernetes.io/provider/ns/name",
                        )
                    }],
                }),
                ports: None,
            }),
            ..Default::default()
        }],
    }
}

/// An Authorization header carrying a 900-second Actor JWT.
fn actor_jwt_header(audiences: &[&str]) -> pb::CredentialHeader {
    pb::CredentialHeader {
        header: "Authorization".into(),
        prefix: "Bearer ".into(),
        actor_jwt: Some(pb::ActorJwtSource {
            audiences: audiences.iter().map(|s| s.to_string()).collect(),
            expiration_seconds: 900,
        }),
        ..Default::default()
    }
}

fn create(actor: Option<pb::ObjectRef>, egress_policy: Option<pb::EgressPolicy>) -> ErrorList {
    validate_create_actor_egress_policy_request(
        &Operation::create(),
        &pb::CreateActorEgressPolicyRequest {
            actor,
            egress_policy,
        },
    )
}

fn pol(f: impl FnOnce(&mut pb::EgressPolicy)) -> Option<pb::EgressPolicy> {
    Some(with(valid_policy(), f))
}

fn md(p: &mut pb::EgressPolicy) -> &mut pb::ResourceMetadata {
    p.metadata.as_mut().unwrap()
}

#[test]
fn create_actor_egress_policy_request() {
    let actor = || oref(HIVE, "actor");
    let p = Some(valid_policy());
    run(vec![
        ("valid", create(actor(), p.clone()), vec![]),
        (
            "missing actor",
            create(None, p.clone()),
            vec![required("actor")],
        ),
        (
            "missing actor hive",
            create(oref("", "actor"), p.clone()),
            vec![required("actor.hive")],
        ),
        (
            "invalid actor hive",
            create(oref("invalid value", "actor"), p.clone()),
            vec![
                dns_label("actor.hive"),
                invalid("egress_policy.metadata.hive"),
            ],
        ),
        (
            "missing actor name",
            create(oref(HIVE, ""), p.clone()),
            vec![required("actor.name")],
        ),
        (
            "invalid actor name",
            create(oref(HIVE, "invalid value"), p.clone()),
            vec![dns_label("actor.name")],
        ),
        (
            "missing policy",
            create(actor(), None),
            vec![required("egress_policy")],
        ),
        (
            "missing metadata",
            create(actor(), pol(|p| p.metadata = None)),
            vec![required("egress_policy.metadata")],
        ),
        (
            "missing policy hive",
            create(actor(), pol(|p| md(p).hive.clear())),
            vec![required("egress_policy.metadata.hive")],
        ),
        (
            "missing default name",
            create(actor(), pol(|p| md(p).name.clear())),
            vec![required("egress_policy.metadata.name")],
        ),
        (
            "wrong policy name",
            create(actor(), pol(|p| md(p).name = "other".into())),
            vec![invalid_by("egress_policy.metadata.name", "custom=default")],
        ),
        (
            "mismatched policy hive",
            create(actor(), pol(|p| md(p).hive = "other".into())),
            vec![invalid("egress_policy.metadata.hive")],
        ),
    ]);
}

/// The cases shared by Get and Delete, which name the policy by its Actor
/// and the singleton name.
fn named_cases(f: impl Fn(Option<pb::ObjectRef>, &str) -> ErrorList) {
    run(vec![
        ("valid", f(oref(HIVE, "actor"), SINGLETON_NAME), vec![]),
        (
            "missing actor",
            f(None, SINGLETON_NAME),
            vec![required("actor")],
        ),
        (
            "missing hive",
            f(oref("", "actor"), SINGLETON_NAME),
            vec![required("actor.hive")],
        ),
        (
            "invalid hive",
            f(oref("invalid value", "actor"), SINGLETON_NAME),
            vec![dns_label("actor.hive")],
        ),
        (
            "missing name",
            f(oref(HIVE, ""), SINGLETON_NAME),
            vec![required("actor.name")],
        ),
        (
            "invalid name",
            f(oref(HIVE, "invalid value"), SINGLETON_NAME),
            vec![dns_label("actor.name")],
        ),
        (
            "missing singleton name",
            f(oref(HIVE, "actor"), ""),
            vec![required("name")],
        ),
        (
            "wrong singleton name",
            f(oref(HIVE, "actor"), "other"),
            vec![invalid_by("name", "custom=default")],
        ),
    ]);
}

#[test]
fn get_actor_egress_policy_request() {
    named_cases(|actor, name| {
        validate_get_actor_egress_policy_request(
            &Operation::create(),
            &pb::GetActorEgressPolicyRequest {
                actor,
                name: name.into(),
            },
        )
    });
}

#[test]
fn update_actor_egress_policy_request() {
    let u = |actor: Option<pb::ObjectRef>, egress_policy: Option<pb::EgressPolicy>| {
        validate_update_actor_egress_policy_request(
            &Operation::create(),
            &pb::UpdateActorEgressPolicyRequest {
                actor,
                egress_policy,
            },
        )
    };
    let actor = || oref(HIVE, "actor");
    run(vec![
        ("valid", u(actor(), Some(valid_policy())), vec![]),
        (
            "missing actor",
            u(None, Some(valid_policy())),
            vec![required("actor")],
        ),
        (
            "missing policy",
            u(actor(), None),
            vec![required("egress_policy")],
        ),
        (
            "mismatched hive",
            u(actor(), pol(|p| md(p).hive = "other".into())),
            vec![invalid("egress_policy.metadata.hive")],
        ),
        (
            "invalid rule",
            u(
                actor(),
                pol(|p| p.rules[0].http.as_mut().unwrap().hostnames.clear()),
            ),
            vec![required("egress_policy.rules[0].http.hostnames")],
        ),
    ]);
}

#[test]
fn delete_actor_egress_policy_request() {
    named_cases(|actor, name| {
        validate_delete_actor_egress_policy_request(
            &Operation::create(),
            &pb::DeleteActorEgressPolicyRequest {
                actor,
                name: name.into(),
                options: None,
            },
        )
    });
}

fn http(p: &mut pb::EgressPolicy) -> &mut pb::HttpRule {
    p.rules[0].http.as_mut().unwrap()
}

fn headers(p: &mut pb::EgressPolicy) -> &mut Vec<pb::CredentialHeader> {
    &mut http(p).effects.as_mut().unwrap().replace_headers
}

fn without_effects(p: &mut pb::EgressPolicy) {
    http(p).effects = None;
}

fn trivial_http(host: &str) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: vec![host.into()],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn https_rule(ports: Option<pb::Ports>, hosts: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        https: Some(pb::HttpsRule {
            hostnames: hosts.iter().map(|s| s.to_string()).collect(),
            ports,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn passthrough(ports: Option<pb::Ports>, hosts: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        tls_passthrough: Some(pb::TlsPassthroughRule {
            hostnames: hosts.iter().map(|s| s.to_string()).collect(),
            ports,
        }),
        ..Default::default()
    }
}

fn numbers(n: &[i32]) -> Option<pb::Ports> {
    Some(pb::Ports {
        all: None,
        numbers: n.to_vec(),
    })
}

fn all_ports() -> Option<pb::Ports> {
    Some(pb::Ports {
        all: Some(pb::AllPorts {}),
        numbers: vec![],
    })
}

/// Defaults the policy, as the server does, and validates it in a create.
fn rules_check(f: &dyn Fn(&mut pb::EgressPolicy)) -> ErrorList {
    let mut p = valid_policy();
    f(&mut p);
    defaults::apply(&mut p);
    create(oref(HIVE, "actor"), Some(p))
}

fn jwt(f: &dyn Fn(&mut pb::ActorJwtSource)) -> pb::CredentialHeader {
    let mut h = actor_jwt_header(&["a"]);
    f(h.actor_jwt.as_mut().unwrap());
    h
}

#[test]
fn egress_policy_rules() {
    let r = "egress_policy.rules";
    let h = "egress_policy.rules[0].http";
    let pat = "egress_policy.rules[0].http.hostnames[0]";
    let ports = "egress_policy.rules[0].http.ports";
    let sh = "egress_policy.rules[0].http.effects.replace_headers[0]";
    let at = |s: &str| leak(format!("{sh}.{s}"));
    let hosts =
        |n: usize| -> Vec<String> { (0..n).map(|i| format!("api{i}.example.com")).collect() };
    let creds = |n: usize| -> Vec<pb::CredentialHeader> {
        (0..n)
            .map(|i| {
                secret_header(
                    &format!("X-Header-{i}"),
                    "hk-secret://example.com/provider/secret",
                )
            })
            .collect()
    };
    run(vec![
        ("valid", rules_check(&|_| {}), vec![]),
        ("no rules", rules_check(&|p| p.rules.clear()), vec![]),
        (
            "many rules",
            rules_check(&|p| p.rules = hosts(256).iter().map(|h| trivial_http(h)).collect()),
            vec![],
        ),
        (
            "too many rules",
            rules_check(&|p| p.rules = hosts(257).iter().map(|h| trivial_http(h)).collect()),
            vec![too_many(r)],
        ),
        (
            "no protocol",
            rules_check(&|p| p.rules[0] = pb::EgressRule::default()),
            vec![invalid_by("egress_policy.rules[0]", "union")],
        ),
        (
            "two protocols",
            rules_check(&|p| p.rules[0].https = https_rule(None, &["api.example.com"]).https),
            vec![invalid_by("egress_policy.rules[0]", "union")],
        ),
        (
            "https rule",
            rules_check(&|p| {
                p.rules[0] =
                    https_rule(numbers(&[443, 8443]), &["api.example.com", "*.example.org"])
            }),
            vec![],
        ),
        (
            "tls passthrough rule",
            rules_check(&|p| p.rules[0] = passthrough(numbers(&[443]), &["api.example.com", "*"])),
            vec![],
        ),
        (
            "tls passthrough rule without ports",
            rules_check(&|p| p.rules[0] = passthrough(None, &["api.example.com"])),
            vec![required("egress_policy.rules[0].tls_passthrough.ports")],
        ),
        (
            "empty pattern list",
            rules_check(&|p| {
                http(p).hostnames.clear();
                without_effects(p);
            }),
            vec![required(leak(format!("{h}.hostnames")))],
        ),
        (
            "long pattern list",
            rules_check(&|p| {
                http(p).hostnames = hosts(256);
                without_effects(p);
            }),
            vec![],
        ),
        (
            "too-long pattern list",
            rules_check(&|p| {
                http(p).hostnames = hosts(257);
                without_effects(p);
            }),
            vec![too_many(leak(format!("{h}.hostnames")))],
        ),
        (
            "missing pattern",
            rules_check(&|p| {
                http(p).hostnames[0].clear();
                without_effects(p);
            }),
            vec![required(pat)],
        ),
        (
            "duplicate pattern",
            rules_check(&|p| http(p).hostnames.push("api.example.com".into())),
            vec![duplicate(leak(format!("{h}.hostnames[1]")))],
        ),
        (
            "invalid pattern",
            rules_check(&|p| http(p).hostnames[0] = "https://example.com".into()),
            vec![invalid(pat)],
        ),
        (
            "uppercase pattern",
            rules_check(&|p| http(p).hostnames[0] = "API.EXAMPLE.COM".into()),
            vec![invalid(pat)],
        ),
        (
            "pattern with trailing dot",
            rules_check(&|p| http(p).hostnames[0] = "api.example.com.".into()),
            vec![invalid(pat)],
        ),
        (
            "IP literal pattern",
            rules_check(&|p| http(p).hostnames[0] = "192.0.2.1".into()),
            vec![invalid(pat)],
        ),
        (
            "pattern with port",
            rules_check(&|p| http(p).hostnames[0] = "example.com:443".into()),
            vec![invalid(pat)],
        ),
        (
            "wildcard without effects",
            rules_check(&|p| {
                http(p).hostnames[0] = "*.example.com".into();
                without_effects(p);
            }),
            vec![],
        ),
        (
            "wildcard with effects",
            rules_check(&|p| http(p).hostnames[0] = "*.example.com".into()),
            vec![],
        ),
        (
            "star pattern",
            rules_check(&|p| http(p).hostnames[0] = "*".into()),
            vec![],
        ),
        (
            "invalid wildcard",
            rules_check(&|p| http(p).hostnames[0] = "api.*.example.com".into()),
            vec![invalid(pat)],
        ),
        (
            "invalid sni pattern",
            rules_check(&|p| p.rules[0] = passthrough(numbers(&[443]), &["*example.com"])),
            vec![invalid(
                "egress_policy.rules[0].tls_passthrough.hostnames[0]",
            )],
        ),
        (
            "ports",
            rules_check(&|p| http(p).ports = numbers(&[80, 8080])),
            vec![],
        ),
        (
            "any port",
            rules_check(&|p| http(p).ports = all_ports()),
            vec![],
        ),
        (
            "all and numbers together",
            rules_check(&|p| {
                http(p).ports = Some(pb::Ports {
                    all: Some(pb::AllPorts {}),
                    numbers: vec![80],
                })
            }),
            vec![invalid_by(ports, "union")],
        ),
        (
            "empty ports",
            rules_check(&|p| http(p).ports = Some(pb::Ports::default())),
            vec![invalid_by(ports, "union")],
        ),
        (
            // An empty list and an absent one are the same on the wire.
            "empty numbers",
            rules_check(&|p| http(p).ports = numbers(&[])),
            vec![invalid_by(ports, "union")],
        ),
        (
            "port zero",
            rules_check(&|p| http(p).ports = numbers(&[0])),
            vec![invalid_by(leak(format!("{ports}.numbers[0]")), "minimum")],
        ),
        (
            "port out of range",
            rules_check(&|p| http(p).ports = numbers(&[65536])),
            vec![invalid_by(leak(format!("{ports}.numbers[0]")), "maximum")],
        ),
        (
            "duplicate port",
            rules_check(&|p| http(p).ports = numbers(&[80, 80])),
            vec![duplicate(leak(format!("{ports}.numbers[1]")))],
        ),
        (
            "too many ports",
            rules_check(&|p| http(p).ports = numbers(&(8000..8017).collect::<Vec<_>>())),
            vec![too_many(leak(format!("{ports}.numbers")))],
        ),
        (
            "negative https port",
            rules_check(&|p| p.rules[0] = https_rule(numbers(&[-1]), &["api.example.com"])),
            vec![invalid_by(
                "egress_policy.rules[0].https.ports.numbers[0]",
                "minimum",
            )],
        ),
        (
            "tls passthrough with empty ports",
            rules_check(&|p| p.rules[0] = passthrough(Some(pb::Ports::default()), &["*"])),
            vec![invalid_by(
                "egress_policy.rules[0].tls_passthrough.ports",
                "union",
            )],
        ),
        (
            "same pattern on different ports",
            rules_check(&|p| {
                p.rules.push(pb::EgressRule {
                    http: Some(pb::HttpRule {
                        hostnames: vec!["api.example.com".into()],
                        ports: numbers(&[8080]),
                        effects: None,
                    }),
                    ..Default::default()
                })
            }),
            vec![],
        ),
        (
            "same pattern on the default port ties",
            rules_check(&|p| p.rules.push(trivial_http("api.example.com"))),
            vec![invalid("egress_policy.rules[1].http.hostnames[0]")],
        ),
        (
            "http and https on their default ports do not tie",
            rules_check(&|p| p.rules.push(https_rule(None, &["api.example.com"]))),
            vec![],
        ),
        (
            "http and https on any port tie",
            rules_check(&|p| {
                http(p).ports = all_ports();
                p.rules.push(https_rule(all_ports(), &["api.example.com"]));
            }),
            vec![invalid("egress_policy.rules[1].https.hostnames[0]")],
        ),
        (
            "http and tls passthrough on every port tie",
            rules_check(&|p| {
                p.rules = vec![
                    pb::EgressRule {
                        http: Some(pb::HttpRule {
                            hostnames: vec!["*".into()],
                            ports: all_ports(),
                            effects: None,
                        }),
                        ..Default::default()
                    },
                    passthrough(all_ports(), &["*"]),
                ]
            }),
            vec![invalid(
                "egress_policy.rules[1].tls_passthrough.hostnames[0]",
            )],
        ),
        (
            "http on its default port and tls passthrough on every port",
            rules_check(&|p| p.rules = vec![trivial_http("*"), passthrough(all_ports(), &["*"])]),
            vec![],
        ),
        (
            "https and tls passthrough on the same port tie",
            rules_check(&|p| {
                p.rules.push(https_rule(None, &["*.example.com"]));
                p.rules
                    .push(passthrough(numbers(&[443]), &["*.example.com"]));
            }),
            vec![invalid(
                "egress_policy.rules[2].tls_passthrough.hostnames[0]",
            )],
        ),
        (
            "missing static header",
            rules_check(&|p| headers(p)[0].header.clear()),
            vec![required(at("header"))],
        ),
        (
            "invalid static header",
            rules_check(&|p| headers(p)[0].header = "bad header".into()),
            vec![invalid(at("header"))],
        ),
        (
            // Header names compare case-insensitively.
            "duplicate header",
            rules_check(&|p| {
                headers(p).push(secret_header(
                    "authorization",
                    "hk-secret://example.com/provider/secret",
                ))
            }),
            vec![duplicate(
                "egress_policy.rules[0].http.effects.replace_headers[1].header",
            )],
        ),
        (
            "same header in later rule",
            rules_check(&|p| {
                let mut later = p.rules[0].clone();
                later.http.as_mut().unwrap().hostnames = vec!["other.example.com".into()];
                p.rules.push(later);
            }),
            vec![],
        ),
        (
            "many headers",
            rules_check(&|p| *headers(p) = creds(16)),
            vec![],
        ),
        (
            "too many headers",
            rules_check(&|p| *headers(p) = creds(17)),
            vec![too_many(
                "egress_policy.rules[0].http.effects.replace_headers",
            )],
        ),
        (
            "invalid prefix",
            rules_check(&|p| headers(p)[0].prefix = "Bearer\r".into()),
            vec![invalid(at("prefix"))],
        ),
        (
            "no credential source",
            rules_check(&|p| headers(p)[0].credential_uri.clear()),
            vec![invalid_by(sh, "union")],
        ),
        (
            "invalid credential URI",
            rules_check(&|p| headers(p)[0].credential_uri = "https://example.com/secret".into()),
            vec![invalid(at("credential_uri"))],
        ),
        (
            "actor JWT on an http rule",
            rules_check(&|p| headers(p)[0] = actor_jwt_header(&["https://api.example.com"])),
            vec![],
        ),
        (
            "actor JWT on an https rule",
            rules_check(&|p| {
                p.rules[0] = pb::EgressRule {
                    https: Some(pb::HttpsRule {
                        hostnames: vec!["api.example.com".into()],
                        effects: Some(pb::HttpRuleEffects {
                            replace_headers: vec![actor_jwt_header(&["https://api.example.com"])],
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            }),
            vec![],
        ),
        (
            "actor JWT next to a credential URI",
            rules_check(&|p| {
                let mut j = actor_jwt_header(&["https://api.example.com"]);
                j.header = "X-Actor-Token".into();
                headers(p).push(j);
            }),
            vec![],
        ),
        (
            "credential URI and actor JWT in one header",
            rules_check(&|p| {
                headers(p)[0].actor_jwt = actor_jwt_header(&["https://api.example.com"]).actor_jwt
            }),
            vec![invalid_by(sh, "union")],
        ),
        (
            "actor JWT without audiences",
            rules_check(&|p| headers(p)[0] = actor_jwt_header(&[])),
            vec![required(at("actor_jwt.audiences"))],
        ),
        (
            "actor JWT with an empty audience",
            rules_check(&|p| headers(p)[0] = actor_jwt_header(&[""])),
            vec![invalid_by(at("actor_jwt.audiences[0]"), "minLength")],
        ),
        (
            "actor JWT with a long audience",
            rules_check(&|p| headers(p)[0] = actor_jwt_header(&[&"a".repeat(513)])),
            vec![too_long(at("actor_jwt.audiences[0]"))],
        ),
        (
            "actor JWT with a repeated audience",
            rules_check(&|p| headers(p)[0] = actor_jwt_header(&["a", "a"])),
            vec![duplicate(at("actor_jwt.audiences[1]"))],
        ),
        (
            "actor JWT with too many audiences",
            rules_check(&|p| {
                let auds: Vec<String> = (0..17).map(|i| format!("aud-{i}")).collect();
                let refs: Vec<&str> = auds.iter().map(String::as_str).collect();
                headers(p)[0] = actor_jwt_header(&refs);
            }),
            vec![too_many(at("actor_jwt.audiences"))],
        ),
        (
            "actor JWT without a lifetime",
            rules_check(&|p| headers(p)[0] = jwt(&|j| j.expiration_seconds = 0)),
            vec![required(at("actor_jwt.expiration_seconds"))],
        ),
        (
            "actor JWT lifetime at its bounds",
            rules_check(&|p| {
                let short = jwt(&|j| j.expiration_seconds = 300);
                let mut long = jwt(&|j| j.expiration_seconds = 3600);
                long.header = "X-Actor-Token".into();
                *headers(p) = vec![short, long];
            }),
            vec![],
        ),
        (
            "actor JWT lifetime too short",
            rules_check(&|p| headers(p)[0] = jwt(&|j| j.expiration_seconds = 299)),
            vec![invalid_by(at("actor_jwt.expiration_seconds"), "minimum")],
        ),
        (
            "actor JWT lifetime too long",
            rules_check(&|p| headers(p)[0] = jwt(&|j| j.expiration_seconds = 3601)),
            vec![invalid_by(at("actor_jwt.expiration_seconds"), "maximum")],
        ),
        (
            "empty effects",
            rules_check(&|p| http(p).effects = Some(pb::HttpRuleEffects::default())),
            vec![required(leak(format!("{h}.effects")))],
        ),
        (
            "effects on an https rule",
            rules_check(&|p| {
                p.rules[0] = pb::EgressRule {
                    https: Some(pb::HttpsRule {
                        hostnames: vec!["api.example.com".into()],
                        effects: Some(pb::HttpRuleEffects::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            }),
            vec![required("egress_policy.rules[0].https.effects")],
        ),
    ]);
}

#[test]
fn credential_uri_validation() {
    for ok in [
        "hk-secret://kubernetes.io/provider/ns/name",
        "hk-secret://vault.example/provider/secret",
    ] {
        assert!(valid_secret_uri(ok), "{ok}");
    }
    for bad in [
        "https://kubernetes.io/provider/ns/name",
        "hk-secret://kubernetes.io/provider",
        "hk-secret://kubernetes.io//provider/secret",
        "hk-secret://kubernetes.io/provider/secret/",
        "hk-secret://kubernetes.io:443/provider/secret",
        // A percent-encoded separator, and percent-encoding of any kind.
        "hk-secret://kubernetes.io/provider/sec%2Fret",
        "hk-secret://kubernetes.io/provider/sec%2Dret",
    ] {
        assert!(!valid_secret_uri(bad), "{bad}");
    }
}

#[test]
fn egress_policy_update_ratchets() {
    // A stored policy that predates a rule keeps validating while unchanged.
    let mut old = valid_policy();
    http(&mut old).hostnames = vec!["A.COM".into()];
    md(&mut old).version = 1;
    let new = with(old.clone(), |p| md(p).version = 2);
    assert_errs(
        "unchanged rules",
        validate_egress_policy_update(&Operation::update(), &Path::root(), &new, &old),
        &[],
    );
    let back = with(old.clone(), |p| md(p).version = 0);
    assert_errs(
        "version cleared",
        validate_egress_policy_update(&Operation::update(), &Path::root(), &back, &old),
        &[invalid_by("metadata.version", "update")],
    );
}
