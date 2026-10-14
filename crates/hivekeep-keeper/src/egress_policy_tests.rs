use super::*;

fn effects() -> Option<api::HttpRuleEffects> {
    Some(api::HttpRuleEffects {
        replace_headers: vec![api::CredentialHeader {
            header: "Authorization".into(),
            credential_uri: "secret://team-a/token".into(),
            ..Default::default()
        }],
    })
}

fn meta(uid: &str) -> Option<api::ResourceMetadata> {
    Some(api::ResourceMetadata {
        hive: "team-a".into(),
        name: "default".into(),
        uid: uid.into(),
        ..Default::default()
    })
}

fn dest(hosts: &[&str], ports: Option<pb::EgressPorts>) -> Option<pb::EgressDestinations> {
    Some(pb::EgressDestinations {
        hostnames: hosts.iter().map(|h| h.to_string()).collect(),
        ports,
    })
}

#[test]
fn to_keeper_egress_policy_cases() {
    let cases: Vec<(&str, Option<api::EgressPolicy>, Option<pb::EgressPolicy>)> = vec![
        ("no policy", None, None),
        (
            "no rules",
            Some(api::EgressPolicy {
                metadata: meta(""),
                ..Default::default()
            }),
            Some(pb::EgressPolicy::default()),
        ),
        (
            // Metadata and effects are the gateway's, and never reach the
            // worker.
            "every protocol",
            Some(api::EgressPolicy {
                metadata: meta("uid"),
                rules: vec![
                    api::EgressRule {
                        http: Some(api::HttpRule {
                            hostnames: vec!["api.example.com".into()],
                            ports: Some(api::Ports {
                                all: None,
                                numbers: vec![80, 8080],
                            }),
                            effects: effects(),
                        }),
                        ..Default::default()
                    },
                    api::EgressRule {
                        https: Some(api::HttpsRule {
                            hostnames: vec!["*.example.com".into()],
                            ports: Some(api::Ports {
                                all: Some(api::AllPorts {}),
                                numbers: vec![],
                            }),
                            effects: effects(),
                        }),
                        ..Default::default()
                    },
                    api::EgressRule {
                        tls_passthrough: Some(api::TlsPassthroughRule {
                            hostnames: vec!["db.example.com".into(), "cache.example.com".into()],
                            ports: Some(api::Ports {
                                all: None,
                                numbers: vec![5432],
                            }),
                        }),
                        ..Default::default()
                    },
                ],
            }),
            Some(pb::EgressPolicy {
                rules: vec![
                    pb::EgressRule {
                        http: dest(
                            &["api.example.com"],
                            Some(pb::EgressPorts {
                                all: None,
                                numbers: vec![80, 8080],
                            }),
                        ),
                        ..Default::default()
                    },
                    pb::EgressRule {
                        https: dest(
                            &["*.example.com"],
                            Some(pb::EgressPorts {
                                all: Some(pb::EgressAllPorts {}),
                                numbers: vec![],
                            }),
                        ),
                        ..Default::default()
                    },
                    pb::EgressRule {
                        tls_passthrough: dest(
                            &["db.example.com", "cache.example.com"],
                            Some(pb::EgressPorts {
                                all: None,
                                numbers: vec![5432],
                            }),
                        ),
                        ..Default::default()
                    },
                ],
            }),
        ),
        (
            "rule without ports",
            Some(api::EgressPolicy {
                rules: vec![api::EgressRule {
                    tls_passthrough: Some(api::TlsPassthroughRule {
                        hostnames: vec!["db.example.com".into()],
                        ports: None,
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Some(pb::EgressPolicy {
                rules: vec![pb::EgressRule {
                    tls_passthrough: dest(&["db.example.com"], None),
                    ..Default::default()
                }],
            }),
        ),
        (
            // A rule of a protocol the copy does not carry keeps its place,
            // and allows nothing.
            "rule without a protocol",
            Some(api::EgressPolicy {
                rules: vec![api::EgressRule::default()],
                ..Default::default()
            }),
            Some(pb::EgressPolicy {
                rules: vec![pb::EgressRule::default()],
            }),
        ),
    ];
    for (name, input, want) in cases {
        let got = input.as_ref().map(to_keeper_egress_policy);
        assert_eq!(got, want, "{name}");
    }
}
