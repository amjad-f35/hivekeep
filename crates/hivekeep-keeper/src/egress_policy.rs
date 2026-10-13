//! An actor's egress policy as the worker sees it.

use hivekeep_proto::api::v1 as api;
use hivekeep_proto::keeper::v1 as pb;

/// The destinations each rule of the control plane's policy allows. Policy
/// metadata and rule effects are dropped: the worker does not apply them,
/// and they name credentials it has no need to see.
pub fn to_keeper_egress_policy(p: &api::EgressPolicy) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: p
            .rules
            .iter()
            .map(|r| pb::EgressRule {
                http: r
                    .http
                    .as_ref()
                    .map(|h| destinations(&h.hostnames, h.ports.as_ref())),
                https: r
                    .https
                    .as_ref()
                    .map(|h| destinations(&h.hostnames, h.ports.as_ref())),
                tls_passthrough: r
                    .tls_passthrough
                    .as_ref()
                    .map(|t| destinations(&t.hostnames, t.ports.as_ref())),
            })
            .collect(),
    }
}

fn destinations(hostnames: &[String], ports: Option<&api::Ports>) -> pb::EgressDestinations {
    pb::EgressDestinations {
        hostnames: hostnames.to_vec(),
        ports: ports.map(|p| pb::EgressPorts {
            all: p.all.as_ref().map(|_| pb::EgressAllPorts {}),
            numbers: p.numbers.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copies_destinations_and_drops_effects() {
        let p = api::EgressPolicy {
            metadata: Some(Default::default()),
            rules: vec![
                api::EgressRule {
                    http: Some(api::HttpRule {
                        hostnames: vec!["a.example.com".into()],
                        ports: Some(api::Ports {
                            all: None,
                            numbers: vec![80, 8080],
                        }),
                        effects: Some(Default::default()),
                    }),
                    https: None,
                    tls_passthrough: None,
                },
                api::EgressRule {
                    http: None,
                    https: None,
                    tls_passthrough: Some(api::TlsPassthroughRule {
                        hostnames: vec!["*.svc".into()],
                        ports: Some(api::Ports {
                            all: Some(api::AllPorts {}),
                            numbers: vec![],
                        }),
                    }),
                },
            ],
        };
        let out = to_keeper_egress_policy(&p);
        assert_eq!(out.rules.len(), 2);
        let http = out.rules[0].http.as_ref().unwrap();
        assert_eq!(http.hostnames, vec!["a.example.com"]);
        assert_eq!(http.ports.as_ref().unwrap().numbers, vec![80, 8080]);
        assert!(http.ports.as_ref().unwrap().all.is_none());
        let tls = out.rules[1].tls_passthrough.as_ref().unwrap();
        assert!(tls.ports.as_ref().unwrap().all.is_some());
        assert!(out.rules[1].http.is_none());
    }
}

#[cfg(test)]
#[path = "egress_policy_tests.rs"]
mod behavior_tests;
