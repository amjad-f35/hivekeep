//! Egress rule constructors and giving an Actor a policy.

use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::clients::Clients;
use crate::harness::T;

/// What a test that is not about egress gives its Actor, since the gate
/// denies an Actor with no policy at all: every name and address, as
/// cleartext HTTP on any port and as intercepted HTTPS on 443. A
/// passthrough rule on every port would tie with the http rule, which the
/// API rejects.
pub fn egress_allow_all() -> Vec<pb::EgressRule> {
    vec![
        pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: vec!["*".into()],
                ports: Some(pb::Ports {
                    all: Some(pb::AllPorts {}),
                    numbers: vec![],
                }),
                effects: None,
            }),
            ..Default::default()
        },
        pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: vec!["*".into()],
                ..Default::default()
            }),
            ..Default::default()
        },
    ]
}

fn strings(patterns: &[&str]) -> Vec<String> {
    patterns.iter().map(|s| s.to_string()).collect()
}

/// Cleartext HTTP on port 80 to the hosts matching `patterns` (exact
/// names, or `*.` plus a name for one leftmost label).
pub fn egress_allow_http(patterns: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: strings(patterns),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// HTTPS on 443, intercepted by the gate, to the hosts matching `patterns`.
pub fn egress_allow_https(patterns: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        https: Some(pb::HttpsRule {
            hostnames: strings(patterns),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// TLS on any port to the hosts matching `patterns`, relayed undecrypted.
pub fn egress_allow_passthrough(patterns: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        tls_passthrough: Some(pb::TlsPassthroughRule {
            hostnames: strings(patterns),
            ports: Some(pb::Ports {
                all: Some(pb::AllPorts {}),
                numbers: vec![],
            }),
        }),
        ..Default::default()
    }
}

/// [`egress_allow_passthrough`] confined to `ports`.
pub fn egress_allow_passthrough_on_ports(ports: &[i32], patterns: &[&str]) -> pb::EgressRule {
    pb::EgressRule {
        tls_passthrough: Some(pb::TlsPassthroughRule {
            hostnames: strings(patterns),
            ports: Some(pb::Ports {
                all: None,
                numbers: ports.to_vec(),
            }),
        }),
        ..Default::default()
    }
}

fn replace_header_effects(header: &str, prefix: &str, credential_uri: &str) -> pb::HttpRuleEffects {
    pb::HttpRuleEffects {
        replace_headers: vec![pb::CredentialHeader {
            header: header.into(),
            prefix: prefix.into(),
            credential_uri: credential_uri.into(),
            ..Default::default()
        }],
    }
}

/// An https rule that, on a match, has the gate resolve `credential_uri`
/// through its credential provider and replace `header` with `prefix` plus
/// the credential.
pub fn egress_inject_header(
    header: &str,
    prefix: &str,
    credential_uri: &str,
    patterns: &[&str],
) -> pb::EgressRule {
    let mut rule = egress_allow_https(patterns);
    if let Some(h) = rule.https.as_mut() {
        h.effects = Some(replace_header_effects(header, prefix, credential_uri));
    }
    rule
}

/// The http-rule counterpart of [`egress_inject_header`].
pub fn egress_inject_header_http(
    header: &str,
    prefix: &str,
    credential_uri: &str,
    patterns: &[&str],
) -> pb::EgressRule {
    let mut rule = egress_allow_http(patterns);
    if let Some(h) = rule.http.as_mut() {
        h.effects = Some(replace_header_effects(header, prefix, credential_uri));
    }
    rule
}

/// Gives `actor` an EgressPolicy with exactly `rules`, replacing any it
/// had. Deleting the Actor deletes the policy, so there is no cleanup.
pub async fn ensure_egress_policy(
    t: &T,
    clients: &Clients,
    actor: &pb::ObjectRef,
    rules: Vec<pb::EgressRule>,
) {
    let mut policy = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: actor.hive.clone(),
            name: SINGLETON_NAME.into(),
            ..Default::default()
        }),
        rules,
    };
    let created = clients
        .api()
        .create_actor_egress_policy(pb::CreateActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            egress_policy: Some(policy.clone()),
        })
        .await;
    match created {
        Ok(_) => return,
        Err(e) if e.code() != Code::AlreadyExists => t.fatal(format!(
            "CreateActorEgressPolicy for {}/{}: {e}",
            actor.hive, actor.name
        )),
        Err(_) => {}
    }
    let existing = clients
        .api()
        .get_actor_egress_policy(pb::GetActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            name: SINGLETON_NAME.into(),
        })
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "GetActorEgressPolicy for {}/{}: {e}",
                actor.hive, actor.name
            ))
        })
        .into_inner();
    policy.metadata = existing.metadata;
    if let Err(e) = clients
        .api()
        .update_actor_egress_policy(pb::UpdateActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            egress_policy: Some(policy),
        })
        .await
    {
        t.fatal(format!(
            "UpdateActorEgressPolicy for {}/{}: {e}",
            actor.hive, actor.name
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors() {
        let all = egress_allow_all();
        assert_eq!(all.len(), 2);
        assert!(all[0]
            .http
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .all
            .is_some());
        let r = egress_inject_header(
            "authorization",
            "Bearer ",
            "hk-secret://x",
            &["api.example"],
        );
        let effects = r.https.unwrap().effects.unwrap();
        assert_eq!(effects.replace_headers[0].credential_uri, "hk-secret://x");
        let p = egress_allow_passthrough_on_ports(&[8443], &["*.example"]);
        assert_eq!(
            p.tls_passthrough.unwrap().ports.unwrap().numbers,
            vec![8443]
        );
        assert!(egress_inject_header_http("x", "", "u", &["h"])
            .http
            .unwrap()
            .effects
            .is_some());
    }
}
