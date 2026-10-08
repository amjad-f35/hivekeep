//! EgressPolicy validation.

use std::collections::HashMap;
use std::fmt;

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::field::{ErrorList, FieldError, Path};
use crate::hostname::HostnamePattern;
use crate::names;
use crate::validation::checks;
use crate::validation::rules::{self, int, list, message, string, Format, HiveRule, Operation};

/// URI scheme of secret references in credential headers.
pub const SECRET_URI_SCHEME: &str = "hk-secret";

fn hostnames(op: &Operation, p: &Path, v: &[String], o: Option<&[String]>) -> ErrorList {
    list(op, p, v, o)
        .max_items(256)
        .required()
        .unique_values()
        .custom(|p, v| {
            let mut errs = ErrorList::new();
            for (i, raw) in v.iter().enumerate() {
                let ip = p.index(i);
                if raw.is_empty() {
                    errs.push(FieldError::required(&ip, ""));
                } else if HostnamePattern::parse(raw).is_err() {
                    errs.push(FieldError::invalid(
                        &ip,
                        raw,
                        r#"must be a DNS hostname, optionally with a complete leftmost-label wildcard, or "*""#,
                    ));
                }
            }
            errs
        })
        .finish()
}

fn ports(op: &Operation, p: &Path, n: &pb::Ports, o: Option<&pb::Ports>) -> ErrorList {
    let set = |x: &pb::Ports| [x.all.is_some(), !x.numbers.is_empty()];
    let now = set(n);
    rules::all([
        rules::union(
            op,
            p,
            &[("all", now[0]), ("numbers", now[1])],
            o.map(set).as_ref().map(|a| a.as_slice()),
        ),
        list(
            op,
            &p.child("numbers"),
            &n.numbers,
            o.map(|o| o.numbers.as_slice()),
        )
        .max_items(16)
        .optional()
        .each(|p, v, _| int(op, p, *v, None::<i64>).min(1).max(65_535).finish())
        .unique_values()
        .finish(),
    ])
}

/// Header names are RFC 9110 tokens.
fn valid_header_name(v: &str) -> bool {
    !v.is_empty()
        && v.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
}

/// Field values may hold any byte but controls (tab excepted) and DEL.
fn valid_header_value(v: &str) -> bool {
    v.bytes().all(|c| c == b'\t' || (c >= b' ' && c != 0x7f))
}

/// `hk-secret://<provider-class>/<provider-name>/<tail...>`: a DNS
/// subdomain host, at least two non-empty path segments, no percent-encoding,
/// userinfo, port, query or fragment.
pub fn valid_secret_uri(raw: &str) -> bool {
    let Ok(u) = crate::uri::Uri::parse(raw) else {
        return false;
    };
    if u.scheme != SECRET_URI_SCHEME
        || u.host.is_empty()
        || u.port.is_some()
        || u.user.is_some()
        || u.query.is_some()
        || u.fragment.is_some()
        || !names::is_dns_subdomain(&u.host)
    {
        return false;
    }
    let path = &u.raw_path;
    if path.contains('%') || !path.starts_with('/') || path.ends_with('/') {
        return false;
    }
    let parts: Vec<&str> = path[1..].split('/').collect();
    parts.len() >= 2 && parts.iter().all(|s| !s.is_empty())
}

fn credential_header(
    op: &Operation,
    p: &Path,
    n: &pb::CredentialHeader,
    o: Option<&pb::CredentialHeader>,
) -> ErrorList {
    let set = |x: &pb::CredentialHeader| [!x.credential_uri.is_empty(), x.actor_jwt.is_some()];
    let now = set(n);
    rules::all([
        rules::union(op, p, &[("credential_uri", now[0]), ("actor_jwt", now[1])], o.map(set).as_ref().map(|a| a.as_slice())),
        string(op, &p.child("header"), &n.header, o.map(|o| o.header.as_str()))
            .required()
            .custom(|p, v| rules::when(!valid_header_name(v), || FieldError::invalid(p, v, "must be an HTTP header name").into()))
            .finish(),
        string(op, &p.child("prefix"), &n.prefix, o.map(|o| o.prefix.as_str()))
            .optional()
            .custom(|p, v| {
                rules::when(!valid_header_value(v), || FieldError::invalid(p, v, "must be a valid HTTP field value prefix").into())
            })
            .finish(),
        string(op, &p.child("credential_uri"), &n.credential_uri, o.map(|o| o.credential_uri.as_str()))
            .optional()
            .custom(|p, v| {
                rules::when(!valid_secret_uri(v), || {
                    FieldError::invalid(p, v, format!("must be {SECRET_URI_SCHEME}://<provider-class>/<provider-name>/<provider-specific-tail>"))
                        .into()
                })
            })
            .finish(),
        message(op, &p.child("actor_jwt"), n.actor_jwt.as_ref(), o.map(|o| o.actor_jwt.as_ref()))
            .optional()
            .then(|p, n, o| {
                rules::all([
                    list(op, &p.child("audiences"), &n.audiences, o.map(|o| o.audiences.as_slice()))
                        .max_items(16)
                        .required()
                        .each(|p, s, _| string(op, p, s, None).min_len(1).max_len(512).finish())
                        .unique_values()
                        .finish(),
                    int(op, &p.child("expiration_seconds"), n.expiration_seconds, o.map(|o| o.expiration_seconds))
                        .required()
                        .min(300)
                        .max(3600)
                        .finish(),
                ])
            })
            .finish(),
    ])
}

fn effects(
    op: &Operation,
    p: &Path,
    n: &pb::HttpRuleEffects,
    o: Option<&pb::HttpRuleEffects>,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if n.replace_headers.is_empty() {
        errs.push(FieldError::required(
            p,
            "at least one effect must be specified",
        ));
    }
    errs.extend(
        list(
            op,
            &p.child("replace_headers"),
            &n.replace_headers,
            o.map(|o| o.replace_headers.as_slice()),
        )
        .max_items(16)
        .optional()
        .each_keyed(
            |h| h.header.to_ascii_lowercase(),
            |p, n, o| credential_header(op, p, n, o),
        )
        .custom(|p, v| {
            let mut seen = std::collections::HashSet::new();
            v.iter()
                .enumerate()
                .filter(|(_, h)| !seen.insert(h.header.to_ascii_lowercase()))
                .map(|(i, h)| FieldError::duplicate(&p.index(i).child("header"), &h.header))
                .collect()
        })
        .finish(),
    );
    errs
}

/// The fields every rule kind shares.
struct RuleParts<'a> {
    hostnames: &'a [String],
    ports: Option<&'a pb::Ports>,
    effects: Option<&'a pb::HttpRuleEffects>,
}

fn rule_parts(
    op: &Operation,
    p: &Path,
    n: RuleParts<'_>,
    o: Option<RuleParts<'_>>,
    ports_required: bool,
) -> ErrorList {
    let ports_rule = message(op, &p.child("ports"), n.ports, o.as_ref().map(|o| o.ports));
    let ports_rule = if ports_required {
        ports_rule.required()
    } else {
        ports_rule.optional()
    };
    rules::all([
        hostnames(
            op,
            &p.child("hostnames"),
            n.hostnames,
            o.as_ref().map(|o| o.hostnames),
        ),
        ports_rule.then(|p, n, o| ports(op, p, n, o)).finish(),
        message(
            op,
            &p.child("effects"),
            n.effects,
            o.as_ref().map(|o| o.effects),
        )
        .optional()
        .then(|p, n, o| effects(op, p, n, o))
        .finish(),
    ])
}

fn http(r: &pb::HttpRule) -> RuleParts<'_> {
    RuleParts {
        hostnames: &r.hostnames,
        ports: r.ports.as_ref(),
        effects: r.effects.as_ref(),
    }
}

fn https(r: &pb::HttpsRule) -> RuleParts<'_> {
    RuleParts {
        hostnames: &r.hostnames,
        ports: r.ports.as_ref(),
        effects: r.effects.as_ref(),
    }
}

fn tls(r: &pb::TlsPassthroughRule) -> RuleParts<'_> {
    RuleParts {
        hostnames: &r.hostnames,
        ports: r.ports.as_ref(),
        effects: None,
    }
}

/// One egress rule; exactly one protocol member is set.
pub fn egress_rule(
    op: &Operation,
    p: &Path,
    n: &pb::EgressRule,
    o: Option<&pb::EgressRule>,
) -> ErrorList {
    let set = |x: &pb::EgressRule| {
        [
            x.http.is_some(),
            x.https.is_some(),
            x.tls_passthrough.is_some(),
        ]
    };
    let now = set(n);
    rules::all([
        rules::union(
            op,
            p,
            &[
                ("http", now[0]),
                ("https", now[1]),
                ("tls_passthrough", now[2]),
            ],
            o.map(set).as_ref().map(|a| a.as_slice()),
        ),
        message(
            op,
            &p.child("http"),
            n.http.as_ref(),
            o.map(|o| o.http.as_ref()),
        )
        .optional()
        .then(|p, n, o| rule_parts(op, p, http(n), o.map(http), false))
        .finish(),
        message(
            op,
            &p.child("https"),
            n.https.as_ref(),
            o.map(|o| o.https.as_ref()),
        )
        .optional()
        .then(|p, n, o| rule_parts(op, p, https(n), o.map(https), false))
        .finish(),
        message(
            op,
            &p.child("tls_passthrough"),
            n.tls_passthrough.as_ref(),
            o.map(|o| o.tls_passthrough.as_ref()),
        )
        .optional()
        .then(|p, n, o| rule_parts(op, p, tls(n), o.map(tls), true))
        .finish(),
    ])
}

/// What a rule port matches: one number, or every port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PortKey {
    Number(i32),
    All,
}

impl fmt::Display for PortKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortKey::Number(n) => write!(f, "port {n}"),
            PortKey::All => f.write_str("every port"),
        }
    }
}

/// Rejects two rules that tie on a hostname pattern and a port, whatever
/// their protocols: the gateway could not tell which applies. Defaults are
/// applied first, so http on 80 and https on 443 never tie.
pub fn rules_do_not_tie(p: &Path, rules: &[pb::EgressRule]) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut seen: HashMap<(String, PortKey), (usize, Path)> = HashMap::new();
    for (i, rule) in rules.iter().enumerate() {
        let (member, hosts, ports) = if let Some(r) = &rule.http {
            ("http", &r.hostnames, r.ports.as_ref())
        } else if let Some(r) = &rule.https {
            ("https", &r.hostnames, r.ports.as_ref())
        } else if let Some(r) = &rule.tls_passthrough {
            ("tls_passthrough", &r.hostnames, r.ports.as_ref())
        } else {
            continue;
        };
        let keys: Vec<PortKey> = match ports {
            Some(pts) if pts.all.is_some() => vec![PortKey::All],
            Some(pts) => pts.numbers.iter().map(|n| PortKey::Number(*n)).collect(),
            None => Vec::new(),
        };
        for (j, pattern) in hosts.iter().enumerate() {
            let path = p.index(i).child(member).child("hostnames").index(j);
            for port in &keys {
                match seen.get(&(pattern.clone(), *port)) {
                    None => {
                        seen.insert((pattern.clone(), *port), (i, path.clone()));
                    }
                    Some((prior, prior_path)) if *prior != i => {
                        errs.push(FieldError::invalid(
                            &path,
                            pattern,
                            format!("ties with {prior_path} on {port}"),
                        ));
                    }
                    Some(_) => {}
                }
            }
        }
    }
    errs
}

fn rule_list(
    op: &Operation,
    p: &Path,
    n: &[pb::EgressRule],
    o: Option<&[pb::EgressRule]>,
    required: bool,
) -> ErrorList {
    let l = list(op, p, n, o).max_items(256);
    let l = if required { l.required() } else { l.optional() };
    l.each(|p, n, o| egress_rule(op, p, n, o))
        .custom(rules_do_not_tie)
        .finish()
}

/// Egress rules applied to a template's golden Actor.
pub fn egress_policy_template(
    op: &Operation,
    p: &Path,
    n: &pb::EgressPolicyTemplate,
    o: Option<&pb::EgressPolicyTemplate>,
) -> ErrorList {
    rule_list(
        op,
        &p.child("rules"),
        &n.rules,
        o.map(|o| o.rules.as_slice()),
        true,
    )
}

/// An EgressPolicy resource.
pub fn egress_policy(
    op: &Operation,
    p: &Path,
    n: &pb::EgressPolicy,
    o: Option<&pb::EgressPolicy>,
) -> ErrorList {
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Required,
        ),
        message(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
        )
        .optional()
        .then(|p, n, _| checks::singleton_name(&p.child("name"), &n.name))
        .finish(),
        rule_list(
            op,
            &p.child("rules"),
            &n.rules,
            o.map(|o| o.rules.as_slice()),
            false,
        ),
    ])
}

fn parent_hive_matches(
    actor: Option<&pb::ObjectRef>,
    policy: Option<&pb::EgressPolicy>,
) -> ErrorList {
    let actor_hive = actor.map(|a| a.hive.as_str()).unwrap_or("");
    let policy_hive = policy
        .and_then(|p| p.metadata.as_ref())
        .map(|m| m.hive.as_str())
        .unwrap_or("");
    rules::when(
        !actor_hive.is_empty() && !policy_hive.is_empty() && actor_hive != policy_hive,
        || {
            FieldError::invalid(
                &Path::new(["egress_policy", "metadata", "hive"]),
                policy_hive,
                "must match actor.hive",
            )
            .into()
        },
    )
}

fn actor_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("actor"), r, None, HiveRule::Required)
}

fn policy_name(op: &Operation, name: &str) -> ErrorList {
    string(op, &Path::from("name"), name, None)
        .required()
        .format(Format::DnsLabel)
        .custom(checks::singleton_name)
        .finish()
}

/// `CreateActorEgressPolicy`.
pub fn validate_create_actor_egress_policy_request(
    op: &Operation,
    req: &pb::CreateActorEgressPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        parent_hive_matches(req.actor.as_ref(), req.egress_policy.as_ref()),
        actor_ref(&op, req.actor.as_ref()),
        message(
            &op,
            &Path::from("egress_policy"),
            req.egress_policy.as_ref(),
            None,
        )
        .required()
        .then(|p, n, _| egress_policy(&op, p, n, None))
        .finish(),
    ])
}

/// `UpdateActorEgressPolicy`.
pub fn validate_update_actor_egress_policy_request(
    op: &Operation,
    req: &pb::UpdateActorEgressPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        parent_hive_matches(req.actor.as_ref(), req.egress_policy.as_ref()),
        actor_ref(&op, req.actor.as_ref()),
        message(
            &op,
            &Path::from("egress_policy"),
            req.egress_policy.as_ref(),
            None,
        )
        .required()
        .then(|p, n, _| egress_policy(&op, p, n, None))
        .finish(),
    ])
}

/// `GetActorEgressPolicy`.
pub fn validate_get_actor_egress_policy_request(
    op: &Operation,
    req: &pb::GetActorEgressPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        policy_name(&op, &req.name),
    ])
}

/// `DeleteActorEgressPolicy`.
pub fn validate_delete_actor_egress_policy_request(
    op: &Operation,
    req: &pb::DeleteActorEgressPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        actor_ref(&op, req.actor.as_ref()),
        policy_name(&op, &req.name),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// Validates an egress policy against the stored one.
pub fn validate_egress_policy_update(
    op: &Operation,
    p: &Path,
    new: &pb::EgressPolicy,
    old: &pb::EgressPolicy,
) -> ErrorList {
    egress_policy(&op.as_update(), p, new, Some(old))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_values() {
        for ok in ["Bearer token", "value\tvalue", "\u{80}\u{81}"] {
            assert!(valid_header_value(ok), "{ok:?}");
        }
        for bad in ["a\rb", "a\nb", "a\0b", "a\x1fb", "a\x7fb"] {
            assert!(!valid_header_value(bad), "{bad:?}");
        }
    }

    #[test]
    fn header_names() {
        for ok in ["Authorization", "x-custom_header", "!#$%&'*+-.^_`|~"] {
            assert!(valid_header_name(ok), "{ok:?}");
        }
        for bad in ["", "bad header", "bad:header", "héader"] {
            assert!(!valid_header_name(bad), "{bad:?}");
        }
    }
}
