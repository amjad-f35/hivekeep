//! Compiled egress policies: request decisions and the ordered SNI rules
//! for TLS connections.

use std::fmt;

use hivekeep_core::hostname::HostnamePattern;
use hivekeep_proto::api::v1 as pb;

use super::Destination;

/// Ports an http or https rule that names none applies to. The API fills
/// these in before storing a policy; repeating them here means an unset
/// field can only ever narrow a rule.
const DEFAULT_HTTP_PORT: u16 = 80;
const DEFAULT_HTTPS_PORT: u16 = 443;

/// The outcome of evaluating a policy against a [`Destination`].
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    /// Whether some rule authorized the destination.
    pub allowed: bool,
    /// Index of the deciding rule in the policy, `None` when nothing matched.
    pub rule_index: Option<usize>,
    /// Effects of the deciding rule, when it declares any.
    pub effects: Option<pb::HttpRuleEffects>,
}

impl Decision {
    /// The decision when no rule matched.
    pub fn deny() -> Self {
        Decision {
            allowed: false,
            rule_index: None,
            effects: None,
        }
    }
}

/// An entry that failed to compile; it is dropped, which only narrows an
/// allow rule and so fails closed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("rules[{rule}].{member}.{field}: {message}")]
pub struct CompileError {
    /// Index of the rule.
    pub rule: usize,
    /// The rule's protocol member (`http`, `https`, `tls_passthrough`).
    pub member: &'static str,
    /// The failing field.
    pub field: &'static str,
    /// What is wrong.
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Http,
    Https,
    TlsPassthrough,
}

#[derive(Debug, Clone, Default)]
struct CompiledRule {
    protocol: Option<Protocol>,
    patterns: Vec<HostnamePattern>,
    ports: Vec<u16>,
    any_port: bool,
    effects: Option<pb::HttpRuleEffects>,
}

/// How a TLS connection is handled when an [`SniRule`] matches first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SniMode {
    /// Terminate TLS and decide each request inside.
    Mitm,
    /// Forward TLS without decrypting it.
    Passthrough,
}

impl SniMode {
    /// The wire spelling shared with the dataplane's policy filter.
    pub fn as_str(self) -> &'static str {
        match self {
            SniMode::Mitm => "mitm",
            SniMode::Passthrough => "passthrough",
        }
    }
}

impl fmt::Display for SniMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An SNI pattern and the mode applied when it matches first.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SniRule {
    /// The pattern as written.
    pub pattern: String,
    /// What to do on a match.
    pub mode: SniMode,
}

/// Pattern specificity, most specific first. `*` ranks below a labeled
/// wildcard so the two never tie.
fn pattern_rank(p: &HostnamePattern) -> u8 {
    match p {
        HostnamePattern::Exact(_) => 0,
        HostnamePattern::Wildcard(_) => 1,
        HostnamePattern::Any => 2,
    }
}

/// Orders matches: name specificity first, then port; lower wins and ties
/// keep policy order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
struct MatchRank {
    name: u8,
    port: u8,
}

impl CompiledRule {
    fn matches_port(&self, port: u16) -> bool {
        self.any_port || self.ports.contains(&port)
    }

    fn port_rank(&self) -> u8 {
        u8::from(self.any_port)
    }

    /// The rank of the best pattern matching `dest`. An address is matched
    /// by `*` alone.
    fn match_name(&self, dest: &Destination) -> Option<u8> {
        self.patterns
            .iter()
            .filter(|p| {
                p.matches(&dest.hostname)
                    || (matches!(p, HostnamePattern::Any) && dest.ip.is_some())
            })
            .map(pattern_rank)
            .min()
    }
}

/// An egress policy with its patterns and ports parsed once.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    rules: Vec<CompiledRule>,
}

impl Policy {
    /// Compiles `policy`. The API validates the same way, so nothing should
    /// fail; an entry that does is dropped and reported. The returned policy
    /// is always usable.
    pub fn compile(policy: &pb::EgressPolicy) -> (Policy, Vec<CompileError>) {
        Self::compile_rules(&policy.rules)
    }

    /// Compiles a list of rules, as [`Policy::compile`] does.
    pub fn compile_rules(rules: &[pb::EgressRule]) -> (Policy, Vec<CompileError>) {
        let mut errs = Vec::new();
        let mut compiled = Policy::default();
        for (i, rule) in rules.iter().enumerate() {
            let mut cr = CompiledRule::default();
            let (member, raw, ports, default_port) = if let Some(r) = &rule.http {
                cr.protocol = Some(Protocol::Http);
                cr.effects = r.effects.clone();
                (
                    "http",
                    &r.hostnames,
                    r.ports.as_ref(),
                    Some(DEFAULT_HTTP_PORT),
                )
            } else if let Some(r) = &rule.https {
                cr.protocol = Some(Protocol::Https);
                cr.effects = r.effects.clone();
                (
                    "https",
                    &r.hostnames,
                    r.ports.as_ref(),
                    Some(DEFAULT_HTTPS_PORT),
                )
            } else if let Some(r) = &rule.tls_passthrough {
                cr.protocol = Some(Protocol::TlsPassthrough);
                ("tls_passthrough", &r.hostnames, r.ports.as_ref(), None)
            } else {
                compiled.rules.push(cr);
                continue;
            };
            let err = |field, message: String| CompileError {
                rule: i,
                member,
                field,
                message,
            };
            for entry in raw {
                match HostnamePattern::parse(entry) {
                    Ok(p) => cr.patterns.push(p),
                    Err(e) => errs.push(err("hostnames", e.to_string())),
                }
            }
            match (ports, default_port) {
                (None, None) => errs.push(err("ports", "required".into())),
                (None, Some(d)) => cr.ports = vec![d],
                (Some(p), _) if p.all.is_some() => cr.any_port = true,
                _ => {}
            }
            for &n in ports.map(|p| p.numbers.as_slice()).unwrap_or_default() {
                match u16::try_from(n) {
                    Ok(port) if port != 0 => cr.ports.push(port),
                    _ => errs.push(err("ports.numbers", format!("{n} is not a port number"))),
                }
            }
            compiled.rules.push(cr);
        }
        (compiled, errs)
    }

    /// Number of rules, dropped entries included. A policy without rules
    /// authorizes nothing.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// The https and tls_passthrough patterns applying to a dialed port,
    /// most specific first (ties keep policy order).
    pub fn sni_rules(&self, port: u16) -> Vec<SniRule> {
        let mut entries: Vec<(MatchRank, SniRule)> = Vec::new();
        for rule in &self.rules {
            if !rule.matches_port(port) {
                continue;
            }
            let mode = match rule.protocol {
                Some(Protocol::Https) => SniMode::Mitm,
                Some(Protocol::TlsPassthrough) => SniMode::Passthrough,
                _ => continue,
            };
            for p in &rule.patterns {
                entries.push((
                    MatchRank {
                        name: pattern_rank(p),
                        port: rule.port_rank(),
                    },
                    SniRule {
                        pattern: p.to_string(),
                        mode,
                    },
                ));
            }
        }
        // Stable sort: equal ranks keep policy order.
        entries.sort_by_key(|(rank, _)| *rank);
        entries.into_iter().map(|(_, r)| r).collect()
    }

    /// Decides one request the gateway can read, on the name or address in
    /// its authority and the port the Actor dialed. `decrypted` selects the
    /// https rules (TLS terminated by the gateway); otherwise http rules
    /// apply. A tls_passthrough rule never decides a request.
    pub fn evaluate_request(&self, dest: &Destination, decrypted: bool) -> Decision {
        let want = if decrypted {
            Protocol::Https
        } else {
            Protocol::Http
        };
        let mut best: Option<(MatchRank, usize)> = None;
        for (i, rule) in self.rules.iter().enumerate() {
            if rule.protocol != Some(want) || (dest.port != 0 && !rule.matches_port(dest.port)) {
                continue;
            }
            let Some(name) = rule.match_name(dest) else {
                continue;
            };
            let rank = MatchRank {
                name,
                port: rule.port_rank(),
            };
            if best.is_none_or(|(b, _)| rank < b) {
                best = Some((rank, i));
            }
        }
        match best {
            Some((_, i)) => Decision {
                allowed: true,
                rule_index: Some(i),
                effects: self.rules[i].effects.clone(),
            },
            None => Decision::deny(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hosts(p: &[&str]) -> Vec<String> {
        p.iter().map(|s| s.to_string()).collect()
    }
    fn http(p: &[&str]) -> pb::EgressRule {
        http_on(None, p)
    }
    fn http_on(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressRule {
        pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: hosts(p),
                ports,
                effects: None,
            }),
            ..Default::default()
        }
    }
    fn https(p: &[&str]) -> pb::EgressRule {
        https_on(None, p)
    }
    fn https_on(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressRule {
        pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: hosts(p),
                ports,
                effects: None,
            }),
            ..Default::default()
        }
    }
    fn passthrough(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressRule {
        pb::EgressRule {
            tls_passthrough: Some(pb::TlsPassthroughRule {
                hostnames: hosts(p),
                ports,
            }),
            ..Default::default()
        }
    }
    fn ports(n: &[i32]) -> Option<pb::Ports> {
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
    fn compile_ok(rules: Vec<pb::EgressRule>) -> Policy {
        let (p, errs) = Policy::compile_rules(&rules);
        assert!(errs.is_empty(), "compile errors: {errs:?}");
        p
    }
    fn host(n: &str) -> Destination {
        Destination::host(n)
    }
    fn allow(i: usize) -> Decision {
        Decision {
            allowed: true,
            rule_index: Some(i),
            effects: None,
        }
    }

    #[test]
    fn compile_reports_and_drops_invalid_entries() {
        let (compiled, errs) = Policy::compile_rules(&[
            http(&["good.example.com", "BAD.example.com"]),
            passthrough(ports(&[443, 0]), &["*"]),
            passthrough(None, &["*"]),
        ]);
        assert_eq!(errs.len(), 3, "{errs:?}");
        assert_eq!(compiled.rule_count(), 3);
        let d = compiled.evaluate_request(&host("good.example.com"), false);
        assert_eq!(d, allow(0));
        assert!(
            !compiled
                .evaluate_request(&host("bad.example.com"), false)
                .allowed
        );
    }

    #[test]
    fn evaluate_request_cases() {
        let effects = pb::HttpRuleEffects {
            replace_headers: vec![pb::CredentialHeader {
                header: "authorization".into(),
                prefix: "Bearer ".into(),
                credential_uri: "hk-secret://k8s/default/token".into(),
                actor_jwt: None,
            }],
        };
        let with_effects = pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: hosts(&["api.example.com"]),
                ports: None,
                effects: Some(effects.clone()),
            }),
            ..Default::default()
        };
        let v4 = |p| Destination::addr("192.0.2.1".parse().unwrap()).with_port(p);
        let deny = Decision::deny();
        let cases: Vec<(&str, Vec<pb::EgressRule>, Destination, bool, Decision)> = vec![
            (
                "no rules denies",
                vec![],
                host("example.com"),
                false,
                deny.clone(),
            ),
            (
                "exact hostname",
                vec![http(&["example.com"])],
                host("example.com"),
                false,
                allow(0),
            ),
            (
                "http rule does not match another name",
                vec![http(&["example.com"])],
                host("example.org"),
                false,
                deny.clone(),
            ),
            (
                "wildcard hostname",
                vec![http(&["*.example.com"])],
                host("api.example.com"),
                false,
                allow(0),
            ),
            (
                "star matches every name",
                vec![http(&["*"])],
                host("anything.example"),
                false,
                allow(0),
            ),
            (
                "any pattern in the rule matches",
                vec![http(&["other.example", "example.com"])],
                host("example.com"),
                false,
                allow(0),
            ),
            (
                "star matches an ip literal",
                vec![http(&["*"])],
                v4(80),
                false,
                allow(0),
            ),
            (
                "a name does not match an ip literal",
                vec![http(&["example.com", "*.example.com"])],
                v4(80),
                false,
                deny.clone(),
            ),
            (
                "an empty destination matches nothing",
                vec![http(&["*"])],
                Destination::default().with_port(80),
                false,
                deny.clone(),
            ),
            (
                "http rule does not decide a decrypted request",
                vec![http(&["api.example.com"])],
                host("api.example.com"),
                true,
                deny.clone(),
            ),
            (
                "https rule does not decide a cleartext request",
                vec![https(&["api.example.com"])],
                host("api.example.com"),
                false,
                deny.clone(),
            ),
            (
                "https rule decides a decrypted request and carries its effects",
                vec![http(&["api.example.com"]), with_effects],
                host("api.example.com"),
                true,
                Decision {
                    allowed: true,
                    rule_index: Some(1),
                    effects: Some(effects),
                },
            ),
            (
                "passthrough rule never decides a request",
                vec![passthrough(all_ports(), &["*"])],
                host("api.example.com"),
                true,
                deny.clone(),
            ),
            (
                "dialed port outside the rule",
                vec![http_on(ports(&[8080]), &["api.example.com"])],
                host("api.example.com").with_port(80),
                false,
                deny.clone(),
            ),
            (
                "dialed port in the rule",
                vec![http_on(ports(&[8080]), &["api.example.com"])],
                host("api.example.com").with_port(8080),
                false,
                allow(0),
            ),
            (
                "default http port",
                vec![http(&["api.example.com"])],
                host("api.example.com").with_port(80),
                false,
                allow(0),
            ),
            (
                "default https port",
                vec![https(&["api.example.com"])],
                host("api.example.com").with_port(443),
                true,
                allow(0),
            ),
            (
                "any port",
                vec![http_on(all_ports(), &["api.example.com"])],
                host("api.example.com").with_port(8080),
                false,
                allow(0),
            ),
            (
                "unknown dialed port is not enforced",
                vec![http_on(ports(&[8080]), &["api.example.com"])],
                host("api.example.com"),
                false,
                allow(0),
            ),
            (
                "exact pattern beats an earlier wildcard",
                vec![http(&["*.example.com"]), http(&["api.example.com"])],
                host("api.example.com"),
                false,
                allow(1),
            ),
            (
                "labeled wildcard beats an earlier star",
                vec![http(&["*"]), http(&["*.example.com"])],
                host("api.example.com"),
                false,
                allow(1),
            ),
            (
                "named port beats an earlier any port on the same pattern",
                vec![
                    http_on(all_ports(), &["api.example.com"]),
                    http_on(ports(&[80]), &["api.example.com"]),
                ],
                host("api.example.com"),
                false,
                allow(1),
            ),
            (
                "name specificity outranks port specificity",
                vec![
                    http_on(ports(&[80]), &["*.example.com"]),
                    http_on(all_ports(), &["api.example.com"]),
                ],
                host("api.example.com"),
                false,
                allow(1),
            ),
            (
                "a full tie keeps policy order",
                vec![http(&["*.example.com"]), http(&["*.example.com"])],
                host("api.example.com"),
                false,
                allow(0),
            ),
            (
                "a rule holding star and an exact name wins the exact name",
                vec![
                    http(&["*", "api.example-corp.com"]),
                    http(&["*.example-corp.com"]),
                ],
                host("api.example-corp.com"),
                false,
                allow(0),
            ),
            (
                "a rule holding star and an exact name loses other names to a labeled wildcard",
                vec![
                    http(&["*", "api.example-corp.com"]),
                    http(&["*.example-corp.com"]),
                ],
                host("admin.example-corp.com"),
                false,
                allow(1),
            ),
            (
                "empty rule matches nothing",
                vec![pb::EgressRule::default(), http(&["example.com"])],
                host("example.com"),
                false,
                allow(1),
            ),
        ];
        for (name, rules, dest, decrypted, want) in cases {
            let got = compile_ok(rules).evaluate_request(&dest, decrypted);
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn sni_rules_cases() {
        let mitm = |p: &[&str]| -> Vec<SniRule> {
            p.iter()
                .map(|p| SniRule {
                    pattern: p.to_string(),
                    mode: SniMode::Mitm,
                })
                .collect()
        };
        let pass = |p: &[&str]| -> Vec<SniRule> {
            p.iter()
                .map(|p| SniRule {
                    pattern: p.to_string(),
                    mode: SniMode::Passthrough,
                })
                .collect()
        };
        let r = |p: &str, mode| SniRule {
            pattern: p.into(),
            mode,
        };
        use SniMode::{Mitm as M, Passthrough as P};
        let cases: Vec<(&str, Vec<pb::EgressRule>, u16, Vec<SniRule>)> = vec![
            ("no rules", vec![], 443, vec![]),
            (
                "http rules decide requests, not connections",
                vec![http(&["api.example.com"])],
                80,
                vec![],
            ),
            (
                "tls_passthrough on a named port",
                vec![passthrough(ports(&[443]), &["tls.example.com", "*"])],
                443,
                pass(&["tls.example.com", "*"]),
            ),
            (
                "tls_passthrough on all ports",
                vec![passthrough(all_ports(), &["tls.example.com"])],
                8443,
                pass(&["tls.example.com"]),
            ),
            (
                "https on its default port",
                vec![https(&["api.example.com", "*.example.org"])],
                443,
                mitm(&["api.example.com", "*.example.org"]),
            ),
            (
                "https default port covers no other",
                vec![https(&["api.example.com"])],
                8443,
                vec![],
            ),
            (
                "https on a named port",
                vec![https_on(ports(&[8443, 9443]), &["api.example.com"])],
                9443,
                mitm(&["api.example.com"]),
            ),
            (
                "https on all ports",
                vec![https_on(all_ports(), &["api.example.com"])],
                12345,
                mitm(&["api.example.com"]),
            ),
            (
                "https and tls_passthrough rules for the dialed port",
                vec![
                    https(&["api.example.com"]),
                    https_on(ports(&[8443]), &["alt.example.com"]),
                    http(&["plain.example.com"]),
                    passthrough(ports(&[443]), &["pinned.example.com"]),
                    passthrough(ports(&[8443]), &["other-pinned.example.com"]),
                ],
                443,
                vec![r("api.example.com", M), r("pinned.example.com", P)],
            ),
            (
                "most specific first across https and tls_passthrough",
                vec![
                    https(&["*"]),
                    passthrough(all_ports(), &["a.example.com"]),
                    https(&["*.example.com"]),
                    https(&["b.example.com"]),
                    passthrough(ports(&[443]), &["*.example.org"]),
                    https_on(all_ports(), &["*.example.net"]),
                ],
                443,
                vec![
                    r("b.example.com", M),
                    r("a.example.com", P),
                    r("*.example.com", M),
                    r("*.example.org", P),
                    r("*.example.net", M),
                    r("*", M),
                ],
            ),
            (
                "patterns of one rule are split by rank",
                vec![
                    https(&["*", "api.example-corp.com"]),
                    https(&["*.example-corp.com"]),
                ],
                443,
                mitm(&["api.example-corp.com", "*.example-corp.com", "*"]),
            ),
            (
                "ties keep policy order",
                vec![
                    https(&["b.example.com", "a.example.com"]),
                    passthrough(ports(&[443]), &["c.example.com"]),
                ],
                443,
                vec![
                    r("b.example.com", M),
                    r("a.example.com", M),
                    r("c.example.com", P),
                ],
            ),
            (
                "invalid patterns dropped",
                vec![https(&["good.example.com", "not a hostname"])],
                443,
                mitm(&["good.example.com"]),
            ),
        ];
        for (name, rules, port, want) in cases {
            let (p, _) = Policy::compile_rules(&rules);
            assert_eq!(p.sni_rules(port), want, "{name}");
        }
    }
}
