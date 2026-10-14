//! Decides DNS queries against an Actor's egress policy: a name resolves
//! only when some rule's hostname pattern matches it, regardless of the
//! rule's protocol or ports.

use std::collections::HashSet;

use hivekeep_core::hostname::{normalize_hostname, HostnamePattern};
use hivekeep_proto::keeper::v1 as pb;

use super::protocol::Question;

/// Hostname patterns of a policy, deduplicated, invalid ones dropped.
#[derive(Debug, Clone, Default)]
pub struct Evaluator {
    patterns: Vec<HostnamePattern>,
}

impl Evaluator {
    /// Collects the patterns of every http, https and tls_passthrough rule.
    /// A missing policy allows nothing.
    pub fn new(policy: Option<&pb::EgressPolicy>) -> Self {
        let mut seen = HashSet::new();
        let mut patterns = Vec::new();
        for rule in policy.map(|p| p.rules.as_slice()).unwrap_or_default() {
            let Some(dest) = rule
                .http
                .as_ref()
                .or(rule.https.as_ref())
                .or(rule.tls_passthrough.as_ref())
            else {
                continue;
            };
            for raw in &dest.hostnames {
                if seen.contains(raw.as_str()) {
                    continue;
                }
                if let Ok(p) = HostnamePattern::parse(raw) {
                    seen.insert(raw.as_str());
                    patterns.push(p);
                }
            }
        }
        Evaluator { patterns }
    }

    /// Whether the query name (presentation form, trailing dot optional)
    /// may be resolved.
    pub fn allows_hostname(&self, qname: &str) -> bool {
        if self.patterns.is_empty() {
            return false;
        }
        let Some(host) = normalize_hostname(qname) else {
            return false;
        };
        self.patterns.iter().any(|p| p.matches(&host))
    }
}

/// Whether `policy` allows the question's name.
pub fn allow_query(policy: Option<&pb::EgressPolicy>, q: &Question) -> bool {
    Evaluator::new(policy).allows_hostname(&q.name_str())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::tunnel::dns::protocol::{rtype, CLASS_INET};

    pub(crate) fn dests(hosts: &[&str]) -> Option<pb::EgressDestinations> {
        Some(pb::EgressDestinations {
            hostnames: hosts.iter().map(|s| s.to_string()).collect(),
            ports: None,
        })
    }

    pub(crate) fn http_policy(hosts: &[&str]) -> pb::EgressPolicy {
        pb::EgressPolicy {
            rules: vec![pb::EgressRule {
                http: dests(hosts),
                ..Default::default()
            }],
        }
    }

    fn https_policy(hosts: &[&str]) -> pb::EgressPolicy {
        pb::EgressPolicy {
            rules: vec![pb::EgressRule {
                https: dests(hosts),
                ..Default::default()
            }],
        }
    }

    #[test]
    fn evaluator_cases() {
        assert!(!Evaluator::default().allows_hostname("example.com."));
        let cases: Vec<(&str, Option<pb::EgressPolicy>, Vec<&str>, Vec<&str>)> = vec![
            (
                "nil policy",
                None,
                vec![],
                vec!["example.com.", "api.example.com.", "."],
            ),
            (
                "empty policy",
                Some(pb::EgressPolicy::default()),
                vec![],
                vec!["example.com.", "api.example.com.", "."],
            ),
            (
                "rule with no protocol",
                Some(pb::EgressPolicy {
                    rules: vec![pb::EgressRule::default()],
                }),
                vec![],
                vec!["example.com.", "api.example.com."],
            ),
            (
                "exact host across http, https, and tls_passthrough",
                Some(pb::EgressPolicy {
                    rules: vec![
                        pb::EgressRule {
                            http: dests(&["http.example.com"]),
                            ..Default::default()
                        },
                        pb::EgressRule {
                            https: dests(&["https.example.com"]),
                            ..Default::default()
                        },
                        pb::EgressRule {
                            tls_passthrough: dests(&["tls.example.com"]),
                            ..Default::default()
                        },
                    ],
                }),
                vec![
                    "http.example.com.",
                    "http.example.com",
                    "HTTP.Example.COM.",
                    "https.example.com.",
                    "tls.example.com.",
                ],
                vec![
                    "example.com.",
                    "sub.http.example.com.",
                    "other.example.com.",
                    "http.example.com..",
                    ".",
                    "",
                ],
            ),
            (
                "leftmost-label wildcard",
                Some(https_policy(&["*.example.com"])),
                vec![
                    "api.example.com.",
                    "API.Example.COM.",
                    "a-b.example.com.",
                    "1.example.com.",
                ],
                vec![
                    "example.com.",
                    "a.b.example.com.",
                    "notexample.com.",
                    ".example.com.",
                    "_dmarc.example.com.",
                    "-bad.example.com.",
                ],
            ),
            (
                "star wildcard",
                Some(http_policy(&["*"])),
                vec![
                    "example.com.",
                    "api.example.com.",
                    "a.b.c.example.org.",
                    "xn--bcher-kva.example.",
                ],
                vec![
                    "",
                    ".",
                    "example.com..",
                    "192.0.2.1.",
                    "01.2.3.4.",
                    "_dmarc.example.com.",
                    "bücher.example.",
                ],
            ),
            (
                "invalid patterns are ignored while valid ones still match",
                Some(http_policy(&[
                    "BAD.EXAMPLE.COM",
                    "*.",
                    "https://bad.example.com",
                    "good.example.com",
                    "*.wild.example.com",
                ])),
                vec!["good.example.com.", "a.wild.example.com."],
                vec!["bad.example.com.", "wild.example.com."],
            ),
        ];
        for (name, policy, allowed, blocked) in cases {
            let e = Evaluator::new(policy.as_ref());
            for q in allowed {
                assert!(e.allows_hostname(q), "{name}: {q:?} should be allowed");
            }
            for q in blocked {
                assert!(!e.allows_hostname(q), "{name}: {q:?} should be blocked");
            }
        }
    }

    #[test]
    fn allow_query_cases() {
        let q = |n: &str| Question::new(n, rtype::A, CLASS_INET);
        let example = q("example.com.");
        let cases: Vec<(&str, Option<pb::EgressPolicy>, Question, bool)> = vec![
            ("no policy", None, example.clone(), false),
            (
                "policy without rules",
                Some(pb::EgressPolicy::default()),
                example.clone(),
                false,
            ),
            (
                "matching exact host",
                Some(https_policy(&["example.com"])),
                example.clone(),
                true,
            ),
            (
                "unlisted host",
                Some(https_policy(&["example.com"])),
                q("other.example.com."),
                false,
            ),
            (
                "matching wildcard host",
                Some(https_policy(&["*.example.com"])),
                q("api.example.com."),
                true,
            ),
            (
                "wildcard does not match apex",
                Some(https_policy(&["*.example.com"])),
                example.clone(),
                false,
            ),
            (
                "wildcard does not match nested subdomain",
                Some(https_policy(&["*.example.com"])),
                q("a.b.example.com."),
                false,
            ),
        ];
        for (name, policy, question, want) in cases {
            assert_eq!(allow_query(policy.as_ref(), &question), want, "{name}");
        }
    }
}
