//! Filter chain selection for one tunnelled connection.

use crate::policy::{EgressPolicy, SniMode};
use crate::store::PolicyStore;

/// Name of the filter chain a connection goes to. These are the keys of
/// the inner listener's filter_chain_matcher in gate-egress.yaml.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chain {
    /// TLS an SNI rule says to terminate.
    Mitm,
    /// TLS an SNI rule says to relay.
    Passthrough,
    /// Not TLS; the cleartext chain applies its own HTTP policy.
    Cleartext,
    /// Nothing allows it. No chain has this name, so Envoy closes the
    /// connection.
    Denied,
}

impl Chain {
    /// The filter-state value for this chain.
    pub fn as_str(self) -> &'static str {
        match self {
            Chain::Mitm => "mitm",
            Chain::Passthrough => "passthrough",
            Chain::Cleartext => "cleartext",
            Chain::Denied => "denied",
        }
    }
}

/// Whether `pattern` matches an already normalized `hostname`. `*` matches
/// any non-empty name, `*.suffix` exactly one non-empty label in front of
/// `suffix`, anything else only itself. This must agree with the gate's own
/// pattern matcher, which validates the rules it sends.
pub fn pattern_matches(pattern: &str, hostname: &str) -> bool {
    if hostname.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return hostname
            .strip_suffix(suffix)
            .and_then(|head| head.strip_suffix('.'))
            .is_some_and(|label| !label.is_empty() && !label.contains('.'));
    }
    pattern == hostname
}

/// ASCII-lowercases an SNI and drops a single trailing dot. Only ASCII is
/// folded, so no Unicode case mapping can turn one name into another.
pub fn normalize_sni(sni: &str) -> String {
    let mut s = sni.to_ascii_lowercase();
    if s.ends_with('.') {
        s.pop();
    }
    s
}

/// The chain decided by the detected transport alone, or `None` for TLS,
/// which the SNI decides. No transport (no inspector claimed the bytes) and
/// raw_buffer are cleartext; anything else is denied.
pub fn transport_verdict(transport: Option<&str>) -> Option<Chain> {
    match transport {
        Some("tls") => None,
        None | Some("raw_buffer") => Some(Chain::Cleartext),
        Some(_) => Some(Chain::Denied),
    }
}

/// The chain for TLS with this SNI: the mode of the first matching rule.
/// Missing policy, missing SNI, no match, or a match on an unknown mode all
/// deny.
pub fn tls_verdict(policy: Option<&EgressPolicy>, sni: Option<&str>) -> Chain {
    let (Some(policy), Some(sni)) = (policy, sni) else {
        return Chain::Denied;
    };
    let host = normalize_sni(sni);
    match policy.rules.iter().find(|r| pattern_matches(&r.pattern, &host)).map(|r| r.mode) {
        Some(SniMode::Mitm) => Chain::Mitm,
        Some(SniMode::Passthrough) => Chain::Passthrough,
        Some(SniMode::Unknown) | None => Chain::Denied,
    }
}

/// Parses the shared policy-hash filter state (a decimal u64).
pub fn parse_hash(state: &[u8]) -> Option<u64> {
    std::str::from_utf8(state).ok()?.parse().ok()
}

/// The chain for TLS given the SNI and the shared policy-hash filter state,
/// looking the policy up in this thread's store.
pub fn stored_policy_verdict(sni: Option<&str>, hash_state: Option<&[u8]>) -> Chain {
    let policy = hash_state.and_then(parse_hash).and_then(|h| PolicyStore::with_local(|s| s.lookup(h)));
    tls_verdict(policy.as_deref(), sni)
}

/// The full decision the inner listener makes: transport first, then for
/// TLS the SNI against the policy the hash state names. The `sni` and
/// `hash_state` closures are only called for TLS.
pub fn connection_verdict(
    transport: Option<&str>,
    sni: impl FnOnce() -> Option<String>,
    hash_state: impl FnOnce() -> Option<Vec<u8>>,
) -> Chain {
    match transport_verdict(transport) {
        Some(chain) => chain,
        None => stored_policy_verdict(sni().as_deref(), hash_state().as_deref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::SniRule;
    use crate::store::Admission;
    use std::rc::Rc;

    fn policy(rules: &[(&str, SniMode)]) -> EgressPolicy {
        EgressPolicy::new(rules.iter().map(|(p, m)| SniRule { pattern: (*p).into(), mode: *m }).collect())
    }

    #[test]
    fn pattern_matching_table() {
        let cases: &[(&str, &str, bool)] = &[
            ("*", "anything.example.com", true),
            ("*", "localhost", true),
            ("*", "", false),
            ("*.example.com", "api.example.com", true),
            ("*.example.com", "example.com", false),
            ("*.example.com", "a.b.example.com", false),
            ("*.example.com", ".example.com", false),
            ("*.example.com", "xexample.com", false),
            ("*.example.com", "api.example.co", false),
            ("*.example.com", "api.example.com.evil", false),
            ("api.example.com", "api.example.com", true),
            ("api.example.com", "www.api.example.com", false),
            ("api.example.com", "api.example.co", false),
            ("example.com", "api.example.com", false),
            // The caller normalizes; the matcher does not.
            ("api.example.com", "API.example.com", false),
            ("api.example.com", "api.example.com.", false),
        ];
        for (pattern, host, want) in cases {
            assert_eq!(pattern_matches(pattern, host), *want, "{pattern:?} vs {host:?}");
        }
    }

    #[test]
    fn normalization() {
        assert_eq!(normalize_sni("API.Example.COM."), "api.example.com");
        assert_eq!(normalize_sni("api.example.com"), "api.example.com");
        assert_eq!(normalize_sni("api.example.com.."), "api.example.com.");
        // U+212A KELVIN SIGN must not fold to "k".
        assert_eq!(normalize_sni("\u{212A}.example.com"), "\u{212A}.example.com");
    }

    #[test]
    fn transport_decides_non_tls() {
        assert_eq!(transport_verdict(Some("tls")), None);
        assert_eq!(transport_verdict(None), Some(Chain::Cleartext));
        assert_eq!(transport_verdict(Some("raw_buffer")), Some(Chain::Cleartext));
        assert_eq!(transport_verdict(Some("quic")), Some(Chain::Denied));
        assert_eq!(transport_verdict(Some("")), Some(Chain::Denied));
    }

    #[test]
    fn tls_without_inputs_is_denied() {
        let any = policy(&[("*", SniMode::Mitm)]);
        assert_eq!(tls_verdict(None, Some("api.example.com")), Chain::Denied);
        assert_eq!(tls_verdict(Some(&any), None), Chain::Denied);
        assert_eq!(tls_verdict(Some(&any), Some("")), Chain::Denied);
        assert_eq!(tls_verdict(Some(&policy(&[])), Some("api.example.com")), Chain::Denied);
    }

    #[test]
    fn tls_matches_exact_and_wildcard_rules() {
        let p = policy(&[("api.example.com", SniMode::Mitm), ("*.example.org", SniMode::Mitm)]);
        assert_eq!(tls_verdict(Some(&p), Some("api.example.com")), Chain::Mitm);
        assert_eq!(tls_verdict(Some(&p), Some("API.EXAMPLE.COM.")), Chain::Mitm);
        assert_eq!(tls_verdict(Some(&p), Some("www.example.org")), Chain::Mitm);
        assert_eq!(tls_verdict(Some(&p), Some("example.org")), Chain::Denied);
        assert_eq!(tls_verdict(Some(&p), Some("a.b.example.org")), Chain::Denied);
        assert_eq!(tls_verdict(Some(&p), Some("www.example.com")), Chain::Denied);
        let any = policy(&[("*", SniMode::Mitm)]);
        assert_eq!(tls_verdict(Some(&any), Some("whatever.test")), Chain::Mitm);
    }

    #[test]
    fn the_first_matching_rule_is_final() {
        let unknown_first = policy(&[("*.example.com", SniMode::Unknown), ("api.example.com", SniMode::Mitm)]);
        assert_eq!(tls_verdict(Some(&unknown_first), Some("api.example.com")), Chain::Denied);
        let known_first = policy(&[
            ("api.example.com", SniMode::Mitm),
            ("pinned.example.com", SniMode::Passthrough),
            ("*.example.com", SniMode::Unknown),
        ]);
        assert_eq!(tls_verdict(Some(&known_first), Some("api.example.com")), Chain::Mitm);
        assert_eq!(tls_verdict(Some(&known_first), Some("pinned.example.com")), Chain::Passthrough);
        assert_eq!(tls_verdict(Some(&known_first), Some("www.example.com")), Chain::Denied);
    }

    #[test]
    fn hash_state_parsing() {
        assert_eq!(parse_hash(b"42"), Some(42));
        assert_eq!(parse_hash(b"not-a-hash"), None);
        assert_eq!(parse_hash(b""), None);
        assert_eq!(parse_hash(b"18446744073709551616"), None);
    }

    fn intern(p: EgressPolicy) -> (Rc<EgressPolicy>, Vec<u8>) {
        let rc = match PolicyStore::with_local(|s| s.admit(Rc::new(p))) {
            Admission::Stored(rc) | Admission::Shared(rc) => rc,
            Admission::Collision => panic!("collision"),
        };
        let state = rc.hash.to_string().into_bytes();
        (rc, state)
    }

    #[test]
    fn connection_verdicts() {
        let rules = || policy(&[("api.mail.test", SniMode::Mitm), ("*.mail.test", SniMode::Passthrough)]);
        let (_held, state) = intern(rules());
        let tls = |sni: Option<&str>, st: Option<Vec<u8>>| {
            connection_verdict(Some("tls"), || sni.map(String::from), || st)
        };
        assert_eq!(tls(Some("api.mail.test"), Some(state.clone())), Chain::Mitm);
        assert_eq!(tls(Some("inbox.mail.test"), Some(state.clone())), Chain::Passthrough);
        assert_eq!(tls(Some("API.Mail.TEST"), Some(state.clone())), Chain::Mitm);
        assert_eq!(tls(Some("mail.test"), Some(state.clone())), Chain::Denied);
        assert_eq!(tls(None, Some(state.clone())), Chain::Denied);
        assert_eq!(tls(Some("api.mail.test"), None), Chain::Denied);
        assert_eq!(tls(Some("api.mail.test"), Some(b"not-a-hash".to_vec())), Chain::Denied);
        assert_eq!(tls(Some("api.mail.test"), Some(b"9999999999999999999".to_vec())), Chain::Denied);
        let (_none, empty) = intern(policy(&[]));
        assert_eq!(tls(Some("api.mail.test"), Some(empty)), Chain::Denied);

        // Non-TLS never reads the SNI or the policy.
        assert_eq!(connection_verdict(None, || panic!("sni read"), || panic!("state read")), Chain::Cleartext);
        assert_eq!(connection_verdict(Some("raw_buffer"), || panic!(), || panic!()), Chain::Cleartext);
        assert_eq!(connection_verdict(Some("quic"), || panic!(), || panic!()), Chain::Denied);
    }
}
