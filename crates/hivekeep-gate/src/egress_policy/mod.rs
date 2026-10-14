//! Egress policy evaluation shared by the egress gateway and the sandbox
//! DNS relay: destinations parsed from authorities, and compiled
//! [`EgressPolicy`](hivekeep_proto::api::v1::EgressPolicy) rules ranked the
//! way the API documents.

mod authority;
mod policy;

pub use authority::{normalize_authority, Destination, InvalidAuthority};
pub use hivekeep_core::hostname::{normalize_hostname, HostnamePattern};
pub use policy::{CompileError, Decision, Policy, SniMode, SniRule};

/// The `--dns-egress-policy` mode enforcing an Actor's egress policy on DNS
/// queries in the worker's DNS relay.
pub const DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL: &str = "enforce-in-tunnel";

/// An unsupported `--dns-egress-policy` value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DnsEgressPolicyError {
    /// The flag was not set.
    #[error("--dns-egress-policy is required")]
    Missing,
    /// The flag names an unknown mode.
    #[error(
        "unsupported --dns-egress-policy {0:?} (must be {DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL:?})"
    )]
    Unsupported(String),
}

/// Checks a `--dns-egress-policy` flag value.
pub fn validate_dns_egress_policy(mode: &str) -> Result<(), DnsEgressPolicyError> {
    match mode {
        "" => Err(DnsEgressPolicyError::Missing),
        DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL => Ok(()),
        other => Err(DnsEgressPolicyError::Unsupported(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_dns_egress_policy_modes() {
        assert!(validate_dns_egress_policy(DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL).is_ok());
        assert_eq!(
            validate_dns_egress_policy(""),
            Err(DnsEgressPolicyError::Missing)
        );
        assert!(validate_dns_egress_policy("unknown").is_err());
    }

    #[test]
    fn parse_hostname_pattern_round_trips_and_rejects() {
        let valid = [
            "example.com",
            "api.example.com",
            "*.example.com",
            "*.com",
            "*",
            "a-b.example.com",
            "1.example.com",
            "xn--bcher-kva.example",
        ];
        for raw in valid {
            let p = HostnamePattern::parse(raw).unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(p.to_string(), raw);
        }
        let invalid = [
            "",
            "*.",
            "**",
            "API.EXAMPLE.COM",
            "example.com.",
            "192.0.2.1",
            "01.2.3.4",
            "example.123",
            "2001:db8::1",
            "example.com:443",
            "https://example.com",
            "api.*.example.com",
            "**.example.com",
            "*example.com",
            "foo..example.com",
            "-a.example.com",
            "bücher.example",
        ];
        for raw in invalid {
            assert!(
                HostnamePattern::parse(raw).is_err(),
                "{raw} should be rejected"
            );
        }
    }

    #[test]
    fn hostname_pattern_matches() {
        let cases = [
            ("example.com", "example.com", true),
            ("example.com", "api.example.com", false),
            ("example.com", "example.org", false),
            ("example.com", "notexample.com", false),
            ("*.example.com", "api.example.com", true),
            ("*.example.com", "example.com", false),
            ("*.example.com", "a.b.example.com", false),
            ("*.example.com", "notexample.com", false),
            ("*.example.com", ".example.com", false),
            ("*.com", "example.com", true),
            ("*.com", "com", false),
            ("*", "example.com", true),
            ("*", "a.b.example.com", true),
            ("*", "", false),
        ];
        for (pattern, host, want) in cases {
            let p = HostnamePattern::parse(pattern).unwrap();
            assert_eq!(p.matches(host), want, "{pattern:?}.matches({host:?})");
        }
    }

    #[test]
    fn normalize_hostname_cases() {
        let cases: [(&str, Option<&str>); 13] = [
            ("example.com", Some("example.com")),
            ("example.com.", Some("example.com")),
            ("API.Example.COM.", Some("api.example.com")),
            ("xn--bcher-kva.example.", Some("xn--bcher-kva.example")),
            ("", None),
            (".", None),
            ("example.com..", None),
            ("192.0.2.1", None),
            ("192.0.2.1.", None),
            ("01.2.3.4.", None),
            ("_dmarc.example.com.", None),
            ("bücher.example.", None),
            ("*.example.com", None),
        ];
        for (raw, want) in cases {
            assert_eq!(normalize_hostname(raw).as_deref(), want, "{raw:?}");
        }
    }
}
