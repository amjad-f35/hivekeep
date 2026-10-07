//! Hostname patterns used by egress rules.
//!
//! The API validates patterns with the same parser the egress gateway
//! matches with, so what is accepted and what can be evaluated never drift.

use std::fmt;

use crate::names::is_dns_subdomain;

/// A parsed hostname pattern: an exact name, `*.<name>` matching exactly one
/// extra leftmost label, or `*` matching every name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HostnamePattern {
    /// Matches every hostname.
    Any,
    /// Matches exactly this hostname.
    Exact(String),
    /// Matches `<label>.<suffix>` for exactly one non-empty label.
    Wildcard(String),
}

/// A string that is not a hostname pattern.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not a valid hostname pattern")]
pub struct InvalidHostnamePattern(pub String);

/// Whether `name` is a lowercase DNS-1123 subdomain whose last label is not
/// all digits, which keeps dotted-decimal addresses (in any spelling) from
/// passing as names.
pub fn is_hostname(name: &str) -> bool {
    if !is_dns_subdomain(name) {
        return false;
    }
    let last = name.rsplit('.').next().unwrap_or("");
    !last.bytes().all(|b| b.is_ascii_digit())
}

/// Lowercases ASCII, strips one trailing dot and checks the result is a
/// hostname, giving the form [`HostnamePattern::matches`] expects.
pub fn normalize_hostname(raw: &str) -> Option<String> {
    let name = raw.strip_suffix('.').unwrap_or(raw).to_ascii_lowercase();
    is_hostname(&name).then_some(name)
}

impl HostnamePattern {
    /// Parses a host or SNI pattern. IP literals, ports, URLs, trailing dots
    /// and any other placement of `*` are rejected.
    pub fn parse(raw: &str) -> Result<Self, InvalidHostnamePattern> {
        if raw == "*" {
            return Ok(HostnamePattern::Any);
        }
        let (name, wildcard) = match raw.strip_prefix("*.") {
            Some(rest) => (rest, true),
            None => (raw, false),
        };
        if !is_hostname(name) {
            return Err(InvalidHostnamePattern(raw.to_string()));
        }
        Ok(if wildcard {
            HostnamePattern::Wildcard(name.to_string())
        } else {
            HostnamePattern::Exact(name.to_string())
        })
    }

    /// Whether a normalized hostname matches. `*.example.com` matches
    /// `api.example.com` but neither `example.com` nor `a.b.example.com`.
    pub fn matches(&self, hostname: &str) -> bool {
        if hostname.is_empty() {
            return false;
        }
        match self {
            HostnamePattern::Any => true,
            HostnamePattern::Exact(n) => hostname == n,
            HostnamePattern::Wildcard(suffix) => hostname
                .strip_suffix(suffix.as_str())
                .and_then(|h| h.strip_suffix('.'))
                .is_some_and(|label| !label.is_empty() && !label.contains('.')),
        }
    }
}

impl fmt::Display for HostnamePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostnamePattern::Any => f.write_str("*"),
            HostnamePattern::Exact(n) => f.write_str(n),
            HostnamePattern::Wildcard(n) => write!(f, "*.{n}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trips() {
        for raw in [
            "example.com",
            "api.example.com",
            "*.example.com",
            "*.com",
            "*",
            "a-b.example.com",
            "1.example.com",
            "xn--bcher-kva.example",
        ] {
            assert_eq!(HostnamePattern::parse(raw).unwrap().to_string(), raw);
        }
        for raw in [
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
        ] {
            assert!(HostnamePattern::parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn matching() {
        let w = HostnamePattern::parse("*.example.com").unwrap();
        assert!(w.matches("api.example.com"));
        assert!(
            !w.matches("example.com")
                && !w.matches("a.b.example.com")
                && !w.matches("xexample.com")
        );
        assert!(HostnamePattern::Any.matches("x.y") && !HostnamePattern::Any.matches(""));
        assert!(HostnamePattern::parse("a.io").unwrap().matches("a.io"));
        assert_eq!(
            normalize_hostname("API.Example.COM."),
            Some("api.example.com".into())
        );
        assert_eq!(normalize_hostname("10.0.0.1"), None);
    }
}
