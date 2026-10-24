//! Lenient URL splitting and `host:port` validation.
//!
//! The instructions name origins the way a person would type them, and the
//! demo forwards them as written; this module only pulls out the parts it
//! needs (scheme, authority, hostname, port, request target) and rejects the
//! shapes that cannot work. Unlike a WHATWG parser it does not repair input:
//! `https:///path` has an empty host and is refused rather than read as host
//! `path`.

/// The pieces of a URL the endpoints use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UrlParts {
    /// Lower-cased scheme, empty when there is none.
    pub scheme: String,
    /// Authority without user info: `host[:port]`, brackets kept.
    pub host: String,
    /// Host name or address, brackets removed.
    pub hostname: String,
    /// Port as written, empty when absent.
    pub port: String,
    /// Path and query for a request line; `/` when the path is empty.
    pub request_uri: String,
}

impl UrlParts {
    /// `host:port` to dial, filling in `default_port` when none was written.
    pub fn dial_address(&self, default_port: &str) -> String {
        if self.port.is_empty() {
            join_host_port(&self.hostname, default_port)
        } else {
            self.host.clone()
        }
    }
}

fn valid_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// Splits `raw` into [`UrlParts`].
pub fn parse_url(raw: &str) -> Result<UrlParts, String> {
    if raw.chars().any(|c| c.is_ascii_control()) {
        return Err(format!("parse {raw:?}: invalid control character in URL"));
    }
    let (scheme, rest) = match raw.find(':') {
        Some(0) => return Err(format!("parse {raw:?}: missing protocol scheme")),
        Some(i) if valid_scheme(&raw[..i]) => (raw[..i].to_ascii_lowercase(), &raw[i + 1..]),
        _ => (String::new(), raw),
    };
    // Drop the fragment; it never goes on the wire.
    let rest = rest.split('#').next().unwrap_or_default();
    let (authority, path_and_query) = match rest.strip_prefix("//") {
        Some(after) => {
            let end = after.find(['/', '?']).unwrap_or(after.len());
            (&after[..end], &after[end..])
        }
        None => ("", rest),
    };
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, h)| h)
        .to_string();
    let (hostname, port) =
        split_url_host(&host).ok_or_else(|| format!("parse {raw:?}: invalid host {host:?}"))?;
    if !port.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "parse {raw:?}: invalid port {:?} after host",
            format!(":{port}")
        ));
    }
    let request_uri = if path_and_query.is_empty() || path_and_query.starts_with('?') {
        format!("/{path_and_query}")
    } else {
        path_and_query.to_string()
    };
    Ok(UrlParts {
        scheme,
        host,
        hostname,
        port,
        request_uri,
    })
}

/// Splits a URL authority into hostname and (possibly empty) port.
fn split_url_host(host: &str) -> Option<(String, String)> {
    if let Some(inner) = host.strip_prefix('[') {
        let close = inner.find(']')?;
        let after = &inner[close + 1..];
        let port = match after {
            "" => "",
            _ => after.strip_prefix(':')?,
        };
        return Some((inner[..close].to_string(), port.to_string()));
    }
    match host.rsplit_once(':') {
        Some((h, p)) => Some((h.to_string(), p.to_string())),
        None => Some((host.to_string(), String::new())),
    }
}

/// Checks that `raw` is an absolute http(s) URL with a host name.
pub fn validate_url(raw: &str) -> Result<UrlParts, String> {
    let parts = parse_url(raw).map_err(|e| format!("invalid URL: {e}"))?;
    if parts.scheme != "http" && parts.scheme != "https" {
        return Err("URL scheme must be http or https".into());
    }
    if parts.hostname.is_empty() {
        return Err("URL must include a hostname".into());
    }
    Ok(parts)
}

/// Joins a host and port, bracketing IPv6 literals.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Splits `host:port` (or `[v6]:port`) strictly: the port is mandatory and a
/// bare host with colons is refused.
pub fn split_host_port(raw: &str) -> Result<(String, String), String> {
    let fail = |why: &str| Err(format!("address {raw}: {why}"));
    let Some(colon) = raw.rfind(':') else {
        return fail("missing port in address");
    };
    let (host, port) = if let Some(inner) = raw.strip_prefix('[') {
        let Some(close) = inner.find(']') else {
            return fail("missing ']' in address");
        };
        // The closing bracket must be immediately followed by the last colon.
        if close + 2 != colon {
            return if close + 2 == raw.len() {
                fail("missing port in address")
            } else {
                fail("too many colons in address")
            };
        }
        (&inner[..close], &raw[colon + 1..])
    } else {
        let host = &raw[..colon];
        if host.contains(':') {
            return fail("too many colons in address");
        }
        (host, &raw[colon + 1..])
    };
    if host.contains('[') || port.contains('[') {
        return fail("unexpected '[' in address");
    }
    if host.contains(']') || port.contains(']') {
        return fail("unexpected ']' in address");
    }
    Ok((host.to_string(), port.to_string()))
}

/// Checks that `raw` is `host:port` with a non-empty host and a port in
/// 1..=65535 — the form a gRPC dial, a forward proxy and a CONNECT
/// authority all take. `what` names the field in the error.
pub fn validate_target(raw: &str, what: &str) -> Result<(), String> {
    let (host, port) =
        split_host_port(raw).map_err(|e| format!("{what} must be host:port: {e}"))?;
    if host.is_empty() {
        return Err(format!("{what} must include a host"));
    }
    match port.parse::<u32>() {
        Ok(n) if (1..=65535).contains(&n) && port.chars().all(|c| c.is_ascii_digit()) => Ok(()),
        _ => Err(format!(
            "{what} port must be a number between 1 and 65535, got {port:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_parts() {
        let p = parse_url("HTTP://user@Example.com:8080/a/b?x=1#frag").unwrap();
        assert_eq!(p.scheme, "http");
        assert_eq!(p.host, "Example.com:8080");
        assert_eq!(p.hostname, "Example.com");
        assert_eq!(p.port, "8080");
        assert_eq!(p.request_uri, "/a/b?x=1");
        assert_eq!(p.dial_address("80"), "Example.com:8080");

        let p = parse_url("https://[::1]/").unwrap();
        assert_eq!((p.hostname.as_str(), p.port.as_str()), ("::1", ""));
        assert_eq!(p.dial_address("443"), "[::1]:443");

        assert_eq!(parse_url("http://h?q").unwrap().request_uri, "/?q");
        assert!(parse_url("http://h:abc/").is_err());
        assert!(parse_url("http://h\n/").is_err());
    }

    #[test]
    fn validate_url_cases() {
        assert!(validate_url("https://allowed.example/").is_ok());
        assert_eq!(
            validate_url("https:///path").unwrap_err(),
            "URL must include a hostname"
        );
        assert_eq!(
            validate_url("file:///etc/passwd").unwrap_err(),
            "URL scheme must be http or https"
        );
        assert_eq!(
            validate_url("example.com").unwrap_err(),
            "URL scheme must be http or https"
        );
    }

    #[test]
    fn validate_target_cases() {
        assert!(validate_target("grpcecho:50051", "target").is_ok());
        assert!(validate_target("[::1]:50051", "target").is_ok());
        for (raw, want) in [
            ("", "target must be host:port"),
            ("grpcecho", "target must be host:port"),
            (":50051", "target must include a host"),
            ("grpcecho:grpc", "target port must be a number"),
            ("grpcecho:0", "target port must be a number"),
            ("grpcecho:70000", "target port must be a number"),
            ("http://grpcecho:50051", "target must be host:port"),
        ] {
            let err = validate_target(raw, "target").unwrap_err();
            assert!(err.starts_with(want), "{raw:?}: {err}");
        }
    }
}
