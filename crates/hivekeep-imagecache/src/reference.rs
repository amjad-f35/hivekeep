//! Image references as registries and container runtimes normalize them:
//! a bare `ubuntu` is `index.docker.io/library/ubuntu:latest`, a first path
//! component with a dot, a colon or `localhost` names the registry.
//!
//! Local registries (localhost, loopback addresses) are pulled over plain
//! HTTP, and can be rewritten to a replacement endpoint, matching the mirror
//! config local Kubernetes-in-Docker registries use.

use std::fmt;
use std::net::IpAddr;

use crate::digest::Digest;
use crate::error::Error;

/// The registry a reference without one resolves to.
pub const DEFAULT_REGISTRY: &str = "index.docker.io";
/// The tag a reference without a tag or digest resolves to.
pub const DEFAULT_TAG: &str = "latest";

/// How a reference pins its image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pin {
    /// A mutable tag.
    Tag(String),
    /// An immutable manifest digest.
    Digest(Digest),
}

/// A parsed, normalized image reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    /// Registry host and optional port, e.g. `index.docker.io` or
    /// `127.0.0.1:5000`.
    pub registry: String,
    /// Repository path within the registry, e.g. `library/ubuntu`.
    pub repository: String,
    /// The tag or digest.
    pub pin: Pin,
    /// Whether the registry is reached over plain HTTP.
    pub insecure: bool,
}

impl Reference {
    /// Parses `s`. `insecure` forces plain HTTP; loopback registries use it
    /// regardless.
    pub fn parse(s: &str, insecure: bool) -> Result<Self, Error> {
        let bad = || Error::msg(format!("could not parse reference: {s}"));
        if s.is_empty() || s.chars().any(|c| c.is_whitespace()) {
            return Err(bad());
        }
        let (name, digest) = match s.split_once('@') {
            Some((n, d)) => (
                n,
                Some(
                    Digest::parse(d)
                        .map_err(|e| e.context(format!("could not parse reference: {s}")))?,
                ),
            ),
            None => (s, None),
        };
        // A tag is a colon after the last slash.
        let (repo_part, tag) = match name.rfind(':') {
            Some(i) if !name[i..].contains('/') => (&name[..i], Some(&name[i + 1..])),
            _ => (name, None),
        };
        let (registry, repository) = split_registry(repo_part);
        if repository.is_empty() || !valid_repository(&repository) || !valid_registry(&registry) {
            return Err(bad());
        }
        let pin = match (digest, tag) {
            (Some(d), _) => Pin::Digest(d),
            (None, Some(t)) => {
                if !valid_tag(t) {
                    return Err(bad());
                }
                Pin::Tag(t.to_string())
            }
            (None, None) => Pin::Tag(DEFAULT_TAG.to_string()),
        };
        let insecure = insecure || is_localhost_or_loopback(&host_of(&registry));
        Ok(Reference {
            registry,
            repository,
            pin,
            insecure,
        })
    }

    /// The repository, `registry/path`: what credentials are looked up by.
    pub fn repository_name(&self) -> String {
        format!("{}/{}", self.registry, self.repository)
    }

    /// The same repository pinned to `digest`.
    pub fn with_digest(&self, digest: Digest) -> Reference {
        Reference {
            pin: Pin::Digest(digest),
            ..self.clone()
        }
    }

    /// The manifest reference string (tag or digest) for the registry API.
    pub fn api_reference(&self) -> String {
        match &self.pin {
            Pin::Tag(t) => t.clone(),
            Pin::Digest(d) => d.to_string(),
        }
    }

    /// The registry's base URL.
    pub fn base_url(&self) -> String {
        let scheme = if self.insecure { "http" } else { "https" };
        // Docker Hub serves its API from a different host than its name.
        let host = if self.registry == DEFAULT_REGISTRY {
            "registry-1.docker.io"
        } else {
            &self.registry
        };
        format!("{scheme}://{host}")
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pin {
            Pin::Tag(t) => write!(f, "{}/{}:{}", self.registry, self.repository, t),
            Pin::Digest(d) => write!(f, "{}/{}@{}", self.registry, self.repository, d),
        }
    }
}

fn split_registry(name: &str) -> (String, String) {
    match name.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            let registry = if first == "docker.io" {
                DEFAULT_REGISTRY
            } else {
                first
            };
            let repo = if registry == DEFAULT_REGISTRY && !rest.contains('/') {
                format!("library/{rest}")
            } else {
                rest.to_string()
            };
            (registry.to_string(), repo)
        }
        _ => {
            let repo = if name.contains('/') {
                name.to_string()
            } else {
                format!("library/{name}")
            };
            (DEFAULT_REGISTRY.to_string(), repo)
        }
    }
}

fn valid_repository(repo: &str) -> bool {
    repo.split('/').all(|c| {
        !c.is_empty()
            && c.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    })
}

fn valid_registry(reg: &str) -> bool {
    !reg.is_empty()
        && reg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(&b))
}

fn valid_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= 128
        && tag.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_alphanumeric() || b == b'_' || (i > 0 && (b == b'.' || b == b'-'))
        })
}

/// `host[:port]` without the port. Bracketed IPv6 literals lose their
/// brackets.
fn host_of(registry: &str) -> String {
    if let Some(rest) = registry.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return rest[..end].to_string();
        }
    }
    match registry.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') && p.bytes().all(|b| b.is_ascii_digit()) => h.to_string(),
        _ => registry.to_string(),
    }
}

/// The registry host of a raw reference string, without port.
pub fn registry_host(reference: &str) -> String {
    let first = reference.split('/').next().unwrap_or_default();
    if first.is_empty() {
        return DEFAULT_REGISTRY.to_string();
    }
    host_of(first)
}

/// Whether `host` is `localhost` or a loopback address.
pub fn is_localhost_or_loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// Whether `reference` names a localhost or loopback registry (docker
/// permits localhost and 127.0.0.0/8; the IPv6 loopback is allowed too).
pub fn is_local_registry(reference: &str) -> bool {
    is_localhost_or_loopback(&registry_host(reference))
}

/// Rewrites a local-registry `reference` to `replacement`'s registry.
pub fn rewrite_local_registry(reference: &str, replacement: &str) -> String {
    if !is_local_registry(reference) {
        return reference.to_string();
    }
    match reference.split_once('/') {
        Some((_, rest)) => format!("{replacement}/{rest}"),
        None => reference.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_docker_hub() {
        let r = Reference::parse("ubuntu", false).unwrap();
        assert_eq!(r.to_string(), "index.docker.io/library/ubuntu:latest");
        assert!(!r.insecure);
        let r = Reference::parse("docker.io/foo/bar:1", false).unwrap();
        assert_eq!(r.repository_name(), "index.docker.io/foo/bar");
        let r = Reference::parse("127.0.0.1:5000/test/app:v1", false).unwrap();
        assert_eq!(r.registry, "127.0.0.1:5000");
        assert_eq!(r.repository, "test/app");
        assert_eq!(r.pin, Pin::Tag("v1".into()));
        assert!(r.insecure);
        assert_eq!(r.base_url(), "http://127.0.0.1:5000");
        let d = format!("sha256:{}", "a".repeat(64));
        let r = Reference::parse(&format!("gcr.io/p/img:tag@{d}"), false).unwrap();
        assert_eq!(r.pin, Pin::Digest(Digest::parse(&d).unwrap()));
        assert!(Reference::parse("NOT A REFERENCE", false).is_err());
        assert!(Reference::parse("Upper/Case", false).is_err());
        assert!(Reference::parse("", false).is_err());
    }

    #[test]
    fn is_local_registry_cases() {
        for (r, want) in [
            ("localhost/foo", true),
            ("localhost:5001/foo", true),
            ("127.0.0.1/foo", true),
            ("127.0.0.1:5001/foo", true),
            ("127.0.0.2/foo", true),
            ("127.0.0.2:8080/foo", true),
            ("kind-registry/foo", false),
            ("kind-registry:5000/foo", false),
            ("my-registry.local/foo", false),
            ("my-registry.local:8080/foo", false),
            ("gcr.io/foo", false),
            ("example.com/foo", false),
            ("foo", false),
            ("", false),
        ] {
            assert_eq!(is_local_registry(r), want, "{r}");
        }
    }

    #[test]
    fn rewrite_local_registry_cases() {
        let rep = "kind-registry:5000";
        for (r, want) in [
            ("localhost/foo", "kind-registry:5000/foo"),
            ("localhost:5001/foo", "kind-registry:5000/foo"),
            ("localhost:8080/foo", "kind-registry:5000/foo"),
            ("127.0.0.1/foo", "kind-registry:5000/foo"),
            ("127.0.0.1:3000/foo", "kind-registry:5000/foo"),
            ("127.0.0.2/foo", "kind-registry:5000/foo"),
            ("127.0.0.2:8080/foo", "kind-registry:5000/foo"),
            ("kind-registry/foo", "kind-registry/foo"),
            ("kind-registry:5000/foo", "kind-registry:5000/foo"),
            ("my-registry.local/foo", "my-registry.local/foo"),
            ("gcr.io/foo", "gcr.io/foo"),
            ("foo", "foo"),
            ("", ""),
        ] {
            assert_eq!(rewrite_local_registry(r, rep), want, "{r}");
        }
    }
}
