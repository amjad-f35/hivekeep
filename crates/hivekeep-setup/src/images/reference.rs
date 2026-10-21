//! Image references: the placeholder references the manifests carry, and
//! parsed registry references for talking to a registry.

use super::{ImageError, COMPONENTS, REGISTRY_PLACEHOLDER};
use regex::Regex;
use std::sync::OnceLock;

/// Matches a whole placeholder reference: the prefix plus everything up to
/// the first character that can end a YAML scalar.
///
/// Matching the whole reference is what makes the rewrite safe: replacing a
/// known prefix literally would turn `hivekeep-keeper-sidecar` into the keeper
/// image with `-sidecar` left dangling. Requiring a character after the slash
/// keeps prose mentions of the placeholder from matching.
pub fn placeholder_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&format!(r#"{REGISTRY_PLACEHOLDER}/[^\s"',\]}}]+"#)).expect("valid regex")
    })
}

/// The component a placeholder reference names, when it names a known one
/// in the canonical `REGISTRY_PLACEHOLDER/<component>:<tag>` form.
pub fn component_of(reference: &str) -> Option<&'static str> {
    let rest = reference
        .strip_prefix(REGISTRY_PLACEHOLDER)?
        .strip_prefix('/')?;
    let (name, tag) = rest.split_once(':')?;
    if tag.is_empty() || name.contains('/') {
        return None;
    }
    COMPONENTS.iter().copied().find(|c| *c == name)
}

/// A parsed registry reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRef {
    /// Registry host, with port when one is given.
    pub registry: String,
    /// Repository path within the registry.
    pub repository: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

const DOCKER_HUB: &str = "index.docker.io";

fn valid_repository(repo: &str) -> bool {
    !repo.is_empty()
        && repo.split('/').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
                && part.starts_with(|c: char| c.is_ascii_alphanumeric())
        })
}

fn valid_registry(host: &str) -> bool {
    !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ".-:[]".contains(c))
}

fn valid_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= 128
        && tag.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
}

fn valid_digest(d: &str) -> bool {
    match d.split_once(':') {
        Some((alg, hex)) => {
            !alg.is_empty() && hex.len() >= 32 && hex.chars().all(|c| c.is_ascii_hexdigit())
        }
        None => false,
    }
}

impl ImageRef {
    /// Parses `[registry/]repository[:tag][@digest]`. A missing registry
    /// means Docker Hub; a missing tag and digest means `latest`.
    pub fn parse(reference: &str) -> Result<Self, ImageError> {
        let bad = |why: &str| {
            ImageError::Other(format!("{reference} is not a valid image reference: {why}"))
        };
        let (rest, digest) = match reference.split_once('@') {
            Some((r, d)) => {
                if !valid_digest(d) {
                    return Err(bad("malformed digest"));
                }
                (r, Some(d.to_string()))
            }
            None => (reference, None),
        };
        // A tag follows the last colon after the last slash.
        let last_slash = rest.rfind('/').map_or(0, |i| i + 1);
        let (name, tag) = match rest[last_slash..].rfind(':') {
            Some(i) => {
                let tag = &rest[last_slash + i + 1..];
                if !valid_tag(tag) {
                    return Err(bad("malformed tag"));
                }
                (&rest[..last_slash + i], Some(tag.to_string()))
            }
            None => (rest, None),
        };
        let (registry, repository) = match name.split_once('/') {
            Some((first, path))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (first.to_string(), path.to_string())
            }
            _ => {
                let repo = if name.contains('/') {
                    name.to_string()
                } else {
                    format!("library/{name}")
                };
                (DOCKER_HUB.to_string(), repo)
            }
        };
        if !valid_registry(&registry) {
            return Err(bad("malformed registry"));
        }
        if !valid_repository(&repository) {
            return Err(bad("malformed repository"));
        }
        let tag = if tag.is_none() && digest.is_none() {
            Some("latest".into())
        } else {
            tag
        };
        Ok(Self {
            registry,
            repository,
            tag,
            digest,
        })
    }

    /// Parses a reference that must carry a digest.
    pub fn parse_digest(reference: &str) -> Result<Self, ImageError> {
        let r = Self::parse(reference)?;
        if r.digest.is_none() {
            return Err(ImageError::Other(format!(
                "{reference} is not a digest reference"
            )));
        }
        Ok(r)
    }

    /// `registry/repository`, which identifies the repository regardless of
    /// tag or digest.
    pub fn context(&self) -> String {
        format!("{}/{}", self.registry, self.repository)
    }

    /// The digest if pinned, else the tag: what a manifest request names.
    pub fn identifier(&self) -> &str {
        self.digest
            .as_deref()
            .or(self.tag.as_deref())
            .unwrap_or("latest")
    }

    /// Plain HTTP for a local registry, as a Kind cluster's local registry
    /// serves; HTTPS everywhere else.
    pub fn scheme(&self) -> &'static str {
        let host = self
            .registry
            .rsplit_once(':')
            .map_or(self.registry.as_str(), |(h, _)| h);
        if host == "localhost" || host == "127.0.0.1" || host == "[::1]" || host.ends_with(".local")
        {
            "http"
        } else {
            "https"
        }
    }

    /// The API host: Docker Hub's registry API lives on a different name.
    pub fn api_host(&self) -> &str {
        if self.registry == DOCKER_HUB {
            "registry-1.docker.io"
        } else {
            &self.registry
        }
    }
}

/// An image platform, `os/architecture[/variant]`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Platform {
    pub os: String,
    pub architecture: String,
    pub variant: String,
}

impl Platform {
    pub fn parse(s: &str) -> Result<Self, ImageError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() < 2 || parts.len() > 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(ImageError::Other(format!("{s:?} is not a platform")));
        }
        Ok(Self {
            os: parts[0].into(),
            architecture: parts[1].into(),
            variant: parts.get(2).map(|v| v.to_string()).unwrap_or_default(),
        })
    }

    /// Whether this image platform can serve `want`. A want without a
    /// variant accepts any variant.
    pub fn satisfies(&self, want: &Platform) -> bool {
        self.os == want.os
            && self.architecture == want.architecture
            && (want.variant.is_empty() || self.variant == want.variant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_references() {
        let r = ImageRef::parse("localhost:5001/hivekeep/hivekeep-api:v1").unwrap();
        assert_eq!(r.registry, "localhost:5001");
        assert_eq!(r.repository, "hivekeep/hivekeep-api");
        assert_eq!(r.tag.as_deref(), Some("v1"));
        assert_eq!(r.scheme(), "http");
        let d = format!("example.com/a/b@sha256:{}", "1".repeat(64));
        let r = ImageRef::parse(&d).unwrap();
        assert_eq!(r.identifier(), format!("sha256:{}", "1".repeat(64)));
        assert_eq!(r.scheme(), "https");
        let hub = ImageRef::parse("ubuntu").unwrap();
        assert_eq!(hub.context(), "index.docker.io/library/ubuntu");
        assert_eq!(hub.tag.as_deref(), Some("latest"));
        assert!(ImageRef::parse("not a repo/image:v1").is_err());
        assert!(ImageRef::parse("example.com/x:v1,latest").is_err());
    }

    #[test]
    fn component_of_requires_canonical_form() {
        assert_eq!(
            component_of("REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0"),
            Some("hivekeep-api")
        );
        assert_eq!(
            component_of("REGISTRY_PLACEHOLDER/hivekeep-keeper-sidecar:0.1.0"),
            None
        );
        assert_eq!(component_of("REGISTRY_PLACEHOLDER/hivekeep-api"), None);
        assert_eq!(
            component_of("REGISTRY_PLACEHOLDER/x/hivekeep-api:0.1.0"),
            None
        );
    }

    #[test]
    fn platform_satisfaction() {
        let want = Platform::parse("linux/arm64").unwrap();
        let have = Platform::parse("linux/arm64/v8").unwrap();
        assert!(have.satisfies(&want));
        assert!(!want.satisfies(&have));
        assert!(Platform::parse("linux").is_err());
    }
}
