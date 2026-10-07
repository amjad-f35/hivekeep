//! OCI image references.
//!
//! Templates must pin images by digest: changing image content under a fixed
//! reference would invalidate snapshots taken from it. The grammar matches
//! the one container runtimes use, so a malformed digest is rejected rather
//! than mistaken for a pin.
//!
//! ```text
//! reference   := name [ ":" tag ] [ "@" digest ]
//! name        := [ domain "/" ] path-component ( "/" path-component )*
//! domain      := host [ ":" port ]
//! path-component := [a-z0-9]+ ( ( "." | "_" | "__" | "-"+ ) [a-z0-9]+ )*
//! tag         := [A-Za-z0-9_] [A-Za-z0-9_.-]{0,127}
//! digest      := algorithm ":" hex
//! ```

use std::fmt;
use std::sync::LazyLock;

use regex::Regex;

/// Maximum length of the name part of a reference.
pub const NAME_MAX_LEN: usize = 255;

static PATH_COMPONENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$").unwrap());
static DOMAIN_COMPONENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])$").unwrap());
static TAG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[\w][\w.-]{0,127}$").unwrap());
static ALGORITHM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z][A-Za-z0-9]*(?:[+._-][A-Za-z][A-Za-z0-9]*)*$").unwrap());
static IDENTIFIER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-f0-9]{64}$").unwrap());

/// Why a string is not an acceptable image reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ImageRefError {
    /// The input is empty.
    #[error("repository name must have at least one component")]
    Empty,
    /// The input does not follow the grammar.
    #[error("invalid reference format")]
    Format,
    /// The repository path contains uppercase letters.
    #[error("repository name must be lowercase")]
    Uppercase,
    /// The name is a bare 64-hex identifier.
    #[error("cannot specify 64-byte hexadecimal strings")]
    Identifier,
    /// The name is too long.
    #[error("repository name must not be more than {NAME_MAX_LEN} characters")]
    NameTooLong,
    /// The digest algorithm is unknown.
    #[error("unsupported digest algorithm")]
    UnsupportedDigest,
    /// The digest does not match its algorithm's encoding.
    #[error("invalid checksum digest format")]
    InvalidDigest,
    /// A valid reference that is not pinned by digest.
    #[error("must be pinned by digest (changing the image invalidates snapshots)")]
    NotPinned,
}

/// A parsed image reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    /// Registry host (and port), if given.
    pub domain: Option<String>,
    /// Repository path.
    pub path: String,
    /// Tag, if given.
    pub tag: Option<String>,
    /// Digest (`algorithm:hex`), if given.
    pub digest: Option<String>,
}

impl ImageRef {
    /// Parses a reference.
    pub fn parse(s: &str) -> Result<ImageRef, ImageRefError> {
        if s.is_empty() {
            return Err(ImageRefError::Empty);
        }
        if IDENTIFIER.is_match(s) {
            return Err(ImageRefError::Identifier);
        }
        let (rest, digest) = match s.split_once('@') {
            Some((r, d)) => (r, Some(d)),
            None => (s, None),
        };
        let last_slash = rest.rfind('/').map(|i| i + 1).unwrap_or(0);
        let (name, tag) = match rest[last_slash..].rfind(':') {
            Some(i) => (&rest[..last_slash + i], Some(&rest[last_slash + i + 1..])),
            None => (rest, None),
        };
        if name.is_empty() {
            return Err(ImageRefError::Format);
        }
        if name.len() > NAME_MAX_LEN {
            return Err(ImageRefError::NameTooLong);
        }
        let (domain, path) = split_domain(name);
        if let Some(d) = domain {
            validate_domain(d)?;
        }
        if path.to_ascii_lowercase() != path {
            return Err(ImageRefError::Uppercase);
        }
        if !path.split('/').all(|c| PATH_COMPONENT.is_match(c)) {
            return Err(ImageRefError::Format);
        }
        if let Some(t) = tag {
            if !TAG.is_match(t) {
                return Err(ImageRefError::Format);
            }
        }
        if let Some(d) = digest {
            validate_digest(d)?;
        }
        Ok(ImageRef {
            domain: domain.map(str::to_string),
            path: path.to_string(),
            tag: tag.map(str::to_string),
            digest: digest.map(str::to_string),
        })
    }

    /// Parses a reference and requires it to carry a digest.
    pub fn parse_pinned(s: &str) -> Result<ImageRef, ImageRefError> {
        let r = Self::parse(s)?;
        if r.digest.is_none() {
            return Err(ImageRefError::NotPinned);
        }
        Ok(r)
    }
}

impl fmt::Display for ImageRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(d) = &self.domain {
            write!(f, "{d}/")?;
        }
        f.write_str(&self.path)?;
        if let Some(t) = &self.tag {
            write!(f, ":{t}")?;
        }
        if let Some(d) = &self.digest {
            write!(f, "@{d}")?;
        }
        Ok(())
    }
}

/// A leading component is a registry when it has a '.' or ':', is
/// `localhost`, or contains uppercase letters.
fn split_domain(name: &str) -> (Option<&str>, &str) {
    match name.split_once('/') {
        Some((first, rest))
            if first.contains(['.', ':'])
                || first == "localhost"
                || first.to_ascii_lowercase() != first =>
        {
            (Some(first), rest)
        }
        _ => (None, name),
    }
}

fn validate_domain(d: &str) -> Result<(), ImageRefError> {
    let (host, port) = if let Some(rest) = d.strip_prefix('[') {
        let end = rest.find(']').ok_or(ImageRefError::Format)?;
        rest[..end]
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| ImageRefError::Format)?;
        let tail = &rest[end + 1..];
        let port = if tail.is_empty() {
            None
        } else {
            Some(tail.strip_prefix(':').ok_or(ImageRefError::Format)?)
        };
        (None, port)
    } else {
        match d.split_once(':') {
            Some((h, p)) => (Some(h), Some(p)),
            None => (Some(d), None),
        }
    };
    if let Some(host) = host {
        if host.is_empty() || !host.split('.').all(|c| DOMAIN_COMPONENT.is_match(c)) {
            return Err(ImageRefError::Format);
        }
    }
    if let Some(p) = port {
        if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ImageRefError::Format);
        }
    }
    Ok(())
}

fn validate_digest(d: &str) -> Result<(), ImageRefError> {
    let (alg, hex) = d.split_once(':').ok_or(ImageRefError::InvalidDigest)?;
    if !ALGORITHM.is_match(alg) || hex.len() < 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ImageRefError::Format);
    }
    let want = match alg {
        "sha256" => 64,
        "sha384" => 96,
        "sha512" => 128,
        _ => return Err(ImageRefError::UnsupportedDigest),
    };
    if hex.len() != want || hex.bytes().any(|b| b.is_ascii_uppercase()) {
        return Err(ImageRefError::InvalidDigest);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn pinned_references() {
        for ok in [
            format!("example.com/app@{SHA}"),
            format!("example.com/app:v1@{SHA}"),
            format!("app@{SHA}"),
            format!("localhost:5000/a/b-c__d.e@{SHA}"),
            format!("[::1]:5000/app@{SHA}"),
        ] {
            let r = ImageRef::parse_pinned(&ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
            assert_eq!(r.to_string(), ok);
        }
    }

    #[test]
    fn rejected_references() {
        assert_eq!(
            ImageRef::parse_pinned("example.com/app:v1"),
            Err(ImageRefError::NotPinned)
        );
        assert!(ImageRef::parse_pinned("example.com/app@sha256:abc").is_err());
        assert_eq!(
            ImageRef::parse("Example.com/App"),
            Err(ImageRefError::Uppercase)
        );
        assert_eq!(ImageRef::parse(""), Err(ImageRefError::Empty));
        assert_eq!(
            ImageRef::parse(&"a".repeat(64).replace('a', "f")),
            Err(ImageRefError::Identifier)
        );
        assert_eq!(
            ImageRef::parse(&format!("app@md5:{}", "a".repeat(32))),
            Err(ImageRefError::UnsupportedDigest)
        );
        assert_eq!(
            ImageRef::parse(&format!(
                "app@{}",
                SHA.to_uppercase().replacen("SHA256", "sha256", 1)
            )),
            Err(ImageRefError::InvalidDigest)
        );
        for bad in [
            "-app",
            "app:",
            "app:-x",
            "a//b",
            "app@",
            "exa_mple.com/app",
            &format!("{}@{SHA}", "x".repeat(513)),
        ] {
            assert!(ImageRef::parse(bad).is_err(), "{bad}");
        }
    }
}
