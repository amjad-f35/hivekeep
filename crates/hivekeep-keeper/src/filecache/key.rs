//! Cache keys: identities of cached content, not addresses.

use sha2::{Digest, Sha256};

/// Joins URI key parts unambiguously: NUL appears in no URI or file name.
const PART_SEPARATOR: &str = "\0";

/// Identifies a cache entry. Two sources serving the same identity share
/// one entry. Built only through the constructors.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    canonical: String,
    dir: String,
}

/// A digest that is not 64 hex characters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("sha256 key: digest {digest:?} {reason}")]
pub struct KeyError {
    digest: String,
    reason: String,
}

impl Key {
    /// The key of content-addressed bytes, by the hex sha256 of their
    /// contents. Uppercase digits are normalized.
    pub fn sha256(hex_digest: &str) -> Result<Key, KeyError> {
        let d = hex_digest.to_ascii_lowercase();
        if d.len() != 64 {
            return Err(KeyError {
                digest: hex_digest.into(),
                reason: format!("has length {}, want 64", d.len()),
            });
        }
        if hex::decode(&d).is_err() {
            return Err(KeyError {
                digest: hex_digest.into(),
                reason: "is not hex".into(),
            });
        }
        Ok(Key::new(format!("sha256:{d}")))
    }

    /// The key of content identified by an immutable source, such as a
    /// shared snapshot's object address. The cache never invalidates, so
    /// the parts must name content that never changes.
    pub fn uri<S: AsRef<str>>(parts: &[S]) -> Key {
        let joined: Vec<&str> = parts.iter().map(|p| p.as_ref()).collect();
        Key::new(format!("uri:{}", joined.join(PART_SEPARATOR)))
    }

    fn new(canonical: String) -> Key {
        let dir = hex::encode(Sha256::digest(canonical.as_bytes()));
        Key { canonical, dir }
    }

    /// The entry's directory name: hex sha256 of the canonical form.
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// The canonical form, for logs and the entry's metadata.
    pub fn canonical(&self) -> &str {
        &self.canonical
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[test]
    fn sha256_normalizes_case() {
        let a = Key::sha256(D).unwrap();
        let b = Key::sha256(&D.to_uppercase()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.canonical(), format!("sha256:{D}"));
        assert_eq!(a.dir().len(), 64);
    }

    #[test]
    fn sha256_rejects_bad_digests() {
        assert!(Key::sha256("").is_err());
        assert!(Key::sha256("abc").is_err());
        assert!(Key::sha256(&"z".repeat(64)).is_err());
    }

    #[test]
    fn uri_parts_are_unambiguous() {
        assert_ne!(Key::uri(&["a/b", "c"]), Key::uri(&["a", "b/c"]));
        assert_eq!(Key::uri(&["gs://b/x"]), Key::uri(&["gs://b/x"]));
    }

    #[test]
    fn key_spaces_are_disjoint() {
        assert_ne!(Key::sha256(D).unwrap(), Key::uri(&[format!("sha256:{D}")]));
    }
}
