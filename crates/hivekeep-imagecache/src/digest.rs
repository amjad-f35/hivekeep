//! Content digests (`sha256:<hex>`): manifest and blob digests, layer
//! diffIDs.

use std::fmt;
use std::io;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};

use crate::error::Error;

/// The only digest algorithm the cache accepts.
pub const SHA256: &str = "sha256";

/// A content digest: an algorithm and its lowercase hex encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest {
    /// The algorithm, `sha256`.
    pub algorithm: String,
    /// The lowercase hex digest.
    pub hex: String,
}

impl Digest {
    /// The sha256 digest with hex `hex` (not validated).
    pub fn sha256(hex: impl Into<String>) -> Self {
        Digest {
            algorithm: SHA256.to_string(),
            hex: hex.into(),
        }
    }

    /// The sha256 digest of `data`.
    pub fn of(data: &[u8]) -> Self {
        Digest::sha256(hex::encode(Sha256::digest(data)))
    }

    /// Parses `algorithm:hex`.
    pub fn parse(s: &str) -> Result<Self, Error> {
        let (alg, hex) = s
            .split_once(':')
            .ok_or_else(|| Error::msg(format!("cannot parse hash: {s:?}")))?;
        if alg != SHA256 {
            return Err(Error::msg(format!("unsupported hash algorithm in {s:?}")));
        }
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(Error::msg(format!("wrong hex encoding in hash {s:?}")));
        }
        Ok(Digest::sha256(hex))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.hex)
    }
}

impl FromStr for Digest {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Digest::parse(s)
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Digest::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// A sha256 hasher that is also an [`io::Write`] sink.
#[derive(Default, Clone)]
pub struct Hasher(Sha256);

impl Hasher {
    /// A fresh hasher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds `data`.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// The digest of everything fed so far.
    pub fn finish(self) -> Digest {
        Digest::sha256(hex::encode(self.0.finalize()))
    }
}

impl io::Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats() {
        let d = Digest::of(b"hello");
        assert_eq!(Digest::parse(&d.to_string()).unwrap(), d);
        assert!(Digest::parse("not-a-hash").is_err());
        assert!(Digest::parse("sha256:abc").is_err());
        assert!(Digest::parse(&format!("md5:{}", "a".repeat(64))).is_err());
        let mut h = Hasher::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finish(), d);
    }
}
