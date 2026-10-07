//! A small, strict URI splitter for object-storage locations and identity
//! URIs.
//!
//! General-purpose URL libraries normalise paths (resolving `.` and `..`)
//! while parsing, which would hide exactly the segments the snapshot layout
//! must reject. [`Uri`] keeps every component as written, and decodes the
//! path only on request.

use std::fmt;

/// Error from [`Uri::parse`] or [`Uri::join_path`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UriError {
    /// A ':' appears where a scheme should start.
    #[error("missing protocol scheme")]
    MissingScheme,
    /// A control character appears anywhere in the input.
    #[error("invalid control character in URI")]
    ControlCharacter,
    /// A '%' is not followed by two hex digits.
    #[error("invalid escape {0:?}")]
    InvalidEscape(String),
    /// The host contains a character that is never valid there.
    #[error("invalid character {0:?} in host name")]
    InvalidHost(char),
    /// The port is not numeric.
    #[error("invalid port {0:?}")]
    InvalidPort(String),
}

/// The components of a URI, kept as written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Uri {
    /// Scheme, without the trailing ':'; empty when absent.
    pub scheme: String,
    /// Opaque part of `scheme:opaque` URIs (no `//`).
    pub opaque: String,
    /// User information before '@', if any.
    pub user: Option<String>,
    /// Host (without port).
    pub host: String,
    /// Port, if any.
    pub port: Option<String>,
    /// Path exactly as written, still percent-encoded.
    pub raw_path: String,
    /// Query after '?', if any.
    pub query: Option<String>,
    /// Fragment after '#', if any.
    pub fragment: Option<String>,
}

fn is_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn host_char_ok(c: char) -> bool {
    c.is_ascii_alphanumeric() || "-._~!$&'()*+,;=[]%:".contains(c)
}

/// Decodes `%XX` escapes, rejecting malformed ones.
pub fn percent_decode(s: &str) -> Result<String, UriError> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .filter(|h| h.iter().all(u8::is_ascii_hexdigit));
            let Some(hex) = hex else {
                let end = (i + 3).min(bytes.len());
                return Err(UriError::InvalidEscape(
                    String::from_utf8_lossy(&bytes[i..end]).into_owned(),
                ));
            };
            out.push(u8::from_str_radix(std::str::from_utf8(hex).unwrap(), 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Percent-encodes the bytes of a raw path that may not appear literally.
fn encode_path(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || "-._~!$&'()*+,;=:@/%".contains(c) {
            out.push(c);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl Uri {
    /// Splits `s` into components.
    pub fn parse(s: &str) -> Result<Uri, UriError> {
        if s.chars().any(|c| c.is_ascii_control()) {
            return Err(UriError::ControlCharacter);
        }
        let mut u = Uri::default();
        let (rest, fragment) = match s.split_once('#') {
            Some((r, f)) => (r, Some(f.to_string())),
            None => (s, None),
        };
        u.fragment = fragment;
        let (mut rest, query) = match rest.split_once('?') {
            Some((r, q)) => (r, Some(q.to_string())),
            None => (rest, None),
        };
        u.query = query;
        if let Some(colon) = rest.find(':') {
            let candidate = &rest[..colon];
            if !candidate.contains('/') {
                if candidate.is_empty() {
                    return Err(UriError::MissingScheme);
                }
                if is_scheme(candidate) {
                    u.scheme = candidate.to_ascii_lowercase();
                    rest = &rest[colon + 1..];
                }
            }
        }
        if !u.scheme.is_empty() && !rest.starts_with('/') {
            u.opaque = rest.to_string();
            return Ok(u);
        }
        if let Some(after) = rest.strip_prefix("//") {
            let (authority, path) = match after.find('/') {
                Some(i) => (&after[..i], &after[i..]),
                None => (after, ""),
            };
            let host_port = match authority.rsplit_once('@') {
                Some((user, hp)) => {
                    u.user = Some(user.to_string());
                    hp
                }
                None => authority,
            };
            let (host, port) =
                if let Some(end) = host_port.strip_prefix('[').and_then(|h| h.find(']')) {
                    let host = &host_port[..end + 2];
                    let tail = &host_port[end + 2..];
                    (host, tail.strip_prefix(':'))
                } else {
                    match host_port.rsplit_once(':') {
                        Some((h, p)) => (h, Some(p)),
                        None => (host_port, None),
                    }
                };
            if let Some(c) = host.chars().find(|&c| !host_char_ok(c)) {
                return Err(UriError::InvalidHost(c));
            }
            percent_decode(host)?;
            if let Some(p) = port {
                if !p.chars().all(|c| c.is_ascii_digit()) {
                    return Err(UriError::InvalidPort(p.to_string()));
                }
                if !p.is_empty() {
                    u.port = Some(p.to_string());
                }
            }
            u.host = host.to_string();
            rest = path;
        }
        percent_decode(rest)?;
        u.raw_path = rest.to_string();
        Ok(u)
    }

    /// The path with percent escapes decoded.
    pub fn path(&self) -> String {
        // Validated at parse time.
        percent_decode(&self.raw_path).unwrap_or_default()
    }

    /// Appends path elements to the path, joining with '/', dropping empty
    /// segments and resolving `.`/`..` the way a lexical path clean does. A
    /// trailing '/' on the last element is kept.
    pub fn join_path<S: AsRef<str>>(&self, elems: &[S]) -> Result<Uri, UriError> {
        let mut segments: Vec<&str> = Vec::new();
        let all = std::iter::once(self.raw_path.as_str()).chain(elems.iter().map(AsRef::as_ref));
        let mut trailing = false;
        for elem in all {
            for seg in elem.split('/') {
                match seg {
                    "" | "." => {}
                    ".." => {
                        segments.pop();
                    }
                    s => segments.push(s),
                }
            }
            trailing = elem.ends_with('/') && !elem.is_empty();
        }
        let mut path = String::new();
        for s in &segments {
            path.push('/');
            path.push_str(s);
        }
        if trailing && !elems.is_empty() {
            path.push('/');
        }
        percent_decode(&path)?;
        let mut out = self.clone();
        out.raw_path = path;
        Ok(out)
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.scheme.is_empty() {
            write!(f, "{}:", self.scheme)?;
        }
        if !self.opaque.is_empty() {
            f.write_str(&self.opaque)?;
        } else {
            if !self.scheme.is_empty() || !self.host.is_empty() || self.user.is_some() {
                f.write_str("//")?;
                if let Some(user) = &self.user {
                    write!(f, "{user}@")?;
                }
                f.write_str(&self.host)?;
                if let Some(port) = &self.port {
                    write!(f, ":{port}")?;
                }
            }
            f.write_str(&encode_path(&self.raw_path))?;
        }
        if let Some(q) = &self.query {
            write!(f, "?{q}")?;
        }
        if let Some(frag) = &self.fragment {
            write!(f, "#{frag}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_components() {
        let u = Uri::parse("gs://user@bucket:99/a/b%20c?x=1#frag").unwrap();
        assert_eq!(u.scheme, "gs");
        assert_eq!(u.user.as_deref(), Some("user"));
        assert_eq!(u.host, "bucket");
        assert_eq!(u.port.as_deref(), Some("99"));
        assert_eq!(u.raw_path, "/a/b%20c");
        assert_eq!(u.path(), "/a/b c");
        assert_eq!(u.query.as_deref(), Some("x=1"));
        assert_eq!(u.fragment.as_deref(), Some("frag"));
    }

    #[test]
    fn opaque_and_schemeless() {
        let u = Uri::parse("gs:bucket/path").unwrap();
        assert_eq!((u.opaque.as_str(), u.host.as_str()), ("bucket/path", ""));
        let u = Uri::parse("bucket/path").unwrap();
        assert_eq!(
            (u.scheme.as_str(), u.raw_path.as_str()),
            ("", "bucket/path")
        );
        assert_eq!(Uri::parse("://bucket"), Err(UriError::MissingScheme));
        assert!(Uri::parse("gs://b/100%zz").is_err());
        assert!(Uri::parse("gs://b c/x").is_err());
    }

    #[test]
    fn join_cleans() {
        let u = Uri::parse("gs://bucket/root/").unwrap();
        assert_eq!(
            u.join_path(&["a", "b"]).unwrap().to_string(),
            "gs://bucket/root/a/b"
        );
        let u = Uri::parse("gs://bucket").unwrap();
        assert_eq!(u.join_path(&["a"]).unwrap().to_string(), "gs://bucket/a");
        assert_eq!(u.join_path(&["a/"]).unwrap().to_string(), "gs://bucket/a/");
        assert!(u.join_path(&["100%done"]).is_err());
    }
}
