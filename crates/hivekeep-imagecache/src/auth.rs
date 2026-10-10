//! Registry authentication: `WWW-Authenticate` challenges and the
//! credentials a pull presents.
//!
//! A registry answers an unauthenticated request with 401 and a challenge.
//! `Basic` means "send the username and password on every request";
//! `Bearer` names a token service that exchanges them (or nothing, for
//! anonymous pulls of public images) for a short-lived token scoped to the
//! repository.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;

/// A username and password (or token) for a registry.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Credential {
    /// The username.
    pub username: String,
    /// The password or token.
    pub password: String,
}

impl Credential {
    /// A credential from its parts.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Credential {
            username: username.into(),
            password: password.into(),
        }
    }

    /// The `Authorization: Basic` header value.
    pub fn basic_header(&self) -> String {
        let raw = format!("{}:{}", self.username, self.password);
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(raw)
        )
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl From<hivekeep_node::credential_provider::AuthConfig> for Credential {
    fn from(a: hivekeep_node::credential_provider::AuthConfig) -> Self {
        Credential::new(a.username, a.password)
    }
}

/// A parsed `WWW-Authenticate` challenge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Challenge {
    /// Send credentials with every request.
    Basic,
    /// Exchange credentials for a token at `realm`.
    Bearer {
        /// The token endpoint.
        realm: String,
        /// The service the token is for.
        service: Option<String>,
        /// The scope the registry asked for, if it named one.
        scope: Option<String>,
    },
}

/// Parses a `WWW-Authenticate` header value.
pub fn parse_challenge(header: &str) -> Option<Challenge> {
    let header = header.trim();
    let (scheme, rest) = header.split_once(' ').unwrap_or((header, ""));
    if scheme.eq_ignore_ascii_case("basic") {
        return Some(Challenge::Basic);
    }
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let params = parse_params(rest);
    Some(Challenge::Bearer {
        realm: params.get("realm")?.clone(),
        service: params.get("service").cloned(),
        scope: params.get("scope").cloned(),
    })
}

/// Parses `key="value", key2=value2` auth parameters.
fn parse_params(s: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = s.trim();
    while !rest.is_empty() {
        let Some((key, after)) = rest.split_once('=') else {
            break;
        };
        let key = key
            .trim()
            .trim_start_matches(',')
            .trim()
            .to_ascii_lowercase();
        let after = after.trim_start();
        let (value, remaining) = if let Some(quoted) = after.strip_prefix('"') {
            let mut value = String::new();
            let mut chars = quoted.char_indices();
            let mut end = quoted.len();
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' => {
                        if let Some((_, n)) = chars.next() {
                            value.push(n);
                        }
                    }
                    '"' => {
                        end = i + 1;
                        break;
                    }
                    c => value.push(c),
                }
            }
            (value, &quoted[end.min(quoted.len())..])
        } else {
            let end = after.find(',').unwrap_or(after.len());
            (after[..end].trim().to_string(), &after[end..])
        };
        out.insert(key, value);
        rest = remaining.trim_start().trim_start_matches(',').trim_start();
    }
    out
}

/// The pull scope for `repository`.
pub fn pull_scope(repository: &str) -> String {
    format!("repository:{repository}:pull")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_challenges() {
        assert_eq!(
            parse_challenge(r#"Basic realm="test""#),
            Some(Challenge::Basic)
        );
        assert_eq!(
            parse_challenge(
                r#"Bearer realm="https://auth.example/token",service="registry.example",scope="repository:a/b:pull""#
            ),
            Some(Challenge::Bearer {
                realm: "https://auth.example/token".into(),
                service: Some("registry.example".into()),
                scope: Some("repository:a/b:pull".into()),
            })
        );
        assert_eq!(
            parse_challenge("Bearer realm=https://t, service=s"),
            Some(Challenge::Bearer {
                realm: "https://t".into(),
                service: Some("s".into()),
                scope: None
            })
        );
        assert_eq!(parse_challenge("Negotiate"), None);
        assert_eq!(parse_challenge("Bearer service=x"), None);
    }

    #[test]
    fn basic_header_and_redaction() {
        let c = Credential::new("robot", "s3cret");
        assert_eq!(c.basic_header(), "Basic cm9ib3Q6czNjcmV0");
        assert!(!format!("{c:?}").contains("s3cret"));
    }
}
