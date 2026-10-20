//! The `hk-secret://` URI grammar of the Kubernetes provider.
//!
//! Only Secrets of the cluster the provider runs in are addressable:
//!
//! ```text
//! hk-secret://k8s.io/default/<namespace>/<secret>/<key>
//! ```
//!
//! The leading `default` locator leaves room for a later
//! `cluster/<cluster>` form naming remote clusters.

use hivekeep_core::uri::Uri;
use hivekeep_core::validation::api::egress_policy::SECRET_URI_SCHEME;

/// The URI host this provider serves.
pub const PROVIDER_NAME: &str = "k8s.io";

/// The reserved first path segment naming the local cluster, the only one
/// served.
pub const LOCAL_LOCATOR: &str = "default";

/// A parsed URI naming one data key of one Secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRef {
    /// Namespace of the Secret.
    pub namespace: String,
    /// Name of the Secret.
    pub name: String,
    /// Data key within the Secret.
    pub key: String,
}

/// Why a URI was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("credential URI {uri:?}: {reason}")]
pub struct UriError {
    /// The rejected URI.
    pub uri: String,
    /// What is wrong with it.
    pub reason: String,
}

/// Characters a path may carry literally. Anything else would need
/// percent-encoding, which the grammar forbids.
fn literal_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || "-._~!$&'()*+,;=:@/".contains(c)
}

/// Parses an `hk-secret://k8s.io/default/<namespace>/<secret>/<key>` URI.
pub fn parse_uri(raw: &str) -> Result<SecretRef, UriError> {
    let fail = |reason: String| UriError {
        uri: raw.to_string(),
        reason,
    };
    let u = Uri::parse(raw).map_err(|e| fail(format!("unparseable: {e}")))?;
    if u.scheme != SECRET_URI_SCHEME {
        return Err(fail(format!(
            "scheme is {:?}, want {SECRET_URI_SCHEME:?}",
            u.scheme
        )));
    }
    if u.host != PROVIDER_NAME || u.port.is_some() || u.user.is_some() {
        return Err(fail(format!(
            "provider is {:?}, this provider serves {PROVIDER_NAME:?}",
            u.host
        )));
    }
    // The grammar has no query or fragment; a caller using one assumed a
    // syntax this provider would silently ignore, so refuse it.
    if u.query.is_some() || u.fragment.is_some() {
        return Err(fail("query and fragment components are not allowed".into()));
    }
    if !u.raw_path.chars().all(literal_path_char) {
        return Err(fail("path must not contain percent-encoding".into()));
    }

    let segments: Vec<&str> = u.raw_path.trim_matches('/').split('/').collect();
    if let Some(i) = segments.iter().position(|s| s.is_empty()) {
        return Err(fail(format!("empty path segment {i}")));
    }
    if segments[0] != LOCAL_LOCATOR {
        return Err(fail(format!(
            "path must begin with {LOCAL_LOCATOR:?} (only local Secrets are supported), got {:?}",
            segments[0]
        )));
    }
    match &segments[1..] {
        [namespace, name, key] => Ok(SecretRef {
            namespace: namespace.to_string(),
            name: name.to_string(),
            key: key.to_string(),
        }),
        tail => Err(fail(format!(
            "want {LOCAL_LOCATOR}/<namespace>/<secret>/<key>, got {} trailing segments",
            tail.len()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uri_cases() {
        let ok = parse_uri("hk-secret://k8s.io/default/ns1/example-api/token").unwrap();
        assert_eq!(
            ok,
            SecretRef {
                namespace: "ns1".into(),
                name: "example-api".into(),
                key: "token".into()
            }
        );
        let bad = [
            ("key required", "hk-secret://k8s.io/default/ns1/example-api"),
            (
                "wrong scheme",
                "https://k8s.io/default/ns1/example-api/token",
            ),
            (
                "wrong provider",
                "hk-secret://vault.io/default/ns1/example-api/token",
            ),
            (
                "missing locator",
                "hk-secret://k8s.io/ns1/example-api/token",
            ),
            (
                "remote form not yet supported",
                "hk-secret://k8s.io/cluster/remote-east/ns1/example-api/token",
            ),
            ("too few segments", "hk-secret://k8s.io/default/ns1"),
            (
                "too many segments",
                "hk-secret://k8s.io/default/ns1/example-api/token/extra",
            ),
            (
                "query not allowed",
                "hk-secret://k8s.io/default/ns1/example-api/token?cluster=remote",
            ),
            (
                "empty query not allowed",
                "hk-secret://k8s.io/default/ns1/example-api/token?",
            ),
            (
                "fragment not allowed",
                "hk-secret://k8s.io/default/ns1/example-api/token#x",
            ),
            (
                "percent-encoded separator",
                "hk-secret://k8s.io/default/ns1/example-api/tok%2Fen",
            ),
            (
                "percent-encoding of any kind",
                "hk-secret://k8s.io/default/ns1/example-api/tok%2Den",
            ),
            (
                "space in path",
                "hk-secret://k8s.io/default/ns1/example-api/tok en",
            ),
            (
                "port on provider",
                "hk-secret://k8s.io:443/default/ns1/example-api/token",
            ),
            ("empty segment", "hk-secret://k8s.io/default/ns1//token"),
            ("unparseable", "://://"),
        ];
        for (name, uri) in bad {
            assert!(
                parse_uri(uri).is_err(),
                "{name}: parse_uri({uri:?}) succeeded"
            );
        }
    }
}
