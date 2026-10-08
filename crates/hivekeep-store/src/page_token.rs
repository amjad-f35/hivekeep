//! Opaque list page tokens.
//!
//! A token is base64 of a small JSON document naming the listing it belongs
//! to (resource kind and scope) and the ordering key of the last item served.
//! Listings resume strictly after that key, so items created or deleted
//! between pages never cause a repeat.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};

const VERSION: u32 = 1;

/// Which listing a token was issued for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListKind {
    /// Hives.
    #[serde(rename = "hive")]
    Hive,
    /// Actors.
    #[serde(rename = "actor")]
    Actor,
    /// ActorTemplates.
    #[serde(rename = "actor-template")]
    ActorTemplate,
    /// Tags.
    #[serde(rename = "tag")]
    Tag,
    /// Workers.
    #[serde(rename = "worker")]
    Worker,
    /// A Worker's assignments.
    #[serde(rename = "worker-assignment")]
    WorkerAssignment,
}

#[derive(Debug, Serialize, Deserialize)]
struct Token {
    v: u32,
    kind: ListKind,
    scope: String,
    last: Vec<String>,
}

/// Encodes a token resuming after `last` in the `(kind, scope)` listing.
pub fn encode(kind: ListKind, scope: &str, last: Vec<String>) -> String {
    let token = Token {
        v: VERSION,
        kind,
        scope: scope.to_string(),
        last,
    };
    STANDARD.encode(serde_json::to_vec(&token).expect("page tokens always serialize"))
}

/// Decodes `token` for the `(kind, scope)` listing whose keys have
/// `key_parts` columns. Returns the key to resume after, or `None` for the
/// first page.
pub fn decode(
    token: &str,
    kind: ListKind,
    scope: &str,
    key_parts: usize,
) -> Result<Option<Vec<String>>> {
    if token.is_empty() {
        return Ok(None);
    }
    let bad = |m: String| StoreError::InvalidPageToken(m);
    let bytes = STANDARD.decode(token).map_err(|e| bad(e.to_string()))?;
    let t: Token = serde_json::from_slice(&bytes).map_err(|e| bad(e.to_string()))?;
    if t.v != VERSION {
        return Err(bad(format!("unsupported version {}", t.v)));
    }
    if t.kind != kind {
        return Err(bad(format!(
            "issued for {:?}, used with {:?}",
            t.kind, kind
        )));
    }
    if t.scope != scope {
        return Err(bad(format!(
            "issued for scope {:?}, used with scope {:?}",
            t.scope, scope
        )));
    }
    if t.last.len() != key_parts {
        return Err(bad(format!(
            "got {} key parts, want {key_parts}",
            t.last.len()
        )));
    }
    Ok(Some(t.last))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let t = encode(ListKind::Actor, "team-a", vec!["a1".into()]);
        assert_eq!(
            decode(&t, ListKind::Actor, "team-a", 1).unwrap(),
            Some(vec!["a1".to_string()])
        );
        assert_eq!(decode("", ListKind::Actor, "", 2).unwrap(), None);
    }

    #[test]
    fn rejects_foreign_tokens() {
        let t = encode(ListKind::Actor, "team-a", vec!["a1".into()]);
        for (kind, scope, parts) in [
            (ListKind::Tag, "team-a", 1),
            (ListKind::Actor, "team-b", 1),
            (ListKind::Actor, "team-a", 2),
        ] {
            assert!(matches!(
                decode(&t, kind, scope, parts),
                Err(StoreError::InvalidPageToken(_))
            ));
        }
        // A global Actor token needs both key parts.
        let short = encode(ListKind::Actor, "", vec!["only-a-hive".into()]);
        assert!(matches!(
            decode(&short, ListKind::Actor, "", 2),
            Err(StoreError::InvalidPageToken(_))
        ));
        for junk in ["%%%", "bm90IGpzb24="] {
            assert!(matches!(
                decode(junk, ListKind::Actor, "", 2),
                Err(StoreError::InvalidPageToken(_))
            ));
        }
    }
}
