//! SPIFFE identities of Actors and of the workers acting for them.
//!
//! An Actor's certificate carries `spiffe://hivekeep-actor.local/actor/<hive>/<name>`
//! as its URI SAN; the tunnel a worker runs on the Actor's behalf carries
//! `spiffe://hivekeep-actor.local/worker-for-actor/<hive>/<name>`.

use crate::names::is_valid_resource_name;
use crate::resource::ActorRef;
use crate::uri::Uri;

/// Trust domain of every Actor identity.
pub const ACTOR_TRUST_DOMAIN: &str = "hivekeep-actor.local";

/// Maximum length of a SPIFFE ID.
pub const SPIFFE_ID_MAX_LEN: usize = 2048;

const ACTOR_SEGMENT: &str = "actor";
const WORKER_FOR_ACTOR_SEGMENT: &str = "worker-for-actor";

/// Why a string is not an acceptable SPIFFE ID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpiffeError {
    /// Not a syntactically valid SPIFFE ID.
    #[error("invalid SPIFFE ID {id:?}: {reason}")]
    Malformed {
        /// The rejected ID.
        id: String,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// A valid SPIFFE ID, but not of the expected shape.
    #[error("{id:?} does not have format spiffe://<trust.domain>/{kind}/<hive>/<name>")]
    WrongShape {
        /// The rejected ID.
        id: String,
        /// The expected first path segment.
        kind: &'static str,
    },
    /// The hive or name segment is not a valid resource name.
    #[error("{value:?} is not a valid {what}")]
    BadName {
        /// The rejected segment.
        value: String,
        /// `"hive"` or `"actor name"`.
        what: &'static str,
    },
}

/// Checks the SPIFFE ID grammar: `spiffe://` followed by a lowercase trust
/// domain of `[a-z0-9._-]` and an optional path of non-empty segments of
/// `[A-Za-z0-9._-]` (no `.`/`..` segments, no trailing '/').
pub fn validate_spiffe_id(id: &str) -> Result<(), SpiffeError> {
    let bad = |reason| {
        Err(SpiffeError::Malformed {
            id: id.to_string(),
            reason,
        })
    };
    if id.is_empty() {
        return bad("cannot be empty");
    }
    if id.len() > SPIFFE_ID_MAX_LEN {
        return bad("too long");
    }
    let Some(rest) = id.strip_prefix("spiffe://") else {
        return bad("scheme is missing or invalid");
    };
    let (td, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if td.is_empty() {
        return bad("trust domain is missing");
    }
    if !td
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_'))
    {
        return bad("trust domain characters are limited to lowercase letters, numbers, dots, dashes, and underscores");
    }
    if path.is_empty() {
        return Ok(());
    }
    for seg in path[1..].split('/') {
        match seg {
            "" => return bad("path cannot contain empty segments or a trailing slash"),
            "." | ".." => return bad("path cannot contain dot segments"),
            s if !s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')) => {
                return bad("path segment characters are limited to letters, numbers, dots, dashes, and underscores")
            }
            _ => {}
        }
    }
    Ok(())
}

fn build(kind: &str, r: &ActorRef) -> String {
    format!("spiffe://{ACTOR_TRUST_DOMAIN}/{kind}/{}/{}", r.hive, r.name)
}

fn parse(id: &str, kind: &'static str) -> Result<ActorRef, SpiffeError> {
    let wrong = || SpiffeError::WrongShape {
        id: id.to_string(),
        kind,
    };
    let u = Uri::parse(id).map_err(|_| wrong())?;
    if u.scheme != "spiffe"
        || u.host != ACTOR_TRUST_DOMAIN
        || u.port.is_some()
        || u.user.is_some()
        || u.query.is_some()
        || u.fragment.is_some()
        || !u.opaque.is_empty()
    {
        return Err(wrong());
    }
    let path = u.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let [first, hive, name] = segments.as_slice() else {
        return Err(wrong());
    };
    if *first != kind {
        return Err(wrong());
    }
    if !is_valid_resource_name(hive) {
        return Err(SpiffeError::BadName {
            value: hive.to_string(),
            what: "hive",
        });
    }
    if !is_valid_resource_name(name) {
        return Err(SpiffeError::BadName {
            value: name.to_string(),
            what: "actor name",
        });
    }
    Ok(ActorRef::new(*hive, *name))
}

/// The SPIFFE ID minted into an Actor's certificate.
pub fn actor_spiffe_id(r: &ActorRef) -> String {
    build(ACTOR_SEGMENT, r)
}

/// Parses an ID built by [`actor_spiffe_id`].
pub fn actor_ref_from_spiffe_id(id: &str) -> Result<ActorRef, SpiffeError> {
    parse(id, ACTOR_SEGMENT)
}

/// The SPIFFE ID minted into the certificate of the tunnel a worker runs for
/// an Actor.
pub fn worker_for_actor_spiffe_id(r: &ActorRef) -> String {
    build(WORKER_FOR_ACTOR_SEGMENT, r)
}

/// Parses an ID built by [`worker_for_actor_spiffe_id`].
pub fn actor_ref_from_worker_for_actor_spiffe_id(id: &str) -> Result<ActorRef, SpiffeError> {
    parse(id, WORKER_FOR_ACTOR_SEGMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_ids() {
        for id in [
            "",
            "spiffe://hivekeep-actor.local",
            "spiffe://hivekeep-actor.local/worker-for-actor/team",
            "spiffe://hivekeep-actor.local/worker-for-actor/team/agent/extra",
            "spiffe://hivekeep-actor.local/actor/team/agent",
            "spiffe://hivekeep-actor.local/worker-for-actor//agent",
            "spiffe://hivekeep-actor.local/worker-for-actor/Team/agent",
            "spiffe://hivekeep-actor.local/worker-for-actor/team/agent?x=1",
            "spiffe://hivekeep-actor.local/worker-for-actor/team/agent#f",
            "spiffe://cluster.local/worker-for-actor/team/agent",
            "spiffe://user@hivekeep-actor.local/worker-for-actor/team/agent",
            "https://hivekeep-actor.local/worker-for-actor/team/agent",
            "hivekeep-actor.local/worker-for-actor/team/agent",
            "spiffe://hivekeep-actor.local/worker-for-actor/team/agent/",
            "spiffe://hivekeep-actor.local/worker-for-actor/te%2Fam/agent",
        ] {
            assert!(
                actor_ref_from_worker_for_actor_spiffe_id(id).is_err(),
                "{id}"
            );
        }
    }

    #[test]
    fn round_trips() {
        let r = ActorRef::new("team-a", "agent-1");
        let id = actor_spiffe_id(&r);
        assert_eq!(id, "spiffe://hivekeep-actor.local/actor/team-a/agent-1");
        assert_eq!(actor_ref_from_spiffe_id(&id).unwrap(), r);
        assert!(actor_ref_from_worker_for_actor_spiffe_id(&id).is_err());
        let wid = worker_for_actor_spiffe_id(&r);
        assert_eq!(
            wid,
            "spiffe://hivekeep-actor.local/worker-for-actor/team-a/agent-1"
        );
        assert_eq!(actor_ref_from_worker_for_actor_spiffe_id(&wid).unwrap(), r);
        assert!(actor_ref_from_spiffe_id(&wid).is_err());
        assert!(validate_spiffe_id(&id).is_ok());
    }

    #[test]
    fn spiffe_grammar() {
        for ok in [
            "spiffe://example.org",
            "spiffe://example.org/ns/default/sa/x_y.z-1",
        ] {
            assert!(validate_spiffe_id(ok).is_ok(), "{ok}");
        }
        for bad in [
            "spiffe://",
            "spiffe://Example.org/x",
            "http://example.org/x",
            "spiffe://example.org/",
            "spiffe://example.org/a//b",
            "spiffe://example.org/a/../b",
            "spiffe://example.org/a?b",
            "spiffe://example.org:8080/a",
        ] {
            assert!(validate_spiffe_id(bad).is_err(), "{bad}");
        }
    }
}
