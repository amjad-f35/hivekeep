//! The claims of Actor tokens and their wire form.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{IdentityError, Result};

/// Claim holding the Hivekeep fields of an Actor's own token.
pub const ACTOR_CLAIM: &str = "actor.hivekeep.io";
/// Claim holding the Hivekeep fields of a worker-for-actor token.
pub const WORKER_FOR_ACTOR_CLAIM: &str = "worker-for-actor.hivekeep.io";

/// Lifetime of worker-for-actor tokens.
pub const WORKER_FOR_ACTOR_TTL: Duration = Duration::from_secs(60 * 60);
/// How far `nbf` is backdated, to tolerate clock skew.
pub const NOT_BEFORE_BACKDATE: Duration = Duration::from_secs(5 * 60);

/// Who a token speaks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenKind {
    /// The Actor itself. Subject `actor/<hive>/<name>`.
    Actor,
    /// A worker, or another Hivekeep component, acting on the Actor's behalf.
    /// Subject `worker-for-actor/<hive>/<name>`.
    WorkerForActor,
}

impl TokenKind {
    /// First segment of the subject.
    pub fn subject_prefix(self) -> &'static str {
        match self {
            TokenKind::Actor => "actor",
            TokenKind::WorkerForActor => "worker-for-actor",
        }
    }

    /// Name of the claim the Hivekeep fields go under.
    pub fn claim_name(self) -> &'static str {
        match self {
            TokenKind::Actor => ACTOR_CLAIM,
            TokenKind::WorkerForActor => WORKER_FOR_ACTOR_CLAIM,
        }
    }

    /// The subject for an Actor.
    pub fn subject(self, hive: &str, actor_name: &str) -> String {
        format!("{}/{hive}/{actor_name}", self.subject_prefix())
    }
}

/// The Hivekeep fields of a token.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorClaims {
    /// Hive of the Actor.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hive: String,
    /// Actor name.
    #[serde(
        rename = "actorName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub actor_name: String,
    /// Actor UID.
    #[serde(rename = "actorUID", default, skip_serializing_if = "String::is_empty")]
    pub actor_uid: String,
    /// Node the Actor was on when a worker-for-actor token was minted, for
    /// relying parties to compare with where it is when the token is used.
    /// Always empty on an Actor's own token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
}

/// The claims of a token to mint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// Who the token speaks for.
    pub kind: TokenKind,
    /// `iss`.
    pub issuer: String,
    /// `sub`.
    pub subject: String,
    /// `aud`; always encoded as an array.
    pub audiences: Vec<String>,
    /// `exp`.
    pub expiration: SystemTime,
    /// `nbf`.
    pub not_before: SystemTime,
    /// `iat`.
    pub issued_at: SystemTime,
    /// `jti`.
    pub jti: String,
    /// The Hivekeep fields.
    pub actor: ActorClaims,
}

/// The JSON payload of a token.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireClaims {
    /// Issuer.
    #[serde(rename = "iss", default, skip_serializing_if = "String::is_empty")]
    pub issuer: String,
    /// Subject.
    #[serde(rename = "sub", default, skip_serializing_if = "String::is_empty")]
    pub subject: String,
    /// Audience: a string or an array of strings.
    #[serde(rename = "aud", default, skip_serializing_if = "Option::is_none")]
    pub audiences: Option<serde_json::Value>,
    /// Expiry, seconds since the epoch.
    #[serde(rename = "exp", default, skip_serializing_if = "is_zero")]
    pub expiration: f64,
    /// Not before, seconds since the epoch.
    #[serde(rename = "nbf", default, skip_serializing_if = "is_zero")]
    pub not_before: f64,
    /// Issued at, seconds since the epoch.
    #[serde(rename = "iat", default, skip_serializing_if = "is_zero")]
    pub issued_at: f64,
    /// Token id.
    #[serde(rename = "jti", default, skip_serializing_if = "String::is_empty")]
    pub jti: String,
    /// Hivekeep fields of an Actor's own token.
    #[serde(
        rename = "actor.hivekeep.io",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub actor: Option<serde_json::Value>,
    /// Hivekeep fields of a worker-for-actor token.
    #[serde(
        rename = "worker-for-actor.hivekeep.io",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub worker_for_actor: Option<serde_json::Value>,
}

fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

/// Whole seconds since the epoch (zero before it).
pub fn unix_seconds(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `t` truncated to whole seconds, the resolution of JWT timestamps.
pub fn truncate_to_seconds(t: SystemTime) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(unix_seconds(t))
}

impl Claims {
    /// The wire form. An Actor's own token may not carry a node.
    pub fn to_wire(&self) -> Result<WireClaims> {
        let actor =
            serde_json::to_value(&self.actor).map_err(|e| IdentityError::Sign(e.to_string()))?;
        let mut wire = WireClaims {
            issuer: self.issuer.clone(),
            subject: self.subject.clone(),
            audiences: Some(serde_json::Value::from(self.audiences.clone())),
            expiration: unix_seconds(self.expiration) as f64,
            not_before: unix_seconds(self.not_before) as f64,
            issued_at: unix_seconds(self.issued_at) as f64,
            jti: self.jti.clone(),
            ..Default::default()
        };
        match self.kind {
            TokenKind::Actor if !self.actor.node.is_empty() => {
                return Err(IdentityError::Sign(
                    "an actor's own token carries no node".into(),
                ));
            }
            TokenKind::Actor => wire.actor = Some(actor),
            TokenKind::WorkerForActor => wire.worker_for_actor = Some(actor),
        }
        Ok(wire)
    }

    /// Claims for an Actor's own token, valid for `lifetime` from `now`
    /// (truncated to seconds, so `exp` equals the reported expiry).
    pub fn for_actor(
        issuer: &str,
        hive: &str,
        actor_name: &str,
        actor_uid: &str,
        audiences: Vec<String>,
        lifetime: Duration,
        now: SystemTime,
    ) -> Result<Self> {
        Self::build(
            TokenKind::Actor,
            issuer,
            hive,
            actor_name,
            actor_uid,
            "",
            audiences,
            lifetime,
            now,
        )
    }

    /// Claims for a worker-for-actor token minted while the Actor is on
    /// `node`, valid for [`WORKER_FOR_ACTOR_TTL`].
    pub fn for_worker_for_actor(
        issuer: &str,
        hive: &str,
        actor_name: &str,
        actor_uid: &str,
        node: &str,
        audiences: Vec<String>,
        now: SystemTime,
    ) -> Result<Self> {
        Self::build(
            TokenKind::WorkerForActor,
            issuer,
            hive,
            actor_name,
            actor_uid,
            node,
            audiences,
            WORKER_FOR_ACTOR_TTL,
            now,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        kind: TokenKind,
        issuer: &str,
        hive: &str,
        actor_name: &str,
        actor_uid: &str,
        node: &str,
        audiences: Vec<String>,
        lifetime: Duration,
        now: SystemTime,
    ) -> Result<Self> {
        // Tokens are only issued bound to an audience.
        if audiences.is_empty() {
            return Err(IdentityError::Refused(
                "at least one audience must be requested".into(),
            ));
        }
        let now = truncate_to_seconds(now);
        Ok(Claims {
            kind,
            issuer: issuer.to_string(),
            subject: kind.subject(hive, actor_name),
            audiences,
            expiration: now + lifetime,
            not_before: now - NOT_BEFORE_BACKDATE,
            issued_at: now,
            jti: uuid::Uuid::new_v4().simple().to_string(),
            actor: ActorClaims {
                hive: hive.to_string(),
                actor_name: actor_name.to_string(),
                actor_uid: actor_uid.to_string(),
                node: node.to_string(),
            },
        })
    }
}

/// Checks that `issuer` is `https://<host>[:port]` with no path, query,
/// fragment or user info.
pub fn validate_issuer(issuer: &str) -> Result<()> {
    let bad = || {
        Err(IdentityError::Refused(format!(
            "issuer {issuer:?} must be https://<host>, with no path, query, or fragment"
        )))
    };
    let Some(authority) = issuer.strip_prefix("https://") else {
        return bad();
    };
    if authority.is_empty()
        || authority.contains(['/', '?', '#', '@'])
        || authority.chars().any(char::is_whitespace)
    {
        return bad();
    }
    let host = match authority.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((h, rest)) if rest.is_empty() || rest.strip_prefix(':').is_some_and(is_port) => h,
            _ => return bad(),
        },
        None => match authority.rsplit_once(':') {
            Some((h, port)) if is_port(port) => h,
            Some(_) => return bad(),
            None => authority,
        },
    };
    if host.is_empty() {
        return bad();
    }
    Ok(())
}

fn is_port(p: &str) -> bool {
    !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issuers() {
        for ok in [
            "https://idp.example.com",
            "https://idp.example.com:8443",
            "https://[::1]:443",
            "https://x",
        ] {
            assert!(validate_issuer(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "http://idp.example.com",
            "https://",
            "https://idp.example.com/",
            "https://idp.example.com/path",
            "https://idp.example.com?q",
            "https://idp.example.com#f",
            "https://user@idp.example.com",
            "https://:443",
            "https://idp:port",
            "idp.example.com",
        ] {
            assert!(validate_issuer(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn wire_form() {
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_750);
        let c = Claims::for_actor(
            "https://idp",
            "team-a",
            "agent",
            "uid-1",
            vec!["aud-1".into()],
            Duration::from_secs(600),
            now,
        )
        .unwrap();
        assert_eq!(c.subject, "actor/team-a/agent");
        assert_eq!(c.issued_at, UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let wire = serde_json::to_value(c.to_wire().unwrap()).unwrap();
        assert_eq!(wire["aud"], serde_json::json!(["aud-1"]));
        assert_eq!(wire["exp"], serde_json::json!(1_700_000_600.0));
        assert_eq!(wire["nbf"], serde_json::json!(1_699_999_700.0));
        assert_eq!(
            wire[ACTOR_CLAIM],
            serde_json::json!({"hive": "team-a", "actorName": "agent", "actorUID": "uid-1"})
        );
        assert!(wire.get(WORKER_FOR_ACTOR_CLAIM).is_none());

        let w = Claims::for_worker_for_actor(
            "https://idp",
            "team-a",
            "agent",
            "uid-1",
            "node-1",
            vec!["a".into()],
            now,
        )
        .unwrap();
        assert_eq!(w.subject, "worker-for-actor/team-a/agent");
        assert_eq!(
            w.expiration.duration_since(w.issued_at).unwrap(),
            WORKER_FOR_ACTOR_TTL
        );
        let wire = serde_json::to_value(w.to_wire().unwrap()).unwrap();
        assert_eq!(wire[WORKER_FOR_ACTOR_CLAIM]["node"], "node-1");
        assert!(wire.get(ACTOR_CLAIM).is_none());
    }

    #[test]
    fn refusals() {
        let now = SystemTime::now();
        assert!(Claims::for_actor(
            "https://idp",
            "h",
            "a",
            "u",
            vec![],
            Duration::from_secs(600),
            now
        )
        .is_err());
        let mut c = Claims::for_actor(
            "https://idp",
            "h",
            "a",
            "u",
            vec!["x".into()],
            Duration::from_secs(600),
            now,
        )
        .unwrap();
        c.actor.node = "n".into();
        assert!(c.to_wire().is_err());
    }
}
