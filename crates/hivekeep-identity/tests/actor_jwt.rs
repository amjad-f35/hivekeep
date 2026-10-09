//! Actor token claims: issuer rules, wire form and token kinds.

use std::time::{Duration, UNIX_EPOCH};

use hivekeep_identity::jwt::claims::{
    validate_issuer, ActorClaims, Claims, TokenKind, ACTOR_CLAIM, WORKER_FOR_ACTOR_CLAIM,
};

#[test]
fn validate_issuer_table() {
    for (name, issuer, want_err) in [
        (
            "in-cluster default",
            "https://idp.hivekeep-system.svc",
            false,
        ),
        ("public host", "https://idp.example.com", false),
        ("port", "https://idp.example.com:8443", false),
        ("empty", "", true),
        ("http", "http://idp.example.com", true),
        ("uppercase scheme", "HTTPS://idp.example.com", true),
        ("no scheme", "idp.example.com", true),
        ("no host", "https:///prod", true),
        ("port without host", "https://:443", true),
        ("opaque", "https:idp.example.com", true),
        ("user info", "https://user:pass@idp.example.com", true),
        ("trailing slash", "https://idp.example.com/", true),
        ("path", "https://idp.example.com/prod", true),
        ("query", "https://idp.example.com?cluster=prod", true),
        ("empty query", "https://idp.example.com?", true),
        ("fragment", "https://idp.example.com#prod", true),
        ("empty fragment", "https://idp.example.com#", true),
    ] {
        assert_eq!(
            validate_issuer(issuer).is_err(),
            want_err,
            "{name}: validate_issuer({issuer:?})"
        );
    }
}

fn base(kind: TokenKind) -> Claims {
    Claims {
        kind,
        issuer: "https://idp.hivekeep-system.svc".into(),
        subject: "x".into(),
        audiences: vec!["a".into()],
        expiration: UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        not_before: UNIX_EPOCH,
        issued_at: UNIX_EPOCH,
        jti: String::new(),
        actor: ActorClaims {
            hive: "team-a".into(),
            actor_name: "my-actor".into(),
            actor_uid: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21".into(),
            node: String::new(),
        },
    }
}

#[test]
fn claims_to_wire_claim_name_per_kind() {
    for (kind, want, other) in [
        (TokenKind::Actor, ACTOR_CLAIM, WORKER_FOR_ACTOR_CLAIM),
        (
            TokenKind::WorkerForActor,
            WORKER_FOR_ACTOR_CLAIM,
            ACTOR_CLAIM,
        ),
    ] {
        let payload = serde_json::to_value(base(kind).to_wire().unwrap()).unwrap();
        let raw = payload.as_object().unwrap();
        assert!(
            raw.contains_key(want),
            "{kind:?}: {want} missing from {payload}"
        );
        for absent in [other, "hivekeep.io"] {
            assert!(
                !raw.contains_key(absent),
                "{kind:?}: unexpected {absent} in {payload}"
            );
        }
        let got: ActorClaims = serde_json::from_value(raw[want].clone()).unwrap();
        assert_eq!(got, base(kind).actor, "{kind:?}");
    }
}

#[test]
fn claims_to_wire_node_on_worker_tokens_only() {
    let mut claims = base(TokenKind::WorkerForActor);
    claims.actor.node = "node-1".into();
    let wire = serde_json::to_value(claims.to_wire().unwrap()).unwrap();
    assert_eq!(wire[WORKER_FOR_ACTOR_CLAIM]["node"], "node-1");
    claims.kind = TokenKind::Actor;
    assert!(
        claims.to_wire().is_err(),
        "accepted a node on an actor's own token"
    );
}

#[test]
fn kind_names() {
    for (kind, prefix, claim) in [
        (TokenKind::Actor, "actor", "actor.hivekeep.io"),
        (
            TokenKind::WorkerForActor,
            "worker-for-actor",
            "worker-for-actor.hivekeep.io",
        ),
    ] {
        assert_eq!(kind.subject_prefix(), prefix);
        assert_eq!(kind.claim_name(), claim);
        assert_eq!(kind.subject("h", "n"), format!("{prefix}/h/n"));
    }
}
