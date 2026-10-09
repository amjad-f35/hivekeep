//! Verifying tokens from an external issuer (e.g. the Kubernetes API
//! server's service-account issuer) against its published key set.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::*;
use hivekeep_identity::jwt::claims::{TokenKind, ACTOR_CLAIM, WORKER_FOR_ACTOR_CLAIM};
use hivekeep_identity::jwt::jwks::{parse_key_set, Jwk};
use hivekeep_identity::jwt::verifier::{
    JwksFileSource, KeySource, Verifier, KEY_REFRESH_INTERVAL, KEY_REFRESH_RETRY_INTERVAL,
};

fn issuer() -> Arc<FakeIssuer> {
    Arc::new(FakeIssuer::default())
}

fn verifier(ti: &Arc<FakeIssuer>) -> Verifier {
    Verifier::new(FAKE_ISSUER, vec![TEST_AUDIENCE.into()], ti.clone())
}

fn rs256(kid: &str, key: &hivekeep_identity::key::PrivateKey) -> String {
    mint_jwt(
        "RS256",
        Some(kid),
        SigningKey::Rsa(key),
        &valid_claims(FAKE_ISSUER),
    )
}

#[test]
fn verifier_retries_initial_discovery_failure() {
    let ti = issuer();
    ti.add(rsa_jwk("key", &rsa_key().public_key()));
    ti.fail_next(1);
    let v = verifier(&ti);
    let token = rs256("key", rsa_key());
    let now = SystemTime::now();
    assert!(
        v.verify(&token, now).is_err(),
        "first verify succeeded, want discovery failure"
    );
    assert!(
        v.verify(&token, now).is_err(),
        "second verify succeeded during retry cooldown"
    );
    assert_eq!(ti.requests(), 1, "key-set requests during retry cooldown");
    v.verify(&token, now + KEY_REFRESH_RETRY_INTERVAL)
        .expect("verify after retry cooldown");
}

#[test]
fn verifier_uses_cached_key_when_refresh_fails() {
    let ti = issuer();
    ti.add(rsa_jwk("key", &rsa_key().public_key()));
    let v = verifier(&ti);
    let token = rs256("key", rsa_key());
    let now = SystemTime::now();
    v.verify(&token, now).unwrap();
    ti.fail_next(1);
    v.verify(&token, now + KEY_REFRESH_INTERVAL)
        .expect("verify with failed refresh");
    v.verify(&token, now + KEY_REFRESH_INTERVAL + Duration::from_secs(1))
        .expect("verify during retry cooldown");
    assert_eq!(ti.requests(), 2, "key-set requests after failed refresh");
}

#[test]
fn verifier_returns_kubernetes_claims() {
    let ti = issuer();
    ti.add(rsa_jwk("key", &rsa_key().public_key()));
    let mut claims = valid_claims(FAKE_ISSUER);
    claims["kubernetes.io"] = serde_json::json!({
        "namespace": "hivekeep-system",
        "serviceaccount": {"name": "keeper", "uid": "sa-uid"},
        "pod": {"name": "keeper-abc", "uid": "pod-uid"},
        "node": {"name": "node-1", "uid": "node-uid"},
        "warnafter": 1_800_000_000u64,
    });
    let token = mint_jwt("RS256", Some("key"), SigningKey::Rsa(rsa_key()), &claims);
    let got = verifier(&ti).verify(&token, SystemTime::now()).unwrap();
    let k = &got.kubernetes;
    assert_eq!(k.namespace, "hivekeep-system");
    assert_eq!(k.service_account_name, "keeper");
    assert_eq!(k.service_account_uid, "sa-uid");
    assert_eq!(
        (k.pod_name.as_str(), k.pod_uid.as_str()),
        ("keeper-abc", "pod-uid")
    );
    assert_eq!(
        (k.node_name.as_str(), k.node_uid.as_str()),
        ("node-1", "node-uid")
    );
    assert_eq!(
        k.warn_after,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000))
    );

    // A token without them reports none.
    let plain = verifier(&ti)
        .verify(&rs256("key", rsa_key()), SystemTime::now())
        .unwrap();
    assert_eq!(plain.kubernetes, Default::default());
}

#[test]
fn verifier_caches_and_refreshes_keys() {
    let ti = issuer();
    let key1 = rsa_key();
    ti.add(rsa_jwk("key-1", &key1.public_key()));
    let v = Verifier::new(
        FAKE_ISSUER,
        vec!["other".into(), TEST_AUDIENCE.into()],
        ti.clone(),
    );
    let now = SystemTime::now();
    let token1 = rs256("key-1", key1);
    for _ in 0..2 {
        v.verify(&token1, now).expect("verify key-1");
    }
    assert_eq!(ti.requests(), 1, "requests after cached verification");

    let key2 = new_rsa_key();
    ti.add(rsa_jwk("key-2", &key2.public_key()));
    v.verify(&rs256("key-2", &key2), now)
        .expect("verify key-2 after rotation");
    assert_eq!(ti.requests(), 2, "requests after unknown key");

    let missing = rs256("missing", &key2);
    for _ in 0..2 {
        assert!(
            v.verify(&missing, now).is_err(),
            "verify(missing) succeeded"
        );
    }
    assert_eq!(ti.requests(), 2, "requests during unknown-key cooldown");

    ti.keys.lock().remove(0);
    assert!(
        v.verify(&token1, now + KEY_REFRESH_INTERVAL).is_err(),
        "removed key still verifies after periodic refresh"
    );
    assert_eq!(ti.requests(), 3, "requests after periodic refresh");
}

#[test]
fn verify_ecdsa_on_every_curve() {
    for (alg, crv) in [("ES256", "P-256"), ("ES384", "P-384"), ("ES512", "P-521")] {
        let ti = issuer();
        let key = EcKey::generate(crv);
        ti.add(key.jwk("ec-1"));
        let token = mint_jwt(
            alg,
            Some("ec-1"),
            SigningKey::Ec(&key),
            &valid_claims(FAKE_ISSUER),
        );
        verifier(&ti)
            .verify(&token, SystemTime::now())
            .unwrap_or_else(|e| panic!("verify({alg}): {e}"));

        // And a signature by another key on the same curve fails.
        let stranger = EcKey::generate(crv);
        let forged = mint_jwt(
            alg,
            Some("ec-1"),
            SigningKey::Ec(&stranger),
            &valid_claims(FAKE_ISSUER),
        );
        let err = verifier(&ti)
            .verify(&forged, SystemTime::now())
            .unwrap_err()
            .to_string();
        assert!(err.contains("signature"), "{alg}: {err}");
    }
}

#[test]
fn verify_rejects_ec_key_for_rs_alg() {
    let ti = issuer();
    ti.add(EcKey::generate("P-256").jwk("ec-1"));
    // The RSA signing key is irrelevant: the key-type check fails first.
    let token = rs256("ec-1", rsa_key());
    let err = verifier(&ti)
        .verify(&token, SystemTime::now())
        .unwrap_err()
        .to_string();
    assert!(err.contains("not an RSA key"), "{err}");
}

#[test]
fn verify_mixed_jwks() {
    let ti = issuer();
    ti.add(rsa_jwk("rsa-1", &rsa_key().public_key()));
    ti.add(EcKey::generate("P-256").jwk("ec-1"));
    verifier(&ti)
        .verify(&rs256("rsa-1", rsa_key()), SystemTime::now())
        .expect("verify with a mixed RSA+EC key set");
}

fn p192_jwk() -> Jwk {
    Jwk {
        kty: "EC".into(),
        kid: "ec-bad".into(),
        crv: "P-192".into(),
        x: "AA".into(),
        y: "AA".into(),
        ..Default::default()
    }
}

#[test]
fn verify_unusable_jwks_key_skipped() {
    let ti = issuer();
    ti.add(rsa_jwk("rsa-1", &rsa_key().public_key()));
    ti.add(p192_jwk());
    verifier(&ti)
        .verify(&rs256("rsa-1", rsa_key()), SystemTime::now())
        .expect("an unusable key in the set must be skipped");
    assert!(
        verifier(&ti)
            .verify(&rs256("ec-bad", rsa_key()), SystemTime::now())
            .is_err(),
        "accepted a token naming a skipped key"
    );
}

#[test]
fn discover_keys_all_unusable() {
    let ti = issuer();
    ti.add(p192_jwk());
    let err = ti.keys().unwrap_err().to_string();
    assert!(err.contains("no usable keys"), "{err}");
}

#[test]
fn discover_keys_empty_jwks() {
    let ti = issuer();
    let err = ti.keys().unwrap_err().to_string();
    assert!(err.contains("empty JWKS"), "{err}");
    let err = parse_key_set(br#"{"keys":[]}"#, "x")
        .unwrap_err()
        .to_string();
    assert!(err.contains("empty JWKS"), "{err}");
}

#[test]
fn file_key_source() {
    let ti = issuer();
    let key = EcKey::generate("P-256");
    ti.add(key.jwk("ec-1"));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jwks.json");
    std::fs::write(&path, ti.jwks_json()).unwrap();
    let v = Verifier::new(
        FAKE_ISSUER,
        vec![TEST_AUDIENCE.into()],
        Arc::new(JwksFileSource::new(&path)),
    );
    let claims = valid_claims(FAKE_ISSUER);
    v.verify(
        &mint_jwt("ES256", Some("ec-1"), SigningKey::Ec(&key), &claims),
        SystemTime::now(),
    )
    .unwrap();
    assert_eq!(ti.requests(), 0, "the file source asked the issuer");

    // Rotation: a key added to the file is found on the unknown-kid refresh.
    let next = EcKey::generate("P-256");
    ti.add(next.jwk("ec-2"));
    std::fs::write(&path, ti.jwks_json()).unwrap();
    v.verify(
        &mint_jwt("ES256", Some("ec-2"), SigningKey::Ec(&next), &claims),
        SystemTime::now(),
    )
    .expect("verify after rotation");
}

#[test]
fn file_key_source_errors() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.json");
    std::fs::write(&empty, r#"{"keys":[]}"#).unwrap();
    let garbage = dir.path().join("garbage.json");
    std::fs::write(&garbage, "not json").unwrap();
    for (name, path) in [
        ("missing file", dir.path().join("absent.json")),
        ("empty set", empty),
        ("not JSON", garbage),
    ] {
        assert!(JwksFileSource::new(path).keys().is_err(), "{name}");
    }
}

#[test]
fn verify_actor_claims() {
    let ti = issuer();
    let key = EcKey::generate("P-256");
    ti.add(key.jwk("ec-1"));
    let v = verifier(&ti).with_actor_claims();
    let with_claim = |name: &str, value: serde_json::Value| {
        let mut c = valid_claims(FAKE_ISSUER);
        c[name] = value;
        c
    };
    let mut both = with_claim(ACTOR_CLAIM, serde_json::json!({"hive": "a"}));
    both[WORKER_FOR_ACTOR_CLAIM] = serde_json::json!({"hive": "a"});

    type Check = fn(&Option<(TokenKind, hivekeep_identity::jwt::claims::ActorClaims)>) -> bool;
    let cases: Vec<(&str, serde_json::Value, &str, Option<Check>)> = vec![
        (
            "absent",
            valid_claims(FAKE_ISSUER),
            "",
            Some(|a| a.is_none()),
        ),
        (
            "actor",
            with_claim(
                ACTOR_CLAIM,
                serde_json::json!({"hive": "a", "actorName": "b", "actorUID": "u"}),
            ),
            "",
            Some(|a| matches!(a, Some((TokenKind::Actor, c)) if c.actor_uid == "u")),
        ),
        (
            "worker-for-actor",
            with_claim(WORKER_FOR_ACTOR_CLAIM, serde_json::json!({"hive": "a"})),
            "",
            Some(|a| matches!(a, Some((TokenKind::WorkerForActor, c)) if c.hive == "a")),
        ),
        (
            "not an object",
            with_claim(ACTOR_CLAIM, "junk".into()),
            ACTOR_CLAIM,
            None,
        ),
        (
            "null",
            with_claim(ACTOR_CLAIM, serde_json::Value::Null),
            "",
            Some(|a| a.is_none()),
        ),
        ("both", both, "both", None),
    ];
    for (name, claims, want_err, check) in cases {
        let got = v.verify(
            &mint_jwt("ES256", Some("ec-1"), SigningKey::Ec(&key), &claims),
            SystemTime::now(),
        );
        if !want_err.is_empty() {
            let err = got.expect_err(name).to_string();
            assert!(err.contains(want_err), "{name}: {err}");
            continue;
        }
        let got = got.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(check.unwrap()(&got.actor), "{name}: {:?}", got.actor);
    }
}

#[test]
fn verify_ignores_actor_claims_on_bad_signature() {
    // A malformed actor claim on a badly signed token fails on the
    // signature: the claim is not parsed before verification.
    let ti = issuer();
    ti.add(EcKey::generate("P-256").jwk("ec-1"));
    let stranger = EcKey::generate("P-256");
    let mut claims = valid_claims(FAKE_ISSUER);
    claims[ACTOR_CLAIM] = "junk".into();
    let err = verifier(&ti)
        .with_actor_claims()
        .verify(
            &mint_jwt("ES256", Some("ec-1"), SigningKey::Ec(&stranger), &claims),
            SystemTime::now(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("signature") && !err.contains(ACTOR_CLAIM),
        "{err}"
    );
}

#[test]
fn verify_leaves_actor_claims_alone_by_default() {
    // A verifier for another issuer neither fails on nor exposes whatever
    // that issuer put under Hivekeep's claim names.
    let ti = issuer();
    let key = EcKey::generate("P-256");
    ti.add(key.jwk("ec-1"));
    let mut claims = valid_claims(FAKE_ISSUER);
    claims[ACTOR_CLAIM] = "junk".into();
    claims[WORKER_FOR_ACTOR_CLAIM] = serde_json::json!({"hive": "a"});
    let got = verifier(&ti)
        .verify(
            &mint_jwt("ES256", Some("ec-1"), SigningKey::Ec(&key), &claims),
            SystemTime::now(),
        )
        .unwrap();
    assert!(got.actor.is_none(), "{:?}", got.actor);
}
