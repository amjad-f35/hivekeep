//! JWT signing authorities: pools, rotation, key publication and
//! thumbprints.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use hivekeep_identity::jwt::authority::{
    JwtAlgorithm, JwtAuthority, JwtPool, JwtSigner, RefreshingJwtPool, VerificationKey,
};
use hivekeep_identity::jwt::claims::{ActorClaims, Claims, TokenKind};
use hivekeep_identity::jwt::jwks::key_set_json;
use hivekeep_identity::jwt::verifier::{JwksFileSource, Verifier};
use hivekeep_identity::key::{KeyAlgorithm, KeyComponents, KeyKind, PrivateKey};

fn vk(
    id: &str,
    algorithm: JwtAlgorithm,
    public_key: hivekeep_identity::key::PublicKey,
) -> VerificationKey {
    VerificationKey {
        key_id: id.into(),
        algorithm,
        public_key,
    }
}

fn jwks_value(keys: &[VerificationKey]) -> serde_json::Value {
    serde_json::from_slice(&key_set_json(keys).unwrap()).unwrap()
}

#[test]
fn jwks() {
    let got = jwks_value(&[
        vk("rsa", JwtAlgorithm::Rs256, rfc_rsa_key()),
        vk("ec", JwtAlgorithm::Es256, rfc_ec_key()),
    ]);
    assert_eq!(
        got["keys"],
        serde_json::json!([
            {"kty": "EC", "kid": "ec", "use": "sig", "alg": "ES256", "crv": "P-256", "x": RFC_EC_X, "y": RFC_EC_Y},
            {"kty": "RSA", "kid": "rsa", "use": "sig", "alg": "RS256", "n": RFC_RSA_N, "e": RFC_RSA_E},
        ])
    );
}

#[test]
fn jwks_round_trip() {
    let rsa = new_rsa_key().public_key();
    let ec = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
        .unwrap()
        .public_key();
    let keys = [
        vk("a", JwtAlgorithm::Rs384, rsa.clone()),
        vk("b", JwtAlgorithm::Es256, ec.clone()),
    ];
    let set = jwks_value(&keys);
    let published = set["keys"].as_array().unwrap();
    assert_eq!(published.len(), keys.len());
    for (key, j) in keys.iter().zip(published) {
        assert_eq!(j["kid"], key.key_id.as_str());
        assert_eq!(j["alg"], key.algorithm.as_str());
        assert_eq!(j["use"], "sig");
        let s = |f: &str| unb64(j[f].as_str().unwrap());
        let decoded = match j["kty"].as_str().unwrap() {
            "RSA" => rsa_spki(&s("n"), &s("e")),
            "EC" => p256_spki(&s("x"), &s("y")),
            other => panic!("kty {other}"),
        };
        assert_eq!(
            decoded.components().unwrap(),
            key.public_key.components().unwrap(),
            "key {} did not decode back to the original",
            key.key_id
        );
    }
}

#[test]
fn jwks_rejects() {
    let rsa = rfc_rsa_key();
    let ec = rfc_ec_key();
    let ed = PrivateKey::generate(KeyAlgorithm::Ed25519)
        .unwrap()
        .public_key();
    let cases: Vec<(&str, Vec<VerificationKey>)> = vec![
        ("no keys", vec![]),
        ("missing ID", vec![vk("", JwtAlgorithm::Rs256, rsa.clone())]),
        (
            "duplicate ID",
            vec![
                vk("k", JwtAlgorithm::Rs256, rsa.clone()),
                vk("k", JwtAlgorithm::Es256, ec.clone()),
            ],
        ),
        (
            "ES256 with RSA key",
            vec![vk("k", JwtAlgorithm::Es256, rsa.clone())],
        ),
        (
            "RS256 with EC key",
            vec![vk("k", JwtAlgorithm::Rs256, ec.clone())],
        ),
        (
            "P-384 key",
            vec![vk("k", JwtAlgorithm::Es256, p384_public_key())],
        ),
        (
            "unsupported key type",
            vec![vk("k", JwtAlgorithm::Es256, ed)],
        ),
    ];
    for (name, keys) in cases {
        assert!(key_set_json(&keys).is_err(), "{name}: published");
    }
    // "missing algorithm", "HMAC algorithm" and "EdDSA": algorithms are a
    // closed enum, so the only way in is parsing, which refuses them.
    for alg in ["", "HS256", "EdDSA", "ES384"] {
        assert!(alg.parse::<JwtAlgorithm>().is_err(), "{alg:?} parsed");
    }
}

#[test]
fn to_jwk_rejects_other_curves() {
    let p384 = p384_public_key();
    assert_eq!(p384.kind(), KeyKind::Other);
    assert!(p384.components().is_err());
    assert!(key_set_json(&[vk("k", JwtAlgorithm::Es256, p384)]).is_err());
}

#[test]
fn jwks_pads_ec_coordinates() {
    // RFC 7518 section 6.2.1.2: coordinates are full-width even when they
    // start with a zero byte.
    let key = loop {
        let k = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
            .unwrap()
            .public_key();
        let spki = k.spki_der();
        let point = &spki[spki.len() - 65..];
        if point[1] == 0 || point[33] == 0 {
            break k;
        }
    };
    let set = jwks_value(&[vk("k", JwtAlgorithm::Es256, key)]);
    let x = unb64(set["keys"][0]["x"].as_str().unwrap());
    let y = unb64(set["keys"][0]["y"].as_str().unwrap());
    assert_eq!((x.len(), y.len()), (32, 32));
}

const FILE_SOURCE_ISSUER: &str = "https://idp.hivekeep-system.svc";

fn worker_token(pool: &JwtPool, now: SystemTime) -> String {
    pool.sign_jwt(&Claims {
        kind: TokenKind::WorkerForActor,
        issuer: FILE_SOURCE_ISSUER.into(),
        subject: "worker-for-actor/a/b".into(),
        audiences: vec!["aud".into()],
        expiration: now + Duration::from_secs(3600),
        not_before: now - Duration::from_secs(300),
        issued_at: now,
        jti: String::new(),
        actor: ActorClaims {
            hive: "a".into(),
            actor_name: "b".into(),
            actor_uid: "u".into(),
            node: String::new(),
        },
    })
    .unwrap()
}

/// The pool's published key set is what a file-backed verifier needs:
/// tokens the pool signs verify, across a rotation, and a stranger's do not.
#[test]
fn jwks_feeds_file_key_source() {
    let first = JwtAuthority::generate(JwtAlgorithm::Es256, Some("first")).unwrap();
    let mut pool = JwtPool::new(vec![first], "first");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jwks.json");
    let publish = |pool: &JwtPool| {
        std::fs::write(
            &path,
            key_set_json(&pool.verification_keys().unwrap()).unwrap(),
        )
        .unwrap()
    };
    publish(&pool);

    let now = SystemTime::now();
    let v = Verifier::new(
        FILE_SOURCE_ISSUER,
        vec!["aud".into()],
        Arc::new(JwksFileSource::new(&path)),
    )
    .with_actor_claims();
    let got = v.verify(&worker_token(&pool, now), now).unwrap();
    assert_eq!(got.subject, "worker-for-actor/a/b");
    assert!(
        matches!(&got.actor, Some((TokenKind::WorkerForActor, c)) if c.hive == "a"),
        "{:?}",
        got.actor
    );

    pool.add(JwtAuthority::generate(JwtAlgorithm::Es256, Some("next")).unwrap())
        .unwrap();
    pool.activate("next").unwrap();
    publish(&pool);
    v.verify(&worker_token(&pool, now), now)
        .expect("verify after rotation");

    let stranger = JwtPool::generate(JwtAlgorithm::Es256, Some("stranger")).unwrap();
    let later = now + Duration::from_secs(120);
    assert!(
        v.verify(&worker_token(&stranger, later), later).is_err(),
        "a token from an unknown authority verified"
    );
}

#[test]
fn refreshing_pool() {
    let ca1 = JwtAuthority::generate(JwtAlgorithm::Es256, Some("1")).unwrap();
    let ca2 = JwtAuthority::generate(JwtAlgorithm::Es256, Some("2")).unwrap();
    let pools = [
        JwtPool::new(vec![ca1.clone()], "1"),
        JwtPool::new(vec![ca1.clone(), ca2.clone()], "1"),
        JwtPool::new(vec![ca1, ca2.clone()], "2"),
        JwtPool::new(vec![ca2], "2"),
    ];
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool.json");
    std::fs::write(&path, pools[0].to_json().unwrap()).unwrap();
    let period = Duration::from_millis(100);
    let refreshing = RefreshingJwtPool::with_period(&path, period).unwrap();
    assert_eq!(
        refreshing.verification_keys().unwrap(),
        pools[0].verification_keys().unwrap()
    );
    for (i, want) in pools.iter().enumerate().skip(1) {
        std::fs::write(&path, want.to_json().unwrap()).unwrap();
        std::thread::sleep(period + Duration::from_millis(20));
        assert_eq!(
            refreshing.verification_keys().unwrap(),
            want.verification_keys().unwrap(),
            "after update {i}"
        );
        assert_eq!(refreshing.current().unwrap().active_id(), want.active_id());
    }
}

fn minimal_claims() -> Claims {
    Claims {
        kind: TokenKind::Actor,
        issuer: String::new(),
        subject: "actor/a/b".into(),
        audiences: vec!["aud".into()],
        expiration: UNIX_EPOCH,
        not_before: UNIX_EPOCH,
        issued_at: UNIX_EPOCH,
        jti: String::new(),
        actor: ActorClaims::default(),
    }
}

fn header_of(jwt: &str) -> String {
    String::from_utf8(unb64(jwt.split('.').next().unwrap())).unwrap()
}

#[test]
fn sign_jwt_header() {
    let pool = JwtPool::generate(JwtAlgorithm::Es256, Some("key-1")).unwrap();
    let header: serde_json::Value =
        serde_json::from_str(&header_of(&pool.sign_jwt(&minimal_claims()).unwrap())).unwrap();
    assert_eq!(
        header,
        serde_json::json!({"typ": "JWT", "alg": "ES256", "kid": "key-1"})
    );
}

fn ids(p: &JwtPool) -> Vec<String> {
    p.authorities().iter().map(|a| a.id().to_string()).collect()
}

fn test_pool(ids: &[&str]) -> JwtPool {
    JwtPool::new(
        ids.iter()
            .map(|id| JwtAuthority::generate(JwtAlgorithm::Es256, Some(id)).unwrap())
            .collect(),
        ids[0],
    )
}

#[test]
fn pool_rotation() {
    let mut pool = test_pool(&["old"]);
    pool.add(JwtAuthority::generate(JwtAlgorithm::Rs256, Some("next")).unwrap())
        .unwrap();
    assert_eq!(pool.active_id(), "old", "adding activated the authority");
    pool.activate("next").unwrap();
    pool.remove("old").unwrap();
    assert_eq!(ids(&pool), ["next"]);
    let jwt = pool.sign_jwt(&minimal_claims()).unwrap();
    assert_eq!(
        header_of(&jwt),
        r#"{"typ":"JWT","alg":"RS256","kid":"next"}"#
    );
}

#[test]
fn pool_rotation_rejects() {
    let dup = JwtAuthority::generate(JwtAlgorithm::Es256, Some("1")).unwrap();
    type Change = Box<dyn Fn(&mut JwtPool) -> hivekeep_identity::Result<()>>;
    let undesignated = {
        let p = test_pool(&["1", "2"]);
        JwtPool::new(p.authorities().to_vec(), "")
    };
    let cases: Vec<(&str, JwtPool, Change)> = vec![
        (
            "add duplicate ID",
            test_pool(&["1", "2"]),
            Box::new(move |p| p.add(dup.clone())),
        ),
        (
            "activate absent",
            test_pool(&["1"]),
            Box::new(|p| p.activate("2")),
        ),
        (
            "remove absent",
            test_pool(&["1"]),
            Box::new(|p| p.remove("2")),
        ),
        (
            "remove active",
            test_pool(&["1", "2"]),
            Box::new(|p| p.remove("1")),
        ),
        (
            "remove first with none designated",
            undesignated,
            Box::new(|p| p.remove("1")),
        ),
    ];
    for (name, mut pool, change) in cases {
        let (want_ids, want_active) = (ids(&pool), pool.active_id().to_string());
        assert!(change(&mut pool).is_err(), "{name}: got no error");
        assert_eq!(ids(&pool), want_ids, "{name}: authorities changed");
        assert_eq!(pool.active_id(), want_active, "{name}: active changed");
    }
    // "add without ID": an authority without an id cannot be built.
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
    assert!(JwtAuthority::new("", JwtAlgorithm::Es256, key).is_err());
}

#[test]
fn pool_active_id() {
    let designated = {
        let mut p = test_pool(&["1", "2"]);
        p.activate("2").unwrap();
        p
    };
    let undesignated = JwtPool::new(test_pool(&["1", "2"]).authorities().to_vec(), "");
    for (name, pool, want) in [
        ("designated", designated, "2"),
        ("none designated", undesignated, "1"),
        ("empty", JwtPool::default(), ""),
    ] {
        assert_eq!(pool.active_id(), want, "{name}");
    }
}

#[test]
fn generate_authority() {
    for alg in [JwtAlgorithm::Rs256, JwtAlgorithm::Es256] {
        let a = JwtAuthority::generate(alg, None).unwrap();
        assert_eq!(a.algorithm(), alg);
        let public = a.verification_key().public_key;
        let thumbprint = public.thumbprint().unwrap();
        assert_eq!(a.id(), thumbprint, "{alg}: id is not the key thumbprint");
        match (alg, public.components().unwrap()) {
            (JwtAlgorithm::Rs256, KeyComponents::Rsa { n, .. }) => {
                assert_eq!(n.len() * 8, 4096, "RS256 key size")
            }
            (JwtAlgorithm::Es256, KeyComponents::P256 { .. }) => {}
            (alg, other) => panic!("{alg}: unexpected key {other:?}"),
        }
        let pool = JwtPool::new(vec![a.clone()], a.id());
        let loaded = JwtPool::from_json(&pool.to_json().unwrap()).unwrap();
        let header: serde_json::Value =
            serde_json::from_str(&header_of(&loaded.sign_jwt(&minimal_claims()).unwrap())).unwrap();
        assert_eq!(header["alg"], alg.as_str());
        assert_eq!(header["kid"], thumbprint.as_str());
    }
}

#[test]
fn generate_authority_explicit_id() {
    let a = JwtAuthority::generate(JwtAlgorithm::Rs256, Some("my-key")).unwrap();
    assert_eq!(a.id(), "my-key");
}

#[test]
fn generate_pool() {
    for (alg, id) in [
        (JwtAlgorithm::Es256, None),
        (JwtAlgorithm::Rs256, Some("my-key")),
    ] {
        let generated = JwtPool::generate(alg, id).unwrap();
        let returned = generated.active_id().to_string();
        let pool = JwtPool::from_json(&generated.to_json().unwrap()).unwrap();
        assert_eq!(pool.authorities().len(), 1);
        let a = &pool.authorities()[0];
        assert_eq!(a.algorithm(), alg);
        if let Some(id) = id {
            assert_eq!(returned, id);
        }
        assert_eq!(a.id(), returned);
        assert_eq!(pool.active_id(), returned);
    }
    // HS256 cannot even be named.
    assert!("HS256".parse::<JwtAlgorithm>().is_err());
}

#[test]
fn generate_authority_rejects_unsupported_algorithm() {
    for alg in [JwtAlgorithm::Rs384, JwtAlgorithm::Rs512] {
        assert!(JwtAuthority::generate(alg, None).is_err(), "{alg}");
    }
    assert!("HS256".parse::<JwtAlgorithm>().is_err());
}

#[test]
fn verification_keys_carry_algorithm() {
    let es = JwtAuthority::generate(JwtAlgorithm::Es256, Some("es")).unwrap();
    let rs = JwtAuthority::generate(JwtAlgorithm::Rs256, Some("rs")).unwrap();
    let pool = JwtPool::new(vec![es, rs], "es");
    let got: std::collections::BTreeMap<String, JwtAlgorithm> = pool
        .verification_keys()
        .unwrap()
        .into_iter()
        .map(|k| (k.key_id, k.algorithm))
        .collect();
    assert_eq!(
        got,
        [
            ("es".to_string(), JwtAlgorithm::Es256),
            ("rs".to_string(), JwtAlgorithm::Rs256)
        ]
        .into()
    );
}

/// Expected values come from
/// `openssl pkey -pubin -outform DER | openssl dgst -sha256 -binary | basenc --base64url`.
#[test]
fn thumbprint() {
    for (name, key, want) in [
        (
            "RSA",
            rfc_rsa_key(),
            "rTIyDPbFltiEsFOBulc6uo3dV0m03o9KI6efmondrrI",
        ),
        (
            "P-256",
            rfc_ec_key(),
            "UblEzfpUTUwyc6pr81BiWn3VO7tqcXIydPU4sZogd2A",
        ),
    ] {
        assert_eq!(key.thumbprint().unwrap(), want, "{name}");
    }
    // Generated keys encode the same way as hand-built ones.
    let ec = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
        .unwrap()
        .public_key();
    let KeyComponents::P256 { x, y } = ec.components().unwrap() else {
        unreachable!()
    };
    assert_eq!(
        ec.thumbprint().unwrap(),
        p256_spki(&x, &y).thumbprint().unwrap()
    );
    let rsa = rsa_key().public_key();
    let KeyComponents::Rsa { n, e } = rsa.components().unwrap() else {
        unreachable!()
    };
    assert_eq!(
        rsa.thumbprint().unwrap(),
        rsa_spki(&n, &e).thumbprint().unwrap()
    );
}

#[test]
fn thumbprint_rejects_unsupported_keys() {
    let ed = PrivateKey::generate(KeyAlgorithm::Ed25519)
        .unwrap()
        .public_key();
    for key in [p384_public_key(), ed] {
        assert!(key.thumbprint().is_err(), "{key:?}");
    }
}
