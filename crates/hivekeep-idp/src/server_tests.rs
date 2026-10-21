use std::sync::Arc;

use hivekeep_identity::jwt::authority::{
    JwtAlgorithm, JwtAuthority, JwtPool, RefreshingJwtPool, VerificationKey,
};
use hivekeep_identity::jwt::claims::Claims;
use hivekeep_identity::jwt::jwks::key_set_json;
use hivekeep_identity::IdentityError;
use http_body_util::BodyExt;
use parking_lot::Mutex;

use super::*;

const ISSUER: &str = "https://idp.hivekeep-system.svc";

/// A key source whose keys, or failure, a test can change.
#[derive(Clone)]
struct FakeKeys(Arc<Mutex<Result<Vec<VerificationKey>, String>>>);

impl FakeKeys {
    fn with(keys: Vec<VerificationKey>) -> Self {
        FakeKeys(Arc::new(Mutex::new(Ok(keys))))
    }

    fn failing() -> Self {
        FakeKeys(Arc::new(Mutex::new(Err("file does not exist".into()))))
    }

    fn set(&self, keys: Result<Vec<VerificationKey>, String>) {
        *self.0.lock() = keys;
    }
}

impl JwtSigner for FakeKeys {
    fn sign_jwt(&self, _claims: &Claims) -> hivekeep_identity::Result<String> {
        Err(IdentityError::Sign("not a signer".into()))
    }

    fn verification_keys(&self) -> hivekeep_identity::Result<Vec<VerificationKey>> {
        self.0.lock().clone().map_err(IdentityError::Keys)
    }
}

/// One freshly generated key per algorithm.
fn verification_keys(algorithms: &[JwtAlgorithm]) -> Vec<VerificationKey> {
    let mut pool = JwtPool::default();
    for alg in algorithms {
        pool.add(JwtAuthority::generate(*alg, None).unwrap())
            .unwrap();
    }
    pool.verification_keys().unwrap()
}

fn key_set(keys: &[VerificationKey]) -> String {
    String::from_utf8(key_set_json(keys).unwrap()).unwrap()
}

struct Reply {
    status: StatusCode,
    headers: http::HeaderMap,
    body: String,
}

fn get<S: JwtSigner>(s: &IdpServer<S>, method: Method, path: &str) -> Reply {
    let resp = s.handle(&method, path);
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = tokio_block_on(resp.into_body().collect())
        .unwrap()
        .to_bytes();
    Reply {
        status,
        headers,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

/// Polls a future that is ready on first poll (a `Full` body).
fn tokio_block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

#[test]
fn serves_documents() {
    let keys = verification_keys(&[JwtAlgorithm::Es256]);
    let s = IdpServer::new(ISSUER, FakeKeys::with(keys.clone())).unwrap();
    let cases = [
        (
            DISCOVERY_PATH,
            format!(
                "{{\"issuer\":\"{ISSUER}\",\
                 \"jwks_uri\":\"{ISSUER}/openid/v1/jwks\",\
                 \"response_types_supported\":[\"id_token\"],\
                 \"subject_types_supported\":[\"public\"],\
                 \"id_token_signing_alg_values_supported\":[\"ES256\"],\
                 \"claims_supported\":[\"iss\",\"sub\",\"aud\",\"exp\",\"nbf\",\"iat\",\"jti\",\"actor.hivekeep.io\",\"worker-for-actor.hivekeep.io\"]}}"
            ),
        ),
        (JWKS_PATH, key_set(&keys)),
    ];
    for (path, want) in cases {
        let r = get(&s, Method::GET, path);
        assert_eq!(r.status, StatusCode::OK, "GET {path}");
        assert_eq!(r.body, want, "GET {path}");
        assert_eq!(
            r.headers[header::CONTENT_TYPE],
            "application/json; charset=utf-8",
            "GET {path}"
        );
    }
}

#[test]
fn new_fails() {
    let keys = verification_keys(&[JwtAlgorithm::Es256]);
    let cases = [
        (
            "issuer not https",
            "http://idp.hivekeep-system.svc",
            FakeKeys::with(keys.clone()),
        ),
        (
            "issuer with path",
            "https://idp.example.com/prod",
            FakeKeys::with(keys.clone()),
        ),
        ("keys unreadable", ISSUER, FakeKeys::failing()),
        ("no keys", ISSUER, FakeKeys::with(Vec::new())),
    ];
    for (name, issuer, keys) in cases {
        assert!(IdpServer::new(issuer, keys).is_err(), "{name}: accepted");
    }
}

#[test]
fn follows_key_changes() {
    let first = verification_keys(&[JwtAlgorithm::Es256]);
    let keys = FakeKeys::with(first.clone());
    let s = IdpServer::new(ISSUER, keys.clone()).unwrap();
    assert_eq!(get(&s, Method::GET, JWKS_PATH).body, key_set(&first));

    let mut rotated = verification_keys(&[JwtAlgorithm::Rs256]);
    rotated.extend(first);
    keys.set(Ok(rotated.clone()));
    assert_eq!(
        get(&s, Method::GET, JWKS_PATH).body,
        key_set(&rotated),
        "after rotation"
    );

    for (name, change) in [
        ("read error", Err("file does not exist".to_string())),
        ("empty pool", Ok(Vec::new())),
    ] {
        keys.set(change);
        for path in [DISCOVERY_PATH, JWKS_PATH] {
            assert_eq!(
                get(&s, Method::GET, path).status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "after {name}: GET {path}"
            );
        }
    }
}

#[test]
fn discovery_advertises_pool_algorithms() {
    let s = IdpServer::new(
        ISSUER,
        FakeKeys::with(verification_keys(&[
            JwtAlgorithm::Rs256,
            JwtAlgorithm::Es256,
            JwtAlgorithm::Es256,
        ])),
    )
    .unwrap();
    let doc: serde_json::Value =
        serde_json::from_str(&get(&s, Method::GET, DISCOVERY_PATH).body).unwrap();
    assert_eq!(
        doc["id_token_signing_alg_values_supported"],
        serde_json::json!(["ES256", "RS256"])
    );
}

#[test]
fn methods_and_unknown_paths() {
    let s = IdpServer::new(
        ISSUER,
        FakeKeys::with(verification_keys(&[JwtAlgorithm::Es256])),
    )
    .unwrap();
    assert_eq!(get(&s, Method::GET, HEALTHZ_PATH).status, StatusCode::OK);

    let head = get(&s, Method::HEAD, JWKS_PATH);
    assert_eq!(head.status, StatusCode::OK);
    assert!(head.body.is_empty(), "HEAD returned a body");

    let post = get(&s, Method::POST, JWKS_PATH);
    assert_eq!(post.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(post.headers[header::ALLOW], "GET, HEAD");

    assert_eq!(
        get(&s, Method::GET, "/openid/v1/other").status,
        StatusCode::NOT_FOUND
    );
}

#[test]
fn publishes_refreshing_pool() {
    let pool = JwtPool::generate(JwtAlgorithm::Es256, None).unwrap();
    let key_id = pool.active_id().to_string();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("pool.json");
    std::fs::write(&file, pool.to_json().unwrap()).unwrap();
    let s = IdpServer::new(ISSUER, RefreshingJwtPool::open(&file).unwrap()).unwrap();

    let set: serde_json::Value =
        serde_json::from_str(&get(&s, Method::GET, JWKS_PATH).body).unwrap();
    let keys = set["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1, "{set}");
    let k = &keys[0];
    assert_eq!(k["kid"], key_id.as_str());
    assert_eq!(k["alg"], "ES256");
    assert!(k.get("d").is_none(), "published a private key: {k}");
}
