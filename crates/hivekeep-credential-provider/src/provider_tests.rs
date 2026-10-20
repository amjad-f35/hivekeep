use std::collections::BTreeMap;

use hivekeep_k8s::fake::{FakeApiServer, Reaction};
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;
use tonic::Code;

use super::*;
use crate::nsauthz::{HivePolicy, PolicyFile, SecretSelector};

const TEAM_A: &str = "spiffe://hivekeep-actor.local/actor/team-a/my-actor";
const TEAM_B: &str = "spiffe://hivekeep-actor.local/actor/team-b/my-actor";
const CREDENTIAL_LABEL: &str = "example.com/credential";

fn secret(name: &str, labels: &[(&str, &str)], value: &str) -> Secret {
    Secret {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("ns1".into()),
            labels: (!labels.is_empty()).then(|| {
                labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            }),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "token".to_string(),
            ByteString(value.as_bytes().to_vec()),
        )])),
        ..Default::default()
    }
}

fn example_api() -> Secret {
    secret("example-api", &[], "s3cr3t")
}

fn fake_with(secrets: &[Secret]) -> FakeApiServer {
    let fake = FakeApiServer::new();
    for s in secrets {
        fake.insert(s);
    }
    fake
}

fn policy(hive: &str, ns: &str, selector: Option<&[(&str, &str)]>) -> HivePolicy {
    HivePolicy {
        hive: hive.into(),
        allowed_namespaces: vec![ns.into()],
        secret_selector: selector.map(|pairs| SecretSelector {
            match_labels: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }),
    }
}

fn authz(policies: Vec<HivePolicy>) -> Option<NamespaceAuthorizer> {
    Some(NamespaceAuthorizer::new(PolicyFile { policies }).unwrap())
}

fn request(uri: &str, actor: &str) -> FetchSecretRequest {
    FetchSecretRequest {
        uri: uri.into(),
        actor_spiffe_id: actor.into(),
    }
}

fn uri(name: &str) -> String {
    format!("hk-secret://k8s.io/default/ns1/{name}/token")
}

#[tokio::test]
async fn fetch_secret_authorization() {
    let cases = [
        ("allowed", TEAM_A, uri("example-api"), Code::Ok),
        (
            "namespace not permitted",
            TEAM_A,
            "hk-secret://k8s.io/default/ns2/example-api/token".to_string(),
            Code::PermissionDenied,
        ),
        (
            "unknown hive",
            TEAM_B,
            uri("example-api"),
            Code::PermissionDenied,
        ),
        (
            "garbage identity",
            "not-a-spiffe-uri",
            uri("example-api"),
            Code::PermissionDenied,
        ),
    ];
    for (name, actor, uri, want) in cases {
        let fake = fake_with(&[example_api()]);
        let p = KubeSecretProvider::new(fake.client(), authz(vec![policy("team-a", "ns1", None)]));
        match p.fetch(&request(&uri, actor)).await {
            Ok(resp) => {
                assert_eq!(want, Code::Ok, "{name}: succeeded");
                assert_eq!(resp.opaque_bytes, b"s3cr3t", "{name}");
            }
            Err(s) => assert_eq!(s.code(), want, "{name}: {s}"),
        }
    }

    // With no authorizer configured, enforcement is bypassed entirely.
    let fake = fake_with(&[example_api()]);
    let p = KubeSecretProvider::new(fake.client(), None);
    p.fetch(&request(&uri("example-api"), "not-a-spiffe-uri"))
        .await
        .expect("disabled authorization enforced");
}

#[tokio::test]
async fn fetch_secret() {
    let cases = [
        ("explicit key", uri("example-api"), Code::Ok),
        (
            "missing key",
            "hk-secret://k8s.io/default/ns1/example-api/nope".to_string(),
            Code::NotFound,
        ),
        ("secret not found", uri("absent"), Code::NotFound),
        (
            "remote form rejected",
            "hk-secret://k8s.io/cluster/remote-east/ns1/example-api/token".to_string(),
            Code::InvalidArgument,
        ),
        (
            "bad uri",
            "hk-secret://vault.io/default/ns1/example-api".to_string(),
            Code::InvalidArgument,
        ),
    ];
    for (name, uri, want) in cases {
        let fake = fake_with(&[example_api()]);
        let p = KubeSecretProvider::new(fake.client(), None);
        match p.fetch(&request(&uri, "")).await {
            Ok(resp) => {
                assert_eq!(want, Code::Ok, "{name}: succeeded");
                assert_eq!(resp.opaque_bytes, b"s3cr3t", "{name}");
            }
            Err(s) => assert_eq!(s.code(), want, "{name}: {s}"),
        }
    }
}

#[tokio::test]
async fn fetch_secret_api_errors() {
    let cases = [
        (403, "Forbidden", Code::PermissionDenied),
        (500, "InternalError", Code::Unavailable),
    ];
    for (code, reason, want) in cases {
        let fake = fake_with(&[example_api()]);
        fake.prepend_reactor("get", "secrets", move |_| {
            Reaction::fail(code, reason, "injected")
        });
        let p = KubeSecretProvider::new(fake.client(), None);
        let err = p
            .fetch(&request(&uri("example-api"), ""))
            .await
            .unwrap_err();
        assert_eq!(err.code(), want, "{code}: {err}");
    }
}

/// A label-narrowed grant admits only Secrets carrying those labels; a
/// grant without a selector admits every Secret in the namespace.
#[tokio::test]
async fn fetch_secret_secret_narrowing() {
    let model = secret("model-key", &[(CREDENTIAL_LABEL, "true")], "s3cr3t");
    let ca_pool = secret("actor-id-ca-pool", &[], "private");
    let label: &[(&str, &str)] = &[(CREDENTIAL_LABEL, "true")];
    let cases = [
        (
            "no narrowing admits everything in the namespace",
            policy("team-a", "ns1", None),
            "actor-id-ca-pool",
            Code::Ok,
        ),
        (
            "label grant admits a labeled secret",
            policy("team-a", "ns1", Some(label)),
            "model-key",
            Code::Ok,
        ),
        (
            "label grant refuses an unlabeled secret",
            policy("team-a", "ns1", Some(label)),
            "actor-id-ca-pool",
            Code::PermissionDenied,
        ),
    ];
    for (name, pol, target, want) in cases {
        let fake = fake_with(&[model.clone(), ca_pool.clone()]);
        let p = KubeSecretProvider::new(fake.client(), authz(vec![pol]));
        match p.fetch(&request(&uri(target), TEAM_A)).await {
            Ok(resp) => {
                assert_eq!(want, Code::Ok, "{name}: succeeded");
                assert!(!resp.opaque_bytes.is_empty(), "{name}: no bytes");
            }
            Err(s) => assert_eq!(s.code(), want, "{name}: {s}"),
        }
    }
}

/// Under a label-narrowed grant an absent Secret and a mismatched one must
/// be indistinguishable, or a caller could enumerate the namespace.
#[tokio::test]
async fn label_grant_hides_existence() {
    let fake = fake_with(&[secret("actor-id-ca-pool", &[], "private")]);
    let p = KubeSecretProvider::new(
        fake.client(),
        authz(vec![policy(
            "team-a",
            "ns1",
            Some(&[(CREDENTIAL_LABEL, "true")]),
        )]),
    );
    let ask = |name: &'static str| {
        let p = p.clone();
        async move { p.fetch(&request(&uri(name), TEAM_A)).await.unwrap_err() }
    };
    let present = ask("actor-id-ca-pool").await;
    let absent = ask("no-such-secret").await;
    assert_eq!(present.code(), Code::PermissionDenied);
    assert_eq!(absent.code(), Code::PermissionDenied);
    assert_eq!(
        present.message().replacen("actor-id-ca-pool", "<name>", 1),
        absent.message().replacen("no-such-secret", "<name>", 1),
        "messages differ beyond the name, so existence leaks"
    );
}

/// While authorization is enforced a missing Secret is PermissionDenied even
/// under an unnarrowed grant; only disabled authorization reports NotFound.
#[tokio::test]
async fn missing_secret_is_permission_denied_when_enforced() {
    let fake = FakeApiServer::new();
    let p = KubeSecretProvider::new(fake.client(), authz(vec![policy("team-a", "ns1", None)]));
    let err = p
        .fetch(&request(&uri("no-such-secret"), TEAM_A))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
}

#[tokio::test]
async fn grants_absent_when_disabled() {
    let fake = FakeApiServer::new();
    assert!(KubeSecretProvider::new(fake.client(), None)
        .grants()
        .is_none());
}
