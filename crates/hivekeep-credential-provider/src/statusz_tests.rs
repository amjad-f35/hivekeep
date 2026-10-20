use hivekeep_k8s::fake::FakeApiServer;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;

use super::*;
use crate::nsauthz::{HivePolicy, NamespaceAuthorizer, PolicyFile};
use crate::pb::v1::FetchSecretRequest;

const SECRET_VALUE: &str = "statusz-must-never-show-this";
const INJECTOR: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-egress";

/// A page over a provider that has already resolved a secret, so the tests
/// can check the value never surfaces.
async fn page() -> (StatusPage, FakeApiServer) {
    let fake = FakeApiServer::new();
    fake.insert(&Secret {
        metadata: ObjectMeta {
            name: Some("example-api".into()),
            namespace: Some("ns1".into()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "token".to_string(),
            ByteString(SECRET_VALUE.as_bytes().to_vec()),
        )])),
        ..Default::default()
    });
    let authz = NamespaceAuthorizer::new(PolicyFile {
        policies: vec![HivePolicy {
            hive: "team-a".into(),
            allowed_namespaces: vec!["ns1".into()],
            secret_selector: None,
        }],
    })
    .unwrap();
    let provider = KubeSecretProvider::new(fake.client(), Some(authz));
    provider
        .fetch(&FetchSecretRequest {
            uri: "hk-secret://k8s.io/default/ns1/example-api/token".into(),
            actor_spiffe_id: "spiffe://hivekeep-actor.local/actor/team-a/my-actor".into(),
        })
        .await
        .unwrap();
    let flags = BTreeMap::from([("injector-identity".to_string(), INJECTOR.to_string())]);
    (
        StatusPage::new(
            provider,
            "hivekeep-credential-provider --x".into(),
            flags,
            INJECTOR.into(),
        ),
        fake,
    )
}

fn get(page: &StatusPage, req: http::Request<()>) -> (http::StatusCode, String, String) {
    let (parts, ()) = req.into_parts();
    let resp = page.handle(&parts);
    let ct = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let status = resp.status();
    let body = futures::executor::block_on(http_body_util::BodyExt::collect(resp.into_body()))
        .unwrap()
        .to_bytes();
    (status, ct, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn statusz_json() {
    let (page, _fake) = page().await;
    for req in [
        http::Request::get("/statusz?format=json").body(()).unwrap(),
        http::Request::get("/statusz")
            .header("Accept", "application/json")
            .body(())
            .unwrap(),
    ] {
        let (status, ct, body) = get(&page, req);
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ct, "application/json");
        let data: StatusContext = serde_json::from_str(&body).unwrap();
        assert_eq!(
            data.policy_grants,
            Some(BTreeMap::from([(
                "team-a".to_string(),
                vec!["ns1".to_string()]
            )]))
        );
        assert_eq!(data.injector_san, INJECTOR);
        assert!(!data.version.is_empty());
        assert_eq!(data.provider_name, PROVIDER_NAME);
        assert!(
            !body.contains(SECRET_VALUE),
            "statusz JSON contains the secret value"
        );
    }
}

/// A resolved secret value on either rendering would be an exfiltration
/// path.
#[tokio::test]
async fn statusz_html_never_shows_secret() {
    let (page, _fake) = page().await;
    let (status, ct, body) = get(&page, http::Request::get("/statusz").body(()).unwrap());
    assert_eq!(status, http::StatusCode::OK);
    assert!(ct.contains("text/html"), "Content-Type = {ct:?}");
    assert!(
        body.contains("team-a"),
        "HTML page is missing the policy grants"
    );
    assert!(
        !body.contains(SECRET_VALUE),
        "statusz HTML contains the secret value"
    );
}

#[tokio::test]
async fn statusz_unknown_path() {
    let (page, _fake) = page().await;
    let (status, _, _) = get(&page, http::Request::get("/other").body(()).unwrap());
    assert_eq!(status, http::StatusCode::NOT_FOUND);
}

#[test]
fn html_escapes_values() {
    let ctx = StatusContext {
        version: "v".into(),
        provider_name: PROVIDER_NAME.into(),
        args: "--x=<script>".into(),
        flags: BTreeMap::from([("a".to_string(), "\"&'".to_string())]),
        injector_san: INJECTOR.into(),
        policy_grants: None,
    };
    let html = render_html(&ctx);
    assert!(!html.contains("<script>"));
    assert!(html.contains("&lt;script&gt;"));
    assert!(html.contains("&#34;&amp;&#39;"));
    assert!(html.contains("authorization disabled"));
}
