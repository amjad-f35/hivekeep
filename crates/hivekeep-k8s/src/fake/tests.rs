use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions};
use kube::runtime::watcher;

use super::*;

fn cm(name: &str) -> ConfigMap {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name, "labels": {"app": "x"}},
        "data": {"k": "v"}
    }))
    .unwrap()
}

fn status_code(err: kube::Error) -> u16 {
    match err {
        kube::Error::Api(s) => s.code,
        other => panic!("not an API error: {other}"),
    }
}

#[tokio::test]
async fn crud_and_conflicts() {
    let fake = FakeApiServer::new();
    let api: Api<ConfigMap> = Api::namespaced(fake.client(), "ns");
    let created = api.create(&PostParams::default(), &cm("a")).await.unwrap();
    assert_eq!(created.metadata.namespace.as_deref(), Some("ns"));
    assert!(created.metadata.uid.is_some());
    assert_eq!(
        status_code(
            api.create(&PostParams::default(), &cm("a"))
                .await
                .unwrap_err()
        ),
        409
    );

    let got = api.get("a").await.unwrap();
    assert_eq!(got.data.unwrap()["k"], "v");
    let list = api
        .list(&ListParams::default().labels("app=x"))
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert!(api
        .list(&ListParams::default().labels("app=y"))
        .await
        .unwrap()
        .items
        .is_empty());

    let mut stale = created.clone();
    let mut fresh = created.clone();
    fresh.data = Some([("k".to_string(), "w".to_string())].into());
    api.replace("a", &PostParams::default(), &fresh)
        .await
        .unwrap();
    stale.data = Some([("k".to_string(), "z".to_string())].into());
    assert_eq!(
        status_code(
            api.replace("a", &PostParams::default(), &stale)
                .await
                .unwrap_err()
        ),
        409
    );

    api.patch(
        "a",
        &PatchParams::default(),
        &Patch::Merge(json!({"data": {"m": "1"}})),
    )
    .await
    .unwrap();
    assert_eq!(api.get("a").await.unwrap().data.unwrap().len(), 2);

    let dp = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some("other".into()),
            resource_version: None,
        }),
        ..Default::default()
    };
    assert_eq!(status_code(api.delete("a", &dp).await.unwrap_err()), 409);
    api.delete("a", &DeleteParams::default()).await.unwrap();
    assert_eq!(status_code(api.get("a").await.unwrap_err()), 404);
}

#[tokio::test]
async fn finalizers_hold_deletion() {
    let fake = FakeApiServer::new();
    let api: Api<ConfigMap> = Api::namespaced(fake.client(), "ns");
    let mut obj = cm("f");
    obj.metadata.finalizers = Some(vec!["x/y".into()]);
    api.create(&PostParams::default(), &obj).await.unwrap();
    api.delete("f", &DeleteParams::default()).await.unwrap();
    let deleting = api.get("f").await.unwrap();
    assert!(deleting.metadata.deletion_timestamp.is_some());
    // No new finalizers on a deleting object.
    let err = api
        .patch(
            "f",
            &PatchParams::default(),
            &Patch::Merge(json!({"metadata": {"finalizers": ["x/y", "x/z"]}})),
        )
        .await
        .unwrap_err();
    assert_eq!(status_code(err), 422);
    api.patch(
        "f",
        &PatchParams::default(),
        &Patch::Merge(json!({"metadata": {"finalizers": null}})),
    )
    .await
    .unwrap();
    assert_eq!(status_code(api.get("f").await.unwrap_err()), 404);
}

#[tokio::test]
async fn status_subresource_and_apply() {
    let fake = FakeApiServer::new();
    let api: Api<Pod> = Api::namespaced(fake.client(), "ns");
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "image": "i"}]}
    }))
    .unwrap();
    api.create(&PostParams::default(), &pod).await.unwrap();
    api.patch_status(
        "p",
        &PatchParams::default(),
        &Patch::Merge(json!({"status": {"phase": "Running"}})),
    )
    .await
    .unwrap();
    // A main-resource write keeps status.
    api.patch(
        "p",
        &PatchParams::default(),
        &Patch::Merge(json!({"status": {"phase": "Failed"}, "metadata": {"labels": {"a": "b"}}})),
    )
    .await
    .unwrap();
    let got = api.get("p").await.unwrap();
    assert_eq!(got.status.unwrap().phase.as_deref(), Some("Running"));
    assert_eq!(got.metadata.labels.unwrap()["a"], "b");

    let pp = PatchParams::apply("mgr").force();
    let applied = json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "labels": {"own": "1"}},
        "spec": {"containers": [{"name": "c", "image": "j"}]}});
    api.patch("p", &pp, &Patch::Apply(&applied)).await.unwrap();
    let applied2 = json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
        "spec": {"containers": [{"name": "c", "image": "j"}]}});
    api.patch("p", &pp, &Patch::Apply(&applied2)).await.unwrap();
    let labels = api.get("p").await.unwrap().metadata.labels.unwrap();
    assert!(
        !labels.contains_key("own"),
        "applied-then-dropped label removed"
    );
    assert_eq!(labels["a"], "b", "unowned label kept");
}

#[tokio::test]
async fn reactors_and_actions() {
    let fake = FakeApiServer::new();
    fake.prepend_reactor("get", "configmaps", |_| {
        Reaction::fail(403, "Forbidden", "denied")
    });
    let api: Api<ConfigMap> = Api::namespaced(fake.client(), "ns");
    assert_eq!(status_code(api.get("x").await.unwrap_err()), 403);
    assert!(fake.actions().iter().any(|a| a.is("get", "configmaps")));
}

#[tokio::test]
async fn watch_streams_changes() {
    let fake = FakeApiServer::new();
    let api: Api<ConfigMap> = Api::namespaced(fake.client(), "ns");
    api.create(&PostParams::default(), &cm("a")).await.unwrap();
    let mut events = watcher(api.clone(), watcher::Config::default().labels("app=x")).boxed();
    let mut names = Vec::new();
    let collect = async {
        while let Some(ev) = events.next().await {
            match ev.unwrap() {
                watcher::Event::InitApply(o) | watcher::Event::Apply(o) => {
                    names.push(o.metadata.name.unwrap());
                    if names.len() == 2 {
                        break;
                    }
                }
                watcher::Event::InitDone => {
                    api.create(&PostParams::default(), &cm("b")).await.unwrap();
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), collect)
        .await
        .unwrap();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn discovery_documents() {
    let fake = FakeApiServer::new();
    fake.serve(
        "certificates.k8s.io/v1beta1",
        &[("clustertrustbundles", "ClusterTrustBundle", false)],
    );
    let client = fake.client();
    let list = client
        .list_api_group_resources("certificates.k8s.io/v1beta1")
        .await
        .unwrap();
    assert_eq!(list.resources[0].name, "clustertrustbundles");
    assert!(client
        .list_api_group_resources("certificates.k8s.io/v1")
        .await
        .is_err());
}
