//! Requests the Actor and Tag commands build, the flag values they parse,
//! and their read-modify-write updates against a fake Control server.

mod common;

use common::*;
use hivekeep_core::resource::ActorRef;
use hivekeep_ctl::client::Api;
use hivekeep_ctl::commands::actor::{
    build_create_request, build_suspend_request, update_actor, ActorUpdate,
};
use hivekeep_ctl::commands::tag::update_scope;
use hivekeep_ctl::connection::{ConnectionConfig, RequestDecorator};
use hivekeep_ctl::refs::{
    hived_ref, parse_ref, parse_suspend_mode, parse_tag_scope, parse_worker_selector,
};
use hivekeep_ctl::verbosity::Logger;
use hivekeep_proto::api::v1 as pb;
use tonic::Status;

async fn connect(fake: &Fake) -> Api {
    let addr = serve(fake.clone()).await;
    let cfg = ConnectionConfig {
        endpoint: Some(addr.to_string()),
        plaintext: true,
        ..Default::default()
    };
    let ch = cfg.connect(&Logger::silent()).await.unwrap();
    Api::new(ch, RequestDecorator::default(), Logger::silent())
}

fn selector(labels: &[(&str, &str)]) -> Option<pb::Selector> {
    Some(pb::Selector {
        match_labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    })
}

#[test]
fn build_create_actor_request() {
    let actor = |template: (&str, &str), tag: Option<(&str, &str)>| pb::CreateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "demo".into(),
                name: "my-counter".into(),
                ..Default::default()
            }),
            actor_template: Some(hived_ref(template.0, template.1)),
            source_tag: tag.map(|(h, n)| hived_ref(h, n)),
            ..Default::default()
        }),
    };
    for (name, template, tag, want) in [
        (
            "bare template name defaults to the actor's hive",
            "counter",
            None,
            Some(actor(("demo", "counter"), None)),
        ),
        (
            "bare tag name defaults to the actor's hive",
            "counter",
            Some("before-upgrade"),
            Some(actor(("demo", "counter"), Some(("demo", "before-upgrade")))),
        ),
        (
            "qualified tag in a different hive",
            "counter",
            Some("other-hive/before-upgrade"),
            Some(actor(
                ("demo", "counter"),
                Some(("other-hive", "before-upgrade")),
            )),
        ),
        (
            "qualified template in a different hive",
            "shared-templates/counter",
            None,
            Some(actor(("shared-templates", "counter"), None)),
        ),
        ("malformed template ref", "a/b/c", None, None),
        ("malformed tag", "counter", Some("a/b/c"), None),
    ] {
        match (
            build_create_request("my-counter", "demo", template, tag),
            want,
        ) {
            (Ok(got), Some(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), None) => assert!(e.is_usage(), "{name}: {e}"),
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn build_suspend_actor_request() {
    for mode in [
        pb::DurabilityMode::Asynchronous,
        pb::DurabilityMode::Synchronous,
    ] {
        assert_eq!(
            build_suspend_request(&ActorRef::new("demo", "my-counter"), mode),
            pb::SuspendActorRequest {
                actor: Some(hived_ref("demo", "my-counter")),
                options: Some(pb::SuspendOptions { mode: mode as i32 }),
            },
            "{mode:?}"
        );
    }
}

#[test]
fn parse_suspend_mode_cases() {
    for (name, mode, want) in [
        ("async", "async", Ok(pb::DurabilityMode::Asynchronous)),
        ("sync", "sync", Ok(pb::DurabilityMode::Synchronous)),
        (
            "empty",
            "",
            Err("invalid --mode \"\": must be one of async, sync"),
        ),
        (
            "unknown",
            "durable",
            Err("invalid --mode \"durable\": must be one of async, sync"),
        ),
    ] {
        assert_eq!(
            parse_suspend_mode(mode),
            want.map_err(String::from),
            "{name}"
        );
    }
}

#[test]
fn parse_worker_selector_cases() {
    for (name, raw, want) in [
        ("empty string clears selector", "", Ok(None)),
        (
            "single label",
            "tier=paid",
            Ok(selector(&[("tier", "paid")])),
        ),
        (
            "multiple labels with prefixed key and empty value",
            "hivekeep.io/pool=fast,tier=paid,empty=",
            Ok(selector(&[
                ("hivekeep.io/pool", "fast"),
                ("tier", "paid"),
                ("empty", ""),
            ])),
        ),
        (
            "exactly 10 labels is allowed",
            "k1=v1,k2=v2,k3=v3,k4=v4,k5=v5,k6=v6,k7=v7,k8=v8,k9=v9,k10=v10",
            Ok(selector(&[
                ("k1", "v1"),
                ("k2", "v2"),
                ("k3", "v3"),
                ("k4", "v4"),
                ("k5", "v5"),
                ("k6", "v6"),
                ("k7", "v7"),
                ("k8", "v8"),
                ("k9", "v9"),
                ("k10", "v10"),
            ])),
        ),
        (
            "more than 10 labels is rejected",
            "k1=v1,k2=v2,k3=v3,k4=v4,k5=v5,k6=v6,k7=v7,k8=v8,k9=v9,k10=v10,k11=v11",
            Err("at most 10 labels are allowed, got 11"),
        ),
        (
            "missing equals sign",
            "tier",
            Err("entry \"tier\" must be in key=value format"),
        ),
        (
            "trailing comma produces empty entry",
            "tier=paid,",
            Err("entry \"\" must be in key=value format"),
        ),
        (
            "duplicate key",
            "tier=free,tier=paid",
            Err("duplicate label key \"tier\""),
        ),
        (
            "invalid label key",
            "bad key!=paid",
            Err("invalid label key \"bad key!\""),
        ),
        (
            "invalid label value",
            "tier=not valid!",
            Err("invalid label value \"not valid!\" for key \"tier\""),
        ),
    ] {
        match (parse_worker_selector(raw), want) {
            (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
            (Err(e), Err(want)) => assert!(e.contains(want), "{name}: {e}"),
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn parse_hived_name() {
    for (name, value, default, want) in [
        (
            "qualified reference",
            "team-a/before-upgrade",
            "default-hive",
            Some(("team-a", "before-upgrade")),
        ),
        (
            "bare name uses the default hive",
            "before-upgrade",
            "default-hive",
            Some(("default-hive", "before-upgrade")),
        ),
        ("bare name with no default hive", "before-upgrade", "", None),
        (
            "name contains an extra slash",
            "team-a/before/upgrade",
            "default-hive",
            None,
        ),
        (
            "empty hive before the slash",
            "/before-upgrade",
            "default-hive",
            None,
        ),
        (
            "empty name after the slash",
            "team-a/",
            "default-hive",
            None,
        ),
        (
            "invalid hive characters",
            "Team_A/before-upgrade",
            "default-hive",
            None,
        ),
        (
            "invalid name characters",
            "team-a/Before_Upgrade",
            "default-hive",
            None,
        ),
    ] {
        match (parse_ref(value, default), want) {
            (Ok(got), Some((h, n))) => assert_eq!(got, hived_ref(h, n), "{name}"),
            (Err(_), None) => {}
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

fn base_actor() -> pb::Actor {
    pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: "demo".into(),
            name: "my-counter".into(),
            uid: "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55".into(),
            version: 3,
            ..Default::default()
        }),
        actor_template: Some(hived_ref("demo", "counter-v1")),
        worker_selector: selector(&[("tier", "free")]),
        source_tag: Some(hived_ref("demo", "seed-tag")),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            ..Default::default()
        }),
    }
}

fn seeded() -> Fake {
    let fake = Fake::default();
    fake.state()
        .actors
        .insert(("demo".into(), "my-counter".into()), base_actor());
    fake
}

#[tokio::test]
async fn update_actor_cases() {
    for (name, change, want_template, want_selector) in [
        (
            "repoint template keeps worker selector",
            ActorUpdate {
                template: Some(hived_ref("shared", "counter-v2")),
                worker_selector: None,
            },
            hived_ref("shared", "counter-v2"),
            selector(&[("tier", "free")]),
        ),
        (
            "update worker selector keeps template",
            ActorUpdate {
                template: None,
                worker_selector: Some(selector(&[("tier", "paid"), ("env", "prod")])),
            },
            hived_ref("demo", "counter-v1"),
            selector(&[("tier", "paid"), ("env", "prod")]),
        ),
        (
            "unset worker selector clears existing selector",
            ActorUpdate {
                template: None,
                worker_selector: Some(None),
            },
            hived_ref("demo", "counter-v1"),
            None,
        ),
        (
            "update both template and worker selector",
            ActorUpdate {
                template: Some(hived_ref("demo", "counter-v2")),
                worker_selector: Some(selector(&[("tier", "paid")])),
            },
            hived_ref("demo", "counter-v2"),
            selector(&[("tier", "paid")]),
        ),
    ] {
        let fake = seeded();
        let api = connect(&fake).await;
        let got = update_actor(&api, hived_ref("demo", "my-counter"), &change)
            .await
            .unwrap();
        assert_eq!(fake.state().calls_to("GetActor").len(), 1, "{name}");
        let mut want = base_actor();
        want.actor_template = Some(want_template);
        want.worker_selector = want_selector;
        want.metadata.as_mut().unwrap().version = 4;
        assert_eq!(got, want, "{name}");
    }
}

#[tokio::test]
async fn update_actor_get_error_is_propagated() {
    let fake = seeded();
    fake.state()
        .fail("GetActor", Status::not_found("not found"));
    let api = connect(&fake).await;
    let err = update_actor(
        &api,
        hived_ref("demo", "my-counter"),
        &ActorUpdate::default(),
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("code = NotFound"), "{err}");
    assert!(
        fake.state().calls_to("UpdateActor").is_empty(),
        "UpdateActor called after GetActor failed"
    );
}

#[tokio::test]
async fn update_actor_update_error_is_propagated() {
    let fake = seeded();
    fake.state().fail(
        "UpdateActor",
        Status::failed_precondition("actor must be SUSPENDED"),
    );
    let api = connect(&fake).await;
    let err = update_actor(
        &api,
        hived_ref("demo", "my-counter"),
        &ActorUpdate::default(),
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("code = FailedPrecondition"), "{err}");
}

#[tokio::test]
async fn update_tag_scope() {
    let tag = pb::Tag {
        metadata: Some(pb::ResourceMetadata {
            hive: "space-1".into(),
            name: "tag-1".into(),
            uid: "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55".into(),
            version: 1,
            ..Default::default()
        }),
        scope: pb::TagScope::Hive as i32,
        status: Some(pb::TagStatus {
            snapshot: Some(pb::Snapshot {
                fidelity: pb::SnapshotFidelity::Memory as i32,
                durable_snapshot: Some(pb::SnapshotStorage {
                    state: pb::SnapshotStorageState::Completed as i32,
                    object: Some(pb::ObjectSnapshot {
                        snapshot_uri: "gs://private/hives/space-1/tags/tag-1".into(),
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let fake = Fake::default();
    fake.state()
        .tags
        .insert(("space-1".into(), "tag-1".into()), tag.clone());
    let api = connect(&fake).await;
    let got = update_scope(&api, hived_ref("space-1", "tag-1"), pb::TagScope::Published)
        .await
        .unwrap();
    assert_eq!(fake.state().calls_to("GetTag").len(), 1);
    // The write carries the uid and version just read.
    let sent = fake.state().calls_to("UpdateTag").pop().unwrap();
    assert!(
        sent.request
            .contains("uid: \"8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55\"")
            && sent.request.contains("version: 1"),
        "{}",
        sent.request
    );
    let mut want = tag;
    want.scope = pb::TagScope::Published as i32;
    want.metadata.as_mut().unwrap().version = 2;
    assert_eq!(got, want);
}

#[test]
fn parse_tag_scope_cases() {
    assert_eq!(parse_tag_scope("published"), Ok(pb::TagScope::Published));
    assert!(parse_tag_scope("global").is_err());
}
