//! hivectl commands end to end against a fake Control server.

mod common;

use common::*;
use hivekeep_proto::api::v1 as pb;
use serde_json::Value;
use tonic::Status;

fn json(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("{e}: {s}"))
}

fn k(h: &str, n: &str) -> (String, String) {
    (h.to_string(), n.to_string())
}

async fn setup() -> (Fake, std::net::SocketAddr) {
    let fake = Fake::default();
    let addr = serve(fake.clone()).await;
    (fake, addr)
}

#[tokio::test]
async fn list_actors_pages_through_and_sorts() {
    let (fake, addr) = setup().await;
    {
        let mut s = fake.state();
        s.page_size = 1;
        for (h, n) in [("team-b", "zebra"), ("team-a", "beta"), ("team-a", "alpha")] {
            s.actors
                .insert(k(h, n), actor(h, n, pb::ActorState::Suspended));
        }
    }
    let run = hivectl(addr, &["get", "actors", "-A", "-o", "json"]).await;
    let v = json(run.ok());
    let names: Vec<_> = v["actors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["alpha", "beta", "zebra"]);
    assert_eq!(fake.state().calls_to("ListActors").len(), 3);

    let run = hivectl(addr, &["get", "actors", "-H", "team-a"]).await;
    let out = run.ok();
    assert!(out.starts_with("HIVE     NAME    TEMPLATE"), "{out}");
    assert_eq!(out.lines().count(), 3);
    assert!(fake
        .state()
        .calls_to("ListActors")
        .last()
        .unwrap()
        .request
        .contains("hive: \"team-a\""));
}

#[tokio::test]
async fn get_actors_by_name() {
    let (fake, addr) = setup().await;
    fake.state()
        .actors
        .insert(k("h", "a1"), actor("h", "a1", pb::ActorState::Running));
    fake.state()
        .actors
        .insert(k("h", "a2"), actor("h", "a2", pb::ActorState::Suspended));

    let one = json(
        hivectl(addr, &["get", "actor", "a1", "-H", "h", "-o", "json"])
            .await
            .ok(),
    );
    assert_eq!(one["metadata"]["name"], "a1");
    assert_eq!(one["status"]["state"], "ACTOR_STATE_RUNNING");

    let two = json(
        hivectl(
            addr,
            &["get", "actors", "a2", "a1", "-H", "h", "-o", "json"],
        )
        .await
        .ok(),
    );
    assert_eq!(two["actors"].as_array().unwrap().len(), 2);

    let run = hivectl(addr, &["get", "actor", "nope", "-H", "h"]).await;
    let msg = run.error().message().to_string();
    assert!(
        msg.starts_with("failed to get actor \"nope\": rpc error: code = NotFound"),
        "{msg}"
    );
}

#[tokio::test]
async fn scope_flags_are_checked_before_connecting() {
    let (fake, addr) = setup().await;
    for (args, want) in [
        (
            &["get", "actor", "a1"][..],
            "-H/--hive is required when getting actors by name",
        ),
        (
            &["get", "tags"],
            "-H/--hive <name> or -A/--all-hives is required",
        ),
        (
            &["get", "actor-templates", "x", "-A"],
            "-A/--all-hives cannot be used when getting actor templates by name",
        ),
        (
            &["get", "actors", "-A", "-H", "h"],
            "-H/--hive and -A/--all-hives are mutually exclusive",
        ),
    ] {
        let run = hivectl(addr, args).await;
        let e = run.error();
        assert!(e.is_usage() && e.message().contains(want), "{args:?}: {e}");
    }
    assert!(fake.state().calls.is_empty());
}

#[tokio::test]
async fn create_actor_resolves_template_and_tag() {
    let (fake, addr) = setup().await;
    let v = json(
        hivectl(
            addr,
            &[
                "create",
                "actor",
                "a1",
                "-H",
                "team",
                "--template",
                "shared/tpl",
                "--tag",
                "v1",
                "-o",
                "json",
            ],
        )
        .await
        .ok(),
    );
    assert_eq!(
        v["actorTemplate"],
        serde_json::json!({"hive": "shared", "name": "tpl"})
    );
    assert_eq!(
        v["sourceTag"],
        serde_json::json!({"hive": "team", "name": "v1"})
    );
    assert!(fake.state().actors.contains_key(&k("team", "a1")));

    let run = hivectl(
        addr,
        &["create", "actor", "a2", "-H", "team", "--template", "a/b/c"],
    )
    .await;
    assert!(run.error().is_usage());
    assert_eq!(fake.state().calls_to("CreateActor").len(), 1);
}

#[tokio::test]
async fn update_actor_reads_modifies_writes() {
    let (fake, addr) = setup().await;
    let mut a = actor("h", "a1", pb::ActorState::Suspended);
    a.worker_selector = Some(pb::Selector {
        match_labels: [("pool".to_string(), "fast".to_string())].into(),
    });
    fake.state().actors.insert(k("h", "a1"), a);

    hivectl(
        addr,
        &["update", "actor", "a1", "-H", "h", "--template", "tpl2"],
    )
    .await
    .ok();
    let stored = fake.state().actors[&k("h", "a1")].clone();
    assert_eq!(stored.actor_template.unwrap().name, "tpl2");
    assert!(
        stored.worker_selector.is_some(),
        "unrelated fields are kept"
    );
    assert_eq!(stored.metadata.unwrap().version, 2);

    hivectl(
        addr,
        &["update", "actor", "a1", "-H", "h", "--worker-selector", ""],
    )
    .await
    .ok();
    assert!(fake.state().actors[&k("h", "a1")].worker_selector.is_none());

    let run = hivectl(addr, &["update", "actor", "a1", "-H", "h"]).await;
    assert!(run
        .error()
        .message()
        .contains("at least one of --template or --worker-selector"));

    fake.state().fail(
        "UpdateActor",
        Status::failed_precondition("actor is running"),
    );
    let run = hivectl(
        addr,
        &["update", "actor", "a1", "-H", "h", "--template", "tpl3"],
    )
    .await;
    assert!(run
        .error()
        .message()
        .starts_with("failed to update actor: rpc error: code = FailedPrecondition"));
}

#[tokio::test]
async fn lifecycle_verbs() {
    let (fake, addr) = setup().await;
    fake.state()
        .actors
        .insert(k("h", "a1"), actor("h", "a1", pb::ActorState::Suspended));

    let v = json(
        hivectl(addr, &["resume", "actor", "a1", "-H", "h", "-o", "json"])
            .await
            .ok(),
    );
    assert_eq!(v["status"]["state"], "ACTOR_STATE_RUNNING");

    hivectl(
        addr,
        &["suspend", "actor", "a1", "-H", "h", "--mode", "sync"],
    )
    .await
    .ok();
    let call = fake.state().calls_to("SuspendActor").pop().unwrap();
    assert!(
        call.request.contains("mode: Synchronous"),
        "{}",
        call.request
    );

    let run = hivectl(
        addr,
        &["suspend", "actor", "a1", "-H", "h", "--mode", "later"],
    )
    .await;
    assert!(run.error().is_usage());

    let v = json(
        hivectl(addr, &["revert", "actor", "a1", "-H", "h", "-o", "json"])
            .await
            .ok(),
    );
    assert_eq!(v["status"]["state"], "ACTOR_STATE_SUSPENDED");

    let run = hivectl(addr, &["delete", "actor", "a1", "-H", "h"]).await;
    assert_eq!(run.ok(), "actor \"a1\" deleted\n");
    assert!(fake.state().actors.is_empty());
}

#[tokio::test]
async fn hives() {
    let (fake, addr) = setup().await;
    hivectl(addr, &["create", "hive", "team-b"]).await.ok();
    hivectl(addr, &["create", "hive", "team-a"]).await.ok();
    let out = hivectl(addr, &["get", "hives"]).await.ok().to_string();
    let names: Vec<_> = out
        .lines()
        .skip(1)
        .map(|l| l.split_whitespace().next().unwrap())
        .collect();
    assert_eq!(names, ["team-a", "team-b"]);
    assert_eq!(
        hivectl(addr, &["get", "hive", "team-a", "-o", "yaml"]).await.ok(),
        "metadata:\n  name: team-a\n  uid: 00000000-0000-4000-8000-000000000001\n  version: \"1\"\n"
    );
    let run = hivectl(addr, &["create", "hive", "team-a"]).await;
    assert!(run.error().message().contains("AlreadyExists"));
    assert_eq!(
        hivectl(addr, &["delete", "hive", "team-b"]).await.ok(),
        "hive \"team-b\" deleted\n"
    );
    assert_eq!(fake.state().hives.len(), 1);
}

#[tokio::test]
async fn tags() {
    let (fake, addr) = setup().await;
    let v = json(
        hivectl(
            addr,
            &[
                "create", "tag", "v1", "-H", "team", "--actor", "a1", "-o", "json",
            ],
        )
        .await
        .ok(),
    );
    assert_eq!(v["scope"], "TAG_SCOPE_HIVE");
    assert_eq!(
        v["sourceActor"],
        serde_json::json!({"hive": "team", "name": "a1"})
    );

    let v = json(
        hivectl(
            addr,
            &[
                "update",
                "tag",
                "v1",
                "-H",
                "team",
                "--scope",
                "published",
                "-o",
                "json",
            ],
        )
        .await
        .ok(),
    );
    assert_eq!(v["scope"], "TAG_SCOPE_PUBLISHED");
    assert_eq!(
        fake.state().tags[&k("team", "v1")].scope,
        pb::TagScope::Published as i32
    );

    let run = hivectl(
        addr,
        &["update", "tag", "v1", "-H", "team", "--scope", "world"],
    )
    .await;
    assert!(run.error().is_usage());

    let out = hivectl(addr, &["get", "tags", "-A"]).await.ok().to_string();
    assert!(
        out.contains("TAG_SCOPE_PUBLISHED   Pending   <none>"),
        "{out}"
    );

    assert_eq!(
        hivectl(addr, &["delete", "tag", "v1", "-H", "team"])
            .await
            .ok(),
        "tag \"v1\" deleted\n"
    );
}

const TEMPLATE: &str = "metadata:\n  hive: demo\n  name: counter\ncontainers:\n- name: counter\n  image: registry.example/counter:1\n";

#[tokio::test]
async fn actor_templates_from_manifests() {
    let (fake, addr) = setup().await;
    let run = hivectl_with_stdin(
        addr,
        &["create", "actor-template", "-f", "-", "-o", "json"],
        TEMPLATE,
    )
    .await;
    assert_eq!(
        json(run.ok())["containers"][0]["image"],
        "registry.example/counter:1"
    );
    assert!(fake.state().templates.contains_key(&k("demo", "counter")));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.yaml");
    std::fs::write(
        &path,
        TEMPLATE.replace("counter\ncontainers", "other\ncontainers"),
    )
    .unwrap();
    hivectl(
        addr,
        &["create", "actor-template", "-f", path.to_str().unwrap()],
    )
    .await
    .ok();

    let out = hivectl(addr, &["get", "actor-templates", "-H", "demo"])
        .await
        .ok()
        .to_string();
    assert!(
        out.lines().nth(1).unwrap().starts_with("demo   counter"),
        "{out}"
    );

    let run = hivectl_with_stdin(
        addr,
        &["create", "actor-template", "-f", "-"],
        "metadata: {name: x}\nbogus: 1\n",
    )
    .await;
    let msg = run.error().message().to_string();
    assert!(
        msg.starts_with("failed to parse actor template manifest \"-\":") && msg.contains("bogus"),
        "{msg}"
    );

    let run = hivectl(
        addr,
        &["create", "actor-template", "-f", "/nonexistent.yaml"],
    )
    .await;
    assert!(run.error().message().contains("/nonexistent.yaml"));

    assert_eq!(
        hivectl(addr, &["delete", "actor-template", "counter", "-H", "demo"])
            .await
            .ok(),
        "actor template \"counter\" deleted\n"
    );
}

fn seed_workers(fake: &Fake) {
    let mut s = fake.state();
    s.workers.insert(
        "w1".into(),
        worker("w1", "ns-a", "pool-1", &[("tier", "gold")], "gvisor"),
    );
    s.workers.insert(
        "w2".into(),
        worker("w2", "ns-a", "pool-2", &[("tier", "silver")], "microvm"),
    );
    s.workers.insert(
        "w3".into(),
        worker("w3", "ns-b", "pool-1", &[("tier", "gold")], "gvisor"),
    );
    s.actors.insert(
        k("team", "a1"),
        running_on(
            actor("team", "a1", pb::ActorState::Running),
            "w2",
            "ns-a",
            "w2-pod",
        ),
    );
    s.actors.insert(
        k("other", "a2"),
        running_on(
            actor("other", "a2", pb::ActorState::Running),
            "w3",
            "ns-b",
            "w3-pod",
        ),
    );
}

async fn worker_names(addr: std::net::SocketAddr, args: &[&str]) -> Vec<String> {
    let mut all = vec!["get", "workers", "-o", "json"];
    all.extend_from_slice(args);
    let v = json(hivectl(addr, &all).await.ok());
    v["workers"]
        .as_array()
        .map(|ws| {
            ws.iter()
                .map(|w| w["metadata"]["name"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn worker_filters() {
    let (fake, addr) = setup().await;
    seed_workers(&fake);
    assert_eq!(worker_names(addr, &[]).await, ["w1", "w2", "w3"]);
    assert_eq!(worker_names(addr, &["-n", "ns-a"]).await, ["w1", "w2"]);
    assert_eq!(worker_names(addr, &["-H", "team"]).await, ["w2"]);
    assert_eq!(worker_names(addr, &["-l", "tier=gold"]).await, ["w1", "w3"]);
    assert_eq!(
        worker_names(
            addr,
            &["-l", "tier in (gold,silver),tier!=silver", "-n", "ns-b"]
        )
        .await,
        ["w3"]
    );
    assert_eq!(
        worker_names(addr, &["--sandbox-class", "microvm"]).await,
        ["w2"]
    );
    assert!(worker_names(addr, &["-H", "empty"]).await.is_empty());

    let v = json(
        hivectl(addr, &["get", "worker", "w1", "-o", "json"])
            .await
            .ok(),
    );
    assert_eq!(v["metadata"]["name"], "w1");

    let calls = fake.state().calls.len();
    let run = hivectl(addr, &["get", "worker", "w1", "-n", "ns-a"]).await;
    assert!(run
        .error()
        .message()
        .contains("-n/--namespace cannot be used when getting workers by name"));
    let run = hivectl(addr, &["get", "workers", "-l", "tier in (gold"]).await;
    assert!(run.error().is_usage());
    assert_eq!(
        fake.state().calls.len(),
        calls,
        "rejected before connecting"
    );
}

#[tokio::test]
async fn worker_state_transitions() {
    let (fake, addr) = setup().await;
    seed_workers(&fake);
    for (verb, state) in [
        ("drain", "WORKER_STATE_DRAINING"),
        ("mark-unavailable", "WORKER_STATE_UNAVAILABLE"),
        ("mark-available", "WORKER_STATE_ACTIVE"),
    ] {
        let v = json(
            hivectl(addr, &[verb, "worker", "w1", "-o", "json"])
                .await
                .ok(),
        );
        assert_eq!(v["status"]["state"], state, "{verb}");
    }
    let run = hivectl(addr, &["drain", "worker", "ghost"]).await;
    assert!(run
        .error()
        .message()
        .starts_with("failed to drain worker \"ghost\": rpc error: code = NotFound"));
}

#[tokio::test]
async fn worker_assignments() {
    let (fake, addr) = setup().await;
    seed_workers(&fake);
    fake.state().assignments.insert(
        "w2".into(),
        vec![pb::ActorAssignment {
            actor: Some(pb::ObjectRef {
                hive: "team".into(),
                name: "a1".into(),
            }),
            actor_uid: "uid-a1".into(),
            resources: Some(pb::Resources {
                cpu_millicores: 250,
                memory_bytes: 256 << 20,
            }),
            ..Default::default()
        }],
    );
    let out = hivectl(addr, &["get", "worker-assignments", "w2"])
        .await
        .ok()
        .to_string();
    assert!(out.starts_with("ACTOR     UID      CPU(MILLICORES)   MEMORY   AGE\nteam/a1   uid-a1   250               256Mi    "), "{out}");
    let v = json(
        hivectl(addr, &["get", "assignments", "w2", "-o", "json"])
            .await
            .ok(),
    );
    assert_eq!(v["actorAssignments"][0]["actorUid"], "uid-a1");
}

const EGRESS: &str = "rules:\n- https:\n    hostnames: [api.example.com]\n";

#[tokio::test]
async fn egress_policy_lifecycle() {
    let (fake, addr) = setup().await;
    fake.state().actors.insert(
        k("team", "a1"),
        actor("team", "a1", pb::ActorState::Suspended),
    );

    // No policy is a state, not a failure.
    let run = hivectl(addr, &["get", "egress-policy", "a1", "-H", "team"]).await;
    assert_eq!(run.ok(), "");
    assert!(
        run.err
            .contains("actor \"a1\" in hive \"team\" has no egress policy"),
        "{}",
        run.err
    );

    let run = hivectl(addr, &["get", "egress-policy", "ghost", "-H", "team"]).await;
    assert_eq!(
        run.error().message(),
        "actor \"ghost\" in hive \"team\" not found"
    );

    let run = hivectl_with_stdin(
        addr,
        &["create", "egress-policy", "a1", "-H", "team", "-f", "-"],
        EGRESS,
    )
    .await;
    let out = run.ok();
    assert!(
        out.starts_with("HIVE   ACTOR   RULES   VERSION   AGE\nteam   a1      1       1"),
        "{out}"
    );
    let stored = fake.state().egress[&k("team", "a1")].clone();
    assert_eq!(
        stored.metadata.as_ref().unwrap().name,
        "default",
        "metadata is filled in"
    );

    // An update needs the preconditions, checked before connecting.
    let calls = fake.state().calls.len();
    let run = hivectl_with_stdin(
        addr,
        &["update", "egress-policy", "a1", "-H", "team", "-f", "-"],
        EGRESS,
    )
    .await;
    assert!(run
        .error()
        .message()
        .contains("lacks the metadata.uid and metadata.version"));
    assert_eq!(fake.state().calls.len(), calls);

    // Round trip: what get prints is what update takes.
    let yaml = hivectl(
        addr,
        &["get", "egress-policy", "a1", "-H", "team", "-o", "yaml"],
    )
    .await
    .ok()
    .to_string();
    let edited = yaml.replace("api.example.com", "api2.example.com");
    hivectl_with_stdin(
        addr,
        &["update", "egress-policy", "a1", "-H", "team", "-f", "-"],
        &edited,
    )
    .await
    .ok();
    assert_eq!(
        fake.state().egress[&k("team", "a1")]
            .metadata
            .as_ref()
            .unwrap()
            .version,
        2
    );

    // Applying the stale copy again is rejected by the server.
    let run = hivectl_with_stdin(
        addr,
        &["update", "egress-policy", "a1", "-H", "team", "-f", "-"],
        &edited,
    )
    .await;
    assert!(
        run.error()
            .message()
            .starts_with("manifest's uid and version preconditions do not match"),
        "{}",
        run.error()
    );

    let run = hivectl_with_stdin(
        addr,
        &["create", "egress-policy", "a1", "-H", "other", "-f", "-"],
        &yaml,
    )
    .await;
    assert!(run
        .error()
        .message()
        .contains("does not match --hive \"other\""));

    let run = hivectl(
        addr,
        &[
            "delete",
            "egress-policy",
            "a1",
            "-H",
            "team",
            "--version",
            "0",
        ],
    )
    .await;
    assert!(run.error().is_usage());
    let run = hivectl(
        addr,
        &[
            "delete",
            "egress-policy",
            "a1",
            "-H",
            "team",
            "--uid",
            "u",
            "--version",
            "2",
        ],
    )
    .await;
    assert_eq!(
        run.ok(),
        "egress policy for actor \"a1\" in hive \"team\" deleted\n"
    );
    let call = fake
        .state()
        .calls_to("DeleteActorEgressPolicy")
        .pop()
        .unwrap();
    assert!(
        call.request.contains("version: 2") && call.request.contains("uid: \"u\""),
        "{}",
        call.request
    );

    let run = hivectl(addr, &["delete", "egress-policy", "a1", "-H", "team"]).await;
    assert_eq!(
        run.error().message(),
        "actor \"a1\" in hive \"team\" has no egress policy"
    );
    let run = hivectl_with_stdin(
        addr,
        &["update", "egress-policy", "a1", "-H", "team", "-f", "-"],
        &yaml,
    )
    .await;
    assert!(run
        .error()
        .message()
        .contains("has no egress policy to update"));
}

#[tokio::test]
async fn access_policy_lifecycle() {
    let (fake, addr) = setup().await;
    fake.state().hives.insert("team-a".into(), hive("team-a"));
    let manifest = "bindings:\n- role: owner\n  members: [\"user:alice\"]\n";

    let run = hivectl(addr, &["get", "access-policy", "--global"]).await;
    assert_eq!(run.ok(), "");
    assert!(run.err.contains("global has no access policy"));

    let run = hivectl(addr, &["get", "access-policy", "-H", "ghost"]).await;
    assert_eq!(run.error().message(), "hive \"ghost\" not found");

    let run = hivectl(addr, &["get", "access-policy"]).await;
    assert!(run.error().is_usage() && run.error().message().contains("one of --hive or --global"));

    let out = hivectl_with_stdin(
        addr,
        &["create", "access-policy", "-H", "team-a", "-f", "-"],
        manifest,
    )
    .await
    .ok()
    .to_string();
    assert_eq!(
        out,
        "SCOPE    ROLE    MEMBERS\nteam-a   owner   user:alice\n"
    );
    let run = hivectl_with_stdin(
        addr,
        &["create", "access-policy", "-H", "team-a", "-f", "-"],
        manifest,
    )
    .await;
    assert!(run
        .error()
        .message()
        .contains("already has an access policy"));

    hivectl_with_stdin(
        addr,
        &["create", "access-policy", "--global", "-f", "-"],
        manifest,
    )
    .await
    .ok();
    let out = hivectl(addr, &["get", "access-policy", "--global"])
        .await
        .ok()
        .to_string();
    assert!(out.contains("<global>   owner"), "{out}");

    let yaml = hivectl(
        addr,
        &["get", "access-policy", "-H", "team-a", "-o", "yaml"],
    )
    .await
    .ok()
    .to_string();
    let edited = yaml.replace("user:alice", "user:bob");
    hivectl_with_stdin(
        addr,
        &["update", "access-policy", "-H", "team-a", "-f", "-"],
        &edited,
    )
    .await
    .ok();
    assert_eq!(
        fake.state().hive_policies["team-a"].bindings[0].members,
        ["user:bob"]
    );

    let run = hivectl_with_stdin(
        addr,
        &["update", "access-policy", "-H", "team-a", "-f", "-"],
        manifest,
    )
    .await;
    assert!(run.error().message().contains("lacks the metadata.uid"));

    assert_eq!(
        hivectl(addr, &["delete", "access-policy", "-H", "team-a"])
            .await
            .ok(),
        "access policy for hive \"team-a\" deleted\n"
    );
    let run = hivectl(addr, &["delete", "access-policy", "-H", "team-a"]).await;
    assert_eq!(
        run.error().message(),
        "hive \"team-a\" has no access policy"
    );
}

#[tokio::test]
async fn request_metadata() {
    let (fake, addr) = setup().await;
    hivectl(addr, &["get", "hives"]).await.ok();
    let call = fake.state().calls_to("ListHives").pop().unwrap();
    assert!(!call.metadata.contains_key("hk-preview-allowed"));
    assert!(!call.metadata.contains_key("traceparent"));

    let run = hivectl(addr, &["get", "hives", "--preview", "--trace"]).await;
    run.ok();
    let call = fake.state().calls_to("ListHives").pop().unwrap();
    assert_eq!(call.metadata["hk-preview-allowed"], "true");
    let tp = &call.metadata["traceparent"];
    assert!(
        run.err
            .contains(&format!("Tracing enabled. Trace ID: {}", &tp[3..35])),
        "{}",
        run.err
    );
    assert!(run.err.contains("no --token-file given"));
}

#[tokio::test]
async fn verbose_logging() {
    let (fake, addr) = setup().await;
    fake.state().actors.insert(
        k("demo", "act-1"),
        actor("demo", "act-1", pb::ActorState::Running),
    );

    let run = hivectl(addr, &["get", "actor", "act-1", "-H", "demo", "-v"]).await;
    run.ok();
    assert!(run.err.contains("Connecting to hivekeep-api at"));
    assert!(
        !run.err.contains("gRPC "),
        "a fast success is quiet at -v: {}",
        run.err
    );

    fake.state().fail("GetActor", Status::unavailable("boom"));
    let run = hivectl(addr, &["get", "actor", "act-1", "-H", "demo", "-v"]).await;
    assert!(
        run.err
            .contains("gRPC /hivekeep.api.v1.Control/GetActor failed after"),
        "{}",
        run.err
    );

    let run = hivectl(addr, &["get", "actor", "act-1", "-H", "demo", "-vv"]).await;
    run.ok();
    assert!(
        run.err.contains("gRPC /hivekeep.api.v1.Control/GetActor\n"),
        "{}",
        run.err
    );
    assert!(run
        .err
        .contains("gRPC /hivekeep.api.v1.Control/GetActor completed in "));

    let run = hivectl(addr, &["get", "actor", "act-1", "-H", "demo", "-vvvvv"]).await;
    run.ok();
    assert!(run.err.contains("gRPC /hivekeep.api.v1.Control/GetActor request: {\"actor\":{\"hive\":\"demo\",\"name\":\"act-1\"}}"), "{}", run.err);
    assert!(run
        .err
        .contains("gRPC /hivekeep.api.v1.Control/GetActor response ("));
}

#[tokio::test]
async fn unreachable_server() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let run = hivectl(addr, &["get", "hives"]).await;
    assert!(
        run.error()
            .message()
            .starts_with("failed to connect to hivekeep-api"),
        "{}",
        run.error()
    );
}
