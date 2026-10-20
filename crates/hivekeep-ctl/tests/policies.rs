//! Access policy and egress policy commands against a fake Control server:
//! the requests sent, what is printed, and how each failure is explained.

mod common;

use std::time::{Duration, UNIX_EPOCH};

use common::*;
use hivekeep_ctl::client::Api;
use hivekeep_ctl::commands::access_policy::{PolicyScope, Runner};
use hivekeep_ctl::commands::egress_policy::{
    create_with, delete_with, get_with, require_actor, update_with,
};
use hivekeep_ctl::connection::{ConnectionConfig, RequestDecorator};
use hivekeep_ctl::error::Result;
use hivekeep_ctl::io::{SharedBuf, Sink};
use hivekeep_ctl::output::{OutputFormat, Printer};
use hivekeep_ctl::verbosity::Logger;
use hivekeep_proto::api::v1 as pb;
use tonic::Status;

/// 2026-01-01T12:00:00Z.
const NOW: i64 = 1_767_268_800;

fn printer(format: OutputFormat) -> Printer {
    Printer::at(format, UNIX_EPOCH + Duration::from_secs(NOW as u64))
}

fn at(secs_before_now: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: NOW - secs_before_now,
        nanos: 0,
    })
}

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

fn debug<T: std::fmt::Debug>(v: &T) -> String {
    format!("{v:?}")
}

fn hive_ref(hive: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: String::new(),
        name: hive.into(),
    })
}

fn err_text(r: &Result<()>) -> String {
    r.as_ref()
        .err()
        .map(|e| e.message().to_string())
        .unwrap_or_default()
}

// Access policies.

type ApRun = for<'a> fn(
    &'a Runner<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>;

struct ApCase {
    name: &'static str,
    scope: PolicyScope,
    setup: fn(&mut State),
    run: ApRun,
    method: &'static str,
    request: Option<String>,
    stdout: &'static str,
    stderr: &'static str,
    error: &'static str,
}

fn stored_policy() -> pb::AccessPolicy {
    pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            uid: "u-1".into(),
            version: 2,
            ..Default::default()
        }),
        bindings: vec![pb::Binding {
            role: "owner".into(),
            members: vec!["user:alice".into()],
        }],
    }
}

fn edit() -> pb::AccessPolicy {
    pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            uid: "u-1".into(),
            version: 2,
            ..Default::default()
        }),
        bindings: vec![],
    }
}

fn with_hive(s: &mut State) {
    s.hives.insert("team-a".into(), hive("team-a"));
}

#[tokio::test]
async fn access_policy_runner() {
    let team_a = || PolicyScope::Hive("team-a".into());
    let cases = vec![
        ApCase {
            name: "get hive",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.reply("GetHiveAccessPolicy", &stored_policy());
            },
            run: |r| Box::pin(r.get()),
            method: "GetHiveAccessPolicy",
            request: Some(debug(&pb::GetHiveAccessPolicyRequest {
                hive: hive_ref("team-a"),
                name: "default".into(),
            })),
            stdout: "team-a   owner   user:alice",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "get global",
            scope: PolicyScope::Global,
            setup: |s| s.reply("GetGlobalAccessPolicy", &stored_policy()),
            run: |r| Box::pin(r.get()),
            method: "GetGlobalAccessPolicy",
            request: Some(debug(&pb::GetGlobalAccessPolicyRequest {
                name: "default".into(),
            })),
            stdout: "<global>   owner   user:alice",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "get hive without a policy",
            scope: team_a(),
            setup: with_hive,
            run: |r| Box::pin(r.get()),
            method: "GetHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "hive \"team-a\" has no access policy",
            error: "",
        },
        ApCase {
            name: "get missing hive",
            scope: team_a(),
            setup: |_| {},
            run: |r| Box::pin(r.get()),
            method: "GetHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "hive \"team-a\" not found",
        },
        ApCase {
            name: "get permission denied",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.fail("GetHiveAccessPolicy", Status::permission_denied("denied"));
            },
            run: |r| Box::pin(r.get()),
            method: "GetHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "PermissionDenied",
        },
        ApCase {
            name: "create hive",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.reply("CreateHiveAccessPolicy", &stored_policy());
            },
            run: |r| Box::pin(r.create(edit())),
            method: "CreateHiveAccessPolicy",
            request: Some(debug(&pb::CreateHiveAccessPolicyRequest {
                hive: hive_ref("team-a"),
                access_policy: Some(edit()),
            })),
            stdout: "team-a   owner   user:alice",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "create global",
            scope: PolicyScope::Global,
            setup: |s| s.reply("CreateGlobalAccessPolicy", &stored_policy()),
            run: |r| Box::pin(r.create(edit())),
            method: "CreateGlobalAccessPolicy",
            request: Some(debug(&pb::CreateGlobalAccessPolicyRequest {
                access_policy: Some(edit()),
            })),
            stdout: "",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "create existing",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.fail("CreateHiveAccessPolicy", Status::already_exists("exists"));
            },
            run: |r| Box::pin(r.create(edit())),
            method: "CreateHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "hivectl update access-policy",
        },
        ApCase {
            name: "update global",
            scope: PolicyScope::Global,
            setup: |s| s.reply("UpdateGlobalAccessPolicy", &stored_policy()),
            run: |r| Box::pin(r.update(edit())),
            method: "UpdateGlobalAccessPolicy",
            request: Some(debug(&pb::UpdateGlobalAccessPolicyRequest {
                access_policy: Some(edit()),
            })),
            stdout: "",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "update stale",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.fail(
                    "UpdateHiveAccessPolicy",
                    Status::aborted("version mismatch"),
                );
            },
            run: |r| Box::pin(r.update(edit())),
            method: "UpdateHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "re-run",
        },
        ApCase {
            name: "update without a policy",
            scope: team_a(),
            setup: with_hive,
            run: |r| Box::pin(r.update(edit())),
            method: "UpdateHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "hivectl create access-policy",
        },
        ApCase {
            name: "delete",
            scope: team_a(),
            setup: |s| {
                with_hive(s);
                s.reply("DeleteHiveAccessPolicy", &stored_policy());
            },
            run: |r| {
                Box::pin(r.delete(Some(pb::DeleteOptions {
                    uid: String::new(),
                    version: 2,
                })))
            },
            method: "DeleteHiveAccessPolicy",
            request: Some(debug(&pb::DeleteHiveAccessPolicyRequest {
                hive: hive_ref("team-a"),
                name: "default".into(),
                options: Some(pb::DeleteOptions {
                    uid: String::new(),
                    version: 2,
                }),
            })),
            stdout: "access policy for hive \"team-a\" deleted",
            stderr: "",
            error: "",
        },
        ApCase {
            name: "delete without a policy",
            scope: team_a(),
            setup: with_hive,
            run: |r| Box::pin(r.delete(None)),
            method: "DeleteHiveAccessPolicy",
            request: None,
            stdout: "",
            stderr: "",
            error: "hive \"team-a\" has no access policy",
        },
    ];
    for c in cases {
        let fake = Fake::default();
        (c.setup)(&mut fake.state());
        let api = connect(&fake).await;
        let (out, out_buf) = Sink::buffer();
        let (err, err_buf) = Sink::buffer();
        let runner = Runner {
            api: &api,
            scope: c.scope,
            printer: printer(OutputFormat::Table),
            out,
            err,
        };
        let result = (c.run)(&runner).await;
        let name = c.name;
        if c.error.is_empty() {
            assert!(result.is_ok(), "{name}: {}", err_text(&result));
        } else {
            assert!(
                err_text(&result).contains(c.error),
                "{name}: {}",
                err_text(&result)
            );
        }
        let calls = fake.state().calls_to(c.method);
        assert_eq!(calls.len(), 1, "{name}: calls to {}", c.method);
        if let Some(want) = c.request {
            assert_eq!(calls[0].request, want, "{name}");
        }
        assert!(
            out_buf.contents().contains(c.stdout),
            "{name}: {}",
            out_buf.contents()
        );
        assert!(
            err_buf.contents().contains(c.stderr),
            "{name}: {}",
            err_buf.contents()
        );
    }
}

// Egress policies.

fn actor_ref() -> pb::ObjectRef {
    pb::ObjectRef {
        hive: "team-a".into(),
        name: "c1".into(),
    }
}

fn http_rule(host: &str) -> pb::EgressRule {
    pb::EgressRule {
        http: Some(pb::HttpRule {
            hostnames: vec![host.into()],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// How the fake answers the Actor read a NotFound triggers.
#[derive(Clone, Copy)]
enum ActorLookup {
    /// The Actor exists.
    Exists,
    /// There is no such Actor.
    Missing,
    /// The read is refused.
    Denied,
}

fn seed_actor(s: &mut State, lookup: ActorLookup) {
    match lookup {
        ActorLookup::Exists => {
            s.actors.insert(
                ("team-a".into(), "c1".into()),
                actor("team-a", "c1", pb::ActorState::Suspended),
            );
        }
        ActorLookup::Missing => {}
        ActorLookup::Denied => s.fail("GetActor", Status::permission_denied("denied")),
    }
}

#[tokio::test]
async fn require_actor_cases() {
    for (name, lookup, want) in [
        ("actor exists", ActorLookup::Exists, ""),
        (
            "missing actor",
            ActorLookup::Missing,
            "actor \"c1\" in hive \"team-a\" not found",
        ),
        (
            "lookup error wraps",
            ActorLookup::Denied,
            "failed to get actor \"c1\" in hive \"team-a\": rpc error: code = PermissionDenied desc = denied",
        ),
    ] {
        let fake = Fake::default();
        seed_actor(&mut fake.state(), lookup);
        let api = connect(&fake).await;
        let got = require_actor(&api, &actor_ref()).await;
        assert_eq!(err_text(&got), want, "{name}");
        let calls = fake.state().calls_to("GetActor");
        assert_eq!(calls.len(), 1, "{name}");
        assert_eq!(
            calls[0].request,
            debug(&pb::GetActorRequest {
                actor: Some(actor_ref())
            }),
            "{name}"
        );
    }
}

/// One egress command run: how the policy RPC answers, how the Actor read
/// answers, and what should come of it.
struct EgressCase {
    name: &'static str,
    format: OutputFormat,
    reply: Option<pb::EgressPolicy>,
    failure: Option<Status>,
    lookup: ActorLookup,
    reads_actor: bool,
    out: String,
    err_out: &'static str,
    error: String,
}

fn case(name: &'static str, format: OutputFormat) -> EgressCase {
    EgressCase {
        name,
        format,
        reply: None,
        failure: None,
        lookup: ActorLookup::Exists,
        reads_actor: false,
        out: String::new(),
        err_out: "",
        error: String::new(),
    }
}

async fn run_egress<F>(method: &str, c: EgressCase, want_request: &str, call: F)
where
    F: for<'a> FnOnce(
        &'a Api,
        Printer,
        Sink,
        Sink,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>,
{
    let fake = Fake::default();
    {
        let mut s = fake.state();
        if let Some(p) = &c.reply {
            s.reply(method, p);
        }
        if let Some(f) = c.failure.clone() {
            s.fail(method, f);
        }
        seed_actor(&mut s, c.lookup);
    }
    let api = connect(&fake).await;
    let (out, out_buf): (Sink, SharedBuf) = Sink::buffer();
    let (err, err_buf) = Sink::buffer();
    let result = call(&api, printer(c.format), out, err).await;
    let name = c.name;
    assert_eq!(err_text(&result), c.error, "{name}");
    let calls = fake.state().calls_to(method);
    assert_eq!(calls.len(), 1, "{name}");
    assert_eq!(calls[0].request, want_request, "{name}");
    let actor_reads = fake.state().calls_to("GetActor");
    assert_eq!(
        actor_reads.len(),
        usize::from(c.reads_actor),
        "{name}: actor reads"
    );
    if c.reads_actor {
        assert_eq!(
            actor_reads[0].request,
            debug(&pb::GetActorRequest {
                actor: Some(actor_ref())
            }),
            "{name}"
        );
    }
    assert_eq!(out_buf.contents(), c.out, "{name}: stdout");
    assert_eq!(err_buf.contents(), c.err_out, "{name}: stderr");
}

#[tokio::test]
async fn get_egress_policy_runner() {
    let policy = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
            version: 1,
            create_time: at(5 * 60),
            ..Default::default()
        }),
        rules: vec![http_rule("api.example.com")],
    };
    let not_found = || Some(Status::not_found("EgressPolicy not found"));
    let cases = vec![
        EgressCase {
            reply: Some(policy.clone()),
            out: "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   c1      1       1         5m\n".into(),
            ..case("table by default", OutputFormat::Table)
        },
        EgressCase {
            reply: Some(policy.clone()),
            out: "metadata:\n  createTime: \"2026-01-01T11:55:00Z\"\n  hive: team-a\n  name: default\n  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\n  version: \"1\"\nrules:\n- http:\n    hostnames:\n    - api.example.com\n".into(),
            ..case("yaml", OutputFormat::Yaml)
        },
        EgressCase {
            failure: not_found(),
            reads_actor: true,
            err_out: "actor \"c1\" in hive \"team-a\" has no egress policy\n",
            ..case(
                "no policy on an existing actor writes a note and succeeds",
                OutputFormat::Yaml,
            )
        },
        EgressCase {
            failure: not_found(),
            lookup: ActorLookup::Missing,
            reads_actor: true,
            error: "actor \"c1\" in hive \"team-a\" not found".into(),
            ..case("missing actor fails", OutputFormat::Yaml)
        },
        EgressCase {
            failure: not_found(),
            lookup: ActorLookup::Denied,
            reads_actor: true,
            error: "failed to get actor \"c1\" in hive \"team-a\": rpc error: code = PermissionDenied desc = denied".into(),
            ..case("actor lookup error wraps", OutputFormat::Yaml)
        },
        EgressCase {
            failure: Some(Status::unavailable("api-server down")),
            error: "failed to get egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = Unavailable desc = api-server down".into(),
            ..case("other error wraps", OutputFormat::Table)
        },
    ];
    let want = debug(&pb::GetActorEgressPolicyRequest {
        actor: Some(actor_ref()),
        name: "default".into(),
    });
    for c in cases {
        run_egress("GetActorEgressPolicy", c, &want, |api, p, out, err| {
            Box::pin(async move { get_with(api, &actor_ref(), &p, &out, &err).await })
        })
        .await;
    }
}

#[tokio::test]
async fn create_egress_policy_runner() {
    let rules = vec![http_rule("api.example.com")];
    // A manifest copied from another Actor still carries that Actor's
    // server-managed fields; they are sent as is for the server to scrub.
    let manifest = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: "old-uid".into(),
            version: 7,
            ..Default::default()
        }),
        rules: rules.clone(),
    };
    let created = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d".into(),
            version: 1,
            create_time: at(0),
            ..Default::default()
        }),
        rules,
    };
    let cases = vec![
        EgressCase {
            reply: Some(created.clone()),
            out: "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   c1      1       1         0s\n".into(),
            ..case("table by default", OutputFormat::Table)
        },
        EgressCase {
            reply: Some(created.clone()),
            out: "metadata:\n  createTime: \"2026-01-01T12:00:00Z\"\n  hive: team-a\n  name: default\n  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\n  version: \"1\"\nrules:\n- http:\n    hostnames:\n    - api.example.com\n".into(),
            ..case("yaml prints the created policy", OutputFormat::Yaml)
        },
        EgressCase {
            failure: Some(Status::already_exists("EgressPolicy already exists")),
            error: "failed to create egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = AlreadyExists desc = EgressPolicy already exists".into(),
            ..case("already exists wraps", OutputFormat::Table)
        },
        EgressCase {
            failure: Some(Status::failed_precondition("parent Actor does not exist")),
            error: "failed to create egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = FailedPrecondition desc = parent Actor does not exist".into(),
            ..case("missing actor wraps", OutputFormat::Table)
        },
    ];
    let want = debug(&pb::CreateActorEgressPolicyRequest {
        actor: Some(actor_ref()),
        egress_policy: Some(manifest.clone()),
    });
    for c in cases {
        let m = manifest.clone();
        run_egress(
            "CreateActorEgressPolicy",
            c,
            &want,
            move |api, p, out, _| {
                Box::pin(async move { create_with(api, &actor_ref(), m, &p, &out).await })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn update_egress_policy_runner() {
    const UID: &str = "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d";
    let rules = vec![
        http_rule("api.example.com"),
        pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: vec!["www.example.com".into()],
                ..Default::default()
            }),
            ..Default::default()
        },
    ];
    // What `get -o yaml` printed, edited: it still carries the uid, version
    // and timestamps of the policy it replaces.
    let manifest = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: UID.into(),
            version: 1,
            create_time: at(60),
            ..Default::default()
        }),
        rules: rules.clone(),
    };
    let updated = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            uid: UID.into(),
            version: 2,
            create_time: at(60),
            update_time: at(0),
            ..Default::default()
        }),
        rules,
    };
    let stale = "manifest's uid and version preconditions do not match those of the stored egress policy for actor \"c1\" in hive \"team-a\" (changed since it was read, or read from another actor's policy); re-run \"get egress-policy -o yaml\" and reapply the edit: rpc error: code = Aborted desc = ";
    let not_found = || Some(Status::not_found("EgressPolicy not found"));
    let cases = vec![
        EgressCase {
            reply: Some(updated.clone()),
            out: "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   c1      2       2         60s\n".into(),
            ..case("table by default", OutputFormat::Table)
        },
        EgressCase {
            reply: Some(updated.clone()),
            out: "metadata:\n  createTime: \"2026-01-01T11:59:00Z\"\n  hive: team-a\n  name: default\n  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d\n  updateTime: \"2026-01-01T12:00:00Z\"\n  version: \"2\"\nrules:\n- http:\n    hostnames:\n    - api.example.com\n- https:\n    hostnames:\n    - www.example.com\n".into(),
            ..case("yaml prints the updated policy", OutputFormat::Yaml)
        },
        EgressCase {
            failure: Some(Status::aborted("EgressPolicy version conflict")),
            error: format!("{stale}EgressPolicy version conflict"),
            ..case("stale version tells the user to re-read", OutputFormat::Table)
        },
        EgressCase {
            failure: Some(Status::aborted("EgressPolicy UID conflict")),
            error: format!("{stale}EgressPolicy UID conflict"),
            ..case("uid conflict tells the user to re-read", OutputFormat::Table)
        },
        EgressCase {
            failure: not_found(),
            lookup: ActorLookup::Missing,
            reads_actor: true,
            error: "actor \"c1\" in hive \"team-a\" not found".into(),
            ..case("missing actor names the actor", OutputFormat::Table)
        },
        EgressCase {
            failure: not_found(),
            reads_actor: true,
            error: "actor \"c1\" in hive \"team-a\" has no egress policy to update; create it with \"hivectl create egress-policy\"".into(),
            ..case("existing actor with no policy points at create", OutputFormat::Table)
        },
        EgressCase {
            failure: not_found(),
            lookup: ActorLookup::Denied,
            reads_actor: true,
            error: "failed to get actor \"c1\" in hive \"team-a\": rpc error: code = PermissionDenied desc = denied".into(),
            ..case("actor lookup error wraps", OutputFormat::Table)
        },
        EgressCase {
            failure: Some(Status::unavailable("api-server down")),
            error: "failed to update egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = Unavailable desc = api-server down".into(),
            ..case("other error wraps", OutputFormat::Table)
        },
    ];
    let want = debug(&pb::UpdateActorEgressPolicyRequest {
        actor: Some(actor_ref()),
        egress_policy: Some(manifest.clone()),
    });
    for c in cases {
        let m = manifest.clone();
        run_egress(
            "UpdateActorEgressPolicy",
            c,
            &want,
            move |api, p, out, _| {
                Box::pin(async move { update_with(api, &actor_ref(), m, &p, &out).await })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn delete_egress_policy_runner() {
    const UID: &str = "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d";
    let deleted = pb::EgressPolicy {
        metadata: Some(pb::ResourceMetadata {
            hive: "team-a".into(),
            name: "default".into(),
            version: 1,
            ..Default::default()
        }),
        rules: vec![http_rule("api.example.com")],
    };
    let confirmed = "egress policy for actor \"c1\" in hive \"team-a\" deleted\n";
    let opts = |uid: &str, version: i64| {
        Some(pb::DeleteOptions {
            uid: uid.into(),
            version,
        })
    };
    let not_found = || Some(Status::not_found("EgressPolicy not found"));
    let cases: Vec<(EgressCase, Option<pb::DeleteOptions>)> = vec![
        (
            EgressCase {
                reply: Some(deleted.clone()),
                out: confirmed.into(),
                ..case("no flags leave options unset", OutputFormat::Table)
            },
            None,
        ),
        (
            EgressCase {
                reply: Some(deleted.clone()),
                out: confirmed.into(),
                ..case("uid and version populate options", OutputFormat::Table)
            },
            opts(UID, 3),
        ),
        (
            EgressCase {
                reply: Some(deleted.clone()),
                out: confirmed.into(),
                ..case("uid alone", OutputFormat::Table)
            },
            opts(UID, 0),
        ),
        (
            EgressCase {
                reply: Some(deleted.clone()),
                out: confirmed.into(),
                ..case("version alone", OutputFormat::Table)
            },
            opts("", 3),
        ),
        (
            EgressCase {
                failure: not_found(),
                reads_actor: true,
                error: "actor \"c1\" in hive \"team-a\" has no egress policy".into(),
                ..case("missing policy on an existing actor fails", OutputFormat::Table)
            },
            None,
        ),
        (
            EgressCase {
                failure: not_found(),
                reads_actor: true,
                error: "actor \"c1\" in hive \"team-a\" has no egress policy".into(),
                ..case("guarded delete not found still reads actor", OutputFormat::Table)
            },
            opts(UID, 3),
        ),
        (
            EgressCase {
                failure: not_found(),
                lookup: ActorLookup::Missing,
                reads_actor: true,
                error: "actor \"c1\" in hive \"team-a\" not found".into(),
                ..case("missing actor fails", OutputFormat::Table)
            },
            None,
        ),
        (
            EgressCase {
                failure: not_found(),
                lookup: ActorLookup::Denied,
                reads_actor: true,
                error: "failed to get actor \"c1\" in hive \"team-a\": rpc error: code = PermissionDenied desc = denied".into(),
                ..case("actor lookup error wraps", OutputFormat::Table)
            },
            None,
        ),
        (
            EgressCase {
                failure: Some(Status::aborted("EgressPolicy version conflict")),
                error: "failed to delete egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = Aborted desc = EgressPolicy version conflict".into(),
                ..case("aborted conflict wraps", OutputFormat::Table)
            },
            opts("", 3),
        ),
        (
            EgressCase {
                failure: Some(Status::unavailable("api-server down")),
                error: "failed to delete egress policy for actor \"c1\" in hive \"team-a\": rpc error: code = Unavailable desc = api-server down".into(),
                ..case("unavailable wraps", OutputFormat::Table)
            },
            None,
        ),
    ];
    for (c, options) in cases {
        let want = debug(&pb::DeleteActorEgressPolicyRequest {
            actor: Some(actor_ref()),
            name: "default".into(),
            options: options.clone(),
        });
        run_egress(
            "DeleteActorEgressPolicy",
            c,
            &want,
            move |api, _, out, _| {
                Box::pin(async move { delete_with(api, &actor_ref(), options, &out).await })
            },
        )
        .await;
    }
}
