//! Hives, egress policies, access policies and authorization end to end.

mod common;

use common::*;
use hivekeep_api::authz::SystemPrincipals;
use hivekeep_api::context::{request_as, system_request, Principal};
use hivekeep_proto::api::v1 as pb;
use tonic::{Code, Request};

#[tokio::test]
async fn hive_crud() {
    let h = Harness::new().await;
    assert_code(
        h.c().create_hive(h.req(hive(HIVE))).await,
        Code::AlreadyExists,
        "already exists",
    );
    h.c().create_hive(h.req(hive("h2"))).await.unwrap();
    let got = h
        .c()
        .get_hive(h.req(pb::GetHiveRequest {
            hive: Some(wref("h2")),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.metadata.unwrap().name, "h2");
    assert_code(
        h.c()
            .get_hive(h.req(pb::GetHiveRequest {
                hive: Some(wref("nope")),
            }))
            .await,
        Code::NotFound,
        "Hive nope not found",
    );
    let l = h
        .c()
        .list_hives(h.req(pb::ListHivesRequest { options: None }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(l.hives.len(), 2);
    assert_code(
        h.c().create_hive(h.req(hive("Bad_Name"))).await,
        Code::InvalidArgument,
        "",
    );
}

#[tokio::test]
async fn delete_hive() {
    let h = Harness::new().await;
    h.create_template().await;
    // Not empty.
    assert_code(
        h.c()
            .delete_hive(h.req(pb::DeleteHiveRequest {
                hive: Some(wref(HIVE)),
                options: None,
            }))
            .await,
        Code::FailedPrecondition,
        "not empty",
    );
    // Scoped to the target hive.
    h.c().create_hive(h.req(hive("h2"))).await.unwrap();
    let r = h
        .c()
        .delete_hive(h.req(pb::DeleteHiveRequest {
            hive: Some(wref("h2")),
            options: Some(pb::DeleteOptions {
                uid: uuid::Uuid::new_v4().to_string(),
                version: 0,
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "does not have uid");
    let d = h
        .c()
        .delete_hive(h.req(pb::DeleteHiveRequest {
            hive: Some(wref("h2")),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(d.metadata.unwrap().delete_time.is_some());
    assert_code(
        h.c()
            .delete_hive(h.req(pb::DeleteHiveRequest {
                hive: Some(wref("h2")),
                options: None,
            }))
            .await,
        Code::NotFound,
        "",
    );
}

fn rules(host: &str) -> Vec<pb::EgressRule> {
    vec![pb::EgressRule {
        https: Some(pb::HttpsRule {
            hostnames: vec![host.into()],
            ports: None,
            effects: None,
        }),
        ..Default::default()
    }]
}

#[tokio::test]
async fn actor_egress_policy_crud() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    let create = |rules| pb::CreateActorEgressPolicyRequest {
        actor: Some(oref(HIVE, "a1")),
        egress_policy: Some(pb::EgressPolicy {
            metadata: Some(md(HIVE, "default")),
            rules,
        }),
    };
    let p = h
        .c()
        .create_actor_egress_policy(h.req(create(rules("example.com"))))
        .await
        .unwrap()
        .into_inner();
    // Defaults fill the https port.
    let ports = p.rules[0].https.as_ref().unwrap().ports.as_ref().unwrap();
    assert_eq!(ports.numbers, vec![443]);
    assert_code(
        h.c()
            .create_actor_egress_policy(h.req(create(rules("example.com"))))
            .await,
        Code::AlreadyExists,
        "",
    );
    let got = h
        .c()
        .get_actor_egress_policy(h.req(pb::GetActorEgressPolicyRequest {
            actor: Some(oref(HIVE, "a1")),
            name: "default".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got, p);
    let mut u = p.clone();
    u.rules = rules("other.example");
    let updated = h
        .c()
        .update_actor_egress_policy(h.req(pb::UpdateActorEgressPolicyRequest {
            actor: Some(oref(HIVE, "a1")),
            egress_policy: Some(u.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        updated.rules[0].https.as_ref().unwrap().hostnames,
        vec!["other.example"]
    );
    assert_code(
        h.c()
            .update_actor_egress_policy(h.req(pb::UpdateActorEgressPolicyRequest {
                actor: Some(oref(HIVE, "a1")),
                egress_policy: Some(u),
            }))
            .await,
        Code::Aborted,
        "version conflict",
    );
    let r = h
        .c()
        .delete_actor_egress_policy(h.req(pb::DeleteActorEgressPolicyRequest {
            actor: Some(oref(HIVE, "a1")),
            name: "default".into(),
            options: Some(pb::DeleteOptions {
                uid: uuid::Uuid::new_v4().to_string(),
                version: 0,
            }),
        }))
        .await;
    assert_code(r, Code::Aborted, "UID conflict");
    h.c()
        .delete_actor_egress_policy(h.req(pb::DeleteActorEgressPolicyRequest {
            actor: Some(oref(HIVE, "a1")),
            name: "default".into(),
            options: None,
        }))
        .await
        .unwrap();
    assert_code(
        h.c()
            .get_actor_egress_policy(h.req(pb::GetActorEgressPolicyRequest {
                actor: Some(oref(HIVE, "a1")),
                name: "default".into(),
            }))
            .await,
        Code::NotFound,
        "EgressPolicy for actor",
    );
    // A missing parent Actor.
    let mut orphan = create(rules("example.com"));
    orphan.actor = Some(oref(HIVE, "ghost"));
    assert_code(
        h.c().create_actor_egress_policy(h.req(orphan)).await,
        Code::FailedPrecondition,
        "parent Actor",
    );
    // Invalid name.
    let mut bad = create(rules("example.com"));
    bad.egress_policy
        .as_mut()
        .unwrap()
        .metadata
        .as_mut()
        .unwrap()
        .name = "other".into();
    assert_code(
        h.c().create_actor_egress_policy(h.req(bad)).await,
        Code::InvalidArgument,
        "",
    );
}

#[tokio::test]
async fn delete_actor_cascades_egress_policy() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_actor("a1", "tmpl1").await;
    h.c()
        .create_actor_egress_policy(h.req(pb::CreateActorEgressPolicyRequest {
            actor: Some(oref(HIVE, "a1")),
            egress_policy: Some(pb::EgressPolicy {
                metadata: Some(md(HIVE, "default")),
                rules: rules("example.com"),
            }),
        }))
        .await
        .unwrap();
    h.delete("a1").await.unwrap();
    h.create_actor("a1", "tmpl1").await;
    assert_code(
        h.c()
            .get_actor_egress_policy(h.req(pb::GetActorEgressPolicyRequest {
                actor: Some(oref(HIVE, "a1")),
                name: "default".into(),
            }))
            .await,
        Code::NotFound,
        "",
    );
}

fn policy(bindings: &[(&str, &str)]) -> pb::AccessPolicy {
    pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            ..Default::default()
        }),
        bindings: bindings
            .iter()
            .map(|(r, m)| pb::Binding {
                role: r.to_string(),
                members: vec![m.to_string()],
            })
            .collect(),
    }
}

#[tokio::test]
async fn global_and_hive_access_policy_crud() {
    let h = Harness::new().await;
    assert_code(
        h.c()
            .get_global_access_policy(h.req(pb::GetGlobalAccessPolicyRequest {
                name: "default".into(),
            }))
            .await,
        Code::NotFound,
        "Global AccessPolicy not found",
    );
    let g = h
        .c()
        .create_global_access_policy(h.req(pb::CreateGlobalAccessPolicyRequest {
            access_policy: Some(policy(&[("owner", "user:alice")])),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_code(
        h.c()
            .create_global_access_policy(h.req(pb::CreateGlobalAccessPolicyRequest {
                access_policy: Some(policy(&[("owner", "user:alice")])),
            }))
            .await,
        Code::AlreadyExists,
        "",
    );
    // editor is not a global role.
    let mut bad = g.clone();
    bad.bindings = policy(&[("editor", "user:bob")]).bindings;
    assert_code(
        h.c()
            .update_global_access_policy(h.req(pb::UpdateGlobalAccessPolicyRequest {
                access_policy: Some(bad),
            }))
            .await,
        Code::InvalidArgument,
        "",
    );
    let mut u = g.clone();
    u.bindings = policy(&[("viewer", "user:bob")]).bindings;
    let u = h
        .c()
        .update_global_access_policy(h.req(pb::UpdateGlobalAccessPolicyRequest {
            access_policy: Some(u),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(u.bindings[0].role, "viewer");

    let hp = h
        .c()
        .create_hive_access_policy(h.req(pb::CreateHiveAccessPolicyRequest {
            hive: Some(wref(HIVE)),
            access_policy: Some(policy(&[("editor", "user:bob")])),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_code(
        h.c()
            .create_hive_access_policy(h.req(pb::CreateHiveAccessPolicyRequest {
                hive: Some(wref("nope")),
                access_policy: Some(policy(&[])),
            }))
            .await,
        Code::FailedPrecondition,
        "parent Hive",
    );
    let got = h
        .c()
        .get_hive_access_policy(h.req(pb::GetHiveAccessPolicyRequest {
            hive: Some(wref(HIVE)),
            name: "default".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got, hp);
    let mut u = hp.clone();
    u.bindings = policy(&[("owner", "user:carol")]).bindings;
    h.c()
        .update_hive_access_policy(h.req(pb::UpdateHiveAccessPolicyRequest {
            hive: Some(wref(HIVE)),
            access_policy: Some(u.clone()),
        }))
        .await
        .unwrap();
    assert_code(
        h.c()
            .update_hive_access_policy(h.req(pb::UpdateHiveAccessPolicyRequest {
                hive: Some(wref(HIVE)),
                access_policy: Some(u),
            }))
            .await,
        Code::Aborted,
        "version conflict",
    );
    h.c()
        .delete_hive_access_policy(h.req(pb::DeleteHiveAccessPolicyRequest {
            hive: Some(wref(HIVE)),
            name: "default".into(),
            options: None,
        }))
        .await
        .unwrap();
    assert_code(
        h.c()
            .get_hive_access_policy(h.req(pb::GetHiveAccessPolicyRequest {
                hive: Some(wref(HIVE)),
                name: "default".into(),
            }))
            .await,
        Code::NotFound,
        "",
    );
}

fn as_user<T>(id: &str, msg: T) -> Request<T> {
    request_as(msg, Some(Principal::jwt(id, ISSUER)), None)
}

#[tokio::test]
async fn access_policy_governance() {
    let h = Harness::with(Options {
        policy_authz: Some((
            vec!["tester".to_string()],
            SystemPrincipals {
                ingress_router: "spiffe://router".into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    })
    .await;

    // Nobody but the bootstrap owner can do anything yet.
    assert_code(
        h.c().create_hive(as_user("alice", hive("h2"))).await,
        Code::PermissionDenied,
        "lacks \"can_create_hive\"",
    );
    assert_code(
        h.c()
            .list_hives(request_as(
                pb::ListHivesRequest { options: None },
                None,
                None,
            ))
            .await,
        Code::Unauthenticated,
        "",
    );
    // Bootstrap owner grants alice editor on the hive and bob global viewer.
    h.c()
        .create_hive_access_policy(h.req(pb::CreateHiveAccessPolicyRequest {
            hive: Some(wref(HIVE)),
            access_policy: Some(policy(&[("editor", "user:alice")])),
        }))
        .await
        .unwrap();
    h.c()
        .create_global_access_policy(h.req(pb::CreateGlobalAccessPolicyRequest {
            access_policy: Some(policy(&[("viewer", "user:bob")])),
        }))
        .await
        .unwrap();
    h.create_template().await;
    let create = pb::CreateActorRequest {
        actor: Some(pb::Actor {
            metadata: Some(md(HIVE, "a1")),
            actor_template: Some(oref(HIVE, "tmpl1")),
            ..Default::default()
        }),
    };
    h.c()
        .create_actor(as_user("alice", create.clone()))
        .await
        .unwrap();
    assert_code(
        h.c().create_actor(as_user("bob", create)).await,
        Code::PermissionDenied,
        "",
    );
    // Viewers read but do not mutate.
    h.c()
        .get_actor(as_user(
            "bob",
            pb::GetActorRequest {
                actor: Some(oref(HIVE, "a1")),
            },
        ))
        .await
        .unwrap();
    h.c()
        .list_actors(as_user(
            "bob",
            pb::ListActorsRequest {
                hive: String::new(),
                options: None,
            },
        ))
        .await
        .unwrap();
    assert_code(
        h.c()
            .delete_actor(as_user(
                "bob",
                pb::DeleteActorRequest {
                    actor: Some(oref(HIVE, "a1")),
                    options: None,
                },
            ))
            .await,
        Code::PermissionDenied,
        "",
    );
    // Editors cannot delete the hive or change its policy.
    assert_code(
        h.c()
            .delete_hive(as_user(
                "alice",
                pb::DeleteHiveRequest {
                    hive: Some(wref(HIVE)),
                    options: None,
                },
            ))
            .await,
        Code::PermissionDenied,
        "",
    );
    assert_code(
        h.c()
            .delete_hive_access_policy(as_user(
                "alice",
                pb::DeleteHiveAccessPolicyRequest {
                    hive: Some(wref(HIVE)),
                    name: "default".into(),
                    options: None,
                },
            ))
            .await,
        Code::PermissionDenied,
        "",
    );
    // Malformed identifiers fail closed before any check.
    assert_code(
        h.c()
            .get_actor(as_user("alice", pb::GetActorRequest { actor: None }))
            .await,
        Code::InvalidArgument,
        "",
    );
    // System roles apply over mTLS only.
    let router = request_as(
        pb::ResumeActorRequest {
            actor: Some(oref(HIVE, "a1")),
        },
        Some(Principal::mtls("spiffe://router")),
        None,
    );
    assert_code(
        h.c().resume_actor(router).await,
        Code::ResourceExhausted,
        "",
    );
    let router_jwt = request_as(
        pb::ResumeActorRequest {
            actor: Some(oref(HIVE, "a1")),
        },
        Some(Principal::jwt("spiffe://router", ISSUER)),
        None,
    );
    assert_code(
        h.c().resume_actor(router_jwt).await,
        Code::PermissionDenied,
        "",
    );
    // In-process system calls bypass authorization.
    h.c()
        .get_actor(system_request(pb::GetActorRequest {
            actor: Some(oref(HIVE, "a1")),
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn external_volumes_require_gate_and_opt_in() {
    let h = Harness::new().await;
    let ext = pb::Volume {
        name: "data".into(),
        external: Some(pb::ExternalVolumeSource {
            template: Some(pb::ExternalVolumeTemplate {
                capacity_bytes: 1 << 30,
                storage_class_name: "standard".into(),
            }),
        }),
        ..Default::default()
    };
    let mut c = container();
    c.volume_mounts = vec![pb::VolumeMount {
        name: "data".into(),
        mount_path: "/data".into(),
    }];
    let r = h
        .c()
        .create_actor_template(h.req(pb::CreateActorTemplateRequest {
            actor_template: Some(pb::ActorTemplate {
                metadata: Some(md(HIVE, "t")),
                containers: vec![c],
                volumes: vec![ext],
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 1000,
                        memory_bytes: 512 << 20,
                    }),
                }),
                ..Default::default()
            }),
        }))
        .await;
    assert!(r.is_err(), "external volumes are a preview feature");
}
