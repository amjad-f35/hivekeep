//! One check per threat, each a subtest named for it. Every negative check
//! has a positive control showing the request could have succeeded: a
//! denial that stops exercising its boundary keeps passing.

use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_e2e::env::system_namespace;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::install;
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::helpers::*;

pub async fn test_security_regression(t: &T, f: &Fixture) {
    t.run("authorization is enforced by default", |t| async move {
        enforced_by_default(&t, f).await
    })
    .await;
    t.run("invalid credentials are rejected", |t| async move {
        invalid_credentials(&t, f).await
    })
    .await;
    t.run("no access across hives", |t| async move {
        tenant_crossing(&t, f).await
    })
    .await;
    t.run("editor cannot grant itself owner", |t| async move {
        privilege_escalation(&t, f).await
    })
    .await;
    t.run("revocation takes effect", |t| async move {
        revocation(&t, f).await
    })
    .await;
    t.run("recreated hive drops old grants", |t| async move {
        hive_recreated(&t, f).await
    })
    .await;
    t.run(
        "recreated AccessPolicy rejects stale updates",
        |t| async move { access_policy_recreated(&t, f).await },
    )
    .await;
    t.run("WorkerService accepts only the keeper", |t| async move {
        worker_service_requires_keeper(&t, f).await
    })
    .await;
}

fn hive_ref(hive: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: String::new(),
        name: hive.into(),
    }
}

/// A default install enforces authorization: a ServiceAccount no policy
/// names gets nothing. An install that left it off would pass every unit
/// test.
async fn enforced_by_default(t: &T, f: &Fixture) {
    let nobody = new_principal(t, &f.clients, &f.hive, "boundary-nobody").await;
    expect_all(
        t,
        vec![
            call("GetActor", &f.bootstrap, get_actor(&f.actor), ALLOWED),
            call("GetActor", &nobody, get_actor(&f.actor), DENIED),
            call(
                "CreateActor",
                &nobody,
                create_actor_unnamed(&f.hive, &f.template),
                DENIED,
            ),
        ],
    )
    .await;
}

/// A bearer token the API cannot verify is refused, not treated as some
/// caller.
async fn invalid_credentials(t: &T, f: &Fixture) {
    let mut forged = f.bootstrap.clone();
    forged.name = "invalid-token".into();
    forged.member.clear();
    forged.apis = apis_with_token(t, &f.clients, "not-a-token").await;
    expect_all(
        t,
        vec![
            call("GetActor", &f.bootstrap, get_actor(&f.actor), ALLOWED),
            call("GetActor", &forged, get_actor(&f.actor), REJECTED),
        ],
    )
    .await;
}

/// An editor of one hive cannot reach into another.
async fn tenant_crossing(t: &T, f: &Fixture) {
    let other = format!("{}-other", f.hive);
    create_hive(t, &f.clients, &other).await;
    let grace = new_principal(t, &f.clients, &f.hive, "boundary-grace").await;
    set_hive_policy(t, &f.clients, &other, vec![binding("editor", &[&grace])]).await;
    expect_all(
        t,
        vec![
            // Control: grace's grant on her own hive works.
            call(
                "ListActors in her hive",
                &grace,
                list_actors(&other),
                ALLOWED,
            ),
            call(
                "GetActor in another hive",
                &grace,
                get_actor(&f.actor),
                DENIED,
            ),
            call(
                "CreateActor in another hive",
                &grace,
                create_actor_unnamed(&f.hive, &f.template),
                DENIED,
            ),
        ],
    )
    .await;
}

/// An editor cannot rewrite the AccessPolicy to make itself owner, while
/// an owner can update it.
async fn privilege_escalation(t: &T, f: &Fixture) {
    let owner = new_principal(t, &f.clients, &f.hive, "escalate-owner").await;
    let editor = new_principal(t, &f.clients, &f.hive, "escalate-editor").await;
    let current = vec![binding("owner", &[&owner]), binding("editor", &[&editor])];
    set_hive_policy(t, &f.clients, &f.hive, current.clone()).await;
    cleanup_hive_policy(t, &f.clients, &f.hive);

    // Rewrites the bindings, guarded by the uid and version the bootstrap
    // owner reads just before, so the update is otherwise valid.
    let update_policy = |bindings: Vec<pb::Binding>| {
        let bootstrap = f.clients.api();
        let hive = f.hive.clone();
        rpc(move |mut a| {
            let (mut bootstrap, hive, bindings) =
                (bootstrap.clone(), hive.clone(), bindings.clone());
            async move {
                let cur = bootstrap
                    .get_hive_access_policy(pb::GetHiveAccessPolicyRequest {
                        hive: Some(hive_ref(&hive)),
                        name: SINGLETON_NAME.into(),
                    })
                    .await?
                    .into_inner();
                a.control
                    .update_hive_access_policy(pb::UpdateHiveAccessPolicyRequest {
                        hive: Some(hive_ref(&hive)),
                        access_policy: Some(pb::AccessPolicy {
                            metadata: cur.metadata,
                            bindings,
                        }),
                    })
                    .await
                    .map(|_| ())
            }
        })
    };

    // Both grants first, so the editor's denial below is for its role and
    // not for having none yet.
    expect_all(
        t,
        vec![
            call("GetActor", &owner, get_actor(&f.actor), ALLOWED),
            call("GetActor", &editor, get_actor(&f.actor), ALLOWED),
        ],
    )
    .await;
    if t.failed() {
        return;
    }
    expect_all(
        t,
        vec![
            call(
                "UpdateHiveAccessPolicy to make itself owner",
                &editor,
                update_policy(vec![binding("owner", &[&owner, &editor])]),
                DENIED,
            ),
            // Control: the same request with the bindings unchanged, as owner.
            call(
                "UpdateHiveAccessPolicy unchanged",
                &owner,
                update_policy(current),
                ALLOWED,
            ),
        ],
    )
    .await;
}

/// Removing a binding, downgrading a role and deleting the AccessPolicy
/// each take effect on every replica, with no cached grant outliving the
/// change. Steps run in order.
async fn revocation(t: &T, f: &Fixture) {
    let carol = new_principal(t, &f.clients, &f.hive, "revoke-carol").await;
    let dan = new_principal(t, &f.clients, &f.hive, "revoke-dan").await;
    let erin = new_principal(t, &f.clients, &f.hive, "revoke-erin").await;
    cleanup_hive_policy(t, &f.clients, &f.hive);
    let create = || create_actor_unnamed(&f.hive, &f.template);
    let (carol, dan, erin) = (&carol, &dan, &erin);

    // Later steps assume each earlier change took effect.
    let ok = t
        .run("granted", |t| async move {
            set_hive_policy(
                &t,
                &f.clients,
                &f.hive,
                vec![binding("editor", &[carol, dan]), binding("viewer", &[erin])],
            )
            .await;
            expect_all(
                &t,
                vec![
                    call("CreateActor", carol, create(), PASSED_CHECK),
                    call("CreateActor", dan, create(), PASSED_CHECK),
                    call("CreateActor", erin, create(), DENIED),
                ],
            )
            .await;
        })
        .await;
    if !ok {
        t.fatal("step \"granted\" failed");
    }
    let ok = t
        .run("binding removed", |t| async move {
            set_hive_policy(
                &t,
                &f.clients,
                &f.hive,
                vec![binding("editor", &[dan]), binding("viewer", &[erin])],
            )
            .await;
            expect_all(
                &t,
                vec![
                    call("CreateActor", carol, create(), DENIED),
                    call("GetActor", carol, get_actor(&f.actor), DENIED),
                    call("CreateActor", dan, create(), PASSED_CHECK),
                ],
            )
            .await;
        })
        .await;
    if !ok {
        t.fatal("step \"binding removed\" failed");
    }
    let ok = t
        .run("editor downgraded to viewer", |t| async move {
            set_hive_policy(
                &t,
                &f.clients,
                &f.hive,
                vec![binding("viewer", &[dan, erin])],
            )
            .await;
            expect_all(
                &t,
                vec![
                    call("CreateActor", dan, create(), DENIED),
                    call("GetActor", dan, get_actor(&f.actor), ALLOWED),
                ],
            )
            .await;
        })
        .await;
    if !ok {
        t.fatal("step \"editor downgraded to viewer\" failed");
    }
    let ok = t
        .run("AccessPolicy deleted", |t| async move {
            if let Err(e) = delete_hive_policy(&f.clients, &f.hive).await {
                t.fatal(format!("DeleteHiveAccessPolicy: {e}"));
            }
            expect_all(
                &t,
                vec![
                    call("GetActor", dan, get_actor(&f.actor), DENIED),
                    call("GetActor", erin, get_actor(&f.actor), DENIED),
                    call("GetActor", &f.bootstrap, get_actor(&f.actor), ALLOWED),
                ],
            )
            .await;
        })
        .await;
    if !ok {
        t.fatal("step \"AccessPolicy deleted\" failed");
    }
}

/// Grants on a hive go with it: a new hive with the same name does not hand
/// the old one's members access.
async fn hive_recreated(t: &T, f: &Fixture) {
    let name = format!("{}-aba", f.hive);
    let frank = new_principal(t, &f.clients, &f.hive, "aba-frank").await;
    create_hive(t, &f.clients, &name).await;
    set_hive_policy(t, &f.clients, &name, vec![binding("owner", &[&frank])]).await;
    expect_all(t, vec![call("GetHive", &frank, get_hive(&name), ALLOWED)]).await;
    if t.failed() {
        return;
    }
    if let Err(e) = delete_hive(&f.clients, &name).await {
        t.fatal(format!("DeleteHive {name:?}: {e}"));
    }
    create_hive(t, &f.clients, &name).await;
    expect_all(
        t,
        vec![
            call("GetHive", &frank, get_hive(&name), DENIED),
            call("GetHive", &f.bootstrap, get_hive(&name), ALLOWED),
        ],
    )
    .await;
}

/// A deleted and recreated AccessPolicy is a new object: it carries none of
/// the old bindings, and an update guarded by the old uid is rejected.
async fn access_policy_recreated(t: &T, f: &Fixture) {
    let gina = new_principal(t, &f.clients, &f.hive, "aba-gina").await;
    set_hive_policy(t, &f.clients, &f.hive, vec![binding("viewer", &[&gina])]).await;
    cleanup_hive_policy(t, &f.clients, &f.hive);
    expect_all(
        t,
        vec![call("GetActor", &gina, get_actor(&f.actor), ALLOWED)],
    )
    .await;
    if t.failed() {
        return;
    }
    let mut api = f.clients.api();
    let get = pb::GetHiveAccessPolicyRequest {
        hive: Some(hive_ref(&f.hive)),
        name: SINGLETON_NAME.into(),
    };
    let old = api
        .get_hive_access_policy(get.clone())
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetHiveAccessPolicy: {e}")))
        .into_inner();
    if let Err(e) = delete_hive_policy(&f.clients, &f.hive).await {
        t.fatal(format!("DeleteHiveAccessPolicy: {e}"));
    }
    let create = pb::CreateHiveAccessPolicyRequest {
        hive: Some(hive_ref(&f.hive)),
        access_policy: Some(pb::AccessPolicy {
            metadata: Some(pb::ResourceMetadata {
                name: SINGLETON_NAME.into(),
                ..Default::default()
            }),
            bindings: vec![],
        }),
    };
    if let Err(e) = api.create_hive_access_policy(create).await {
        t.fatal(format!("CreateHiveAccessPolicy: {e}"));
    }
    // The new policy has none of the old one's bindings.
    expect_all(
        t,
        vec![call("GetActor", &gina, get_actor(&f.actor), DENIED)],
    )
    .await;
    // An update guarded by the old policy's uid and version must not apply.
    let stale = api
        .update_hive_access_policy(pb::UpdateHiveAccessPolicyRequest {
            hive: Some(hive_ref(&f.hive)),
            access_policy: Some(pb::AccessPolicy {
                metadata: old.metadata,
                bindings: vec![binding("owner", &[&gina])],
            }),
        })
        .await;
    match stale {
        Err(e) if e.code() == Code::Aborted => {}
        other => t.error(format!(
            "UpdateHiveAccessPolicy with the deleted policy's uid: got {:?}, want Aborted",
            other.map(|_| ())
        )),
    }
    let cur = api
        .get_hive_access_policy(get)
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetHiveAccessPolicy: {e}")))
        .into_inner();
    if !cur.bindings.is_empty() {
        t.error(format!(
            "recreated AccessPolicy bindings = {:?}, want none",
            cur.bindings
        ));
    }
}

/// The WorkerService rejects every caller but the keeper. Its RPCs skip the
/// permission checks; each handler authenticates the keeper by its mTLS
/// certificate, so that is all that guards them. A bearer token is rejected
/// even when it is the keeper's own ServiceAccount's or the bootstrap
/// owner's.
async fn worker_service_requires_keeper(t: &T, f: &Fixture) {
    let ns = system_namespace();
    let sa = install::KEEPER_SERVICE_ACCOUNT;
    // Every install creates the keeper's ServiceAccount, so a missing one
    // fails rather than skipping these checks.
    if !service_account_exists(t, &f.clients, &ns, sa).await {
        t.fatal(format!("ServiceAccount {ns}/{sa} not found"));
    }
    let keeper_token = principal_for(t, &f.clients, &ns, sa).await;

    // The worker named is never created: a handler that let the caller
    // through would answer NotFound, not reject it.
    let worker = pb::ObjectRef {
        hive: String::new(),
        name: "authz-missing".into(),
    };
    let actor = f.actor.clone();
    const UID: &str = "00000000-0000-4000-8000-000000000000";

    let w = worker.clone();
    let register = rpc(move |mut a| {
        let req = pb::RegisterWorkerRequest {
            worker: Some(w.clone()),
            ..Default::default()
        };
        async move { a.workers.register_worker(req).await.map(|_| ()) }
    });
    let (w, ac) = (worker.clone(), actor.clone());
    let request_suspend = rpc(move |mut a| {
        let req = pb::RequestActorSuspendRequest {
            worker: Some(w.clone()),
            actor: Some(ac.clone()),
            ..Default::default()
        };
        async move { a.workers.request_actor_suspend(req).await.map(|_| ()) }
    });
    // Validated before the caller is authenticated, so the request must be
    // valid; the CSR is only parsed after authentication.
    let (w, ac) = (worker.clone(), actor.clone());
    let mint_cert = rpc(move |mut a| {
        let req = pb::MintWorkerActorCertificateRequest {
            worker: Some(w.clone()),
            actor: Some(ac.clone()),
            actor_uid: UID.into(),
            certificate_signing_request: b"not a CSR".to_vec(),
        };
        async move {
            a.workers
                .mint_worker_actor_certificate(req)
                .await
                .map(|_| ())
        }
    });
    let ac = actor.clone();
    let mint_jwt = rpc(move |mut a| {
        let req = pb::MintWorkerActorJwtRequest {
            actor: Some(ac.clone()),
            actor_uid: UID.into(),
            audiences: vec!["authz-e2e".into()],
        };
        async move { a.workers.mint_worker_actor_jwt(req).await.map(|_| ()) }
    });
    let ac = actor.clone();
    let report = rpc(move |mut a| {
        let req = pb::ReportActorSnapshotStateRequest {
            actor: Some(ac.clone()),
            actor_uid: UID.into(),
            state: Some(Default::default()),
        };
        async move { a.workers.report_actor_snapshot_state(req).await.map(|_| ()) }
    });

    let mut calls = Vec::new();
    for (name, r) in [
        ("RegisterWorker", register),
        ("RequestActorSuspend", request_suspend),
        ("MintWorkerActorCertificate", mint_cert),
        ("MintWorkerActorJwt", mint_jwt),
        ("ReportActorSnapshotState", report),
    ] {
        calls.push(call(name, &f.bootstrap, r.clone(), REJECTED));
        calls.push(call(name, &keeper_token, r, REJECTED));
    }
    expect_all(t, calls).await;
}
