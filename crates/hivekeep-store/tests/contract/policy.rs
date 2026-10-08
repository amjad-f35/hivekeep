use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;

type S = Arc<dyn Store>;

fn egress(hive: &str) -> pb::EgressPolicy {
    pb::EgressPolicy {
        metadata: md(hive, "default"),
        rules: vec![pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: vec!["api.example.com".into()],
                ports: Some(pb::Ports {
                    all: None,
                    numbers: vec![80],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

pub async fn egress_lifecycle(s: S) {
    mk_hive(&*s, HIVE).await;
    let a = s
        .create_actor(&actor(HIVE, "session-1", pb::ActorState::Running))
        .await
        .unwrap();
    let r = ActorRef::of(&a);
    let policy = egress(HIVE);

    let created = s.create_egress_policy(&r, &policy).await.unwrap();
    let md = created.metadata.as_ref().unwrap();
    assert_eq!(
        (md.name.as_str(), md.hive.as_str(), md.version),
        ("default", HIVE, 1)
    );
    assert!(!md.uid.is_empty() && md.create_time.is_some() && md.update_time.is_some());
    assert!(matches!(
        s.create_egress_policy(&r, &policy).await,
        Err(StoreError::AlreadyExists)
    ));
    assert_eq!(s.get_egress_policy(&r).await.unwrap(), created);

    let noop = |_: &mut pb::EgressPolicy| Ok(());
    assert!(matches!(
        s.update_egress_policy(&r, Precondition::default(), &mut noop.clone())
            .await,
        Err(StoreError::PreconditionRequired(_))
    ));
    assert!(matches!(
        s.update_egress_policy(&r, Precondition::new(created.uid(), 99), &mut noop.clone())
            .await,
        Err(StoreError::VersionConflict)
    ));
    assert!(matches!(
        s.update_egress_policy(
            &r,
            Precondition::new("replacement-uid", 1),
            &mut noop.clone()
        )
        .await,
        Err(StoreError::UidConflict)
    ));
    let updated = s
        .update_egress_policy(&r, Precondition::of(&created), &mut |p| {
            let md = p.metadata_mut();
            md.hive = "other".into();
            md.name = "other".into();
            p.rules = vec![pb::EgressRule {
                tls_passthrough: Some(pb::TlsPassthroughRule {
                    hostnames: vec!["*".into()],
                    ports: Some(pb::Ports {
                        all: None,
                        numbers: vec![443],
                    }),
                }),
                ..Default::default()
            }];
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        (updated.hive(), updated.name(), updated.version()),
        (HIVE, "default", 2)
    );
    assert_eq!(updated.uid(), created.uid());
    assert_eq!(updated.rules.len(), 1);

    let deleted = s
        .delete_egress_policy(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert!(deleted.metadata.as_ref().unwrap().delete_time.is_some());
    let mut expected = deleted.clone();
    expected.metadata_mut().delete_time = None;
    assert_eq!(expected, updated);
    assert!(s.get_egress_policy(&r).await.unwrap_err().is_not_found());
}

pub async fn egress_delete_preconditions(s: S) {
    mk_hive(&*s, HIVE).await;
    let r = ActorRef::new(HIVE, "session-1");
    s.create_actor(&suspended_actor(HIVE, "session-1"))
        .await
        .unwrap();
    let policy = pb::EgressPolicy {
        metadata: md(HIVE, "default"),
        rules: vec![],
    };
    crate::check_delete_preconditions!(
        || async { s.create_egress_policy(&r, &policy).await.unwrap() },
        |p| s.delete_egress_policy(&r, p),
        || async { s.get_egress_policy(&r).await.is_ok() }
    );
}

/// A policy needs its Actor, goes with it, and is not inherited by a
/// recreated Actor of the same name.
pub async fn egress_follows_actor(s: S) {
    mk_hive(&*s, HIVE).await;
    let r = ActorRef::new(HIVE, "session-1");
    let policy = pb::EgressPolicy {
        metadata: md(HIVE, "default"),
        rules: vec![],
    };
    assert!(matches!(
        s.create_egress_policy(&r, &policy).await,
        Err(StoreError::FailedPrecondition)
    ));
    let first = s
        .create_actor(&actor(HIVE, &r.name, pb::ActorState::Deleting))
        .await
        .unwrap();
    s.create_egress_policy(&r, &policy).await.unwrap();
    s.delete_actor(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert!(s.get_egress_policy(&r).await.unwrap_err().is_not_found());
    let replacement = s
        .create_actor(&actor(HIVE, &r.name, pb::ActorState::Running))
        .await
        .unwrap();
    assert_ne!(replacement.uid(), first.uid());
    assert!(s.get_egress_policy(&r).await.unwrap_err().is_not_found());
}

fn access_policy(member: &str) -> pb::AccessPolicy {
    pb::AccessPolicy {
        metadata: md("", "default"),
        bindings: vec![pb::Binding {
            role: "admin".into(),
            members: vec![member.into()],
        }],
    }
}

pub async fn global_access_lifecycle(s: S) {
    assert!(s
        .get_global_access_policy()
        .await
        .unwrap_err()
        .is_not_found());
    let created = s
        .create_global_access_policy(&access_policy("user:a"))
        .await
        .unwrap();
    assert_eq!(created.version(), 1);
    assert_eq!(created.name(), "default");
    assert!(!created.uid().is_empty());
    assert_eq!(created.bindings, access_policy("user:a").bindings);
    assert!(matches!(
        s.create_global_access_policy(&access_policy("user:b"))
            .await,
        Err(StoreError::AlreadyExists)
    ));
    assert_eq!(s.get_global_access_policy().await.unwrap(), created);
    assert!(matches!(
        s.update_global_access_policy(Precondition::new(created.uid(), 0), &mut |_| Ok(()))
            .await,
        Err(StoreError::PreconditionRequired(_))
    ));
    assert!(matches!(
        s.update_global_access_policy(Precondition::new(FOREIGN_UID, 1), &mut |_| Ok(()))
            .await,
        Err(StoreError::UidConflict)
    ));
    assert!(matches!(
        s.update_global_access_policy(Precondition::new(created.uid(), 99), &mut |_| Ok(()))
            .await,
        Err(StoreError::VersionConflict)
    ));
    let updated = s
        .update_global_access_policy(Precondition::of(&created), &mut |p| {
            p.bindings[0].members.push("user:b".into());
            p.metadata_mut().name = "renamed".into();
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(updated.version(), 2);
    assert_eq!(updated.name(), "default");
    assert_eq!(updated.bindings[0].members, vec!["user:a", "user:b"]);
    assert!(matches!(
        s.update_global_access_policy(Precondition::of(&created), &mut |_| Ok(()))
            .await,
        Err(StoreError::VersionConflict)
    ));
    assert_eq!(s.get_global_access_policy().await.unwrap(), updated);
}

pub async fn hive_access_lifecycle(s: S) {
    assert!(matches!(
        s.create_hive_access_policy("team-a", &access_policy("user:a"))
            .await,
        Err(StoreError::FailedPrecondition)
    ));
    mk_hive(&*s, "team-a").await;
    assert!(s
        .get_hive_access_policy("team-a")
        .await
        .unwrap_err()
        .is_not_found());
    let created = s
        .create_hive_access_policy("team-a", &access_policy("user:a"))
        .await
        .unwrap();
    assert!(matches!(
        s.create_hive_access_policy("team-a", &access_policy("user:a"))
            .await,
        Err(StoreError::AlreadyExists)
    ));
    let updated = s
        .update_hive_access_policy("team-a", Precondition::of(&created), &mut |p| {
            p.bindings.clear();
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(updated.version(), 2);
    assert!(updated.bindings.is_empty());
    assert!(matches!(
        s.delete_hive_access_policy("team-a", DeletePreconditions::version(1))
            .await,
        Err(StoreError::VersionConflict)
    ));
    assert!(matches!(
        s.delete_hive_access_policy("team-a", DeletePreconditions::uid(FOREIGN_UID))
            .await,
        Err(StoreError::UidConflict)
    ));
    let deleted = s
        .delete_hive_access_policy(
            "team-a",
            DeletePreconditions::new(updated.uid(), updated.version()),
        )
        .await
        .unwrap();
    assert!(deleted.metadata.as_ref().unwrap().delete_time.is_some());
    let mut undeleted = deleted.clone();
    undeleted.metadata_mut().delete_time = None;
    assert_eq!(undeleted, updated);
    assert!(s
        .get_hive_access_policy("team-a")
        .await
        .unwrap_err()
        .is_not_found());

    // Deleting the Hive takes its policy with it.
    s.create_hive_access_policy("team-a", &access_policy("user:a"))
        .await
        .unwrap();
    s.delete_hive("team-a", DeletePreconditions::none())
        .await
        .unwrap();
    mk_hive(&*s, "team-a").await;
    assert!(s
        .get_hive_access_policy("team-a")
        .await
        .unwrap_err()
        .is_not_found());
}
