use std::collections::BTreeSet;
use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;
use super::mark;

type S = Arc<dyn Store>;

fn names(items: &[pb::Actor]) -> BTreeSet<String> {
    items.iter().map(|a| a.name().to_string()).collect()
}

fn set_state(a: &mut pb::Actor, state: pb::ActorState) {
    a.status.get_or_insert_with(Default::default).state = state as i32;
}

#[derive(Debug, thiserror::Error)]
#[error("mutation rejected")]
struct Rejected;

pub async fn get_not_found(s: S) {
    let err = s
        .get_actor(&ActorRef::new(HIVE, "non-existent"))
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
}

pub async fn create(s: S) {
    mk_hive(&*s, HIVE).await;
    let input = actor(HIVE, "session-1", pb::ActorState::Suspended);
    let created = s.create_actor(&input).await.unwrap();
    let md = created.metadata.as_ref().unwrap();
    assert!(!md.uid.is_empty(), "no server-assigned uid");
    assert_eq!(md.version, 1);
    assert!(md.create_time.is_some() && md.update_time.is_some());
    assert!(
        input.uid().is_empty() && input.version() == 0,
        "create mutated its input"
    );
    assert_eq!(s.get_actor(&ActorRef::of(&input)).await.unwrap(), created);
    assert_eq!(without_server_fields(&created), input);
}

pub async fn create_already_exists(s: S) {
    mk_hive(&*s, HIVE).await;
    let a = actor(HIVE, "session-1", pb::ActorState::Suspended);
    s.create_actor(&a).await.unwrap();
    assert!(matches!(
        s.create_actor(&a).await,
        Err(StoreError::AlreadyExists)
    ));
}

pub async fn malformed_snapshot_uri(s: S) {
    mk_hive(&*s, HIVE).await;
    const MALFORMED: &str = "gs://private/not-a-snapshot";
    let set_uri = |a: &mut pb::Actor| {
        let snap = &mut a.status.as_mut().unwrap().snapshots[0];
        snap.durable_snapshot
            .as_mut()
            .unwrap()
            .object
            .as_mut()
            .unwrap()
            .snapshot_uri = MALFORMED.into();
    };
    let mut bad = suspended_actor(HIVE, "session-1");
    set_uri(&mut bad);
    assert!(
        s.create_actor(&bad).await.is_err(),
        "create with a malformed snapshot URI succeeded"
    );

    let created = s
        .create_actor(&suspended_actor(HIVE, "session-2"))
        .await
        .unwrap();
    let err = s
        .update_actor(
            &ActorRef::of(&created),
            Precondition::of(&created),
            &mut |a| {
                set_uri(a);
                Ok(())
            },
        )
        .await;
    assert!(err.is_err(), "update to a malformed snapshot URI succeeded");
}

pub async fn update(s: S) {
    mk_hive(&*s, HIVE).await;
    let created = s
        .create_actor(&actor(HIVE, "session-1", pb::ActorState::Suspended))
        .await
        .unwrap();
    let r = ActorRef::of(&created);
    let updated = s
        .update_actor(&r, Precondition::of(&created), &mut |a| {
            set_state(a, pb::ActorState::Running);
            // Server-owned metadata comes from the stored record, not the
            // mutation.
            let md = a.metadata_mut();
            md.uid = "client-supplied-uid".into();
            md.create_time = Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            });
            md.version = 99;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        updated.status.as_ref().unwrap().state,
        pb::ActorState::Running as i32
    );
    assert_eq!(updated.version(), 2);
    assert_eq!(updated.uid(), created.uid());
    assert_eq!(
        updated.metadata.as_ref().unwrap().create_time,
        created.metadata.as_ref().unwrap().create_time
    );
    assert_eq!(created.version(), 1);
    assert_eq!(s.get_actor(&r).await.unwrap(), updated);
}

pub async fn update_conflict(s: S) {
    mk_hive(&*s, HIVE).await;
    let a = actor(HIVE, "session-1", pb::ActorState::Suspended);
    s.create_actor(&a).await.unwrap();
    let r = ActorRef::of(&a);
    let first = s.get_actor(&r).await.unwrap();
    let second = s.get_actor(&r).await.unwrap();
    s.update_actor(&r, Precondition::of(&first), &mut |a| {
        set_state(a, pb::ActorState::Running);
        Ok(())
    })
    .await
    .unwrap();
    let err = s
        .update_actor(&r, Precondition::of(&second), &mut |a| {
            set_state(a, pb::ActorState::Suspended);
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::VersionConflict), "{err:?}");
}

pub async fn update_uid_conflict(s: S) {
    mk_hive(&*s, HIVE).await;
    let new = || actor(HIVE, "session-1", pb::ActorState::Suspended);
    let original = s.create_actor(&new()).await.unwrap();
    let r = ActorRef::of(&original);
    s.update_actor(&r, Precondition::of(&original), &mut |a| {
        set_state(a, pb::ActorState::Deleting);
        Ok(())
    })
    .await
    .unwrap();
    s.delete_actor(&r, DeletePreconditions::none())
        .await
        .unwrap();
    let recreated = s.create_actor(&new()).await.unwrap();
    assert_ne!(
        recreated.uid(),
        original.uid(),
        "recreated actor reused its uid"
    );
    let mut ran = false;
    let err = s
        .update_actor(&r, Precondition::of(&original), &mut |a| {
            ran = true;
            set_state(a, pb::ActorState::Running);
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(!ran, "mutate ran past its precondition");
    assert!(matches!(err, StoreError::UidConflict), "{err:?}");
    assert_eq!(s.get_actor(&r).await.unwrap(), recreated);
}

pub async fn update_mutate_error(s: S) {
    mk_hive(&*s, HIVE).await;
    let created = s
        .create_actor(&actor(HIVE, "session-1", pb::ActorState::Suspended))
        .await
        .unwrap();
    let r = ActorRef::of(&created);
    let err = s
        .update_actor(&r, Precondition::of(&created), &mut |a| {
            set_state(a, pb::ActorState::Running);
            Err(StoreError::rejected(Rejected))
        })
        .await
        .unwrap_err();
    assert!(err.rejection::<Rejected>().is_some(), "{err:?}");
    assert_eq!(s.get_actor(&r).await.unwrap(), created);
}

pub async fn update_missing_precondition(s: S) {
    mk_hive(&*s, HIVE).await;
    let created = s
        .create_actor(&actor(HIVE, "session-1", pb::ActorState::Suspended))
        .await
        .unwrap();
    let r = ActorRef::of(&created);
    for p in [
        Precondition::default(),
        Precondition::new(created.uid(), 0),
        Precondition::new("", created.version()),
    ] {
        let err = s
            .update_actor(&r, p, &mut |_| panic!("mutate ran for a blind write"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PreconditionRequired(_)),
            "{err:?}"
        );
    }
}

pub async fn mark_for_deletion(s: S) {
    mk_hive(&*s, HIVE).await;
    mark::check(
        &*s,
        ActorRef::new(HIVE, "session-1"),
        suspended_actor(HIVE, "session-1"),
        Some(|a: &mut pb::Actor| {
            set_state(a, pb::ActorState::Deleting);
            Ok(())
        }),
        Some(|a: &pb::Actor| a.status.as_ref().unwrap().state == pb::ActorState::Deleting as i32),
    )
    .await;
}

/// The store does not gate deletes on state; the workflow does.
pub async fn delete_in_any_state(s: S) {
    mk_hive(&*s, HIVE).await;
    for (i, state) in [
        pb::ActorState::Suspended,
        pb::ActorState::Crashed,
        pb::ActorState::Deleting,
        pb::ActorState::Running,
    ]
    .into_iter()
    .enumerate()
    {
        let r = ActorRef::new(HIVE, format!("session-{i}"));
        s.create_actor(&actor(HIVE, &r.name, state)).await.unwrap();
        let deleted = s
            .delete_actor(&r, DeletePreconditions::none())
            .await
            .unwrap();
        assert_eq!(deleted.name(), r.name);
        assert!(s.get_actor(&r).await.unwrap_err().is_not_found());
    }
}

pub async fn delete_not_found(s: S) {
    let err = s
        .delete_actor(
            &ActorRef::new(HIVE, "non-existent"),
            DeletePreconditions::none(),
        )
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
}

/// Exercises the waivable delete guards against a store holding one object
/// at a time. `create` makes a fresh incarnation, `delete` and `exists` act
/// on its key.
#[macro_export]
macro_rules! check_delete_preconditions {
    ($create:expr, $delete:expr, $exists:expr) => {{
        use hivekeep_store::{DeletePreconditions as D, Resource as _, StoreError as E};
        let r = $delete(D::new(FOREIGN_UID, 1)).await;
        assert!(
            matches!(r, Err(E::NotFound)),
            "guarded delete of a missing object: {r:?}"
        );

        let obj = $create().await;
        let (uid, version) = (obj.uid().to_string(), obj.version());
        let r = $delete(D::version(version + 1)).await;
        assert!(
            matches!(r, Err(E::VersionConflict)),
            "stale version alone: {r:?}"
        );
        let r = $delete(D::new(uid.clone(), version + 1)).await;
        assert!(
            matches!(r, Err(E::VersionConflict)),
            "matching uid, stale version: {r:?}"
        );
        let r = $delete(D::uid(FOREIGN_UID)).await;
        assert!(matches!(r, Err(E::UidConflict)), "foreign uid alone: {r:?}");
        let r = $delete(D::new(FOREIGN_UID, version)).await;
        assert!(
            matches!(r, Err(E::UidConflict)),
            "foreign uid, matching version: {r:?}"
        );
        assert!($exists().await, "a refused delete removed the object");

        $delete(D::version(version))
            .await
            .expect("matching version alone");
        let obj = $create().await;
        $delete(D::uid(obj.uid()))
            .await
            .expect("matching uid alone");
        let obj = $create().await;
        $delete(D::new(obj.uid(), obj.version()))
            .await
            .expect("matching uid and version");
        assert!(!$exists().await, "object survived its delete");
    }};
}

pub async fn delete_preconditions(s: S) {
    mk_hive(&*s, HIVE).await;
    let r = ActorRef::new(HIVE, "session-1");
    crate::check_delete_preconditions!(
        || async {
            s.create_actor(&suspended_actor(HIVE, "session-1"))
                .await
                .unwrap()
        },
        |p| s.delete_actor(&r, p),
        || async { s.get_actor(&r).await.is_ok() }
    );
}

pub async fn list_empty(s: S) {
    assert!(s
        .list_actors("", &ListOptions::new(1000, ""))
        .await
        .unwrap()
        .items
        .is_empty());
}

pub async fn list(s: S) {
    mk_hive(&*s, HIVE).await;
    s.create_actor(&suspended_actor(HIVE, "id1")).await.unwrap();
    s.create_actor(&suspended_actor(HIVE, "id2")).await.unwrap();
    let got = s
        .list_actors("", &ListOptions::new(1000, ""))
        .await
        .unwrap();
    assert_eq!(
        names(&got.items),
        BTreeSet::from(["id1".to_string(), "id2".to_string()])
    );
    assert!(!got.has_next_page());
}

pub async fn list_pagination(s: S) {
    mk_hive(&*s, HIVE).await;
    for i in 0..5 {
        s.create_actor(&actor(HIVE, &format!("name{i}"), pb::ActorState::Suspended))
            .await
            .unwrap();
    }
    let mut all = Vec::new();
    let mut token = String::new();
    loop {
        let page = s
            .list_actors("", &ListOptions::new(2, token))
            .await
            .unwrap();
        assert!(page.items.len() <= 2);
        all.extend(page.items);
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(all.len(), 5);
    assert_eq!(names(&all).len(), 5, "duplicates across pages");
}

pub async fn list_scoped_by_hive(s: S) {
    mk_hive(&*s, "team-a").await;
    mk_hive(&*s, "team-b").await;
    for (h, n) in [("team-a", "a1"), ("team-a", "a2"), ("team-b", "b1")] {
        s.create_actor(&actor(h, n, pb::ActorState::Suspended))
            .await
            .unwrap();
    }
    let set = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
    let opts = ListOptions::new(1000, "");
    assert_eq!(
        names(&s.list_actors("team-a", &opts).await.unwrap().items),
        set(&["a1", "a2"])
    );
    assert_eq!(
        names(&s.list_actors("team-b", &opts).await.unwrap().items),
        set(&["b1"])
    );
    assert_eq!(
        names(&s.list_actors("", &opts).await.unwrap().items),
        set(&["a1", "a2", "b1"])
    );
    assert!(s.get_actor(&ActorRef::new("team-a", "a1")).await.is_ok());
    assert!(s
        .get_actor(&ActorRef::new("team-b", "a1"))
        .await
        .unwrap_err()
        .is_not_found());
    assert!(s
        .get_actor(&ActorRef::new("", "a1"))
        .await
        .unwrap_err()
        .is_not_found());

    // A scoped token cannot resume a different scope.
    let page = s
        .list_actors("team-a", &ListOptions::new(1, ""))
        .await
        .unwrap();
    assert!(page.has_next_page());
    assert!(matches!(
        s.list_actors("team-b", &ListOptions::new(1, page.next_page_token.clone()))
            .await,
        Err(StoreError::InvalidPageToken(_))
    ));
    assert!(matches!(
        s.list_actors("", &ListOptions::new(1, page.next_page_token))
            .await,
        Err(StoreError::InvalidPageToken(_))
    ));
}
