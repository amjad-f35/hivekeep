use std::sync::Arc;

use hivekeep_core::resource::TagRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;
use super::mark;

type S = Arc<dyn Store>;

/// Marks a reserved Tag's pending copy COMPLETED, the one change its
/// snapshot allows.
fn finalize(t: &mut pb::Tag) -> Result<()> {
    let snap = t.status.as_mut().unwrap().snapshot.as_mut().unwrap();
    snap.durable_snapshot.as_mut().unwrap().state = pb::SnapshotStorageState::Completed as i32;
    Ok(())
}

fn snapshot_state(t: &pb::Tag) -> i32 {
    t.status
        .as_ref()
        .unwrap()
        .snapshot
        .as_ref()
        .unwrap()
        .durable_snapshot
        .as_ref()
        .unwrap()
        .state
}

fn snapshot_uri_mut(t: &mut pb::Tag) -> &mut String {
    &mut t
        .status
        .as_mut()
        .unwrap()
        .snapshot
        .as_mut()
        .unwrap()
        .durable_snapshot
        .as_mut()
        .unwrap()
        .object
        .as_mut()
        .unwrap()
        .snapshot_uri
}

async fn seed_actor(s: &S, hive: &str, name: &str) -> pb::Actor {
    mk_hive(&**s, hive).await;
    s.create_actor(&suspended_actor(hive, name)).await.unwrap()
}

/// Reserves and finalizes `tag`, for tests about a usable Tag.
async fn store_tag(s: &S, tag: &pb::Tag) -> pb::Tag {
    let reserved = s.create_tag(tag).await.unwrap();
    s.update_tag(
        &TagRef::of(&reserved),
        Precondition::of(&reserved),
        &mut finalize,
    )
    .await
    .unwrap()
}

pub async fn lifecycle(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let r = TagRef::new("team-a", "production");
    let input = in_progress_tag("production", &a);
    let pending = s.create_tag(&input).await.unwrap();
    assert_eq!(pending.version(), 1);
    assert!(!pending.uid().is_empty());
    assert!(
        input.uid().is_empty() && input.version() == 0,
        "create mutated its input"
    );

    let tag = s.get_tag(&r).await.unwrap();
    assert_eq!(
        snapshot_state(&tag),
        pb::SnapshotStorageState::Pending as i32
    );
    assert_eq!(without_server_fields(&tag), input);
    assert!(s
        .get_tag(&TagRef::new("team-a", "missing"))
        .await
        .unwrap_err()
        .is_not_found());

    let ready = s
        .update_tag(&r, Precondition::of(&tag), &mut finalize)
        .await
        .unwrap();
    let mut ready_uri = ready.clone();
    assert_eq!(
        *snapshot_uri_mut(&mut ready_uri),
        tag_snapshot_uri("gs://private", "team-a", "production")
    );
    assert_eq!(
        snapshot_state(&ready),
        pb::SnapshotStorageState::Completed as i32
    );

    let updated = s
        .update_tag(&r, Precondition::of(&ready), &mut |t| {
            t.scope = pb::TagScope::Published as i32;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(updated.scope, pb::TagScope::Published as i32);
    assert_eq!(updated.version(), ready.version() + 1);
    assert!(matches!(
        s.update_tag(&r, Precondition::of(&ready), &mut |_| Ok(()))
            .await,
        Err(StoreError::VersionConflict)
    ));
    assert!(matches!(
        s.delete_hive("team-a", DeletePreconditions::none()).await,
        Err(StoreError::FailedPrecondition)
    ));
    assert_eq!(
        s.delete_tag(&r, DeletePreconditions::none()).await.unwrap(),
        updated
    );
    assert!(s.get_tag(&r).await.unwrap_err().is_not_found());
}

pub async fn delete_preconditions(s: S) {
    let a = seed_actor(&s, HIVE, "actor-1").await;
    let r = TagRef::new(HIVE, "production");
    crate::check_delete_preconditions!(
        || async {
            s.create_tag(&in_progress_tag("production", &a))
                .await
                .unwrap()
        },
        |p| s.delete_tag(&r, p),
        || async { s.get_tag(&r).await.is_ok() }
    );
}

pub async fn immutable_fields(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let tag = store_tag(&s, &in_progress_tag("production", &a)).await;
    let r = TagRef::of(&tag);
    type Change = fn(&mut pb::Tag);
    let cases: Vec<(&str, Change)> = vec![
        ("snapshot uri", |t| {
            *snapshot_uri_mut(t) = "gs://private/elsewhere".into()
        }),
        ("snapshot fidelity", |t| {
            t.status
                .as_mut()
                .unwrap()
                .snapshot
                .as_mut()
                .unwrap()
                .fidelity = pb::SnapshotFidelity::Volumes as i32
        }),
        ("clearing the snapshot", |t| {
            t.status.as_mut().unwrap().snapshot = None
        }),
        ("actor template uid", |t| {
            t.status.as_mut().unwrap().actor_template_uid = "other-template-uid".into()
        }),
        ("name", |t| t.metadata_mut().name = "staging".into()),
    ];
    for (name, change) in cases {
        let err = s
            .update_tag(&r, Precondition::of(&tag), &mut |t| {
                change(t);
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::ImmutableField(_)),
            "{name}: {err:?}"
        );
    }
    assert_eq!(s.get_tag(&r).await.unwrap(), tag);
}

/// A Tag's snapshot is keyed on the uid the store assigns, so the Tag is
/// reserved without one and the snapshot recorded by an update; after that
/// it is fixed like any other.
pub async fn record_snapshot_after_reserve(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let r = TagRef::new("team-a", "production");
    let mut input = in_progress_tag("production", &a);
    let pending = input.status.as_mut().unwrap().snapshot.take();
    let reserved = s.create_tag(&input).await.unwrap();
    let recorded = s
        .update_tag(&r, Precondition::of(&reserved), &mut |t| {
            t.status.as_mut().unwrap().snapshot = pending.clone();
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(recorded.status.as_ref().unwrap().snapshot, pending);
    let err = s
        .update_tag(&r, Precondition::of(&recorded), &mut |t| {
            *snapshot_uri_mut(t) = "gs://private/elsewhere".into();
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::ImmutableField(_)), "{err:?}");
}

pub async fn reused_name(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let input = in_progress_tag("production", &a);
    s.create_tag(&input).await.unwrap();
    assert!(matches!(
        s.create_tag(&input).await,
        Err(StoreError::AlreadyExists)
    ));
    assert_eq!(
        s.get_tag(&TagRef::new("team-a", "production"))
            .await
            .unwrap()
            .version(),
        1
    );
}

pub async fn unknown_hive(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let mut tag = in_progress_tag("production", &a);
    tag.metadata_mut().hive = "team-missing".into();
    assert!(matches!(
        s.create_tag(&tag).await,
        Err(StoreError::FailedPrecondition)
    ));
}

pub async fn malformed_snapshot_uri(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let mut malformed = in_progress_tag("production", &a);
    *snapshot_uri_mut(&mut malformed) = "gs://private/not-a-snapshot".into();
    assert!(s.create_tag(&malformed).await.is_err());

    let mut input = in_progress_tag("staging", &a);
    input.status.as_mut().unwrap().snapshot = None;
    let reserved = s.create_tag(&input).await.unwrap();
    let bad = malformed.status.as_ref().unwrap().snapshot.clone();
    let r = s
        .update_tag(
            &TagRef::of(&reserved),
            Precondition::of(&reserved),
            &mut |t| {
                t.status.as_mut().unwrap().snapshot = bad.clone();
                Ok(())
            },
        )
        .await;
    assert!(matches!(r, Err(StoreError::InvalidArgument(_))), "{r:?}");
}

pub async fn update_missing_precondition(s: S) {
    let a = seed_actor(&s, "team-a", "actor-1").await;
    let created = store_tag(&s, &in_progress_tag("production", &a)).await;
    let r = TagRef::of(&created);
    for p in [
        Precondition::default(),
        Precondition::new(created.uid(), 0),
        Precondition::new("", created.version()),
    ] {
        let err = s
            .update_tag(&r, p, &mut |_| panic!("mutate ran for a blind write"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PreconditionRequired(_)),
            "{err:?}"
        );
    }
}

pub async fn mark_for_deletion(s: S) {
    let a = seed_actor(&s, HIVE, "actor-1").await;
    mark::check(
        &*s,
        TagRef::new(HIVE, "production"),
        in_progress_tag("production", &a),
        None,
        None,
    )
    .await;
}

pub async fn list_pagination_and_scope(s: S) {
    for hive in ["team-a", "team-b"] {
        let a = seed_actor(&s, hive, "actor-1").await;
        for i in 0..3 {
            store_tag(&s, &in_progress_tag(&format!("tag-{i}"), &a)).await;
        }
    }
    let walk = |hive: &'static str| {
        let s = s.clone();
        async move {
            let mut out = Vec::new();
            let mut token = String::new();
            loop {
                let page = s
                    .list_tags(hive, &ListOptions::new(2, token))
                    .await
                    .unwrap();
                out.extend(page.items);
                if page.next_page_token.is_empty() {
                    return out;
                }
                token = page.next_page_token;
            }
        }
    };
    assert_eq!(walk("team-a").await.len(), 3);
    assert_eq!(walk("").await.len(), 6);
}
