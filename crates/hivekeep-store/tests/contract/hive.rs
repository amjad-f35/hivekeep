use std::collections::BTreeSet;
use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;

type S = Arc<dyn Store>;

pub async fn list_pagination(s: S) {
    for i in 0..5 {
        mk_hive(&*s, &format!("team-{i}")).await;
    }
    let mut all = Vec::new();
    let mut token = String::new();
    loop {
        let page = s.list_hives(&ListOptions::new(2, token)).await.unwrap();
        all.extend(page.items.into_iter().map(|h| h.name().to_string()));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(all, vec!["team-0", "team-1", "team-2", "team-3", "team-4"]);
}

pub async fn create(s: S) {
    let want = hive("team-a");
    let created = s.create_hive(&want).await.unwrap();
    assert!(!created.uid().is_empty());
    assert_eq!(created.version(), 1);
    assert_eq!(s.get_hive("team-a").await.unwrap(), created);
    assert_eq!(without_server_fields(&created), want);
}

pub async fn create_already_exists(s: S) {
    mk_hive(&*s, "team-a").await;
    assert!(matches!(
        s.create_hive(&hive("team-a")).await,
        Err(StoreError::AlreadyExists)
    ));
}

pub async fn get_not_found(s: S) {
    assert!(s.get_hive("nope").await.unwrap_err().is_not_found());
}

pub async fn list(s: S) {
    for n in ["team-a", "team-b", "team-c"] {
        mk_hive(&*s, n).await;
    }
    let got: BTreeSet<_> = s
        .list_hives(&ListOptions::new(1000, ""))
        .await
        .unwrap()
        .items
        .iter()
        .map(|h| h.name().to_string())
        .collect();
    assert_eq!(got, ["team-a", "team-b", "team-c"].map(String::from).into());
}

pub async fn list_empty(s: S) {
    assert!(s
        .list_hives(&ListOptions::new(1000, ""))
        .await
        .unwrap()
        .items
        .is_empty());
}

pub async fn delete_empty(s: S) {
    mk_hive(&*s, "team-a").await;
    let deleted = s
        .delete_hive("team-a", DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(deleted.name(), "team-a");
    assert!(deleted.metadata.as_ref().unwrap().delete_time.is_some());
    assert!(s.get_hive("team-a").await.unwrap_err().is_not_found());
}

pub async fn delete_not_found(s: S) {
    assert!(s
        .delete_hive("nope", DeletePreconditions::none())
        .await
        .unwrap_err()
        .is_not_found());
}

pub async fn delete_preconditions(s: S) {
    crate::check_delete_preconditions!(
        || async { s.create_hive(&hive("team-a")).await.unwrap() },
        |p| s.delete_hive("team-a", p),
        || async { s.get_hive("team-a").await.is_ok() }
    );

    // Matching guards do not override the emptiness rule.
    let h = s.create_hive(&hive("team-a")).await.unwrap();
    let r = ActorRef::new("team-a", "id1");
    s.create_actor(&suspended_actor(&r.hive, &r.name))
        .await
        .unwrap();
    assert!(matches!(
        s.delete_hive("team-a", DeletePreconditions::new(h.uid(), h.version()))
            .await,
        Err(StoreError::FailedPrecondition)
    ));
    assert!(s.get_hive("team-a").await.is_ok());
    s.delete_actor(&r, DeletePreconditions::none())
        .await
        .unwrap();
    s.delete_hive("team-a", DeletePreconditions::new(h.uid(), h.version()))
        .await
        .unwrap();
}

pub async fn delete_non_empty_rejected(s: S) {
    mk_hive(&*s, "team-a").await;
    s.create_actor(&actor("team-a", "id1", pb::ActorState::Deleting))
        .await
        .unwrap();
    assert!(matches!(
        s.delete_hive("team-a", DeletePreconditions::none()).await,
        Err(StoreError::FailedPrecondition)
    ));
    assert!(s.get_hive("team-a").await.is_ok());
}

pub async fn delete_after_actors_removed(s: S) {
    mk_hive(&*s, "team-a").await;
    s.create_actor(&actor("team-a", "id1", pb::ActorState::Deleting))
        .await
        .unwrap();
    assert!(matches!(
        s.delete_hive("team-a", DeletePreconditions::none()).await,
        Err(StoreError::FailedPrecondition)
    ));
    s.delete_actor(&ActorRef::new("team-a", "id1"), DeletePreconditions::none())
        .await
        .unwrap();
    s.delete_hive("team-a", DeletePreconditions::none())
        .await
        .unwrap();
}

pub async fn delete_while_other_non_empty(s: S) {
    mk_hive(&*s, "team-a").await;
    mk_hive(&*s, "team-b").await;
    s.create_actor(&actor("team-b", "id1", pb::ActorState::Suspended))
        .await
        .unwrap();
    s.delete_hive("team-a", DeletePreconditions::none())
        .await
        .unwrap();
    assert!(s.get_hive("team-a").await.unwrap_err().is_not_found());
    assert!(matches!(
        s.delete_hive("team-b", DeletePreconditions::none()).await,
        Err(StoreError::FailedPrecondition)
    ));
}
