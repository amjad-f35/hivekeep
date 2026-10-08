use std::sync::Arc;

use hivekeep_core::resource::ActorTemplateRef;
use hivekeep_store::*;

use super::fixtures::*;
use super::mark;

type S = Arc<dyn Store>;

pub async fn lifecycle(s: S) {
    mk_hive(&*s, "team-a").await;
    let input = template("team-a", "tmpl-a");
    let created = s.create_actor_template(&input).await.unwrap();
    assert!(!created.uid().is_empty());
    assert_eq!(created.version(), 1);
    assert!(
        input.uid().is_empty() && input.version() == 0,
        "create mutated its input"
    );
    let r = ActorTemplateRef::new("team-a", "tmpl-a");
    assert_eq!(s.get_actor_template(&r).await.unwrap(), created);
    assert_eq!(without_server_fields(&created), input);
    s.delete_actor_template(&r, DeletePreconditions::none())
        .await
        .unwrap();
    assert!(s.get_actor_template(&r).await.unwrap_err().is_not_found());
}

pub async fn delete_preconditions(s: S) {
    mk_hive(&*s, HIVE).await;
    let r = ActorTemplateRef::new(HIVE, "tmpl-a");
    crate::check_delete_preconditions!(
        || async {
            s.create_actor_template(&template(HIVE, "tmpl-a"))
                .await
                .unwrap()
        },
        |p| s.delete_actor_template(&r, p),
        || async { s.get_actor_template(&r).await.is_ok() }
    );
}

pub async fn update_missing_precondition(s: S) {
    mk_hive(&*s, "team-a").await;
    let created = s
        .create_actor_template(&template("team-a", "tmpl-a"))
        .await
        .unwrap();
    let r = ActorTemplateRef::new("team-a", "tmpl-a");
    for p in [
        Precondition::default(),
        Precondition::new(created.uid(), 0),
        Precondition::new("", created.version()),
    ] {
        let err = s
            .update_actor_template(&r, p, &mut |_| panic!("mutate ran for a blind write"))
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
        ActorTemplateRef::new(HIVE, "tmpl-a"),
        template(HIVE, "tmpl-a"),
        None,
        None,
    )
    .await;
}

pub async fn block_hive_deletion(s: S) {
    mk_hive(&*s, "team-a").await;
    s.create_actor_template(&template("team-a", "tmpl-a"))
        .await
        .unwrap();
    assert!(matches!(
        s.delete_hive("team-a", DeletePreconditions::none()).await,
        Err(StoreError::FailedPrecondition)
    ));
}

pub async fn rename_is_immutable(s: S) {
    mk_hive(&*s, "team-a").await;
    let created = s
        .create_actor_template(&template("team-a", "tmpl-a"))
        .await
        .unwrap();
    let r = ActorTemplateRef::new("team-a", "tmpl-a");
    let err = s
        .update_actor_template(&r, Precondition::of(&created), &mut |t| {
            t.metadata_mut().name = "tmpl-b".into();
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::ImmutableField(_)), "{err:?}");
    assert_eq!(s.get_actor_template(&r).await.unwrap(), created);
}

pub async fn list_scoped(s: S) {
    for h in ["team-a", "team-b"] {
        mk_hive(&*s, h).await;
        for n in ["t1", "t2", "t3"] {
            s.create_actor_template(&template(h, n)).await.unwrap();
        }
    }
    let mut scoped = Vec::new();
    let mut token = String::new();
    loop {
        let page = s
            .list_actor_templates("team-b", &ListOptions::new(2, token))
            .await
            .unwrap();
        let more = page.has_next_page();
        scoped.extend(
            page.items
                .into_iter()
                .map(|t| format!("{}/{}", t.hive(), t.name())),
        );
        if !more {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(scoped, vec!["team-b/t1", "team-b/t2", "team-b/t3"]);
    let all = s
        .list_actor_templates("", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(all.items.len(), 6);
}
