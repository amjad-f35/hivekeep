//! The mark-for-deletion contract, shared by every resource that has one.

use async_trait::async_trait;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{
    DeletePreconditions, Mutate, Precondition, Resource, Result, Store, StoreError,
};

#[derive(Debug, thiserror::Error)]
#[error("refused")]
struct Refused;

/// One resource's store methods, for [`check`].
#[async_trait]
pub trait Markable: Resource + std::fmt::Debug {
    type Ref: Send + Sync;
    async fn create(s: &dyn Store, v: &Self) -> Result<Self>;
    async fn get(s: &dyn Store, r: &Self::Ref) -> Result<Self>;
    async fn update(
        s: &dyn Store,
        r: &Self::Ref,
        p: Precondition,
        m: Mutate<'_, Self>,
    ) -> Result<Self>;
    async fn mark(
        s: &dyn Store,
        r: &Self::Ref,
        p: DeletePreconditions,
        m: Option<Mutate<'_, Self>>,
    ) -> Result<Self>;
}

macro_rules! markable {
    ($t:ty, $r:ty, $create:ident, $get:ident, $update:ident, $mark:ident) => {
        #[async_trait]
        impl Markable for $t {
            type Ref = $r;
            async fn create(s: &dyn Store, v: &Self) -> Result<Self> {
                s.$create(v).await
            }
            async fn get(s: &dyn Store, r: &$r) -> Result<Self> {
                s.$get(r).await
            }
            async fn update(
                s: &dyn Store,
                r: &$r,
                p: Precondition,
                m: Mutate<'_, Self>,
            ) -> Result<Self> {
                s.$update(r, p, m).await
            }
            async fn mark(
                s: &dyn Store,
                r: &$r,
                p: DeletePreconditions,
                m: Option<Mutate<'_, Self>>,
            ) -> Result<Self> {
                s.$mark(r, p, m).await
            }
        }
    };
}

markable!(
    pb::Actor,
    ActorRef,
    create_actor,
    get_actor,
    update_actor,
    mark_actor_for_deletion
);
markable!(
    pb::ActorTemplate,
    ActorTemplateRef,
    create_actor_template,
    get_actor_template,
    update_actor_template,
    mark_actor_template_for_deletion
);
markable!(
    pb::Tag,
    TagRef,
    create_tag,
    get_tag,
    update_tag,
    mark_tag_for_deletion
);

#[async_trait]
impl Markable for pb::Worker {
    type Ref = String;
    async fn create(s: &dyn Store, v: &Self) -> Result<Self> {
        s.create_worker(v).await
    }
    async fn get(s: &dyn Store, r: &String) -> Result<Self> {
        s.get_worker(r).await
    }
    async fn update(
        s: &dyn Store,
        r: &String,
        p: Precondition,
        m: Mutate<'_, Self>,
    ) -> Result<Self> {
        s.update_worker(r, p, m).await
    }
    async fn mark(
        s: &dyn Store,
        r: &String,
        p: DeletePreconditions,
        m: Option<Mutate<'_, Self>>,
    ) -> Result<Self> {
        s.mark_worker_for_deletion(r, p, m).await
    }
}

/// Only a mark sets delete_time, and no update clears it. A mark checks its
/// guards, writes nothing when mutate refuses, and keeps the first stamp when
/// repeated. `mutate`, if given, must land in the first mark's write, as
/// `mutated` checks.
pub async fn check<T: Markable>(
    s: &dyn Store,
    r: T::Ref,
    mut input: T,
    mutate: Option<fn(&mut T) -> Result<()>>,
    mutated: Option<fn(&T) -> bool>,
) {
    input.metadata_mut().delete_time = Some(hivekeep_store::resource::now());
    let created = T::create(s, &input).await.expect("create");
    assert!(
        created.metadata().unwrap().delete_time.is_none(),
        "a new resource carries delete_time"
    );

    // A plain update cannot set it.
    let smuggled = T::update(s, &r, Precondition::of(&created), &mut |t: &mut T| {
        t.metadata_mut().delete_time = Some(hivekeep_store::resource::now());
        Ok(())
    })
    .await
    .expect("update");
    assert!(
        smuggled.metadata().unwrap().delete_time.is_none(),
        "a plain update set delete_time"
    );

    assert!(matches!(
        T::mark(s, &r, DeletePreconditions::uid("other-uid"), None).await,
        Err(StoreError::UidConflict)
    ));
    assert!(matches!(
        T::mark(s, &r, DeletePreconditions::version(created.version()), None).await,
        Err(StoreError::VersionConflict)
    ));

    // A refusing mutate writes nothing, so nothing is stamped.
    let err = T::mark(
        s,
        &r,
        DeletePreconditions::none(),
        Some(&mut |_: &mut T| Err(StoreError::rejected(Refused))),
    )
    .await
    .unwrap_err();
    assert!(
        err.rejection::<Refused>().is_some(),
        "refusal came back as {err:?}"
    );
    let unmarked = T::get(s, &r).await.expect("get");
    assert!(unmarked.metadata().unwrap().delete_time.is_none());
    assert_eq!(
        unmarked.version(),
        smuggled.version(),
        "a refused mark wrote"
    );

    // Unguarded, so the version having moved since `created` is fine.
    let marked = match mutate {
        Some(f) => {
            let mut f = f;
            T::mark(s, &r, DeletePreconditions::none(), Some(&mut f)).await
        }
        None => T::mark(s, &r, DeletePreconditions::none(), None).await,
    }
    .expect("mark");
    if let Some(mutated) = mutated {
        assert!(mutated(&marked), "mark lost mutate's change: {marked:?}");
    }
    let m = marked.metadata().unwrap();
    assert!(m.delete_time.is_some(), "mark left delete_time unset");
    assert_eq!(
        m.delete_time, m.update_time,
        "delete_time is not the write's update_time"
    );
    assert_eq!(marked.version(), smuggled.version() + 1);
    assert_eq!(T::get(s, &r).await.unwrap(), marked);

    // A plain update cannot clear it.
    let cleared = T::update(s, &r, Precondition::of(&marked), &mut |t: &mut T| {
        t.metadata_mut().delete_time = None;
        Ok(())
    })
    .await
    .expect("update");
    assert_eq!(cleared.metadata().unwrap().delete_time, m.delete_time);

    // The guards still bite after the mark.
    assert!(matches!(
        T::mark(
            s,
            &r,
            DeletePreconditions::version(smuggled.version()),
            None
        )
        .await,
        Err(StoreError::VersionConflict)
    ));

    // A second mark writes but keeps the first stamp.
    let again = T::mark(s, &r, DeletePreconditions::uid(cleared.uid()), None)
        .await
        .expect("second mark");
    assert_eq!(again.metadata().unwrap().delete_time, m.delete_time);
    assert_eq!(again.version(), cleared.version() + 1);
}
