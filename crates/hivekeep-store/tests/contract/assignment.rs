use std::collections::BTreeMap;
use std::sync::Arc;

use hivekeep_core::compute::sum_allocated;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;

type S = Arc<dyn Store>;

#[derive(Debug, thiserror::Error)]
#[error("worker is full")]
struct Full;

async fn bind(s: &S, w: &str, a: pb::ActorAssignment) -> pb::ActorAssignment {
    s.bind_actor_to_worker(w, &a, None)
        .await
        .unwrap_or_else(|e| panic!("bind {}: {e}", a.actor_uid))
}

async fn all_assignments(s: &S, w: &str) -> Vec<pb::ActorAssignment> {
    s.list_worker_assignments(w, &ListOptions::default())
        .await
        .unwrap()
        .items
}

pub async fn bind_adds_assignment_and_allocation(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let mut watch = s.watch_workers().await.unwrap();
    let a = assignment("uid-1", 500, 1 << 20);
    let stored = bind(&s, WORKER, a.clone()).await;
    assert_eq!(stored.metadata.as_ref().unwrap().name, "uid-1");
    assert_eq!(stored.version(), 1);
    let got = s.get_worker_assignment(WORKER, "uid-1").await.unwrap();
    assert_eq!(got, stored);
    let mut bare = got.clone();
    bare.metadata = None;
    assert_eq!(bare, a);

    let w = s.get_worker(WORKER).await.unwrap();
    let want = resources(1, 500, 1 << 20);
    assert_eq!(allocated(&w), want);
    assert_eq!(w.version(), 2, "a bind advances the worker");
    let event = receive(&mut watch).await;
    assert_eq!(event.kind, WorkerEventType::Updated);
    assert_eq!(allocated(&event.worker), want);
}

/// A retried claim binds the same Actor twice; it must be counted once, at
/// its latest size.
pub async fn bind_replaces_same_actor(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 500, 1 << 20)).await;
    bind(&s, WORKER, assignment("uid-1", 250, 1 << 21)).await;
    let w = s.get_worker(WORKER).await.unwrap();
    assert_eq!(allocated(&w), resources(1, 250, 1 << 21));
    assert_eq!(all_assignments(&s, WORKER).await.len(), 1);
}

pub async fn rebind_keeps_identity(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 500, 1 << 20)).await;
    let first = s.get_worker_assignment(WORKER, "uid-1").await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 250, 1 << 21)).await;
    let again = s.get_worker_assignment(WORKER, "uid-1").await.unwrap();
    assert_eq!(again.uid(), first.uid());
    assert_eq!(
        again.metadata.as_ref().unwrap().create_time,
        first.metadata.as_ref().unwrap().create_time
    );
    assert_eq!(again.version(), first.version() + 1);
}

pub async fn refused_admission_leaves_nothing(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let err = s
        .bind_actor_to_worker(
            WORKER,
            &assignment("uid-1", 500, 0),
            Some(&|_| Err(StoreError::rejected(Full))),
        )
        .await
        .unwrap_err();
    assert!(err.rejection::<Full>().is_some(), "{err:?}");
    assert!(s
        .get_worker_assignment(WORKER, "uid-1")
        .await
        .unwrap_err()
        .is_not_found());
    let w = s.get_worker(WORKER).await.unwrap();
    assert_eq!(allocated(&w).map(|a| a.actors).unwrap_or(0), 0);
    assert_eq!(w.version(), 1);
}

/// A replacement is admitted against the Worker with the old reservation
/// taken off, so an Actor is never counted twice.
pub async fn admits_replacement_that_grew(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 500, 0)).await;
    let seen = parking_lot::Mutex::new(Vec::new());
    let admit = |w: &pb::Worker| {
        seen.lock().push(allocated(w));
        Err(StoreError::rejected(Full))
    };
    let err = s
        .bind_actor_to_worker(WORKER, &assignment("uid-1", 5000, 0), Some(&admit))
        .await
        .unwrap_err();
    assert!(err.rejection::<Full>().is_some(), "{err:?}");
    let seen = seen.into_inner();
    assert_eq!(seen.len(), 1, "admit ran {} times", seen.len());
    assert_eq!(
        seen[0].map(|a| a.actors).unwrap_or(0),
        0,
        "previous reservation still counted"
    );
    let w = s.get_worker(WORKER).await.unwrap();
    assert_eq!(
        allocated(&w),
        resources(1, 500, 0),
        "a refused replacement changed the allocation"
    );
}

/// Admission runs against the Worker as the bind will write it, so
/// concurrent binds cannot all take the last place.
pub async fn admit_sees_concurrent_binds(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let mut tasks = Vec::new();
    for i in 0..16 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            let room_for_one = |w: &pb::Worker| {
                if allocated(w).is_some_and(|a| a.actors >= 1) {
                    Err(StoreError::rejected(Full))
                } else {
                    Ok(())
                }
            };
            s.bind_actor_to_worker(
                WORKER,
                &assignment(&format!("uid-{i}"), 0, 0),
                Some(&room_for_one),
            )
            .await
            .is_ok()
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(
        allocated(&s.get_worker(WORKER).await.unwrap())
            .unwrap()
            .actors,
        1
    );
}

/// An Actor belongs to one Worker; a second Worker's claim is refused and
/// both Workers are left as they were.
pub async fn actor_already_on_another_worker(s: S) {
    for name in [WORKER, OTHER_WORKER] {
        s.create_worker(&worker(name, &format!("pod-{name}")))
            .await
            .unwrap();
    }
    bind(&s, WORKER, assignment("uid-1", 500, 1 << 20)).await;
    let other = s.get_worker(OTHER_WORKER).await.unwrap();
    let err = s
        .bind_actor_to_worker(OTHER_WORKER, &assignment("uid-1", 500, 1 << 20), None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::FailedPrecondition), "{err:?}");
    assert_eq!(s.find_worker_hosting_actor("uid-1").await.unwrap(), WORKER);
    assert_eq!(
        allocated(&s.get_worker(WORKER).await.unwrap()),
        resources(1, 500, 1 << 20)
    );
    let refused = s.get_worker(OTHER_WORKER).await.unwrap();
    assert_eq!(allocated(&refused), None);
    assert_eq!(refused.version(), other.version());
}

/// Two Workers claiming one Actor at the same moment: exactly one wins and
/// neither counts an Actor it does not hold.
pub async fn concurrent_claims_of_one_actor(s: S) {
    let names = [WORKER, OTHER_WORKER];
    for name in names {
        s.create_worker(&worker(name, &format!("pod-{name}")))
            .await
            .unwrap();
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(names.len()));
    let mut tasks = Vec::new();
    for name in names {
        let s = s.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            s.bind_actor_to_worker(name, &assignment("uid-1", 500, 1 << 20), None)
                .await
                .is_ok()
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
    for name in names {
        let w = s.get_worker(name).await.unwrap();
        let held = all_assignments(&s, name).await;
        assert!(
            same_allocation(allocated(&w), sum_allocated(&held)),
            "{name}: allocation {:?} disagrees with its {} assignments",
            allocated(&w),
            held.len()
        );
    }
}

pub async fn bind_worker_not_found(s: S) {
    let err = s
        .bind_actor_to_worker("no-such-worker", &assignment("uid-1", 0, 0), None)
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
}

pub async fn release(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 500, 1 << 20)).await;
    bind(&s, WORKER, assignment("uid-2", 250, 1 << 21)).await;
    let released = s
        .release_actor_from_worker(WORKER, "uid-1")
        .await
        .unwrap()
        .expect("nothing was released");
    // The returned Worker feeds caches, so it is the post-release state.
    let want = resources(1, 250, 1 << 21);
    assert_eq!(allocated(&released), want);
    assert!(s
        .get_worker_assignment(WORKER, "uid-1")
        .await
        .unwrap_err()
        .is_not_found());
    assert!(s.get_worker_assignment(WORKER, "uid-2").await.is_ok());
    let w = s.get_worker(WORKER).await.unwrap();
    assert_eq!(allocated(&w), want);
    assert_eq!(w, released);
}

/// Release runs on retrying paths: a second pass converges without writing.
pub async fn release_already_released(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let before = s.get_worker(WORKER).await.unwrap();
    assert!(s
        .release_actor_from_worker(WORKER, "uid-1")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        s.get_worker(WORKER).await.unwrap().version(),
        before.version()
    );
    assert!(s
        .release_actor_from_worker("no-such-worker", "uid-1")
        .await
        .unwrap_err()
        .is_not_found());
}

pub async fn list_scoped_to_one_worker(s: S) {
    for pod in ["pod-1", "pod-2"] {
        s.create_worker(&worker(&format!("worker-{pod}"), pod))
            .await
            .unwrap();
    }
    bind(&s, "worker-pod-1", assignment("uid-1", 0, 0)).await;
    bind(&s, "worker-pod-1", assignment("uid-2", 0, 0)).await;
    bind(&s, "worker-pod-2", assignment("uid-3", 0, 0)).await;
    let got: Vec<_> = all_assignments(&s, "worker-pod-1")
        .await
        .into_iter()
        .map(|a| a.actor_uid)
        .collect();
    assert_eq!(got, vec!["uid-1", "uid-2"]);
    assert!(s
        .get_worker_assignment("worker-pod-1", "uid-3")
        .await
        .unwrap_err()
        .is_not_found());
}

pub async fn list_paginates(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    for i in 0..5 {
        bind(&s, WORKER, assignment(&format!("uid-{i}"), 1, 1)).await;
    }
    let mut seen = BTreeMap::<String, usize>::new();
    let mut token = String::new();
    loop {
        let page = s
            .list_worker_assignments(WORKER, &ListOptions::new(2, token))
            .await
            .unwrap();
        assert!(page.items.len() <= 2);
        for a in &page.items {
            *seen.entry(a.actor_uid.clone()).or_default() += 1;
        }
        if !page.has_next_page() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(seen.len(), 5);
    assert!(
        seen.values().all(|&n| n == 1),
        "an assignment repeated across pages: {seen:?}"
    );
}

pub async fn list_empty(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    assert!(all_assignments(&s, WORKER).await.is_empty());
}

pub async fn find_worker_hosting_actor(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    assert!(s
        .find_worker_hosting_actor("uid-1")
        .await
        .unwrap_err()
        .is_not_found());
    bind(&s, WORKER, assignment("uid-1", 0, 0)).await;
    assert_eq!(s.find_worker_hosting_actor("uid-1").await.unwrap(), WORKER);
    s.release_actor_from_worker(WORKER, "uid-1").await.unwrap();
    assert!(s
        .find_worker_hosting_actor("uid-1")
        .await
        .unwrap_err()
        .is_not_found());
}

pub async fn delete_worker_drops_assignments(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    bind(&s, WORKER, assignment("uid-1", 0, 0)).await;
    s.delete_worker(WORKER, DeletePreconditions::none())
        .await
        .unwrap();
    assert!(all_assignments(&s, WORKER).await.is_empty());
    assert!(s
        .find_worker_hosting_actor("uid-1")
        .await
        .unwrap_err()
        .is_not_found());
}

/// Churns a Worker through binds, rebinds and releases, requiring after
/// every step that the stored total equals the assignments it summarizes,
/// and that it returns to exactly nothing.
pub async fn allocated_tracks_churn(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let mut rng = Rng::new(0x5eed);
    for step in 0..300 {
        let uid = format!("uid-{}", rng.below(12));
        if rng.below(3) < 2 {
            let a = assignment(
                &uid,
                (rng.below(4) as i64 + 1) * 500,
                (rng.below(4) as i64 + 1) << 24,
            );
            s.bind_actor_to_worker(WORKER, &a, None)
                .await
                .unwrap_or_else(|e| panic!("step {step}: bind {uid}: {e}"));
        } else {
            s.release_actor_from_worker(WORKER, &uid)
                .await
                .unwrap_or_else(|e| panic!("step {step}: release {uid}: {e}"));
        }
        let w = s.get_worker(WORKER).await.unwrap();
        let held = all_assignments(&s, WORKER).await;
        assert_eq!(
            allocated(&w),
            sum_allocated(&held),
            "after step {step} ({uid})"
        );
    }
    for a in all_assignments(&s, WORKER).await {
        s.release_actor_from_worker(WORKER, &a.actor_uid)
            .await
            .unwrap();
    }
    let w = s.get_worker(WORKER).await.unwrap();
    assert_eq!(
        allocated(&w),
        None,
        "after releasing everything the total is not zero"
    );
}
