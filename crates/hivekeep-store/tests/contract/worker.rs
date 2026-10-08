use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use hivekeep_store::*;

use super::fixtures::*;
use super::mark;

type S = Arc<dyn Store>;

fn set_state(w: &mut pb::Worker, state: pb::WorkerState) {
    w.status.get_or_insert_with(Default::default).state = state as i32;
}

#[derive(Debug, thiserror::Error)]
#[error("nothing to do")]
struct NothingToDo;

#[derive(Debug, thiserror::Error)]
#[error("already assigned")]
struct Taken;

pub async fn get_not_found(s: S) {
    assert!(s.get_worker(WORKER).await.unwrap_err().is_not_found());
}

pub async fn create(s: S) {
    let mut watch = s.watch_workers().await.unwrap();
    let input = worker(WORKER, "pod-1");
    let created = s.create_worker(&input).await.unwrap();
    let got = s.get_worker(WORKER).await.unwrap();
    assert_eq!(got, created);
    assert!(!got.uid().is_empty());
    assert_eq!(got.version(), 1);
    assert_eq!(got.hive(), "");
    let md = got.metadata.as_ref().unwrap();
    assert!(md.create_time.is_some() && md.update_time.is_some());
    assert_eq!(without_server_fields(&got), input);
    let event = receive(&mut watch).await;
    assert_eq!(event.kind, WorkerEventType::Created);
    assert_eq!(event.worker, got);
}

pub async fn create_already_exists(s: S) {
    let w = worker(WORKER, "pod-1");
    s.create_worker(&w).await.unwrap();
    assert!(matches!(
        s.create_worker(&w).await,
        Err(StoreError::AlreadyExists)
    ));
}

pub async fn update(s: S) {
    let input = worker(WORKER, "pod-1");
    let created = s.create_worker(&input).await.unwrap();
    // Subscribed after the create so only the update is on the channel.
    let mut watch = s.watch_workers().await.unwrap();
    let updated = s
        .update_worker(WORKER, Precondition::of(&created), &mut |w| {
            set_state(w, pb::WorkerState::Draining);
            Ok(())
        })
        .await
        .unwrap();
    let got = s.get_worker(WORKER).await.unwrap();
    assert_eq!(got, updated);
    assert_eq!(got.version(), 2);
    let mut want = input.clone();
    set_state(&mut want, pb::WorkerState::Draining);
    assert_eq!(without_server_fields(&got), want);
    let event = receive(&mut watch).await;
    assert_eq!(event.kind, WorkerEventType::Updated);
    assert_eq!(event.worker, got);
}

pub async fn update_not_found(s: S) {
    // A well-formed guard, so it is the missing worker that decides.
    let err = s
        .update_worker(WORKER, Precondition::new(OTHER_WORKER, 1), &mut |_| {
            panic!("mutate ran for a worker that does not exist")
        })
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err:?}");
}

pub async fn update_missing_precondition(s: S) {
    let created = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    for p in [
        Precondition::default(),
        Precondition::new(created.uid(), 0),
        Precondition::new("", created.version()),
    ] {
        let err = s
            .update_worker(WORKER, p, &mut |_| panic!("mutate ran for a blind write"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PreconditionRequired(_)),
            "{err:?}"
        );
    }
}

pub async fn mark_for_deletion(s: S) {
    mark::check(
        &*s,
        WORKER.to_string(),
        worker(WORKER, "pod-1"),
        Some(|w: &mut pb::Worker| {
            w.status = Some(pb::WorkerStatus {
                state: pb::WorkerState::Draining as i32,
                ..Default::default()
            });
            Ok(())
        }),
        Some(|w: &pb::Worker| w.status.as_ref().unwrap().state == pb::WorkerState::Draining as i32),
    )
    .await;
}

/// A mark publishes the marked Worker like any other update.
pub async fn mark_for_deletion_event(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let mut watch = s.watch_workers().await.unwrap();
    let marked = s
        .mark_worker_for_deletion(WORKER, DeletePreconditions::none(), None)
        .await
        .unwrap();
    let event = receive(&mut watch).await;
    assert_eq!(event.kind, WorkerEventType::Updated);
    assert_eq!(event.worker, marked);
}

/// A recreated Worker gets a fresh uid, and a guard naming the old one must
/// not reach it.
pub async fn update_uid_conflict(s: S) {
    let original = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    s.delete_worker(WORKER, DeletePreconditions::none())
        .await
        .unwrap();
    let recreated = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    assert_ne!(recreated.uid(), original.uid());
    let err = s
        .update_worker(WORKER, Precondition::of(&original), &mut |_| {
            panic!("mutate ran past its precondition")
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UidConflict), "{err:?}");
    assert_eq!(s.get_worker(WORKER).await.unwrap(), recreated);
}

pub async fn update_conflict(s: S) {
    s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let observed = s.get_worker(WORKER).await.unwrap();
    s.update_worker(WORKER, Precondition::of(&observed), &mut |w| {
        set_state(w, pb::WorkerState::Draining);
        Ok(())
    })
    .await
    .unwrap();
    let err = s
        .update_worker(WORKER, Precondition::of(&observed), &mut |w| {
            set_state(w, pb::WorkerState::Active);
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::VersionConflict), "{err:?}");
}

/// A refused mutation leaves the Worker exactly as it was, at the same
/// version, so callers can report "already in the desired state".
pub async fn update_mutate_error(s: S) {
    let created = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let err = s
        .update_worker(WORKER, Precondition::of(&created), &mut |w| {
            w.labels.insert("tier".into(), "edited-anyway".into());
            Err(StoreError::rejected(NothingToDo))
        })
        .await
        .unwrap_err();
    assert!(err.rejection::<NothingToDo>().is_some(), "{err:?}");
    let got = s.get_worker(WORKER).await.unwrap();
    assert!(got.labels.is_empty(), "aborted mutation was written");
    assert_eq!(got.version(), 1);
}

/// Claimants that all observed the same free Worker must not all win.
pub async fn concurrent_assign(s: S) {
    let created = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    const CLAIMANTS: i64 = 8;
    let mut tasks = Vec::new();
    for i in 0..CLAIMANTS {
        let s = s.clone();
        let p = Precondition::of(&created);
        tasks.push(tokio::spawn(async move {
            s.update_worker(WORKER, p, &mut |w| {
                if allocated(w).is_some_and(|a| a.actors > 0) {
                    return Err(StoreError::rejected(Taken));
                }
                w.status.get_or_insert_with(Default::default).allocated = resources(1, i + 1, 0);
                Ok(())
            })
            .await
        }));
    }
    let mut winners = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => winners += 1,
            Err(StoreError::VersionConflict) => {}
            Err(e) if e.rejection::<Taken>().is_some() => {}
            Err(e) => panic!("unexpected error {e:?}"),
        }
    }
    assert_eq!(winners, 1, "want exactly one winning claimant");
    let got = s.get_worker(WORKER).await.unwrap();
    let cpu = allocated(&got).unwrap().cpu_millicores;
    assert!((1..=CLAIMANTS).contains(&cpu));
    assert_eq!(got.version(), 2, "create plus the single winning claim");
}

pub async fn delete(s: S) {
    let created = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let mut watch = s.watch_workers().await.unwrap();
    let deleted = s
        .delete_worker(WORKER, DeletePreconditions::none())
        .await
        .unwrap();
    assert_eq!(deleted, created);
    assert!(s.get_worker(WORKER).await.unwrap_err().is_not_found());
    let event = receive(&mut watch).await;
    assert_eq!(event.kind, WorkerEventType::Deleted);
    assert_eq!(event.worker.name(), WORKER);
}

/// Absence is reported, not swallowed.
pub async fn delete_not_found(s: S) {
    assert!(s
        .delete_worker(WORKER, DeletePreconditions::none())
        .await
        .unwrap_err()
        .is_not_found());
}

pub async fn delete_preconditions(s: S) {
    crate::check_delete_preconditions!(
        || async { s.create_worker(&worker(WORKER, "pod-1")).await.unwrap() },
        |p| s.delete_worker(WORKER, p),
        || async { s.get_worker(WORKER).await.is_ok() }
    );
}

pub async fn watch_closed_on_close(s: S) {
    let mut watch = s.watch_workers().await.unwrap();
    watch.close();
    let next = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .expect("recv hung after close");
    assert!(next.is_none(), "got an event after close");
}

pub async fn list(s: S) {
    s.create_worker(&worker(WORKER, "pod1")).await.unwrap();
    s.create_worker(&worker(OTHER_WORKER, "pod2"))
        .await
        .unwrap();
    let got = s.list_workers(&ListOptions::new(1000, "")).await.unwrap();
    let pods: BTreeSet<_> = got.items.iter().map(|w| w.worker_pod.clone()).collect();
    assert_eq!(
        pods,
        BTreeSet::from(["pod1".to_string(), "pod2".to_string()])
    );
}

pub async fn list_empty(s: S) {
    assert!(s
        .list_workers(&ListOptions::new(1000, ""))
        .await
        .unwrap()
        .items
        .is_empty());
}

pub async fn list_pagination(s: S) {
    for i in 0..5 {
        s.create_worker(&worker(
            &format!("bb2e6a1c-0000-4000-8000-00000000000{i}"),
            &format!("pod{i}"),
        ))
        .await
        .unwrap();
    }
    let mut pods = Vec::new();
    let mut token = String::new();
    loop {
        let page = s.list_workers(&ListOptions::new(2, token)).await.unwrap();
        pods.extend(page.items.into_iter().map(|w| w.worker_pod));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(pods.len(), 5);
    assert_eq!(
        pods.iter().collect::<BTreeSet<_>>().len(),
        5,
        "duplicates across pages"
    );
}

/// Worker epochs are status fields advanced by guarded updates: a stale
/// read cannot hand out an epoch twice.
pub async fn epochs_advance(s: S) {
    let created = s.create_worker(&worker(WORKER, "pod-1")).await.unwrap();
    let issue = |w: &mut pb::Worker| {
        let st = w.status.get_or_insert_with(Default::default);
        st.issued_epoch += 1;
        Ok(())
    };
    let first = s
        .update_worker(WORKER, Precondition::of(&created), &mut issue.clone())
        .await
        .unwrap();
    assert_eq!(first.status.as_ref().unwrap().issued_epoch, 1);
    assert!(matches!(
        s.update_worker(WORKER, Precondition::of(&created), &mut issue.clone())
            .await,
        Err(StoreError::VersionConflict)
    ));
    let registered = s
        .update_worker(WORKER, Precondition::of(&first), &mut |w| {
            let st = w.status.get_or_insert_with(Default::default);
            st.registered_epoch = st.issued_epoch;
            st.observed_epoch = st.issued_epoch;
            Ok(())
        })
        .await
        .unwrap();
    let st = registered.status.unwrap();
    assert_eq!(
        (st.issued_epoch, st.registered_epoch, st.observed_epoch),
        (1, 1, 1)
    );
}
