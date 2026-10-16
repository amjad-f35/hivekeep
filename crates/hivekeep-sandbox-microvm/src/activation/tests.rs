use super::*;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

type Counters = Vec<(&'static str, u64)>;

fn sample(source: pb::StatsSource, at: i64) -> pb::WorkloadStatsSample {
    pb::WorkloadStatsSample {
        source: source as i32,
        observed_at_unix_nano: at,
        ..Default::default()
    }
}

/// Folds one reading; a single counter named "" is the sample's own CPU.
async fn measure(a: &Activation, r: &Counters) -> u64 {
    let r = r.clone();
    a.measure(&CancellationToken::new(), || async move {
        let mut s = sample(pb::StatsSource::GuestAgent, 0);
        if r.len() == 1 && r[0].0.is_empty() {
            s.cpu_usage_usec = r[0].1;
            return Ok((Some(s), None));
        }
        let parts = r.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        Ok((Some(s), Some(parts)))
    })
    .await
    .unwrap()
    .cpu_usage_usec
}

#[tokio::test]
async fn epoch() {
    let start = SystemTime::UNIX_EPOCH + Duration::new(1700, 5);
    let a = Activation::new(start, false);
    let want = unix_nanos(start);
    assert_eq!(a.with_epoch(Default::default()).epoch_unix_nano, want);
    let s = a
        .measure(&CancellationToken::new(), || async {
            Ok((Some(sample(pb::StatsSource::Cgroup, 0)), None))
        })
        .await
        .unwrap();
    assert_eq!(s.epoch_unix_nano, want);
}

#[tokio::test]
async fn cpu_accumulates_per_counter() {
    let max = u64::MAX;
    let cases: Vec<(&str, bool, Vec<Counters>, Vec<u64>)> = vec![
        (
            "counter from zero counts in full",
            false,
            vec![vec![("", 400)], vec![("", 900)]],
            vec![400, 900],
        ),
        (
            "resumed counter counts from the first reading",
            true,
            vec![vec![("", 5000)], vec![("", 5400)], vec![("", 6000)]],
            vec![0, 400, 1000],
        ),
        (
            "a failed read neither lowers nor re-adds",
            true,
            vec![
                vec![("a", 600), ("b", 400)],
                vec![("a", 605)],
                vec![("a", 606), ("b", 404)],
            ],
            vec![0, 5, 10],
        ),
        (
            "an exited container stops contributing",
            false,
            vec![
                vec![("a", 600), ("b", 400)],
                vec![("a", 700)],
                vec![("a", 750)],
            ],
            vec![1000, 1100, 1150],
        ),
        (
            "a restarted container counts its new value",
            false,
            vec![vec![("a", 600)], vec![("a", 50)], vec![("a", 80)]],
            vec![600, 650, 680],
        ),
        (
            "a counter first seen later in a resumed guest only sets its baseline",
            true,
            vec![
                vec![("a", 600)],
                vec![("a", 610), ("b", 9000)],
                vec![("a", 620), ("b", 9010)],
            ],
            vec![0, 10, 30],
        ),
        (
            "a corrupt maximum then a restart stays at the maximum",
            false,
            vec![vec![("a", max)], vec![("a", 5)]],
            vec![max, max],
        ),
        (
            "a counter first seen later in a cold activation counts in full",
            false,
            vec![vec![("a", 600)], vec![("a", 610), ("b", 90)]],
            vec![600, 700],
        ),
    ];
    for (name, resumes, readings, want) in cases {
        let a = Activation::new(SystemTime::now(), resumes);
        for (i, r) in readings.iter().enumerate() {
            assert_eq!(measure(&a, r).await, want[i], "{name}: reading {i}");
        }
    }
}

#[tokio::test]
async fn a_failed_reading_is_returned_and_changes_nothing() {
    let a = Activation::new(SystemTime::now(), true);
    let err = a
        .measure(&CancellationToken::new(), || async {
            Err(Error::msg("guest gone"))
        })
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "guest gone");
    assert_eq!(
        measure(&a, &vec![("", 700)]).await,
        0,
        "the first good reading is the baseline"
    );
}

#[tokio::test]
async fn store_keeps_the_newest() {
    let a = Activation::new(SystemTime::now(), false);
    assert!(a.latest().is_none());
    let newer = sample(pb::StatsSource::Cgroup, 20);
    let older = sample(pb::StatsSource::Cgroup, 10);
    a.store(newer.clone());
    a.store(older);
    assert_eq!(a.latest(), Some(newer.clone()));
    let pending = sample(pb::StatsSource::Unspecified, 30);
    a.store(pending.clone());
    assert_eq!(a.latest(), Some(pending));
    a.finish(|m| assert_eq!(m, Some(&newer)));
}

#[test]
fn no_initial_record_after_the_final_one() {
    let a = Activation::new(SystemTime::now(), false);
    a.finish(|m| assert!(m.is_none()));
    let mut wrote = false;
    a.initial(Some(sample(pb::StatsSource::Cgroup, 1)), |_| wrote = true);
    assert!(!wrote && a.latest().is_none());
}

#[tokio::test]
async fn readings_never_overlap() {
    let a = Arc::new(Activation::new(SystemTime::now(), false));
    let in_read = Arc::new(AtomicI32::new(0));
    let raw = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let (a, in_read, raw) = (a.clone(), in_read.clone(), raw.clone());
        tasks.push(tokio::spawn(async move {
            a.measure(&CancellationToken::new(), || async {
                assert_eq!(
                    in_read.fetch_add(1, Ordering::SeqCst),
                    0,
                    "readings overlapped"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
                in_read.fetch_sub(1, Ordering::SeqCst);
                let mut s = sample(pb::StatsSource::Cgroup, 0);
                s.cpu_usage_usec = raw.fetch_add(10, Ordering::SeqCst) + 10;
                Ok((Some(s), None))
            })
            .await
            .unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let last = raw.load(Ordering::SeqCst);
    assert_eq!(measure(&a, &vec![("", last)]).await, 160);
}

#[tokio::test]
async fn a_waiting_reading_gives_up_with_its_context() {
    let a = Arc::new(Activation::new(SystemTime::now(), false));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = a.clone();
    tokio::spawn(async move {
        holder
            .measure(&CancellationToken::new(), || async move {
                let _ = held_tx.send(());
                let _ = release_rx.await;
                Ok((Some(Default::default()), None))
            })
            .await
    });
    held_rx.await.unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = a
        .measure(&cancel, || async {
            panic!("read ran while another held the lock");
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Cancelled), "{err}");
    drop(release_tx);
}

#[test]
fn periodic_records_only_between_initial_and_final() {
    let a = Activation::new(SystemTime::now(), false);
    let mut wrote = Vec::new();
    assert!(!a.sampling());
    a.periodic(sample(pb::StatsSource::Cgroup, 1), |_| {
        wrote.push("periodic-before")
    });
    a.initial(Some(sample(pb::StatsSource::Cgroup, 2)), |_| {
        wrote.push("initial")
    });
    assert!(a.sampling());
    a.periodic(sample(pb::StatsSource::Cgroup, 3), |_| {
        wrote.push("periodic")
    });
    a.initial(Some(sample(pb::StatsSource::Cgroup, 4)), |_| {
        wrote.push("initial-again")
    });
    a.finish(|_| wrote.push("final"));
    assert!(!a.sampling());
    a.periodic(sample(pb::StatsSource::Cgroup, 5), |_| {
        wrote.push("periodic-after")
    });
    assert_eq!(wrote, ["initial", "periodic", "final"]);
}

#[test]
fn a_failed_initial_reading_opens_the_window() {
    let a = Activation::new(SystemTime::now(), true);
    a.initial(None, |_| {
        panic!("wrote an initial record for a failed reading")
    });
    assert!(a.sampling());
    let mut wrote = false;
    a.periodic(sample(pb::StatsSource::Unspecified, 1), |_| wrote = true);
    assert!(wrote);
}

#[test]
fn final_is_written_once() {
    let a = Activation::new(SystemTime::now(), false);
    let mut writes = 0;
    a.finish(|_| writes += 1);
    a.finish(|_| writes += 1);
    assert_eq!(writes, 1);
}

#[test]
fn a_newer_pending_sample_does_not_hide_an_older_measured_one() {
    let a = Activation::new(SystemTime::now(), false);
    let older = sample(pb::StatsSource::Cgroup, 10);
    a.store(sample(pb::StatsSource::Cgroup, 5));
    let pending = sample(pb::StatsSource::Unspecified, 20);
    a.store(pending.clone());
    a.store(older.clone());
    assert_eq!(a.latest(), Some(pending));
    a.finish(|m| assert_eq!(m, Some(&older)));
}

#[tokio::test]
async fn a_reading_without_a_sample_fails() {
    let a = Activation::new(SystemTime::now(), true);
    let err = a
        .measure(&CancellationToken::new(), || async { Ok((None, None)) })
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), NO_SAMPLE);
    assert_eq!(measure(&a, &vec![("", 700)]).await, 0);
}
