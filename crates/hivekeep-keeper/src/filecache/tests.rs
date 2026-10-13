use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::atomicfile::mode_of;

fn store(dir: &Path, min_age: Duration) -> Store {
    Store::open(
        dir.join("cache"),
        Options {
            min_age,
            fetch_timeout: Duration::from_secs(30),
        },
    )
    .unwrap()
}

fn file_fetcher(body: &'static [u8], calls: Arc<AtomicUsize>) -> Fetcher {
    fetcher(move |_ctx, path: PathBuf| async move {
        calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        fs::write(&path, body).map_err(|e| tonic::Status::internal(e.to_string()))
    })
}

fn tree_fetcher(calls: Arc<AtomicUsize>) -> Fetcher {
    fetcher(move |_ctx, dir: PathBuf| async move {
        calls.fetch_add(1, Ordering::SeqCst);
        fs::create_dir_all(dir.join("bin")).unwrap();
        fs::write(dir.join("runsc"), b"runsc").unwrap();
        fs::write(dir.join("bin/helper"), b"helper").unwrap();
        Ok(())
    })
}

fn age_entry(s: &Store, key: &Key, by: Duration) {
    let dir = s.entry_dir_for(key);
    let t = std::time::SystemTime::now() - by;
    let f = fs::File::open(&dir).unwrap();
    f.set_times(fs::FileTimes::new().set_modified(t).set_accessed(t))
        .unwrap();
}

#[tokio::test]
async fn open_creates_layout() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    assert!(s.root().join("entries").is_dir());
    assert!(s.root().join("tmp").is_dir());
}

#[tokio::test]
async fn get_file_to_miss_then_hit() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["gs://b/snap/mem"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = Ctx::background();
    let a = d.path().join("a");
    s.get_file_to(&ctx, &key, &a, file_fetcher(b"bytes", calls.clone()))
        .await
        .unwrap();
    let b = d.path().join("b");
    s.get_file_to(&ctx, &key, &b, file_fetcher(b"other", calls.clone()))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fs::read(&b).unwrap(), b"bytes");
    // Published read-only.
    assert_eq!(mode_of(&fs::metadata(&a).unwrap()) & 0o222, 0);
}

#[tokio::test]
async fn concurrent_callers_share_one_fetch() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["k"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut hs = Vec::new();
    for i in 0..8 {
        let s = s.clone();
        let key = key.clone();
        let calls = calls.clone();
        let dst = d.path().join(format!("dst{i}"));
        hs.push(tokio::spawn(async move {
            s.get_file_to(&Ctx::background(), &key, &dst, file_fetcher(b"x", calls))
                .await
        }));
    }
    for h in hs {
        h.await.unwrap().unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn canceled_waiter_does_not_abort_flight() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["slow"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = Ctx::background();
    ctx.cancel();
    let err = s
        .get_file_to(
            &ctx,
            &key,
            &d.path().join("a"),
            file_fetcher(b"x", calls.clone()),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CacheError::Canceled(_)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The fetch finished for later callers.
    s.get_file_to(
        &Ctx::background(),
        &key,
        &d.path().join("b"),
        file_fetcher(b"y", calls.clone()),
    )
    .await
    .unwrap();
    assert_eq!(fs::read(d.path().join("b")).unwrap(), b"x");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fetch_error_reaches_caller_then_retries() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["flaky"]);
    let failing =
        fetcher(|_ctx, _p: PathBuf| async { Err(tonic::Status::not_found("no such object")) });
    let err = s
        .get_file_to(&Ctx::background(), &key, &d.path().join("a"), failing)
        .await
        .unwrap_err();
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::NotFound);
    let calls = Arc::new(AtomicUsize::new(0));
    s.get_file_to(
        &Ctx::background(),
        &key,
        &d.path().join("a"),
        file_fetcher(b"ok", calls),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn fetch_timeout_bounds_a_hung_fetch() {
    let d = tempfile::tempdir().unwrap();
    let s = Store::open(
        d.path().join("cache"),
        Options {
            min_age: DEFAULT_MIN_AGE,
            fetch_timeout: Duration::from_millis(50),
        },
    )
    .unwrap();
    let hung = fetcher(|_ctx, _p: PathBuf| async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    });
    let err: tonic::Status = s
        .get_file_to(
            &Ctx::background(),
            &Key::uri(&["hung"]),
            &d.path().join("a"),
            hung,
        )
        .await
        .unwrap_err()
        .into();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn rejects_existing_and_relative_dst() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["k"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let dst = d.path().join("exists");
    fs::write(&dst, b"mine").unwrap();
    let err = s
        .get_file_to(
            &Ctx::background(),
            &key,
            &dst,
            file_fetcher(b"x", calls.clone()),
        )
        .await
        .unwrap_err();
    assert!(err.is_exists());
    let err = s
        .get_file_to(
            &Ctx::background(),
            &key,
            Path::new("rel"),
            file_fetcher(b"x", calls),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CacheError::Invalid(_)));
}

#[tokio::test]
async fn fetcher_producing_no_file_fails() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let none = fetcher(|_ctx, _p: PathBuf| async { Ok(()) });
    assert!(s
        .get_file_to(
            &Ctx::background(),
            &Key::uri(&["k"]),
            &d.path().join("a"),
            none
        )
        .await
        .is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn keeps_execute_bits() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let exec = fetcher(|_ctx, p: PathBuf| async move {
        fs::write(&p, b"#!/bin/sh").unwrap();
        crate::atomicfile::chmod(&p, 0o755).unwrap();
        Ok(())
    });
    let dst = d.path().join("bin");
    s.get_file_to(&Ctx::background(), &Key::uri(&["exe"]), &dst, exec)
        .await
        .unwrap();
    assert_eq!(mode_of(&fs::metadata(&dst).unwrap()), 0o555);
}

#[tokio::test]
async fn get_dir_to_miss_then_hit() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::sha256(&"a".repeat(64)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let a = d.path().join("a");
    s.get_dir_to(&Ctx::background(), &key, &a, tree_fetcher(calls.clone()))
        .await
        .unwrap();
    let b = d.path().join("b");
    s.get_dir_to(&Ctx::background(), &key, &b, tree_fetcher(calls.clone()))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fs::read(b.join("bin/helper")).unwrap(), b"helper");
    assert!(s
        .get_dir_to(&Ctx::background(), &key, &b, tree_fetcher(calls))
        .await
        .unwrap_err()
        .is_exists());
}

#[tokio::test]
async fn empty_tree_fails_and_kind_mismatch_is_an_error() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let empty = fetcher(|_ctx, _p: PathBuf| async { Ok(()) });
    assert!(s
        .get_dir_to(
            &Ctx::background(),
            &Key::uri(&["e"]),
            &d.path().join("e"),
            empty
        )
        .await
        .is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let key = Key::uri(&["f"]);
    s.get_file_to(
        &Ctx::background(),
        &key,
        &d.path().join("f"),
        file_fetcher(b"x", calls.clone()),
    )
    .await
    .unwrap();
    assert!(s
        .get_dir_to(
            &Ctx::background(),
            &key,
            &d.path().join("g"),
            tree_fetcher(calls)
        )
        .await
        .is_err());
}

#[tokio::test]
async fn copy_is_private() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let key = Key::uri(&["copy"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let a = d.path().join("a");
    s.get_file_copy_to(
        &Ctx::background(),
        &key,
        &a,
        file_fetcher(b"orig", calls.clone()),
    )
    .await
    .unwrap();
    fs::write(&a, b"mutated").unwrap();
    let b = d.path().join("b");
    s.get_file_copy_to(&Ctx::background(), &key, &b, file_fetcher(b"x", calls))
        .await
        .unwrap();
    assert_eq!(fs::read(&b).unwrap(), b"orig");
    assert_eq!(mode_of(&fs::metadata(&b).unwrap()), 0o600);
}

#[tokio::test]
async fn sweep_removes_debris_only() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), DEFAULT_MIN_AGE);
    let calls = Arc::new(AtomicUsize::new(0));
    let key = Key::uri(&["kept"]);
    s.get_file_to(
        &Ctx::background(),
        &key,
        &d.path().join("a"),
        file_fetcher(b"x", calls),
    )
    .await
    .unwrap();
    fs::create_dir(s.root().join("tmp/partial")).unwrap();
    fs::create_dir(s.root().join(".rm-old")).unwrap();
    let st = s.sweep_debris().await.unwrap();
    assert_eq!(
        st,
        SweepStats {
            tmp_removed: 1,
            retired_removed: 1
        }
    );
    assert!(s.entry_dir_for(&key).exists());
}

#[tokio::test]
async fn evict_takes_lru_unlinked_old_entries() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), Duration::from_secs(60));
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = Ctx::background();
    let keys: Vec<Key> = (0..3).map(|i| Key::uri(&[format!("k{i}")])).collect();
    for (i, k) in keys.iter().enumerate() {
        let dst = d.path().join(format!("dst{i}"));
        s.get_file_to(&ctx, k, &dst, file_fetcher(b"0123456789", calls.clone()))
            .await
            .unwrap();
        // Drop the consumer link so the entry is evictable.
        fs::remove_file(&dst).unwrap();
    }
    // k0 oldest, k1 middle, k2 young.
    age_entry(&s, &keys[0], Duration::from_secs(600));
    age_entry(&s, &keys[1], Duration::from_secs(300));
    let stats = s.evict_unused(1, false).await.unwrap();
    assert_eq!(stats.retired, 1);
    assert_eq!(stats.skipped_young, 1);
    assert!(!s.entry_dir_for(&keys[0]).exists());
    assert!(s.entry_dir_for(&keys[1]).exists());
    assert!(s.evict_unused(0, false).await.unwrap() == EvictStats::default());
}

#[tokio::test]
async fn evict_never_takes_a_linked_entry_and_dry_run_touches_nothing() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), Duration::ZERO);
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = Ctx::background();
    let linked = Key::uri(&["linked"]);
    let free = Key::uri(&["free"]);
    s.get_file_to(
        &ctx,
        &linked,
        &d.path().join("held"),
        file_fetcher(b"x", calls.clone()),
    )
    .await
    .unwrap();
    s.get_file_to(
        &ctx,
        &free,
        &d.path().join("tmpdst"),
        file_fetcher(b"y", calls),
    )
    .await
    .unwrap();
    fs::remove_file(d.path().join("tmpdst")).unwrap();
    let dry = s.evict_unused(i64::MAX, true).await.unwrap();
    assert_eq!(dry.retired, 1);
    assert_eq!(dry.skipped_linked, 1);
    assert!(s.entry_dir_for(&free).exists());
    let real = s.evict_unused(i64::MAX, false).await.unwrap();
    assert_eq!(real.retired, 1);
    assert!(s.entry_dir_for(&linked).exists());
    assert!(!s.entry_dir_for(&free).exists());
    assert!(s.total_bytes().await.unwrap() > 0);
}

#[tokio::test]
async fn partly_linked_tree_is_not_evicted() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path(), Duration::ZERO);
    let calls = Arc::new(AtomicUsize::new(0));
    let key = Key::uri(&["tree"]);
    let dst = d.path().join("tree");
    s.get_dir_to(&Ctx::background(), &key, &dst, tree_fetcher(calls))
        .await
        .unwrap();
    fs::remove_file(dst.join("runsc")).unwrap();
    let st = s.evict_unused(i64::MAX, false).await.unwrap();
    assert_eq!(st.skipped_linked, 1);
    fs::remove_dir_all(&dst).unwrap();
    let st = s.evict_unused(i64::MAX, false).await.unwrap();
    assert_eq!(st.retired, 1);
    assert!(st.freed_bytes > 0);
}
