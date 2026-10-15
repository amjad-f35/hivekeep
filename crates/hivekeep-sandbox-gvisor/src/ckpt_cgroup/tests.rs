use super::*;
use crate::actor_path::pid_file_path;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const UID: &str = "0b5f3c1e-7a2d-4e8f-9c61-2d4a8b7e5f10";

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    leaf: PathBuf,
    dirs: ActorDirs,
    /// Replaces the move once set, to inject failures.
    failure: Arc<Mutex<Option<i32>>>,
}

impl Fixture {
    fn new(sandbox_pid: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cg");
        let actor_root = tmp.path().join("actor");
        let dirs = ActorDirs {
            root_dir: actor_root.display().to_string(),
            ..Default::default()
        };
        let leaf = root.join(gvisor_cgroup_leaf(UID, SANDBOX_ID));
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::write(leaf.join("cgroup.procs"), format!("100\n{sandbox_pid}\n")).unwrap();
        std::fs::write(leaf.join("memory.max"), "805306368\n").unwrap();
        std::fs::write(leaf.join("cpu.max"), "200000 100000\n").unwrap();
        if !sandbox_pid.is_empty() {
            let f = pid_file_path(&dirs, SANDBOX_ID);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, format!("{sandbox_pid}\n")).unwrap();
        }
        Fixture {
            _tmp: tmp,
            root,
            leaf,
            dirs,
            failure: Arc::new(Mutex::new(None)),
        }
    }

    fn ckpt_dir(&self) -> PathBuf {
        checkpoint_cgroup_dir(&self.root, UID)
    }

    /// Models cgroup v2 migration: moving out of the checkpoint cgroup
    /// empties it, so rmdir succeeds.
    fn move_pid(&self) -> MovePid {
        let ckpt = self.ckpt_dir();
        let failure = self.failure.clone();
        Arc::new(move |dir: &Path, pid: i32| {
            if let Some(errno) = *failure.lock() {
                return Err(io::Error::from_raw_os_error(errno));
            }
            write_cgroup_pid(dir, pid)?;
            if dir != ckpt {
                let _ = std::fs::remove_file(ckpt.join("cgroup.procs"));
                let _ = std::fs::remove_file(ckpt.join("cpu.max"));
            }
            Ok(())
        })
    }

    /// Empties the checkpoint cgroup, as tearing the sandbox down does.
    fn empty_ckpt(&self) {
        let _ = std::fs::remove_file(self.ckpt_dir().join("cpu.max"));
        let _ = std::fs::remove_file(self.ckpt_dir().join("cgroup.procs"));
    }
}

fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap()
}

#[test]
fn isolation_moves_only_the_sentry() {
    let f = Fixture::new("101");
    let c = isolate_checkpoint_cache(&f.root, &f.dirs, UID, f.move_pid()).unwrap();
    assert_eq!(read(&f.ckpt_dir().join("cgroup.procs")), "101");
    assert_eq!(read(&f.ckpt_dir().join("cpu.max")), "200000 100000\n");
    assert!(!f.ckpt_dir().join("memory.max").exists());
    c.restore().unwrap();
    assert_eq!(read(&f.leaf.join("cgroup.procs")), "101");
    c.remove().unwrap();
}

#[test]
fn isolation_needs_the_pid_file() {
    let f = Fixture::new("");
    assert!(isolate_checkpoint_cache(&f.root, &f.dirs, UID, f.move_pid()).is_err());
    assert!(!f.ckpt_dir().exists());
}

#[test]
fn isolation_rejects_a_pid_outside_the_leaf() {
    let f = Fixture::new("101");
    std::fs::write(f.leaf.join("cgroup.procs"), "100\n").unwrap();
    assert!(isolate_checkpoint_cache(&f.root, &f.dirs, UID, f.move_pid()).is_err());
    assert!(!f.ckpt_dir().exists());
    assert_eq!(read(&f.leaf.join("cgroup.procs")), "100\n");
}

#[test]
fn restore_after_the_leaf_is_gone() {
    let f = Fixture::new("101");
    let c = isolate_checkpoint_cache(&f.root, &f.dirs, UID, f.move_pid()).unwrap();
    std::fs::remove_dir_all(&f.leaf).unwrap();
    assert_eq!(c.restore().unwrap_err().kind(), io::ErrorKind::NotFound);
    assert_eq!(read(&c.dir.join("cgroup.procs")), "101");
}

#[test]
fn restore_skips_an_exited_sentry() {
    let f = Fixture::new("101");
    let c = isolate_checkpoint_cache(&f.root, &f.dirs, UID, f.move_pid()).unwrap();
    std::fs::write(c.dir.join("cgroup.procs"), "").unwrap();
    std::fs::write(f.leaf.join("cgroup.procs"), "100\n").unwrap();
    c.restore().unwrap();
    assert_eq!(read(&f.leaf.join("cgroup.procs")), "100\n");
    // The sentry exiting between the check and the move.
    std::fs::write(c.dir.join("cgroup.procs"), "101\n").unwrap();
    *f.failure.lock() = Some(libc::ESRCH);
    c.restore().unwrap();
}

#[tokio::test]
async fn terminates_before_removing_on_success() {
    let f = Fixture::new("101");
    let terminated = AtomicBool::new(false);
    checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async {
            assert_eq!(read(&f.ckpt_dir().join("cgroup.procs")), "101");
            Ok(())
        },
        || async {
            terminated.store(true, Ordering::SeqCst);
            f.empty_ckpt();
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(terminated.load(Ordering::SeqCst));
    assert!(!f.ckpt_dir().exists());
}

#[tokio::test]
async fn checkpoints_unisolated_without_a_pid() {
    let f = Fixture::new("");
    let terminated = AtomicBool::new(false);
    checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async {
            assert!(!f.ckpt_dir().exists());
            Ok(())
        },
        || async {
            terminated.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(terminated.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_checkpoint_keeps_the_workload() {
    let f = Fixture::new("101");
    let terminated = AtomicBool::new(false);
    let err = checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async { Err(Error::msg("checkpoint boom")) },
        || async {
            terminated.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("checkpoint boom"));
    assert!(!terminated.load(Ordering::SeqCst));
    assert_eq!(read(&f.leaf.join("cgroup.procs")), "101");
    assert!(!f.ckpt_dir().exists());
}

#[tokio::test]
async fn failed_checkpoint_and_rollback_terminates() {
    let f = Fixture::new("101");
    let terminated = AtomicBool::new(false);
    let failure = f.failure.clone();
    let err = checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async {
            *failure.lock() = Some(libc::EACCES);
            Err(Error::msg("checkpoint boom"))
        },
        || async {
            terminated.store(true, Ordering::SeqCst);
            f.empty_ckpt();
            Ok(())
        },
    )
    .await;
    assert!(err.is_err());
    assert!(terminated.load(Ordering::SeqCst));
    assert!(!f.ckpt_dir().exists());
}

#[tokio::test]
async fn failed_checkpoint_with_the_leaf_gone_terminates() {
    let f = Fixture::new("101");
    let terminated = AtomicBool::new(false);
    let err = checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async {
            std::fs::remove_dir_all(&f.leaf).unwrap();
            Err(Error::msg("checkpoint boom"))
        },
        || async {
            terminated.store(true, Ordering::SeqCst);
            f.empty_ckpt();
            Ok(())
        },
    )
    .await;
    assert!(err.is_err());
    assert!(terminated.load(Ordering::SeqCst));
    assert!(!f.ckpt_dir().exists());
}

#[tokio::test]
async fn sentry_left_running_goes_back_to_its_leaf() {
    let f = Fixture::new("101");
    checkpoint_isolated(
        &f.root,
        UID,
        &f.dirs,
        f.move_pid(),
        async { Ok(()) },
        // The checkpoint cgroup stays non-empty: the sentry survived.
        || async { Err(Error::msg("terminate boom")) },
    )
    .await
    .unwrap();
    assert_eq!(read(&f.leaf.join("cgroup.procs")), "101");
    assert!(!f.ckpt_dir().exists());
}

#[test]
fn removes_stale_checkpoint_cgroups() {
    let root = tempfile::tempdir().unwrap();
    let stale = checkpoint_cgroup_dir(root.path(), UID);
    std::fs::create_dir(&stale).unwrap();
    let busy = checkpoint_cgroup_dir(root.path(), "busy");
    std::fs::create_dir(&busy).unwrap();
    std::fs::write(busy.join("cgroup.procs"), "42").unwrap();
    let leaf = root.path().join(gvisor_cgroup_leaf(UID, SANDBOX_ID));
    std::fs::create_dir(&leaf).unwrap();
    remove_stale_checkpoint_cgroups(root.path());
    assert!(!stale.exists());
    assert!(busy.exists());
    assert!(leaf.exists());
}

fn restore_dir() -> (tempfile::TempDir, PathBuf) {
    let t = tempfile::tempdir().unwrap();
    let d = t.path().join("restore-dir");
    std::fs::create_dir_all(d.join("fs")).unwrap();
    for n in [
        "checkpoint.img",
        "pages.img",
        "fs/pages.img",
        "fs/fscheckpoint.pb",
        "manifest.json",
    ] {
        std::fs::write(d.join(n), "staged data").unwrap();
    }
    (t, d)
}

fn capture() -> (Evict, tokio::sync::mpsc::UnboundedReceiver<Vec<File>>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Arc::new(move |f| drop(tx.send(f))), rx)
}

const DATA: [&str; 3] = ["checkpoint.img", "pages.img", "fs/pages.img"];

#[tokio::test]
async fn drops_staged_restore_files_at_once() {
    let (_t, d) = restore_dir();
    let (evict, mut rx) = capture();
    let called = Arc::new(AtomicBool::new(false));
    let c = called.clone();
    let wait: WaitRestore = Box::new(move |_| {
        c.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    });
    drop_actor_checkpoint_cache_async(CancellationToken::new(), UID, &d, false, Some(wait), evict);
    assert!(!called.load(Ordering::SeqCst));
    for n in DATA {
        assert!(!d.join(n).exists(), "{n}");
    }
    for n in ["fs/fscheckpoint.pb", "manifest.json"] {
        assert!(d.join(n).exists(), "{n}");
    }
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn shared_restore_files_go_after_eviction() {
    let (t, d) = restore_dir();
    let cache = t.path().join("cache");
    std::fs::create_dir(&cache).unwrap();
    for n in ["checkpoint.img", "pages.img"] {
        std::fs::hard_link(d.join(n), cache.join(n)).unwrap();
    }
    let (evict, mut rx) = capture();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let wait: WaitRestore = Box::new(move |_| {
        Box::pin(async move {
            let _ = done_rx.await;
            Ok(())
        })
    });
    drop_actor_checkpoint_cache_async(CancellationToken::new(), UID, &d, false, Some(wait), evict);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "evicted before the restore wait completed"
    );
    for n in DATA {
        assert!(d.join(n).exists(), "{n} unlinked early");
    }
    done_tx.send(()).unwrap();
    let evicted = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evicted.len(), 3);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while DATA.iter().any(|n| d.join(n).exists()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "files survived eviction"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for n in ["checkpoint.img", "pages.img"] {
        assert!(cache.join(n).exists(), "the cache's link to {n} went too");
    }
}

#[tokio::test]
async fn preserved_dir_is_evicted_once_the_wait_completes() {
    let (_t, d) = restore_dir();
    let (evict, mut rx) = capture();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let wait: WaitRestore = Box::new(move |_| {
        Box::pin(async move {
            let _ = done_rx.await;
            Ok(())
        })
    });
    drop_actor_checkpoint_cache_async(CancellationToken::new(), UID, &d, true, Some(wait), evict);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(rx.try_recv().is_err());
    done_tx.send(()).unwrap();
    let evicted = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let mut names: Vec<String> = evicted
        .iter()
        .map(|f| f.path.strip_prefix(&d).unwrap().display().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["checkpoint.img", "fs/pages.img", "pages.img"]);
    for n in DATA {
        assert!(d.join(n).exists(), "preserved {n} was dropped");
    }
}

#[tokio::test]
async fn failed_wait_skips_eviction() {
    let (_t, d) = restore_dir();
    let (evict, mut rx) = capture();
    let wait: WaitRestore = Box::new(|_| Box::pin(async { Err(Error::msg("runsc wait boom")) }));
    drop_actor_checkpoint_cache_async(CancellationToken::new(), UID, &d, true, Some(wait), evict);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn waits_no_longer_than_the_actor_is_hosted() {
    let (_t, d) = restore_dir();
    let (evict, mut rx) = capture();
    let actor = CancellationToken::new();
    let (tx, waited) = tokio::sync::oneshot::channel();
    let wait: WaitRestore = Box::new(move |tok| {
        Box::pin(async move {
            tok.cancelled().await;
            let _ = tx.send(());
            Err(Error::Cancelled)
        })
    });
    drop_actor_checkpoint_cache_async(actor.clone(), UID, &d, true, Some(wait), evict);
    actor.cancel();
    // Either the wait saw the cancellation or the select dropped it first.
    let _ = tokio::time::timeout(Duration::from_secs(5), waited).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "evicted after the actor was unhosted"
    );
}
