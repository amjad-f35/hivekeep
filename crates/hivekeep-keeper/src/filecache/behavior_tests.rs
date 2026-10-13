//! Behavioural tests of the store: layout, keys, read-through gets of files,
//! trees and copies, eviction and its retire gate, and a get/evict storm.

use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;

use super::evict::{size_entry, Candidate};
use super::*;
use crate::atomicfile::{chmod, mode_of};
use crate::sparsefile::allocated_bytes;

const EVICT_ALL: i64 = 1 << 40;
const TEST_DIGEST: &str = "af2a7458c2c05df1a01d0b2f335f4849a2de84e83160fefdc31a6266015642d4";

// ---- helpers -------------------------------------------------------------

/// A store under a fresh temp dir, which lives as long as the value.
struct TestStore {
    _dir: tempfile::TempDir,
    s: Store,
}

impl std::ops::Deref for TestStore {
    type Target = Store;
    fn deref(&self) -> &Store {
        &self.s
    }
}

fn new_store_with(min_age: Duration, fetch_timeout: Duration) -> TestStore {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(
        dir.path().join("cache"),
        Options {
            min_age,
            fetch_timeout,
        },
    )
    .unwrap();
    TestStore { _dir: dir, s }
}

fn new_store() -> TestStore {
    new_store_with(DEFAULT_MIN_AGE, DEFAULT_FETCH_TIMEOUT)
}

fn new_store_min_age(min_age: Duration) -> TestStore {
    new_store_with(min_age, DEFAULT_FETCH_TIMEOUT)
}

fn bg() -> Ctx {
    Ctx::background()
}

fn status(e: std::io::Error) -> tonic::Status {
    tonic::Status::internal(e.to_string())
}

/// Creates `path` with exactly `mode` (no umask) and `data`.
fn write_new(path: &Path, data: &[u8], mode: u32) -> std::io::Result<()> {
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)?;
    f.write_all(data)?;
    chmod(path, mode)
}

/// A fresh destination path, beside the store on its filesystem.
fn dst_path(s: &Store, name: &str) -> PathBuf {
    let dir = s.root().parent().unwrap().join("consumer");
    fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn entry_dir(s: &Store, k: &Key) -> PathBuf {
    s.inner.entry_dir(k)
}

fn data_path(s: &Store, k: &Key) -> PathBuf {
    s.inner.data_path(k)
}

fn tmp_children(s: &Store) -> usize {
    fs::read_dir(s.inner.tmp_dir()).unwrap().count()
}

fn entry_exists(s: &Store, k: &Key) -> bool {
    match fs::metadata(entry_dir(s, k)) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => panic!("stat entry: {e}"),
    }
}

fn set_mtime(path: &Path, t: SystemTime) {
    let f = fs::File::open(path).unwrap();
    f.set_times(fs::FileTimes::new().set_modified(t).set_accessed(t))
        .unwrap();
}

fn ago(d: Duration) -> SystemTime {
    SystemTime::now() - d
}

fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

fn is_lower_hex_64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A file fetcher writing fixed content, counting its runs.
#[derive(Clone)]
struct Counting {
    content: Arc<Vec<u8>>,
    calls: Arc<AtomicUsize>,
}

impl Counting {
    fn new(content: &str) -> Self {
        Counting {
            content: Arc::new(content.as_bytes().to_vec()),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn fetcher(&self) -> Fetcher {
        let c = self.clone();
        fetcher(move |_ctx, p: PathBuf| async move {
            c.calls.fetch_add(1, Ordering::SeqCst);
            write_new(&p, &c.content, 0o600).map_err(status)
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// A fetcher producing a mostly-hole file of `size` logical bytes with one
/// 4 KiB data extent at `data_at`.
fn sparse_fetcher(size: u64, data_at: u64) -> Fetcher {
    fetcher(move |_ctx, p: PathBuf| async move {
        let f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&p)
            .map_err(status)?;
        f.set_len(size).map_err(status)?;
        use std::os::unix::fs::FileExt;
        f.write_all_at(&[0u8; 4 << 10], data_at).map_err(status)?;
        Ok(())
    })
}

/// Plants a published entry with the given data files (relative to the
/// entry dir), bypassing retrieval.
fn publish_test_entry(s: &Store, name: &str, files: &[(&str, &str)]) -> Key {
    let k = Key::uri(&[format!("test://{name}")]);
    let dir = entry_dir(s, &k);
    if files.is_empty() {
        fs::create_dir_all(&dir).unwrap();
    }
    for (rel, content) in files {
        let path = dir.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, content).unwrap();
        chmod(&path, 0o444).unwrap();
    }
    write_entry_meta(&dir, &k).unwrap();
    k
}

/// One regular file a tree fetcher writes.
type TreeFile = (&'static str, &'static str, u32);

/// The shape of an extracted sandbox release: an executable next to a
/// directory of helpers.
const RELEASE_TREE: &[TreeFile] = &[
    ("runsc", "runsc bytes", 0o755),
    ("gvisor-bin/containerd", "shim bytes", 0o755),
    ("gvisor-bin/LICENSE", "license", 0o644),
    ("gvisor-bin/docs/README", "readme", 0o600),
];

#[derive(Clone)]
struct TreeFetch {
    files: &'static [TreeFile],
    calls: Arc<AtomicUsize>,
}

impl TreeFetch {
    fn new(files: &'static [TreeFile]) -> Self {
        TreeFetch {
            files,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn fetcher(&self) -> Fetcher {
        let t = self.clone();
        fetcher(move |_ctx, dir: PathBuf| async move {
            t.calls.fetch_add(1, Ordering::SeqCst);
            for (rel, content, mode) in t.files {
                let path = dir.join(rel);
                fs::create_dir_all(path.parent().unwrap()).map_err(status)?;
                write_new(&path, content.as_bytes(), *mode).map_err(status)?;
            }
            Ok(())
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// Asserts `dst` mirrors `files`: same content, each file a hard link to
/// the cache copy, read-only with its execute bits kept.
fn check_tree(s: &Store, key: &Key, dst: &Path, files: &[TreeFile]) {
    for (rel, content, mode) in files {
        let got = fs::read(dst.join(rel)).unwrap();
        assert_eq!(got, content.as_bytes(), "{rel}: content");
        let fi = fs::metadata(dst.join(rel)).unwrap();
        assert_eq!(mode_of(&fi), 0o444 | (mode & 0o111), "{rel}: mode");
        let cfi = fs::metadata(data_path(s, key).join(rel)).unwrap();
        assert!(same_file(&fi, &cfi), "{rel}: dst and cache copy differ");
    }
}

/// Materializes a file entry, backdates its last-use clock by `age`, and
/// drops the consumer link unless `keep_link`.
async fn cache_entry(s: &Store, name: &str, content: &str, age: Duration, keep_link: bool) -> Key {
    let key = Key::uri(&[format!("test://{name}")]);
    let dst = dst_path(s, &format!("evict-{name}"));
    s.get_file_to(&bg(), &key, &dst, Counting::new(content).fetcher())
        .await
        .unwrap();
    if !keep_link {
        fs::remove_file(&dst).unwrap();
    }
    set_mtime(&entry_dir(s, &key), ago(age));
    key
}

/// Materializes a tree entry, backdates it, and keeps only `keep` of the
/// consumer's files linked (`None`: the whole consumer dir goes).
async fn cache_tree(s: &Store, name: &str, age: Duration, keep: Option<&[&str]>) -> (Key, PathBuf) {
    let key = Key::uri(&[format!("test://tree/{name}")]);
    let dst = dst_path(s, &format!("evict-tree-{name}"));
    s.get_dir_to(&bg(), &key, &dst, TreeFetch::new(RELEASE_TREE).fetcher())
        .await
        .unwrap();
    match keep {
        None => fs::remove_dir_all(&dst).unwrap(),
        Some(keep) => {
            for (rel, _, _) in RELEASE_TREE {
                if !keep.contains(rel) {
                    fs::remove_file(dst.join(rel)).unwrap();
                }
            }
        }
    }
    set_mtime(&entry_dir(s, &key), ago(age));
    (key, dst)
}

fn no_retired_leftovers(s: &Store) {
    for child in fs::read_dir(s.root()).unwrap() {
        let name = child.unwrap().file_name().to_string_lossy().into_owned();
        assert!(
            !name.starts_with(RM_PREFIX),
            "retired dir {name} left behind"
        );
    }
}

fn candidate(s: &Store, key: &Key) -> Candidate {
    let fi = fs::metadata(entry_dir(s, key)).unwrap();
    Candidate {
        dir: key.dir().to_string(),
        mtime: fi.modified().unwrap(),
        size: 1,
        linked: false,
    }
}

// ---- layout, options, meta, sweep, sizing ---------------------------------

#[tokio::test]
async fn new_creates_layout_and_reopen_keeps_entries() {
    let s = new_store();
    for dir in [s.inner.entries_dir(), s.inner.tmp_dir()] {
        assert!(dir.is_dir(), "{dir:?}");
    }
    let k = publish_test_entry(&s, "survivor", &[(DATA_NAME, "x")]);
    Store::open(s.root(), Options::default()).unwrap();
    assert!(data_path(&s, &k).exists(), "entry lost across reopen");
}

#[tokio::test]
async fn new_defaults_and_options() {
    let s = new_store_with(Options::default().min_age, Options::default().fetch_timeout);
    assert_eq!(s.min_age(), DEFAULT_MIN_AGE);
    assert_eq!(s.fetch_timeout(), DEFAULT_FETCH_TIMEOUT);
    let s = new_store_with(Duration::from_secs(1), Duration::from_secs(60));
    assert_eq!(s.min_age(), Duration::from_secs(1));
    assert_eq!(s.fetch_timeout(), Duration::from_secs(60));
}

#[tokio::test]
async fn entry_meta_round_trip() {
    let s = new_store();
    let k = publish_test_entry(&s, "meta", &[(DATA_NAME, "x")]);
    let raw = fs::read(entry_dir(&s, &k).join(META_NAME)).unwrap();
    let m: EntryMeta = serde_json::from_slice(&raw).unwrap();
    assert_eq!(m.key, k.to_string());
    assert!(m.created_at.timestamp() > 0, "meta createdAt is zero");
}

#[tokio::test]
async fn sweep_debris_removes_tmp_and_retired_only() {
    let s = new_store();
    let kept = publish_test_entry(&s, "kept", &[(DATA_NAME, "x")]);
    fs::write(s.inner.tmp_dir().join("dl-1"), "partial").unwrap();
    fs::create_dir_all(s.inner.tmp_dir().join("dl-2/nested")).unwrap();
    fs::create_dir_all(s.root().join(format!("{RM_PREFIX}deadbeef/nested"))).unwrap();

    let stats = s.sweep_debris().await.unwrap();
    assert_eq!(
        stats,
        SweepStats {
            tmp_removed: 2,
            retired_removed: 1
        }
    );
    assert_eq!(tmp_children(&s), 0);
    assert!(!s.root().join(format!("{RM_PREFIX}deadbeef")).exists());
    assert!(
        data_path(&s, &kept).exists(),
        "published entry removed by sweep"
    );

    assert_eq!(s.sweep_debris().await.unwrap(), SweepStats::default());
}

#[tokio::test]
async fn total_bytes_sums_allocated_bytes_of_published_files() {
    let s = new_store();
    assert_eq!(s.total_bytes().await.unwrap(), 0);

    let file = publish_test_entry(&s, "file", &[(DATA_NAME, "12345")]);
    let tree = publish_test_entry(
        &s,
        "tree",
        &[
            ("data/runsc", "1234567890"),
            ("data/gvisor-bin/help", "123"),
        ],
    );
    fs::write(s.inner.tmp_dir().join("dl-1"), "zzzz").unwrap();

    let total = s.total_bytes().await.unwrap();
    let fd = entry_dir(&s, &file);
    let td = entry_dir(&s, &tree);
    let want: i64 = [
        fd.join(DATA_NAME),
        fd.join(META_NAME),
        td.join("data/runsc"),
        td.join("data/gvisor-bin/help"),
        td.join(META_NAME),
    ]
    .iter()
    .map(|p| allocated_bytes(&fs::metadata(p).unwrap()))
    .sum();
    assert_eq!(total, want);
}

#[tokio::test]
async fn total_bytes_counts_allocated_not_logical() {
    let s = new_store();
    const LOGICAL: u64 = 64 << 20;
    let key = Key::uri(&["test://sparse"]);
    s.get_file_to(
        &bg(),
        &key,
        &dst_path(&s, "sparse"),
        sparse_fetcher(LOGICAL, LOGICAL / 2),
    )
    .await
    .unwrap();
    let alloc = allocated_bytes(&fs::metadata(data_path(&s, &key)).unwrap());
    if alloc >= (LOGICAL / 2) as i64 {
        eprintln!("skipping: cached file is not sparse on this filesystem");
        return;
    }
    let total = s.total_bytes().await.unwrap();
    assert!(
        total < (LOGICAL / 2) as i64,
        "total {total}: logical sizes counted"
    );
}

// ---- keys ----------------------------------------------------------------

#[test]
fn sha256_key() {
    let k = Key::sha256(TEST_DIGEST).unwrap();
    assert_eq!(k.to_string(), format!("sha256:{TEST_DIGEST}"));
    assert!(is_lower_hex_64(k.dir()), "{}", k.dir());
    assert_eq!(Key::sha256(&TEST_DIGEST.to_uppercase()).unwrap(), k);
}

#[test]
fn sha256_key_rejects_bad_digests() {
    for d in [
        String::new(),
        "abc123".to_string(),
        format!("{TEST_DIGEST}00"),
        format!("{}g", &TEST_DIGEST[..63]),
    ] {
        assert!(Key::sha256(&d).is_err(), "{d:?} accepted");
    }
}

#[test]
fn uri_key() {
    let base = Key::uri(&["gs://bucket/golden-v3", "mem.img"]);
    assert_eq!(base, Key::uri(&["gs://bucket/golden-v3", "mem.img"]));
    assert!(is_lower_hex_64(base.dir()));
    let distinct = [
        base.clone(),
        Key::uri(&["gs://bucket/golden-v3/mem.img"]),
        Key::uri(&["gs://bucket/golden-v3", "mem.img", ""]),
        Key::uri(&["gs://bucket", "golden-v3/mem.img"]),
    ];
    for (i, a) in distinct.iter().enumerate() {
        for (j, b) in distinct.iter().enumerate() {
            assert!(i == j || a != b, "keys {i} and {j} collide");
        }
    }
}

#[test]
fn key_spaces_are_disjoint() {
    assert_ne!(Key::sha256(TEST_DIGEST).unwrap(), Key::uri(&[TEST_DIGEST]));
}

// ---- get_file_to ---------------------------------------------------------

#[tokio::test]
async fn get_file_to_miss_then_hit_shares_one_inode() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let f = Counting::new("golden bytes");
    let d1 = dst_path(&s, "d1");
    s.get_file_to(&bg(), &key, &d1, f.fetcher()).await.unwrap();
    assert_eq!(fs::read(&d1).unwrap(), b"golden bytes");
    let fi = fs::metadata(&d1).unwrap();
    assert_eq!(mode_of(&fi), 0o444);

    let d2 = dst_path(&s, "d2");
    s.get_file_to(&bg(), &key, &d2, f.fetcher()).await.unwrap();
    assert_eq!(f.calls(), 1);
    let fi2 = fs::metadata(&d2).unwrap();
    let cfi = fs::metadata(data_path(&s, &key)).unwrap();
    assert!(same_file(&fi, &fi2) && same_file(&fi, &cfi));
    assert_eq!(tmp_children(&s), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_file_to_concurrent_callers_share_one_fetch() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = watch::channel(false);
    let mut hs = Vec::new();
    for i in 0..16 {
        let (s2, key, calls, mut rx) =
            (s.s.clone(), key.clone(), calls.clone(), release_rx.clone());
        let dst = dst_path(&s, &format!("d{i}"));
        hs.push(tokio::spawn(async move {
            let f = fetcher(move |_ctx, p: PathBuf| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _ = rx.wait_for(|v| *v).await;
                write_new(&p, b"x", 0o600).map_err(status)
            });
            s2.get_file_to(&bg(), &key, &dst, f).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    release_tx.send(true).unwrap();
    for (i, h) in hs.into_iter().enumerate() {
        h.await
            .unwrap()
            .unwrap_or_else(|e| panic!("caller {i}: {e}"));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_file_to_canceled_waiter_does_not_abort_flight() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, mut entered_rx) = watch::channel(false);
    let (release_tx, release_rx) = watch::channel(false);
    let f = {
        let calls = calls.clone();
        let mut release_rx = release_rx.clone();
        fetcher(move |ctx: Ctx, p: PathBuf| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let _ = entered_tx.send(true);
            tokio::select! {
                _ = release_rx.wait_for(|v| *v) => {}
                // Must not fire on the caller's cancel.
                e = ctx.done() => return Err(tonic::Status::from(e)),
            }
            write_new(&p, b"x", 0o600).map_err(status)
        })
    };
    let ctx = bg();
    let caller = {
        let (s2, key, ctx, dst) = (
            s.s.clone(),
            key.clone(),
            ctx.clone(),
            dst_path(&s, "canceled"),
        );
        tokio::spawn(async move { s2.get_file_to(&ctx, &key, &dst, f).await })
    };
    entered_rx.wait_for(|v| *v).await.unwrap();
    ctx.cancel();
    let err = caller.await.unwrap().unwrap_err();
    assert!(matches!(err, CacheError::Canceled(_)), "{err}");

    release_tx.send(true).unwrap();
    let later = Counting {
        content: Arc::new(b"y".to_vec()),
        calls: calls.clone(),
    };
    s.get_file_to(&bg(), &key, &dst_path(&s, "later"), later.fetcher())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_file_to_fetch_error_reaches_all_waiters_then_retries() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = watch::channel(false);
    let mut hs = Vec::new();
    for i in 0..4 {
        let (s2, key, calls, mut rx) =
            (s.s.clone(), key.clone(), calls.clone(), release_rx.clone());
        let dst = dst_path(&s, &format!("f{i}"));
        hs.push(tokio::spawn(async move {
            let f = fetcher(move |_ctx, _p: PathBuf| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _ = rx.wait_for(|v| *v).await;
                Err(tonic::Status::unavailable("bucket unreachable"))
            });
            s2.get_file_to(&bg(), &key, &dst, f).await
        }));
    }
    // Every caller must have joined the one flight before it fails; a late
    // caller would rightly start a second fetch.
    tokio::time::sleep(Duration::from_millis(300)).await;
    release_tx.send(true).unwrap();
    for (i, h) in hs.into_iter().enumerate() {
        match h.await.unwrap() {
            Err(CacheError::Fetch { status, .. }) => {
                assert_eq!(status.message(), "bucket unreachable", "caller {i}")
            }
            other => panic!("caller {i}: {other:?}, want the fetch error"),
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(tmp_children(&s), 0);
    assert!(
        !entry_exists(&s, &key),
        "entry published despite failed fetch"
    );

    let ok = Counting::new("recovered");
    s.get_file_to(&bg(), &key, &dst_path(&s, "retry"), ok.fetcher())
        .await
        .unwrap();
    assert_eq!(ok.calls(), 1);
}

#[tokio::test]
async fn get_file_to_fetch_timeout_bounds_a_hung_fetch() {
    let s = new_store_with(DEFAULT_MIN_AGE, Duration::from_millis(300));
    let key = Key::uri(&["gs://bucket/golden", "hung.img"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let hung = {
        let calls = calls.clone();
        fetcher(move |ctx: Ctx, _p: PathBuf| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::from(ctx.done().await))
        })
    };
    let err: tonic::Status = s
        .get_file_to(&bg(), &key, &dst_path(&s, "hung"), hung)
        .await
        .unwrap_err()
        .into();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded, "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(tmp_children(&s), 0);
    assert!(!entry_exists(&s, &key));

    let ok = Counting::new("recovered");
    s.get_file_to(&bg(), &key, &dst_path(&s, "after"), ok.fetcher())
        .await
        .unwrap();
    assert_eq!(ok.calls(), 1);
}

#[tokio::test]
async fn get_file_to_rejects_existing_dst() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let dst = dst_path(&s, "occupied");
    fs::write(&dst, "previous").unwrap();
    let err = s
        .get_file_to(&bg(), &key, &dst, Counting::new("x").fetcher())
        .await
        .unwrap_err();
    assert!(err.is_exists(), "{err}");
    assert_eq!(fs::read(&dst).unwrap(), b"previous");
}

#[tokio::test]
async fn get_file_to_rejects_bad_arguments() {
    // A zero key and a missing fetcher cannot be expressed: keys come only
    // from the constructors and a fetcher is a value, not a nullable.
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let f = Counting::new("x");
    for dst in ["", "relative/dst"] {
        let err = s
            .get_file_to(&bg(), &key, Path::new(dst), f.fetcher())
            .await
            .unwrap_err();
        assert!(matches!(err, CacheError::Invalid(_)), "{dst:?}: {err}");
    }
    assert_eq!(f.calls(), 0);
}

#[tokio::test]
async fn get_file_to_fetcher_producing_no_file_fails() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let noop = fetcher(|_ctx, _p: PathBuf| async { Ok(()) });
    assert!(s
        .get_file_to(&bg(), &key, &dst_path(&s, "empty"), noop)
        .await
        .is_err());
    assert!(!entry_exists(&s, &key));
}

#[tokio::test]
async fn get_file_to_hit_touches_last_use() {
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let f = Counting::new("x");
    s.get_file_to(&bg(), &key, &dst_path(&s, "first"), f.fetcher())
        .await
        .unwrap();
    let stale = ago(Duration::from_secs(3600));
    set_mtime(&entry_dir(&s, &key), stale);
    s.get_file_to(&bg(), &key, &dst_path(&s, "second"), f.fetcher())
        .await
        .unwrap();
    let m = fs::metadata(entry_dir(&s, &key))
        .unwrap()
        .modified()
        .unwrap();
    assert!(
        m > stale + Duration::from_secs(60),
        "mtime not refreshed by hit"
    );
}

#[tokio::test]
async fn get_file_to_published_copy_is_write_protected() {
    if is_root() {
        eprintln!("skipping: root ignores file permissions");
        return;
    }
    let s = new_store();
    let key = Key::uri(&["gs://bucket/golden", "mem.img"]);
    let dst = dst_path(&s, "d");
    s.get_file_to(&bg(), &key, &dst, Counting::new("precious").fetcher())
        .await
        .unwrap();
    assert!(
        fs::write(&dst, "mutated").is_err(),
        "write through link succeeded"
    );
    assert_eq!(fs::read(data_path(&s, &key)).unwrap(), b"precious");
}

#[tokio::test]
async fn get_file_to_keeps_execute_bits() {
    let s = new_store();
    for (name, fetched, published) in [
        ("binary", 0o755, 0o555),
        ("data", 0o644, 0o444),
        ("private binary", 0o700, 0o544),
    ] {
        let key = Key::uri(&[format!("test://mode/{name}")]);
        let f = fetcher(move |_ctx, p: PathBuf| async move {
            write_new(&p, b"bits", fetched).map_err(status)
        });
        let dst = dst_path(&s, &format!("mode-{name}"));
        s.get_file_to(&bg(), &key, &dst, f).await.unwrap();
        assert_eq!(mode_of(&fs::metadata(&dst).unwrap()), published, "{name}");
    }
}

// ---- get_dir_to ----------------------------------------------------------

#[tokio::test]
async fn get_dir_to_miss_then_hit() {
    let s = new_store();
    let key = Key::sha256(&"a".repeat(64)).unwrap();
    let t = TreeFetch::new(RELEASE_TREE);
    let d1 = dst_path(&s, "t1");
    s.get_dir_to(&bg(), &key, &d1, t.fetcher()).await.unwrap();
    check_tree(&s, &key, &d1, RELEASE_TREE);
    let d2 = dst_path(&s, "t2");
    s.get_dir_to(&bg(), &key, &d2, t.fetcher()).await.unwrap();
    check_tree(&s, &key, &d2, RELEASE_TREE);
    assert_eq!(t.calls(), 1);

    // The consumer owns its directories, but not the linked bytes.
    fs::write(d1.join("gvisor-bin/mine"), "x").expect("consumer cannot write into its own dir");
    assert!(!data_path(&s, &key).join("gvisor-bin/mine").exists());
    if !is_root() {
        assert!(fs::write(d1.join("runsc"), "mutated").is_err());
    }

    assert_eq!(tmp_children(&s), 0);
    let mut siblings: Vec<String> = fs::read_dir(d1.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    siblings.sort();
    assert_eq!(siblings, ["t1", "t2"], "link-out left debris beside dst");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_dir_to_same_destination_loser_sees_complete_tree() {
    let s = new_store();
    let key = Key::uri(&["test://release"]);
    let t = TreeFetch::new(RELEASE_TREE);
    let dst = dst_path(&s, "shared");
    let mut hs = Vec::new();
    for _ in 0..6 {
        let (s2, key, dst, f) = (s.s.clone(), key.clone(), dst.clone(), t.fetcher());
        hs.push(tokio::spawn(async move {
            s2.get_dir_to(&bg(), &key, &dst, f).await
        }));
    }
    let mut wins = 0;
    for (i, h) in hs.into_iter().enumerate() {
        match h.await.unwrap() {
            Ok(()) => wins += 1,
            Err(e) if e.is_exists() => {}
            Err(e) => panic!("caller {i}: {e}, want ok or exists"),
        }
    }
    assert_eq!(wins, 1);
    check_tree(&s, &key, &dst, RELEASE_TREE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_dir_to_concurrent_callers_share_one_fetch() {
    let s = new_store();
    let key = Key::uri(&["test://release"]);
    let t = TreeFetch::new(RELEASE_TREE);
    let mut hs = Vec::new();
    for i in 0..8 {
        let (s2, key, f) = (s.s.clone(), key.clone(), t.fetcher());
        let dst = dst_path(&s, &format!("c{i}"));
        hs.push(tokio::spawn(async move {
            s2.get_dir_to(&bg(), &key, &dst, f).await
        }));
    }
    for h in hs {
        h.await.unwrap().unwrap();
    }
    assert_eq!(t.calls(), 1);
}

#[tokio::test]
async fn get_dir_to_rejects_existing_dst_but_publishes() {
    let s = new_store();
    let key = Key::uri(&["test://release"]);
    let dst = dst_path(&s, "taken");
    fs::create_dir(&dst).unwrap();
    let err = s
        .get_dir_to(&bg(), &key, &dst, TreeFetch::new(RELEASE_TREE).fetcher())
        .await
        .unwrap_err();
    assert!(err.is_exists(), "{err}");
    assert!(data_path(&s, &key).join("runsc").exists());
}

#[tokio::test]
async fn get_dir_to_rejects_bad_arguments() {
    let s = new_store();
    let t = TreeFetch::new(RELEASE_TREE);
    let err = s
        .get_dir_to(
            &bg(),
            &Key::uri(&["test://release"]),
            Path::new("relative/dst"),
            t.fetcher(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CacheError::Invalid(_)));
    assert_eq!(t.calls(), 0);
}

#[tokio::test]
async fn get_dir_to_empty_tree_fails() {
    let s = new_store();
    let key = Key::uri(&["test://empty"]);
    assert!(s
        .get_dir_to(
            &bg(),
            &key,
            &dst_path(&s, "e"),
            TreeFetch::new(&[]).fetcher()
        )
        .await
        .is_err());
    assert!(!entry_exists(&s, &key));
}

#[tokio::test]
async fn get_dir_to_rejects_symlink_in_tree() {
    let s = new_store();
    let key = Key::uri(&["test://symlinked"]);
    let f = fetcher(|_ctx, dir: PathBuf| async move {
        write_new(&dir.join("runsc"), b"x", 0o755).map_err(status)?;
        std::os::unix::fs::symlink("runsc", dir.join("alias")).map_err(status)
    });
    assert!(s
        .get_dir_to(&bg(), &key, &dst_path(&s, "l"), f)
        .await
        .is_err());
    assert!(!entry_exists(&s, &key));
    assert_eq!(tmp_children(&s), 0);
}

#[tokio::test]
async fn get_kind_mismatch_is_an_error() {
    let s = new_store();
    let tree_key = Key::uri(&["test://tree"]);
    let t = TreeFetch::new(RELEASE_TREE);
    s.get_dir_to(&bg(), &tree_key, &dst_path(&s, "t"), t.fetcher())
        .await
        .unwrap();
    let file_key = Key::uri(&["test://file"]);
    let f = Counting::new("one file");
    s.get_file_to(&bg(), &file_key, &dst_path(&s, "f"), f.fetcher())
        .await
        .unwrap();

    assert!(s
        .get_file_to(&bg(), &tree_key, &dst_path(&s, "t-as-file"), f.fetcher())
        .await
        .is_err());
    assert!(s
        .get_file_copy_to(&bg(), &tree_key, &dst_path(&s, "t-as-copy"), f.fetcher())
        .await
        .is_err());
    assert!(s
        .get_dir_to(&bg(), &file_key, &dst_path(&s, "f-as-tree"), t.fetcher())
        .await
        .is_err());
    assert_eq!(
        (t.calls(), f.calls()),
        (1, 1),
        "a mismatch must not refetch"
    );
}

// ---- get_file_copy_to ----------------------------------------------------

#[tokio::test]
async fn get_file_copy_to_miss_then_hit() {
    let s = new_store();
    let key = Key::uri(&["golden", "memory"]);
    let f = Counting::new("golden memory");
    let first = dst_path(&s, "first");
    let second = dst_path(&s, "second");
    for dst in [&first, &second] {
        s.get_file_copy_to(&bg(), &key, dst, f.fetcher())
            .await
            .unwrap();
        assert_eq!(fs::read(dst).unwrap(), b"golden memory");
    }
    assert_eq!(f.calls(), 1);
    let fi1 = fs::metadata(&first).unwrap();
    let fi2 = fs::metadata(&second).unwrap();
    assert!(!same_file(&fi1, &fi2), "copies share an inode");
    assert_eq!(fi1.nlink(), 1, "a copy must not be linked to the cache");
    assert_ne!(mode_of(&fi1) & 0o200, 0, "copy is not owner-writable");
}

#[tokio::test]
async fn get_file_copy_to_mutation_isolation() {
    let s = new_store();
    let key = Key::uri(&["golden", "config"]);
    let f = Counting::new("original");
    let first = dst_path(&s, "first");
    s.get_file_copy_to(&bg(), &key, &first, f.fetcher())
        .await
        .unwrap();
    fs::write(&first, "mutated by consumer").unwrap();
    let second = dst_path(&s, "second");
    s.get_file_copy_to(&bg(), &key, &second, f.fetcher())
        .await
        .unwrap();
    assert_eq!(fs::read(&second).unwrap(), b"original");
    assert_eq!(f.calls(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_file_copy_to_concurrent_single_flight() {
    let s = new_store();
    let key = Key::uri(&["golden", "memory"]);
    let f = Counting::new("golden memory");
    let mut hs = Vec::new();
    for i in 0..8 {
        let (s2, key, fe) = (s.s.clone(), key.clone(), f.fetcher());
        let dst = dst_path(&s, &format!("copy-{i}"));
        hs.push(tokio::spawn(async move {
            s2.get_file_copy_to(&bg(), &key, &dst, fe).await
        }));
    }
    for (i, h) in hs.into_iter().enumerate() {
        h.await.unwrap().unwrap();
        assert_eq!(
            fs::read(dst_path(&s, &format!("copy-{i}"))).unwrap(),
            b"golden memory"
        );
    }
    assert_eq!(f.calls(), 1);
}

#[tokio::test]
async fn get_file_copy_to_evicted_entry_refetches() {
    let s = new_store_min_age(Duration::from_millis(1));
    let key = Key::uri(&["golden", "memory"]);
    let f = Counting::new("golden memory");
    let first = dst_path(&s, "first");
    s.get_file_copy_to(&bg(), &key, &first, f.fetcher())
        .await
        .unwrap();
    set_mtime(&entry_dir(&s, &key), ago(Duration::from_secs(3600)));
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!((stats.retired, stats.skipped_linked), (1, 0));
    assert_eq!(fs::read(&first).unwrap(), b"golden memory");
    s.get_file_copy_to(&bg(), &key, &dst_path(&s, "second"), f.fetcher())
        .await
        .unwrap();
    assert_eq!(f.calls(), 2, "miss after eviction");
}

#[tokio::test]
async fn get_file_copy_to_preserves_sparseness() {
    let s = new_store();
    let key = Key::uri(&["golden", "sparse"]);
    // Large enough that every filesystem tested keeps the source sparse.
    const SIZE: u64 = 64 << 20;
    let dst = dst_path(&s, "sparse-copy");
    s.get_file_copy_to(&bg(), &key, &dst, sparse_fetcher(SIZE, 1 << 20))
        .await
        .unwrap();
    assert_eq!(fs::metadata(&dst).unwrap().len(), SIZE);
    let src_alloc = allocated_bytes(&fs::metadata(data_path(&s, &key)).unwrap());
    if src_alloc >= (SIZE / 2) as i64 {
        eprintln!("skipping: cached entry is not sparse on this filesystem");
        return;
    }
    let dst_alloc = allocated_bytes(&fs::metadata(&dst).unwrap());
    assert!(
        dst_alloc < (SIZE / 2) as i64,
        "copy allocated {dst_alloc}: holes filled"
    );
}

#[tokio::test]
async fn get_file_copy_to_rejects_bad_arguments() {
    let s = new_store();
    let f = Counting::new("x");
    let key = Key::uri(&["k"]);
    let err = s
        .get_file_copy_to(&bg(), &key, Path::new("relative/path"), f.fetcher())
        .await
        .unwrap_err();
    assert!(matches!(err, CacheError::Invalid(_)));
    let exists = dst_path(&s, "exists");
    fs::write(&exists, "occupied").unwrap();
    assert!(s
        .get_file_copy_to(&bg(), &key, &exists, f.fetcher())
        .await
        .unwrap_err()
        .is_exists());
    assert_eq!(fs::read(&exists).unwrap(), b"occupied");
}

// ---- eviction ------------------------------------------------------------

#[tokio::test]
async fn evict_unused_non_positive_target_is_noop() {
    let s = new_store_min_age(Duration::ZERO);
    let key = cache_entry(&s, "kept", "x", Duration::from_secs(3600), false).await;
    assert_eq!(
        s.evict_unused(0, false).await.unwrap(),
        EvictStats::default()
    );
    assert!(entry_exists(&s, &key));
}

#[tokio::test]
async fn evict_unused_takes_lru_and_stops_at_target() {
    let s = new_store_min_age(Duration::ZERO);
    let h = Duration::from_secs(3600);
    let oldest = cache_entry(&s, "oldest", "aaaaa", 3 * h, false).await;
    let middle = cache_entry(&s, "middle", "bbbbb", 2 * h, false).await;
    let newest = cache_entry(&s, "newest", "ccccc", h, false).await;
    let stats = s.evict_unused(1, false).await.unwrap();
    assert!(stats.retired == 1 && stats.freed_bytes >= 5, "{stats:?}");
    assert!(!entry_exists(&s, &oldest));
    assert!(entry_exists(&s, &middle) && entry_exists(&s, &newest));
    no_retired_leftovers(&s);
}

#[tokio::test]
async fn evict_unused_respects_min_age() {
    let s = new_store_min_age(Duration::from_secs(3600));
    let young = cache_entry(&s, "young", "x", Duration::from_secs(1800), false).await;
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!((stats.retired, stats.skipped_young), (0, 1));
    assert!(entry_exists(&s, &young));
}

#[tokio::test]
async fn evict_unused_takes_only_unlinked_entries() {
    let s = new_store_min_age(Duration::ZERO);
    let h = Duration::from_secs(3600);
    let linked = cache_entry(&s, "linked", "xx", 3 * h, true).await;
    let unlinked = cache_entry(&s, "unlinked", "yy", h, false).await;
    let stats = s.evict_unused(1, false).await.unwrap();
    assert!(
        stats.retired == 1 && stats.freed_bytes > 0 && stats.skipped_linked == 1,
        "{stats:?}"
    );
    assert!(!entry_exists(&s, &unlinked));
    assert!(entry_exists(&s, &linked));
}

#[tokio::test]
async fn evict_unused_never_evicts_a_linked_entry() {
    let s = new_store_min_age(Duration::ZERO);
    let key = Key::uri(&["test://held"]);
    let f = Counting::new("held bytes");
    let dst = dst_path(&s, "held");
    s.get_file_to(&bg(), &key, &dst, f.fetcher()).await.unwrap();
    let stale = ago(Duration::from_secs(3600));
    set_mtime(&entry_dir(&s, &key), stale);

    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!(
        (stats.retired, stats.freed_bytes, stats.skipped_linked),
        (0, 0, 1)
    );
    assert!(entry_exists(&s, &key));
    assert_eq!(fs::read(&dst).unwrap(), b"held bytes");
    let dst2 = dst_path(&s, "held2");
    s.get_file_to(&bg(), &key, &dst2, f.fetcher())
        .await
        .unwrap();
    assert_eq!(f.calls(), 1, "the second caller hits");

    fs::remove_file(&dst).unwrap();
    fs::remove_file(&dst2).unwrap();
    set_mtime(&entry_dir(&s, &key), stale);
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!(stats.retired, 1);
    assert!(!entry_exists(&s, &key));
}

#[tokio::test]
async fn evict_unused_dry_run_touches_nothing() {
    let s = new_store_min_age(Duration::ZERO);
    let h = Duration::from_secs(3600);
    let a = cache_entry(&s, "a", "aa", 2 * h, false).await;
    let b = cache_entry(&s, "b", "bb", h, true).await;
    let stats = s.evict_unused(EVICT_ALL, true).await.unwrap();
    assert!(
        stats.retired == 1 && stats.freed_bytes > 0 && stats.skipped_linked == 1,
        "{stats:?}"
    );
    assert!(entry_exists(&s, &a) && entry_exists(&s, &b));
    no_retired_leftovers(&s);
}

#[tokio::test]
async fn evict_unused_removal_failure_is_not_counted_freed() {
    if is_root() {
        eprintln!("skipping: root ignores directory permissions");
        return;
    }
    let s = new_store_min_age(Duration::ZERO);
    let key = publish_test_entry(&s, "stuck", &[("data/locked/f", "12345")]);
    let locked = entry_dir(&s, &key).join("data/locked");
    chmod(&locked, 0o555).unwrap();
    set_mtime(&entry_dir(&s, &key), ago(Duration::from_secs(3600)));

    let res = s.evict_unused(EVICT_ALL, false).await;
    // Let the temp dir cleanup succeed wherever the dir ended up.
    for child in fs::read_dir(s.root()).unwrap() {
        let p = child.unwrap().path().join("data/locked");
        if p.exists() {
            chmod(&p, 0o700).unwrap();
        }
    }
    let (stats, _err) = res.expect_err("eviction succeeded despite an unremovable entry");
    assert_eq!(stats.freed_bytes, 0);
    assert_eq!(stats.retired, 1, "the retire itself succeeded");
    assert!(!entry_exists(&s, &key));
}

#[tokio::test]
async fn evict_unused_freed_bytes_are_allocated_not_logical() {
    let s = new_store_min_age(Duration::ZERO);
    const LOGICAL: u64 = 64 << 20;
    let key = Key::uri(&["test://sparse"]);
    let dst = dst_path(&s, "sparse");
    s.get_file_to(&bg(), &key, &dst, sparse_fetcher(LOGICAL, LOGICAL / 2))
        .await
        .unwrap();
    if allocated_bytes(&fs::metadata(data_path(&s, &key)).unwrap()) >= (LOGICAL / 2) as i64 {
        eprintln!("skipping: cached file is not sparse on this filesystem");
        return;
    }
    fs::remove_file(&dst).unwrap();
    set_mtime(&entry_dir(&s, &key), ago(Duration::from_secs(3600)));
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!(stats.retired, 1);
    assert!(
        stats.freed_bytes > 0 && stats.freed_bytes < (LOGICAL / 2) as i64,
        "{stats:?}"
    );
}

#[tokio::test]
async fn evict_unused_reports_unreadable_entries() {
    if is_root() {
        eprintln!("skipping: root ignores directory permissions");
        return;
    }
    let s = new_store_min_age(Duration::ZERO);
    let healthy = cache_entry(&s, "healthy", "bytes", Duration::from_secs(3600), false).await;
    let broken = publish_test_entry(&s, "broken", &[(DATA_NAME, "x")]);
    chmod(&entry_dir(&s, &broken), 0o000).unwrap();
    let res = s.evict_unused(EVICT_ALL, false).await;
    chmod(&entry_dir(&s, &broken), 0o755).unwrap();
    let (stats, err) = res.expect_err("unreadable entry not reported");
    assert!(err.to_string().contains(broken.dir()), "{err}");
    assert_eq!(stats.retired, 1, "{stats:?}");
    assert!(!entry_exists(&s, &healthy));
}

#[tokio::test]
async fn evict_unused_removes_retired_leftovers() {
    let s = new_store_min_age(Duration::ZERO);
    let leftover = s.root().join(format!("{RM_PREFIX}aaaa-stuck"));
    fs::create_dir_all(&leftover).unwrap();
    fs::write(leftover.join(DATA_NAME), "stranded").unwrap();
    chmod(&leftover.join(DATA_NAME), 0o444).unwrap();
    s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert!(!leftover.exists());
}

#[tokio::test]
async fn retire_entry_vetoes_when_last_use_moved() {
    let s = new_store_min_age(Duration::ZERO);
    let key = cache_entry(&s, "busy", "x", Duration::from_secs(3600), false).await;
    let listed = candidate(&s, &key);
    set_mtime(&entry_dir(&s, &key), SystemTime::now());
    assert_eq!(s.inner.retire_entry(&listed).unwrap(), None);
    assert!(entry_exists(&s, &key));
}

#[tokio::test]
async fn retire_entry_vetoes_when_linked_since_listing() {
    let s = new_store_min_age(Duration::ZERO);
    let key = cache_entry(&s, "relinked", "x", Duration::from_secs(3600), false).await;
    let listed = candidate(&s, &key);
    // A link without the touch a hit makes, as a failed touch would leave.
    let held = dst_path(&s, "held");
    fs::hard_link(data_path(&s, &key), &held).unwrap();
    assert_eq!(s.inner.retire_entry(&listed).unwrap(), None);
    assert!(entry_exists(&s, &key));
    fs::remove_file(&held).unwrap();
    assert!(s.inner.retire_entry(&listed).unwrap().is_some());
}

#[tokio::test]
async fn retire_entry_retires_unchanged_entry() {
    let s = new_store_min_age(Duration::ZERO);
    let key = cache_entry(&s, "stale", "x", Duration::from_secs(3600), false).await;
    let rm = s
        .inner
        .retire_entry(&candidate(&s, &key))
        .unwrap()
        .expect("unchanged entry not retired");
    assert!(!entry_exists(&s, &key));
    assert!(rm
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with(RM_PREFIX));
    assert!(rm.exists(), "retired dir missing before removal");
    assert_eq!(s.sweep_debris().await.unwrap().retired_removed, 1);
}

#[tokio::test]
async fn evict_unused_unlinked_tree_frees_its_bytes() {
    let s = new_store_min_age(Duration::ZERO);
    let (key, _) = cache_tree(&s, "gone", Duration::from_secs(3600), None).await;
    let (size, linked) = size_entry(&entry_dir(&s, &key)).unwrap();
    assert!(!linked);
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!((stats.retired, stats.freed_bytes), (1, size));
    assert!(!entry_exists(&s, &key));
    no_retired_leftovers(&s);
}

#[tokio::test]
async fn evict_unused_partly_linked_tree_is_not_evicted() {
    let s = new_store_min_age(Duration::ZERO);
    let (key, dst) = cache_tree(
        &s,
        "held",
        Duration::from_secs(3600),
        Some(&["gvisor-bin/LICENSE"]),
    )
    .await;
    assert!(size_entry(&entry_dir(&s, &key)).unwrap().1);
    let stats = s.evict_unused(EVICT_ALL, false).await.unwrap();
    assert_eq!((stats.retired, stats.skipped_linked), (0, 1));
    assert!(entry_exists(&s, &key));
    assert_eq!(
        fs::read(dst.join("gvisor-bin/LICENSE")).unwrap(),
        b"license"
    );
    no_retired_leftovers(&s);
}

#[tokio::test]
async fn evict_unused_takes_only_unlinked_trees() {
    let s = new_store_min_age(Duration::ZERO);
    let h = Duration::from_secs(3600);
    let (linked, _) = cache_tree(&s, "old-linked", 3 * h, Some(&["runsc"])).await;
    let (unlinked, _) = cache_tree(&s, "new-unlinked", h, None).await;
    let stats = s.evict_unused(1, false).await.unwrap();
    assert_eq!(stats.retired, 1);
    assert!(!entry_exists(&s, &unlinked) && entry_exists(&s, &linked));
}

// ---- storm ---------------------------------------------------------------

/// Linkers, copiers and evictors on the same keys: every get succeeds with
/// the right content; eviction may force refetches, never failures. Two
/// seeded keys stay idle, so eviction provably retires something and a
/// later get of one of them refetches.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn stress_gets_against_eviction() {
    let s = new_store_min_age(Duration::from_millis(200));
    const KEYS: usize = 8;
    const HOT: usize = KEYS - 2;
    const GETTERS: usize = 4;
    let content = |k: usize| format!("content-{k}");
    let key = |k: usize| Key::uri(&["stress".to_string(), k.to_string()]);
    let fetches = Arc::new(AtomicUsize::new(0));
    let fetcher_for = {
        let fetches = fetches.clone();
        move |k: usize| {
            let fetches = fetches.clone();
            fetcher(move |_ctx, p: PathBuf| async move {
                fetches.fetch_add(1, Ordering::SeqCst);
                write_new(&p, format!("content-{k}").as_bytes(), 0o600).map_err(status)
            })
        }
    };

    for k in 0..KEYS {
        let seed = dst_path(&s, &format!("seed-{k}"));
        s.get_file_to(&bg(), &key(k), &seed, fetcher_for(k))
            .await
            .unwrap();
        fs::remove_file(&seed).unwrap();
        set_mtime(&entry_dir(&s, &key(k)), ago(Duration::from_secs(3600)));
    }

    let stop = tokio_util::sync::CancellationToken::new();
    let retired = Arc::new(AtomicI64::new(0));
    let mut hs = Vec::new();
    for g in 0..GETTERS {
        let (s2, stop, fetcher_for) = (s.s.clone(), stop.clone(), fetcher_for.clone());
        let consumer = dst_path(&s, "x").parent().unwrap().to_path_buf();
        hs.push(tokio::spawn(async move {
            let mut seed = g as u64 + 1;
            let mut i = 0usize;
            while !stop.is_cancelled() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let k = ((seed >> 33) as usize) % HOT;
                let dst = consumer.join(format!("g{g}-i{i}"));
                let res = if g % 2 == 0 {
                    s2.get_file_to(&bg(), &key(k), &dst, fetcher_for(k)).await
                } else {
                    s2.get_file_copy_to(&bg(), &key(k), &dst, fetcher_for(k))
                        .await
                };
                res.map_err(|e| format!("getter {g}: {e}"))?;
                let got = fs::read(&dst).map_err(|e| e.to_string())?;
                if got != content(k).as_bytes() {
                    return Err(format!("getter {g} key {k}: content {got:?}"));
                }
                if i.is_multiple_of(2) {
                    fs::remove_file(&dst).map_err(|e| e.to_string())?;
                }
                i += 1;
            }
            Ok::<(), String>(())
        }));
    }
    for _ in 0..2 {
        let (s2, stop, retired) = (s.s.clone(), stop.clone(), retired.clone());
        hs.push(tokio::spawn(async move {
            while !stop.is_cancelled() {
                let st = s2
                    .evict_unused(EVICT_ALL, false)
                    .await
                    .map_err(|(_, e)| format!("evictor: {e}"))?;
                retired.fetch_add(st.retired as i64, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        }));
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    stop.cancel();
    for h in hs {
        if let Err(e) = h.await.unwrap() {
            panic!("{e}");
        }
    }

    assert!(
        retired.load(Ordering::SeqCst) >= (KEYS - HOT) as i64,
        "storm retired {} entries",
        retired.load(Ordering::SeqCst)
    );
    let pre = fetches.load(Ordering::SeqCst);
    let idle = KEYS - 1;
    let refetched = dst_path(&s, "post-storm-refetch");
    s.get_file_to(&bg(), &key(idle), &refetched, fetcher_for(idle))
        .await
        .unwrap();
    assert_eq!(fs::read(&refetched).unwrap(), content(idle).as_bytes());
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        pre + 1,
        "idle key never evicted?"
    );

    assert_eq!(tmp_children(&s), 0);
    s.sweep_debris().await.unwrap();
    assert!(s.total_bytes().await.unwrap() >= 0);
    let consumer = refetched.parent().unwrap();
    for e in fs::read_dir(consumer).unwrap() {
        let e = e.unwrap();
        if e.file_name().to_string_lossy().starts_with('g') {
            assert!(
                !fs::read(e.path()).unwrap().is_empty(),
                "{:?} unreadable",
                e.path()
            );
        }
    }
}
