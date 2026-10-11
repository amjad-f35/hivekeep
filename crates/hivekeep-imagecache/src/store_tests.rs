//! Pull-path tests against the in-memory registry, plus the fixtures the
//! other store test modules share.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_node::context::Ctx;
use parking_lot::Mutex;
use tar::EntryType;

use super::*;
use crate::auth::Credential;
use crate::backoff::Backoff;
use crate::keychain::{Keychain, StaticKeychain};
use crate::testutil::image::{lib_layers, mem_layer, MemLayer, TarEntry};
use crate::testutil::registry::{hook, Action, Hook, ReqInfo, TestRegistry};
use crate::unpack::read_whiteouts;

// ---- shared fixtures ----

pub(crate) fn fixture_store(opts: StoreOptions) -> (tempfile::TempDir, Store) {
    let t = tempfile::tempdir().unwrap();
    let s = Store::open(t.path(), opts).unwrap();
    (t, s)
}

pub(crate) async fn must_ensure(store: &Store, r: &str) -> Image {
    match store.ensure_image(&Ctx::background(), r).await {
        Ok(i) => i,
        Err(e) => panic!("ensure_image({r}): {e}"),
    }
}

/// Shifts `path`'s mtime `age` into the past.
pub(crate) fn backdate(path: &Path, age: Duration) {
    let past = filetime::FileTime::from_system_time(SystemTime::now() - age);
    filetime::set_file_times(path, past, past)
        .unwrap_or_else(|e| panic!("backdating {path:?}: {e}"));
}

/// Ages every record and layer dir past min-age.
pub(crate) fn backdate_store(store: &Store, age: Duration) {
    for dir in [store.manifests_dir(), store.layers_dir()] {
        for e in fs::read_dir(&dir).unwrap().flatten() {
            if !e.file_name().to_string_lossy().starts_with('.') {
                backdate(&e.path(), age);
            }
        }
    }
}

pub(crate) fn layer_dirs_on_disk(store: &Store) -> Vec<String> {
    fs::read_dir(store.layers_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with('.'))
        .collect()
}

/// Pushes a one-file image and returns its full reference.
pub(crate) fn one_file_image(reg: &TestRegistry, repo_tag: &str, name: &str, body: &str) -> String {
    reg.push(
        repo_tag,
        ImageConfig::default(),
        &[mem_layer(&[TarEntry::file(name, body).mode(0o644)])],
    );
    reg.reference(repo_tag)
}

pub(crate) fn hours(h: u64) -> Duration {
    Duration::from_secs(h * 3600)
}

/// Polls `cond` for up to ten seconds.
pub(crate) async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

fn records(store: &Store) -> Vec<PathBuf> {
    fs::read_dir(store.manifests_dir())
        .unwrap()
        .flatten()
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| e.path())
        .collect()
}

fn assert_pool_empty(store: &Store) {
    let entries: Vec<_> = fs::read_dir(store.layers_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(
        entries.is_empty(),
        "unexpected entries in layer pool: {entries:?}"
    );
}

fn read_lib(layer_dir: &Path) -> String {
    fs::read_to_string(layer_dir.join(LAYER_FS_DIR_NAME).join("lib")).unwrap()
}

fn layer_dir_of(store: &Store, l: &MemLayer) -> PathBuf {
    store.layer_dir(&l.diff_id)
}

/// A registry whose blob GETs block while gated.
pub(crate) struct GatedRegistry {
    pub reg: TestRegistry,
    gates: Arc<Mutex<HashMap<String, Arc<Gate>>>>,
}

pub(crate) struct Gate {
    open: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Gate {
    pub(crate) fn release(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
    async fn wait(&self) {
        loop {
            let n = self.notify.notified();
            if self.open.load(Ordering::SeqCst) {
                return;
            }
            n.await;
        }
    }
}

impl GatedRegistry {
    pub(crate) async fn start() -> Self {
        let gates: Arc<Mutex<HashMap<String, Arc<Gate>>>> = Arc::default();
        let g = gates.clone();
        let h: Hook = Arc::new(move |r: ReqInfo| {
            let gate = if r.method == "GET" && r.path.contains("/blobs/") {
                g.lock()
                    .iter()
                    .find(|(hex, _)| r.path.contains(hex.as_str()))
                    .map(|(_, gate)| gate.clone())
            } else {
                None
            };
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.wait().await;
                }
                Action::Pass
            })
        });
        GatedRegistry {
            reg: TestRegistry::start_with(h).await,
            gates,
        }
    }

    /// Blocks future GETs of the layer's blob until released.
    pub(crate) fn gate(&self, l: &MemLayer) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            open: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        });
        self.gates.lock().insert(l.digest.hex.clone(), gate.clone());
        gate
    }
}

impl Drop for GatedRegistry {
    fn drop(&mut self) {
        // Never leave a pull blocked at test exit.
        for g in self.gates.lock().values() {
            g.release();
        }
    }
}

pub(crate) fn gated_test_layers() -> (MemLayer, MemLayer, MemLayer) {
    (
        mem_layer(&[TarEntry::file("a", &"a".repeat(2048))]),
        mem_layer(&[TarEntry::file("b", &"b".repeat(2048))]),
        mem_layer(&[TarEntry::file("c", &"c".repeat(2048))]),
    )
}

/// Captures log output for the current thread (and tasks spawned from it).
pub(crate) struct LogCapture {
    buf: Arc<Mutex<Vec<u8>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

impl LogCapture {
    pub(crate) fn start() -> Self {
        let buf: Arc<Mutex<Vec<u8>>> = Arc::default();
        let b = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || LogWriter(b.clone()))
            .finish();
        LogCapture {
            buf,
            _guard: tracing::subscriber::set_default(sub),
        }
    }
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }
}

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ---- imagecache ----

#[tokio::test]
async fn options_apply() {
    let kc: Arc<dyn Keychain> = Arc::new(StaticKeychain::default());
    let p = Platform {
        os: "linux".into(),
        architecture: "amd64".into(),
        variant: String::new(),
    };
    let (_t, s) = fixture_store(
        StoreOptions::default()
            .with_keychain(kc.clone())
            .with_localhost_registry_replacement("kind-registry:5000")
            .with_platform(p.clone()),
    );
    assert!(Arc::ptr_eq(s.options().keychain.as_ref().unwrap(), &kc));
    assert_eq!(
        s.options().localhost_registry_replacement,
        "kind-registry:5000"
    );
    assert_eq!(s.options().platform.as_ref(), Some(&p));
}

// A pull survives transient 429s on every request, the /v2/ ping included.
#[tokio::test]
async fn ensure_image_retries_rate_limit() {
    let throttle = Arc::new(AtomicI32::new(0));
    let rejections = Arc::new(AtomicI32::new(0));
    let (t, rj) = (throttle.clone(), rejections.clone());
    let reg = TestRegistry::start_with(hook(move |_| {
        if t.fetch_sub(1, Ordering::SeqCst) > 0 {
            rj.fetch_add(1, Ordering::SeqCst);
            Action::Respond(429, vec![])
        } else {
            t.store(0, Ordering::SeqCst);
            Action::Pass
        }
    }))
    .await;
    let r = one_file_image(&reg, "test/pause:1", "pause", "pause");
    throttle.store(2, Ordering::SeqCst);
    let fast = Backoff {
        duration: Duration::from_millis(1),
        factor: 2.0,
        jitter: 0.1,
        steps: 4,
        cap: Duration::ZERO,
    };
    let (_t, s) = fixture_store(StoreOptions::default().with_backoff(fast));
    s.ensure_image(&Ctx::background(), &r).await.unwrap();
    assert_eq!(
        rejections.load(Ordering::SeqCst),
        2,
        "the throttling was not exercised"
    );
}

#[tokio::test]
async fn ensure_image_tag_pull_and_digest_hit() {
    let reg = TestRegistry::start().await;
    let base = mem_layer(&[
        TarEntry::dir("bin/"),
        TarEntry::file("bin/sh", "#!/sh\n").mode(0o755),
    ]);
    let top = mem_layer(&[
        TarEntry::dir("app/"),
        TarEntry::file("app/main", "main").mode(0o755),
        TarEntry::file("bin/.wh.sh", ""),
        TarEntry::file("app/.wh..wh..opq", ""),
    ]);
    let cfg = ImageConfig {
        env: vec!["FOO=bar".into()],
        ..Default::default()
    };
    reg.push("test/app:latest", cfg, &[base, top]);
    let (t, store) = fixture_store(StoreOptions::default());

    let img = must_ensure(&store, &reg.reference("test/app:latest")).await;
    assert_eq!(img.layer_dirs.len(), 2);
    assert_eq!(img.config.env, vec!["FOO=bar"]);
    assert!(
        img.layer_dirs[0].join("fs/bin/sh").exists(),
        "base layer content missing"
    );
    assert!(
        img.layer_dirs[1].join("fs/app/main").exists(),
        "top layer content missing"
    );
    let wh = read_whiteouts(&img.layer_dirs[1]).unwrap();
    assert_eq!(wh.whiteouts, vec!["bin/sh"]);
    assert_eq!(wh.opaques, vec!["app"]);
    assert!(fs::symlink_metadata(img.layer_dirs[1].join("fs/bin/.wh.sh")).is_err());

    // A digest reference hits with the registry gone, through a fresh store
    // on the same root (startup recovery over a populated pool).
    reg.close();
    let store2 = Store::open(t.path(), StoreOptions::default()).unwrap();
    let img2 = must_ensure(&store2, &reg.reference(&format!("test/app@{}", img.digest))).await;
    assert_eq!(img2.layer_dirs, img.layer_dirs);
    assert_eq!(img2.config.env, img.config.env);
}

// Two images sharing a base layer share its tree.
#[tokio::test]
async fn ensure_image_shared_layers_deduplicated() {
    let reg = TestRegistry::start().await;
    let shared = mem_layer(&[TarEntry::dir("lib/"), TarEntry::file("lib/base.so", "base")]);
    reg.push(
        "test/a:latest",
        ImageConfig::default(),
        &[shared.clone(), mem_layer(&[TarEntry::file("a", "a")])],
    );
    reg.push(
        "test/b:latest",
        ImageConfig::default(),
        &[shared, mem_layer(&[TarEntry::file("b", "b")])],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let a = must_ensure(&store, &reg.reference("test/a:latest")).await;
    let b = must_ensure(&store, &reg.reference("test/b:latest")).await;
    assert_eq!(a.layer_dirs[0], b.layer_dirs[0]);
    assert_eq!(fs::read_dir(store.layers_dir()).unwrap().count(), 3);
}

// Startup recovery sweeps crash debris and refuses unknown layouts.
#[test]
fn new_recovery_and_versioning() {
    let (t, s) = fixture_store(StoreOptions::default());
    let orphan = s.layers_dir().join(".tmp-deadbeef-123");
    fs::create_dir_all(&orphan).unwrap();
    let rec_orphan = s.manifests_dir().join(".deadbeef.json.tmp-456");
    fs::write(&rec_orphan, "{").unwrap();
    let record = s.manifests_dir().join("deadbeef.json");
    fs::write(&record, "{}").unwrap();
    Store::open(t.path(), StoreOptions::default()).unwrap();
    assert!(!orphan.exists());
    assert!(!rec_orphan.exists());
    assert!(
        record.exists(),
        "completed manifest record swept by recovery"
    );
    fs::write(t.path().join(VERSION_FILE_NAME), "99\n").unwrap();
    assert!(Store::open(t.path(), StoreOptions::default()).is_err());
}

// A platform index resolves to the host platform's child, and the image is
// recorded under both digests.
#[tokio::test]
async fn ensure_image_resolves_platform_index() {
    let reg = TestRegistry::start().await;
    let host = Platform::host_linux();
    let other = Platform {
        architecture: if host.architecture == "amd64" {
            "arm64".into()
        } else {
            "amd64".into()
        },
        ..host.clone()
    };
    let mine = crate::testutil::image::build_image(
        ImageConfig::default(),
        &[mem_layer(&[TarEntry::file("mine", "1")])],
        None,
    );
    let theirs = crate::testutil::image::build_image(
        ImageConfig::default(),
        &[mem_layer(&[TarEntry::file("theirs", "2")])],
        None,
    );
    let index = reg.push_index(
        "test/multi:latest",
        &[(other, theirs), (host, mine.clone())],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let img = must_ensure(&store, &reg.reference("test/multi:latest")).await;
    assert_eq!(img.digest, index);
    assert!(img.layer_dirs[0].join("fs/mine").exists());
    assert!(
        store.record_path(&mine.digest).exists(),
        "platform child record missing"
    );
    assert!(store.cached_image(&mine.digest).unwrap().is_some());
}

// ---- keychain ----

/// A registry open until `require_auth`, then requiring these credentials.
async fn authed_registry(
    user: &'static str,
    pass: &'static str,
) -> (TestRegistry, Arc<AtomicBool>) {
    let required = Arc::new(AtomicBool::new(false));
    let r = required.clone();
    let reg = TestRegistry::start_with(hook(move |req| {
        if r.load(Ordering::SeqCst)
            && req.basic.as_ref() != Some(&(user.to_string(), pass.to_string()))
        {
            return Action::Respond(
                401,
                vec![("www-authenticate".into(), r#"Basic realm="test""#.into())],
            );
        }
        Action::Pass
    }))
    .await;
    (reg, required)
}

/// Hands out fixed credentials, recording what it was asked about.
#[derive(Default)]
struct RecordingKeychain {
    creds: Vec<Credential>,
    asked: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Keychain for RecordingKeychain {
    async fn candidates(&self, _: &Ctx, repository: &str) -> crate::error::Result<Vec<Credential>> {
        self.asked.lock().push(repository.to_string());
        Ok(self.creds.clone())
    }
}

#[tokio::test]
async fn ensure_image_keychain_authenticates_pull() {
    let (reg, require) = authed_registry("robot", "s3cret").await;
    let r = reg.reference("test/app:latest");
    reg.push(
        "test/app:latest",
        ImageConfig::default(),
        &[mem_layer(&[
            TarEntry::dir("app/"),
            TarEntry::file("app/main", "main").mode(0o755),
        ])],
    );
    require.store(true, Ordering::SeqCst);
    let (_t, anon) = fixture_store(StoreOptions::default());
    assert!(
        anon.ensure_image(&Ctx::background(), &r).await.is_err(),
        "anonymous pull succeeded"
    );

    let kc = Arc::new(RecordingKeychain {
        creds: vec![Credential::new("robot", "s3cret")],
        ..Default::default()
    });
    let (_t2, s) = fixture_store(StoreOptions::default().with_keychain(kc.clone()));
    s.ensure_image(&Ctx::background(), &r).await.unwrap();
    let asked = kc.asked.lock().clone();
    assert!(!asked.is_empty(), "keychain was never consulted");
    for a in asked {
        assert_eq!(a, format!("{}/test/app", reg.host));
    }
}

#[tokio::test]
async fn ensure_image_tries_candidates_in_order() {
    let (reg, require) = authed_registry("robot", "s3cret").await;
    let tag = reg.reference("test/app:latest");
    let img = reg.push(
        "test/app:latest",
        ImageConfig::default(),
        &[mem_layer(&[TarEntry::file("app", "app")])],
    );
    require.store(true, Ordering::SeqCst);
    let wrong = Credential::new("robot", "wrong");
    let right = Credential::new("robot", "s3cret");
    for (name, r, creds, want_err) in [
        (
            "rejected then accepted, digest ref",
            reg.reference(&format!("test/app@{}", img.digest)),
            vec![wrong.clone(), right.clone()],
            false,
        ),
        (
            "rejected then accepted, tag ref",
            tag.clone(),
            vec![wrong.clone(), right.clone()],
            false,
        ),
        (
            "all rejected",
            tag.clone(),
            vec![wrong.clone(), wrong.clone()],
            true,
        ),
        ("none, anonymous", tag.clone(), vec![], true),
    ] {
        let kc = Arc::new(RecordingKeychain {
            creds,
            ..Default::default()
        });
        let (_t, s) = fixture_store(StoreOptions::default().with_keychain(kc.clone()));
        let res = s.ensure_image(&Ctx::background(), &r).await;
        assert_eq!(res.is_err(), want_err, "{name}: {:?}", res.err());
        assert!(
            !kc.asked.lock().is_empty(),
            "{name}: keychain was never consulted"
        );
    }
}

// The first credential can read manifests but not blobs.
#[tokio::test]
async fn ensure_image_retries_candidate_rejected_on_blobs() {
    let locked = Arc::new(AtomicBool::new(false));
    let l = locked.clone();
    let reg = TestRegistry::start_with(hook(move |req| {
        if l.load(Ordering::SeqCst) {
            let ok = match &req.basic {
                None => false,
                Some((_, p)) => !req.path.contains("/blobs/") || p == "s3cret",
            };
            if !ok {
                return Action::Respond(
                    401,
                    vec![("www-authenticate".into(), r#"Basic realm="test""#.into())],
                );
            }
        }
        Action::Pass
    }))
    .await;
    let r = reg.reference("test/app:latest");
    reg.push(
        "test/app:latest",
        ImageConfig::default(),
        &[mem_layer(&[TarEntry::file("app", "app")])],
    );
    locked.store(true, Ordering::SeqCst);
    let kc = Arc::new(StaticKeychain(vec![
        Credential::new("robot", "manifests-only"),
        Credential::new("robot", "s3cret"),
    ]));
    let (_t, s) = fixture_store(StoreOptions::default().with_keychain(kc));
    s.ensure_image(&Ctx::background(), &r).await.unwrap();
}

// Bearer token flow: the registry names a token service, which exchanges
// the credential for a repository-scoped token.
#[tokio::test]
async fn ensure_image_bearer_token_flow() {
    let host = Arc::new(Mutex::new(String::new()));
    let scopes = Arc::new(Mutex::new(Vec::<String>::new()));
    let (h, sc) = (host.clone(), scopes.clone());
    let reg = TestRegistry::start_with(hook(move |req| {
        if req.path == "/token" {
            if req.basic != Some(("robot".into(), "s3cret".into())) {
                return Action::Respond(401, vec![]);
            }
            sc.lock().push(req.path.clone());
            return Action::RespondBody(
                200,
                vec![("content-type".into(), "application/json".into())],
                br#"{"token":"tok"}"#.to_vec(),
            );
        }
        if req.authorization.as_deref() != Some("Bearer tok") {
            let realm = format!(r#"Bearer realm="http://{}/token",service="test""#, h.lock());
            return Action::Respond(401, vec![("www-authenticate".into(), realm)]);
        }
        Action::Pass
    }))
    .await;
    *host.lock() = reg.host.clone();
    let r = one_file_image(&reg, "test/bearer:latest", "f", "x");
    let (_t, anon) = fixture_store(StoreOptions::default());
    assert!(anon.ensure_image(&Ctx::background(), &r).await.is_err());
    let kc = Arc::new(StaticKeychain(vec![Credential::new("robot", "s3cret")]));
    let (_t2, s) = fixture_store(StoreOptions::default().with_keychain(kc));
    must_ensure(&s, &r).await;
    assert!(!scopes.lock().is_empty(), "token service never asked");
}

// ---- verify ----

// A blob whose bytes differ from the manifest digest is rejected, even
// though it decompresses to the claimed diffID.
#[tokio::test]
async fn pull_rejects_blob_not_matching_manifest_digest() {
    let tamper = Arc::new(Mutex::new(String::new()));
    let t = tamper.clone();
    let reg = TestRegistry::start_with(hook(move |req| {
        let hex = t.lock().clone();
        if req.method == "GET"
            && !hex.is_empty()
            && req.path.ends_with(&format!("/blobs/sha256:{hex}"))
        {
            return Action::Tamper(Box::new(|mut b| {
                // Byte 4 is the gzip header MTIME, covered by no gzip check.
                b[4] ^= 0xff;
                b
            }));
        }
        Action::Pass
    }))
    .await;
    let (good, _) = lib_layers();
    reg.push(
        "test/tampered:latest",
        ImageConfig::default(),
        std::slice::from_ref(&good),
    );
    *tamper.lock() = good.digest.hex.clone();
    let (_t, s) = fixture_store(StoreOptions::default());
    let err = s
        .ensure_image(&Ctx::background(), &reg.reference("test/tampered:latest"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("error verifying sha256 checksum"),
        "{err}"
    );
    assert_pool_empty(&s);
}

// An image claiming another layer's diffID cannot plant its content under
// that diffID.
#[tokio::test]
async fn pull_rejects_content_not_matching_claimed_diff_id() {
    let reg = TestRegistry::start().await;
    let (good, evil) = lib_layers();
    reg.push_lying("test/evil:latest", &[evil], vec![good.diff_id.clone()]);
    reg.push("test/good:latest", ImageConfig::default(), &[good]);
    let logs = LogCapture::start();
    let (_t, s) = fixture_store(StoreOptions::default());
    let err = s
        .ensure_image(&Ctx::background(), &reg.reference("test/evil:latest"))
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::DiffIdMismatch), "{err}");
    let text = logs.text();
    assert!(
        text.contains("WARN") && text.contains("Image layer does not match its claimed diffID"),
        "no mismatch WARN in logs:\n{text}"
    );
    assert_pool_empty(&s);
    let img = must_ensure(&s, &reg.reference("test/good:latest")).await;
    assert_eq!(read_lib(&img.layer_dirs[0]), "good");
}

// A content mismatch is the image's, so it is not re-downloaded per
// candidate credential.
#[tokio::test]
async fn diff_id_mismatch_not_retried_per_credential() {
    let (good, evil) = lib_layers();
    let gets = Arc::new(AtomicI32::new(0));
    let (g, path) = (gets.clone(), format!("/blobs/{}", evil.digest));
    let reg = TestRegistry::start_with(hook(move |req| {
        if req.method == "GET" && req.path.ends_with(&path) {
            g.fetch_add(1, Ordering::SeqCst);
        }
        Action::Pass
    }))
    .await;
    reg.push_lying("test/evil:latest", &[evil], vec![good.diff_id.clone()]);
    let kc = Arc::new(StaticKeychain(vec![
        Credential::new("a", "a"),
        Credential::new("b", "b"),
    ]));
    let (_t, s) = fixture_store(StoreOptions::default().with_keychain(kc));
    let err = s
        .ensure_image(&Ctx::background(), &reg.reference("test/evil:latest"))
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::DiffIdMismatch), "{err}");
    assert_eq!(
        gets.load(Ordering::SeqCst),
        1,
        "lying blob downloaded more than once"
    );
}

/// A layer whose stream opens only once released, then fails with `err`.
struct GatedLayer {
    inner: MemLayer,
    started: Arc<tokio::sync::Notify>,
    started_flag: Arc<AtomicBool>,
    release: Arc<Gate>,
    err: Option<Error>,
}

impl GatedLayer {
    fn new(inner: MemLayer, err: Option<Error>) -> Self {
        GatedLayer {
            inner,
            started: Arc::new(tokio::sync::Notify::new()),
            started_flag: Arc::new(AtomicBool::new(false)),
            release: Arc::new(Gate {
                open: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            }),
            err,
        }
    }
}

#[async_trait::async_trait]
impl Layer for GatedLayer {
    fn digest(&self) -> Digest {
        self.inner.digest.clone()
    }
    async fn uncompressed(&self, ctx: &Ctx) -> Result<crate::layer::LayerReader> {
        self.started_flag.store(true, Ordering::SeqCst);
        self.started.notify_waiters();
        self.release.wait().await;
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.inner.uncompressed(ctx).await
    }
}

/// Counts opens of its stream.
struct OpenCountingLayer {
    inner: MemLayer,
    opens: Arc<AtomicI32>,
}

#[async_trait::async_trait]
impl Layer for OpenCountingLayer {
    fn digest(&self) -> Digest {
        self.inner.digest.clone()
    }
    async fn uncompressed(&self, ctx: &Ctx) -> Result<crate::layer::LayerReader> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.uncompressed(ctx).await
    }
}

// A lying pull holds its own flight, so an honest pull of the same diffID
// neither waits on it nor inherits its error.
#[tokio::test]
async fn lying_pull_does_not_fail_concurrent_honest_pull() {
    let (_t, s) = fixture_store(StoreOptions::default());
    let (good, evil) = lib_layers();
    let diff_id = good.diff_id.clone();
    let lying = Arc::new(GatedLayer::new(evil, None));
    let (s2, d2, l2) = (s.clone(), diff_id.clone(), lying.clone());
    let lying_task =
        tokio::spawn(async move { s2.ensure_layer(&Ctx::background(), &d2, l2).await });
    wait_for("the lying flight to open", || {
        lying.started_flag.load(Ordering::SeqCst)
    })
    .await;

    let dir = s
        .ensure_layer(&Ctx::background(), &diff_id, Arc::new(good))
        .await
        .unwrap();
    assert_eq!(read_lib(&dir), "good");
    lying.release.release();
    let err = lying_task.await.unwrap().unwrap_err();
    assert!(err.is(ErrorKind::DiffIdMismatch), "{err}");
    assert_eq!(read_lib(&dir), "good");
}

// Pulls of the same blob share one flight and its result.
#[tokio::test]
async fn same_blob_pulls_share_one_flight() {
    let (_t, s) = fixture_store(StoreOptions::default());
    let (good, _) = lib_layers();
    let diff_id = good.diff_id.clone();
    let leader = Arc::new(GatedLayer::new(
        good.clone(),
        Some(Error::msg("registry unavailable")),
    ));
    let (s1, d1, l1) = (s.clone(), diff_id.clone(), leader.clone());
    let leader_task =
        tokio::spawn(async move { s1.ensure_layer(&Ctx::background(), &d1, l1).await });
    wait_for("the leader's flight to open", || {
        leader.started_flag.load(Ordering::SeqCst)
    })
    .await;
    let opens = Arc::new(AtomicI32::new(0));
    let joiner = Arc::new(OpenCountingLayer {
        inner: good,
        opens: opens.clone(),
    });
    let (s2, d2) = (s.clone(), diff_id.clone());
    let joiner_task =
        tokio::spawn(async move { s2.ensure_layer(&Ctx::background(), &d2, joiner).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    leader.release.release();
    assert!(leader_task.await.unwrap().is_err());
    let err = joiner_task.await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("registry unavailable"),
        "joiner error = {err}"
    );
    assert_eq!(
        opens.load(Ordering::SeqCst),
        0,
        "joiner opened its own layer"
    );
}

// Bytes after the tar end marker are hashed but never land on disk.
#[tokio::test]
async fn recorded_size_excludes_trailing_bytes() {
    let tar = crate::testutil::image::build_tar(&[TarEntry::file("lib", "good")]);
    let mut padded = tar.clone();
    padded.extend(vec![0u8; 1 << 20]);
    let layer = MemLayer::from_raw(padded);
    let (_t, s) = fixture_store(StoreOptions::default());
    let dir = s
        .ensure_layer(&Ctx::background(), &layer.diff_id.clone(), Arc::new(layer))
        .await
        .unwrap();
    let size = crate::sizes::recorded_layer_size(&dir)
        .unwrap()
        .expect("a size file");
    assert!(
        size > 0 && size <= tar.len() as i64,
        "recorded size = {size}"
    );
}

// ---- mid-pull behavior ----

// A record removed mid-pull is rewritten: success implies the record
// exists and reaches the layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_rewrites_record_evicted_mid_pull() {
    let g = GatedRegistry::start().await;
    let (free, gated, _) = gated_test_layers();
    g.reg.push(
        "test/midpull:latest",
        ImageConfig::default(),
        &[free.clone(), gated.clone()],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let gate = g.gate(&gated);
    let (s, r) = (store.clone(), g.reg.reference("test/midpull:latest"));
    let task = tokio::spawn(async move { s.ensure_image(&Ctx::background(), &r).await });
    let free_dir = layer_dir_of(&store, &free);
    wait_for("free layer to land", || {
        free_dir.join(LAYER_FS_DIR_NAME).exists()
    })
    .await;
    for r in records(&store) {
        fs::remove_file(r).unwrap();
    }
    gate.release();
    let img = task.await.unwrap().unwrap();
    assert!(
        store.record_path(&img.digest).exists(),
        "layers stranded: record gone"
    );
    assert!(store.cached_image(&img.digest).unwrap().is_some());
}

// The per-layer progress touch keeps a slow pull's record fresh, and that
// freshness vetoes an eviction pass while the pull runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progress_touch_keeps_slow_pull_record_fresh() {
    let g = GatedRegistry::start().await;
    let (free, g1, g2) = gated_test_layers();
    g.reg.push(
        "test/slowpull:latest",
        ImageConfig::default(),
        &[free.clone(), g1.clone(), g2.clone()],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let (gate1, gate2) = (g.gate(&g1), g.gate(&g2));
    let (s, r) = (store.clone(), g.reg.reference("test/slowpull:latest"));
    let task = tokio::spawn(async move { s.ensure_image(&Ctx::background(), &r).await });
    let free_dir = layer_dir_of(&store, &free);
    wait_for("free layer to land", || {
        free_dir.join(LAYER_FS_DIR_NAME).exists()
    })
    .await;
    let rec = records(&store)
        .pop()
        .expect("no record mid-pull: record-first ordering broken");
    backdate(&rec, hours(3));
    gate1.release();
    wait_for("progress touch after a layer completes", || {
        fs::metadata(&rec)
            .ok()
            .and_then(|m| m.modified().ok())
            .is_some_and(|m| SystemTime::now().duration_since(m).unwrap_or_default() < hours(1))
    })
    .await;
    let (_, err) = store.evict_unused(&Ctx::background(), 1 << 62, false);
    assert!(err.is_none(), "{err:?}");
    assert!(rec.exists(), "freshly touched mid-pull record evicted");
    gate2.release();
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

// A layer dir yanked mid-pull fails the pull cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_reverify_fails_cleanly_on_yanked_layer() {
    let g = GatedRegistry::start().await;
    let (free, gated, _) = gated_test_layers();
    g.reg.push(
        "test/yank:latest",
        ImageConfig::default(),
        &[free.clone(), gated.clone()],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let gate = g.gate(&gated);
    let (s, r) = (store.clone(), g.reg.reference("test/yank:latest"));
    let task = tokio::spawn(async move { s.ensure_image(&Ctx::background(), &r).await });
    let free_dir = layer_dir_of(&store, &free);
    wait_for("free layer to land", || {
        free_dir.join(LAYER_FS_DIR_NAME).exists()
    })
    .await;
    remove_all_writable(&free_dir).unwrap();
    gate.release();
    let err = task.await.unwrap().unwrap_err();
    assert!(err.to_string().contains("vanished during pull"), "{err}");
}

// A mid-flight pull holds a valid partial record, and a caller that stops
// waiting does not interrupt the detached pull.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_pull_leaves_resumable_record() {
    let g = GatedRegistry::start().await;
    let (free, gated, _) = gated_test_layers();
    g.reg.push(
        "test/interrupted:latest",
        ImageConfig::default(),
        &[free.clone(), gated.clone()],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let gate = g.gate(&gated);
    let ctx = Ctx::background();
    let (s, r, c) = (
        store.clone(),
        g.reg.reference("test/interrupted:latest"),
        ctx.clone(),
    );
    let task = tokio::spawn(async move { s.ensure_image(&c, &r).await });
    let free_dir = layer_dir_of(&store, &free);
    wait_for("free layer to land", || {
        free_dir.join(LAYER_FS_DIR_NAME).exists()
    })
    .await;
    ctx.cancel();
    let err = task.await.unwrap().unwrap_err();
    assert!(err.is(ErrorKind::Canceled), "{err}");

    let recs = records(&store);
    assert!(!recs.is_empty(), "interrupted pull left no record");
    let rec: ImageRecord = serde_json::from_slice(&fs::read(&recs[0]).unwrap()).unwrap();
    assert_eq!(
        rec.diff_ids.len(),
        2,
        "record must reference the full image"
    );
    assert!(free_dir.join(LAYER_FS_DIR_NAME).exists());
    assert!(!layer_dir_of(&store, &gated).exists());

    // A retry joins the still-running flight.
    gate.release();
    let img = must_ensure(&store, &g.reg.reference("test/interrupted:latest")).await;
    assert!(store.cached_image(&img.digest).unwrap().is_some());
}

// The pull timeout bounds a detached pull.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_pull_bounded_by_pull_timeout() {
    let g = GatedRegistry::start().await;
    let (free, gated, _) = gated_test_layers();
    g.reg.push(
        "test/wedged:latest",
        ImageConfig::default(),
        &[free, gated.clone()],
    );
    let (_t, store) =
        fixture_store(StoreOptions::default().with_pull_timeout(Duration::from_millis(200)));
    let _gate = g.gate(&gated);
    let err = store
        .ensure_image(&Ctx::background(), &g.reg.reference("test/wedged:latest"))
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::DeadlineExceeded), "{err}");
}

// ---- pins ----

// Layers a pull already landed survive an eviction pass that runs while it
// downloads the rest, even one removing its record as wedged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_pins_layers_against_eviction() {
    let g = GatedRegistry::start().await;
    let (free, gated, _) = gated_test_layers();
    g.reg.push(
        "test/pinned:latest",
        ImageConfig::default(),
        &[free.clone(), gated.clone()],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let gate = g.gate(&gated);
    let (s, r) = (store.clone(), g.reg.reference("test/pinned:latest"));
    let task = tokio::spawn(async move { s.ensure_image(&Ctx::background(), &r).await });
    let free_dir = layer_dir_of(&store, &free);
    wait_for("free layer to land", || {
        free_dir.join(LAYER_FS_DIR_NAME).exists()
    })
    .await;

    let mut dry = crate::gc::EvictStats::default();
    let mut stats = crate::gc::EvictStats::default();
    let st = store.clone();
    wait_for("the aged record to reach retirement", || {
        backdate_store(&st, hours(3));
        let (d, e1) = st.evict_unused(&Ctx::background(), i64::MAX, true);
        assert!(e1.is_none(), "{e1:?}");
        let (s2, e2) = st.evict_unused(&Ctx::background(), i64::MAX, false);
        assert!(e2.is_none(), "{e2:?}");
        dry = d;
        stats = s2;
        dry.skipped_fresh == 0 && stats.skipped_fresh == 0
    })
    .await;
    assert_eq!(
        (dry.freed_bytes, dry.evicted_layers),
        (0, 0),
        "dry run counted the pinned layer"
    );
    assert_eq!(stats.candidates, 1);
    assert!(
        free_dir.join(LAYER_FS_DIR_NAME).exists(),
        "pinned layer was retired mid-pull"
    );
    assert_eq!(stats.evicted_layers, 0);

    gate.release();
    let img = task.await.unwrap().unwrap();
    for d in &img.layer_dirs {
        assert!(d.join(LAYER_FS_DIR_NAME).exists());
    }
    assert!(!store.pinned(&free.diff_id.hex), "pin outlived its pull");
}

// A pull pins before entering the layer flight, so a joiner is protected
// once the leader's flight returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_pins_before_joining_layer_flight() {
    let reg = TestRegistry::start().await;
    let (_t, store) = fixture_store(StoreOptions::default());
    let layer = mem_layer(&[TarEntry::file("f", &"p".repeat(512))]);
    reg.push(
        "test/pin-before-join:latest",
        ImageConfig::default(),
        std::slice::from_ref(&layer),
    );
    let gate = Arc::new(Gate {
        open: AtomicBool::new(false),
        notify: tokio::sync::Notify::new(),
    });
    let held = Arc::new(AtomicBool::new(false));
    let key = layer_flight_key(&layer.diff_id, &layer.digest);
    let (s, l, g, h) = (store.clone(), layer.clone(), gate.clone(), held.clone());
    let lead = store.layer_flights.run(&key, move || async move {
        h.store(true, Ordering::SeqCst);
        g.wait().await;
        s.unpack_layer_to_pool(&Ctx::background(), &l.diff_id.clone(), &l)
            .await
    });
    wait_for("the flight to be held", || held.load(Ordering::SeqCst)).await;

    let (s, r) = (store.clone(), reg.reference("test/pin-before-join:latest"));
    let task = tokio::spawn(async move { s.ensure_image(&Ctx::background(), &r).await });
    wait_for("the pull to pin its layer", || {
        store.pinned(&layer.diff_id.hex)
    })
    .await;
    assert!(
        !task.is_finished(),
        "pull returned while its layer flight was held"
    );
    gate.release();
    lead.await.unwrap();
    task.await.unwrap().unwrap();
    assert!(!store.pinned(&layer.diff_id.hex), "pin outlived its pull");
}

// ---- metrics ----

fn metered_store() -> (tempfile::TempDir, Store, prometheus::Registry) {
    let registry = prometheus::Registry::new();
    let m = crate::metrics::RequestMetrics::register(&registry).unwrap();
    let (t, s) = fixture_store(StoreOptions::default().with_metrics(m));
    (t, s, registry)
}

fn collect_requests(registry: &prometheus::Registry) -> HashMap<(String, String), u64> {
    let mut out = HashMap::new();
    for mf in registry.gather() {
        if mf.name() != crate::metrics::REQUESTS_METRIC {
            continue;
        }
        assert_eq!(mf.get_field_type(), prometheus::proto::MetricType::COUNTER);
        for m in mf.get_metric() {
            let label = |n: &str| {
                m.get_label()
                    .iter()
                    .find(|l| l.name() == n)
                    .map(|l| l.value().to_string())
                    .unwrap_or_default()
            };
            *out.entry((label("outcome"), label("error_type")))
                .or_insert(0) += m.get_counter().value() as u64;
        }
    }
    out
}

fn series(o: &str, e: &str) -> (String, String) {
    (o.to_string(), e.to_string())
}

#[tokio::test]
async fn requests_counts_miss_then_hit() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/metrics:latest", "app/main", "main");
    let (_t, s, registry) = metered_store();
    for _ in 0..3 {
        must_ensure(&s, &r).await;
    }
    let got = collect_requests(&registry);
    assert_eq!(got.get(&series("miss", "")), Some(&1), "{got:?}");
    assert_eq!(got.get(&series("hit", "")), Some(&2), "{got:?}");
    assert_eq!(got.len(), 2, "{got:?}");
}

#[tokio::test]
async fn requests_records_failures() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/fails:latest", "app/main", "main");
    let cancelled = Ctx::background();
    cancelled.cancel();
    let expired = Ctx::background().with_timeout(Duration::ZERO);
    for (name, reference, ctx, want) in [
        (
            "unparseable ref",
            "NOT A REFERENCE".to_string(),
            Ctx::background(),
            series("error", "_OTHER"),
        ),
        (
            "no such tag",
            reg.reference("test/fails:absent"),
            Ctx::background(),
            series("error", "404"),
        ),
        (
            "caller gave up",
            r.clone(),
            cancelled,
            series("cancelled", ""),
        ),
        (
            "caller ran out of time",
            r.clone(),
            expired,
            series("timeout", ""),
        ),
    ] {
        let (_t, s, registry) = metered_store();
        assert!(
            s.ensure_image(&ctx, &reference).await.is_err(),
            "{name}: succeeded"
        );
        let got = collect_requests(&registry);
        assert_eq!(got.get(&want), Some(&1), "{name}: {got:?}");
    }
}

#[tokio::test]
async fn requests_without_meter_is_no_op() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/nometer:latest", "app/main", "main");
    let (_t, s) = fixture_store(StoreOptions::default());
    must_ensure(&s, &r).await;
}

#[test]
fn layer_tar_helpers_cover_devices() {
    // The fixture writer emits device entries the unpacker must skip.
    let t =
        crate::testutil::image::build_tar(&[TarEntry::new("dev/null", EntryType::Char).dev(1, 3)]);
    assert!(t.len() >= 1024);
}
