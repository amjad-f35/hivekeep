use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use hivekeep_node::context::Ctx;

use super::*;
use crate::manifest::ImageConfig;
use crate::spec::{write_spec, ImageVolumeOverlay, OVERLAY_SPEC_FILE_NAME};
use crate::store::tests::{
    backdate, backdate_store, fixture_store, hours, layer_dirs_on_disk, must_ensure, one_file_image,
};
use crate::store::StoreOptions;
use crate::testutil::image::{mem_layer, MemLayer, TarEntry};
use crate::testutil::registry::TestRegistry;

fn evict(store: &Store, target: i64, dry: bool) -> EvictStats {
    let (stats, err) = store.evict_unused(&Ctx::background(), target, dry);
    assert!(err.is_none(), "evict_unused: {err:?}");
    stats
}

fn layer(name: &str, c: char, n: usize) -> MemLayer {
    mem_layer(&[TarEntry::file(name, &c.to_string().repeat(n)).mode(0o644)])
}

fn spec_in(actors: &Path, actor: &str, spec: &mut OverlaySpec) -> std::path::PathBuf {
    let bundle = actors.join(actor).join("bundles").join("main");
    fs::create_dir_all(&bundle).unwrap();
    write_spec(&bundle, spec).unwrap();
    bundle
}

fn strings(dirs: &[std::path::PathBuf]) -> Vec<String> {
    dirs.iter()
        .map(|d| d.to_string_lossy().into_owned())
        .collect()
}

fn is_root() -> bool {
    // SAFETY: no preconditions.
    unsafe { libc::geteuid() == 0 }
}

/// Removes all permissions, restoring them on drop.
struct Denied(std::path::PathBuf, u32);

impl Denied {
    fn new(p: &Path) -> Self {
        let mode = fs::metadata(p).unwrap().permissions().mode() & 0o7777;
        fs::set_permissions(p, fs::Permissions::from_mode(0o000)).unwrap();
        Denied(p.to_path_buf(), mode)
    }
}

impl Drop for Denied {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(self.1));
    }
}

#[tokio::test]
async fn evict_unused_min_age_veto() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/fresh:latest", "f", "hi");
    let (_t, store) = fixture_store(StoreOptions::default());
    must_ensure(&store, &r).await;
    let stats = evict(&store, i64::MAX, false);
    assert_eq!(
        (stats.evicted_images, stats.evicted_layers),
        (0, 0),
        "{stats:?}"
    );
    assert!(!layer_dirs_on_disk(&store).is_empty());
}

#[tokio::test]
async fn evict_unused_lru_shared_layers_and_repull() {
    let reg = TestRegistry::start().await;
    let shared = layer("shared", 's', 2048);
    reg.push(
        "test/a:latest",
        ImageConfig::default(),
        &[shared.clone(), layer("a", 'a', 2048)],
    );
    reg.push(
        "test/b:latest",
        ImageConfig::default(),
        &[shared, layer("b", 'b', 2048)],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let a = must_ensure(&store, &reg.reference("test/a:latest")).await;
    let b = must_ensure(&store, &reg.reference("test/b:latest")).await;
    assert_eq!(a.layer_dirs[0], b.layer_dirs[0]);
    backdate_store(&store, hours(3));
    backdate(&store.record_path(&a.digest), hours(4));

    // A small target: only the LRU image goes; the shared layer survives.
    let stats = evict(&store, 1, false);
    assert_eq!(stats.evicted_images, 1, "{stats:?}");
    assert!(stats.freed_bytes > 0);
    assert!(!store.record_path(&a.digest).exists());
    assert!(store.record_path(&b.digest).exists());
    assert!(!a.layer_dirs[1].exists(), "A's private layer not evicted");
    assert!(
        a.layer_dirs[0].exists(),
        "shared layer evicted while B references it"
    );

    evict(&store, i64::MAX, false);
    assert!(layer_dirs_on_disk(&store).is_empty());
    let a2 = must_ensure(&store, &reg.reference("test/a:latest")).await;
    assert!(a2.layer_dirs[1].join("fs/a").exists());
}

#[tokio::test]
async fn evict_unused_root_set() {
    let reg = TestRegistry::start().await;
    let refs = [
        "test/rooted:latest",
        "test/layer-rooted:latest",
        "test/loose:latest",
    ];
    for r in refs {
        let name = format!("f-{}", &r[r.len() - 8..]);
        reg.push(
            r,
            ImageConfig::default(),
            &[mem_layer(&[TarEntry::file(&name, r)])],
        );
    }
    let actors = tempfile::tempdir().unwrap();
    let (_t, store) = fixture_store(StoreOptions::default().with_actors_dir(actors.path()));
    let rooted = must_ensure(&store, &reg.reference(refs[0])).await;
    let layer_rooted = must_ensure(&store, &reg.reference(refs[1])).await;
    let loose = must_ensure(&store, &reg.reference(refs[2])).await;

    spec_in(
        actors.path(),
        "actor-1",
        &mut OverlaySpec {
            image_digest: rooted.digest.to_string(),
            layers: strings(&rooted.layer_dirs),
            ..Default::default()
        },
    );
    spec_in(
        actors.path(),
        "actor-2",
        &mut OverlaySpec {
            layers: strings(&layer_rooted.layer_dirs),
            ..Default::default()
        },
    );
    let rs = store.in_use().unwrap();
    assert!(rs.image_digests.contains(&rooted.digest.to_string()));
    let hex = layer_rooted.layer_dirs[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(rs.layer_hexes.contains(&hex));

    backdate_store(&store, hours(3));
    let stats = evict(&store, i64::MAX, false);
    assert!(store.record_path(&rooted.digest).exists());
    assert!(rooted.layer_dirs[0].exists());
    // A record whose exact layer set a spec roots is itself rooted.
    assert!(layer_rooted.layer_dirs[0].exists());
    assert!(store.record_path(&layer_rooted.digest).exists());
    assert!(stats.rooted_images >= 2, "{stats:?}");
    assert!(!store.record_path(&loose.digest).exists());
    assert!(!loose.layer_dirs[0].exists());
}

// Image volumes root their images like the rootfs does.
#[tokio::test]
async fn evict_unused_root_set_image_volumes() {
    let reg = TestRegistry::start().await;
    for r in ["test/rootfs:latest", "test/volume:latest"] {
        let name = format!("f-{}", &r[r.len() - 8..]);
        reg.push(
            r,
            ImageConfig::default(),
            &[mem_layer(&[TarEntry::file(&name, r)])],
        );
    }
    let actors = tempfile::tempdir().unwrap();
    let (_t, store) = fixture_store(StoreOptions::default().with_actors_dir(actors.path()));
    let rootfs = must_ensure(&store, &reg.reference("test/rootfs:latest")).await;
    let volume = must_ensure(&store, &reg.reference("test/volume:latest")).await;
    spec_in(
        actors.path(),
        "actor-1",
        &mut OverlaySpec {
            image_digest: rootfs.digest.to_string(),
            layers: strings(&rootfs.layer_dirs),
            image_volumes: vec![ImageVolumeOverlay {
                name: "agent".into(),
                image_digest: volume.digest.to_string(),
                layers: strings(&volume.layer_dirs),
            }],
            ..Default::default()
        },
    );
    let rs = store.in_use().unwrap();
    assert!(rs.image_digests.contains(&volume.digest.to_string()));
    assert!(rs.layer_hexes.contains(
        &volume.layer_dirs[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    ));
    backdate_store(&store, hours(3));
    evict(&store, i64::MAX, false);
    assert!(store.record_path(&volume.digest).exists());
    assert!(volume.layer_dirs[0].exists());
}

// An unreadable record gates the whole pass.
#[tokio::test]
async fn evict_unused_skips_pass_on_unreadable_record() {
    if is_root() {
        eprintln!("skipped: chmod 0000 does not block reads for root");
        return;
    }
    let reg = TestRegistry::start().await;
    let shared = layer("shared", 's', 2048);
    reg.push(
        "test/unreadable-a:latest",
        ImageConfig::default(),
        &[shared.clone(), layer("a", 'a', 2048)],
    );
    reg.push(
        "test/unreadable-b:latest",
        ImageConfig::default(),
        &[shared, layer("b", 'b', 2048)],
    );
    let (_t, store) = fixture_store(StoreOptions::default());
    let a = must_ensure(&store, &reg.reference("test/unreadable-a:latest")).await;
    let b = must_ensure(&store, &reg.reference("test/unreadable-b:latest")).await;
    backdate_store(&store, hours(3));
    let _deny = Denied::new(&store.record_path(&a.digest));
    let (stats, err) = store.evict_unused(&Ctx::background(), i64::MAX, false);
    assert!(err.is_some(), "no error on an unreadable record");
    assert_eq!(
        (
            stats.evicted_images,
            stats.evicted_layers,
            stats.freed_bytes
        ),
        (0, 0, 0)
    );
    assert!(store.record_path(&b.digest).exists());
    assert!(
        b.layer_dirs[0].exists(),
        "shared layer retired under an unreadable record"
    );
}

// An unreadable actors dir, bundles dir or spec gates the pass.
#[tokio::test]
async fn evict_unused_skips_pass_on_unreadable_roots() {
    if is_root() {
        eprintln!("skipped: chmod 0000 does not block reads for root");
        return;
    }
    for case in [
        "actors dir unreadable",
        "bundles dir unreadable",
        "bundle spec unreadable",
    ] {
        let reg = TestRegistry::start().await;
        let r = one_file_image(&reg, "test/rootgate:latest", "f", &"g".repeat(1024));
        let actors = tempfile::tempdir().unwrap();
        let (_t, store) = fixture_store(StoreOptions::default().with_actors_dir(actors.path()));
        let img = must_ensure(&store, &r).await;
        let bundle = spec_in(
            actors.path(),
            "actor-1",
            &mut OverlaySpec {
                image_digest: img.digest.to_string(),
                layers: strings(&img.layer_dirs),
                ..Default::default()
            },
        );
        backdate_store(&store, hours(3));
        let target = match case {
            "actors dir unreadable" => actors.path().to_path_buf(),
            "bundles dir unreadable" => actors.path().join("actor-1/bundles"),
            _ => bundle.join(OVERLAY_SPEC_FILE_NAME),
        };
        let deny = Denied::new(&target);
        let (stats, err) = store.evict_unused(&Ctx::background(), i64::MAX, false);
        drop(deny);
        let err = err.unwrap_or_else(|| panic!("{case}: no error"));
        assert!(err.is(ErrorKind::IncompleteEnumeration), "{case}: {err}");
        assert_eq!(
            (stats.evicted_images, stats.evicted_layers),
            (0, 0),
            "{case}"
        );
        assert!(store.record_path(&img.digest).exists(), "{case}");
        assert!(img.layer_dirs[0].exists(), "{case}");
    }
}

// A record whose references cannot be established gates the pass and
// survives it.
#[test]
fn evict_unused_skips_pass_on_bad_record() {
    for (case, bad) in [
        ("undecodable JSON", "{not json"),
        (
            "garbled diffID",
            r#"{"version":1,"diffIDs":["not-a-hash"]}"#,
        ),
    ] {
        let (_t, store) = fixture_store(StoreOptions::default());
        let layer = store.layers_dir().join("cc".repeat(32));
        fs::create_dir_all(layer.join("fs")).unwrap();
        let bad_path = store.record_path(&Digest::sha256("dd".repeat(32)));
        fs::write(&bad_path, bad).unwrap();
        backdate_store(&store, hours(3));
        let (stats, err) = store.evict_unused(&Ctx::background(), i64::MAX, false);
        let err = err.unwrap_or_else(|| panic!("{case}: no error"));
        assert!(err.is(ErrorKind::IncompleteEnumeration), "{case}: {err}");
        assert_eq!(
            (stats.evicted_images, stats.evicted_layers),
            (0, 0),
            "{case}"
        );
        assert!(bad_path.exists(), "{case}");
        assert!(layer.exists(), "{case}");
    }
}

// Dry runs write nothing, not even a size-file backfill.
#[tokio::test]
async fn evict_unused_dry_run_does_not_backfill_sizes() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/nobackfill:latest", "f", &"n".repeat(1024));
    let (_t, store) = fixture_store(StoreOptions::default());
    let img = must_ensure(&store, &r).await;
    let size_path = img.layer_dirs[0].join(crate::store::LAYER_SIZE_FILE_NAME);
    fs::remove_file(&size_path).unwrap();
    backdate_store(&store, hours(3));
    let stats = evict(&store, i64::MAX, true);
    assert!(stats.freed_bytes > 0, "{stats:?}");
    assert!(!size_path.exists(), "dry run wrote a size file");
    assert!(img.layer_dirs[0].exists());
}

#[tokio::test]
async fn evict_unused_dry_run() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/dryrun:latest", "f", &"d".repeat(1024));
    let (_t, store) = fixture_store(StoreOptions::default());
    let img = must_ensure(&store, &r).await;
    backdate_store(&store, hours(3));
    let stats = evict(&store, i64::MAX, true);
    assert_eq!(
        (stats.evicted_images, stats.evicted_layers),
        (1, 1),
        "{stats:?}"
    );
    assert!(stats.freed_bytes > 0);
    assert!(store.record_path(&img.digest).exists());
    assert!(img.layer_dirs[0].exists());
}

// Cache hits, re-pulls and free-everything passes race; a returned image
// always references live layers and a fresh record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ensure_image_and_evict() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/race:latest", "f", &"r".repeat(512));
    let (_t, store) = fixture_store(StoreOptions::default());
    let digest_ref = reg.reference(&format!(
        "test/race@{}",
        must_ensure(&store, &r).await.digest
    ));
    for i in 0..25 {
        backdate_store(&store, hours(3));
        let (s1, s2, dr) = (store.clone(), store.clone(), digest_ref.clone());
        let ensure = tokio::spawn(async move { s1.ensure_image(&Ctx::background(), &dr).await });
        let evictor = tokio::task::spawn_blocking(move || {
            s2.evict_unused(&Ctx::background(), i64::MAX, false).1
        });
        let img = ensure
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("iteration {i}: {e}"));
        assert!(evictor.await.unwrap().is_none());
        for d in &img.layer_dirs {
            assert!(
                d.join("fs/f").exists(),
                "iteration {i}: returned layer dir unusable"
            );
        }
        let m = fs::metadata(store.record_path(&img.digest))
            .unwrap()
            .modified()
            .unwrap();
        assert!(
            std::time::SystemTime::now()
                .duration_since(m)
                .unwrap_or_default()
                < Duration::from_secs(60),
            "iteration {i}: returned image's record is stale"
        );
    }
}

// ---- damaged and in-between pool states ----

// A layer with no record is crash debris: reclaimed at startup, never by a
// periodic pass.
#[tokio::test]
async fn orphan_layer_reclaimed_at_startup() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/orphan:latest", "f", &"o".repeat(4096));
    let (t, store) = fixture_store(StoreOptions::default());
    let img = must_ensure(&store, &r).await;
    fs::remove_file(store.record_path(&img.digest)).unwrap();
    backdate_store(&store, hours(3));
    evict(&store, i64::MAX, false);
    assert!(img.layer_dirs[0].exists(), "periodic pass swept an orphan");
    let reopened = Store::open(t.path(), StoreOptions::default()).unwrap();
    assert!(layer_dirs_on_disk(&reopened).is_empty());
    assert_eq!(reopened.cache_size().unwrap(), 0);
}

// A digestless spec keeps its record alive by the exact-set rule, so record
// and layers go together once the bundle does.
#[tokio::test]
async fn digestless_spec_layers_reclaimed_after_bundle_gone() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/upgrade:latest", "f", &"u".repeat(4096));
    let actors = tempfile::tempdir().unwrap();
    let (_t, store) = fixture_store(StoreOptions::default().with_actors_dir(actors.path()));
    let img = must_ensure(&store, &r).await;
    spec_in(
        actors.path(),
        "actor-old",
        &mut OverlaySpec {
            layers: strings(&img.layer_dirs),
            ..Default::default()
        },
    );
    backdate_store(&store, hours(3));
    evict(&store, i64::MAX, false);
    assert!(img.layer_dirs[0].exists());
    assert!(store.record_path(&img.digest).exists());
    fs::remove_dir_all(actors.path().join("actor-old")).unwrap();
    backdate_store(&store, hours(3));
    evict(&store, i64::MAX, false);
    assert!(layer_dirs_on_disk(&store).is_empty());
}

// A fresh record protects even an old layer: pulls write records first.
#[test]
fn record_first_protects_in_flight_pull() {
    let (_t, store) = fixture_store(StoreOptions::default());
    let diff_id = Digest::sha256("5c".repeat(32));
    let pending = Digest::sha256("6d".repeat(32));
    let digest = Digest::sha256("7e".repeat(32));
    let dir = store.layer_dir(&diff_id);
    fs::create_dir_all(dir.join("fs")).unwrap();
    store
        .write_record(
            &digest,
            &ImageRecord {
                version: 1,
                diff_ids: vec![diff_id.to_string(), pending.to_string()],
                ..Default::default()
            },
        )
        .unwrap();
    backdate(&dir, hours(3));
    evict(&store, i64::MAX, false);
    assert!(dir.exists());
    assert!(store.record_path(&digest).exists());
}

// A wedged pull's stale record is restored while its fresh layer is kept,
// and both go together once the layer ages.
#[test]
fn wedged_pull_fresh_layers_not_stranded() {
    let (_t, store) = fixture_store(StoreOptions::default());
    let diff_id = Digest::sha256("5c".repeat(32));
    let digest = Digest::sha256("7e".repeat(32));
    let dir = store.layer_dir(&diff_id);
    fs::create_dir_all(dir.join("fs")).unwrap();
    store
        .write_record(
            &digest,
            &ImageRecord {
                version: 1,
                diff_ids: vec![diff_id.to_string()],
                ..Default::default()
            },
        )
        .unwrap();
    backdate(&store.record_path(&digest), hours(3));
    for _ in 0..4 {
        evict(&store, i64::MAX, false);
    }
    assert!(dir.exists(), "fresh partial layer evicted");
    assert!(
        store.record_path(&digest).exists(),
        "record gone while its fresh layer survives"
    );
    backdate(&store.record_path(&digest), hours(3));
    backdate(&dir, hours(3));
    evict(&store, i64::MAX, false);
    assert!(!store.record_path(&digest).exists());
    assert!(!dir.exists());
}

// A record whose deletion would strand a rooted layer is restored.
#[test]
fn eviction_restores_record_when_layer_rooted() {
    let actors = tempfile::tempdir().unwrap();
    let (_t, store) = fixture_store(StoreOptions::default().with_actors_dir(actors.path()));
    let shared = Digest::sha256("1a".repeat(32));
    let private = Digest::sha256("2b".repeat(32));
    let digest = Digest::sha256("3c".repeat(32));
    for d in [&shared, &private] {
        fs::create_dir_all(store.layer_dir(d).join("fs")).unwrap();
    }
    store
        .write_record(
            &digest,
            &ImageRecord {
                version: 1,
                diff_ids: vec![shared.to_string(), private.to_string()],
                ..Default::default()
            },
        )
        .unwrap();
    spec_in(
        actors.path(),
        "actor-x",
        &mut OverlaySpec {
            layers: vec![store.layer_dir(&shared).to_string_lossy().into_owned()],
            ..Default::default()
        },
    );
    backdate_store(&store, hours(3));
    evict(&store, i64::MAX, false);
    assert!(store.layer_dir(&shared).exists(), "rooted layer evicted");
    assert!(
        store.record_path(&digest).exists(),
        "record gone while its rooted layer survives"
    );
}

// The startup scan skips itself when any record fails to read, while open
// still succeeds.
#[tokio::test]
async fn startup_orphan_scan_skipped_when_enumeration_incomplete() {
    let reg = TestRegistry::start().await;
    let r = one_file_image(&reg, "test/enum:latest", "f", &"e".repeat(2048));
    let (t, store) = fixture_store(StoreOptions::default());
    let img = must_ensure(&store, &r).await;
    backdate_store(&store, hours(3));
    let orphan = store.layers_dir().join("aa".repeat(32));
    fs::create_dir_all(orphan.join("fs")).unwrap();
    backdate(&orphan, hours(3));
    fs::write(
        store.record_path(&Digest::sha256("bb".repeat(32))),
        "{not json",
    )
    .unwrap();
    Store::open(t.path(), StoreOptions::default()).expect("open must survive a corrupt record");
    assert!(orphan.exists(), "scan ran on an incomplete enumeration");
    for d in &img.layer_dirs {
        assert!(d.exists());
    }
}

// Non-digest dir names are never treated as layers.
#[test]
fn non_layer_dirs_ignored() {
    let (t, store) = fixture_store(StoreOptions::default().with_min_age(Duration::ZERO));
    let junk = store.layers_dir().join("notadigest");
    fs::create_dir_all(&junk).unwrap();
    let stats = evict(&store, i64::MAX, false);
    assert_eq!(stats.orphan_layers, 0);
    Store::open(
        t.path(),
        StoreOptions::default().with_min_age(Duration::ZERO),
    )
    .unwrap();
    assert!(junk.exists());
}

// A pull failing partway leaves a record, never unexplained layers.
#[tokio::test]
async fn failed_pull_leaves_resumable_record_not_orphans() {
    let reg = TestRegistry::start().await;
    let good = layer("ok", 'g', 2048);
    let bad = MemLayer::from_raw(b"this is not a tar archive at all".to_vec());
    reg.push("test/badlayer:latest", ImageConfig::default(), &[good, bad]);
    let (t, store) = fixture_store(StoreOptions::default());
    assert!(store
        .ensure_image(&Ctx::background(), &reg.reference("test/badlayer:latest"))
        .await
        .is_err());
    let recs: Vec<_> = fs::read_dir(store.manifests_dir())
        .unwrap()
        .flatten()
        .collect();
    assert!(!recs.is_empty(), "failed pull left no record");
    backdate_store(&store, hours(3));
    for r in &recs {
        backdate(&r.path(), Duration::ZERO);
    }
    let before = layer_dirs_on_disk(&store);
    let reopened = Store::open(t.path(), StoreOptions::default()).unwrap();
    assert_eq!(layer_dirs_on_disk(&reopened).len(), before.len());
}
