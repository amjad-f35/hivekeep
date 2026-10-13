//! Populating, refreshing and fencing system-info volumes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;

use super::*;
use crate::trustbundle::tests::pem;
use crate::trustbundle::{sanitize_certificate_bundle, EGRESS_CTB, EGRESS_NAME};

/// A throwaway certificate in canonical PEM form, so a one-certificate
/// bundle projects byte for byte.
fn cert() -> String {
    let body: Vec<u8> = (0..200).map(|_| crate::rand::u64() as u8).collect();
    String::from_utf8(sanitize_certificate_bundle(pem(&body).as_bytes(), 0).unwrap()).unwrap()
}

/// Stands in for the cluster's trust bundle objects.
#[derive(Clone, Default)]
struct CtbStore(Arc<Mutex<HashMap<String, String>>>);

impl CtbStore {
    fn set(&self, raw: &str) {
        self.0.lock().insert(EGRESS_CTB.into(), raw.into());
    }
    fn remove(&self) {
        self.0.lock().remove(EGRESS_CTB);
    }
    fn source(&self) -> Source {
        let m = self.0.clone();
        Source::new(
            Arc::new(move |name: &str| Ok(m.lock().get(name).cloned())),
            Vec::new(),
        )
    }
    fn refresher(&self) -> SystemInfoRefresher {
        SystemInfoRefresher::new(self.source())
    }
}

fn store_with(raw: &str) -> CtbStore {
    let s = CtbStore::default();
    s.set(raw);
    s
}

fn trust_spec(rel: &str) -> pb::SystemInfoVolumeSource {
    pb::SystemInfoVolumeSource {
        data_sources: vec![pb::SystemInfoDataSource {
            actor_metadata: None,
            trust_bundle: Some(pb::TrustBundleDataSource {
                names: vec![EGRESS_NAME.into()],
                path: rel.into(),
            }),
        }],
    }
}

fn metadata_spec() -> pb::SystemInfoVolumeSource {
    let item = |field: pb::ActorMetadataField, path: &str| pb::ActorMetadataItem {
        field: field as i32,
        path: path.into(),
    };
    pb::SystemInfoVolumeSource {
        data_sources: vec![pb::SystemInfoDataSource {
            actor_metadata: Some(pb::ActorMetadataDataSource {
                items: vec![
                    item(pb::ActorMetadataField::Name, "actor-name"),
                    item(pb::ActorMetadataField::Hive, "hive"),
                    item(pb::ActorMetadataField::Uid, "identity/actor-uid"),
                ],
            }),
            trust_bundle: None,
        }],
    }
}

fn trust_volume(dir: &Path, uid: &str) -> SystemInfoVolume {
    SystemInfoVolume {
        name: "trust".into(),
        root: dir.join(uid).join("system-info/trust"),
        spec: trust_spec("ca.pem"),
    }
}

/// Registers `uid` with one volume projecting the egress bundle at
/// `<dir>/<uid>/system-info/trust/ca.pem`.
fn register_trust(r: &SystemInfoRefresher, dir: &Path, uid: &str) -> Arc<RegisteredActor> {
    r.register(
        uid,
        &ActorRef::new("team-a", uid),
        vec![trust_volume(dir, uid)],
    )
    .unwrap_or_else(|e| panic!("register {uid}: {e}"))
}

fn projected_path(dir: &Path, uid: &str) -> PathBuf {
    dir.join(uid).join("system-info/trust/ca.pem")
}

fn read_projected(dir: &Path, uid: &str) -> String {
    std::fs::read_to_string(projected_path(dir, uid)).unwrap()
}

#[cfg(unix)]
fn inode(p: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(p).unwrap().ino()
}

#[cfg(unix)]
fn chmod(p: &Path, mode: u32) {
    crate::atomicfile::chmod(p, mode).unwrap();
}

fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(unix)]
#[test]
fn refreshes_running_actors_on_change() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    for uid in ["uid-1", "uid-2"] {
        register_trust(&r, d.path(), uid);
        assert_eq!(read_projected(d.path(), uid), a, "{uid}");
    }
    store.set(&b);
    r.refresh_bundle(EGRESS_NAME).unwrap();
    for uid in ["uid-1", "uid-2"] {
        assert_eq!(read_projected(d.path(), uid), b, "{uid}");
    }
    // A replay with unchanged contents must not replace the file.
    let before = inode(&projected_path(d.path(), "uid-1"));
    r.refresh_bundle(EGRESS_NAME).unwrap();
    assert_eq!(inode(&projected_path(d.path(), "uid-1")), before);
}

#[test]
fn keeps_last_good_on_failure() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    register_trust(&r, d.path(), "uid-1");

    // Backing object deleted: no error (the next change retries, not the
    // queue), and the last good bundle survives.
    store.remove();
    assert!(r.refresh_bundle(EGRESS_NAME).is_ok());
    assert_eq!(read_projected(d.path(), "uid-1"), a);

    // Backing object unusable.
    store.set("no certificates here");
    let _ = r.refresh_bundle(EGRESS_NAME);
    assert_eq!(read_projected(d.path(), "uid-1"), a);

    // Recovers when the bundle heals.
    store.set(&b);
    r.refresh_bundle(EGRESS_NAME).unwrap();
    assert_eq!(read_projected(d.path(), "uid-1"), b);
}

#[test]
fn deregister_stops_refreshing() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    register_trust(&r, d.path(), "uid-1");
    r.deregister("uid-1");
    store.set(&b);
    let _ = r.refresh_bundle(EGRESS_NAME);
    assert_eq!(read_projected(d.path(), "uid-1"), a);
}

#[test]
fn register_empty_stops_refreshing() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    register_trust(&r, d.path(), "uid-1");
    // Back under a spec with no system-info volumes: still tracked, but its
    // former volumes stop refreshing.
    r.deregister("uid-1");
    r.register("uid-1", &ActorRef::new("team-a", "uid-1"), Vec::new())
        .unwrap();
    assert!(
        r.is_registered("uid-1"),
        "every running actor stays tracked"
    );
    store.set(&b);
    let _ = r.refresh_bundle(EGRESS_NAME);
    assert_eq!(read_projected(d.path(), "uid-1"), a);
}

#[test]
fn register_rewrites_from_current_state() {
    // A restarted keeper applies a rotation it missed at the next register.
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let d = tempfile::tempdir().unwrap();
    register_trust(&store.refresher(), d.path(), "uid-1");
    store.set(&b);
    register_trust(&store.refresher(), d.path(), "uid-1");
    assert_eq!(read_projected(d.path(), "uid-1"), b);
}

#[cfg(unix)]
#[test]
fn write_failure_isolated_and_retried() {
    if is_root() {
        eprintln!("skipping: permission-based write failures need non-root");
        return;
    }
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    for uid in ["uid-1", "uid-2"] {
        register_trust(&r, d.path(), uid);
    }
    // One actor's volume refuses writes; the rotation still reaches the
    // other, and the error surfaces for a retry.
    let blocked = d.path().join("uid-1/system-info/trust");
    chmod(&blocked, 0o555);
    store.set(&b);
    assert!(r.refresh_bundle(EGRESS_NAME).is_err());
    assert_eq!(read_projected(d.path(), "uid-2"), b);
    assert_eq!(read_projected(d.path(), "uid-1"), a);

    chmod(&blocked, 0o755);
    r.refresh_bundle(EGRESS_NAME).unwrap();
    assert_eq!(read_projected(d.path(), "uid-1"), b);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_pipeline_retries_failed_writes() {
    if is_root() {
        eprintln!("skipping: permission-based write failures need non-root");
        return;
    }
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = Arc::new(store.refresher());
    let d = tempfile::tempdir().unwrap();
    register_trust(&r, d.path(), "uid-1");
    let blocked = d.path().join("uid-1/system-info/trust");
    chmod(&blocked, 0o555);

    let ctx = Ctx::background();
    let run = tokio::spawn(r.clone().run(ctx.clone()));
    store.set(&b);
    r.on_trust_bundle_object(EGRESS_CTB);

    // Attempts against the read-only directory keep the last good file;
    // once it heals, a retry delivers the rotation with no further event.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(read_projected(d.path(), "uid-1"), a);
    chmod(&blocked, 0o755);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while read_projected(d.path(), "uid-1") != b {
        assert!(
            std::time::Instant::now() < deadline,
            "rotation never applied by the retry loop"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ctx.cancel();
    run.await.unwrap();
}

#[test]
fn rotation_leaves_unchanged_files_alone() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("system-info/vol1");
    let mut spec = metadata_spec();
    spec.data_sources
        .extend(trust_spec("trust/ca.pem").data_sources);
    r.register(
        "uid-1",
        &ActorRef::new("team-a", "actor-1"),
        vec![SystemInfoVolume {
            name: "vol1".into(),
            root: root.clone(),
            spec,
        }],
    )
    .unwrap();

    let metadata = root.join("actor-name");
    let old = SystemTime::now() - Duration::from_secs(3600);
    std::fs::File::options()
        .write(true)
        .open(&metadata)
        .unwrap()
        .set_modified(old)
        .unwrap();
    store.set(&b);
    r.refresh_bundle(EGRESS_NAME).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("trust/ca.pem")).unwrap(),
        b
    );
    assert_eq!(
        std::fs::metadata(&metadata).unwrap().modified().unwrap(),
        old,
        "metadata rewritten by a bundle rotation"
    );
}

fn metadata_volume(root: &Path) -> SystemInfoVolume {
    SystemInfoVolume {
        name: "vol1".into(),
        root: root.to_path_buf(),
        spec: metadata_spec(),
    }
}

#[cfg(unix)]
#[test]
fn register_writes_actor_metadata() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("system-info/vol1");
    let r = CtbStore::default().refresher();
    r.register(
        "uid-golden",
        &ActorRef::new("probe-hive", "golden-actor"),
        vec![metadata_volume(&root)],
    )
    .unwrap();
    // A snapshot of one actor seeding another: the files take the new
    // values.
    r.register(
        "uid-alpha",
        &ActorRef::new("probe-hive", "probe-alpha"),
        vec![metadata_volume(&root)],
    )
    .unwrap();
    for (path, want) in [
        ("actor-name", "probe-alpha"),
        ("hive", "probe-hive"),
        ("identity/actor-uid", "uid-alpha"),
    ] {
        let target = root.join(path);
        // Raw values, no trailing newline.
        assert_eq!(std::fs::read_to_string(&target).unwrap(), want, "{path}");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o644, "{path}");
    }
}

#[test]
fn register_stable_real_paths() {
    // Restores re-bind guest state by the paths recorded at suspend, so a
    // regeneration must neither move nor drop real paths.
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("system-info/vol1");
    let r = CtbStore::default().refresher();
    r.register(
        "uid-golden",
        &ActorRef::new("probe-hive", "golden-actor"),
        vec![metadata_volume(&root)],
    )
    .unwrap();
    let paths = ["actor-name", "hive", "identity/actor-uid"];
    let mut before = HashMap::new();
    for p in paths {
        let visible = root.join(p);
        assert!(
            std::fs::symlink_metadata(&visible).unwrap().is_file(),
            "{p} is not a regular file"
        );
        before.insert(p, std::fs::canonicalize(&visible).unwrap());
    }
    r.register(
        "uid-alpha",
        &ActorRef::new("probe-hive", "probe-alpha"),
        vec![metadata_volume(&root)],
    )
    .unwrap();
    for p in paths {
        let real = std::fs::canonicalize(root.join(p)).unwrap();
        assert_eq!(real, before[p], "{p} moved on regeneration");
        assert!(before[p].exists(), "{p}: pre-rewrite real path gone");
    }
}

#[test]
fn lifecycle_unblocked_during_refresh() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = Arc::new(store.refresher());
    let d = tempfile::tempdir().unwrap();
    register_trust(&r, d.path(), "uid-a");
    register_trust(&r, d.path(), "uid-b");

    // Pin uid-a's lock so the refresh blocks on it mid-walk.
    let stuck = r.registration("uid-a").unwrap();
    let guard = stuck.stale.lock();
    store.set(&b);
    let (rtx, rrx) = std::sync::mpsc::channel();
    {
        let r = r.clone();
        std::thread::spawn(move || {
            let _ = rtx.send(r.refresh_bundle(EGRESS_NAME));
        });
    }
    let (ltx, lrx) = std::sync::mpsc::channel();
    {
        let (r, dir) = (r.clone(), d.path().to_path_buf());
        std::thread::spawn(move || {
            register_trust(&r, &dir, "uid-c");
            r.deregister("uid-b");
            let _ = ltx.send(());
        });
    }
    lrx.recv_timeout(Duration::from_secs(10))
        .expect("register/deregister of other actors blocked behind a refresh");
    assert!(
        rrx.try_recv().is_err(),
        "refresh returned while uid-a's lock was held"
    );
    drop(guard);
    rrx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    assert_eq!(read_projected(d.path(), "uid-a"), b);
}

#[test]
fn deregister_marks_stale() {
    let store = store_with(&cert());
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    let cur = register_trust(&r, d.path(), "uid-1");
    r.deregister("uid-1");
    assert!(cur.is_stale(), "deregister left the removed entry unfenced");
}

#[test]
fn register_twice_supersedes() {
    let store = store_with(&cert());
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    let first = register_trust(&r, d.path(), "uid-1");
    r.register("uid-1", &ActorRef::new("team-a", "uid-1"), Vec::new())
        .unwrap();
    assert!(first.is_stale());
    assert!(!Arc::ptr_eq(&r.registration("uid-1").unwrap(), &first));
}

#[test]
fn stale_deregister_preserves_newer_registration() {
    let store = store_with(&cert());
    let r = store.refresher();
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = register_trust(&r, d1.path(), "uid-1");
    let newer = register_trust(&r, d2.path(), "uid-1");
    // Cleanup from the first registration after the second went live.
    r.deregister_owned(&first);
    assert!(Arc::ptr_eq(&r.registration("uid-1").unwrap(), &newer));
    r.deregister_owned(&newer);
    assert!(r.registration("uid-1").is_none());
}

#[test]
fn deregister_owned_of_an_unrelated_owner() {
    // An owner is never null here; the closest case is cleanup by an owner
    // of another actor, with and without a registration present.
    for registered in [false, true] {
        let r = CtbStore::default().refresher();
        let other = r
            .register("uid-other", &ActorRef::new("h", "o"), Vec::new())
            .unwrap();
        r.deregister("uid-other");
        let current = registered.then(|| {
            r.register("uid-1", &ActorRef::new("h", "a"), Vec::new())
                .unwrap()
        });
        r.deregister_owned(&other);
        match (current, r.registration("uid-1")) {
            (Some(c), Some(got)) => assert!(Arc::ptr_eq(&c, &got)),
            (None, None) => {}
            _ => panic!("registered={registered}: cleanup changed uid-1"),
        }
    }
}

#[test]
fn failed_replacement_drops_registration() {
    let store = store_with(&cert());
    let r = store.refresher();
    let d = tempfile::tempdir().unwrap();
    let previous = register_trust(&r, d.path(), "uid-1");
    // A file where the new volume directory goes forces a real write
    // failure after the previous registration was superseded.
    let blocked = d.path().join("blocked");
    std::fs::write(&blocked, b"not a directory").unwrap();
    let err = r
        .register(
            "uid-1",
            &ActorRef::new("h", "a"),
            vec![SystemInfoVolume {
                name: "trust".into(),
                root: blocked,
                spec: trust_spec("ca.pem"),
            }],
        )
        .map(|_| ())
        .unwrap_err();
    assert!(err.message().contains("while creating"), "{err}");
    assert!(r.registration("uid-1").is_none());
    assert!(previous.is_stale());
}

#[test]
fn system_info_volumes_for_spec() {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let spec = pb::WorkloadSpec {
        containers: vec![],
        volumes: vec![
            pb::Volume {
                name: "data".into(),
                durable_dir: Some(Default::default()),
                ..Default::default()
            },
            pb::Volume {
                name: "trust".into(),
                system_info: Some(trust_spec("ca.pem")),
                ..Default::default()
            },
            pb::Volume {
                name: "meta".into(),
                system_info: Some(metadata_spec()),
                ..Default::default()
            },
        ],
    };
    let got = system_info_volumes_for(&paths, "uid-1", Some(&spec));
    let names: Vec<_> = got.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(names, ["trust", "meta"], "spec order");
    for v in &got {
        assert_eq!(v.root, paths.system_info_volume_root("uid-1", &v.name));
    }
    assert_eq!(got[0].spec, trust_spec("ca.pem"));
    assert!(
        system_info_volumes_for(&paths, "uid-1", Some(&pb::WorkloadSpec::default())).is_empty()
    );
    assert!(system_info_volumes_for(&paths, "uid-1", None).is_empty());
}

#[test]
fn concurrent_lifecycle() {
    let (a, b) = (cert(), cert());
    let store = store_with(&a);
    let r = Arc::new(store.refresher());
    let d = tempfile::tempdir().unwrap();
    std::thread::scope(|s| {
        for i in 0..4 {
            let (r, dir) = (r.clone(), d.path());
            s.spawn(move || {
                let uid = format!("uid-{i}");
                for _ in 0..20 {
                    register_trust(&r, dir, &uid);
                    r.deregister(&uid);
                }
            });
        }
        let (r, store) = (r.clone(), store.clone());
        s.spawn(move || {
            for n in 0..40 {
                store.set(if n % 2 == 0 { &b } else { &a });
                let _ = r.refresh_bundle(EGRESS_NAME);
            }
        });
    });
}

#[cfg(unix)]
#[test]
fn write_confined_to_root() {
    // A symlink inside the volume must not route a file outside it.
    let d = tempfile::tempdir().unwrap();
    let (root, outside) = (d.path().join("vol"), d.path().join("outside"));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();
    let mut payload = std::collections::BTreeMap::new();
    payload.insert("escape/ca.pem".to_string(), b"x".to_vec());
    assert!(apply(&root, &payload).is_err());
    assert!(
        !outside.join("ca.pem").exists(),
        "file escaped the volume root"
    );
}

#[test]
fn register_trust_bundle() {
    let c = cert();
    // Junk around the certificate: only the certificate block survives,
    // and the duplicate is dropped.
    let junk = "garbage\n-----BEGIN EC PRIVATE KEY-----\neA==\n-----END EC PRIVATE KEY-----\n";
    let store = store_with(&format!("{junk}{c}{c}"));
    let d = tempfile::tempdir().unwrap();
    register_trust(&store.refresher(), d.path(), "uid-1");
    assert_eq!(read_projected(d.path(), "uid-1"), c);

    // A resolution failure fails the start rather than project an empty
    // trust file, and leaves nothing registered.
    let empty = CtbStore::default();
    let r = empty.refresher();
    let d2 = tempfile::tempdir().unwrap();
    let err = r
        .register(
            "uid-2",
            &ActorRef::new("team-a", "actor-2"),
            vec![SystemInfoVolume {
                name: "trust".into(),
                root: d2.path().join("trust"),
                spec: trust_spec("ca.pem"),
            }],
        )
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.message().contains("not found") && err.message().contains(r#""trust""#),
        "{err}"
    );
    assert!(r.registration("uid-2").is_none());

    // Cleanup after a failed first write keeps a newer registration: the
    // failed register returned no owner, so there is nothing to clean up
    // with, and the newer one stays.
    let r = empty.refresher();
    assert!(r
        .register(
            "uid-3",
            &ActorRef::new("team-a", "actor-3"),
            vec![SystemInfoVolume {
                name: "trust".into(),
                root: d2.path().join("trust3"),
                spec: trust_spec("ca.pem"),
            }],
        )
        .is_err());
    let newer = r
        .register(
            "uid-3",
            &ActorRef::new("team-a", "actor-3"),
            vec![SystemInfoVolume {
                name: "metadata".into(),
                root: d2.path().join("metadata"),
                spec: metadata_spec(),
            }],
        )
        .unwrap();
    assert!(Arc::ptr_eq(&r.registration("uid-3").unwrap(), &newer));

    // Re-registration supersedes the stale entry.
    let r = store.refresher();
    let (d3, d4) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    register_trust(&r, d3.path(), "uid-rereg");
    register_trust(&r, d4.path(), "uid-rereg");
    assert_eq!(read_projected(d4.path(), "uid-rereg"), c);
}

#[test]
fn bundle_object_events_enqueue_the_bundle() {
    // Adds, updates and deletes all arrive as the object's name; only the
    // object backing an allowlisted bundle queues it.
    let r = CtbStore::default().refresher();
    for _event in ["add", "update", "delete", "tombstone"] {
        r.on_trust_bundle_object(EGRESS_CTB);
        assert_eq!(r.drain_events(), [EGRESS_NAME]);
    }
    r.on_trust_bundle_object("my-own-bundle");
    assert!(r.drain_events().is_empty());
}
