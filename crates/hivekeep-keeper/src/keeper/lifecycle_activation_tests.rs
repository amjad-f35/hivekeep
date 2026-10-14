//! Run, Restore and Terminate against a fake worker on a real node layout.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use tonic::{Code, Status};

use super::lifecycle_harness::*;
use super::run::commit_sandbox_assets;
use crate::actordirs;
use crate::assets::{read_sandbox_record, write_sandbox_record, AssetEntry, SandboxAssetsRecord};
use crate::assets::{GVISOR_ASSET_NAME, MANIFEST_NAME};
use crate::context::Ctx;
use crate::keeper::spec::to_worker_fidelity;
use crate::system_info::system_info_volumes_for;
use crate::trustbundle::tests::pem;
use crate::trustbundle::EGRESS_NAME;

fn bg() -> Ctx {
    Ctx::background()
}

/// The runs a GC test steps the actor through, from Run to Terminate, at
/// `fidelity`, with snapshots under `snapshots_dir` when given.
async fn run_local_snapshot_gc(fidelity: pb::SnapshotFidelity, snapshots_dir: Option<PathBuf>) {
    const SNAP1: &str = "pause-snap-1";
    const SNAP2: &str = "pause-snap-2";
    let node = Node::new(
        &[("checkpoint.img", "guest-memory")],
        NodeOpts {
            snapshots_dir,
            ..Default::default()
        },
    );
    node.worker.worker_epoch.store(7, Ordering::SeqCst);
    let tarball = release_tarball("runsc bytes");
    node.serve_release(&tarball);
    let rec = sandbox_record(&tarball);
    node.advertise(&rec);
    let spec = pb::WorkloadSpec {
        containers: vec![pb::Container {
            name: "app".into(),
            image: IMAGE.into(),
            command: vec!["/bin/app".into()],
            ..Default::default()
        }],
        volumes: vec![],
    };
    let k = &node.keeper;
    let run = k
        .run(
            &bg(),
            pb::RunRequest {
                hive: HIVE.into(),
                actor_name: ACTOR.into(),
                actor_uid: UID.into(),
                actor_template_hive: "default".into(),
                actor_template_name: "counter".into(),
                worker_pod_uid: POD.into(),
                sandbox_runtime: Some(runtime_for(&rec)),
                spec: Some(spec.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("Run");
    // The control plane stamps the actor with the epoch of the worker
    // process that started it.
    assert_eq!(run.worker_epoch, 7);

    let checkpoint = |name: &str, generation: i32| pb::CheckpointRequest {
        hive: HIVE.into(),
        actor_name: ACTOR.into(),
        actor_uid: UID.into(),
        actor_template_hive: "default".into(),
        actor_template_name: "counter".into(),
        worker_pod_uid: POD.into(),
        spec: Some(spec.clone()),
        snapshot: Some(lifecycle_snapshot(UID, name, generation, fidelity)),
        upload_config: Some(pb::UploadConfig {
            mode: pb::UploadMode::Deferred as i32,
        }),
    };
    let st = k
        .checkpoint(&bg(), checkpoint(SNAP1, 1))
        .await
        .expect("Checkpoint")
        .state
        .unwrap();
    assert!(
        node.paths
            .local_snapshot_dir(UID, SNAP1)
            .join("checkpoint.img")
            .is_file(),
        "pause did not write the local snapshot"
    );
    // An actor with a local snapshot stays resumable here, so it keeps its
    // staged assets.
    assert!(
        node.staged_runsc(UID).exists(),
        "paused actor lost its staged runsc"
    );
    let local = st.latest_local.as_ref().unwrap();
    assert_eq!(uuid(Some(local)), SNAP1);
    assert!(local.local_commit_time.is_some());
    assert!(st.latest_durable.is_none());

    // The resume restores from that local snapshot, not from object storage.
    let restore = k
        .restore(
            &bg(),
            pb::RestoreRequest {
                hive: HIVE.into(),
                actor_name: ACTOR.into(),
                actor_uid: UID.into(),
                actor_template_hive: "default".into(),
                actor_template_name: "counter".into(),
                worker_pod_uid: POD.into(),
                sandbox_runtime: Some(runtime_for(&rec)),
                spec: Some(spec.clone()),
                snapshot: Some(lifecycle_snapshot(UID, SNAP1, 1, fidelity)),
                ..Default::default()
            },
        )
        .await
        .expect("Restore");
    assert_eq!(restore.worker_epoch, 7);
    assert_eq!(
        node.worker.restored("checkpoint.img").as_deref(),
        Some("guest-memory")
    );
    assert!(
        node.staged_runsc(UID).exists(),
        "resumed actor has no staged runsc"
    );
    for rpc in ["CheckpointWorkload", "RestoreWorkload"] {
        assert_eq!(
            node.worker.fidelities.lock()[rpc],
            to_worker_fidelity(fidelity as i32),
            "{rpc} carried another fidelity"
        );
    }
    assert!(node.worker.preserved(), "a local restore preserves its dir");
    assert_eq!(
        std::fs::read_dir(node.paths.restore_state_dir(UID))
            .unwrap()
            .count(),
        0,
        "restore-state stays empty on a local restore"
    );

    // The second checkpoint replaces the first snapshot.
    k.checkpoint(&bg(), checkpoint(SNAP2, 2))
        .await
        .expect("Checkpoint 2");
    assert!(!exists(&node.paths.local_snapshot_dir(UID, SNAP1)));
    assert!(node
        .paths
        .local_snapshot_dir(UID, SNAP2)
        .join("checkpoint.img")
        .is_file());

    k.terminate(
        &bg(),
        pb::TerminateRequest {
            hive: HIVE.into(),
            actor_name: ACTOR.into(),
            actor_uid: UID.into(),
            actor_template_hive: "default".into(),
            actor_template_name: "counter".into(),
            worker_pod_uid: POD.into(),
            spec: Some(spec.clone()),
        },
    )
    .await
    .expect("Terminate");

    // Every RPC hands the worker the same directories, except that a local
    // restore points restore_dir at the local snapshot.
    for rpc in [
        "RunWorkload",
        "CheckpointWorkload",
        "RestoreWorkload",
        "TerminateWorkload",
    ] {
        let mut want = node.paths.actor_dirs(UID);
        if rpc == "RestoreWorkload" {
            want.restore_dir = node
                .paths
                .local_snapshot_dir(UID, SNAP1)
                .to_string_lossy()
                .into_owned();
        }
        assert_eq!(node.worker.dirs(rpc), Some(want), "{rpc} actor dirs");
    }
    assert!(!exists(&node.paths.local_checkpoints_dir(UID)));
    assert_eq!(
        node.tracker().state(UID).await,
        pb::ActorSnapshotState::default()
    );
    assert_dir_gone("actor dir", &node.paths.actor_dir(UID));
    assert_dir_gone("actor snapshot dir", &node.paths.actor_snapshot_dir(UID));
}

#[tokio::test]
async fn local_snapshot_gc_volumes() {
    run_local_snapshot_gc(pb::SnapshotFidelity::Volumes, None).await;
}

#[tokio::test]
async fn local_snapshot_gc_rootfs() {
    run_local_snapshot_gc(pb::SnapshotFidelity::Rootfs, None).await;
}

#[tokio::test]
async fn local_snapshot_gc_memory() {
    run_local_snapshot_gc(pb::SnapshotFidelity::Memory, None).await;
}

/// The GC flow with the snapshots root on another filesystem than the
/// actors root: /dev/shm, a tmpfs on Linux.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_snapshot_gc_with_separate_snapshots_filesystem() {
    use std::os::unix::fs::MetadataExt;
    let Ok(shm) = tempfile::Builder::new()
        .prefix("hk-snapshots-")
        .tempdir_in("/dev/shm")
    else {
        eprintln!("skipping: no /dev/shm");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    if std::fs::metadata(shm.path()).unwrap().dev() == std::fs::metadata(tmp.path()).unwrap().dev()
    {
        eprintln!("skipping: /dev/shm is on the temp dir's filesystem");
        return;
    }
    for f in [
        pb::SnapshotFidelity::Volumes,
        pb::SnapshotFidelity::Rootfs,
        pb::SnapshotFidelity::Memory,
    ] {
        run_local_snapshot_gc(f, Some(shm.path().join(format!("snapshots-{}", f as i32)))).await;
    }
}

/// Restore runs the actor with the runtime its request names, not the one
/// the snapshot manifest recorded, and records it before RestoreWorkload.
#[tokio::test]
async fn restore_uses_request_sandbox_runtime() {
    let node = Node::new(&[("checkpoint.img", "guest-memory")], NodeOpts::default());
    let (cp_tar, rs_tar) = (release_tarball("runsc v1"), release_tarball("runsc v2"));
    let (cp_rec, rs_rec) = (sandbox_record(&cp_tar), sandbox_record(&rs_tar));
    node.serve_release(&cp_tar);
    node.advertise(&cp_rec);
    let mut a = PluginActor {
        node,
        rec: cp_rec.clone(),
        spec: pb::WorkloadSpec {
            containers: vec![pb::Container {
                name: "app".into(),
                image: IMAGE.into(),
                command: vec!["/bin/app".into()],
                ..Default::default()
            }],
            volumes: vec![],
        },
    };
    a.k().run(&bg(), a.run_request()).await.expect("Run");
    a.checkpointed("pause-snap-1", 1).await;
    let manifest = SandboxAssetsRecord::parse(
        &std::fs::read(
            a.paths()
                .local_snapshot_dir(UID, "pause-snap-1")
                .join(MANIFEST_NAME),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest.assets[GVISOR_ASSET_NAME].sha256,
        cp_rec.assets[GVISOR_ASSET_NAME].sha256
    );

    a.node.serve_release(&rs_tar);
    a.node.advertise(&rs_rec);
    a.rec = rs_rec.clone();
    let req = a.restore_request(&a.snapshot("pause-snap-1", 1));

    // A Restore that fails after reaching the worker still leaves the
    // record for a later Terminate to tear the sandbox down with.
    std::fs::remove_file(a.paths().actor_sandbox_assets_file(UID)).unwrap();
    *a.worker().restore_err.lock() = Some(Status::deadline_exceeded("restore timed out"));
    assert!(a.k().restore(&bg(), req.clone()).await.is_err());
    let got = read_sandbox_record(a.paths(), UID).expect("record after a failed RestoreWorkload");
    assert_eq!(
        got.assets[GVISOR_ASSET_NAME].sha256,
        rs_rec.assets[GVISOR_ASSET_NAME].sha256
    );

    *a.worker().restore_err.lock() = None;
    a.k().restore(&bg(), req).await.expect("Restore");
    let got = read_sandbox_record(a.paths(), UID).unwrap();
    assert_eq!(
        got.assets[GVISOR_ASSET_NAME].sha256, rs_rec.assets[GVISOR_ASSET_NAME].sha256,
        "the restored actor runs the request's runtime"
    );
}

fn trust_volume_spec(path: &str) -> pb::SystemInfoVolumeSource {
    pb::SystemInfoVolumeSource {
        data_sources: vec![pb::SystemInfoDataSource {
            actor_metadata: None,
            trust_bundle: Some(pb::TrustBundleDataSource {
                path: path.into(),
                names: vec![EGRESS_NAME.into()],
            }),
        }],
    }
}

fn b64(body: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(body)
}

#[derive(Clone, Copy, PartialEq)]
enum Failure {
    Asset,
    Reset,
    Record,
    Manifest,
}

/// A failed activation keeps a live projection only until it starts
/// resetting the actor's directories. Once the reset deletes the files,
/// their old registration is fenced, including when the reset itself or an
/// early preparation step fails.
#[tokio::test]
async fn activation_failure_before_registration() {
    struct Case {
        name: &'static str,
        restore: bool,
        failure: Failure,
        want_err: &'static str,
        want_present: bool,
        published_volume: bool,
    }
    let case = |name, restore, failure, want_err, want_present, published_volume| Case {
        name,
        restore,
        failure,
        want_err,
        want_present,
        published_volume,
    };
    for tc in [
        case(
            "Run asset failure preserves projection",
            false,
            Failure::Asset,
            "asset unavailable",
            true,
            false,
        ),
        case(
            "Run partial reset removes registration",
            false,
            Failure::Reset,
            "while creating volumes dir",
            false,
            false,
        ),
        case(
            "Run record failure removes registration",
            false,
            Failure::Record,
            "while recording sandbox assets",
            false,
            false,
        ),
        case(
            "Restore partial reset removes registration",
            true,
            Failure::Reset,
            "while creating volumes dir",
            false,
            false,
        ),
        case(
            "Restore manifest failure removes registration",
            true,
            Failure::Manifest,
            "no uploaded copy",
            false,
            false,
        ),
        case(
            "Restore asset failure removes registration",
            true,
            Failure::Asset,
            "asset unavailable",
            false,
            false,
        ),
        case(
            "Run retry keeps a published volume",
            false,
            Failure::Record,
            "while recording sandbox assets",
            false,
            true,
        ),
        case(
            "Restore retry keeps a published volume",
            true,
            Failure::Manifest,
            "no uploaded copy",
            false,
            true,
        ),
    ] {
        const UID: &str = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a61";
        const SNAP: &str = "pause-snap-1";
        let node = Node::new(&[], NodeOpts::default());
        *node.egress_ca().lock() = Some(pem(b"cert-a"));
        let spec = pb::WorkloadSpec {
            containers: vec![],
            volumes: vec![pb::Volume {
                name: "trust".into(),
                system_info: Some(trust_volume_spec("ca.pem")),
                ..Default::default()
            }],
        };
        let registered = node
            .refresher
            .register(
                UID,
                &ActorRef::new("team-a", "actor-1"),
                system_info_volumes_for(&node.paths, UID, Some(&spec)),
            )
            .unwrap();
        let projected = node
            .paths
            .system_info_volume_root(UID, "trust")
            .join("ca.pem");
        let initial = std::fs::read_to_string(&projected).unwrap();
        assert!(
            initial.contains(&b64(b"cert-a")),
            "{}: initial projection",
            tc.name
        );

        let tarball = release_tarball("runsc bytes");
        let rec = sandbox_record(&tarball);
        if tc.restore && tc.failure != Failure::Manifest {
            let mut manifest = rec.clone();
            manifest.snapshot_files = vec!["checkpoint.img".into()];
            write_local_snapshot(
                &node.paths.local_snapshot_dir(UID, SNAP),
                &manifest,
                &[("checkpoint.img", "guest-memory")],
            );
        }
        if tc.failure == Failure::Reset {
            // A file where the volumes dir goes fails the reset after it
            // removed the system-info roots.
            std::fs::create_dir_all(node.paths.actor_dir(UID)).unwrap();
            std::fs::write(node.paths.external_volume_mounts_dir(UID), b"").unwrap();
        }
        if tc.failure == Failure::Record {
            std::fs::create_dir_all(node.paths.actor_sandbox_assets_file(UID)).unwrap();
        }
        // A non-empty dir fails a remove like a live mount would, so the
        // activation only gets past the reset if it leaves volumes/ alone.
        let volume_file = node
            .paths
            .external_volume_host_path(UID, "data")
            .join("data.txt");
        if tc.published_volume {
            std::fs::create_dir_all(volume_file.parent().unwrap()).unwrap();
            std::fs::write(&volume_file, b"volume data").unwrap();
        }
        // The live activation's staged binary.
        let live = node.staged_runsc(UID);
        std::fs::create_dir_all(live.parent().unwrap()).unwrap();
        std::fs::write(&live, b"live runsc").unwrap();
        node.serve_release(&tarball);
        if tc.failure == Failure::Asset {
            *node.gcs.err.lock() = Some("asset unavailable".into());
        }
        node.advertise(&rec);
        let err = if tc.restore {
            node.keeper
                .restore(
                    &bg(),
                    pb::RestoreRequest {
                        hive: "team-a".into(),
                        actor_name: "actor-1".into(),
                        actor_uid: UID.into(),
                        worker_pod_uid: POD.into(),
                        sandbox_runtime: Some(runtime_for(&rec)),
                        spec: Some(spec.clone()),
                        snapshot: Some(lifecycle_snapshot(
                            UID,
                            SNAP,
                            1,
                            pb::SnapshotFidelity::Memory,
                        )),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
        } else {
            node.keeper
                .run(
                    &bg(),
                    pb::RunRequest {
                        hive: "team-a".into(),
                        actor_name: "actor-1".into(),
                        actor_uid: UID.into(),
                        worker_pod_uid: POD.into(),
                        sandbox_runtime: Some(runtime_for(&rec)),
                        spec: Some(spec.clone()),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
        }
        .expect_err(tc.name);
        assert!(
            err.message().contains(tc.want_err),
            "{}: error {err:?}, want {:?}",
            tc.name,
            tc.want_err
        );
        assert!(
            !exists(&node.paths.sandbox_assets_staging_dir(UID)),
            "{}: staging dir left behind",
            tc.name
        );
        if tc.published_volume {
            assert_eq!(
                std::fs::read(&volume_file).ok().as_deref(),
                Some(&b"volume data"[..]),
                "{}: activation touched the published volume",
                tc.name
            );
        }
        if tc.want_present {
            assert_eq!(
                std::fs::read_to_string(&projected).unwrap(),
                initial,
                "{}",
                tc.name
            );
            let cur = node.refresher.registration(UID).expect("still registered");
            assert!(
                Arc::ptr_eq(&cur, &registered) && !registered.is_stale(),
                "{}",
                tc.name
            );
            assert_eq!(std::fs::read(&live).unwrap(), b"live runsc", "{}", tc.name);
        } else {
            // Past the reset, the record still describes the previous
            // activation (or nothing), so none of this one's links remain:
            // at most the previous activation's own, untouched.
            if let Ok(entries) = std::fs::read_dir(node.paths.sandbox_assets_dir(UID)) {
                for e in entries.flatten() {
                    assert!(
                        e.file_name() == GVISOR_ASSET_NAME
                            && std::fs::read(&live).ok().as_deref() == Some(&b"live runsc"[..]),
                        "{}: failed activation left {:?} in the assets dir",
                        tc.name,
                        e.file_name()
                    );
                }
            }
            assert!(
                !exists(&projected),
                "{}: the reset kept the projection",
                tc.name
            );
            assert!(node.refresher.registration(UID).is_none(), "{}", tc.name);
            assert!(
                registered.is_stale(),
                "{}: old registration not fenced",
                tc.name
            );
        }
        *node.egress_ca().lock() = Some(pem(b"cert-b"));
        node.refresher.refresh_bundle(EGRESS_NAME).unwrap();
        if tc.want_present {
            assert!(
                std::fs::read_to_string(&projected)
                    .unwrap()
                    .contains(&b64(b"cert-b")),
                "{}: the preserved projection stopped refreshing",
                tc.name
            );
        } else {
            assert!(
                !exists(&projected),
                "{}: a refresh recreated a fenced projection",
                tc.name
            );
        }
    }
}

/// A Run or Restore that fails after registering its system-info volumes
/// removes its own registration.
#[tokio::test]
async fn activation_failure_after_registration_removes_own_registration() {
    for restore in [false, true] {
        let uid = if restore {
            "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a63"
        } else {
            "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a62"
        };
        let node = Node::new(&[], NodeOpts::default());
        *node.egress_ca().lock() = Some(pem(b"cert"));
        let tarball = release_tarball("runsc bytes");
        node.serve_release(&tarball);
        let rec = sandbox_record(&tarball);
        node.advertise(&rec);
        let spec = pb::WorkloadSpec {
            containers: vec![pb::Container {
                name: "app".into(),
                image: MISSING_IMAGE.into(),
                ..Default::default()
            }],
            volumes: vec![pb::Volume {
                name: "trust".into(),
                system_info: Some(trust_volume_spec("ca.pem")),
                ..Default::default()
            }],
        };
        let err = if restore {
            let mut manifest = rec.clone();
            manifest.snapshot_files = vec!["checkpoint.img".into()];
            write_local_snapshot(
                &node.paths.local_snapshot_dir(uid, "pause-snap-after"),
                &manifest,
                &[("checkpoint.img", "guest-memory")],
            );
            node.keeper
                .restore(
                    &bg(),
                    pb::RestoreRequest {
                        hive: "team-a".into(),
                        actor_name: "actor-restore-after".into(),
                        actor_uid: uid.into(),
                        worker_pod_uid: POD.into(),
                        sandbox_runtime: Some(runtime_for(&rec)),
                        spec: Some(spec),
                        snapshot: Some(lifecycle_snapshot(
                            uid,
                            "pause-snap-after",
                            1,
                            pb::SnapshotFidelity::Memory,
                        )),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
        } else {
            node.keeper
                .run(
                    &bg(),
                    pb::RunRequest {
                        hive: "team-a".into(),
                        actor_name: "actor-run".into(),
                        actor_uid: uid.into(),
                        worker_pod_uid: POD.into(),
                        sandbox_runtime: Some(runtime_for(&rec)),
                        spec: Some(spec),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
        }
        .unwrap_err();
        assert!(
            err.message().contains("while creating \"app\" OCI bundle"),
            "restore={restore}: {err:?}, want the post-registration OCI preparation failure"
        );
        assert!(
            node.paths
                .system_info_volume_root(uid, "trust")
                .join("ca.pem")
                .is_file(),
            "the registration wrote its projection before the bundle failed"
        );
        assert!(
            node.refresher.registration(uid).is_none(),
            "restore={restore}"
        );
    }
}

/// Restore reads a usable local copy in place even when an uploaded one
/// exists, and stages the upload when the local copy is absent or unusable.
#[tokio::test]
async fn restore_prefers_local_and_falls_back_to_object_storage() {
    const SNAP: &str = "pause-snap-1";
    let tarball = release_tarball("runsc bytes");
    let rec = sandbox_record(&tarball);
    let mut manifest = rec.clone();
    manifest.snapshot_files = vec!["checkpoint.img".into()];
    type Prep = Box<dyn Fn(&std::path::Path)>;
    let m1 = manifest.clone();
    let m2 = manifest.clone();
    let m3 = manifest.clone();
    let cases: Vec<(&str, Prep, bool)> = vec![
        (
            "usable local copy is preferred over object storage",
            Box::new(move |d| write_local_snapshot(d, &m1, &[("checkpoint.img", "local-memory")])),
            true,
        ),
        (
            "absent local copy falls back to object storage",
            Box::new(|_| {}),
            false,
        ),
        (
            "local copy with missing listed file falls back to object storage",
            Box::new(move |d| write_local_snapshot(d, &m2, &[])),
            false,
        ),
        (
            "local copy with corrupt manifest falls back to object storage",
            Box::new(|d| {
                std::fs::create_dir_all(d).unwrap();
                std::fs::write(d.join(MANIFEST_NAME), b"{not-json").unwrap();
            }),
            false,
        ),
        (
            "local copy with symlinked snapshot file falls back to object storage",
            Box::new(move |d| {
                write_local_snapshot(d, &m3, &[]);
                let target = d.parent().unwrap().join("outside.img");
                std::fs::write(&target, b"outside").unwrap();
                std::os::unix::fs::symlink(&target, d.join("checkpoint.img")).unwrap();
            }),
            false,
        ),
    ];
    for (name, prep, want_local) in cases {
        let node = Node::new(&[("checkpoint.img", "")], NodeOpts::default());
        node.serve_release(&tarball);
        let src = node.dir.path().join("remote-src");
        write_local_snapshot(
            &src,
            &manifest,
            &[("checkpoint.img", "object-storage-memory")],
        );
        node.upload(PAUSED_SNAPSHOT_URI, &src, &manifest).await;
        let local = node.paths.local_snapshot_dir(UID, SNAP);
        prep(&local);
        node.advertise(&rec);
        node.keeper
            .restore(
                &bg(),
                pb::RestoreRequest {
                    hive: HIVE.into(),
                    actor_name: ACTOR.into(),
                    actor_uid: UID.into(),
                    actor_template_hive: "default".into(),
                    actor_template_name: "counter".into(),
                    worker_pod_uid: POD.into(),
                    sandbox_runtime: Some(runtime_for(&rec)),
                    spec: Some(pb::WorkloadSpec {
                        containers: vec![pb::Container {
                            name: "app".into(),
                            image: IMAGE.into(),
                            command: vec!["/bin/app".into()],
                            ..Default::default()
                        }],
                        volumes: vec![],
                    }),
                    snapshot: Some(pb::Snapshot {
                        snapshot_uuid: SNAP.into(),
                        generation: 1,
                        fidelity: pb::SnapshotFidelity::Memory as i32,
                        object: Some(pb::ObjectStorage {
                            snapshot_uri: PAUSED_SNAPSHOT_URI.into(),
                            sharing: 0,
                        }),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: Restore: {e:?}"));
        let (want_content, want_dir) = if want_local {
            ("local-memory", local.clone())
        } else {
            ("object-storage-memory", node.paths.restore_state_dir(UID))
        };
        assert_eq!(
            node.worker.restored("checkpoint.img").as_deref(),
            Some(want_content),
            "{name}"
        );
        assert_eq!(node.worker.preserved(), want_local, "{name}");
        assert_eq!(
            node.worker.dirs("RestoreWorkload").unwrap().restore_dir,
            want_dir.to_string_lossy(),
            "{name}"
        );
        if want_local {
            assert_eq!(
                std::fs::read_dir(node.paths.restore_state_dir(UID))
                    .unwrap()
                    .count(),
                0,
                "{name}: restore-state is empty on a local restore"
            );
        }
    }
}

/// A shared restore that fails after staging its files leaves nothing in
/// restore-state, so no link holds the snapshot cache's entries.
#[tokio::test]
async fn restore_failure_removes_staged_snapshot_files() {
    const UID: &str = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a66";
    let node = Node::new(
        &[("checkpoint.img", "")],
        NodeOpts {
            snapshot_cache: true,
            ..Default::default()
        },
    );
    *node.worker.restore_err.lock() = Some(Status::internal("sandbox would not start"));
    let tarball = release_tarball("runsc bytes");
    node.serve_release(&tarball);
    let rec = sandbox_record(&tarball);
    let mut manifest = rec.clone();
    manifest.snapshot_files = vec!["checkpoint.img".into()];
    let src = node.dir.path().join("src");
    write_local_snapshot(&src, &manifest, &[("checkpoint.img", "shared-memory")]);
    node.upload(PAUSED_SNAPSHOT_URI, &src, &manifest).await;
    node.advertise(&rec);
    let err = node
        .keeper
        .restore(
            &bg(),
            pb::RestoreRequest {
                hive: HIVE.into(),
                actor_name: ACTOR.into(),
                actor_uid: UID.into(),
                actor_template_hive: "default".into(),
                actor_template_name: "counter".into(),
                worker_pod_uid: POD.into(),
                sandbox_runtime: Some(runtime_for(&rec)),
                spec: Some(pb::WorkloadSpec {
                    containers: vec![pb::Container {
                        name: "app".into(),
                        image: IMAGE.into(),
                        command: vec!["/bin/app".into()],
                        ..Default::default()
                    }],
                    volumes: vec![],
                }),
                snapshot: Some(pb::Snapshot {
                    snapshot_uuid: "pause-snap-1".into(),
                    generation: 1,
                    fidelity: pb::SnapshotFidelity::Memory as i32,
                    object: Some(pb::ObjectStorage {
                        snapshot_uri: PAUSED_SNAPSHOT_URI.into(),
                        sharing: pb::SnapshotSharing::Shared as i32,
                    }),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal, "{err:?}");
    assert!(
        node.worker.dirs("RestoreWorkload").is_some(),
        "the failure must come after the files were staged"
    );
    if let Ok(entries) = std::fs::read_dir(node.paths.restore_state_dir(UID)) {
        assert_eq!(
            entries.count(),
            0,
            "restore-state holds files after a failed restore"
        );
    }
    let stats = node
        .snapshot_cache
        .as_ref()
        .unwrap()
        .evict_unused(1 << 40, true)
        .await
        .unwrap();
    // The manifest and checkpoint.img, both unlinked.
    assert_eq!((stats.retired, stats.skipped_linked), (2, 0), "{stats:?}");
}

/// A Restore with no usable local copy naming a snapshot never uploaded
/// fails NotFound, saying why the local copy did not serve.
#[tokio::test]
async fn restore_without_usable_local_copy() {
    const UID: &str = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a65";
    let tarball = release_tarball("runsc bytes");
    let rec = sandbox_record(&tarball);
    for (unusable, cause) in [(false, "no local copy"), (true, "unusable local snapshot")] {
        let node = Node::new(&[], NodeOpts::default());
        if unusable {
            write_local_snapshot(
                &node.paths.local_snapshot_dir(UID, "pause-snap-1"),
                &SandboxAssetsRecord {
                    sandbox_class: "gvisor".into(),
                    snapshot_files: vec!["checkpoint.img".into()],
                    ..Default::default()
                },
                &[],
            );
        }
        node.advertise(&rec);
        let err = node
            .keeper
            .restore(
                &bg(),
                pb::RestoreRequest {
                    hive: "team-a".into(),
                    actor_name: "actor-1".into(),
                    actor_uid: UID.into(),
                    worker_pod_uid: POD.into(),
                    sandbox_runtime: Some(runtime_for(&rec)),
                    spec: Some(pb::WorkloadSpec::default()),
                    snapshot: Some(lifecycle_snapshot(
                        UID,
                        "pause-snap-1",
                        1,
                        pb::SnapshotFidelity::Memory,
                    )),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound, "{err:?}");
        assert!(err.message().contains("no uploaded copy"), "{err:?}");
        assert!(
            err.message().contains(cause),
            "{err:?}, want it to say {cause:?}"
        );
    }
}

/// Terminate naming no worker skips the worker while it still prunes local
/// snapshots and removes the actor's directories.
#[tokio::test]
async fn terminate_without_worker_pod_uid() {
    const UID: &str = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a64";
    let node = Node::new(&[], NodeOpts::default());
    actordirs::reset_actor_dirs(&node.paths, UID).unwrap();
    let snap = node.paths.local_snapshot_dir(UID, "pause-snap-1");
    std::fs::create_dir_all(&snap).unwrap();
    std::fs::write(snap.join("checkpoint.img"), b"guest-memory").unwrap();
    node.keeper
        .terminate(
            &bg(),
            pb::TerminateRequest {
                hive: HIVE.into(),
                actor_name: ACTOR.into(),
                actor_uid: UID.into(),
                actor_template_hive: "default".into(),
                actor_template_name: "counter".into(),
                worker_pod_uid: String::new(),
                spec: Some(pb::WorkloadSpec {
                    containers: vec![pb::Container {
                        name: "app".into(),
                        image: MISSING_IMAGE.into(),
                        ..Default::default()
                    }],
                    volumes: vec![],
                }),
            },
        )
        .await
        .expect("Terminate");
    assert!(
        node.worker.dirs("TerminateWorkload").is_none(),
        "the worker was called"
    );
    assert!(!exists(&node.paths.local_checkpoints_dir(UID)));
    assert!(!exists(&node.paths.actor_snapshot_dir(UID)));
    assert!(!exists(&node.paths.actor_dir(UID)));
}

/// A Terminate still naming a worker reclaims the actor's state when the
/// sandbox is gone: the pod's socket went with it, the socket outlived its
/// process, or the actor has no sandbox record.
#[tokio::test]
async fn terminate_reclaims_state_without_a_live_sandbox() {
    for case in [
        "socket gone",
        "socket outlived its process",
        "no sandbox record",
    ] {
        let node = Node::new(&[], NodeOpts::default());
        for dir in [
            node.paths.durable_dir_volume_mounts_dir(UID),
            node.paths.checkpoint_state_dir(UID),
            node.paths.restore_state_dir(UID),
        ] {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("payload"), b"actor state").unwrap();
        }
        std::fs::create_dir_all(node.paths.local_snapshot_dir(UID, "pause-snap-1")).unwrap();
        match case {
            "socket gone" => {
                node.remove_socket();
                // A record whose asset is not in the cache fails the asset
                // staging if Terminate read it before checking the socket.
                let mut rec = SandboxAssetsRecord {
                    sandbox_class: "gvisor".into(),
                    ..Default::default()
                };
                rec.assets.insert(
                    GVISOR_ASSET_NAME.into(),
                    AssetEntry {
                        url: "gs://test-bucket/gvisor-release.tar.gz".into(),
                        sha256: "a".repeat(64),
                    },
                );
                write_sandbox_record(&node.paths, UID, &rec).unwrap();
            }
            "socket outlived its process" => {
                let tarball = release_tarball("runsc bytes");
                node.serve_release(&tarball);
                write_sandbox_record(&node.paths, UID, &sandbox_record(&tarball)).unwrap();
                node.worker.set_on_terminate(fallible_hook(|| async {
                    Err(Status::unavailable("worker shutting down"))
                }));
            }
            _ => {}
        }
        node.keeper
            .terminate(
                &bg(),
                pb::TerminateRequest {
                    hive: HIVE.into(),
                    actor_name: ACTOR.into(),
                    actor_uid: UID.into(),
                    actor_template_hive: "default".into(),
                    actor_template_name: "counter".into(),
                    worker_pod_uid: POD.into(),
                    spec: Some(pb::WorkloadSpec::default()),
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{case}: Terminate: {e:?}"));
        assert_dir_gone("actor dir", &node.paths.actor_dir(UID));
        assert_dir_gone("actor snapshot dir", &node.paths.actor_snapshot_dir(UID));
    }
}

/// A repeated Terminate succeeds, whether one caller retries or a delete
/// sends a second Terminate, naming no worker, after a worker delete's.
#[tokio::test]
async fn terminate_can_be_retried() {
    for (name, fail_first, second_without_worker, want_teardowns) in [
        ("after a terminate that succeeded", false, false, 1),
        (
            "after a terminate that failed past the teardown",
            true,
            false,
            2,
        ),
        (
            "delete after a worker-delete terminate that succeeded",
            false,
            true,
            1,
        ),
        (
            "delete after a worker-delete terminate that failed partway",
            true,
            true,
            1,
        ),
    ] {
        let a = run_plugin_actor(NodeOpts::default()).await;
        a.checkpointed("pause-snap-1", 1).await;
        // A populated volume directory, which the reclaim refuses to delete
        // through.
        let straggler = a
            .paths()
            .external_volume_host_path(UID, "data")
            .join("file");
        if fail_first {
            std::fs::create_dir_all(straggler.parent().unwrap()).unwrap();
            std::fs::write(&straggler, b"still mounted").unwrap();
        }
        let first = a.terminate_request();
        let res = a.k().terminate(&bg(), first.clone()).await;
        if fail_first {
            assert!(
                res.is_err(),
                "{name}: first Terminate deleted through a populated volume"
            );
            // The unmount finishes before the second Terminate.
            std::fs::remove_file(&straggler).unwrap();
        } else {
            res.unwrap_or_else(|e| panic!("{name}: first Terminate: {e:?}"));
        }
        let mut second = first;
        if second_without_worker {
            second.worker_pod_uid.clear();
        }
        a.k()
            .terminate(&bg(), second)
            .await
            .unwrap_or_else(|e| panic!("{name}: second Terminate: {e:?}"));
        assert_eq!(
            a.worker().terminations.load(Ordering::SeqCst),
            want_teardowns,
            "{name}: TerminateWorkload calls"
        );
        assert_dir_gone("actor dir", &a.paths().actor_dir(UID));
        assert_dir_gone("actor snapshot dir", &a.paths().actor_snapshot_dir(UID));
    }
}

/// A live worker refusing TerminateWorkload fails the Terminate without
/// touching the actor's directories or local snapshots.
#[tokio::test]
async fn terminate_worker_failure_preserves_state() {
    let a = run_plugin_actor(NodeOpts::default()).await;
    a.checkpointed("pause-snap-1", 1).await;
    let durable = a
        .paths()
        .durable_dir_volume_mount_point(UID, "work")
        .join("payload");
    std::fs::create_dir_all(durable.parent().unwrap()).unwrap();
    std::fs::write(&durable, b"runtime-payload").unwrap();
    a.worker().set_on_terminate(fallible_hook(|| async {
        Err(Status::internal("sandbox busy"))
    }));
    let err = a
        .k()
        .terminate(&bg(), a.terminate_request())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal, "{err:?}");
    assert!(durable.is_file(), "durable-dir payload removed");
    assert!(a
        .paths()
        .local_snapshot_dir(UID, "pause-snap-1")
        .join("checkpoint.img")
        .is_file());
}

/// Two actors needing the same asset at once share one download, each with
/// its own link to the one cached copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runs_share_one_asset_download() {
    use std::os::unix::fs::MetadataExt;
    let node = Arc::new(Node::new(&[], NodeOpts::default()));
    let tarball = release_tarball("runsc bytes");
    node.serve_release(&tarball);
    *node.gcs.delay.lock() = Duration::from_millis(200);
    let rec = sandbox_record(&tarball);
    node.advertise(&rec);
    let uids = [
        "01234567-89ab-cdef-0123-456789abcde1",
        "01234567-89ab-cdef-0123-456789abcde2",
    ];
    let runs = uids.iter().map(|uid| {
        let (node, rec) = (node.clone(), rec.clone());
        let uid = uid.to_string();
        tokio::spawn(async move {
            node.keeper
                .run(
                    &bg(),
                    pb::RunRequest {
                        hive: HIVE.into(),
                        actor_name: format!("counter-{}", &uid[uid.len() - 1..]),
                        actor_uid: uid.clone(),
                        actor_template_hive: "default".into(),
                        actor_template_name: "counter".into(),
                        worker_pod_uid: POD.into(),
                        sandbox_runtime: Some(runtime_for(&rec)),
                        spec: Some(pb::WorkloadSpec {
                            containers: vec![pb::Container {
                                name: "app".into(),
                                image: IMAGE.into(),
                                command: vec!["/bin/app".into()],
                                ..Default::default()
                            }],
                            volumes: vec![],
                        }),
                        ..Default::default()
                    },
                )
                .await
        })
    });
    for r in futures::future::join_all(runs).await {
        r.unwrap().expect("Run");
    }
    assert_eq!(
        node.gcs.gets.load(Ordering::SeqCst),
        1,
        "downloads for two Runs"
    );
    let mut inodes = Vec::new();
    for uid in uids {
        let staged = node.staged_runsc(uid);
        assert_eq!(std::fs::read(&staged).unwrap(), b"runsc bytes");
        let m = std::fs::metadata(&staged).unwrap();
        inodes.push((m.dev(), m.ino()));
        assert!(!exists(&node.paths.sandbox_assets_staging_dir(uid)));
    }
    assert_eq!(inodes[0], inodes[1], "the two links are not one inode");
}

/// A path outside the staging dir fails the commit before anything moves.
#[tokio::test]
async fn commit_sandbox_assets_checks_paths_before_moving() {
    let node = Node::new(&[], NodeOpts::default());
    let (staging, fin) = (
        node.paths.sandbox_assets_staging_dir(UID),
        node.paths.sandbox_assets_dir(UID),
    );
    let runsc = std::path::Path::new(GVISOR_ASSET_NAME).join("runsc");
    for d in [staging.join(GVISOR_ASSET_NAME), fin.join(GVISOR_ASSET_NAME)] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(staging.join(&runsc), b"new").unwrap();
    std::fs::write(fin.join(&runsc), b"previous").unwrap();
    let staged = staging.join(&runsc).to_string_lossy().into_owned();
    let mut paths = std::collections::BTreeMap::from([
        (GVISOR_ASSET_NAME.to_string(), staged.clone()),
        ("stray".to_string(), "/elsewhere/kernel".to_string()),
    ]);
    assert!(commit_sandbox_assets(&fin, &staging, &mut paths).is_err());
    assert_eq!(std::fs::read(fin.join(&runsc)).unwrap(), b"previous");
    assert!(
        staging.join(&runsc).exists(),
        "staged set gone after a refused commit"
    );
    assert_eq!(
        paths[GVISOR_ASSET_NAME], staged,
        "paths rewritten by a refused commit"
    );

    paths.remove("stray");
    commit_sandbox_assets(&fin, &staging, &mut paths).unwrap();
    assert_eq!(std::fs::read(fin.join(&runsc)).unwrap(), b"new");
    assert_eq!(paths[GVISOR_ASSET_NAME], fin.join(&runsc).to_string_lossy());
    assert!(!exists(&staging));
}

/// Run and Restore refuse a runtime no worker here advertises before
/// fetching or staging anything, and either request without a runtime is
/// invalid.
#[tokio::test]
async fn run_and_restore_reject_unadvertised_runtime() {
    let node = Node::new(&[], NodeOpts::default());
    let tarball = release_tarball("runsc bytes");
    node.serve_release(&tarball);
    let rec = sandbox_record(&tarball);
    for restore in [false, true] {
        let call = |rt: Option<pb::ActorSandboxRuntime>| {
            let k = node.keeper.clone();
            async move {
                if restore {
                    k.restore(
                        &bg(),
                        pb::RestoreRequest {
                            hive: "team-a".into(),
                            actor_name: "actor-1".into(),
                            actor_uid: UID.into(),
                            worker_pod_uid: POD.into(),
                            sandbox_runtime: rt,
                            spec: Some(pb::WorkloadSpec::default()),
                            snapshot: Some(lifecycle_snapshot(
                                UID,
                                "pause-snap-1",
                                1,
                                pb::SnapshotFidelity::Memory,
                            )),
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
                } else {
                    k.run(
                        &bg(),
                        pb::RunRequest {
                            hive: "team-a".into(),
                            actor_name: "actor-1".into(),
                            actor_uid: UID.into(),
                            worker_pod_uid: POD.into(),
                            sandbox_runtime: rt,
                            spec: Some(pb::WorkloadSpec::default()),
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
                }
            }
        };
        let err = call(Some(runtime_for(&rec))).await.unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "restore={restore}: {err:?}"
        );
        assert!(err.message().contains("not advertised"), "{err:?}");
        assert_eq!(node.gcs.gets.load(Ordering::SeqCst), 0);
        assert!(
            !exists(&node.paths.actor_dir(UID)),
            "a refused call made the actor dir"
        );
        assert_eq!(call(None).await.unwrap_err().code(), Code::InvalidArgument);
    }
    // Once a worker advertises it, the same runtime resolves.
    node.advertise(&rec);
    node.runtimes
        .advertised_runtime(Some(&runtime_for(&rec)))
        .expect("advertised");
}

/// Repeated retryable worker failures to restore one snapshot end in a
/// crash; the streak starts over after giving up, a success, another
/// snapshot, a crashing failure, or a failure in the keeper's own steps,
/// and an attempt the keeper stopped waiting on leaves it as it is.
#[tokio::test]
async fn restore_stops_retrying_after_consecutive_unavailable_failures() {
    const MAX: u32 = super::MAX_CONSECUTIVE_RETRYABLE_RESTORE_FAILURES;
    let mut a = run_plugin_actor(NodeOpts::default()).await;
    let snap = a.checkpointed("pause-snap-1", 1).await;
    let mut req = a.restore_request(&snap);
    async fn code(a: &PluginActor, req: &pb::RestoreRequest, ctx: &Ctx) -> Code {
        match a.k().restore(ctx, req.clone()).await {
            Ok(_) => Code::Ok,
            Err(e) => e.code(),
        }
    }
    async fn fail_retryably(a: &PluginActor, req: &pb::RestoreRequest, times: u32, after: &str) {
        *a.worker().restore_err.lock() = Some(Status::unavailable(
            "while reseeding guest CRNG: context deadline exceeded",
        ));
        for i in 1..=times {
            assert_eq!(
                code(a, req, &bg()).await,
                Code::Unavailable,
                "Restore {i} {after}"
            );
        }
    }
    fail_retryably(&a, &req, MAX - 1, "at first").await;
    assert_eq!(code(&a, &req, &bg()).await, Code::Internal);

    fail_retryably(&a, &req, MAX - 1, "after giving up").await;
    *a.worker().restore_err.lock() = None;
    assert_eq!(code(&a, &req, &bg()).await, Code::Ok);
    fail_retryably(&a, &req, MAX - 1, "after a success").await;

    *a.worker().restore_err.lock() = None;
    req = a.restore_request(&a.checkpointed("pause-snap-2", 2).await);
    fail_retryably(&a, &req, MAX - 1, "of another snapshot").await;

    *a.worker().restore_err.lock() = Some(Status::internal("corrupt snapshot config"));
    assert_eq!(code(&a, &req, &bg()).await, Code::Internal);
    fail_retryably(&a, &req, MAX - 1, "after a crash").await;

    a.with_external_volumes(&["vol-1"]);
    let mut vreq = req.clone();
    vreq.spec = Some(a.spec.clone());
    *a.node.volumes.mount_err.lock() = Some(Status::internal("attach failed"));
    assert_eq!(
        code(&a, &vreq, &bg()).await,
        Code::Internal,
        "failing volume mount"
    );
    *a.node.volumes.mount_err.lock() = None;
    fail_retryably(&a, &vreq, MAX - 1, "after a failed mount").await;

    // An attempt the keeper stopped waiting on leaves the actor resuming
    // and the count as is, so the next retryable failure is the last.
    let ctx = bg();
    let c2 = ctx.clone();
    *a.worker().on_restore.lock() = Some(hook(move || {
        let c = c2.clone();
        async move { c.cancel() }
    }));
    assert_eq!(code(&a, &vreq, &ctx).await, Code::Cancelled);
    *a.worker().on_restore.lock() = None;
    assert_eq!(code(&a, &vreq, &bg()).await, Code::Internal);
}

/// Restore never reads a local snapshot from before the node's last boot:
/// it reads the upload, or fails NotFound naming the unusable copy.
#[tokio::test]
async fn restore_local_snapshot_from_earlier_boot() {
    let tarball = release_tarball("runsc bytes");
    let rec = sandbox_record(&tarball);
    let mut manifest = rec.clone();
    manifest.snapshot_files = vec!["checkpoint.img".into()];
    manifest.boot_id = "boot-1".into();
    for uploaded in [true, false] {
        let node = Node::new(
            &[("checkpoint.img", "")],
            NodeOpts {
                boot_id: "boot-2".into(),
                ..Default::default()
            },
        );
        node.serve_release(&tarball);
        node.advertise(&rec);
        if uploaded {
            let src = node.dir.path().join("remote-src");
            write_local_snapshot(
                &src,
                &manifest,
                &[("checkpoint.img", "object-storage-memory")],
            );
            node.upload(PAUSED_SNAPSHOT_URI, &src, &manifest).await;
        }
        write_local_snapshot(
            &node.paths.local_snapshot_dir(UID, "pause-snap-1"),
            &manifest,
            &[("checkpoint.img", "local-memory")],
        );
        let res = node
            .keeper
            .restore(
                &bg(),
                pb::RestoreRequest {
                    hive: HIVE.into(),
                    actor_name: ACTOR.into(),
                    actor_uid: UID.into(),
                    actor_template_hive: "default".into(),
                    actor_template_name: "counter".into(),
                    worker_pod_uid: POD.into(),
                    sandbox_runtime: Some(runtime_for(&rec)),
                    spec: Some(pb::WorkloadSpec {
                        containers: vec![pb::Container {
                            name: "app".into(),
                            image: IMAGE.into(),
                            command: vec!["/bin/app".into()],
                            ..Default::default()
                        }],
                        volumes: vec![],
                    }),
                    snapshot: Some(pb::Snapshot {
                        snapshot_uuid: "pause-snap-1".into(),
                        generation: 1,
                        fidelity: pb::SnapshotFidelity::Memory as i32,
                        object: Some(pb::ObjectStorage {
                            snapshot_uri: PAUSED_SNAPSHOT_URI.into(),
                            sharing: 0,
                        }),
                    }),
                    ..Default::default()
                },
            )
            .await;
        if uploaded {
            res.expect("Restore");
            assert_eq!(
                node.worker.restored("checkpoint.img").as_deref(),
                Some("object-storage-memory")
            );
            assert!(!node.worker.preserved());
        } else {
            let err = res.unwrap_err();
            assert_eq!(err.code(), Code::NotFound, "{err:?}");
            assert!(err.message().contains("unusable local snapshot"), "{err:?}");
        }
    }
}

/// How a failed RestoreWorkload reaches the control plane.
#[test]
fn worker_restore_error_cases() {
    use super::restore::worker_restore_error;
    let live = Ctx::background();
    let canceled = Ctx::background();
    canceled.cancel();
    for (name, ctx, err, want) in [
        (
            "worker Unavailable stays Unavailable",
            &live,
            Status::unavailable("while reseeding guest CRNG: context deadline exceeded"),
            Code::Unavailable,
        ),
        (
            "worker DeadlineExceeded with a live caller is Internal",
            &live,
            Status::deadline_exceeded("context deadline exceeded"),
            Code::Internal,
        ),
        (
            "worker Canceled with a live caller is Internal",
            &live,
            Status::cancelled("context canceled"),
            Code::Internal,
        ),
        (
            "worker Internal is Internal",
            &live,
            Status::internal("corrupt snapshot config"),
            Code::Internal,
        ),
        (
            "a canceled caller reports Canceled",
            &canceled,
            Status::internal("worker aborted"),
            Code::Cancelled,
        ),
    ] {
        let got = worker_restore_error(ctx, err.clone());
        assert_eq!(got.code(), want, "{name}: {got:?}");
        assert!(
            got.message().contains(err.message()),
            "{name}: {got:?} drops the cause"
        );
    }
}
