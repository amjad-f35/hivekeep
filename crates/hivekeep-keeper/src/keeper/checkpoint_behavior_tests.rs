use super::*;

#[test]
fn checkpoint_snapshot_files_cases() {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    for (name, files, data, required, want_err) in [
        (
            "required and present",
            s(&["checkpoint.img"]),
            s(&[]),
            true,
            false,
        ),
        (
            "data subset",
            s(&["checkpoint.img", "data.tar"]),
            s(&["data.tar"]),
            true,
            false,
        ),
        (
            "fs subdir data subset",
            s(&["checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img"]),
            s(&["fs/fscheckpoint.pb", "fs/multitar.img"]),
            true,
            false,
        ),
        (
            "data not a snapshot file",
            s(&["checkpoint.img"]),
            s(&["data.tar"]),
            true,
            true,
        ),
        ("optional and empty", s(&[]), s(&[]), false, false),
        ("required and empty", s(&[]), s(&[]), true, true),
        (
            "escapes the directory",
            s(&["../outside"]),
            s(&[]),
            true,
            true,
        ),
        ("nested", s(&["a/b"]), s(&[]), true, true),
        ("dot", s(&["."]), s(&[]), true, true),
        (
            "unclean alias",
            s(&["checkpoint.img", "./checkpoint.img"]),
            s(&[]),
            true,
            true,
        ),
        (
            "duplicate",
            s(&["checkpoint.img", "checkpoint.img"]),
            s(&[]),
            true,
            true,
        ),
        (
            "manifest name",
            s(&["checkpoint.img", MANIFEST_NAME]),
            s(&[]),
            true,
            true,
        ),
    ] {
        let resp = wpb::CheckpointWorkloadResponse {
            snapshot_files: files,
            data_snapshot_files: data.clone(),
        };
        let got = checkpoint_snapshot_files(&resp, required);
        assert_eq!(got.is_err(), want_err, "{name}: {got:?}");
        if let Ok((_, d)) = got {
            assert_eq!(d, data, "{name}");
        }
    }
}

#[test]
fn should_have_snapshots_cases() {
    let req =
        |fidelity: pb::SnapshotFidelity, volumes: Option<Vec<pb::Volume>>| pb::CheckpointRequest {
            snapshot: Some(pb::Snapshot {
                fidelity: fidelity as i32,
                ..Default::default()
            }),
            spec: volumes.map(|volumes| pb::WorkloadSpec {
                volumes,
                containers: vec![],
            }),
            ..Default::default()
        };
    let durable = pb::Volume {
        name: "durable".into(),
        durable_dir: Some(pb::DurableDirVolumeSource {}),
        ..Default::default()
    };
    let csi = pb::Volume {
        name: "csi".into(),
        external: Some(Default::default()),
        ..Default::default()
    };
    for (name, r, want) in [
        (
            "full fidelity always expects snapshots",
            req(pb::SnapshotFidelity::Memory, None),
            true,
        ),
        (
            "rootfs fidelity always expects snapshots",
            req(pb::SnapshotFidelity::Rootfs, None),
            true,
        ),
        (
            "volumes fidelity with durable volumes expects snapshots",
            req(pb::SnapshotFidelity::Volumes, Some(vec![durable.clone()])),
            true,
        ),
        (
            "volumes fidelity with only external volumes does not expect snapshots",
            req(pb::SnapshotFidelity::Volumes, Some(vec![csi.clone()])),
            false,
        ),
        (
            "volumes fidelity with durable and external volumes expects snapshots",
            req(pb::SnapshotFidelity::Volumes, Some(vec![durable, csi])),
            true,
        ),
        (
            "volumes fidelity with no volumes does not expect snapshots",
            req(pb::SnapshotFidelity::Volumes, Some(vec![])),
            false,
        ),
    ] {
        assert_eq!(should_have_snapshots(&r), want, "{name}");
    }
}
