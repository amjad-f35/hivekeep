use super::*;

#[test]
fn snapshot_manifest_actor_metadata() {
    let rec = SandboxAssetsRecord {
        hive: "team-a".into(),
        actor_name: "actor-1".into(),
        actor_uid: "actor-uid".into(),
        actor_template_hive: "templates".into(),
        actor_template_name: "agent".into(),
        fidelity: crate::labels::fidelity::MEMORY.into(),
        ..Default::default()
    };
    let got = String::from_utf8(rec.to_json()).unwrap();
    for want in [
        r#""hive":"team-a""#,
        r#""actorName":"actor-1""#,
        r#""actorUid":"actor-uid""#,
        r#""actorTemplateHive":"templates""#,
        r#""actorTemplateName":"agent""#,
        r#""fidelity":"memory""#,
    ] {
        assert!(got.contains(want), "manifest {got} missing {want}");
    }
}

/// The on-node record written at Run and Restore has no fidelity: it
/// parses back with none and serializes no fidelity key.
#[test]
fn snapshot_manifest_fidelity_absent() {
    let legacy = br#"{"sandboxClass":"gvisor","snapshotFiles":["checkpoint.img"]}"#;
    let rec = SandboxAssetsRecord::parse(legacy).unwrap();
    assert_eq!(rec.fidelity, "");
    let got = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    }
    .to_json();
    assert!(
        !String::from_utf8(got.clone())
            .unwrap()
            .contains(r#""fidelity""#),
        "fidelity-less record serialized a fidelity key: {}",
        String::from_utf8_lossy(&got)
    );
}

#[test]
fn snapshot_manifest_rejects_non_local_file() {
    let m = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        snapshot_files: vec!["../outside".into()],
        ..Default::default()
    }
    .to_json();
    assert!(
        SandboxAssetsRecord::parse(&m).is_err(),
        "accepted a path outside the checkpoint directory"
    );
}

#[test]
fn snapshot_manifest_rejects_data_file_not_in_snapshot_files() {
    let m = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        snapshot_files: vec!["checkpoint.img".into()],
        data_snapshot_files: vec!["data.tar".into()],
        ..Default::default()
    }
    .to_json();
    assert!(
        SandboxAssetsRecord::parse(&m).is_err(),
        "accepted a data file that is not one of the snapshot files"
    );
}

/// A manifest naming nothing but what it must parses: every field has a
/// default, as a record written by an older keeper may lack any of them.
#[test]
fn snapshot_manifest_fields_are_optional() {
    let rec = SandboxAssetsRecord::parse(b"{}").unwrap();
    assert_eq!(rec, SandboxAssetsRecord::default());
    let rec = SandboxAssetsRecord::parse(br#"{"bootId":"boot-1"}"#).unwrap();
    assert_eq!(rec.boot_id, "boot-1");
}
