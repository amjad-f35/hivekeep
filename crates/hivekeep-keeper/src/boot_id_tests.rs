use super::*;
use crate::assets::MANIFEST_NAME;

#[test]
fn read_boot_id_reads_and_trims() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("boot_id");
    let want = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f";
    std::fs::write(&p, format!("{want}\n")).unwrap();
    assert_eq!(read_boot_id(&p).unwrap(), want);
}

#[test]
fn read_boot_id_rejects_an_empty_file() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("boot_id");
    std::fs::write(&p, " \n\t").unwrap();
    assert!(read_boot_id(&p).is_err());
}

#[test]
fn read_boot_id_rejects_a_missing_file() {
    let d = tempfile::tempdir().unwrap();
    assert!(read_boot_id(&d.path().join("missing")).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn read_boot_id_from_host() {
    let got = read_boot_id(Path::new(BOOT_ID_PATH)).unwrap();
    assert!(!got.is_empty());
}

#[test]
fn local_snapshot_boot_check_cases() {
    const ACTOR: &str = "01234567-89ab-cdef-0123-456789abcdef";
    const SNAP: &str = "pause-snap-1";
    for (name, manifest, want) in [
        (
            "records the current boot",
            Some(r#"{"bootId":"boot-2"}"#),
            true,
        ),
        (
            "records an earlier boot",
            Some(r#"{"bootId":"boot-1"}"#),
            false,
        ),
        ("records no boot", Some("{}"), false),
        ("does not parse", Some(r#"{"bootId":"#), false),
        ("is missing", None, false),
    ] {
        let d = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(d.path()));
        if let Some(m) = manifest {
            let dir = paths.local_snapshot_dir(ACTOR, SNAP);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(MANIFEST_NAME), m).unwrap();
        }
        let check = local_snapshot_boot_check(paths, "boot-2".into());
        assert_eq!(check(ACTOR, SNAP), want, "{name}");
    }
}
