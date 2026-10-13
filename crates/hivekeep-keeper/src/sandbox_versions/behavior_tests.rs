//! Sandbox versions: per-architecture records, bundle hashes, the runsc
//! feature probe, and the runtimes file.

use std::collections::BTreeMap;
use std::time::Duration;

use super::*;
use crate::assets::{AssetEntry, GVISOR_ASSET_NAME};
use std::collections::HashMap;

const SHA: &str = "d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f";

fn gvisor_config(assets: HashMap<String, BTreeMap<String, AssetFile>>) -> SandboxConfig {
    SandboxConfig {
        name: "gvisor-default".into(),
        generation: 1,
        sandbox_class: "gvisor".into(),
        default_version: "v1".into(),
        versions: vec![SandboxVersion {
            name: "v1".into(),
            state: VersionState::Enabled,
            assets,
        }],
    }
}

fn gvisor_files() -> BTreeMap<String, AssetFile> {
    BTreeMap::from([(
        GVISOR_ASSET_NAME.to_string(),
        AssetFile {
            url: "gs://bucket/gvisor".into(),
            sha256: SHA.into(),
        },
    )])
}

#[test]
fn record_for_this_architecture() {
    let cfg = gvisor_config(HashMap::from([(node_arch().to_string(), gvisor_files())]));
    let mut want = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    };
    want.assets.insert(
        GVISOR_ASSET_NAME.into(),
        AssetEntry {
            url: "gs://bucket/gvisor".into(),
            sha256: SHA.into(),
        },
    );
    assert_eq!(record_for_arch(&cfg, &cfg.versions[0]), Some(want));

    let cfg = gvisor_config(HashMap::from([("other-arch".to_string(), gvisor_files())]));
    assert_eq!(record_for_arch(&cfg, &cfg.versions[0]), None);
}

fn rec(class: &str, assets: &[(&str, &str, &str)]) -> SandboxAssetsRecord {
    let mut r = SandboxAssetsRecord {
        sandbox_class: class.into(),
        ..Default::default()
    };
    for (name, url, sha) in assets {
        r.assets.insert(
            name.to_string(),
            AssetEntry {
                url: url.to_string(),
                sha256: sha.to_string(),
            },
        );
    }
    r
}

/// The hash covers each asset's name and digest only; these values are
/// what workers already advertise, so they must not change.
#[test]
fn bundle_hash_values() {
    for (name, r, want) in [
        (
            "gvisor asset",
            rec("gvisor", &[(GVISOR_ASSET_NAME, "gs://bucket/gvisor", SHA)]),
            "sha256:f86e6f2bd8e25ac31a22e51e6e57e0eb6741eb0b0df77082989cc1e40f906fe1",
        ),
        (
            "another URL and class",
            rec("microvm", &[(GVISOR_ASSET_NAME, "gs://other/gvisor", SHA)]),
            "sha256:f86e6f2bd8e25ac31a22e51e6e57e0eb6741eb0b0df77082989cc1e40f906fe1",
        ),
        (
            "the asset under another name",
            rec("gvisor", &[("renamed", "gs://bucket/gvisor", SHA)]),
            "sha256:510948cc43e084528a7c76c1627c5bfb2d140be0b620f33282c03fc2debd65af",
        ),
        (
            "several assets",
            rec("microvm", &[("kernel", "", "aaaa"), ("rootfs", "", "bbbb")]),
            "sha256:fc253c8ca0d24c485ef86a377c0cd2cf97fbf922e9cabf290971c55f051158ba",
        ),
    ] {
        assert_eq!(r.bundle_hash(), want, "{name}");
    }
}

fn script(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let p = dir.join(name);
    std::fs::write(&p, format!("#!/bin/sh\n{body}")).unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    p
}

#[tokio::test]
async fn runs_runsc_cpu_features() {
    let d = tempfile::tempdir().unwrap();
    let t = CPU_FEATURES_TIMEOUT;
    let got = run_cpu_features(&script(d.path(), "ok", "echo pni,avx2\n"), t)
        .await
        .unwrap();
    assert_eq!(got, vec!["pni", "avx2"]);

    let err = run_cpu_features(&script(d.path(), "fail", "echo boom >&2\nexit 1\n"), t)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, SyncError::Permanent(m) if m.contains("boom")),
        "{err:?}"
    );

    let err = run_cpu_features(&script(d.path(), "empty", "exit 0\n"), t)
        .await
        .unwrap_err();
    assert!(matches!(err, SyncError::Permanent(_)), "{err:?}");

    let err = run_cpu_features(&d.path().join("missing"), t)
        .await
        .unwrap_err();
    assert!(
        !matches!(err, SyncError::Permanent(_)),
        "a missing binary is not permanent: {err:?}"
    );

    let err = run_cpu_features(
        &script(d.path(), "hang", "exec sleep 10\n"),
        Duration::from_millis(50),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, SyncError::Other(m) if m.contains("timed out")),
        "a hanging binary: {err:?}"
    );
}

fn runtimes(default_hash: &str) -> RuntimesFile {
    RuntimesFile {
        sandbox_classes: vec![RuntimesClass {
            sandbox_class: "gvisor".into(),
            default_runtime: Runtime {
                version: BTreeMap::from([(
                    "sandbox_bundle_hash".to_string(),
                    default_hash.to_string(),
                )]),
            },
            restorable_runtimes: vec![Runtime {
                version: BTreeMap::from([(
                    "sandbox_bundle_hash".to_string(),
                    "sha256:41ab".to_string(),
                )]),
            }],
        }],
    }
}

#[test]
fn writes_the_runtimes_file() {
    // (name, existing, missing dir, file, want wrote, want error)
    for (name, existing, no_dir, v, want_wrote, want_err) in [
        (
            "new file",
            None,
            false,
            runtimes("sha256:7c2e"),
            true,
            false,
        ),
        (
            "equal content is not rewritten",
            Some(runtimes("sha256:7c2e")),
            false,
            runtimes("sha256:7c2e"),
            false,
            false,
        ),
        (
            "different content replaces the file",
            Some(runtimes("sha256:7c2e")),
            false,
            runtimes("sha256:9d01"),
            true,
            false,
        ),
        (
            "missing directory",
            None,
            true,
            runtimes("sha256:7c2e"),
            false,
            true,
        ),
    ] {
        let d = tempfile::tempdir().unwrap();
        let dir = if no_dir {
            d.path().join("missing")
        } else {
            d.path().to_path_buf()
        };
        let path = dir.join("sandbox-runtimes.json");
        let mut before = None;
        if let Some(e) = &existing {
            write_runtimes_file(&path, e).unwrap();
            before = Some(std::fs::metadata(&path).unwrap().modified().unwrap());
            std::thread::sleep(Duration::from_millis(10));
        }
        let res = write_runtimes_file(&path, &v);
        assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
        if want_err {
            assert_eq!(
                std::fs::read_dir(d.path()).unwrap().count(),
                0,
                "{name}: a temporary file was left"
            );
            continue;
        }
        assert_eq!(res.unwrap(), want_wrote, "{name}");
        assert_eq!(read_runtimes_file(&path).unwrap(), v, "{name}");
        if let Some(b) = before {
            let changed = std::fs::metadata(&path).unwrap().modified().unwrap() > b;
            assert_eq!(changed, want_wrote, "{name}: mtime changed");
        }
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "{name}: want only the file"
        );
    }
}

#[test]
fn reads_the_runtimes_file() {
    let valid = r#"{"sandboxClasses": [{"sandboxClass": "gvisor", "defaultRuntime": {"version": {"sandbox_bundle_hash": "sha256:7c2e"}}, "restorableRuntimes": [{"version": {"sandbox_bundle_hash": "sha256:41ab"}}]}]}"#;
    let mut want = runtimes("sha256:7c2e");
    want.sandbox_classes[0].restorable_runtimes[0] = Runtime {
        version: BTreeMap::from([("sandbox_bundle_hash".to_string(), "sha256:41ab".to_string())]),
    };
    for (name, contents, want) in [
        ("missing file", None, None),
        ("corrupt file", Some("{not json"), None),
        (
            "unknown field",
            Some(
                r#"{"sandboxClasses": [{"sandboxClass": "gvisor", "defaultRuntime": {"version": {"sandbox_bundle_hash": "sha256:7c2e"}}}], "nope": 1}"#,
            ),
            None,
        ),
        ("valid file", Some(valid), Some(want)),
    ] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("sandbox-runtimes.json");
        if let Some(c) = contents {
            std::fs::write(&path, c).unwrap();
        }
        let got = read_runtimes_file(&path);
        assert_eq!(got.as_ref().ok(), want.as_ref(), "{name}: {got:?}");
        if contents.is_none() {
            // A missing file stays recognizable as one.
            assert!(!path.exists());
        }
    }
}

/// The production probe encodes with the shared cpu_features bitmask, so
/// equal feature sets advertise equal values whatever their order.
#[test]
fn system_probe_encodes_the_shared_bitmask() {
    let p = SystemHostProbe;
    let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(p.encode_cpu_features("amd64", &[]).unwrap(), "0");
    assert_eq!(
        p.encode_cpu_features("arm64", &names(&["fp", " ", "asimd"]))
            .unwrap(),
        "3"
    );
    assert_eq!(
        p.encode_cpu_features("amd64", &names(&["avx", "sse4_1"]))
            .unwrap(),
        p.encode_cpu_features("amd64", &names(&["sse4_1", "avx", "avx"]))
            .unwrap(),
    );
    let err = p
        .encode_cpu_features("amd64", &names(&["no-such-feature"]))
        .unwrap_err();
    assert!(err.contains("no-such-feature"), "{err}");
    assert!(p.encode_cpu_features("riscv64", &names(&["fp"])).is_err());
}
