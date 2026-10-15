use super::*;
use crate::cdi::{Device, DeviceNode, Edits, Mount};
use std::os::unix::fs::symlink;

/// Stands in for the trusted hook binary a caller supplies.
const TEST_HOOK_BINARY: &str = "/opt/toolkit/cdi-hook";

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// A device node's numbers, so expectations match the host the test runs on.
fn stat_dev(path: &str) -> (i64, i64) {
    use std::os::unix::fs::MetadataExt;
    dev_numbers(std::fs::metadata(path).unwrap().rdev())
}

/// The options the gVisor worker uses, applying the "all" device of the
/// spec in `spec_dir`.
fn inject_fixture(bundle: &Path, spec_dir: &Path) -> Result<()> {
    let data = std::fs::read(spec_dir.join("nvidia.json"))
        .map_err(|e| Error::io("reading the spec", e))?;
    let spec = cdi::parse(&data)?;
    into_bundle(
        bundle,
        &spec,
        &Options {
            devices: s(&["all"]),
            allowed_hooks: ["create-symlinks", "enable-cuda-compat"]
                .map(String::from)
                .into(),
            hook_binary: TEST_HOOK_BINARY.into(),
            allowed_host_dirs: s(&["/usr/local/nvidia"]),
            library_dirs: s(&["/usr/local/nvidia/lib64"]),
            drop_env: s(&["NVIDIA_VISIBLE_DEVICES"]),
        },
    )
}

fn write_spec(dir: &Path, json: &str) -> PathBuf {
    let spec_dir = dir.join("cdi");
    std::fs::create_dir_all(&spec_dir).unwrap();
    std::fs::write(spec_dir.join("nvidia.json"), json).unwrap();
    spec_dir
}

/// A bundle with an empty rootfs (already mounted when injection runs) and
/// the given process.
fn new_bundle(dir: &Path, process: Value) -> PathBuf {
    let bundle = dir.join("bundle");
    std::fs::create_dir_all(bundle.join("rootfs")).unwrap();
    let cfg = json!({"ociVersion": "1.0.0", "process": process});
    std::fs::write(bundle.join("config.json"), cfg.to_string()).unwrap();
    bundle
}

fn read_cfg(bundle: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(bundle.join("config.json")).unwrap()).unwrap()
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn inject_gpu_into_bundle() {
    let dir = tempfile::tempdir().unwrap();
    // One node with its numbers left out (as nvidia-ctk emits), so they
    // must come from the host: /dev/null is everywhere. The hooks are kept
    // (allowlisted), excluded (update-ldcache) and unknown (a newer
    // toolkit's), which is dropped rather than run unreviewed.
    let spec_dir = write_spec(
        dir.path(),
        r#"{
  "cdiVersion": "0.6.0",
  "kind": "nvidia.com/gpu",
  "devices": [
    {
      "name": "all",
      "containerEdits": {
        "deviceNodes": [{"path": "/dev/null"}],
        "env": ["NVIDIA_TEST=1"],
        "hooks": [
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "create-symlinks", "--link", "a::b"]},
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "update-ldcache", "--folder", "/usr/local/nvidia/lib64"]},
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "some-future-hook", "--flag", "v"]}
        ]
      }
    }
  ]
}"#,
    );
    // The CUDA base image sets NVIDIA_VISIBLE_DEVICES; it must go.
    let bundle = new_bundle(
        dir.path(),
        json!({"args": ["true"], "env": ["NVIDIA_VISIBLE_DEVICES=all"]}),
    );
    inject_fixture(&bundle, &spec_dir).unwrap();

    let got = read_cfg(&bundle);
    let dev = got["linux"]["devices"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["path"] == "/dev/null")
        .expect("/dev/null injected")
        .clone();
    let (major, minor) = stat_dev("/dev/null");
    assert_eq!(dev["type"], "c");
    assert_eq!(
        (dev["major"].as_i64(), dev["minor"].as_i64()),
        (Some(major), Some(minor))
    );
    let env = strings(&got["process"]["env"]);
    assert!(env.contains(&"NVIDIA_TEST=1".to_string()), "{env:?}");
    // Stripped, so runsc's nvproxy does not invoke nvidia-container-cli.
    assert!(
        !env.iter().any(|e| e.starts_with("NVIDIA_VISIBLE_DEVICES=")),
        "{env:?}"
    );
    let hooks = got["hooks"]["createContainer"].as_array().unwrap();
    let kept: Vec<_> = hooks
        .iter()
        .map(|h| {
            assert!(
                h["path"].as_str().unwrap().starts_with(TEST_HOOK_BINARY),
                "{h}"
            );
            strings(&h["args"])[1].clone()
        })
        .collect();
    assert_eq!(kept, ["create-symlinks"]);
}

/// The two cases that decide whether a CUDA program loads libcuda.so.1.
#[test]
fn prepend_library_path_cases() {
    let dirs = s(&["/usr/local/nvidia/lib64"]);
    for (name, env, want) in [
        (
            "no existing value",
            s(&["PATH=/bin"]),
            "LD_LIBRARY_PATH=/usr/local/nvidia/lib64",
        ),
        (
            "template value is kept after the driver dir",
            s(&["LD_LIBRARY_PATH=/opt/app/lib"]),
            "LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/opt/app/lib",
        ),
        (
            "already present is left alone",
            s(&["LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/x"]),
            "LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/x",
        ),
    ] {
        let got = prepend_library_path(env, &dirs);
        let found: Vec<_> = got
            .iter()
            .filter(|e| e.starts_with("LD_LIBRARY_PATH="))
            .collect();
        assert_eq!(found, [want], "{name}");
    }
}

/// The keeper re-unpacks the bundle before each Run and Restore, and the
/// edits are appends, so without the guard a second injection would double
/// every device, mount, env entry and hook.
#[test]
fn inject_gpu_into_bundle_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    // Explicit numbers, so the device need not exist on the test host.
    let spec_dir = write_spec(
        dir.path(),
        r#"{
  "cdiVersion": "0.6.0",
  "kind": "nvidia.com/gpu",
  "devices": [
    {
      "name": "all",
      "containerEdits": {
        "deviceNodes": [{"path": "/dev/nvidia0", "major": 195, "minor": 0, "type": "c"}],
        "env": ["NVIDIA_TEST=1"],
        "hooks": [
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "create-symlinks", "--link", "a::b"]}
        ]
      }
    }
  ]
}"#,
    );
    let bundle = new_bundle(dir.path(), json!({"args": ["true"]}));
    let shape = || {
        let c = read_cfg(&bundle);
        let len = |v: &Value| v.as_array().map_or(0, Vec::len);
        [
            len(&c["linux"]["devices"]),
            len(&c["mounts"]),
            len(&c["process"]["env"]),
            len(&c["hooks"]["createContainer"]),
        ]
    };
    inject_fixture(&bundle, &spec_dir).unwrap();
    let first = shape();
    assert_ne!(first[0], 0, "no devices added");
    inject_fixture(&bundle, &spec_dir).unwrap();
    assert_eq!(shape(), first, "(devices, mounts, env, hooks)");
}

/// The two ways staging could write into the worker's mount namespace: a
/// directory the image redirects out of the rootfs, and a SONAME that is
/// not a bare file name.
#[test]
fn stage_soname_symlinks_confined_to_rootfs() {
    for (name, soname, escape) in [
        (
            "image redirects the driver dir out of the rootfs",
            "libcuda.so.1",
            Some("usr/lib/gpu"),
        ),
        (
            "SONAME climbs out with ../",
            "../../../../escaped.so.1",
            None,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let rootfs = dir.path().join("rootfs");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(rootfs.join("usr/lib")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let victim = outside.join("libcuda.so.1");
        std::fs::write(&victim, "do not delete").unwrap();
        if let Some(e) = escape {
            symlink(&outside, rootfs.join(e)).unwrap();
        }
        let host_dir = dir.path().join("host");
        std::fs::create_dir_all(&host_dir).unwrap();
        let host_lib = host_dir.join("libcuda.so.580.65.06");
        std::fs::write(&host_lib, "stub").unwrap();

        let reader = move |_: &Path, _: &Path| Ok(Some(soname.to_string()));
        // Refusing outright and skipping are both fine; writing outside is not.
        let _ = stage_soname_symlinks_with(
            &rootfs,
            &[BundleMount {
                source: host_lib.display().to_string(),
                destination: "/usr/lib/gpu/libcuda.so.580.65.06".into(),
            }],
            &[host_dir.display().to_string()],
            &reader,
        );
        assert_eq!(
            std::fs::read_to_string(&victim).unwrap(),
            "do not delete",
            "{name}"
        );
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1, "{name}");
    }
}

#[test]
fn inject_gpu_into_bundle_fails_without_a_spec() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = new_bundle(dir.path(), json!({}));
    let empty = dir.path().join("cdi-empty");
    std::fs::create_dir_all(&empty).unwrap();
    assert!(inject_fixture(&bundle, &empty).is_err());
}

fn spec_for_mount(m: Mount) -> Spec {
    Spec {
        devices: vec![Device {
            name: "all".into(),
            container_edits: Edits {
                device_nodes: vec![DeviceNode {
                    path: "/dev/nvidia0".into(),
                    kind: "c".into(),
                    major: 195,
                    minor: 0,
                }],
                mounts: vec![m],
                ..Default::default()
            },
        }],
        ..Default::default()
    }
}

#[test]
fn into_bundle_host_mounts_allowlist_and_confinement() {
    let tmp = tempfile::tempdir().unwrap();
    // Canonical, so the allowed dir is not itself behind a symlink.
    let dir = std::fs::canonicalize(tmp.path()).unwrap();
    let allowed = dir.join("allowed-driver");
    let outside = dir.join("outside");
    std::fs::create_dir_all(allowed.join("lib64")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let valid_lib = allowed.join("lib64/libcuda.so.580.65.06");
    std::fs::write(&valid_lib, "stub-lib").unwrap();
    let outside_secret = outside.join("libsecret.so.1.0");
    std::fs::write(&outside_secret, "secret").unwrap();
    let escape_file = allowed.join("lib64/libescape.so.1.0");
    symlink(&outside_secret, &escape_file).unwrap();
    let escape_rel = allowed.join("lib64/librel-escape.so.1.0");
    symlink("../../outside/libsecret.so.1.0", &escape_rel).unwrap();
    let escape_dir = allowed.join("escaped-dir");
    symlink(&outside, &escape_dir).unwrap();

    let reader = |_: &Path, _: &Path| Ok(Some("libcuda.so.1".to_string()));
    let host = Host {
        read_soname: &reader,
        ..Host::default()
    };
    let p = |p: &Path| p.display().to_string();
    let allowed_dirs = vec![p(&allowed)];
    let m = |host: String, container: &str| Mount {
        host_path: host,
        container_path: container.into(),
        ..Default::default()
    };
    let cases: Vec<(&str, Mount, Vec<String>, bool)> = vec![
        (
            "allowed regular file mount succeeds",
            m(
                p(&valid_lib),
                "/usr/local/nvidia/lib64/libcuda.so.580.65.06",
            ),
            allowed_dirs.clone(),
            false,
        ),
        (
            "empty allowlist rejects mount",
            m(
                p(&valid_lib),
                "/usr/local/nvidia/lib64/libcuda.so.580.65.06",
            ),
            vec![],
            true,
        ),
        (
            "hostPath outside allowed directory is rejected",
            m(
                p(&outside_secret),
                "/usr/local/nvidia/lib64/libsecret.so.1.0",
            ),
            allowed_dirs.clone(),
            true,
        ),
        (
            "unclean hostPath with dot-dot is rejected",
            m(
                format!("{}/../outside/libsecret.so.1.0", p(&allowed)),
                "/usr/local/nvidia/lib64/libsecret.so.1.0",
            ),
            allowed_dirs.clone(),
            true,
        ),
        (
            "absolute symlink escaping allowed directory is rejected",
            m(p(&escape_file), "/usr/local/nvidia/lib64/libescape.so.1.0"),
            allowed_dirs.clone(),
            true,
        ),
        (
            "relative symlink escaping allowed directory is rejected",
            m(
                p(&escape_rel),
                "/usr/local/nvidia/lib64/librel-escape.so.1.0",
            ),
            allowed_dirs.clone(),
            true,
        ),
        (
            "parent directory symlink escaping allowed directory is rejected",
            m(
                p(&escape_dir.join("libsecret.so.1.0")),
                "/usr/local/nvidia/lib64/libsecret.so.1.0",
            ),
            allowed_dirs.clone(),
            true,
        ),
        (
            "relative containerPath is rejected",
            m(p(&valid_lib), "usr/local/nvidia/lib64/libcuda.so.580.65.06"),
            allowed_dirs.clone(),
            true,
        ),
        (
            "unclean containerPath is rejected",
            m(p(&valid_lib), "/usr/local/../../escaped"),
            allowed_dirs.clone(),
            true,
        ),
    ];
    for (name, mount, allowed_host_dirs, want_err) in cases {
        let b = tempfile::tempdir().unwrap();
        let bundle = new_bundle(b.path(), json!({"args": ["true"]}));
        let got = into_bundle_on(
            &bundle,
            &spec_for_mount(mount),
            &Options {
                devices: s(&["all"]),
                allowed_host_dirs,
                ..Default::default()
            },
            &host,
        );
        assert_eq!(got.is_err(), want_err, "{name}: {got:?}");
        if !want_err {
            assert_eq!(
                std::fs::read_link(bundle.join("rootfs/usr/local/nvidia/lib64/libcuda.so.1"))
                    .unwrap(),
                Path::new("libcuda.so.580.65.06"),
                "{name}"
            );
        }
    }
}

#[test]
fn into_bundle_device_path_confinement() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = std::fs::canonicalize(tmp.path()).unwrap();
    let fake_dev = dir.join("dev");
    std::fs::create_dir_all(&fake_dev).unwrap();
    std::fs::create_dir_all(dir.join("outside")).unwrap();
    let regular = fake_dev.join("not-a-device");
    std::fs::write(&regular, "plain").unwrap();
    let escape = fake_dev.join("escaped-dev");
    symlink("/dev/null", &escape).unwrap();
    let host = Host {
        dev_root: fake_dev.clone(),
        ..Host::default()
    };
    let p = |p: &Path| p.display().to_string();
    let node = |path: String, major: i64, minor: i64| DeviceNode {
        path,
        major,
        minor,
        ..Default::default()
    };
    for (name, dev) in [
        (
            "device path outside the device root is rejected",
            node("/etc/shadow".into(), 195, 0),
        ),
        (
            "unclean device path with dot-dot is rejected",
            node(format!("{}/../outside/node", p(&fake_dev)), 195, 0),
        ),
        (
            "symlink escaping the device root is rejected when major is 0",
            node(p(&escape), 0, 0),
        ),
        (
            "symlink escaping the device root is rejected even when major is set",
            node(p(&escape), 1, 3),
        ),
        (
            "regular file inside the device root is rejected",
            node(p(&regular), 0, 0),
        ),
    ] {
        let b = tempfile::tempdir().unwrap();
        let bundle = new_bundle(b.path(), json!({"args": ["true"]}));
        let spec = Spec {
            devices: vec![Device {
                name: "all".into(),
                container_edits: Edits {
                    device_nodes: vec![dev],
                    ..Default::default()
                },
            }],
            ..Default::default()
        };
        let got = into_bundle_on(
            &bundle,
            &spec,
            &Options {
                devices: s(&["all"]),
                ..Default::default()
            },
            &host,
        );
        assert!(got.is_err(), "{name}");
    }
}

#[test]
fn stage_soname_symlinks_host_confinement() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = std::fs::canonicalize(tmp.path()).unwrap();
    let allowed = dir.join("allowed");
    let outside = dir.join("outside");
    let rootfs = dir.join("rootfs");
    for d in [&allowed, &outside, &rootfs] {
        std::fs::create_dir_all(d).unwrap();
    }
    let outside_file = outside.join("liboutside.so.1.0");
    std::fs::write(&outside_file, "not-allowed").unwrap();
    let escape = allowed.join("libescape.so.1.0");
    symlink(&outside_file, &escape).unwrap();
    let allowed_dirs = [allowed.display().to_string()];

    // A host path that is not allowed.
    assert!(stage_soname_symlinks(
        &rootfs,
        &[BundleMount {
            source: outside_file.display().to_string(),
            destination: "/usr/lib/liboutside.so.1.0".into(),
        }],
        &allowed_dirs,
    )
    .is_err());
    // A symlink inside the allowed dir pointing outside it.
    assert!(stage_soname_symlinks(
        &rootfs,
        &[BundleMount {
            source: escape.display().to_string(),
            destination: "/usr/lib/libescape.so.1.0".into(),
        }],
        &allowed_dirs,
    )
    .is_err());
    assert!(elf_soname_in(&allowed, Path::new("libescape.so.1.0")).is_err());
}

#[test]
fn elf_soname_reads_a_real_library_layout() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("libx.so.1.2"),
        crate::elf_soname::tests::elf64(Some("libx.so.1")),
    )
    .unwrap();
    assert_eq!(
        elf_soname_in(dir.path(), Path::new("libx.so.1.2"))
            .unwrap()
            .as_deref(),
        Some("libx.so.1")
    );
    std::fs::create_dir(dir.path().join("d.so.1")).unwrap();
    assert!(elf_soname_in(dir.path(), Path::new("d.so.1")).is_err());
}

#[test]
fn clean_matches_lexical_rules() {
    for (input, want) in [
        ("/a/b/../c", "/a/c"),
        ("/a//b/", "/a/b"),
        ("/../x", "/x"),
        ("a/../../b", "../b"),
        ("", "."),
        ("/", "/"),
    ] {
        assert_eq!(clean(input), want, "{input}");
    }
}
