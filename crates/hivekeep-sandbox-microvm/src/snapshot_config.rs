//! Repointing a snapshot's per-Actor paths at the restoring Actor.
//!
//! The VMM serializes the config the VM booted with into the snapshot and
//! reopens every path in it at restore, so each must exist on this node for
//! this Actor: the hybrid-vsock socket, the console capture files, the
//! virtio-fs socket, and the kernel and guest image the keeper staged under
//! this Actor's directory (the source Actor's copies may be gone).

use std::io::{self, Write};
use std::path::Path;

use serde_json::Value;

use crate::kata::paths::{
    console_log_path, serial_log_path, virtiofsd_socket_path, vsock_socket_path, FS_TAG,
};
use crate::vm_boot::GUEST_IMAGE_PMEM_ID;

/// The file in a snapshot holding the frozen base sandbox id: the id the
/// guest's virtio-fs paths were first shared under, invariant across every
/// restore of the lineage (the config's socket paths are not: they are
/// rewritten to the current Actor on each restore).
pub const BASE_ID_FILE: &str = "base-id";

fn set_str(obj: &mut serde_json::Map<String, Value>, key: &str, want: &str, changed: &mut bool) {
    if obj.get(key).and_then(Value::as_str) != Some(want) {
        obj.insert(key.into(), Value::from(want));
        *changed = true;
    }
}

/// Rewrites `config.json` in `dir` for Actor `id` with its `kernel` and
/// `image`. A config already pointing at them is left untouched, so a
/// preserved snapshot stays as it was. The write goes through a temporary
/// file and a rename: truncating in place would write through a shared
/// inode.
pub fn rewrite_snapshot_paths(dir: &Path, id: &str, kernel: &str, image: &str) -> io::Result<()> {
    let path = dir.join("config.json");
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path)?)
        .map_err(|e| io::Error::other(format!("parsing {}: {e}", path.display())))?;
    let bad = |msg: String| io::Error::other(format!("snapshot config {} {msg}", path.display()));
    let obj = cfg
        .as_object_mut()
        .ok_or_else(|| bad("is not an object".into()))?;
    let mut changed = false;
    if let Some(v) = obj.get_mut("vsock").and_then(Value::as_object_mut) {
        set_str(
            v,
            "socket",
            &vsock_socket_path(id).to_string_lossy(),
            &mut changed,
        );
    }
    for (key, want) in [
        ("console", console_log_path(id)),
        ("serial", serial_log_path(id)),
    ] {
        if let Some(dev) = obj.get_mut(key).and_then(Value::as_object_mut) {
            if dev.get("mode").and_then(Value::as_str) == Some("File") {
                set_str(dev, "file", &want.to_string_lossy(), &mut changed);
            }
        }
    }
    if let Some(fss) = obj.get_mut("fs").and_then(Value::as_array_mut) {
        for f in fss {
            let f = f
                .as_object_mut()
                .ok_or_else(|| bad("has a malformed fs device".into()))?;
            match f.get("tag").and_then(Value::as_str).unwrap_or_default() {
                FS_TAG => set_str(
                    f,
                    "socket",
                    &virtiofsd_socket_path(id).to_string_lossy(),
                    &mut changed,
                ),
                other => return Err(bad(format!("has fs device with unknown tag {other:?}"))),
            }
        }
    }
    let payload = obj
        .get_mut("payload")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| bad("has no payload".into()))?;
    set_str(payload, "kernel", kernel, &mut changed);
    let mut guest_image = false;
    if let Some(pmems) = obj.get_mut("pmem").and_then(Value::as_array_mut) {
        for pm in pmems {
            let pm = pm
                .as_object_mut()
                .ok_or_else(|| bad("has a malformed pmem device".into()))?;
            match pm.get("id").and_then(Value::as_str).unwrap_or_default() {
                GUEST_IMAGE_PMEM_ID => {
                    guest_image = true;
                    set_str(pm, "file", image, &mut changed);
                }
                other => return Err(bad(format!("has pmem device with unknown id {other:?}"))),
            }
        }
    }
    if !guest_image {
        return Err(bad(format!("has no {GUEST_IMAGE_PMEM_ID:?} pmem device")));
    }
    if !changed {
        return Ok(());
    }
    let out = serde_json::to_vec(&cfg).map_err(io::Error::other)?;
    let tmp = dir.join(format!(".config.json.tmp-{}", std::process::id()));
    let res = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&out)?;
        drop(f);
        std::fs::rename(&tmp, &path)
    })();
    if res.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::MetadataExt;

    const ID: &str = "actor-uid";
    const KERNEL: &str = "/var/lib/hivekeep/actors/actor-uid/sandbox-assets/kata-kernel";
    const IMAGE: &str = "/var/lib/hivekeep/actors/actor-uid/sandbox-assets/kata-image";

    fn write(fs: Value) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        let cfg = json!({
            "vsock": {"cid": 3, "socket": "/run/vc/vm/golden/clh.sock"},
            "console": {"mode": "File", "file": "/run/vc/vm/golden/console.log"},
            "serial": {"mode": "File", "file": "/run/vc/vm/golden/serial.log"},
            "fs": fs,
            "payload": {"kernel": "/var/lib/hivekeep/actors/golden/sandbox-assets/kata-kernel", "cmdline": "console=hvc0"},
            "pmem": [{"id": GUEST_IMAGE_PMEM_ID, "file": "/var/lib/hivekeep/actors/golden/sandbox-assets/kata-image", "discard_writes": true}],
        });
        std::fs::write(
            d.path().join("config.json"),
            serde_json::to_vec(&cfg).unwrap(),
        )
        .unwrap();
        d
    }

    fn lower_only() -> Value {
        json!([{"tag": FS_TAG, "socket": "/run/vc/vm/golden/virtiofsd.sock"}])
    }

    fn read(d: &Path) -> Value {
        serde_json::from_slice(&std::fs::read(d.join("config.json")).unwrap()).unwrap()
    }

    fn mutate(d: &Path, f: impl FnOnce(&mut serde_json::Map<String, Value>)) {
        let mut v = read(d);
        f(v.as_object_mut().unwrap());
        std::fs::write(d.join("config.json"), serde_json::to_vec(&v).unwrap()).unwrap();
    }

    #[test]
    fn repoints_the_share_socket() {
        let d = write(lower_only());
        rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).unwrap();
        assert_eq!(
            read(d.path())["fs"][0]["socket"],
            virtiofsd_socket_path(ID).to_string_lossy().as_ref()
        );
    }

    #[test]
    fn an_unknown_fs_tag_is_an_error() {
        for tag in ["somethingElse", "hkUpper", "hkDurable", "hkCSI"] {
            let d = write(json!([{"tag": tag, "socket": "/run/vc/vm/golden/other.sock"}]));
            assert!(
                rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).is_err(),
                "{tag}"
            );
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn a_broken_config_is_an_error() {
        let cases: Vec<(&str, Box<dyn FnOnce(&mut serde_json::Map<String, Value>)>)> = vec![
            (
                "missing payload",
                Box::new(|c| {
                    c.remove("payload");
                }),
            ),
            (
                "malformed payload",
                Box::new(|c| {
                    c.insert("payload".into(), json!("vmlinux"));
                }),
            ),
            (
                "malformed pmem entry",
                Box::new(|c| {
                    c.insert("pmem".into(), json!(["not a device"]));
                }),
            ),
            (
                "missing pmem list",
                Box::new(|c| {
                    c.remove("pmem");
                }),
            ),
            (
                "missing guest image device",
                Box::new(|c| {
                    c.insert("pmem".into(), json!([]));
                }),
            ),
            (
                "unknown pmem device",
                Box::new(|c| {
                    c.insert(
                        "pmem".into(),
                        json!([{"id": "scratch-disk", "file": "/somewhere/else"}]),
                    );
                }),
            ),
        ];
        for (name, f) in cases {
            let d = write(lower_only());
            mutate(d.path(), f);
            assert!(
                rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn repoints_vsock_consoles_kernel_and_image() {
        let d = write(lower_only());
        rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).unwrap();
        let v = read(d.path());
        assert_eq!(
            v["vsock"]["socket"],
            vsock_socket_path(ID).to_string_lossy().as_ref()
        );
        assert_eq!(v["payload"]["kernel"], KERNEL);
        assert_eq!(
            v["pmem"],
            json!([{"id": GUEST_IMAGE_PMEM_ID, "file": IMAGE, "discard_writes": true}])
        );
        assert_eq!(
            v["console"]["file"],
            console_log_path(ID).to_string_lossy().as_ref()
        );
        assert_eq!(
            v["serial"]["file"],
            serial_log_path(ID).to_string_lossy().as_ref()
        );
    }

    #[test]
    fn a_hardlinked_config_is_not_written_through() {
        let d = write(lower_only());
        let cfg = d.path().join("config.json");
        let before = std::fs::read(&cfg).unwrap();
        let other = tempfile::tempdir().unwrap();
        let cached = other.path().join("config.json");
        std::fs::hard_link(&cfg, &cached).unwrap();
        rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).unwrap();
        assert_eq!(
            read(d.path())["fs"][0]["socket"],
            virtiofsd_socket_path(ID).to_string_lossy().as_ref()
        );
        assert_eq!(std::fs::read(&cached).unwrap(), before);
        let names: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["config.json"], "stray files left behind");
    }

    #[test]
    fn matching_paths_leave_the_config_untouched() {
        let d = tempfile::tempdir().unwrap();
        let cfg = json!({
            "vsock": {"cid": 3, "socket": vsock_socket_path(ID)},
            "console": {"mode": "File", "file": console_log_path(ID)},
            "serial": {"mode": "File", "file": serial_log_path(ID)},
            "fs": [{"tag": FS_TAG, "socket": virtiofsd_socket_path(ID)}],
            "payload": {"kernel": KERNEL, "cmdline": "console=hvc0"},
            "pmem": [{"id": GUEST_IMAGE_PMEM_ID, "file": IMAGE, "discard_writes": true}],
        });
        let p = d.path().join("config.json");
        std::fs::write(&p, serde_json::to_vec(&cfg).unwrap()).unwrap();
        let before = std::fs::metadata(&p).unwrap().ino();
        rewrite_snapshot_paths(d.path(), ID, KERNEL, IMAGE).unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().ino(), before);
    }
}
