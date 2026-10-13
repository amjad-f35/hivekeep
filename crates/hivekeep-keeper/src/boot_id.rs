//! The node's boot ID, which tells local snapshots written on this boot
//! from ones that may have been torn by a reboot.

use std::path::Path;
use std::sync::Arc;

use crate::assets::{read_snapshot_manifest, SandboxAssetsRecord};
use crate::paths::NodePaths;
use crate::snapshot_tracker::BootCheck;

/// Where Linux exposes the boot ID: a random UUID generated once per boot.
/// It is not namespaced, so the keeper reads the node's.
pub const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Reads the boot ID at `path`. It is never empty, so a manifest recording
/// none never matches it.
pub fn read_boot_id(path: &Path) -> Result<String, String> {
    let b = std::fs::read_to_string(path).map_err(|e| format!("while reading the boot ID: {e}"))?;
    let id = b.trim();
    if id.is_empty() {
        return Err(format!("boot ID file {} is empty", path.display()));
    }
    Ok(id.to_string())
}

/// The tracker's check of whether an actor's local snapshot was written on
/// the boot `boot_id`: whether its manifest records it. A missing or
/// unparsable manifest records none.
pub fn local_snapshot_boot_check(paths: Arc<NodePaths>, boot_id: String) -> BootCheck {
    Arc::new(move |actor_uid: &str, snapshot: &str| {
        let Ok(m) = read_snapshot_manifest(&paths.local_snapshot_dir(actor_uid, snapshot)) else {
            return false;
        };
        SandboxAssetsRecord::parse(&m).is_ok_and(|r| r.boot_id == boot_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::MANIFEST_NAME;

    #[test]
    fn reads_and_trims() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("boot_id");
        std::fs::write(&p, "abc-123\n").unwrap();
        assert_eq!(read_boot_id(&p).unwrap(), "abc-123");
        std::fs::write(&p, "  \n").unwrap();
        assert!(read_boot_id(&p).is_err());
        assert!(read_boot_id(&d.path().join("missing")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_the_real_boot_id() {
        assert!(!read_boot_id(Path::new(BOOT_ID_PATH)).unwrap().is_empty());
    }

    #[test]
    fn boot_check_matches_manifest() {
        let d = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(d.path()));
        let check = local_snapshot_boot_check(paths.clone(), "boot-1".into());
        assert!(
            !check("u", "s1"),
            "a missing copy counts as an earlier boot"
        );
        let dir = paths.local_snapshot_dir("u", "s1");
        std::fs::create_dir_all(&dir).unwrap();
        let rec = SandboxAssetsRecord {
            boot_id: "boot-1".into(),
            ..Default::default()
        };
        std::fs::write(dir.join(MANIFEST_NAME), rec.to_json()).unwrap();
        assert!(check("u", "s1"));
        let rec = SandboxAssetsRecord::default();
        std::fs::write(dir.join(MANIFEST_NAME), rec.to_json()).unwrap();
        assert!(!check("u", "s1"));
    }
}

#[cfg(test)]
#[path = "boot_id_tests.rs"]
mod behavior_tests;
