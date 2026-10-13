//! Postconditions of atomic replacement: content, mode, and no temp files.

use super::*;

fn names(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn write_over_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("state.json");
    fs::write(&target, "old").unwrap();
    write(&target, b"new", 0o644).unwrap();
    // replaces content
    assert_eq!(fs::read_to_string(&target).unwrap(), "new");
    // sets permissions
    assert_eq!(mode_of(&fs::metadata(&target).unwrap()), 0o644);
    // leaves no temp files
    assert_eq!(names(dir.path()), ["state.json"]);
}

#[test]
fn write_sync_replaces_and_leaves_no_temp_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime.json");
    write_sync(&path, b"v1", 0o600).unwrap();
    write_sync(&path, b"v2", 0o600).unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "v2");
    assert_eq!(names(dir.path()), ["runtime.json"]);
}
