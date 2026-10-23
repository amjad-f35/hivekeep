//! Counters persisted as a decimal number in a file.

use parking_lot::Mutex;
use std::fs;
use std::path::Path;

/// Serialises every read-modify-write of counter files in the process.
static FILE_LOCK: Mutex<()> = Mutex::new(());

/// Reads the number stored at `path` (missing or unparsable counts as 0),
/// adds one, writes it back and returns it. Returns -1 when the write fails,
/// which the response shows verbatim so a read-only mount is obvious.
pub fn increment(path: &Path) -> i64 {
    let _guard = FILE_LOCK.lock();
    let current = fs::read_to_string(path)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let next = current + 1;
    match fs::write(path, next.to_string()) {
        Ok(()) => next,
        Err(_) => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_one_and_counts_up() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.txt");
        assert_eq!(increment(&p), 1);
        assert_eq!(increment(&p), 2);
        assert_eq!(fs::read_to_string(&p).unwrap(), "2");
    }

    #[test]
    fn garbage_resets_to_one() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.txt");
        fs::write(&p, "not a number").unwrap();
        assert_eq!(increment(&p), 1);
    }

    #[test]
    fn unwritable_path_reports_minus_one() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("missing-dir").join("a.txt");
        assert_eq!(increment(&p), -1);
    }
}
