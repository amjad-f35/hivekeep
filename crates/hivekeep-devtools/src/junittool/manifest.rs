//! The manifest of JUnit files a CI run is expected to write: one path per line.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// File name of the manifest kept beside the JUnit files by default.
pub const DEFAULT_MANIFEST_NAME: &str = "expected-junit.txt";

/// Errors reading or appending to a manifest.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The path is already recorded. Registration is the only point where both
    /// runs are still visible; after the overwrite the first leaves no trace.
    #[error(
        "{0}: already registered by an earlier run; give each run its own path, \
         or the second overwrites the first and only the second is verified"
    )]
    Duplicate(String),
    #[error("refusing to register an empty entry")]
    Empty,
    #[error("entry {0:?} contains a newline: one path per line")]
    Newline(String),
    #[error("creating manifest directory: {0}")]
    CreateDir(io::Error),
    #[error("reading manifest: {0}")]
    Read(io::Error),
    #[error("opening manifest: {0}")]
    Open(io::Error),
    #[error("appending to manifest: {0}")]
    Append(io::Error),
}

/// Returns the recorded paths, deduplicated, in first-seen order. Duplicates
/// collapse rather than fail: a run repeated locally records its path again.
pub fn read_manifest(path: &Path) -> Result<Vec<String>, ManifestError> {
    let raw = std::fs::read_to_string(path).map_err(ManifestError::Read)?;
    Ok(dedupe(raw.split('\n')))
}

/// Trims, drops blanks, and keeps the first occurrence of each entry.
fn dedupe<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in lines.map(str::trim) {
        if !line.is_empty() && !out.iter().any(|e| e == line) {
            out.push(line.to_string());
        }
    }
    out
}

/// Appends `entry` to the manifest at `path`, creating it (and its directory)
/// if absent. With `reject_duplicate`, a repeat is an error rather than a no-op.
///
/// Callers register before running, so a run killed part-way still has a
/// record and `verify` reports its file as missing.
pub fn register(path: &Path, entry: &str, reject_duplicate: bool) -> Result<(), ManifestError> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err(ManifestError::Empty);
    }
    if entry.contains(['\n', '\r']) {
        return Err(ManifestError::Newline(entry.to_string()));
    }
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(ManifestError::CreateDir)?;
    }

    let existing = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(ManifestError::Read(e)),
    };
    if dedupe(existing.split('\n')).iter().any(|e| e == entry) {
        return if reject_duplicate {
            Err(ManifestError::Duplicate(entry.to_string()))
        } else {
            Ok(())
        };
    }

    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(ManifestError::Open)?;
    writeln!(f, "{entry}").map_err(ManifestError::Append)
}

/// The manifest beside the JUnit file, so runs sharing an artifact directory
/// share a manifest.
pub fn default_manifest(entry: &str) -> PathBuf {
    Path::new(entry)
        .parent()
        .unwrap_or(Path::new(""))
        .join(DEFAULT_MANIFEST_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .split('\n')
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn register_creates_and_appends() {
        let dir = tempfile::tempdir().unwrap();
        // The parent directory does not exist yet: registration can precede any
        // other write to the artifact directory.
        let m = dir.path().join("nested").join("expected-junit.txt");
        register(&m, "/a/one.xml", true).unwrap();
        register(&m, "/a/two.xml", true).unwrap();
        assert_eq!(lines(&m), ["/a/one.xml", "/a/two.xml"]);
    }

    #[test]
    fn register_duplicate() {
        // (reject, want_err): two runs sharing a path is rejected when asked; a
        // run repeated locally is tolerated and not duplicated.
        for (reject, want_err) in [(true, true), (false, false)] {
            let dir = tempfile::tempdir().unwrap();
            let m = dir.path().join("expected-junit.txt");
            register(&m, "/a/one.xml", reject).unwrap();
            let err = register(&m, "/a/one.xml", reject);
            if want_err {
                assert!(matches!(err, Err(ManifestError::Duplicate(_))), "{err:?}");
            } else {
                err.unwrap();
            }
            assert_eq!(lines(&m).len(), 1);
        }
    }

    #[test]
    fn register_distinguishes_similar_paths() {
        let dir = tempfile::tempdir().unwrap();
        let m = dir.path().join("expected-junit.txt");
        for e in [
            "/a/e2e.xml",
            "/a/e2e-microvm.xml",
            "/a/e2e-microvm-mitm.xml",
        ] {
            register(&m, e, true).unwrap();
        }
        assert_eq!(lines(&m).len(), 3);
    }

    #[test]
    fn register_rejects_malformed_entries() {
        for entry in [
            "",
            "   ",
            "/a/one.xml\n/a/two.xml",
            "/a/one.xml\r/a/two.xml",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let m = dir.path().join("expected-junit.txt");
            assert!(register(&m, entry, true).is_err(), "{entry:?}");
        }
    }

    #[test]
    fn register_trims_entry() {
        let dir = tempfile::tempdir().unwrap();
        let m = dir.path().join("expected-junit.txt");
        register(&m, "  /a/one.xml  ", true).unwrap();
        assert_eq!(lines(&m), ["/a/one.xml"]);
        // The trimmed form is what a repeat is compared against.
        assert!(matches!(
            register(&m, "/a/one.xml", true),
            Err(ManifestError::Duplicate(_))
        ));
    }

    #[test]
    fn default_manifest_is_beside_the_entry() {
        assert_eq!(
            default_manifest("/tmp/artifacts/e2e-gvisor.xml"),
            Path::new("/tmp/artifacts").join("expected-junit.txt")
        );
    }

    #[test]
    fn read_manifest_cases() {
        let dir = tempfile::tempdir().unwrap();
        let cases: &[(&str, &str, &[&str])] = &[
            ("one per line", "a.xml\nb.xml\n", &["a.xml", "b.xml"]),
            (
                "duplicates collapse",
                "a.xml\nb.xml\na.xml\n",
                &["a.xml", "b.xml"],
            ),
            (
                "all duplicates collapse to one",
                "a.xml\na.xml\na.xml\n",
                &["a.xml"],
            ),
            (
                "first occurrence sets the order",
                "b.xml\na.xml\nb.xml\n",
                &["b.xml", "a.xml"],
            ),
            (
                "blank lines ignored",
                "\na.xml\n\n\nb.xml\n",
                &["a.xml", "b.xml"],
            ),
            (
                "whitespace trimmed",
                "  a.xml  \n\tb.xml\t\n",
                &["a.xml", "b.xml"],
            ),
            ("no trailing newline", "a.xml", &["a.xml"]),
            ("empty manifest yields nothing", "", &[]),
            ("whitespace-only yields nothing", "\n  \n\t\n", &[]),
        ];
        for (name, content, want) in cases {
            let p = dir.path().join(format!("{}.txt", name.replace(' ', "_")));
            std::fs::write(&p, content).unwrap();
            assert_eq!(read_manifest(&p).unwrap(), *want, "{name}");
        }
    }

    #[test]
    fn read_manifest_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_manifest(&dir.path().join("absent.txt")).is_err());
    }

    #[test]
    fn register_then_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let m = dir.path().join("expected-junit.txt");
        let want = ["/a/gvisor.xml", "/a/microvm.xml", "/a/mitm.xml"];
        for e in want {
            register(&m, e, true).unwrap();
        }
        assert_eq!(read_manifest(&m).unwrap(), want);
    }
}
