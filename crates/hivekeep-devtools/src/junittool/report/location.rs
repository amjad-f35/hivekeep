//! Finding the source line a failure points at, relative to the workspace root.
//!
//! Three kinds of line name a source position in Rust test output:
//!
//! - the panic report, `thread 't' panicked at crates/foo/src/bar.rs:12:5:`
//!   (or the older `panicked at 'msg', crates/foo/src/bar.rs:12:5`);
//! - compiler diagnostics, `  --> crates/foo/src/bar.rs:12:5`;
//! - backtrace frames, `             at ./crates/foo/src/bar.rs:12:5`.
//!
//! Cargo compiles workspace members with paths relative to the workspace
//! root, so those paths resolve directly under it; absolute paths are kept
//! only when they lie inside the root. Positions in the standard library or
//! in registry dependencies are skipped in favour of the next candidate.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

/// A source position under the workspace root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// Slash-separated, relative to the root.
    pub file: String,
    pub line: u32,
    pub col: Option<u32>,
}

static PANIC_AT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r" panicked at (?:'.*', )?([^\s']+\.rs):(\d+):(\d+)").expect("valid regex")
});
static DIAGNOSTIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*--> (\S+\.rs):(\d+):(\d+)").expect("valid regex"));
static FRAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s+at (\S+\.rs):(\d+)(?::(\d+))?\s*$").expect("valid regex"));

/// Resolves source paths against one workspace root.
#[derive(Debug, Clone)]
pub struct Resolver {
    root: PathBuf,
    /// The root with symlinks resolved, for absolute paths printed that way.
    canonical: Option<PathBuf>,
}

impl Resolver {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            canonical: root.canonicalize().ok(),
        }
    }

    /// The first position in `lines` that resolves to a file under the root.
    pub fn find<'a>(&self, lines: impl IntoIterator<Item = &'a String>) -> Option<Location> {
        lines.into_iter().find_map(|l| {
            let (file, line, col) = candidate(l)?;
            Some(Location {
                file: self.resolve(file)?,
                line,
                col,
            })
        })
    }

    /// `file` relative to the root, if it names an existing file inside it.
    pub fn resolve(&self, file: &str) -> Option<String> {
        let p = Path::new(file.strip_prefix("./").unwrap_or(file));
        let rel = if p.is_absolute() {
            std::iter::once(&self.root)
                .chain(self.canonical.as_ref())
                .find_map(|r| p.strip_prefix(r).ok())?
        } else {
            p
        };
        if rel.as_os_str().is_empty() || !self.root.join(rel).is_file() {
            return None;
        }
        Some(rel.to_string_lossy().replace('\\', "/"))
    }
}

/// The (file, line, column) a single output line names, if any.
fn candidate(l: &str) -> Option<(&str, u32, Option<u32>)> {
    let m = PANIC_AT
        .captures(l)
        .or_else(|| DIAGNOSTIC.captures(l))
        .or_else(|| FRAME.captures(l))?;
    let file = m.get(1)?.as_str();
    let line = m.get(2)?.as_str().parse().ok()?;
    let col = m.get(3).and_then(|c| c.as_str().parse().ok());
    Some((file, line, col))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates() {
        for (line, want) in [
            (
                "thread 'a::b' panicked at crates/foo/src/bar.rs:12:5:",
                Some(("crates/foo/src/bar.rs", 12, Some(5))),
            ),
            (
                "thread 'a::b' (77) panicked at crates/foo/src/bar.rs:12:5:",
                Some(("crates/foo/src/bar.rs", 12, Some(5))),
            ),
            (
                "thread 'a' panicked at 'x, y: z', src/lib.rs:3:9",
                Some(("src/lib.rs", 3, Some(9))),
            ),
            (
                "   --> crates/foo/src/lib.rs:5:28",
                Some(("crates/foo/src/lib.rs", 5, Some(28))),
            ),
            (
                "             at ./crates/foo/src/lib.rs:40:13",
                Some(("./crates/foo/src/lib.rs", 40, Some(13))),
            ),
            (
                "             at /src/hk/crates/foo/src/lib.rs:40",
                Some(("/src/hk/crates/foo/src/lib.rs", 40, None)),
            ),
            ("assertion `left == right` failed", None),
            ("see crates/foo/src/lib.rs:4:1 for details", None),
        ] {
            assert_eq!(candidate(line), want, "{line:?}");
        }
    }

    #[test]
    fn resolve_under_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::create_dir_all(root.join("crates/foo/src")).unwrap();
        std::fs::write(root.join("crates/foo/src/lib.rs"), "").unwrap();
        let r = Resolver::new(root);
        let abs = root.join("crates/foo/src/lib.rs").display().to_string();
        let canon = root
            .canonicalize()
            .unwrap()
            .join("crates/foo/src/lib.rs")
            .display()
            .to_string();

        for (file, want) in [
            ("crates/foo/src/lib.rs", Some("crates/foo/src/lib.rs")),
            ("./crates/foo/src/lib.rs", Some("crates/foo/src/lib.rs")),
            (abs.as_str(), Some("crates/foo/src/lib.rs")),
            (canon.as_str(), Some("crates/foo/src/lib.rs")),
            // Relative to a member, not the root: not resolvable without guessing.
            ("src/lib.rs", None),
            ("crates/foo/src/missing.rs", None),
            ("/rustc/abc/library/core/src/panicking.rs", None),
            ("crates/foo/src", None),
        ] {
            assert_eq!(r.resolve(file).as_deref(), want, "{file:?}");
        }
    }

    #[test]
    fn find_skips_positions_outside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("crates/foo/src")).unwrap();
        std::fs::write(root.join("crates/foo/src/lib.rs"), "").unwrap();
        let lines: Vec<String> = [
            "thread 't' panicked at /cargo/registry/src/dep-1.0/src/lib.rs:9:1:",
            "boom",
            "stack backtrace:",
            "   3: foo::tests::t",
            "             at ./crates/foo/src/lib.rs:40:13",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            Resolver::new(root).find(&lines),
            Some(Location {
                file: "crates/foo/src/lib.rs".into(),
                line: 40,
                col: Some(13),
            })
        );
        assert_eq!(Resolver::new(root).find(&lines[..2]), None);
    }
}
