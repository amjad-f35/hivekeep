//! End-to-end checks of the `hivekeep-junittool` binary against fixtures.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/junittool")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// The tool with the CI environment cleared, so results do not depend on
/// where the tests run.
fn tool() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_hivekeep-junittool"));
    for var in [
        "CI",
        "GITHUB_ACTIONS",
        "GITHUB_STEP_SUMMARY",
        "GITHUB_WORKSPACE",
    ] {
        c.env_remove(var);
    }
    c
}

fn run(c: &mut Command) -> (i32, String, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = c.output().unwrap();
    (
        status.code().unwrap_or(-1),
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
    )
}

#[test]
fn register_then_verify() {
    let dir = tempfile::tempdir().unwrap();
    let e2e = dir.path().join("e2e.xml");
    let unit = dir.path().join("unit.xml");
    for p in [&e2e, &unit] {
        let (code, _, err) = run(tool().arg("register").arg(p));
        assert_eq!(code, 0, "{err}");
    }
    let manifest = dir.path().join("expected-junit.txt");
    assert_eq!(
        std::fs::read_to_string(&manifest).unwrap().lines().count(),
        2
    );

    // A registered run that never wrote its file fails the check.
    std::fs::copy(fixtures().join("e2e.xml"), &e2e).unwrap();
    let (code, _, err) = run(tool().arg("verify").arg("--manifest").arg(&manifest));
    assert_eq!(code, 1);
    assert!(err.contains("unit.xml"), "{err}");

    std::fs::copy(fixtures().join("e2e.xml"), &unit).unwrap();
    let (code, out, err) = run(tool().arg("verify").arg("--manifest").arg(&manifest));
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("TOTAL"), "{out}");

    // In CI a second registration of the same path is an error.
    let (code, _, err) = run(tool().env("CI", "true").arg("register").arg(&e2e));
    assert_eq!(code, 1);
    assert!(err.contains("already registered"), "{err}");
}

#[test]
fn report_reads_both_formats() {
    let dir = tempfile::tempdir().unwrap();
    let summary = dir.path().join("summary.md");
    let (code, out, err) = run(tool()
        .arg("report")
        .arg("--annotate")
        .arg("--root")
        .arg(workspace_root())
        .arg("--summary")
        .arg(&summary)
        .arg(fixtures()));
    // Reporting never fails the job; the test steps and `verify` do.
    assert_eq!(code, 0, "{err}");
    for want in [
        "e2e: 1 passed, 1 failed, 0 skipped",
        "--- FAIL: hivekeep-e2e::suite actor::resumes_from_snapshot",
        "unit: 1 passed, 1 failed, 1 skipped",
        "--- FAIL: hivekeep_devtools (unittests src/lib.rs) junittool::fixture::fails",
        "    assertion `left == right` failed",
        "::error file=crates/hivekeep-devtools/src/lib.rs,line=1,col=1,title=e2e%3A hivekeep-e2e%3A%3Asuite actor%3A%3Aresumes_from_snapshot::",
        "::error file=crates/hivekeep-devtools/src/lib.rs,line=1,col=1,title=unit%3A ",
    ] {
        assert!(out.contains(want), "missing {want:?} in:\n{out}");
    }
    let md = std::fs::read_to_string(&summary).unwrap();
    assert!(md.contains("| e2e | 1 | 1 | 0 |"), "{md}");
    assert!(md.contains("| unit | 1 | 1 | 1 |"), "{md}");
}

#[test]
fn report_finds_the_workspace_root_by_default() {
    let (code, out, err) = run(tool()
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("src"))
        .arg("report")
        .arg("--annotate=true")
        .arg(fixtures().join("unit.json")));
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains("::error file=crates/hivekeep-devtools/src/lib.rs,line=1,col=1,"),
        "{out}"
    );
}

#[test]
fn report_survives_a_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let (code, out, _) = run(tool()
        .arg("report")
        .arg("--annotate=false")
        .arg(dir.path().join("absent.json")));
    assert_eq!(code, 0);
    assert!(out.starts_with("absent: no results: "), "{out}");
}
