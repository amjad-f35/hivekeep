//! Records the JUnit files a CI run will write, then checks they reported tests.
//!
//! ```text
//! hivekeep-junittool register <path>            record a file this run will write
//! hivekeep-junittool verify --manifest <file>   require every recorded file to hold tests
//! hivekeep-junittool report <path|dir>...        summarize failures from test results
//! ```
//!
//! Registering before the run has two effects: the expected set comes from the
//! runs themselves, so adding a run needs no change to `verify`; and a run that
//! died part-way is distinguishable from one that never existed.
//!
//! `report` reads cargo-nextest JUnit files (`target/nextest/<profile>/junit.xml`)
//! and libtest JSON event streams, written with
//!
//! ```text
//! cargo test -- -Z unstable-options --format json --report-time > unit.json 2>&1
//! ```
//!
//! (the JSON format needs a nightly toolchain or `RUSTC_BOOTSTRAP=1`). Merging
//! stderr is optional; it lets the report name each test binary and see
//! compiler errors and crashes. Annotations point at panic locations, which
//! cargo prints relative to the workspace root.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use hivekeep_devtools::apitool::validate;
use hivekeep_devtools::junittool::{junit, manifest, report};

#[derive(Parser)]
#[command(
    name = "hivekeep-junittool",
    about = "Fail-closed checks for CI test results"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Record a JUnit file this run will write.
    Register {
        /// Manifest to append to (default: expected-junit.txt beside the entry).
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Treat a repeat registration as an error (defaults to true when CI=true).
        #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
        reject_duplicate: Option<bool>,
        /// The JUnit file path.
        entry: String,
    },
    /// Require every recorded (or listed) JUnit file to report tests.
    Verify {
        /// Minimum number of test cases EACH file must report.
        #[arg(long, default_value_t = 1)]
        min: u64,
        /// Manifest listing the JUnit paths that were registered.
        #[arg(long, conflicts_with = "paths")]
        manifest: Option<PathBuf>,
        /// JUnit files, or directories of them.
        paths: Vec<String>,
    },
    /// List each failed test with its panic message and output.
    Report {
        /// File to append a markdown summary to (default: $GITHUB_STEP_SUMMARY).
        #[arg(long, env = "GITHUB_STEP_SUMMARY")]
        summary: Option<PathBuf>,
        /// Print CI error annotations for failures (defaults to true in GitHub Actions).
        #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
        annotate: Option<bool>,
        /// Cargo workspace root that panic locations are relative to
        /// (default: $GITHUB_WORKSPACE, else the workspace containing the
        /// working directory).
        #[arg(long, env = "GITHUB_WORKSPACE")]
        root: Option<PathBuf>,
        /// libtest JSON streams or nextest JUnit files, or directories of them
        /// (*.json, *.jsonl, *.xml).
        #[arg(required = true)]
        paths: Vec<String>,
    },
}

/// Strict in CI, permissive locally, where re-running into the same file is ordinary.
fn in_ci() -> bool {
    std::env::var("CI").is_ok_and(|v| v == "true")
}

fn main() -> ExitCode {
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::cli(
        "hivekeep-junittool",
        &Default::default(),
    ));
    let code = match Cli::parse().command {
        Command::Register {
            manifest,
            reject_duplicate,
            entry,
        } => run_register(manifest, reject_duplicate.unwrap_or_else(in_ci), &entry),
        Command::Verify {
            min,
            manifest,
            paths,
        } => run_verify(min, manifest, paths),
        Command::Report {
            summary,
            annotate,
            root,
            paths,
        } => {
            let annotate = annotate
                .unwrap_or_else(|| std::env::var("GITHUB_ACTIONS").is_ok_and(|v| v == "true"));
            let root = root
                .filter(|p| !p.as_os_str().is_empty())
                .or_else(|| validate::repo_root().ok());
            run_report(
                summary.filter(|p| !p.as_os_str().is_empty()),
                annotate,
                root.as_deref(),
                paths,
            )
        }
    };
    ExitCode::from(code)
}

fn run_register(manifest_path: Option<PathBuf>, reject: bool, entry: &str) -> u8 {
    let path = manifest_path.unwrap_or_else(|| manifest::default_manifest(entry));
    match manifest::register(&path, entry, reject) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("junittool register: {e}");
            1
        }
    }
}

fn run_verify(min: u64, manifest_path: Option<PathBuf>, args: Vec<String>) -> u8 {
    let paths = match (manifest_path, args.is_empty()) {
        (Some(m), _) => match manifest::read_manifest(&m) {
            Ok(p) if p.is_empty() => {
                eprintln!(
                    "junittool verify: {} is empty. No test run registered a JUnit file, so nothing ran.",
                    m.display()
                );
                return 1;
            }
            Ok(p) => p,
            Err(e) => {
                eprintln!("junittool verify: {e}");
                return 1;
            }
        },
        (None, false) => match junit::expand(&args, &[".xml"]) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("junittool verify: {e}");
                return 1;
            }
        },
        (None, true) => {
            eprintln!("junittool verify: pass --manifest or one or more JUnit paths");
            return 2;
        }
    };

    let verdict = junit::evaluate(&paths, min);
    let _ = verdict.write(&mut std::io::stdout(), &mut std::io::stderr(), min);
    if verdict.ok() {
        0
    } else {
        1
    }
}

/// Only reports; the test steps and `verify` decide whether the job fails.
fn run_report(
    summary: Option<PathBuf>,
    annotate: bool,
    root: Option<&Path>,
    args: Vec<String>,
) -> u8 {
    let paths = match junit::expand(&args, report::INPUT_EXTENSIONS) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("junittool report: {e}");
            return 1;
        }
    };
    let mut stdout = std::io::stdout();
    let mut runs = Vec::new();
    for p in &paths {
        let r = report::parse_run(Path::new(p));
        let _ = r.write_text(&mut stdout);
        runs.push(r);
    }
    if annotate {
        let _ = report::write_annotations(&mut stdout, &runs, root);
    }
    if let Some(summary) = summary {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&summary);
        let mut f = match file {
            Ok(f) => f,
            Err(e) => {
                eprintln!("junittool report: {e}");
                return 1;
            }
        };
        if let Err(e) =
            report::write_markdown(&mut f, &runs, report::SUMMARY_BUDGET).and_then(|_| f.flush())
        {
            eprintln!("junittool report: failed to write summary: {e}");
            return 1;
        }
    }
    0
}
