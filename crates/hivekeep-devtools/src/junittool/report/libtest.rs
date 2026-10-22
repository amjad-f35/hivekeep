//! Reading libtest's JSON event stream.
//!
//! `cargo test -- -Z unstable-options --format json --report-time` makes every
//! test binary print one JSON object per line:
//!
//! ```text
//! {"type":"suite","event":"started","test_count":2}
//! {"type":"test","event":"started","name":"a::ok"}
//! {"type":"test","name":"a::ok","event":"ok","exec_time":0.001}
//! {"type":"test","name":"a::bad","event":"failed","exec_time":0.002,"stdout":"thread 'a::bad' panicked at ..."}
//! {"type":"suite","event":"failed","passed":1,"failed":1,"ignored":0,...}
//! ```
//!
//! A run of several binaries is several suites back to back. The events do
//! not say which binary a suite came from, so when cargo's own stderr is
//! merged into the file (`2>&1`), its `Running <target> (<binary>)` lines name
//! the suite that follows, and its compiler errors and crash reports become
//! visible too. Any line that is not an event is kept as output of the suite
//! it appeared in, or of the run when no suite was open.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use super::excerpt::{
    excerpt, head, lines_of, panic_block, tail, MAX_CONTEXT_LINES, MAX_MESSAGE_LINES,
};
use super::{Failure, Run};

/// One event. Fields not used here (counts on suite events, bench results)
/// are ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    event: String,
    name: String,
    stdout: String,
    /// Why a test failed when there was no panic to report, e.g. a
    /// `should_panic` test that returned normally.
    message: String,
    /// Set on failures caused by an exceeded time limit.
    reason: String,
    /// Seconds; a number in current toolchains, a `"0.001s"` string in some
    /// older ones.
    exec_time: Option<serde_json::Value>,
}

impl Event {
    fn elapsed(&self) -> f64 {
        match &self.exec_time {
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or_default(),
            Some(serde_json::Value::String(s)) => {
                s.trim().trim_end_matches('s').parse().unwrap_or_default()
            }
            _ => 0.0,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Started and not yet reported.
    #[default]
    Running,
    Passed,
    Failed,
    Ignored,
}

#[derive(Debug, Default)]
struct TestState {
    outcome: Outcome,
    elapsed: f64,
    output: Vec<String>,
    /// libtest's own explanation, from `message` or `reason`.
    note: Vec<String>,
    /// libtest warned the test had run for over 60 seconds.
    slow: bool,
}

#[derive(Debug, Default)]
struct Suite {
    name: String,
    /// `ok` or `failed`; empty if the suite never finished.
    result: String,
    tests: BTreeMap<String, TestState>,
    /// Lines that were not events, printed while the suite was open.
    output: Vec<String>,
}

/// cargo's `Running unittests src/lib.rs (target/debug/deps/foo-0123abcd)` line.
static RUNNING: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*Running (.+?) \((?:.*/)?([^/()]+?)(?:-[0-9a-f]{8,})?(?:\.exe)?\)\s*$")
        .expect("valid regex")
});
/// cargo's `Doc-tests foo` line.
static DOC_TESTS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*Doc-tests (\S+)\s*$").expect("valid regex"));
/// cargo's summary of a failed compilation.
static COULD_NOT_COMPILE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^error: could not compile `([^`]+)`").expect("valid regex"));

/// Adds the results in the event stream `raw` to `run`.
pub(super) fn parse(raw: &str, run: &mut Run) {
    let mut suites: Vec<Suite> = Vec::new();
    let mut open: Option<Suite> = None;
    let mut next_name: Option<String> = None;
    // Lines printed outside any suite: compiler output, cargo's notes.
    let mut loose: Vec<String> = Vec::new();

    for line in raw.lines() {
        let event = line
            .trim_start()
            .starts_with('{')
            .then(|| serde_json::from_str::<Event>(line).ok())
            .flatten()
            .filter(|e| !e.kind.is_empty());
        let Some(e) = event else {
            if let Some(name) = suite_name(line) {
                next_name = Some(name);
            } else if let Some(s) = open.as_mut() {
                s.output.push(line.to_string());
            } else {
                loose.push(line.to_string());
            }
            continue;
        };

        match (e.kind.as_str(), e.event.as_str()) {
            ("suite", "started") => {
                // A suite left open was cut short; keep what it reported.
                suites.extend(open.take());
                let name = next_name
                    .take()
                    .unwrap_or_else(|| format!("suite {}", suites.len() + 1));
                open = Some(Suite {
                    name,
                    ..Suite::default()
                });
            }
            ("suite", "ok" | "failed") => {
                if let Some(mut s) = open.take() {
                    s.result = e.event;
                    suites.push(s);
                }
            }
            ("test" | "bench", ev) => {
                let s = open.get_or_insert_with(|| Suite {
                    name: next_name
                        .take()
                        .unwrap_or_else(|| format!("suite {}", suites.len() + 1)),
                    ..Suite::default()
                });
                record(run, s, &e, ev);
            }
            _ => {}
        }
    }
    suites.extend(open);

    for s in &suites {
        add_suite(run, s);
    }
    add_build_failures(run, &loose);
}

/// Applies one test event to its suite.
fn record(run: &mut Run, s: &mut Suite, e: &Event, ev: &str) {
    let t = s.tests.entry(e.name.clone()).or_default();
    let outcome = match ev {
        "started" => {
            // A failure sticks if the test runs again.
            if t.outcome != Outcome::Failed {
                t.outcome = Outcome::Running;
            }
            return;
        }
        "timeout" => {
            t.slow = true;
            return;
        }
        "ok" => {
            run.passed += 1;
            Outcome::Passed
        }
        "failed" => {
            run.failed += 1;
            Outcome::Failed
        }
        "ignored" => {
            run.skipped += 1;
            Outcome::Ignored
        }
        // A bench result is reported under its own event name.
        _ if e.kind == "bench" => {
            run.passed += 1;
            Outcome::Passed
        }
        _ => return,
    };
    if t.outcome != Outcome::Failed {
        t.outcome = outcome;
    }
    t.elapsed = e.elapsed();
    t.output.extend(lines_of(&e.stdout));
    for note in [&e.message, &e.reason] {
        t.note.extend(lines_of(note));
    }
}

/// The name for the suite a `Running`/`Doc-tests` line announces.
fn suite_name(line: &str) -> Option<String> {
    if let Some(m) = RUNNING.captures(line) {
        return Some(format!("{} ({})", &m[2], &m[1]));
    }
    DOC_TESTS
        .captures(line)
        .map(|m| format!("{} (doctests)", &m[1]))
}

fn add_suite(run: &mut Run, s: &Suite) {
    let mut failed = 0;
    let mut unfinished = Vec::new();
    for (name, t) in &s.tests {
        match t.outcome {
            Outcome::Failed => {
                failed += 1;
                run.failures.push(test_failure(&s.name, name, t));
            }
            Outcome::Running => unfinished.push((name.as_str(), t.slow)),
            Outcome::Passed | Outcome::Ignored => {}
        }
    }

    let mut f = Failure {
        pkg: s.name.clone(),
        ..Failure::default()
    };
    if !unfinished.is_empty() {
        // The binary died (or was killed) with these tests still running.
        run.failed += unfinished.len();
        let names: Vec<String> = unfinished
            .iter()
            .map(|(n, slow)| {
                if *slow {
                    format!("{n} (over 60s)")
                } else {
                    n.to_string()
                }
            })
            .collect();
        f.label = format!("did not finish: {}", head(&names, 5).join(", "));
        if names.len() > 5 {
            f.label += &format!(" and {} more", names.len() - 5);
        }
        f.message = excerpt(&s.output, MAX_MESSAGE_LINES);
    } else if s.result.is_empty() {
        run.failed += 1;
        f.label = "did not finish".into();
        f.message = excerpt(&s.output, MAX_MESSAGE_LINES);
    } else if s.result == "failed" && failed == 0 {
        run.failed += 1;
        f.label = "failed outside of a test".into();
        f.message = excerpt(&s.output, MAX_MESSAGE_LINES);
    } else {
        return;
    }
    if f.message.is_empty() {
        f.message = vec!["the test binary exited without reporting a result".into()];
    }
    run.failures.push(f);
}

fn test_failure(suite: &str, name: &str, t: &TestState) -> Failure {
    let mut message = t.note.clone();
    match panic_block(&t.output, MAX_MESSAGE_LINES) {
        Some(p) => message.extend(p),
        None if message.is_empty() => message = tail(&t.output, MAX_MESSAGE_LINES),
        None => {}
    }
    message.truncate(MAX_MESSAGE_LINES);
    let context = tail(&t.output, MAX_CONTEXT_LINES);
    Failure {
        pkg: suite.to_string(),
        test: name.to_string(),
        elapsed: t.elapsed,
        context: if context == message {
            Vec::new()
        } else {
            context
        },
        message,
        ..Failure::default()
    }
}

/// Whether `l` opens a compiler diagnostic, as opposed to cargo reporting
/// that a test binary failed.
fn is_compiler_error(l: &str) -> bool {
    l.starts_with("error[")
        || (l.starts_with("error: ")
            && !l.starts_with("error: test failed")
            && !l.contains(" target failed")
            && !l.contains(" targets failed"))
}

/// One failure per crate cargo could not compile, with the diagnostics
/// printed before it.
fn add_build_failures(run: &mut Run, loose: &[String]) {
    let mut start: Option<usize> = None;
    for (i, l) in loose.iter().enumerate() {
        if let Some(m) = COULD_NOT_COMPILE.captures(l) {
            let block = start.map(|s| &loose[s..i]).unwrap_or(&[]);
            run.failed += 1;
            run.failures.push(Failure {
                pkg: m[1].to_string(),
                label: "build failed".into(),
                message: if block.is_empty() {
                    vec![l.clone()]
                } else {
                    head(block, MAX_MESSAGE_LINES)
                },
                ..Failure::default()
            });
            start = None;
        } else if start.is_none() && is_compiler_error(l) {
            start = Some(i);
        }
    }
}
