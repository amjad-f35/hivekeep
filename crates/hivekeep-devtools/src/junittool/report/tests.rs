use super::*;

use serde_json::json;

fn v(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|s| s.to_string()).collect()
}

fn fail(pkg: &str, test: &str, label: &str, message: &[&str], context: &[&str]) -> Failure {
    Failure {
        pkg: pkg.into(),
        test: test.into(),
        label: label.into(),
        elapsed: 0.0,
        message: v(message),
        context: v(context),
    }
}

fn write(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, content).unwrap();
    p
}

/// Joins JSON events and raw lines into one stream.
fn stream(items: &[serde_json::Value]) -> String {
    items
        .iter()
        .map(|i| match i {
            serde_json::Value::String(s) => format!("{s}\n"),
            other => format!("{other}\n"),
        })
        .collect()
}

fn started(name: &str) -> serde_json::Value {
    json!({"type": "test", "event": "started", "name": name})
}

fn parsed(raw: &str) -> Run {
    let mut r = Run {
        name: "unit".into(),
        ..Run::default()
    };
    parse_str(raw, &mut r);
    r
}

#[test]
fn libtest_classifies_failures() {
    const LIB: &str = "foo (unittests src/lib.rs)";
    let raw = stream(&[
        json!("   Compiling foo v0.1.0 (/w/crates/foo)"),
        json!("     Running unittests src/lib.rs (target/debug/deps/foo-0123456789abcdef)"),
        json!({"type": "suite", "event": "started", "test_count": 7}),
        // A passing test: nothing is reported about it.
        started("a::pass"),
        json!({"type": "test", "name": "a::pass", "event": "ok", "exec_time": 0.001}),
        started("a::skip"),
        json!({"type": "test", "name": "a::skip", "event": "ignored", "message": "needs a cluster"}),
        // An assertion: the panic block is the message, the hint is dropped.
        started("a::assert"),
        json!({"type": "test", "name": "a::assert", "event": "failed", "exec_time": 0.25,
               "stdout": "setting up\n\
                          thread 'a::assert' panicked at crates/foo/src/a.rs:13:9:\n\
                          assertion `left == right` failed\n  left: 1\n right: 2\n\
                          note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n"}),
        // A logged panic from elsewhere is not the test's own.
        started("a::quotes_panic"),
        json!({"type": "test", "name": "a::quotes_panic", "event": "failed",
               "stdout": "pod logs:\n    thread 'main' panicked at src/main.rs:1:1:\n\
                          thread 'a::quotes_panic' panicked at crates/foo/src/a.rs:42:5:\npod crashed\n"}),
        // No panic at all: libtest's own explanation is all there is.
        started("a::no_panic"),
        json!({"type": "test", "name": "a::no_panic", "event": "failed",
               "message": "test did not panic as expected"}),
        // The 60-second warning is not a result; the string time is parsed.
        started("a::slow"),
        json!({"type": "test", "name": "a::slow", "event": "timeout"}),
        json!({"type": "test", "name": "a::slow", "event": "ok", "exec_time": "61.5s"}),
        started("a::too_slow"),
        json!({"type": "test", "name": "a::too_slow", "event": "failed",
               "reason": "time limit exceeded"}),
        json!({"type": "suite", "event": "failed", "passed": 2, "failed": 4, "ignored": 1}),
        json!("error: test failed, to rerun pass `-p foo --lib`"),
        // A binary that crashed: the stream stops with tests still running.
        json!("     Running tests/crash.rs (target/debug/deps/crash-0123456789abcdef)"),
        json!({"type": "suite", "event": "started", "test_count": 3}),
        started("deep"),
        started("hang"),
        json!({"type": "test", "name": "hang", "event": "timeout"}),
        started("quick"),
        json!({"type": "test", "name": "quick", "event": "ok"}),
        json!("thread 'deep' has overflowed its stack"),
        json!("fatal runtime error: stack overflow"),
        // Unnamed (stderr not merged), failed with no failed test.
        json!({"type": "suite", "event": "started", "test_count": 0}),
        json!({"type": "suite", "event": "failed", "passed": 0, "failed": 0}),
        json!("   Doc-tests foo"),
        json!({"type": "suite", "event": "started", "test_count": 1}),
        started("src/lib.rs - f (line 3)"),
        json!({"type": "test", "name": "src/lib.rs - f (line 3)", "event": "ok"}),
        json!({"type": "suite", "event": "ok"}),
        json!("not json"),
        json!("{\"not\": \"an event\"}"),
        // Cut off right after starting.
        json!({"type": "suite", "event": "started", "test_count": 1}),
    ]);

    let got = parsed(&raw);
    assert!(got.error.is_none(), "{:?}", got.error);
    assert_eq!((got.passed, got.failed, got.skipped), (4, 8, 1));

    let assert_out = [
        "setting up",
        "thread 'a::assert' panicked at crates/foo/src/a.rs:13:9:",
        "assertion `left == right` failed",
        "  left: 1",
        " right: 2",
        "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
    ];
    let quotes_out = [
        "pod logs:",
        "    thread 'main' panicked at src/main.rs:1:1:",
        "thread 'a::quotes_panic' panicked at crates/foo/src/a.rs:42:5:",
        "pod crashed",
    ];
    let no_result = ["the test binary exited without reporting a result"];
    let want = vec![
        Failure {
            elapsed: 0.25,
            ..fail(LIB, "a::assert", "", &assert_out[1..5], &assert_out)
        },
        fail(
            LIB,
            "a::no_panic",
            "",
            &["test did not panic as expected"],
            &[],
        ),
        fail(LIB, "a::quotes_panic", "", &quotes_out[2..], &quotes_out),
        fail(LIB, "a::too_slow", "", &["time limit exceeded"], &[]),
        fail(
            "crash (tests/crash.rs)",
            "",
            "did not finish: deep, hang (over 60s)",
            &[
                "thread 'deep' has overflowed its stack",
                "fatal runtime error: stack overflow",
            ],
            &[],
        ),
        fail("suite 3", "", "failed outside of a test", &no_result, &[]),
        fail("suite 5", "", "did not finish", &no_result, &[]),
    ];
    assert_eq!(got.failures, want);
}

#[test]
fn libtest_failure_sticks_across_reruns() {
    let raw = stream(&[
        json!({"type": "suite", "event": "started", "test_count": 1}),
        started("t"),
        json!({"type": "test", "name": "t", "event": "failed", "stdout": "first\n"}),
        started("t"),
        json!({"type": "test", "name": "t", "event": "ok"}),
        json!({"type": "suite", "event": "ok"}),
    ]);
    let got = parsed(&raw);
    assert_eq!((got.passed, got.failed), (1, 1));
    assert_eq!(
        got.failures,
        vec![fail("suite 1", "t", "", &["first"], &[])]
    );
}

#[test]
fn libtest_reports_build_failures() {
    let raw = [
        "   Compiling foo v0.1.0 (/w/crates/foo)",
        "warning: unused import: `std::io`",
        "error[E0425]: cannot find value `x` in this scope",
        "  --> crates/foo/src/lib.rs:5:28",
        "   |",
        "5  |     x",
        "error: could not compile `foo` (lib test) due to 1 previous error",
        "error: could not compile `bar` (lib) due to 1 previous error",
        "error: 2 targets failed:",
    ]
    .join("\n");
    let got = parsed(&raw);
    assert_eq!((got.passed, got.failed, got.skipped), (0, 2, 0));
    assert_eq!(
        got.failures,
        vec![
            fail(
                "foo",
                "",
                "build failed",
                &[
                    "error[E0425]: cannot find value `x` in this scope",
                    "  --> crates/foo/src/lib.rs:5:28",
                    "   |",
                    "5  |     x",
                ],
                &[],
            ),
            fail(
                "bar",
                "",
                "build failed",
                &["error: could not compile `bar` (lib) due to 1 previous error"],
                &[],
            ),
        ]
    );
}

#[test]
fn libtest_events_without_a_suite_still_count() {
    // A stream trimmed to test events alone (grep'd from a larger log).
    let raw = stream(&[
        started("a"),
        json!({"type": "test", "name": "a", "event": "ok"}),
        json!({"type": "bench", "name": "b", "event": "ok", "median": 10, "deviation": 1}),
    ]);
    let got = parsed(&raw);
    // The suite never ended, but nothing was left running, so only the
    // missing end counts as a failure.
    assert_eq!((got.passed, got.failed), (2, 1));
    assert_eq!(got.failures.len(), 1);
    assert_eq!(got.failures[0].label, "did not finish");
}

const NEXTEST: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="6" failures="3" errors="0">
  <testsuite name="hivekeep-foo" tests="4" failures="1" skipped="1">
    <testcase name="a::ok" classname="hivekeep-foo" time="0.010"/>
    <testcase name="a::bad" classname="hivekeep-foo" time="0.250">
      <failure type="test failure" message="thread &apos;a::bad&apos; panicked at crates/foo/src/a.rs:13:9">thread &apos;a::bad&apos; panicked at crates/foo/src/a.rs:13:9:
assertion `left == right` failed &amp; more</failure>
      <system-out>setup &lt;done&gt;
</system-out>
      <system-err><![CDATA[thread 'a::bad' panicked at crates/foo/src/a.rs:13:9:
assertion `left == right` failed & more
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
]]></system-err>
    </testcase>
    <testcase name="a::flaky" classname="hivekeep-foo" time="0.1">
      <flakyFailure type="test failure">thread 'a::flaky' panicked at x.rs:1:1:<system-out>first try</system-out></flakyFailure>
    </testcase>
    <testcase name="a::skip" classname="hivekeep-foo"><skipped/></testcase>
  </testsuite>
  <testsuite name="hivekeep-foo::e2e" tests="2" failures="2">
    <testcase name="hang" classname="" time="30.000">
      <failure type="test timeout"/>
      <system-out>waiting&#10;still waiting</system-out>
      <rerunFailure type="test timeout"><system-out>earlier attempt</system-out></rerunFailure>
    </testcase>
    <testcase name="logs_only" time="0.5">
      <error type="test failure"></error>
      <system-out>line 1
line 2</system-out>
    </testcase>
  </testsuite>
</testsuites>
"#;

#[test]
fn nextest_junit_classifies_failures() {
    let got = parsed(NEXTEST);
    assert!(got.error.is_none(), "{:?}", got.error);
    assert_eq!((got.passed, got.failed, got.skipped), (2, 3, 1));
    let bad_msg = [
        "thread 'a::bad' panicked at crates/foo/src/a.rs:13:9:",
        "assertion `left == right` failed & more",
    ];
    let want =
        vec![
        Failure {
            elapsed: 0.25,
            ..fail(
                "hivekeep-foo",
                "a::bad",
                "",
                &bad_msg,
                &[
                    "setup <done>",
                    bad_msg[0],
                    bad_msg[1],
                    "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
                ],
            )
        },
        Failure {
            elapsed: 30.0,
            ..fail(
                "hivekeep-foo::e2e",
                "hang",
                "",
                &["(test timeout)", "waiting", "still waiting"],
                &["waiting", "still waiting"],
            )
        },
        Failure {
            elapsed: 0.5,
            ..fail("hivekeep-foo::e2e", "logs_only", "", &["line 1", "line 2"], &[])
        },
    ];
    assert_eq!(got.failures, want);
}

#[test]
fn nextest_failure_summary_is_the_last_resort() {
    let got = parsed(
        r#"<testsuites><testsuite name="s"><testcase name="t">
             <failure message="exit code 101"/>
           </testcase></testsuite></testsuites>"#,
    );
    assert_eq!(
        got.failures,
        vec![fail("s", "t", "", &["exit code 101"], &[])]
    );
}

#[test]
fn parse_run_detects_the_format() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let json = write(
        d,
        "unit.json",
        &stream(&[
            json!({"type": "suite", "event": "started", "test_count": 1}),
            started("t"),
            json!({"type": "test", "name": "t", "event": "ok"}),
            json!({"type": "suite", "event": "ok"}),
        ]),
    );
    let r = parse_run(&json);
    assert_eq!((r.name.as_str(), r.passed, r.failed), ("unit", 1, 0));
    assert!(r.error.is_none());

    // Content decides, not the extension; a byte-order mark is tolerated.
    let xml = write(d, "e2e.json", &format!("\u{feff}  {NEXTEST}"));
    let r = parse_run(&xml);
    assert_eq!((r.name.as_str(), r.passed, r.failed), ("e2e", 2, 3));

    let broken = write(d, "broken.xml", "<testsuites><testsuite>");
    assert!(parse_run(&broken)
        .error
        .unwrap()
        .starts_with("invalid JUnit XML"));
}

#[test]
fn parse_run_missing() {
    let dir = tempfile::tempdir().unwrap();
    let got = parse_run(&dir.path().join("e2e-gvisor.json"));
    assert!(got.error.is_some());
    assert_eq!(got.name, "e2e-gvisor");
}

#[test]
fn write_text_lists_failures() {
    let runs = [
        Run {
            name: "unit".into(),
            passed: 3,
            failed: 2,
            skipped: 1,
            failures: vec![
                fail("foo", "a::x", "", &["boom", &"y".repeat(1200)], &[]),
                fail("", "", "build failed", &["error: nope"], &[]),
            ],
            ..Run::default()
        },
        Run {
            name: "e2e".into(),
            error: Some("No such file".into()),
            ..Run::default()
        },
    ];
    let mut b = Vec::new();
    for r in &runs {
        r.write_text(&mut b).unwrap();
    }
    let want = format!(
        "unit: 3 passed, 2 failed, 1 skipped\n\
         --- FAIL: foo a::x\n    boom\n    {} ...\n\
         --- FAIL: build failed\n    error: nope\n\
         e2e: no results: No such file\n",
        "y".repeat(1000)
    );
    assert_eq!(String::from_utf8(b).unwrap(), want);
}

#[test]
fn write_annotations_points_at_sources() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(
        root,
        "Cargo.toml",
        "[workspace]\nmembers = [\"crates/foo\"]\n",
    );
    std::fs::create_dir_all(root.join("crates/foo/src")).unwrap();
    write(&root.join("crates/foo/src"), "a.rs", "");
    let abs = root.join("crates/foo/src/a.rs").display().to_string();
    let pkg = "foo (unittests src/lib.rs)";

    let runs = vec![Run {
        name: "unit".into(),
        failures: vec![
            fail(
                pkg,
                "a::x",
                "",
                &[
                    "thread 'a::x' panicked at crates/foo/src/a.rs:13:9:",
                    "50% off, a: b",
                ],
                &[],
            ),
            // The panic is in the standard library: no location.
            fail(
                pkg,
                "a::y",
                "",
                &["thread 'a::y' panicked at /rustc/abc/library/core/src/x.rs:1:1:"],
                &[],
            ),
            // The panic is in a dependency; the backtrace names our frame.
            fail(
                pkg,
                "a::z",
                "",
                &["thread 'a::z' panicked at /cargo/registry/src/dep/src/lib.rs:9:1:"],
                &["stack backtrace:", &format!("             at {abs}:31")],
            ),
            fail(
                "foo",
                "",
                "build failed",
                &[
                    "error[E0425]: cannot find value `x`",
                    "  --> crates/foo/src/a.rs:5:28",
                ],
                &[],
            ),
        ],
        ..Run::default()
    }];
    let mut b = Vec::new();
    write_annotations(&mut b, &runs, Some(root)).unwrap();
    let want = "\
::error file=crates/foo/src/a.rs,line=13,col=9,title=unit%3A foo (unittests src/lib.rs) a%3A%3Ax::thread 'a::x' panicked at crates/foo/src/a.rs:13:9:%0A50%25 off, a: b
::error title=unit%3A foo (unittests src/lib.rs) a%3A%3Ay::thread 'a::y' panicked at /rustc/abc/library/core/src/x.rs:1:1:
::error file=crates/foo/src/a.rs,line=31,title=unit%3A foo (unittests src/lib.rs) a%3A%3Az::thread 'a::z' panicked at /cargo/registry/src/dep/src/lib.rs:9:1:
::error file=crates/foo/src/a.rs,line=5,col=28,title=unit%3A foo build failed::error[E0425]: cannot find value `x`%0A  --> crates/foo/src/a.rs:5:28
";
    assert_eq!(String::from_utf8(b).unwrap(), want);

    // Without a root nothing is located.
    let mut b = Vec::new();
    write_annotations(&mut b, &runs, None).unwrap();
    assert!(!String::from_utf8(b).unwrap().contains("file="));
}

#[test]
fn write_annotations_capped() {
    let r = Run {
        name: "e2e".into(),
        failures: (0..12)
            .map(|i| fail("foo", &format!("t{i}"), "", &[], &[]))
            .collect(),
        ..Run::default()
    };
    let mut b = Vec::new();
    write_annotations(&mut b, &[r], None).unwrap();
    let s = String::from_utf8(b).unwrap();
    let lines: Vec<&str> = s.trim().split('\n').collect();
    assert_eq!(lines.len(), MAX_ANNOTATIONS);
    assert_eq!(
        *lines.last().unwrap(),
        "::error::3 more test failures, see the job summary"
    );
}

#[test]
fn write_markdown_renders_runs() {
    let runs = vec![
        Run {
            name: "unit".into(),
            passed: 3,
            failed: 2,
            failures: vec![
                Failure {
                    elapsed: 1.5,
                    ..fail("foo", "a::x<y>", "", &["has ``` in it"], &["log line"])
                },
                fail("", "", "build failed", &["error: nope"], &[]),
            ],
            ..Run::default()
        },
        Run {
            name: "root".into(),
            error: Some("file does not exist".into()),
            ..Run::default()
        },
    ];
    let mut b = Vec::new();
    write_markdown(&mut b, &runs, SUMMARY_BUDGET).unwrap();
    let s = String::from_utf8(b).unwrap();
    for want in [
        "| unit | 3 | 2 | 0 |",
        "| root | - | - | - |",
        "<code>a::x&lt;y&gt;</code> in <code>foo</code> (1.50s)",
        "````text\nhas ``` in it\n````\n",
        "<details><summary>output</summary>\n\n```text\nlog line\n```\n",
        "\n<code>build failed</code>\n\n```text\nerror: nope\n```\n",
    ] {
        assert!(s.contains(want), "missing {want:?} in:\n{s}");
    }

    let mut b = Vec::new();
    write_markdown(&mut b, &runs, 200).unwrap();
    let s = String::from_utf8(b).unwrap();
    assert!(
        // The large entry is left out; the small one after it still fits.
        !s.contains("a::x")
            && s.contains("error: nope")
            && s.contains("1 more failures did not fit"),
        "{s}"
    );
}
