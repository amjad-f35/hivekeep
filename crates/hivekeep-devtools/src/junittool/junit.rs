//! Reading JUnit XML counts and deciding whether every file reported tests.

use std::io::{self, Write};
use std::path::Path;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

/// Test counts from one JUnit file (or a sum of them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub tests: u64,
    pub failures: u64,
    pub errors: u64,
    pub skipped: u64,
}

impl Totals {
    pub fn add(&mut self, o: Totals) {
        self.tests += o.tests;
        self.failures += o.failures;
        self.errors += o.errors;
        self.skipped += o.skipped;
    }
}

/// Errors reading a JUnit file.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("reading {path}: {source}")]
    Read { path: String, source: io::Error },
    #[error("parsing {path}: {message}")]
    Xml { path: String, message: String },
}

/// Sums the counts of the root element's `<testsuite>` children. The root's own
/// attributes are ignored, so a root disagreeing with its children cannot
/// inflate the total.
pub fn parse(path: &Path) -> Result<Totals, ParseError> {
    let raw = std::fs::read_to_string(path).map_err(|source| ParseError::Read {
        path: path.display().to_string(),
        source,
    })?;
    parse_str(&raw).map_err(|message| ParseError::Xml {
        path: path.display().to_string(),
        message,
    })
}

fn parse_str(raw: &str) -> Result<Totals, String> {
    let mut reader = Reader::from_str(raw);
    let mut depth = 0usize;
    let mut seen_root = false;
    let mut out = Totals::default();
    loop {
        match reader.read_event().map_err(|e| e.to_string())? {
            Event::Start(e) => {
                if depth == 1 {
                    out.add(suite_totals(&e)?);
                }
                seen_root = true;
                depth += 1;
            }
            Event::Empty(e) => {
                if depth == 1 {
                    out.add(suite_totals(&e)?);
                }
                if depth == 0 {
                    return Ok(out);
                }
            }
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(out);
                }
            }
            Event::Eof => {
                return Err(if seen_root {
                    "unexpected EOF".into()
                } else {
                    "EOF: no root element".into()
                })
            }
            _ => {}
        }
    }
}

fn suite_totals(e: &BytesStart<'_>) -> Result<Totals, String> {
    if e.local_name().as_ref() != b"testsuite" {
        return Ok(Totals::default());
    }
    let mut t = Totals::default();
    for attr in e.attributes() {
        let attr = attr.map_err(|e| e.to_string())?;
        let slot = match attr.key.local_name().as_ref() {
            b"tests" => &mut t.tests,
            b"failures" => &mut t.failures,
            b"errors" => &mut t.errors,
            b"skipped" => &mut t.skipped,
            _ => continue,
        };
        let value = attr.unescape_value().map_err(|e| e.to_string())?;
        let value = value.trim();
        *slot = if value.is_empty() {
            0
        } else {
            value
                .parse()
                .map_err(|_| format!("invalid count {value:?}"))?
        };
    }
    Ok(t)
}

/// Resolves each argument to files. A directory contributes the files directly
/// inside it ending in one of `exts`, sorted; anything else passes through
/// literally, so a missing file is reported by whatever reads it.
pub fn expand(args: &[String], exts: &[&str]) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for a in args {
        let p = Path::new(a);
        if !p.is_dir() {
            out.push(a.clone());
            continue;
        }
        let mut matches: Vec<String> = std::fs::read_dir(p)?
            .filter_map(Result::ok)
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                exts.iter().any(|ext| name.ends_with(ext))
            })
            .map(|e| p.join(e.file_name()).display().to_string())
            .collect();
        matches.sort();
        out.extend(matches);
    }
    Ok(out)
}

/// One file's contribution. With an error the counts are meaningless.
#[derive(Debug)]
pub struct Row {
    pub path: String,
    pub totals: Totals,
    pub error: Option<ParseError>,
}

/// The verify decision, kept apart from reporting so it can be tested without
/// capturing output.
#[derive(Debug, Default)]
pub struct Verdict {
    pub rows: Vec<Row>,
    pub grand: Totals,
    /// Files that reported fewer than the minimum tests.
    pub empty: Vec<String>,
    /// Files that are missing or not valid JUnit.
    pub unreadable: Vec<String>,
}

impl Verdict {
    /// Whether every file was readable and met the minimum.
    pub fn ok(&self) -> bool {
        self.empty.is_empty() && self.unreadable.is_empty()
    }

    /// Renders the per-file table to `out` and the reasons for failure to `err_out`.
    pub fn write(&self, out: &mut dyn Write, err_out: &mut dyn Write, min: u64) -> io::Result<()> {
        let mut table = vec![vec![
            "FILE".to_string(),
            "TESTS".into(),
            "FAILURES".into(),
            "ERRORS".into(),
            "SKIPPED".into(),
        ]];
        for row in &self.rows {
            if row.error.is_some() {
                table.push(vec![
                    row.path.clone(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                ]);
            } else {
                table.push(totals_row(&row.path, row.totals));
            }
        }
        table.push(totals_row("TOTAL", self.grand));
        write_table(out, &table)?;

        for row in &self.rows {
            if let Some(e) = &row.error {
                writeln!(err_out, "junittool: {e}")?;
            }
        }
        for p in &self.empty {
            writeln!(
                err_out,
                "junittool: {p} reported {} test(s), want at least {min}. \
                 Check the package list and test filter of the run that wrote it.",
                self.tests_for(p)
            )?;
        }
        Ok(())
    }

    fn tests_for(&self, path: &str) -> u64 {
        self.rows
            .iter()
            .find(|r| r.path == path)
            .map(|r| r.totals.tests)
            .unwrap_or(0)
    }
}

fn totals_row(label: &str, t: Totals) -> Vec<String> {
    vec![
        label.to_string(),
        t.tests.to_string(),
        t.failures.to_string(),
        t.errors.to_string(),
        t.skipped.to_string(),
    ]
}

/// Left-aligned columns separated by at least two spaces.
fn write_table(out: &mut dyn Write, rows: &[Vec<String>]) -> io::Result<()> {
    let cols = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..cols)
        .map(|c| {
            rows.iter()
                .filter_map(|r| r.get(c))
                .map(|s| s.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for row in rows {
        let mut line = String::new();
        for (c, cell) in row.iter().enumerate() {
            if c + 1 == row.len() {
                line.push_str(cell);
            } else {
                line.push_str(&format!("{cell:<w$}", w = widths[c] + 2));
            }
        }
        writeln!(out, "{line}")?;
    }
    Ok(())
}

/// Reads and classifies every path. `min` applies per file, never to the total,
/// so one empty file fails even when others are full. Order matches `paths`.
pub fn evaluate(paths: &[String], min: u64) -> Verdict {
    let mut v = Verdict::default();
    for p in paths {
        match parse(Path::new(p)) {
            Ok(t) => {
                v.grand.add(t);
                if t.tests < min {
                    v.empty.push(p.clone());
                }
                v.rows.push(Row {
                    path: p.clone(),
                    totals: t,
                    error: None,
                });
            }
            Err(e) => {
                v.unreadable.push(p.clone());
                v.rows.push(Row {
                    path: p.clone(),
                    totals: Totals::default(),
                    error: Some(e),
                });
            }
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn write(dir: &Path, name: &str, content: &str) -> String {
        let p = dir.join(name);
        std::fs::write(&p, content).unwrap();
        p.display().to_string()
    }

    const TWO_SUITES: &str = r#"<testsuites tests="10" failures="1" errors="0">
  <testsuite name="pkg/a" tests="7" failures="1" errors="0" skipped="2"></testsuite>
  <testsuite name="pkg/b" tests="3" failures="0" errors="0" skipped="0"></testsuite>
</testsuites>"#;

    // What a reporter emits when a package matched no tests: suite present,
    // count zero. Distinct from a document with no suites at all.
    const ZERO_TESTS: &str = r#"<testsuites tests="0" failures="0" errors="0">
  <testsuite name="pkg/a" tests="0" failures="0"></testsuite>
</testsuites>"#;

    // The shape cargo-nextest writes: one suite per test binary, with
    // testcases (and their output) nested inside.
    const NEXTEST: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="1" errors="0" uuid="x" time="0.1">
  <testsuite name="hivekeep-devtools" tests="3" disabled="1" errors="0" failures="1" skipped="1">
    <testcase name="a::ok" classname="hivekeep-devtools" time="0.01"/>
    <testcase name="a::bad" classname="hivekeep-devtools" time="0.02">
      <failure type="test failure">thread 'a::bad' panicked at src/a.rs:3:5:</failure>
      <system-out><![CDATA[<testsuite tests="99"/>]]></system-out>
    </testcase>
    <testcase name="a::skip" classname="hivekeep-devtools"><skipped/></testcase>
  </testsuite>
</testsuites>"#;

    #[test]
    fn parse_cases() {
        let dir = tempfile::tempdir().unwrap();
        let cases: Vec<(&str, &str, Option<Totals>)> = vec![
            (
                "two suites are summed",
                TWO_SUITES,
                Some(Totals {
                    tests: 10,
                    failures: 1,
                    errors: 0,
                    skipped: 2,
                }),
            ),
            ("zero-test suite", ZERO_TESTS, Some(Totals::default())),
            (
                "no suites at all",
                "<testsuites></testsuites>",
                Some(Totals::default()),
            ),
            (
                "root attribute ignored in favour of children",
                r#"<testsuites tests="5"><testsuite tests="0"></testsuite></testsuites>"#,
                Some(Totals::default()),
            ),
            (
                "self-closing suites count",
                r#"<testsuites><testsuite tests="4"/></testsuites>"#,
                Some(Totals {
                    tests: 4,
                    ..Totals::default()
                }),
            ),
            (
                "nextest report with a nested testcase body",
                NEXTEST,
                Some(Totals {
                    tests: 3,
                    failures: 1,
                    errors: 0,
                    skipped: 1,
                }),
            ),
            ("malformed", "not xml at all", None),
            ("empty file", "", None),
        ];
        for (name, content, want) in cases {
            let got = parse(Path::new(&write(
                dir.path(),
                &format!("{}.xml", name.replace(' ', "_")),
                content,
            )));
            match want {
                Some(want) => assert_eq!(got.unwrap(), want, "{name}"),
                None => assert!(got.is_err(), "{name}: {got:?}"),
            }
        }
    }

    #[test]
    fn parse_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(parse(&dir.path().join("absent.xml")).is_err());
    }

    #[test]
    fn expand_cases() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let a = write(d, "a.xml", TWO_SUITES);
        let b = write(d, "b.xml", TWO_SUITES);
        write(d, "notes.txt", "ignored");
        let empty = d.join("empty");
        std::fs::create_dir(&empty).unwrap();
        let dir_arg = vec![d.display().to_string()];

        // A directory expands to its xml files, sorted; nothing else is picked up.
        assert_eq!(
            expand(&dir_arg, &[".xml"]).unwrap(),
            vec![a.clone(), b.clone()]
        );
        // An empty directory contributes nothing.
        assert!(expand(&[empty.display().to_string()], &[".xml"])
            .unwrap()
            .is_empty());
        // A missing path passes through literally, so evaluate reports it.
        let missing = d.join("absent.xml").display().to_string();
        assert_eq!(
            expand(std::slice::from_ref(&missing), &[".xml"]).unwrap(),
            vec![missing]
        );
        // Several extensions: a report directory holds both kinds of input.
        let j = write(d, "unit.json", "{}");
        assert_eq!(expand(&dir_arg, &[".json", ".xml"]).unwrap(), vec![a, b, j]);
    }

    #[test]
    fn evaluate_cases() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let good = write(d, "good.xml", TWO_SUITES);
        let good2 = write(d, "good2.xml", TWO_SUITES);
        let zero = write(d, "zero.xml", ZERO_TESTS);
        let bad = write(d, "bad.xml", "not xml");
        let missing = d.join("absent.xml").display().to_string();

        // (name, paths, min, ok, empty, unreadable, total tests)
        type Case = (&'static str, Vec<String>, u64, bool, usize, usize, u64);
        let cases: Vec<Case> = vec![
            (
                "all populated",
                vec![good.clone(), good2.clone()],
                1,
                true,
                0,
                0,
                20,
            ),
            (
                "single zero-test file",
                vec![zero.clone()],
                1,
                false,
                1,
                0,
                0,
            ),
            // min is per file: tests in one file must not cover for another
            // that reported none.
            (
                "one empty file among populated ones",
                vec![good.clone(), zero.clone()],
                1,
                false,
                1,
                0,
                10,
            ),
            ("missing file", vec![missing.clone()], 1, false, 0, 1, 0),
            ("malformed file", vec![bad.clone()], 1, false, 0, 1, 0),
            (
                "missing and empty are counted separately",
                vec![missing.clone(), zero.clone()],
                1,
                false,
                1,
                1,
                0,
            ),
            (
                "min above the reported count fails a populated file",
                vec![good.clone()],
                11,
                false,
                1,
                0,
                10,
            ),
            (
                "min at the reported count passes",
                vec![good.clone()],
                10,
                true,
                0,
                0,
                10,
            ),
            ("no paths is vacuously ok", vec![], 1, true, 0, 0, 0),
        ];
        for (name, paths, min, ok, empty, unreadable, total) in cases {
            let v = evaluate(&paths, min);
            assert_eq!(v.ok(), ok, "{name}");
            assert_eq!(v.empty.len(), empty, "{name}");
            assert_eq!(v.unreadable.len(), unreadable, "{name}");
            assert_eq!(v.grand.tests, total, "{name}");
            assert_eq!(v.rows.len(), paths.len(), "{name}");
        }
    }

    // The failure must name the file; otherwise the reader compares them by hand.
    #[test]
    fn write_names_the_offending_file() {
        let dir = tempfile::tempdir().unwrap();
        let zero = write(dir.path(), "zero.xml", ZERO_TESTS);
        let good = write(dir.path(), "good.xml", TWO_SUITES);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        evaluate(&[good.clone(), zero.clone()], 1)
            .write(&mut out, &mut err, 1)
            .unwrap();
        let (out, err) = (
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        );
        assert!(err.contains(&zero), "{err}");
        assert!(!err.contains(&good), "{err}");
        assert!(out.contains("TOTAL"), "{out}");
    }

    #[test]
    fn totals_add() {
        let mut got = Totals {
            tests: 1,
            failures: 2,
            errors: 3,
            skipped: 4,
        };
        got.add(Totals {
            tests: 10,
            failures: 20,
            errors: 30,
            skipped: 40,
        });
        assert_eq!(
            got,
            Totals {
                tests: 11,
                failures: 22,
                errors: 33,
                skipped: 44
            }
        );
    }

    #[test]
    fn table_aligns_columns() {
        let mut out = Vec::new();
        write_table(
            &mut out,
            &[
                vec!["A".into(), "BB".into()],
                vec!["CCC".into(), "D".into()],
            ],
        )
        .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "A    BB\nCCC  D\n");
    }
}
