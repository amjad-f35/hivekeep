//! Summarizing failures from Rust test results.
//!
//! Two inputs are understood, told apart by content rather than extension:
//!
//! - libtest's JSON event stream (`cargo test -- -Z unstable-options
//!   --format json --report-time`), optionally with cargo's stderr merged in;
//! - JUnit XML as written by cargo-nextest.
//!
//! Each file becomes a [`Run`] whose failures are rendered as plain text for
//! the step log, a markdown job summary, and CI error annotations pointing at
//! the panicking source line under the workspace root.

mod excerpt;
mod libtest;
pub mod location;
mod nextest;

use std::io::{self, Write};
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

pub use location::{Location, Resolver};

const MAX_LINE_LEN: usize = 1000;
/// Step summaries over 1 MiB are rejected by the CI host; stay below it.
pub const SUMMARY_BUDGET: usize = 900 << 10;
/// The CI host drops error annotations past 10 per step.
pub const MAX_ANNOTATIONS: usize = 10;
/// File extensions `report` picks up from a directory argument.
pub const INPUT_EXTENSIONS: &[&str] = &[".json", ".jsonl", ".xml"];

/// A failed test, or a test binary (or crate build) that failed with no
/// failed test to blame.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Failure {
    /// The test binary (suite) or crate the failure belongs to.
    pub pkg: String,
    /// The test path, e.g. `module::tests::case`; empty for a binary failure.
    pub test: String,
    /// Used in place of `test` for a binary failure.
    pub label: String,
    pub elapsed: f64,
    pub message: Vec<String>,
    pub context: Vec<String>,
}

impl Failure {
    fn title(&self) -> &str {
        if self.test.is_empty() {
            &self.label
        } else {
            &self.test
        }
    }

    /// `pkg` and title, separated when both are present.
    fn qualified(&self) -> String {
        if self.pkg.is_empty() {
            self.title().to_string()
        } else {
            format!("{} {}", self.pkg, self.title())
        }
    }
}

/// The result of one input file.
#[derive(Debug, Default)]
pub struct Run {
    pub name: String,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub failures: Vec<Failure>,
    pub error: Option<String>,
}

/// Parses the results at `path`. A read or parse error is recorded on the run
/// rather than returned, so one bad file does not hide the others.
pub fn parse_run(path: &Path) -> Run {
    let mut r = Run {
        name: path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default(),
        ..Run::default()
    };
    let raw = match std::fs::read(path) {
        Ok(b) => String::from_utf8_lossy(&b).into_owned(),
        Err(e) => {
            r.error = Some(e.to_string());
            return r;
        }
    };
    parse_str(&raw, &mut r);
    r
}

/// Adds the results in `raw`, in either input format, to `r`.
pub fn parse_str(raw: &str, r: &mut Run) {
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    if raw.trim_start().starts_with('<') {
        if let Err(e) = nextest::parse(raw, r) {
            r.error = Some(format!("invalid JUnit XML: {e}"));
        }
    } else {
        libtest::parse(raw, r);
    }
}

impl Run {
    /// Plain-text summary for the step log.
    pub fn write_text(&self, w: &mut dyn Write) -> io::Result<()> {
        if let Some(e) = &self.error {
            return writeln!(w, "{}: no results: {e}", self.name);
        }
        writeln!(
            w,
            "{}: {} passed, {} failed, {} skipped",
            self.name, self.passed, self.failed, self.skipped
        )?;
        for f in &self.failures {
            writeln!(w, "--- FAIL: {}", f.qualified())?;
            for l in clip(&f.message) {
                writeln!(w, "    {l}")?;
            }
        }
        Ok(())
    }
}

/// Caps line length so one huge log line cannot swamp the output.
fn clip(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .map(|l| {
            if l.len() > MAX_LINE_LEN {
                let mut n = MAX_LINE_LEN;
                while n > 0 && !l.is_char_boundary(n) {
                    n -= 1;
                }
                format!("{} ...", &l[..n])
            } else {
                l.clone()
            }
        })
        .collect()
}

/// HTML-escapes `s` the way the summary renderer expects.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '"' => out.push_str("&#34;"),
            c => out.push(c),
        }
    }
    out
}

/// Renders runs as a markdown job summary, leaving out failures once the
/// output would pass `budget` bytes.
pub fn write_markdown(w: &mut dyn Write, runs: &[Run], budget: usize) -> io::Result<()> {
    let mut b = String::from("| run | passed | failed | skipped |\n|:--|--:|--:|--:|\n");
    for r in runs {
        if r.error.is_some() {
            b.push_str(&format!("| {} | - | - | - |\n", r.name));
        } else {
            b.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                r.name, r.passed, r.failed, r.skipped
            ));
        }
    }
    let mut omitted = 0;
    for r in runs.iter().filter(|r| !r.failures.is_empty()) {
        b.push_str(&format!("\n#### {}\n", html_escape(&r.name)));
        for f in &r.failures {
            let entry = f.markdown();
            if b.len() + entry.len() > budget {
                omitted += 1;
                continue;
            }
            b.push_str(&entry);
        }
    }
    if omitted > 0 {
        b.push_str(&format!(
            "\n{omitted} more failures did not fit, see the step logs.\n"
        ));
    }
    w.write_all(b.as_bytes())
}

impl Failure {
    fn markdown(&self) -> String {
        let mut b = format!("\n<code>{}</code>", html_escape(self.title()));
        if !self.pkg.is_empty() {
            b.push_str(&format!(" in <code>{}</code>", html_escape(&self.pkg)));
        }
        if !self.test.is_empty() {
            b.push_str(&format!(" ({:.2}s)", self.elapsed));
        }
        b.push_str("\n\n");
        write_code_block(&mut b, &clip(&self.message));
        if !self.context.is_empty() {
            b.push_str("\n<details><summary>output</summary>\n\n");
            write_code_block(&mut b, &clip(&self.context));
            b.push_str("\n</details>\n");
        }
        b
    }
}

static BACKTICKS: LazyLock<Regex> = LazyLock::new(|| Regex::new("`+").expect("valid regex"));

/// Fences lines with more backticks than any run inside them, so test output
/// cannot end the block early.
fn write_code_block(b: &mut String, lines: &[String]) {
    let n = lines
        .iter()
        .flat_map(|l| BACKTICKS.find_iter(l).map(|m| m.len() + 1))
        .fold(3, usize::max);
    let fence = "`".repeat(n);
    b.push_str(&format!("{fence}text\n"));
    for l in lines {
        b.push_str(l);
        b.push('\n');
    }
    b.push_str(&format!("{fence}\n"));
}

/// Prints one CI error annotation per failure. With a workspace `root`, each
/// points at the first source position in its message or output that lies
/// under the root (normally the panic location).
pub fn write_annotations(w: &mut dyn Write, runs: &[Run], root: Option<&Path>) -> io::Result<()> {
    let resolver = root.map(Resolver::new);
    let total: usize = runs.iter().map(|r| r.failures.len()).sum();
    let mut n = 0;
    for r in runs {
        for f in &r.failures {
            if n == MAX_ANNOTATIONS - 1 && total > MAX_ANNOTATIONS {
                return writeln!(
                    w,
                    "::error::{} more test failures, see the job summary",
                    total - n
                );
            }
            n += 1;
            let title = format!("{}: {}", r.name, f.qualified());
            let mut props = format!("title={}", escape_property(&title));
            if let Some(loc) = resolver
                .as_ref()
                .and_then(|res| res.find(f.message.iter().chain(&f.context)))
            {
                let col = loc.col.map(|c| format!("col={c},")).unwrap_or_default();
                props = format!(
                    "file={},line={},{col}{props}",
                    escape_property(&loc.file),
                    loc.line
                );
            }
            writeln!(
                w,
                "::error {props}::{}",
                escape_data(&clip(&f.message).join("\n"))
            )?;
        }
    }
    Ok(())
}

/// Escapes an annotation message.
fn escape_data(s: &str) -> String {
    s.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

/// Escapes an annotation property value.
fn escape_property(s: &str) -> String {
    escape_data(s).replace(':', "%3A").replace(',', "%2C")
}

#[cfg(test)]
mod tests;
