//! Picking the lines of a test's output that explain its failure.
//!
//! Rust tests fail by panicking, and the panic hook writes one unindented
//! `thread '<name>' panicked at <file>:<line>:<col>:` line followed by the
//! payload. That block is almost always the best summary, so it is preferred
//! over the tail of whatever the test logged.

/// Lines kept for a failure's message.
pub(super) const MAX_MESSAGE_LINES: usize = 20;
/// Lines kept for the output shown beside the message.
pub(super) const MAX_CONTEXT_LINES: usize = 60;

/// Whether `l` is the panic hook's report line. Logs quoting a panic from
/// elsewhere (a pod's output, say) are normally indented and do not match.
pub(super) fn is_panic_line(l: &str) -> bool {
    l.starts_with("thread '") && l.contains(" panicked at ")
}

/// Whether `l` reports the test process dying rather than a test failing.
fn is_crash_line(l: &str) -> bool {
    is_panic_line(l)
        || l.starts_with("fatal runtime error: ")
        || (l.starts_with("thread '") && l.contains("has overflowed its stack"))
        || l.starts_with("thread caused non-unwinding panic")
        || l.trim_start()
            .starts_with("process didn't exit successfully")
}

/// The hint the panic hook appends when backtraces are off; it never helps
/// explain a failure.
fn is_backtrace_hint(l: &str) -> bool {
    l.starts_with("note: run with `RUST_BACKTRACE=")
}

/// Up to `n` lines starting at the first panic report, if there is one.
pub(super) fn panic_block(lines: &[String], n: usize) -> Option<Vec<String>> {
    let i = lines.iter().position(|l| is_panic_line(l))?;
    Some(from(&lines[i..], n))
}

/// Up to `n` lines likely to explain why a test process failed: the first
/// panic or crash report, else the last lines it printed.
pub(super) fn excerpt(lines: &[String], n: usize) -> Vec<String> {
    match lines.iter().position(|l| is_crash_line(l)) {
        Some(i) => from(&lines[i..], n),
        None => tail(lines, n),
    }
}

/// Up to `n` lines of `lines`, leaving out the backtrace hint.
fn from(lines: &[String], n: usize) -> Vec<String> {
    lines
        .iter()
        .filter(|l| !is_backtrace_hint(l))
        .take(n)
        .cloned()
        .collect()
}

pub(super) fn head(lines: &[String], n: usize) -> Vec<String> {
    lines[..lines.len().min(n)].to_vec()
}

pub(super) fn tail(lines: &[String], n: usize) -> Vec<String> {
    lines[lines.len().saturating_sub(n)..].to_vec()
}

/// Splits captured output into lines, without the trailing empty one.
pub(super) fn lines_of(s: &str) -> Vec<String> {
    s.lines().map(str::to_string).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn panic_lines() {
        for (line, want) in [
            ("thread 'a::b' panicked at src/a.rs:1:2:", true),
            ("thread 'a::b' (1234) panicked at src/a.rs:1:2:", true),
            ("thread '<unnamed>' panicked at 'boom', src/a.rs:1:2", true),
            ("    thread 'x' panicked at src/a.rs:1:2:", false),
            ("pod log: thread 'x' panicked at src/a.rs:1:2:", false),
            ("panicked at src/a.rs:1:2", false),
        ] {
            assert_eq!(is_panic_line(line), want, "{line:?}");
        }
    }

    #[test]
    fn panic_block_starts_at_the_panic_and_drops_the_hint() {
        let out = v(&[
            "setting up",
            "thread 't' panicked at src/a.rs:3:5:",
            "assertion `left == right` failed",
            "  left: 1",
            " right: 2",
            "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
        ]);
        assert_eq!(panic_block(&out, 20).unwrap(), out[1..5].to_vec());
        assert_eq!(panic_block(&out, 2).unwrap(), out[1..3].to_vec());
        assert!(panic_block(&v(&["just logs"]), 20).is_none());
    }

    #[test]
    fn excerpt_prefers_crash_reports() {
        let lines = v(&["1", "2", "3", "4"]);
        assert_eq!(excerpt(&lines, 2), v(&["3", "4"]));
        let overflow = v(&[
            "log",
            "thread 'deep' has overflowed its stack",
            "fatal runtime error: stack overflow",
            "more",
        ]);
        assert_eq!(excerpt(&overflow, 2), overflow[1..3].to_vec());
        let abort = v(&[
            "log",
            "error: test failed, to rerun pass `--lib`",
            "Caused by:",
            "  process didn't exit successfully: `t` (signal: 6, SIGABRT: process abort signal)",
        ]);
        assert_eq!(excerpt(&abort, 5), abort[3..].to_vec());
    }

    #[test]
    fn head_tail_and_lines() {
        let l = v(&["a", "b", "c"]);
        assert_eq!(head(&l, 2), v(&["a", "b"]));
        assert_eq!(head(&l, 9), l);
        assert_eq!(tail(&l, 2), v(&["b", "c"]));
        assert_eq!(tail(&l, 9), l);
        assert_eq!(lines_of("a\nb\n"), v(&["a", "b"]));
        assert!(lines_of("").is_empty());
    }
}
