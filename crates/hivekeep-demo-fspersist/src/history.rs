//! The persisted, newest-first history file.

use parking_lot::Mutex;
use std::io;
use std::path::Path;

/// The file on the Actor's root filesystem whose persistence is on show.
pub const HISTORY_PATH: &str = "/pod-history.log";
/// Lines kept, so the file stays small and readable.
pub const MAX_LINES: usize = 20;

static LOCK: Mutex<()> = Mutex::new(());

/// The count after the one recorded in the newest (first) line, or 0 when
/// there is no usable history.
pub fn next_count(lines: &[String]) -> u64 {
    let Some(first) = lines.first() else { return 0 };
    for field in first.split('|') {
        if let Some(v) = field.trim().strip_prefix("count=") {
            if let Ok(n) = v.parse::<u64>() {
                return n + 1;
            }
        }
    }
    0
}

/// The line recorded for one request.
pub fn format_line(ip: &str, count: u64, now: chrono::DateTime<chrono::Local>) -> String {
    format!(
        "pod={ip} | count={count} | time={}",
        now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    )
}

/// Prepends a line for `ip` to the history at `path`, keeps the newest
/// [`MAX_LINES`], writes it back and returns the new contents. On a write
/// error the would-be contents are returned alongside the error.
pub fn append(path: &Path, ip: &str) -> Result<String, (String, io::Error)> {
    let _guard = LOCK.lock();
    let mut lines: Vec<String> = std::fs::read_to_string(path)
        .map(|data| {
            data.lines()
                .filter(|l| !l.trim().is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let line = format_line(ip, next_count(&lines), chrono::Local::now());
    lines.insert(0, line.clone());
    lines.truncate(MAX_LINES);
    let out = lines.join("\n") + "\n";
    if let Err(e) = std::fs::write(path, &out) {
        tracing::error!(err = %e, "Error writing history file");
        return Err((out, e));
    }
    tracing::info!(line = %line, "Appended history line");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn next_count_cases() {
        for (name, lines, want) in [
            ("empty", s(&[]), 0),
            (
                "first line wins",
                s(&["pod=a | count=4 | time=t", "pod=b | count=9 | time=t"]),
                5,
            ),
            ("no count field", s(&["pod=a | time=t"]), 0),
            ("garbage count", s(&["pod=a | count=x | time=t"]), 0),
        ] {
            assert_eq!(next_count(&lines), want, "{name}");
        }
    }

    #[test]
    fn append_accumulates_and_caps() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("h.log");
        let first = append(&p, "10.0.0.1").unwrap();
        assert!(first.starts_with("pod=10.0.0.1 | count=0 | time="));
        let mut last = String::new();
        for _ in 0..30 {
            last = append(&p, "10.0.0.2").unwrap();
        }
        let lines: Vec<&str> = last.lines().collect();
        assert_eq!(lines.len(), MAX_LINES);
        assert!(
            lines[0].starts_with("pod=10.0.0.2 | count=30 |"),
            "{}",
            lines[0]
        );
        assert!(last.ends_with('\n'));
    }

    #[test]
    fn append_reports_write_failure() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("missing").join("h.log");
        let (out, _) = append(&p, "10.0.0.1").unwrap_err();
        assert!(out.contains("count=0"));
    }
}
