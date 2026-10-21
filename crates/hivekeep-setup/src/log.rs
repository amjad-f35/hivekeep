//! Installer progress output.
//!
//! The installer talks to a human watching a terminal, so its output is plain
//! lines rather than structured tracing events: a coloured `[step]` banner per
//! phase, informational lines, warnings on stderr, and a fixed-shape timing
//! line (`  (label took N.NNNs)`) that log scrapers can attribute slow installs
//! with.

use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

const COLOR_CYAN: &str = "\x1b[1;36m";
const COLOR_RESET: &str = "\x1b[0m";

type Sink = Arc<Mutex<Box<dyn Write + Send>>>;

fn sink() -> &'static Mutex<Option<Sink>> {
    static SINK: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();
    SINK.get_or_init(|| Mutex::new(None))
}

/// Redirects progress output (not warnings) to `w`. `None` restores stdout.
pub fn set_output(w: Option<Box<dyn Write + Send>>) {
    *sink().lock().unwrap_or_else(|e| e.into_inner()) = w.map(|w| Arc::new(Mutex::new(w)));
}

/// Writes `text` verbatim to the progress output.
pub fn write_raw(text: &str) {
    let current = sink().lock().unwrap_or_else(|e| e.into_inner()).clone();
    match current {
        Some(s) => {
            let mut w = s.lock().unwrap_or_else(|e| e.into_inner());
            let _ = w.write_all(text.as_bytes());
            let _ = w.flush();
        }
        None => {
            let mut out = io::stdout().lock();
            let _ = out.write_all(text.as_bytes());
            let _ = out.flush();
        }
    }
}

/// Announces a phase of the install.
pub fn step(msg: &str) {
    write_raw(&format!("{COLOR_CYAN}[step]: {msg}{COLOR_RESET}\n"));
}

/// Writes an informational line.
pub fn info(msg: &str) {
    write_raw(&format!("{msg}\n"));
}

/// Writes a warning to stderr, which stays visible when stdout is redirected.
pub fn warn(msg: &str) {
    eprintln!("Warning: {msg}");
}

/// Formats the timing line for an operation that started at `start`.
pub fn elapsed_line(start: Instant, label: &str) -> String {
    format!("  ({label} took {:.3}s)\n", start.elapsed().as_secs_f64())
}

/// Writes the timing line for an operation that started at `start`.
pub fn elapsed(start: Instant, label: &str) {
    write_raw(&elapsed_line(start, label));
}

/// Prints the timing line when dropped, so every exit path of a scope is
/// timed.
pub struct Timer {
    start: Instant,
    label: String,
}

impl Timer {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            label: label.into(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        elapsed(self.start, &self.label);
    }
}

/// `step!("deploying {}", name)`.
#[macro_export]
macro_rules! step {
    ($($arg:tt)*) => { $crate::log::step(&format!($($arg)*)) };
}

/// `info!("applied {} objects", n)`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::log::info(&format!($($arg)*)) };
}

/// `warn!("not recording: {}", err)`.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::log::warn(&format!($($arg)*)) };
}

/// A writer that collects into a shared buffer, for capturing output in tests.
#[derive(Clone, Default)]
pub struct Capture(pub Arc<Mutex<Vec<u8>>>);

impl Capture {
    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn elapsed_has_a_fixed_shape() {
        let start = Instant::now() - Duration::from_millis(1500);
        let line = elapsed_line(start, "image resolve");
        let want = regex::Regex::new(r"^  \(image resolve took 1\.5\d\ds\)\n$").unwrap();
        assert!(want.is_match(&line), "elapsed printed {line:?}");
    }
}
