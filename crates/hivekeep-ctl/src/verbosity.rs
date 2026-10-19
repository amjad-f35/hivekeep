//! `-v` diagnostics.
//!
//! Each `-v` raises the verbosity by one. Messages are written to stderr
//! when the verbosity is at least their level.

use std::fmt;
use std::time::Duration;

use crate::io::Sink;

/// `-v`: high-level progress, non-fatal errors, retries and waits.
pub const BASIC: u8 = 1;
/// `-vv`: step-by-step execution flow.
pub const FLOW: u8 = 2;
/// `-vvvvv`: request and response payloads and other tracing detail.
pub const TRACE: u8 = 5;

/// An operation slower than this is reported at [`BASIC`] even when its
/// normal completion message is at [`FLOW`].
pub const SLOW_OP_THRESHOLD: Duration = Duration::from_secs(2);

/// Writes leveled diagnostics to a sink.
#[derive(Clone)]
pub struct Logger {
    level: u8,
    sink: Sink,
}

impl Logger {
    /// A logger at `level` writing to `sink`.
    pub fn new(level: u8, sink: Sink) -> Self {
        Logger { level, sink }
    }

    /// A logger that prints nothing.
    pub fn silent() -> Self {
        Logger::new(0, Sink::null())
    }

    /// The configured verbosity.
    pub fn level(&self) -> u8 {
        self.level
    }

    /// Whether messages at `level` are printed.
    pub fn enabled(&self, level: u8) -> bool {
        self.level >= level
    }

    /// Prints `msg` if `level` is enabled.
    pub fn log(&self, level: u8, msg: impl fmt::Display) {
        if self.enabled(level) {
            let text = msg.to_string();
            self.sink.line(text.trim_end_matches('\n'));
        }
    }

    /// Prints a completion message at [`FLOW`], or at [`BASIC`] when the
    /// operation took at least [`SLOW_OP_THRESHOLD`].
    pub fn completed(&self, elapsed: Duration, msg: impl fmt::Display) {
        let level = if elapsed >= SLOW_OP_THRESHOLD {
            BASIC
        } else {
            FLOW
        };
        self.log(level, msg);
    }
}

/// Renders a duration rounded to milliseconds, like `1.234s` or `15ms`.
pub fn round_ms(d: Duration) -> String {
    let ms = (d.as_secs_f64() * 1000.0).round() as u64;
    if ms == 0 {
        "0s".to_string()
    } else if ms < 1000 {
        format!("{ms}ms")
    } else {
        let secs = ms as f64 / 1000.0;
        let s = format!("{secs:.3}");
        let s = s.trim_end_matches('0').trim_end_matches('.');
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels() {
        for (level, basic, flow, trace) in [
            (0, false, false, false),
            (BASIC, true, false, false),
            (FLOW, true, true, false),
            (TRACE, true, true, true),
        ] {
            let (sink, buf) = Sink::buffer();
            let log = Logger::new(level, sink);
            log.log(BASIC, "basic-msg");
            log.log(FLOW, "flow-msg");
            log.log(TRACE, "trace-msg");
            let got = buf.contents();
            assert_eq!(got.contains("basic-msg"), basic, "level {level}: {got:?}");
            assert_eq!(got.contains("flow-msg"), flow, "level {level}: {got:?}");
            assert_eq!(got.contains("trace-msg"), trace, "level {level}: {got:?}");
        }
    }

    #[test]
    fn slow_completion_is_basic() {
        let (sink, buf) = Sink::buffer();
        let log = Logger::new(BASIC, sink);
        log.completed(Duration::from_millis(5), "fast");
        log.completed(Duration::from_secs(3), "slow");
        assert_eq!(buf.contents(), "slow\n");
    }

    #[test]
    fn durations() {
        assert_eq!(round_ms(Duration::from_micros(400)), "0s");
        assert_eq!(round_ms(Duration::from_millis(15)), "15ms");
        assert_eq!(round_ms(Duration::from_millis(1500)), "1.5s");
        assert_eq!(round_ms(Duration::from_secs(2)), "2s");
    }
}
