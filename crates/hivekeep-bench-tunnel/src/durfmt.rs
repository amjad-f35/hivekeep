//! Compact duration formatting for reports: `10s`, `1.5s`, `300ms`, `0s`.

use std::time::Duration;

/// Formats `d` with the largest unit of seconds, milliseconds or
/// microseconds that keeps it at least one, trimming trailing zeros.
pub fn format(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns == 0 {
        return "0s".into();
    }
    let (value, unit) = if ns >= 1_000_000_000 {
        (ns as f64 / 1e9, "s")
    } else if ns >= 1_000_000 {
        (ns as f64 / 1e6, "ms")
    } else if ns >= 1_000 {
        (ns as f64 / 1e3, "µs")
    } else {
        (ns as f64, "ns")
    };
    let mut s = format!("{value:.9}");
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s + unit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        for (d, want) in [
            (Duration::ZERO, "0s"),
            (Duration::from_secs(10), "10s"),
            (Duration::from_millis(1500), "1.5s"),
            (Duration::from_millis(300), "300ms"),
            (Duration::from_micros(250), "250µs"),
        ] {
            assert_eq!(format(d), want);
        }
    }
}
