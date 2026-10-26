//! Durations as the load generator's config and logs spell them.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A signed duration carried in a config payload as a JSON number of
/// seconds, which is how the locust flags spell every time value. Negative
/// values decode so that validation can name them; [`Seconds::duration`]
/// clamps them to zero.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seconds(i64);

impl Seconds {
    /// Zero seconds.
    pub const ZERO: Seconds = Seconds(0);

    /// A duration of `nanos` nanoseconds.
    pub const fn from_nanos(nanos: i64) -> Self {
        Seconds(nanos)
    }

    /// A duration of `ms` milliseconds.
    pub const fn from_millis(ms: i64) -> Self {
        Seconds(ms * 1_000_000)
    }

    /// A duration of `secs` seconds.
    pub const fn from_secs(secs: i64) -> Self {
        Seconds(secs * 1_000_000_000)
    }

    /// The duration in nanoseconds, possibly negative.
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// Whether the duration is below zero.
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// The duration as a [`Duration`]; negative values read as zero.
    pub fn duration(self) -> Duration {
        Duration::from_nanos(self.0.max(0) as u64)
    }

    /// The duration in (fractional) seconds.
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }
}

impl From<Duration> for Seconds {
    fn from(d: Duration) -> Self {
        Seconds(d.as_nanos().min(i64::MAX as u128) as i64)
    }
}

impl fmt::Display for Seconds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_nanos(self.0))
    }
}

impl fmt::Debug for Seconds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Seconds {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(self.as_secs_f64())
    }
}

impl<'de> Deserialize<'de> for Seconds {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let secs = Option::<f64>::deserialize(d)
            .map_err(|e| serde::de::Error::custom(format!("seconds: {e}")))?;
        Ok(match secs {
            None => Seconds::ZERO,
            Some(s) => Seconds((s * 1e9) as i64),
        })
    }
}

/// Formats `d` the compact way logs print durations: `0s`, `1.5µs`,
/// `500ms`, `2s`, `1m30s`, `2h0m0s`.
pub fn format_duration(d: Duration) -> String {
    format_nanos(d.as_nanos().min(i64::MAX as u128) as i64)
}

/// [`format_duration`] over a signed count of nanoseconds.
pub fn format_nanos(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }
    let neg = nanos < 0;
    let u = nanos.unsigned_abs();
    let mut out = String::new();
    if u < 1_000_000_000 {
        // Below a second: one unit with a fractional part.
        let (unit, scale) = if u < 1_000 {
            ("ns", 1)
        } else if u < 1_000_000 {
            ("µs", 1_000)
        } else {
            ("ms", 1_000_000)
        };
        out.push_str(&fraction(u, scale));
        out.push_str(unit);
    } else {
        let hours = u / 3_600_000_000_000;
        let rem = u % 3_600_000_000_000;
        let mins = rem / 60_000_000_000;
        let secs_nanos = rem % 60_000_000_000;
        if hours > 0 {
            out.push_str(&format!("{hours}h{mins}m"));
        } else if mins > 0 {
            out.push_str(&format!("{mins}m"));
        }
        out.push_str(&fraction(secs_nanos, 1_000_000_000));
        out.push('s');
    }
    if neg {
        out.insert(0, '-');
    }
    out
}

/// `v / scale` with trailing fractional zeros dropped.
fn fraction(v: u64, scale: u64) -> String {
    let whole = v / scale;
    let frac = v % scale;
    if frac == 0 {
        return whole.to_string();
    }
    let digits = scale.to_string().len() - 1;
    let s = format!("{frac:0digits$}");
    format!("{whole}.{}", s.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_log_durations() {
        for (nanos, want) in [
            (0, "0s"),
            (1, "1ns"),
            (1_500, "1.5µs"),
            (500_000_000, "500ms"),
            (1_250_000, "1.25ms"),
            (2_000_000_000, "2s"),
            (90_000_000_000, "1m30s"),
            (7_200_000_000_000, "2h0m0s"),
            (-500_000_000, "-500ms"),
            (1_500_000_000, "1.5s"),
        ] {
            assert_eq!(format_nanos(nanos), want, "{nanos}");
        }
    }

    #[test]
    fn seconds_round_trip_json() {
        let s: Seconds = serde_json::from_str("0.5").unwrap();
        assert_eq!(s, Seconds::from_millis(500));
        assert_eq!(serde_json::to_string(&s).unwrap(), "0.5");
        let neg: Seconds = serde_json::from_str("-1").unwrap();
        assert!(neg.is_negative());
        assert_eq!(neg.duration(), Duration::ZERO);
        assert!(serde_json::from_str::<Seconds>("\"soon\"").is_err());
    }
}
