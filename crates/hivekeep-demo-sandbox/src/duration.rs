//! Parsing of compound durations like `1h2m3.5s`, `250ms` or `90s`.

use std::time::Duration;

/// Why a duration string was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid duration {0:?}")]
pub struct InvalidDuration(pub String);

/// Parses a sequence of `<number><unit>` terms (units `ns`, `us`, `µs`,
/// `ms`, `s`, `m`, `h`; fractions allowed). A bare `0` is zero. Negative
/// durations are refused.
pub fn parse(raw: &str) -> Result<Duration, InvalidDuration> {
    let bad = || InvalidDuration(raw.to_string());
    let s = raw.strip_prefix('+').unwrap_or(raw);
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.is_empty() || s.starts_with('-') {
        return Err(bad());
    }
    let mut rest = s;
    let mut total = 0f64;
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(bad)?;
        let (num, tail) = rest.split_at(num_len);
        if num.is_empty() || num == "." {
            return Err(bad());
        }
        let value: f64 = num.parse().map_err(|_| bad())?;
        let unit_len = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, next) = tail.split_at(unit_len);
        let nanos_per = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => return Err(bad()),
        };
        total += value * nanos_per;
        rest = next;
    }
    Ok(Duration::from_nanos(total.round() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted() {
        for (raw, want) in [
            ("0", Duration::ZERO),
            ("30s", Duration::from_secs(30)),
            ("1m30s", Duration::from_secs(90)),
            ("250ms", Duration::from_millis(250)),
            ("1.5s", Duration::from_millis(1500)),
            ("2h", Duration::from_secs(7200)),
            ("10us", Duration::from_micros(10)),
        ] {
            assert_eq!(parse(raw), Ok(want), "{raw}");
        }
    }

    #[test]
    fn refused() {
        for raw in ["", "5", "-1s", "s", "1x", "1.2.3s", "."] {
            assert!(parse(raw).is_err(), "{raw}");
        }
    }
}
