//! Durations in the `1s` / `500ms` / `1m30s` spelling the flags and query
//! parameters use.

use std::time::Duration;

/// Parses a sequence of `<number><unit>` terms (units `ns`, `us`, `µs`, `ms`,
/// `s`, `m`, `h`; fractions allowed), or a bare `0`.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let bad = || format!("invalid duration {input:?}");
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    if input.is_empty() {
        return Err(bad());
    }
    let mut rest = input;
    let mut total = 0f64;
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(bad)?;
        if num_len == 0 {
            return Err(bad());
        }
        let value: f64 = rest[..num_len].parse().map_err(|_| bad())?;
        rest = &rest[num_len..];
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..unit_len] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return Err(bad()),
        };
        total += value * scale;
        rest = &rest[unit_len..];
    }
    Duration::try_from_secs_f64(total).map_err(|_| bad())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_terms() {
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
        for bad in ["", "soon", "1", "s", "1x", "-1s"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} parsed");
        }
    }
}
