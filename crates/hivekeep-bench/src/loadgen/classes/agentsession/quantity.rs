//! Byte sizes as Kubernetes quantities (`16Mi`, `64Ki`, `1G`, `1.5Gi`) and
//! unit-suffixed durations (`2s`, `1.5s`, `1m30s`), the two value formats the
//! script files use.

use crate::loadgen::duration::Seconds;

/// Parses a quantity as a positive whole byte count.
pub fn parse_size(s: &str) -> Result<i64, String> {
    let n = parse_quantity(s)?;
    match n {
        Some(n) if n > 0 => Ok(n),
        _ => Err(format!("{s:?} is not a positive whole byte count")),
    }
}

/// The integral value of a quantity, `None` when it is fractional or too
/// large.
fn parse_quantity(s: &str) -> Result<Option<i64>, String> {
    let bad = || {
        format!("quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$': {s:?}")
    };
    let s = s.trim();
    let (neg, rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let num_end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(rest.len());
    let (number, suffix) = rest.split_at(num_end);
    if number.is_empty() || number == "." || number.matches('.').count() > 1 {
        return Err(bad());
    }
    let (whole, frac) = number.split_once('.').unwrap_or((number, ""));
    let digits = format!("{whole}{frac}");
    let mantissa: i128 = digits.parse().map_err(|_| bad())?;
    let mut num = mantissa;
    let mut den: i128 = 10i128.checked_pow(frac.len() as u32).ok_or_else(bad)?;
    let binary = |p: u32| 1i128 << (10 * p);
    let pow10 = |e: u32| 10i128.checked_pow(e);
    match suffix {
        "" => {}
        "Ki" => num *= binary(1),
        "Mi" => num *= binary(2),
        "Gi" => num *= binary(3),
        "Ti" => num *= binary(4),
        "Pi" => num *= binary(5),
        "Ei" => num *= binary(6),
        "n" => den *= 1_000_000_000,
        "u" => den *= 1_000_000,
        "m" => den *= 1_000,
        "k" => num *= 1_000,
        "M" => num *= 1_000_000,
        "G" => num *= 1_000_000_000,
        "T" => num *= 1_000_000_000_000,
        "P" => num *= 1_000_000_000_000_000,
        "E" => num *= 1_000_000_000_000_000_000,
        exp if exp.starts_with(['e', 'E']) => {
            let e: i32 = exp[1..].parse().map_err(|_| bad())?;
            if e.unsigned_abs() > 30 {
                return Ok(None);
            }
            let p = pow10(e.unsigned_abs()).ok_or_else(bad)?;
            if e >= 0 {
                num = num.checked_mul(p).ok_or_else(bad)?;
            } else {
                den *= p;
            }
        }
        _ => return Err(bad()),
    }
    if num % den != 0 {
        return Ok(None);
    }
    let v = num / den;
    let v = if neg { -v } else { v };
    Ok(i64::try_from(v).ok())
}

/// Renders a byte count as a binary-SI quantity: the largest power of 1024
/// that divides it exactly (`32Mi`, `1Gi`, `1536Mi`, `1500`).
pub fn format_size(n: i64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const SUFFIXES: [&str; 6] = ["Ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
    let mut v = n;
    let mut i = 0;
    while i < SUFFIXES.len() && v % 1024 == 0 {
        v /= 1024;
        i += 1;
    }
    if i == 0 {
        n.to_string()
    } else {
        format!("{v}{}", SUFFIXES[i - 1])
    }
}

/// Parses a duration written as signed decimal numbers with units (`ns`,
/// `us`, `µs`, `ms`, `s`, `m`, `h`), e.g. `2s`, `1.5s`, `1h30m`.
pub fn parse_duration(s: &str) -> Result<Seconds, String> {
    let bad = || format!("invalid duration {s:?}");
    let (neg, mut rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "0" {
        return Ok(Seconds::ZERO);
    }
    if rest.is_empty() {
        return Err(bad());
    }
    let mut total: f64 = 0.0;
    while !rest.is_empty() {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if end == 0 {
            return Err(bad());
        }
        let v: f64 = rest[..end].parse().map_err(|_| bad())?;
        rest = &rest[end..];
        let uend = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..uend] {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            "" => return Err(format!("missing unit in duration {s:?}")),
            u => return Err(format!("unknown unit {u:?} in duration {s:?}")),
        };
        rest = &rest[uend..];
        total += v * scale;
    }
    if total > i64::MAX as f64 {
        return Err(bad());
    }
    let nanos = total.round() as i64;
    Ok(Seconds::from_nanos(if neg { -nanos } else { nanos }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_parse_like_quantities() {
        for (s, want) in [
            ("16Mi", 16 << 20),
            ("64Ki", 64 << 10),
            ("1Gi", 1 << 30),
            ("1.5Gi", 3 << 29),
            ("1G", 1_000_000_000),
            ("1500", 1500),
            ("1e3", 1000),
            ("2k", 2000),
        ] {
            assert_eq!(parse_size(s), Ok(want), "{s}");
        }
        for s in ["0", "-1Mi", "0.5", "1500m", "abc", "1Xi", ""] {
            assert!(parse_size(s).is_err(), "{s}");
        }
    }

    #[test]
    fn sizes_format_in_binary_units() {
        for (n, want) in [
            (0, "0"),
            (1500, "1500"),
            (2048, "2Ki"),
            (32 << 20, "32Mi"),
            (1536 << 20, "1536Mi"),
            (1 << 30, "1Gi"),
        ] {
            assert_eq!(format_size(n), want);
            if n > 0 {
                assert_eq!(parse_size(want), Ok(n));
            }
        }
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("2s"), Ok(Seconds::from_secs(2)));
        assert_eq!(parse_duration("1.5s"), Ok(Seconds::from_millis(1500)));
        assert_eq!(parse_duration("500ms"), Ok(Seconds::from_millis(500)));
        assert_eq!(parse_duration("1m30s"), Ok(Seconds::from_secs(90)));
        assert_eq!(parse_duration("0"), Ok(Seconds::ZERO));
        assert!(parse_duration("-1s").unwrap().is_negative());
        for s in ["", "5", "1x", "s"] {
            assert!(parse_duration(s).is_err(), "{s}");
        }
        assert_eq!(
            parse_duration(&Seconds::from_secs(90).to_string()),
            Ok(Seconds::from_secs(90))
        );
    }
}
