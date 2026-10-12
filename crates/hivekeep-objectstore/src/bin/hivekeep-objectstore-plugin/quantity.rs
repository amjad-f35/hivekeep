//! Byte counts written as Kubernetes quantities, such as `8Gi`.

/// Parses a positive whole number of bytes written as a quantity: an
/// integer with an optional binary (`Ki`..`Ei`) or decimal (`k`..`E`) suffix.
/// Fractions, milli-units and non-positive values are refused.
pub fn parse_capacity(s: &str) -> Result<i64, String> {
    let bad = || format!("{s:?} is not a positive whole number of bytes");
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split);
    if digits.is_empty() {
        return Err(bad());
    }
    let n: i64 = digits.parse().map_err(|_| bad())?;
    let mult: i64 = match suffix {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        _ => return Err(bad()),
    };
    let v = n.checked_mul(mult).ok_or_else(bad)?;
    if v <= 0 {
        return Err(bad());
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_capacities() {
        for (s, want) in [("8Gi", 8i64 << 30), ("512Mi", 512 << 20), ("1000", 1000)] {
            assert_eq!(parse_capacity(s), Ok(want), "{s}");
        }
        for s in ["", "0", "-1Gi", "0.5", "1m", "lots"] {
            assert!(parse_capacity(s).is_err(), "{s:?}");
        }
    }
}
