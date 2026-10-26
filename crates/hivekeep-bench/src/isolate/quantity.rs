//! Kubernetes resource quantities (`1500m`, `4Gi`, `1e9`) read as the
//! integers a worker reports: CPU in millicores and memory in bytes, both
//! rounded up as the API machinery rounds.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

/// Why a quantity could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid quantity {0:?}")]
pub struct InvalidQuantity(pub String);

/// `mantissa * 10^exp10 * 2^exp2`.
struct Parsed {
    negative: bool,
    mantissa: i128,
    exp10: i32,
    exp2: u32,
}

fn parse(s: &str) -> Result<Parsed, InvalidQuantity> {
    let bad = || InvalidQuantity(s.to_string());
    let (negative, rest) = match s.as_bytes().first() {
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
    let (int, frac) = number.split_once('.').unwrap_or((number, ""));
    let digits = format!("{int}{frac}");
    let mantissa: i128 = digits.parse().map_err(|_| bad())?;
    let mut exp10 = -(frac.len() as i32);
    let mut exp2 = 0;
    match suffix {
        "" => {}
        "Ki" => exp2 = 10,
        "Mi" => exp2 = 20,
        "Gi" => exp2 = 30,
        "Ti" => exp2 = 40,
        "Pi" => exp2 = 50,
        "Ei" => exp2 = 60,
        "n" => exp10 -= 9,
        "u" => exp10 -= 6,
        "m" => exp10 -= 3,
        "k" => exp10 += 3,
        "M" => exp10 += 6,
        "G" => exp10 += 9,
        "T" => exp10 += 12,
        "P" => exp10 += 15,
        "E" => exp10 += 18,
        s if s.starts_with(['e', 'E']) => {
            let e: i32 = s[1..].parse().map_err(|_| bad())?;
            exp10 += e;
        }
        _ => return Err(bad()),
    }
    Ok(Parsed {
        negative,
        mantissa,
        exp10,
        exp2,
    })
}

/// The quantity scaled by `10^scale`, rounded up, saturating at the `i64`
/// range.
fn scaled(s: &str, scale: i32) -> Result<i64, InvalidQuantity> {
    let p = parse(s)?;
    let sat = |neg: bool| if neg { i64::MIN } else { i64::MAX };
    let Some(mut num) = p.mantissa.checked_mul(1i128 << p.exp2) else {
        return Ok(sat(p.negative));
    };
    let e = p.exp10 + scale;
    if e >= 0 {
        match 10i128
            .checked_pow(e as u32)
            .and_then(|m| num.checked_mul(m))
        {
            Some(v) => num = v,
            None => return Ok(sat(p.negative)),
        }
    } else {
        let div = match 10i128.checked_pow((-e) as u32) {
            Some(d) => d,
            None => return Ok(0),
        };
        // Round away from zero's floor: up for positive values, and toward
        // zero for negative ones, as ceiling does.
        let q = num / div;
        num = if !p.negative && num % div != 0 {
            q + 1
        } else {
            q
        };
    }
    let v = if p.negative { -num } else { num };
    Ok(i64::try_from(v).unwrap_or(sat(p.negative)))
}

/// The quantity in thousandths, rounded up: CPU millicores.
pub fn milli_value(s: &str) -> Result<i64, InvalidQuantity> {
    scaled(s, 3)
}

/// The quantity as an integer, rounded up: memory bytes.
pub fn value(s: &str) -> Result<i64, InvalidQuantity> {
    scaled(s, 0)
}

/// The `cpu` entry of a resource list in millicores; 0 when unset or
/// unreadable.
pub fn cpu_milli(list: Option<&BTreeMap<String, Quantity>>) -> i64 {
    list.and_then(|l| l.get("cpu"))
        .and_then(|q| milli_value(&q.0).ok())
        .unwrap_or(0)
}

/// The `memory` entry of a resource list in bytes; 0 when unset or
/// unreadable.
pub fn memory_bytes(list: Option<&BTreeMap<String, Quantity>>) -> i64 {
    list.and_then(|l| l.get("memory"))
        .and_then(|q| value(&q.0).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_like_the_api_machinery() {
        assert_eq!(milli_value("1500m").unwrap(), 1500);
        assert_eq!(milli_value("0.5").unwrap(), 500);
        assert_eq!(milli_value("86").unwrap(), 86_000);
        assert_eq!(milli_value("1.0001m").unwrap(), 2);
        assert_eq!(value("4Gi").unwrap(), 4 << 30);
        assert_eq!(value("160Gi").unwrap(), 160 << 30);
        assert_eq!(value("1G").unwrap(), 1_000_000_000);
        assert_eq!(value("1e9").unwrap(), 1_000_000_000);
        assert_eq!(value("1500m").unwrap(), 2);
        assert_eq!(value("12345Ki").unwrap(), 12345 * 1024);
        for bad in ["", "abc", "1Zi", "1..2", "."] {
            assert!(value(bad).is_err(), "{bad}");
        }
    }
}
