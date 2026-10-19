//! Resource quantities in the notation cluster tooling uses: `500m` CPU,
//! `2Gi` memory.

/// Renders a byte count with the largest binary suffix that divides it
/// exactly (`2Gi`, `1536`). Counts below 1024 use decimal suffixes, so that
/// `1000` reads as `1k`.
pub fn format_bytes(n: i64) -> String {
    const BINARY: [&str; 7] = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
    const DECIMAL: [&str; 7] = ["", "k", "M", "G", "T", "P", "E"];
    if n == 0 {
        return "0".into();
    }
    let (base, suffixes): (i128, &[&str; 7]) = if n.unsigned_abs() < 1024 {
        (1000, &DECIMAL)
    } else {
        (1024, &BINARY)
    };
    let mut v = n as i128;
    let mut exp = 0;
    while exp < 6 && v % base == 0 {
        v /= base;
        exp += 1;
    }
    format!("{v}{}", suffixes[exp])
}

/// A parsed quantity, held in units of 10^-9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Quantity {
    nanos: i128,
}

/// A string that is not a quantity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid quantity {0:?}")]
pub struct ParseQuantityError(String);

impl Quantity {
    /// Parses `500m`, `1.5`, `12345Ki`, `2e3` and the like.
    pub fn parse(s: &str) -> Result<Self, ParseQuantityError> {
        let err = || ParseQuantityError(s.to_string());
        let t = s.trim();
        let num_end = t
            .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+' || c == '-'))
            .unwrap_or(t.len());
        let (num, suffix) = t.split_at(num_end);
        if num.is_empty() {
            return Err(err());
        }
        // (numerator, denominator) of the suffix's multiplier.
        let (mul, div): (i128, i128) = match suffix {
            "" => (1, 1),
            "n" => (1, 1_000_000_000),
            "u" => (1, 1_000_000),
            "m" => (1, 1000),
            "k" => (1000, 1),
            "M" => (1_000_000, 1),
            "G" => (1_000_000_000, 1),
            "T" => (1_000_000_000_000, 1),
            "P" => (1_000_000_000_000_000, 1),
            "E" => (1_000_000_000_000_000_000, 1),
            "Ki" => (1 << 10, 1),
            "Mi" => (1 << 20, 1),
            "Gi" => (1 << 30, 1),
            "Ti" => (1 << 40, 1),
            "Pi" => (1 << 50, 1),
            "Ei" => (1 << 60, 1),
            s if s.starts_with(['e', 'E']) => {
                let exp: u32 = s[1..].parse().map_err(|_| err())?;
                (10i128.checked_pow(exp).ok_or_else(err)?, 1)
            }
            _ => return Err(err()),
        };
        let (neg, digits) = match num.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, num.strip_prefix('+').unwrap_or(num)),
        };
        let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(err());
        }
        if !int_part
            .chars()
            .chain(frac_part.chars())
            .all(|c| c.is_ascii_digit())
        {
            return Err(err());
        }
        // Value as an exact fraction: mantissa / 10^frac_len.
        let mantissa: i128 = format!("{int_part}{frac_part}")
            .trim_start_matches('0')
            .parse()
            .unwrap_or(0);
        let scale = 10i128.pow(frac_part.len() as u32);
        let num = mantissa * mul * 1_000_000_000;
        let den = scale * div;
        // Round away from zero to the nearest nano, so nothing is lost.
        let mut nanos = num / den + i128::from(num % den != 0);
        if neg {
            nanos = -nanos;
        }
        Ok(Quantity { nanos })
    }

    /// The value in thousandths, rounded up.
    pub fn milli_value(&self) -> i64 {
        ceil_div(self.nanos, 1_000_000) as i64
    }

    /// The value in whole units, rounded up.
    pub fn value(&self) -> i64 {
        ceil_div(self.nanos, 1_000_000_000) as i64
    }
}

impl std::ops::Add for Quantity {
    type Output = Quantity;
    fn add(self, rhs: Quantity) -> Quantity {
        Quantity {
            nanos: self.nanos + rhs.nanos,
        }
    }
}

fn ceil_div(a: i128, b: i128) -> i128 {
    let q = a / b;
    if a % b > 0 {
        q + 1
    } else {
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes() {
        assert_eq!(format_bytes(0), "0");
        assert_eq!(format_bytes(500), "500");
        assert_eq!(format_bytes(1000), "1k");
        assert_eq!(format_bytes(1 << 30), "1Gi");
        assert_eq!(format_bytes(4 << 30), "4Gi");
        assert_eq!(format_bytes(1536), "1536");
        assert_eq!(format_bytes(3 << 20), "3Mi");
    }

    #[test]
    fn parse() {
        assert_eq!(Quantity::parse("250m").unwrap().milli_value(), 250);
        assert_eq!(Quantity::parse("123456789n").unwrap().milli_value(), 124);
        assert_eq!(Quantity::parse("1").unwrap().milli_value(), 1000);
        assert_eq!(Quantity::parse("1.5").unwrap().milli_value(), 1500);
        assert_eq!(Quantity::parse("12345Ki").unwrap().value(), 12345 * 1024);
        assert_eq!(Quantity::parse("2e3").unwrap().value(), 2000);
        assert_eq!(Quantity::parse("64Mi").unwrap().value(), 64 << 20);
        assert!(Quantity::parse("abc").is_err());
        assert!(Quantity::parse("1Q").is_err());
    }

    #[test]
    fn add() {
        let q = Quantity::parse("100m").unwrap() + Quantity::parse("150m").unwrap();
        assert_eq!(q.milli_value(), 250);
    }
}
