//! Human-friendly byte sizes: plain bytes, or a `Ki`/`Mi`/`Gi` (binary) or
//! `K`/`M`/`G` (decimal) suffix, e.g. `512Mi`, `2Gi`, `1000000`.

/// Why a size did not parse.
#[derive(Debug, thiserror::Error)]
#[error("parsing size {input:?}: {source}")]
pub struct SizeError {
    input: String,
    #[source]
    source: std::num::ParseIntError,
}

const MULTIPLIERS: &[(&str, i64)] = &[
    ("Ki", 1 << 10),
    ("Mi", 1 << 20),
    ("Gi", 1 << 30),
    ("K", 1000),
    ("M", 1000 * 1000),
    ("G", 1000 * 1000 * 1000),
];

/// Parses `s` into a byte count. Negative values parse; callers that need a
/// non-negative size check for themselves.
pub fn parse_bytes(s: &str) -> Result<i64, SizeError> {
    let err = |source| SizeError {
        input: s.to_string(),
        source,
    };
    for (suffix, mult) in MULTIPLIERS {
        if let Some(v) = s.strip_suffix(suffix) {
            let n: i64 = v.trim().parse().map_err(err)?;
            return Ok(n * mult);
        }
    }
    s.trim().parse().map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bytes_cases() {
        let ok: &[(&str, i64)] = &[
            ("512Mi", 512 << 20),
            ("2Gi", 2 << 30),
            ("16Ki", 16 << 10),
            ("1G", 1_000_000_000),
            ("4096", 4096),
            // Rejected later by WriteRAM.
            ("-1Gi", -(1 << 30)),
        ];
        for (input, want) in ok {
            assert_eq!(parse_bytes(input).unwrap(), *want, "{input}");
        }
        for input in ["", "Gi", "1.5Gi"] {
            assert!(parse_bytes(input).is_err(), "{input:?} should fail");
        }
    }
}
