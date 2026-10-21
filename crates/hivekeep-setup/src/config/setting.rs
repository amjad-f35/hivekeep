//! The declaration of one configurable value, and the parsing every channel
//! (flag, environment, file) shares.

use std::time::Duration;

/// The type of a setting's value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValueKind {
    #[default]
    String,
    Bool,
    Int,
    Duration,
}

impl ValueKind {
    /// What the kind accepts, for error messages.
    pub fn description(self) -> &'static str {
        match self {
            Self::Bool => "true or false",
            Self::Int => "an integer",
            Self::Duration => "a duration such as 60s or 5m",
            Self::String => "a string",
        }
    }
}

/// One configurable value. It is the single definition: the flag, the
/// environment variable and the file key are all projections of it, so a
/// setting cannot reach one channel and miss another.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Setting {
    /// Canonical name and path in the configuration file, dot-separated:
    /// `api.postgres.schema`.
    pub key: String,
    /// Environment variable that supplies it.
    pub env: String,
    /// Command-line name without dashes. Empty only for secrets.
    pub flag: String,
    pub kind: ValueKind,
    /// Applies when no channel supplies a value.
    pub default: String,
    /// Suppresses the value wherever configuration is displayed, and keeps it
    /// off the command line.
    pub secret: bool,
    /// Flag help text.
    pub usage: String,
    /// Command paths the flag is bound on, as typed (`deploy benchmarks`).
    /// Empty means a persistent flag on the root. Only the flag channel is
    /// scoped: the environment and the file reach every setting.
    pub commands: Vec<String>,
}

/// A parsed setting value.
#[derive(Clone, Debug, PartialEq)]
pub enum Parsed {
    String(String),
    Bool(bool),
    Int(i64),
    Duration(Duration),
}

impl Setting {
    /// A string setting. Chain the builder methods for the rest.
    pub fn new(key: &str, env: &str, flag: &str) -> Self {
        Self {
            key: key.into(),
            env: env.into(),
            flag: flag.into(),
            ..Default::default()
        }
    }
    pub fn kind(mut self, kind: ValueKind) -> Self {
        self.kind = kind;
        self
    }
    pub fn default_value(mut self, default: &str) -> Self {
        self.default = default.into();
        self
    }
    pub fn secret(mut self) -> Self {
        self.secret = true;
        self
    }
    pub fn usage(mut self, usage: &str) -> Self {
        self.usage = usage.into();
        self
    }
    pub fn commands(mut self, commands: &[&str]) -> Self {
        self.commands = commands.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Every way the setting can be supplied, for errors raised where the
    /// reader may know only one channel. A setting with no flag (a secret)
    /// names the two it has.
    pub fn channels(&self) -> String {
        if self.flag.is_empty() {
            format!("{} or config.{}", self.env, self.key)
        } else {
            format!("{}, config.{} or --{}", self.env, self.key, self.flag)
        }
    }

    /// Whether the setting gets a command-line flag. A secret never does,
    /// whatever its declaration says: a command line is visible in `ps` and
    /// shell history.
    pub fn bindable(&self) -> bool {
        !self.flag.is_empty() && !self.secret
    }

    /// Converts a raw string to the setting's kind, with a message naming the
    /// setting rather than the value's type.
    pub fn parse(&self, raw: &str) -> Result<Parsed, String> {
        match self.kind {
            ValueKind::Bool => match raw {
                "true" | "1" => Ok(Parsed::Bool(true)),
                "false" | "0" | "" => Ok(Parsed::Bool(false)),
                _ => Err(format!("{} must be true or false, got {raw:?}", self.key)),
            },
            // Strict: "3x" and "2.5" are rejected, so a value one channel
            // accepts cannot be rejected by another when a record replays it.
            ValueKind::Int => raw
                .parse::<i64>()
                .map(Parsed::Int)
                .map_err(|_| format!("{} must be an integer, got {raw:?}", self.key)),
            ValueKind::Duration => parse_duration(raw).map(Parsed::Duration).map_err(|_| {
                format!(
                    "{} must be a duration such as 60s or 5m, got {raw:?}",
                    self.key
                )
            }),
            ValueKind::String => Ok(Parsed::String(raw.to_string())),
        }
    }
}

/// Parses a duration such as `300ms`, `1.5h`, `2h45m` or `60s`. A bare `0` is
/// zero; any other unitless number is rejected.
pub fn parse_duration(raw: &str) -> Result<Duration, String> {
    let s = raw.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if body == "0" {
        return Ok(Duration::ZERO);
    }
    if body.is_empty() {
        return Err(format!("invalid duration {raw:?}"));
    }
    let mut total = 0f64;
    let mut rest = body;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(format!("invalid duration {raw:?}"));
        }
        let n: f64 = rest[..num_end]
            .parse()
            .map_err(|_| format!("invalid duration {raw:?}"))?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let scale = match unit {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "" => return Err(format!("missing unit in duration {raw:?}")),
            _ => return Err(format!("unknown unit {unit:?} in duration {raw:?}")),
        };
        total += n * scale;
    }
    if neg && total != 0.0 {
        // Negative durations exist only to be rejected by callers that need a
        // positive one; represent them as zero with an error-free parse would
        // hide that, so refuse here.
        return Err(format!("negative duration {raw:?}"));
    }
    Ok(Duration::from_secs_f64(total))
}

/// Formats a duration the way [`parse_duration`] reads it back, in the
/// compact `1h2m3s` / `1m0s` / `500ms` form.
pub fn format_duration(d: Duration) -> String {
    if d.is_zero() {
        return "0s".into();
    }
    let total_ns = d.as_nanos();
    if total_ns < 1_000 {
        return format!("{total_ns}ns");
    }
    if total_ns < 1_000_000 {
        return format!("{}µs", trim_float(total_ns as f64 / 1e3));
    }
    if total_ns < 1_000_000_000 {
        return format!("{}ms", trim_float(total_ns as f64 / 1e6));
    }
    let secs_total = d.as_secs();
    let h = secs_total / 3600;
    let m = (secs_total % 3600) / 60;
    let s = (secs_total % 60) as f64 + f64::from(d.subsec_nanos()) / 1e9;
    let mut out = String::new();
    if h > 0 {
        out.push_str(&format!("{h}h"));
    }
    if h > 0 || m > 0 {
        out.push_str(&format!("{m}m"));
    }
    out.push_str(&format!("{}s", trim_float(s)));
    out
}

fn trim_float(v: f64) -> String {
    let s = format!("{v:.9}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_round_trip() {
        for (raw, secs) in [
            ("60s", 60.0),
            ("5m", 300.0),
            ("1m0s", 60.0),
            ("1h30m", 5400.0),
            ("300ms", 0.3),
            ("0", 0.0),
        ] {
            assert_eq!(
                parse_duration(raw).unwrap(),
                Duration::from_secs_f64(secs),
                "{raw}"
            );
        }
        assert_eq!(format_duration(Duration::from_secs(60)), "1m0s");
        assert_eq!(format_duration(Duration::from_secs(120)), "2m0s");
        assert_eq!(format_duration(Duration::from_secs(5400)), "1h30m0s");
        assert_eq!(format_duration(Duration::from_millis(300)), "300ms");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        for bad in ["5", "x", "5x", "", "-5s"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }
}
