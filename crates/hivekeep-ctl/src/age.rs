//! Resource ages, rendered compactly (`5m`, `3h`, `2d`).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Renders a duration with the precision that matters at its size: seconds
/// up to two minutes, then minutes, hours, days and years.
pub fn human_duration(seconds: i64) -> String {
    // Allow a little clock skew: anything within two seconds is "now".
    if seconds < -1 {
        return "<invalid>".into();
    }
    if seconds < 0 {
        return "0s".into();
    }
    if seconds < 60 * 2 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 10 {
        let s = seconds % 60;
        return if s == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m{s}s")
        };
    }
    if minutes < 60 * 3 {
        return format!("{minutes}m");
    }
    let hours = seconds / 3600;
    if hours < 8 {
        let m = minutes % 60;
        return if m == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h{m}m")
        };
    }
    if hours < 48 {
        return format!("{hours}h");
    }
    if hours < 24 * 8 {
        let h = hours % 24;
        return if h == 0 {
            format!("{}d", hours / 24)
        } else {
            format!("{}d{h}h", hours / 24)
        };
    }
    if hours < 24 * 365 * 2 {
        return format!("{}d", hours / 24);
    }
    if hours < 24 * 365 * 8 {
        let dy = (hours / 24) % 365;
        return if dy == 0 {
            format!("{}y", hours / 24 / 365)
        } else {
            format!("{}y{dy}d", hours / 24 / 365)
        };
    }
    format!("{}y", hours / 24 / 365)
}

/// Seconds since the Unix epoch, signed.
fn unix_seconds(t: SystemTime) -> f64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}

/// The age at `now` of a resource created at `created`. A missing timestamp
/// counts as the Unix epoch.
pub fn format_age(created: Option<&prost_types::Timestamp>, now: SystemTime) -> String {
    let created_secs = created
        .map(|t| t.seconds as f64 + f64::from(t.nanos) / 1e9)
        .unwrap_or(0.0);
    let elapsed = unix_seconds(now) - created_secs;
    human_duration(elapsed.trunc() as i64)
}

/// A timestamp `ago` before `now`, for tests and fixtures.
pub fn timestamp_before(now: SystemTime, ago: Duration) -> prost_types::Timestamp {
    prost_types::Timestamp::from(now - ago)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        for (secs, want) in [
            (-5, "<invalid>"),
            (-1, "0s"),
            (30, "30s"),
            (119, "119s"),
            (150, "2m30s"),
            (300, "5m"),
            (45 * 60, "45m"),
            (5 * 3600, "5h"),
            (5 * 3600 + 120, "5h2m"),
            (20 * 3600, "20h"),
            (72 * 3600, "3d"),
            (75 * 3600, "3d3h"),
            (30 * 86400, "30d"),
            (3 * 365 * 86400, "3y"),
            (3 * 365 * 86400 + 86400, "3y1d"),
            (20 * 365 * 86400, "20y"),
        ] {
            assert_eq!(human_duration(secs), want, "{secs}s");
        }
    }

    #[test]
    fn ages() {
        let now = UNIX_EPOCH + Duration::from_secs(1_767_268_800);
        for (ago, want) in [
            (Duration::from_secs(5 * 60), "5m"),
            (Duration::from_secs(5 * 3600), "5h"),
            (Duration::from_secs(72 * 3600), "3d"),
        ] {
            let ts = timestamp_before(now, ago);
            assert_eq!(format_age(Some(&ts), now), want);
        }
        assert_eq!(format_age(None, now), "56y");
    }
}
