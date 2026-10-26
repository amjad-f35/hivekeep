//! The HkEnvUser runtime config.

use serde::{Deserialize, Serialize};

use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{Codec, Lifecycle, Typed, WaitTime};

/// The HkEnvUser slice: wait and live windows, lifecycle modes and the
/// exec cap per wake.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct HkEnvKnobs {
    /// Gap between a suspend and the user's next resume.
    #[serde(flatten)]
    pub wait: WaitTime,
    /// Resume and suspend modes.
    #[serde(flatten)]
    pub lifecycle: Lifecycle,
    /// Lower bound on how long an Actor stays resumed.
    #[serde(rename = "min_live_time")]
    pub min_live: Seconds,
    /// Upper bound of the live window.
    #[serde(rename = "max_live_time")]
    pub max_live: Seconds,
    /// Execs per wake; below 1 reads as 1.
    pub max_pings_per_wake: i64,
}

/// The defaults: up to 500ms between cycles, one exec a wake.
pub fn defaults() -> HkEnvKnobs {
    HkEnvKnobs {
        wait: WaitTime {
            max_wait: Seconds::from_millis(500),
            ..Default::default()
        },
        max_pings_per_wake: 1,
        ..Default::default()
    }
}

/// The HkEnvUser codec.
pub fn codec() -> Box<dyn Codec> {
    Box::new(Typed::new(defaults()).with_validate(|k| {
        k.wait.validate()?;
        k.lifecycle.validate()?;
        if k.min_live.is_negative() {
            return Err(format!("min_live_time cannot be negative: {}", k.min_live));
        }
        if k.max_live.is_negative() {
            return Err(format!("max_live_time cannot be negative: {}", k.max_live));
        }
        if k.max_live < k.min_live {
            return Err(format!(
                "max_live_time ({}) cannot be less than min_live_time ({})",
                k.max_live, k.min_live
            ));
        }
        Ok(())
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadgen::dynconfig::{Holder, LIFECYCLE_MODE_ASYNC, RESUME_MODE_EXPLICIT};

    #[test]
    fn hk_env_codec() {
        let h = Holder::new(codec());
        assert_eq!(h.get::<HkEnvKnobs>(), defaults());
        h.apply(
            br#"{
                "min_wait_time": 0.2, "max_wait_time": 1.0,
                "min_live_time": 9, "max_live_time": 14,
                "resume_mode": "explicit", "lifecycle_mode": "async",
                "max_pings_per_wake": 2
            }"#,
        )
        .unwrap();
        let want = HkEnvKnobs {
            wait: WaitTime {
                min_wait: Seconds::from_millis(200),
                max_wait: Seconds::from_secs(1),
            },
            lifecycle: Lifecycle {
                resume_mode: RESUME_MODE_EXPLICIT.into(),
                lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
            },
            min_live: Seconds::from_secs(9),
            max_live: Seconds::from_secs(14),
            max_pings_per_wake: 2,
        };
        assert_eq!(h.get::<HkEnvKnobs>(), want);
        for (name, payload) in [
            ("negative min wait", r#"{"min_wait_time": -1}"#),
            ("negative max wait", r#"{"max_wait_time": -1}"#),
            (
                "max wait below min wait",
                r#"{"min_wait_time": 2, "max_wait_time": 1}"#,
            ),
            ("negative min live", r#"{"min_live_time": -1}"#),
            ("negative max live", r#"{"max_live_time": -1}"#),
            (
                "max live below min live",
                r#"{"min_live_time": 14, "max_live_time": 9}"#,
            ),
            ("invalid resume mode", r#"{"resume_mode": "invalid_mode"}"#),
            (
                "invalid lifecycle mode",
                r#"{"lifecycle_mode": "invalid_lifecycle"}"#,
            ),
        ] {
            assert!(h.apply(payload.as_bytes()).is_err(), "{name}");
            assert_eq!(
                h.get::<HkEnvKnobs>(),
                want,
                "{name}: a refused payload changed the config"
            );
        }
    }
}
