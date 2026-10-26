//! The SweperfUser slice of the runtime config. Each zero falls back to a
//! built-in default; the master fills the keys from its sweperf flags and
//! the shared lifecycle-mode flag.

use serde::{Deserialize, Serialize};

use crate::loadgen::dynconfig::{
    Lifecycle, Typed, WaitTime, LIFECYCLE_MODE_NONE, RESUME_MODE_IMPLICIT,
};

/// What a sweperf user reads per task and per cycle.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Knobs {
    /// Think time between cycles.
    #[serde(flatten)]
    pub wait: WaitTime,
    /// How each cycle ends: `async` (or empty) suspends asynchronously,
    /// `sync` synchronously. `none` and an implicit resume are refused:
    /// every cycle ends in a suspend and resumes with ResumeActor.
    #[serde(flatten)]
    pub lifecycle: Lifecycle,
    /// The workload template.
    #[serde(rename = "sweperf_template")]
    pub template: String,
    /// Trajectory steps per task.
    #[serde(rename = "sweperf_total_steps")]
    pub total_steps: i64,
    /// Cycles the steps are split into.
    #[serde(rename = "sweperf_num_cycles")]
    pub num_cycles: i64,
    /// `/status` poll interval in milliseconds.
    #[serde(rename = "sweperf_poll_interval_ms")]
    pub poll_interval_ms: i64,
}

fn validate(k: &Knobs) -> Result<(), String> {
    k.wait.validate()?;
    if k.total_steps < 0 {
        return Err(format!(
            "sweperf_total_steps cannot be negative: {}",
            k.total_steps
        ));
    }
    if k.num_cycles < 0 {
        return Err(format!(
            "sweperf_num_cycles cannot be negative: {}",
            k.num_cycles
        ));
    }
    if k.poll_interval_ms < 0 {
        return Err(format!(
            "sweperf_poll_interval_ms cannot be negative: {}",
            k.poll_interval_ms
        ));
    }
    k.lifecycle.validate()?;
    if k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_NONE {
        return Err(format!(
            "sweperf does not support lifecycle_mode {:?}: every cycle ends in a suspend",
            k.lifecycle.lifecycle_mode
        ));
    }
    if k.lifecycle.resume_mode == RESUME_MODE_IMPLICIT {
        return Err(format!(
            "sweperf does not support resume_mode {:?}: it resumes with ResumeActor",
            k.lifecycle.resume_mode
        ));
    }
    Ok(())
}

/// The class's codec.
pub fn codec() -> Typed<Knobs> {
    Typed::new(Knobs::default()).with_validate(validate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadgen::duration::Seconds;
    use crate::loadgen::dynconfig::{Holder, LIFECYCLE_MODE_ASYNC, RESUME_MODE_EXPLICIT};

    /// The codec takes a payload naming every key the class reads and
    /// refuses the values it cannot run on.
    #[test]
    fn knobs_codec() {
        let h = Holder::new(Box::new(codec()));
        assert_eq!(h.get::<Knobs>(), Knobs::default());
        h.apply(
            br#"{
            "min_wait_time": 0.1, "max_wait_time": 0.5,
            "sweperf_template": "sweperf-astropy-7336",
            "sweperf_total_steps": 21,
            "sweperf_num_cycles": 4,
            "sweperf_poll_interval_ms": 100,
            "lifecycle_mode": "async",
            "resume_mode": "explicit"
        }"#,
        )
        .unwrap();
        let want = Knobs {
            wait: WaitTime {
                min_wait: Seconds::from_millis(100),
                max_wait: Seconds::from_millis(500),
            },
            template: "sweperf-astropy-7336".into(),
            total_steps: 21,
            num_cycles: 4,
            poll_interval_ms: 100,
            lifecycle: Lifecycle {
                lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
                resume_mode: RESUME_MODE_EXPLICIT.into(),
            },
        };
        assert_eq!(h.get::<Knobs>(), want);
        for (name, payload) in [
            ("negative total steps", r#"{"sweperf_total_steps": -1}"#),
            ("negative num cycles", r#"{"sweperf_num_cycles": -1}"#),
            (
                "negative poll interval",
                r#"{"sweperf_poll_interval_ms": -1}"#,
            ),
            (
                "max wait below min wait",
                r#"{"min_wait_time": 2, "max_wait_time": 1}"#,
            ),
            ("lifecycle mode none", r#"{"lifecycle_mode": "none"}"#),
            (
                "unknown lifecycle mode",
                r#"{"lifecycle_mode": "hibernate"}"#,
            ),
            ("implicit resume mode", r#"{"resume_mode": "implicit"}"#),
        ] {
            assert!(
                h.apply(payload.as_bytes()).is_err(),
                "{name}: took {payload}"
            );
            assert_eq!(
                h.get::<Knobs>(),
                want,
                "{name}: a refused payload changed the config"
            );
        }
    }
}
