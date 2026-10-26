//! The runtime config each glutton-family class reads: the keys it names,
//! the defaults an unset key falls back to, and the rules a fetched value
//! must pass before the worker takes it on.

use serde::{Deserialize, Serialize};

use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{Codec, Lifecycle, Typed, WaitTime};

/// The GluttonUser slice: wait and live windows, lifecycle modes,
/// working-set sizes, CPU load and the ping cap.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct GluttonKnobs {
    /// Gap between a suspend and the user's next resume.
    #[serde(flatten)]
    pub wait: WaitTime,
    /// Resume and suspend modes.
    #[serde(flatten)]
    pub lifecycle: Lifecycle,
    /// Lower bound on how long an Actor stays resumed after its first ping;
    /// a zero window suspends right after the ping.
    #[serde(rename = "min_live_time")]
    pub min_live: Seconds,
    /// Upper bound of the live window.
    #[serde(rename = "max_live_time")]
    pub max_live: Seconds,
    /// Resident RAM filled through WriteRAM, suffixed (`2Gi`); empty
    /// disables. The glutton parses sizes, so a bad one fails as a request
    /// error rather than here.
    pub mem_target: String,
    /// RAM re-randomized in place each cycle; empty disables.
    pub mem_churn: String,
    /// RAM walked after each resume, or `all`; empty disables.
    pub mem_read: String,
    /// Threads each burning `cpu_duty_cycle` of a core through UseCPU; 0
    /// disables.
    pub cpu_cores: i64,
    /// Fraction of a core each CPU thread burns.
    pub cpu_duty_cycle: f64,
    /// Pings per resume/suspend cycle; below 1 reads as 1.
    pub max_pings_per_wake: i64,
}

fn validate_live(min: Seconds, max: Seconds) -> Result<(), String> {
    if min.is_negative() {
        return Err(format!("min_live_time cannot be negative: {min}"));
    }
    if max.is_negative() {
        return Err(format!("max_live_time cannot be negative: {max}"));
    }
    if max < min {
        return Err(format!(
            "max_live_time ({max}) cannot be less than min_live_time ({min})"
        ));
    }
    Ok(())
}

/// The GluttonUser defaults: up to 500ms between cycles, one ping a wake.
pub fn glutton_defaults() -> GluttonKnobs {
    GluttonKnobs {
        wait: WaitTime {
            max_wait: Seconds::from_millis(500),
            ..Default::default()
        },
        max_pings_per_wake: 1,
        ..Default::default()
    }
}

/// The GluttonUser codec.
pub fn glutton_codec() -> Box<dyn Codec> {
    Box::new(Typed::new(glutton_defaults()).with_validate(|k| {
        k.wait.validate()?;
        k.lifecycle.validate()?;
        validate_live(k.min_live, k.max_live)?;
        if k.cpu_cores < 0 {
            return Err(format!("cpu_cores cannot be negative: {}", k.cpu_cores));
        }
        if !(0.0..=1.0).contains(&k.cpu_duty_cycle) {
            return Err(format!(
                "cpu_duty_cycle must be between 0.0 and 1.0, got: {:.6}",
                k.cpu_duty_cycle
            ));
        }
        Ok(())
    }))
}

/// DurdirUser read mode: ship the file's bytes back.
pub const READ_MODE_DATA: &str = "data";
/// DurdirUser read mode: ship only the digest.
pub const READ_MODE_DIGEST: &str = "digest";

/// The DurdirUser slice.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurDirKnobs {
    /// Gap between cycles.
    #[serde(flatten)]
    pub wait: WaitTime,
    /// Resume and suspend modes.
    #[serde(flatten)]
    pub lifecycle: Lifecycle,
    /// File written and re-read each cycle, in bytes; 0 uses the default.
    #[serde(rename = "durdir_file_size_bytes")]
    pub file_size: i64,
    /// `data` or `digest`; empty reads as data.
    #[serde(rename = "durdir_read_mode")]
    pub read_mode: String,
    /// ActorTemplate name; empty uses the default.
    #[serde(rename = "durdir_template")]
    pub template: String,
}

/// The DurdirUser codec.
pub fn durdir_codec() -> Box<dyn Codec> {
    Box::new(Typed::new(DurDirKnobs::default()).with_validate(|k| {
        k.wait.validate()?;
        k.lifecycle.validate()?;
        if k.file_size < 0 {
            return Err(format!(
                "durdir_file_size_bytes cannot be negative: {}",
                k.file_size
            ));
        }
        if k.file_size > i32::MAX as i64 {
            return Err(format!(
                "durdir_file_size_bytes cannot exceed {} (2 GiB), got: {}",
                i32::MAX,
                k.file_size
            ));
        }
        if !k.read_mode.is_empty()
            && k.read_mode != READ_MODE_DATA
            && k.read_mode != READ_MODE_DIGEST
        {
            return Err(format!(
                "invalid durdir_read_mode {:?}: must be {READ_MODE_DATA:?} or {READ_MODE_DIGEST:?}",
                k.read_mode
            ));
        }
        Ok(())
    }))
}

/// The SpawnUser slice. Each zero keeps the matching worker flag.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnKnobs {
    /// Actors in the batch.
    pub total_actors: i64,
    /// Actors created at once.
    pub spawn_concurrency: i64,
    /// Per-Actor budget.
    pub actor_deadline: Seconds,
}

/// The SpawnUser codec.
pub fn spawn_codec() -> Box<dyn Codec> {
    Box::new(Typed::new(SpawnKnobs::default()).with_validate(|k| {
        if k.total_actors < 0 {
            return Err(format!(
                "total_actors cannot be negative: {}",
                k.total_actors
            ));
        }
        if k.spawn_concurrency < 0 {
            return Err(format!(
                "spawn_concurrency cannot be negative: {}",
                k.spawn_concurrency
            ));
        }
        if k.actor_deadline.is_negative() {
            return Err(format!(
                "actor_deadline cannot be negative: {}",
                k.actor_deadline
            ));
        }
        Ok(())
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadgen::dynconfig::{
        Holder, LIFECYCLE_MODE_ASYNC, LIFECYCLE_MODE_SYNC, RESUME_MODE_EXPLICIT,
        RESUME_MODE_IMPLICIT,
    };

    fn window() -> WaitTime {
        WaitTime {
            min_wait: Seconds::from_millis(100),
            max_wait: Seconds::from_millis(500),
        }
    }

    fn check<T: Clone + PartialEq + std::fmt::Debug + 'static>(
        name: &str,
        codec: Box<dyn Codec>,
        initial: T,
        valid: &str,
        want: T,
        invalid: &[(&str, &str)],
    ) {
        let h = Holder::new(codec);
        assert_eq!(h.get::<T>(), initial, "{name}: fresh holder");
        h.apply(valid.as_bytes())
            .unwrap_or_else(|e| panic!("{name}: apply(valid): {e}"));
        assert_eq!(h.get::<T>(), want, "{name}: decoded");
        for (case, payload) in invalid {
            assert!(
                h.apply(payload.as_bytes()).is_err(),
                "{name}/{case}: took {payload}"
            );
            assert_eq!(
                h.get::<T>(),
                want,
                "{name}/{case}: a refused payload changed the config"
            );
        }
    }

    #[test]
    fn class_codecs_glutton() {
        check(
            "glutton",
            glutton_codec(),
            glutton_defaults(),
            r#"{
                "min_wait_time": 0.1, "max_wait_time": 0.5,
                "min_live_time": 9, "max_live_time": 14,
                "resume_mode": "explicit", "lifecycle_mode": "async",
                "mem_target": "2Gi", "mem_churn": "512Mi", "mem_read": "all",
                "cpu_cores": 2, "cpu_duty_cycle": 0.1,
                "max_pings_per_wake": 3
            }"#,
            GluttonKnobs {
                wait: window(),
                lifecycle: Lifecycle {
                    resume_mode: RESUME_MODE_EXPLICIT.into(),
                    lifecycle_mode: LIFECYCLE_MODE_ASYNC.into(),
                },
                min_live: Seconds::from_secs(9),
                max_live: Seconds::from_secs(14),
                mem_target: "2Gi".into(),
                mem_churn: "512Mi".into(),
                mem_read: "all".into(),
                cpu_cores: 2,
                cpu_duty_cycle: 0.1,
                max_pings_per_wake: 3,
            },
            &[
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
                ("negative cpu cores", r#"{"cpu_cores": -1}"#),
                ("negative cpu duty cycle", r#"{"cpu_duty_cycle": -0.1}"#),
                ("cpu duty cycle above 1", r#"{"cpu_duty_cycle": 1.5}"#),
                ("negative trace (common)", r#"{"trace_probability": -0.1}"#),
                ("trace above 1 (common)", r#"{"trace_probability": 1.5}"#),
                ("wrong type for a count", r#"{"cpu_cores": "two"}"#),
            ],
        );
    }

    #[test]
    fn class_codecs_durdir() {
        check(
            "durdir",
            durdir_codec(),
            DurDirKnobs::default(),
            r#"{
                "min_wait_time": 0.1, "max_wait_time": 0.5,
                "resume_mode": "implicit", "lifecycle_mode": "sync",
                "durdir_file_size_bytes": 1048576,
                "durdir_read_mode": "digest",
                "durdir_template": "glutton-durdir-data"
            }"#,
            DurDirKnobs {
                wait: window(),
                lifecycle: Lifecycle {
                    resume_mode: RESUME_MODE_IMPLICIT.into(),
                    lifecycle_mode: LIFECYCLE_MODE_SYNC.into(),
                },
                file_size: 1048576,
                read_mode: READ_MODE_DIGEST.into(),
                template: "glutton-durdir-data".into(),
            },
            &[
                ("negative file size", r#"{"durdir_file_size_bytes": -100}"#),
                (
                    "file size over 2 GiB",
                    r#"{"durdir_file_size_bytes": 2147483648}"#,
                ),
                (
                    "invalid read mode",
                    r#"{"durdir_read_mode": "invalid_read"}"#,
                ),
                (
                    "invalid lifecycle mode",
                    r#"{"lifecycle_mode": "invalid_lifecycle"}"#,
                ),
                (
                    "max wait below min wait",
                    r#"{"min_wait_time": 2, "max_wait_time": 1}"#,
                ),
            ],
        );
    }

    #[test]
    fn class_codecs_spawn() {
        check(
            "spawn",
            spawn_codec(),
            SpawnKnobs::default(),
            r#"{"total_actors": 50, "spawn_concurrency": 5, "actor_deadline": 60}"#,
            SpawnKnobs {
                total_actors: 50,
                spawn_concurrency: 5,
                actor_deadline: Seconds::from_secs(60),
            },
            &[
                ("negative total actors", r#"{"total_actors": -1}"#),
                ("negative spawn concurrency", r#"{"spawn_concurrency": -1}"#),
                ("negative actor deadline", r#"{"actor_deadline": -1}"#),
            ],
        );
    }
}
