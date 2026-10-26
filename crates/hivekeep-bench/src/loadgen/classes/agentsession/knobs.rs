//! The agent-session slice of the runtime config, filled by the master
//! from its `--agentsession-*` flags.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::loadgen::duration::Seconds;
use crate::loadgen::dynconfig::{Codec, Lifecycle, Typed, LIFECYCLE_MODE_NONE};

/// How long a step keeps retrying a wake the fleet had no room for, unless
/// configured otherwise.
pub const DEFAULT_RESUME_RETRY_BUDGET: Duration = Duration::from_secs(60);

/// The class's knobs. [`Default`] is the all-zero value tests build on; the
/// codec's defaults add the retry budget.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Knobs {
    /// Resume and lifecycle modes.
    #[serde(flatten)]
    pub lifecycle: Lifecycle,
    /// Built-in script variant; empty falls back to the default.
    #[serde(rename = "agentsession_script")]
    pub script: String,
    /// A script file on the worker; wins over `script` when set.
    #[serde(rename = "agentsession_script_file")]
    pub script_file: String,
    /// Multiplies the per-step think times; 0 reads as 1.
    #[serde(rename = "agentsession_think_scale")]
    pub think_scale: f64,
    /// Comma-separated 1-based step numbers after which the Actor is
    /// suspended; empty suspends after every step.
    #[serde(rename = "agentsession_snapshot_steps")]
    pub snapshot_steps: String,
    /// Spreads session starts: each user waits a uniform share of it before
    /// creating its Actor, so the fleet does not move through the script in
    /// waves. Zero starts every session at once.
    #[serde(rename = "agentsession_phase_in")]
    pub phase_in: Seconds,
    /// How long a step keeps retrying a wake the fleet had no room for
    /// before giving the step up; zero gives the wake one attempt.
    #[serde(rename = "agentsession_resume_retry_budget")]
    pub resume_retry_budget: Seconds,
}

/// The codec's defaults.
pub fn defaults() -> Knobs {
    Knobs {
        resume_retry_budget: DEFAULT_RESUME_RETRY_BUDGET.into(),
        ..Default::default()
    }
}

fn validate(k: &Knobs) -> Result<(), String> {
    k.lifecycle.validate()?;
    if k.think_scale < 0.0 {
        return Err(format!(
            "agentsession_think_scale cannot be negative: {:.6}",
            k.think_scale
        ));
    }
    if k.phase_in.is_negative() {
        return Err(format!(
            "agentsession_phase_in cannot be negative: {}",
            k.phase_in
        ));
    }
    if k.resume_retry_budget.is_negative() {
        return Err(format!(
            "agentsession_resume_retry_budget cannot be negative: {}",
            k.resume_retry_budget
        ));
    }
    let steps = parse_snapshot_steps(&k.snapshot_steps)?;
    if !steps.is_empty() && k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_NONE {
        return Err(format!(
            "agentsession_snapshot_steps cannot be used with lifecycle_mode {LIFECYCLE_MODE_NONE:?}"
        ));
    }
    Ok(())
}

/// The typed codec.
pub fn typed_codec() -> Typed<Knobs> {
    Typed::new(defaults()).with_validate(validate)
}

/// The codec the worker builds the holder from.
pub fn codec() -> Box<dyn Codec> {
    Box::new(typed_codec())
}

/// Parses a comma-separated list of 1-based step numbers (`3,10,20`). An
/// empty string is the empty set: suspend after every step.
pub fn parse_snapshot_steps(raw: &str) -> Result<BTreeSet<usize>, String> {
    let mut steps = BTreeSet::new();
    if raw.trim().is_empty() {
        return Ok(steps);
    }
    for entry in raw.split(',') {
        match entry.trim().parse::<i64>() {
            Ok(n) if n > 0 => {
                steps.insert(n as usize);
            }
            _ => {
                return Err(format!(
                    "invalid agentsession_snapshot_steps entry {entry:?}: must be a positive 1-based step number"
                ))
            }
        }
    }
    Ok(steps)
}
