//! The `AgentSessionUser` load class: each virtual user is one agent
//! session replaying a scripted step sequence (a YAML file under
//! `scripts/`, selected by `agentsession_script`) against a glutton Actor.
//! After every step the Actor is suspended for the step's think gap; by
//! default the next step's first request wakes it through the gate router,
//! so the benchmark measures the wake latency a user of an oversubscribed
//! fleet sees.
//!
//! Stats rows: `Step_<name>` per step; `WakeFirstTouch` for the first
//! request after each suspend (in implicit resume mode, the held-request
//! wake latency itself); `TimeToWake` for the whole wake, from the first
//! resume attempt to that first request, including time spent retrying
//! while the fleet had no room. `agentsession_phase_in` staggers session
//! starts so a fleet spawned in one ramp does not reach every step in a
//! single wave.

pub mod errors;
pub mod knobs;
pub mod quantity;
pub mod runtime;
pub mod script;
pub mod script_file;
pub mod scripts;
pub mod session;
pub mod validate;

use std::sync::Arc;

pub use quantity::format_size;
pub use script::{Op, OpKind, Script, Step};
pub use script_file::{decode, encode, ScriptError};
pub use scripts::{load, load_file, names, DEFAULT_SCRIPT};
pub use validate::{budgets, validate, Budgets};

use crate::loadgen::userclass::Entry;

/// The locust user class name.
pub const AGENT_SESSION_USER_CLASS: &str = "AgentSessionUser";

/// The class's registry entry.
pub fn entries() -> Vec<Entry> {
    vec![Entry {
        name: "agentsession",
        locust_file: "agentsession.py",
        user_class: AGENT_SESSION_USER_CLASS,
        config: Some(knobs::codec),
        init: |cfg| Arc::new(runtime::Runtime::new(cfg)),
    }]
}

#[cfg(test)]
mod tests;
