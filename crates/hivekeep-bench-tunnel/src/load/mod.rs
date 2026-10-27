//! The benchmark's load generator: an open-loop pacer, the traffic modes
//! that exercise each tunnel path, the scenarios built from them, and the
//! ladder that runs a scenario at rising rates while reading the tunnel's
//! own CPU and memory from its admin endpoint.

pub mod egress_actor;
pub mod ladder;
pub mod modes;
pub mod pacer;
pub mod scenarios;
pub mod stats;

pub use ladder::Runner;
pub use modes::{Mode, Targets};
pub use scenarios::{names, select, Scenario, SCENARIOS};
