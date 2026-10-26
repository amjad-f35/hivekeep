//! The types a decoded agent-session script becomes.
//!
//! A script is the whole benchmark spelled out: one agent session as a
//! sequence of steps. Each step says in plain words what the agent is doing
//! and lists the resource operations the real action costs the sandbox, so
//! the workload is read and changed in the YAML under `scripts/`, not in the
//! driver (see [`super::script_file`] for the format).
//!
//! Between steps the agent waits for its model: the driver suspends the
//! Actor, sleeps the step's think time, and lets the next step's first
//! request wake it through the gate router. That idle gap is what the
//! benchmark is about, so think times are script data.

use crate::loadgen::duration::Seconds;

/// The kind of one resource operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum OpKind {
    /// Pushes bytes from the driver through the router into the Actor,
    /// which writes them to disk: network ingress plus a disk write, the
    /// shape of a download.
    Ingest,
    /// Spins the sandbox's CPU for a wall-clock duration.
    BurnCpu,
    /// Writes locally generated random bytes to a sandbox file.
    WriteDisk,
    /// Reads and hashes a sandbox file without returning its bytes.
    ReadDiskDigest,
    /// Reads a sandbox file and returns its bytes: disk read plus network
    /// egress through the router's response.
    ReadDiskData,
    /// Allocates a resident RAM array of random bytes.
    FillRam,
    /// Re-randomizes part of an existing RAM array in place, so the next
    /// snapshot has fresh pages.
    ChurnRam,
    /// Touches one byte per page of a RAM array; after a resume this
    /// measures the cost of paging the array back in.
    WalkRam,
    /// A minimal round trip through the router.
    #[default]
    Ping,
    /// Keeps the Actor awake and idle for a wall-clock duration without any
    /// request: the agent waits on something outside the sandbox (a model
    /// response, a user typing) and stays resident meanwhile.
    Dwell,
}

/// One resource effect inside a step, executed as a single glutton request
/// through the router (a dwell sends nothing).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Op {
    /// What the op does.
    pub kind: OpKind,
    /// File or RAM-array name inside the sandbox.
    pub key: String,
    /// Payload, file or RAM size in bytes.
    pub bytes: i64,
    /// CPU-burn or dwell wall-clock milliseconds.
    pub millis: i64,
    /// CPU-burn threads.
    pub parallel: i32,
}

impl Op {
    /// An op of `kind` with no arguments.
    pub fn of(kind: OpKind) -> Op {
        Op {
            kind,
            ..Default::default()
        }
    }

    /// The wake probe sent ahead of every step.
    pub fn ping() -> Op {
        Op::of(OpKind::Ping)
    }
}

/// One agent action: what the agent is doing, the think time before it,
/// and the resource operations acting it out.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    /// Keys the step's stats row: `Step_<name>`.
    pub name: String,
    /// What the agent is doing, for people reading the script.
    pub agent: String,
    /// How long the model thinks before this step; the Actor is suspended
    /// for this gap (scaled by `--agentsession-think-scale`).
    pub think: Seconds,
    /// The resource effects, in order.
    pub ops: Vec<Op>,
}

/// A loaded script: a named step sequence and the Actor memory it needs.
#[derive(Debug, Clone, PartialEq)]
pub struct Script {
    /// The script's name.
    pub name: String,
    /// The smallest Actor memory limit the script is known to run under,
    /// in bytes. Required, and at least the declared RAM plus disk (the data
    /// dir is tmpfs). The driver refuses a smaller template.
    pub min_actor_memory: i64,
    /// The steps, in order.
    pub steps: Vec<Step>,
}
