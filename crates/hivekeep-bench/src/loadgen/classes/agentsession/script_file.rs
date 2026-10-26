//! The YAML form of a script.
//!
//! ```yaml
//! name: coding-session
//! min_actor_memory: 1Gi
//! steps:
//!   - name: 01_read_task
//!     agent: Boots, reads the task prompt, loads its context window
//!     think: 2s
//!     ops:
//!       - fill_ram: {key: agent_context, size: 32Mi}
//!       - ping: {}
//! ```
//!
//! Each op is a one-key map from the op kind to that kind's arguments.
//! Sizes are Kubernetes quantities (`16Mi`, `64Ki`); think times are
//! durations (`2s`, `1.5s`). Unknown kinds, unknown fields and arguments a
//! kind does not take are errors.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use super::quantity::{format_size, parse_duration, parse_size};
use super::script::{Op, OpKind, Script, Step};
use super::validate::validate;

/// Why a script could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptError(pub String);

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ScriptError {}

fn err(msg: impl Into<String>) -> ScriptError {
    ScriptError(msg.into())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ScriptDoc {
    name: String,
    min_actor_memory: String,
    steps: Vec<StepDoc>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StepDoc {
    name: String,
    agent: String,
    think: String,
    ops: Vec<BTreeMap<String, OpArgs>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OpArgs {
    key: String,
    size: String,
    millis: i64,
    parallel: i32,
}

/// Which arguments an op kind takes.
#[derive(Debug, Clone, Copy)]
struct OpSpec {
    kind: OpKind,
    key: bool,
    size: bool,
    millis: bool,
    /// Takes `parallel` (implies millis).
    parallel: bool,
}

const fn spec(kind: OpKind, key: bool, size: bool, millis: bool, parallel: bool) -> OpSpec {
    OpSpec {
        kind,
        key,
        size,
        millis,
        parallel,
    }
}

/// Every op kind by its script name.
const OP_SPECS: &[(&str, OpSpec)] = &[
    ("ingest", spec(OpKind::Ingest, true, true, false, false)),
    ("burn_cpu", spec(OpKind::BurnCpu, false, false, true, true)),
    (
        "write_disk",
        spec(OpKind::WriteDisk, true, true, false, false),
    ),
    (
        "read_disk_digest",
        spec(OpKind::ReadDiskDigest, true, false, false, false),
    ),
    (
        "read_disk_data",
        spec(OpKind::ReadDiskData, true, false, false, false),
    ),
    ("fill_ram", spec(OpKind::FillRam, true, true, false, false)),
    (
        "churn_ram",
        spec(OpKind::ChurnRam, true, true, false, false),
    ),
    ("walk_ram", spec(OpKind::WalkRam, true, false, false, false)),
    ("ping", spec(OpKind::Ping, false, false, false, false)),
    ("dwell", spec(OpKind::Dwell, false, false, true, false)),
];

fn spec_named(name: &str) -> Option<OpSpec> {
    OP_SPECS.iter().find(|(n, _)| *n == name).map(|(_, s)| *s)
}

/// The script name of an op kind.
pub fn op_name(kind: OpKind) -> &'static str {
    OP_SPECS
        .iter()
        .find(|(_, s)| s.kind == kind)
        .map(|(n, _)| *n)
        .unwrap_or("unknown")
}

/// Keys name files under the Actor's data dir, so nothing that could
/// escape it is accepted (the same rule the glutton applies).
pub(super) static KEY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[a-zA-Z0-9_-]+$").unwrap());

/// Script names double as the `agentsession_script` knob value and the
/// built-in file name.
pub(super) static SCRIPT_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[a-z0-9][a-z0-9-]*$").unwrap());

/// Parses a YAML script and validates it.
pub fn decode(data: &[u8]) -> Result<Script, ScriptError> {
    let doc: ScriptDoc =
        serde_yaml::from_slice(data).map_err(|e| err(format!("parse script: {e}")))?;
    let mut s = Script {
        name: doc.name,
        min_actor_memory: 0,
        steps: Vec::new(),
    };
    if !doc.min_actor_memory.is_empty() {
        s.min_actor_memory =
            parse_size(&doc.min_actor_memory).map_err(|e| err(format!("min_actor_memory: {e}")))?;
    }
    for (i, sd) in doc.steps.into_iter().enumerate() {
        let name = sd.name.clone();
        let step = decode_step(sd).map_err(|e| err(format!("step {} ({name:?}): {e}", i + 1)))?;
        s.steps.push(step);
    }
    validate(&s)?;
    Ok(s)
}

fn decode_step(sd: StepDoc) -> Result<Step, ScriptError> {
    let mut step = Step {
        name: sd.name,
        agent: sd.agent,
        think: Default::default(),
        ops: Vec::new(),
    };
    if !sd.think.is_empty() {
        step.think = parse_duration(&sd.think).map_err(|e| err(format!("think: {e}")))?;
    }
    for (i, od) in sd.ops.into_iter().enumerate() {
        step.ops
            .push(decode_op(od).map_err(|e| err(format!("op {}: {e}", i + 1)))?);
    }
    Ok(step)
}

fn decode_op(od: BTreeMap<String, OpArgs>) -> Result<Op, ScriptError> {
    if od.len() != 1 {
        return Err(err(format!(
            "an op is a single-key map, got {} keys",
            od.len()
        )));
    }
    let (name, args) = od.into_iter().next().expect("one entry");
    let spec = spec_named(&name).ok_or_else(|| err(format!("unknown op kind {name:?}")))?;
    let mut o = Op::of(spec.kind);
    match (spec.key, args.key.is_empty()) {
        (true, true) => return Err(err(format!("{name}: key is required"))),
        (false, false) => return Err(err(format!("{name}: takes no key"))),
        _ => {}
    }
    if !args.key.is_empty() {
        if !KEY_RE.is_match(&args.key) {
            return Err(err(format!(
                "{name}: key {:?} must match {}",
                args.key,
                KEY_RE.as_str()
            )));
        }
        o.key = args.key;
    }
    match (spec.size, args.size.is_empty()) {
        (true, true) => return Err(err(format!("{name}: size is required"))),
        (false, false) => return Err(err(format!("{name}: takes no size"))),
        _ => {}
    }
    if !args.size.is_empty() {
        o.bytes = parse_size(&args.size).map_err(|e| err(format!("{name}: size: {e}")))?;
    }
    if spec.millis {
        if args.millis <= 0 {
            return Err(err(format!("{name}: millis must be positive")));
        }
        o.millis = args.millis;
    } else if args.millis != 0 {
        return Err(err(format!("{name}: takes no millis")));
    }
    if spec.parallel {
        if args.parallel < 0 {
            return Err(err(format!("{name}: parallel cannot be negative")));
        }
        o.parallel = args.parallel.max(1);
    } else if args.parallel != 0 {
        return Err(err(format!("{name}: takes no parallel")));
    }
    Ok(o)
}

/// A YAML scalar for `v`, quoted when plain text would not read back.
fn scalar(v: &str) -> String {
    serde_yaml::to_string(v)
        .map(|s| s.trim_end().to_string())
        .unwrap_or_else(|_| format!("{v:?}"))
}

/// Renders a script as YAML in the form [`decode`] reads, one flow-style
/// line per op so the file reads like the step table it is.
pub fn encode(s: &Script) -> String {
    let mut out = String::new();
    out.push_str(&format!("name: {}\n", scalar(&s.name)));
    out.push_str(&format!(
        "min_actor_memory: {}\n",
        format_size(s.min_actor_memory)
    ));
    out.push_str("steps:\n");
    for st in &s.steps {
        out.push_str(&format!("  - name: {}\n", scalar(&st.name)));
        out.push_str(&format!("    agent: {}\n", scalar(&st.agent)));
        out.push_str(&format!("    think: {}\n", st.think));
        out.push_str("    ops:\n");
        for o in &st.ops {
            out.push_str(&format!("      - {}\n", encode_op(o)));
        }
    }
    out
}

fn encode_op(o: &Op) -> String {
    let name = op_name(o.kind);
    let spec = spec_named(name).expect("every kind has a spec");
    let mut args = Vec::new();
    if spec.key {
        args.push(format!("key: {}", scalar(&o.key)));
    }
    if spec.size {
        args.push(format!("size: {}", format_size(o.bytes)));
    }
    if spec.millis {
        args.push(format!("millis: {}", o.millis));
    }
    if spec.parallel {
        args.push(format!("parallel: {}", o.parallel));
    }
    format!("{name}: {{{}}}", args.join(", "))
}
