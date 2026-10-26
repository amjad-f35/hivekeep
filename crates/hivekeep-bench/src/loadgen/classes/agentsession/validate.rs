//! A script's invariants and its declared footprint.

use std::collections::{HashMap, HashSet};

use super::quantity::format_size;
use super::script::{OpKind, Script, Step};
use super::script_file::{op_name, ScriptError, SCRIPT_NAME_RE};

/// What a script declares it holds in the sandbox: resident RAM (the
/// largest fill per array) and files in the data dir (the largest object
/// per key). The data dir is tmpfs, so both come out of the Actor's memory
/// limit. The guest's real peak (kernel, guest agent, allocator
/// transients) sits on top of this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Budgets {
    /// Declared resident RAM, bytes.
    pub ram: i64,
    /// Declared files, bytes.
    pub disk: i64,
}

/// Sums the bytes a step sequence declares.
pub fn budgets(steps: &[Step]) -> Budgets {
    let mut ram: HashMap<&str, i64> = HashMap::new();
    let mut disk: HashMap<&str, i64> = HashMap::new();
    for s in steps {
        for o in &s.ops {
            let m = match o.kind {
                OpKind::FillRam => &mut ram,
                OpKind::Ingest | OpKind::WriteDisk => &mut disk,
                _ => continue,
            };
            let e = m.entry(o.key.as_str()).or_default();
            *e = (*e).max(o.bytes);
        }
    }
    Budgets {
        ram: ram.values().sum(),
        disk: disk.values().sum(),
    }
}

/// Checks a script: a usable name, a memory floor covering what it
/// declares, unique non-empty steps with positive think times, and no op
/// that uses a sandbox object before an earlier op created it (which would
/// otherwise fail at run time with NOT_FOUND from the glutton).
pub fn validate(s: &Script) -> Result<(), ScriptError> {
    let e = |m: String| Err(ScriptError(m));
    if !SCRIPT_NAME_RE.is_match(&s.name) {
        return e(format!(
            "script name {:?} must match {}",
            s.name,
            SCRIPT_NAME_RE.as_str()
        ));
    }
    if s.steps.is_empty() {
        return e(format!("script {:?} has no steps", s.name));
    }
    let mut seen = HashSet::new();
    let mut ram_filled = HashSet::new();
    let mut disk_written = HashSet::new();
    for (i, st) in s.steps.iter().enumerate() {
        let at = format!("step {} ({:?})", i + 1, st.name);
        if st.name.is_empty() || st.agent.is_empty() {
            return e(format!("{at}: name and agent must be set"));
        }
        if !seen.insert(st.name.as_str()) {
            return e(format!("{at}: duplicate step name"));
        }
        if st.think.as_nanos() <= 0 {
            return e(format!("{at}: think time must be positive"));
        }
        if st.ops.is_empty() {
            return e(format!("{at}: has no ops"));
        }
        for (j, o) in st.ops.iter().enumerate() {
            match o.kind {
                OpKind::FillRam => {
                    ram_filled.insert(o.key.as_str());
                }
                OpKind::ChurnRam | OpKind::WalkRam if !ram_filled.contains(o.key.as_str()) => {
                    return e(format!(
                        "{at} op {}: {} of RAM array {:?} before any fill_ram",
                        j + 1,
                        op_name(o.kind),
                        o.key
                    ));
                }
                OpKind::Ingest | OpKind::WriteDisk => {
                    disk_written.insert(o.key.as_str());
                }
                OpKind::ReadDiskDigest | OpKind::ReadDiskData
                    if !disk_written.contains(o.key.as_str()) =>
                {
                    return e(format!(
                        "{at} op {}: {} of {:?} before any ingest or write_disk",
                        j + 1,
                        op_name(o.kind),
                        o.key
                    ));
                }
                _ => {}
            }
        }
    }
    let b = budgets(&s.steps);
    if s.min_actor_memory <= 0 {
        return e(format!(
            "script {:?}: min_actor_memory is required (declared RAM {} + disk {})",
            s.name,
            format_size(b.ram),
            format_size(b.disk)
        ));
    }
    if s.min_actor_memory < b.ram + b.disk {
        return e(format!(
            "script {:?}: min_actor_memory {} is below the declared RAM {} + disk {}",
            s.name,
            format_size(s.min_actor_memory),
            format_size(b.ram),
            format_size(b.disk)
        ));
    }
    Ok(())
}
