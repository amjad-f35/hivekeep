//! Per-phase timing of checkpoints and restores, logged as one record per
//! operation so benchmarking tooling can join it with the keeper's own.

use std::time::{Duration, Instant};

use hivekeep_core::attr;
use hivekeep_core::resource::ActorAttribution;
use serde_json::{Map, Value};

/// Key prefix of restore phase durations.
pub const RESTORE_DURATION_KEY: &str = "worker.actor.restore.duration";
/// Key prefix of checkpoint phase durations.
pub const CHECKPOINT_DURATION_KEY: &str = "worker.actor.checkpoint.duration";
/// The number of application containers on a restore record; the `app_*`
/// phases are sums over them.
pub const CONTAINER_COUNT_KEY: &str = "hk.actor.container.count";

/// Phase names, suffixed onto the duration keys.
pub mod phase {
    /// Validation, locking and clearing state.
    pub const PREP: &str = "prep";
    /// Preparing the egress tunnel.
    pub const EGRESS_PREPARE: &str = "egress_prepare";
    /// Setting up the sandbox network.
    pub const NET_SETUP: &str = "net_setup";
    /// `runsc create` of the sandbox.
    pub const SANDBOX_CREATE: &str = "sandbox_create";
    /// Starting or restoring the sandbox.
    pub const SANDBOX_RESTORE: &str = "sandbox_restore";
    /// Composing the application rootfs.
    pub const APP_ROOTFS: &str = "app_rootfs";
    /// `runsc create` of the applications.
    pub const APP_CREATE: &str = "app_create";
    /// Starting or restoring the applications.
    pub const APP_RESTORE: &str = "app_restore";
    /// Waiting for the wakeup probes.
    pub const WAKEUP_PROBE: &str = "wakeup_probe";
    /// Activating the tunnel, egress and ingress.
    pub const ACTIVATE: &str = "activate";
    /// `runsc checkpoint`.
    pub const CHECKPOINT: &str = "checkpoint";
    /// `runsc fscheckpoint`.
    pub const FS_CHECKPOINT: &str = "fs_checkpoint";
    /// Tearing the workload down.
    pub const TEARDOWN: &str = "teardown";
    /// The whole operation, named as the keeper names its own.
    pub const TOTAL: &str = hivekeep_core::attr::snapshot_phase::TOTAL;
}

/// One timed phase. A zero duration never ran and is left off the record.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Phase {
    /// Suffix of the duration key.
    pub name: &'static str,
    /// How long it took.
    pub d: Duration,
}

/// Elapsed time of each RestoreWorkload phase. Under VOLUMES and ROOTFS the
/// `sandbox_restore`/`app_restore` phases time the cold start standing in for
/// a restore. `activate` covers both egress and ingress activation.
#[derive(Debug, Default, Clone, Copy)]
#[allow(missing_docs)]
pub struct RestoreTiming {
    pub prep: Duration,
    pub egress_prepare: Duration,
    pub net_setup: Duration,
    pub sandbox_create: Duration,
    pub sandbox_restore: Duration,
    pub app_rootfs: Duration,
    pub app_create: Duration,
    pub app_restore: Duration,
    pub wakeup_probe: Duration,
    pub activate: Duration,
    pub total: Duration,
}

impl RestoreTiming {
    /// The phases in order.
    pub fn phases(&self) -> Vec<Phase> {
        let p = |name, d| Phase { name, d };
        vec![
            p(phase::PREP, self.prep),
            p(phase::EGRESS_PREPARE, self.egress_prepare),
            p(phase::NET_SETUP, self.net_setup),
            p(phase::SANDBOX_CREATE, self.sandbox_create),
            p(phase::SANDBOX_RESTORE, self.sandbox_restore),
            p(phase::APP_ROOTFS, self.app_rootfs),
            p(phase::APP_CREATE, self.app_create),
            p(phase::APP_RESTORE, self.app_restore),
            p(phase::WAKEUP_PROBE, self.wakeup_probe),
            p(phase::ACTIVATE, self.activate),
            p(phase::TOTAL, self.total),
        ]
    }
}

/// Elapsed time of each CheckpointWorkload phase. MEMORY runs `runsc
/// checkpoint`; VOLUMES and ROOTFS both run `runsc fscheckpoint`, told apart
/// by the fidelity on the record.
#[derive(Debug, Default, Clone, Copy)]
#[allow(missing_docs)]
pub struct CheckpointTiming {
    pub prep: Duration,
    pub checkpoint: Duration,
    pub fs_checkpoint: Duration,
    pub teardown: Duration,
    pub total: Duration,
}

impl CheckpointTiming {
    /// The phases in order.
    pub fn phases(&self) -> Vec<Phase> {
        let p = |name, d| Phase { name, d };
        vec![
            p(phase::PREP, self.prep),
            p(phase::CHECKPOINT, self.checkpoint),
            p(phase::FS_CHECKPOINT, self.fs_checkpoint),
            p(phase::TEARDOWN, self.teardown),
            p(phase::TOTAL, self.total),
        ]
    }
}

/// The time since `*last`, advancing it, so consecutive calls time
/// consecutive phases.
pub fn lap(last: &mut Instant) -> Duration {
    let now = Instant::now();
    let d = now.duration_since(*last);
    *last = now;
    d
}

/// The fidelity as the label value records carry; anything unknown is
/// `unknown`, so no wire value widens the label set. The worker and keeper
/// fidelity enums share their numbering.
pub fn fidelity_label(fidelity: i32) -> &'static str {
    attr::snapshot_fidelity_value(fidelity)
}

/// The record of one checkpoint or restore: the Actor's identity, the
/// fidelity, `error.type` when it failed (the gRPC code), and the seconds of
/// each phase that ran under `<duration_key>.<phase>`.
pub fn snapshot_phase_record(
    a: &ActorAttribution,
    fidelity: i32,
    duration_key: &str,
    err: Option<tonic::Code>,
    phases: &[Phase],
) -> Map<String, Value> {
    let mut m = actor_fields(a);
    m.insert(
        attr::SNAPSHOT_FIDELITY_KEY.into(),
        fidelity_label(fidelity).into(),
    );
    if let Some(code) = err {
        m.insert(attr::ERROR_TYPE_KEY.into(), format!("{code:?}").into());
    }
    for p in phases.iter().filter(|p| !p.d.is_zero()) {
        m.insert(
            format!("{duration_key}.{}", p.name),
            p.d.as_secs_f64().into(),
        );
    }
    m
}

/// An Actor's identity as record fields, under the workspace-wide keys.
pub fn actor_fields(a: &ActorAttribution) -> Map<String, Value> {
    attr::actor_log_attrs(a)
        .into_iter()
        .map(|(k, v)| (k.to_string(), Value::String(v)))
        .collect()
}

/// Logs a record under `msg`.
pub fn log_record(msg: &str, record: &Map<String, Value>) {
    let record = Value::Object(record.clone());
    tracing::info!(record = %record, "{msg}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::uid_only;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn restore_record() {
        let t = RestoreTiming {
            prep: ms(10),
            egress_prepare: ms(120),
            net_setup: ms(30),
            sandbox_create: ms(200),
            sandbox_restore: ms(900),
            app_rootfs: ms(15),
            app_create: ms(50),
            app_restore: ms(400),
            wakeup_probe: ms(250),
            activate: ms(2),
            total: ms(2000),
        };
        let mut rec = snapshot_phase_record(
            &uid_only("uid-abc"),
            3,
            RESTORE_DURATION_KEY,
            None,
            &t.phases(),
        );
        rec.insert(CONTAINER_COUNT_KEY.into(), 2.into());
        let rec: Map<String, Value> =
            serde_json::from_str(&Value::Object(rec).to_string()).unwrap();
        for (k, want) in [
            ("prep", 0.01),
            ("egress_prepare", 0.12),
            ("net_setup", 0.03),
            ("sandbox_create", 0.2),
            ("sandbox_restore", 0.9),
            ("app_rootfs", 0.015),
            ("app_create", 0.05),
            ("app_restore", 0.4),
            ("wakeup_probe", 0.25),
            ("activate", 0.002),
            ("total", 2.0),
        ] {
            assert_eq!(
                rec[&format!("{RESTORE_DURATION_KEY}.{k}")].as_f64(),
                Some(want),
                "{k}"
            );
        }
        assert_eq!(rec[CONTAINER_COUNT_KEY], 2);
        assert_eq!(rec[attr::ACTOR_UID_KEY], "uid-abc");
        assert_eq!(rec[attr::SNAPSHOT_FIDELITY_KEY], "memory");
        assert!(!rec.contains_key("error.type"));
    }

    #[test]
    fn checkpoint_record_skips_phases_that_never_ran() {
        for (f, label) in [(1, "volumes"), (2, "rootfs")] {
            let t = CheckpointTiming {
                prep: ms(20),
                fs_checkpoint: ms(300),
                teardown: ms(150),
                total: ms(500),
                ..Default::default()
            };
            let rec = snapshot_phase_record(
                &uid_only("u"),
                f,
                CHECKPOINT_DURATION_KEY,
                Some(tonic::Code::Internal),
                &t.phases(),
            );
            for (k, want) in [
                ("prep", 0.02),
                ("fs_checkpoint", 0.3),
                ("teardown", 0.15),
                ("total", 0.5),
            ] {
                assert_eq!(
                    rec[&format!("{CHECKPOINT_DURATION_KEY}.{k}")].as_f64(),
                    Some(want),
                    "{k}"
                );
            }
            for absent in ["checkpoint", "durable_dir"] {
                assert!(
                    !rec.contains_key(&format!("{CHECKPOINT_DURATION_KEY}.{absent}")),
                    "{absent}"
                );
            }
            assert_eq!(rec[attr::SNAPSHOT_FIDELITY_KEY], label);
            assert_eq!(rec["error.type"], "Internal");
        }
        assert_eq!(fidelity_label(99), "unknown");
    }

    #[test]
    fn laps_are_consecutive() {
        let mut last = Instant::now() - Duration::from_secs(1);
        let before = last;
        assert!(lap(&mut last) >= Duration::from_secs(1));
        assert!(last > before);
        assert!(lap(&mut last) < Duration::from_secs(1));
    }
}
