// Table-driven cases spell out their tuple types.
#![allow(clippy::type_complexity)]

use hivekeep_k8s::crd::{SandboxConfigReference, WorkerPoolStatus};
use k8s_openapi::jiff::Timestamp;

use super::super::test_support::{
    class_status, make_sandbox_config, make_worker_pool, make_worker_pool_classes, with_finalizer,
};
use super::*;

const GV: SandboxClass = SandboxClass::Gvisor;
const VM: SandboxClass = SandboxClass::Microvm;

fn t(secs: i64) -> Time {
    Time(Timestamp::from_second(secs).unwrap())
}

const T0: i64 = 1_790_812_800; // 2026-10-01T00:00:00Z
const T1: i64 = T0 + 60;

fn sc(name: &str, class: SandboxClass, default: bool, created: i64) -> SandboxConfig {
    let mut c = make_sandbox_config(name, class, default);
    c.metadata.creation_timestamp = Some(t(created));
    c
}

fn deleting(mut c: SandboxConfig) -> SandboxConfig {
    c.metadata.deletion_timestamp = Some(t(T1));
    c
}

fn protected(c: SandboxConfig) -> SandboxConfig {
    with_finalizer(c, WORKER_POOL_PROTECTION_FINALIZER)
}

fn gvisor(r: &str) -> WorkerPoolSandboxClass {
    WorkerPoolSandboxClass {
        name: GV,
        config_ref: (!r.is_empty()).then(|| SandboxConfigReference::new(r)),
    }
}

const NO_DEFAULT: &str = r#"no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.hivekeep.io/is-class-default=true"#;
const DEFAULT_DELETING: &str =
    r#"the default SandboxConfig for sandbox class "gvisor" is being deleted"#;

#[test]
fn resolve_sandbox_config_cases() {
    struct Case {
        name: &'static str,
        entry: WorkerPoolSandboxClass,
        in_use: &'static str,
        configs: Vec<SandboxConfig>,
        want: &'static str,
        reason: &'static str,
        message: String,
    }
    let c = |name, entry, in_use, configs, want, reason, message: &str| Case {
        name,
        entry,
        in_use,
        configs,
        want,
        reason,
        message: message.to_string(),
    };
    let mut annotated_true = sc("no", GV, false, T0);
    annotated_true.metadata.annotations =
        Some([(CLASS_DEFAULT_ANNOTATION.to_string(), "True".to_string())].into());
    let cases = vec![
        c(
            "explicit ref resolves even when another config is the default",
            gvisor("custom"),
            "",
            vec![sc("custom", GV, false, T0), sc("def", GV, true, T1)],
            "custom",
            "",
            r#"resolved to SandboxConfig "custom""#,
        ),
        c(
            "explicit ref not found",
            gvisor("missing"),
            "",
            vec![sc("def", GV, true, T0)],
            "",
            REASON_SANDBOX_CONFIG_NOT_FOUND,
            r#"SandboxConfig "missing" not found"#,
        ),
        c(
            "explicit ref not found keeps the config in use",
            gvisor("missing"),
            "old",
            vec![sc("old", GV, false, T0)],
            "old",
            REASON_SANDBOX_CONFIG_NOT_FOUND,
            r#"SandboxConfig "missing" not found; keeping "old""#,
        ),
        c(
            "explicit ref of another class",
            gvisor("vm"),
            "",
            vec![sc("vm", VM, false, T0)],
            "",
            REASON_SANDBOX_CLASS_MISMATCH,
            r#"SandboxConfig "vm" is for sandbox class "microvm", not "gvisor""#,
        ),
        c(
            "explicit ref of another class keeps the config in use",
            gvisor("vm"),
            "old",
            vec![sc("vm", VM, false, T0), sc("old", GV, false, T0)],
            "old",
            REASON_SANDBOX_CLASS_MISMATCH,
            r#"SandboxConfig "vm" is for sandbox class "microvm", not "gvisor"; keeping "old""#,
        ),
        c(
            "explicit ref being deleted takes no new users",
            gvisor("dying"),
            "",
            vec![deleting(sc("dying", GV, false, T0))],
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "dying" is being deleted"#,
        ),
        c(
            "explicit ref being deleted is kept but reported by a pool already using it",
            gvisor("dying"),
            "dying",
            vec![protected(deleting(sc("dying", GV, false, T0)))],
            "dying",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "dying" is being deleted; keeping "dying""#,
        ),
        c(
            "explicit ref being deleted and released is not kept by a pool using it",
            gvisor("dying"),
            "dying",
            vec![deleting(sc("dying", GV, false, T0))],
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "dying" is being deleted"#,
        ),
        c(
            "explicit ref being deleted keeps the previous config in use",
            gvisor("dying"),
            "old",
            vec![
                deleting(sc("dying", GV, false, T0)),
                sc("old", GV, false, T0),
            ],
            "old",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "dying" is being deleted; keeping "old""#,
        ),
        c(
            "no default",
            gvisor(""),
            "",
            vec![sc("plain", GV, false, T0)],
            "",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            NO_DEFAULT,
        ),
        c(
            "default of another class is ignored",
            gvisor(""),
            "",
            vec![sc("vm", VM, true, T0)],
            "",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            NO_DEFAULT,
        ),
        c(
            "annotation must be exactly true",
            gvisor(""),
            "",
            vec![annotated_true],
            "",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            NO_DEFAULT,
        ),
        c(
            "default being deleted takes no new users",
            gvisor(""),
            "",
            vec![deleting(sc("dying", GV, true, T0))],
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            DEFAULT_DELETING,
        ),
        c(
            "several defaults being deleted are reported",
            gvisor(""),
            "",
            vec![
                deleting(sc("dying-new", GV, true, T1)),
                deleting(sc("dying-old", GV, true, T0)),
            ],
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            DEFAULT_DELETING,
        ),
        c(
            "default being deleted is kept but reported by a pool already using it",
            gvisor(""),
            "dying",
            vec![protected(deleting(sc("dying", GV, true, T0)))],
            "dying",
            REASON_SANDBOX_CONFIG_DELETING,
            &format!(r#"{DEFAULT_DELETING}; keeping "dying""#),
        ),
        c(
            "default being deleted and released is not kept by a pool using it",
            gvisor(""),
            "dying",
            vec![deleting(sc("dying", GV, true, T0))],
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            DEFAULT_DELETING,
        ),
        c(
            "a live default replaces the deleting one in use, even when older",
            gvisor(""),
            "dying",
            vec![deleting(sc("dying", GV, true, T1)), sc("old", GV, true, T0)],
            "old",
            "",
            r#"resolved to SandboxConfig "old""#,
        ),
        c(
            "config in use that lost the default annotation is kept but reported",
            gvisor(""),
            "plain",
            vec![sc("plain", GV, false, T0)],
            "plain",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            &format!(r#"{NO_DEFAULT}; keeping "plain""#),
        ),
        c(
            "config in use that no longer exists is not kept",
            gvisor(""),
            "gone",
            vec![sc("plain", GV, false, T0)],
            "",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            NO_DEFAULT,
        ),
        c(
            "config in use of another class is not kept",
            gvisor(""),
            "vm",
            vec![sc("vm", VM, true, T0)],
            "",
            REASON_DEFAULT_CONFIG_NOT_FOUND,
            NO_DEFAULT,
        ),
        c(
            "newest default wins",
            gvisor(""),
            "",
            vec![
                sc("a-old", GV, true, T0),
                sc("z-new", GV, true, T1),
                sc("plain", GV, false, T1),
            ],
            "z-new",
            "",
            r#"resolved to SandboxConfig "z-new""#,
        ),
        c(
            "newest default wins regardless of list order",
            gvisor(""),
            "",
            vec![sc("z-new", GV, true, T1), sc("a-old", GV, true, T0)],
            "z-new",
            "",
            r#"resolved to SandboxConfig "z-new""#,
        ),
        c(
            "creation time tie goes to the first name",
            gvisor(""),
            "",
            vec![sc("b", GV, true, T0), sc("a", GV, true, T0)],
            "a",
            "",
            r#"resolved to SandboxConfig "a""#,
        ),
    ];
    for tc in cases {
        let in_use = (!tc.in_use.is_empty()).then_some(tc.in_use);
        let (got, cond) = resolve_sandbox_config(&tc.entry, in_use, &tc.configs);
        let got = got.map(|s| s.name_any()).unwrap_or_default();
        let want = Resolution {
            resolved: tc.reason.is_empty(),
            reason: if tc.reason.is_empty() {
                REASON_RESOLVED
            } else {
                tc.reason
            }
            .to_string(),
            message: tc.message.clone(),
        };
        assert_eq!((got.as_str(), &cond), (tc.want, &want), "{}", tc.name);
    }
}

#[test]
fn worker_pool_may_use_sandbox_config_cases() {
    let config = make_sandbox_config("sc", GV, false);
    let gv = WorkerPoolSandboxClass {
        name: GV,
        config_ref: None,
    };
    let vm = WorkerPoolSandboxClass {
        name: VM,
        config_ref: None,
    };
    let with_ref = |e: &WorkerPoolSandboxClass, n: &str| WorkerPoolSandboxClass {
        config_ref: Some(SandboxConfigReference::new(n)),
        ..e.clone()
    };
    let using = |n: &str| class_status(VM, Some(n), None);
    let cases = vec![
        ("runs the config's class", vec![gv.clone()], vec![], true),
        (
            "runs the config's class with another ref",
            vec![with_ref(&gv, "other")],
            vec![],
            true,
        ),
        (
            "spec names the config",
            vec![with_ref(&vm, "sc")],
            vec![],
            true,
        ),
        (
            "status names the config",
            vec![vm.clone()],
            vec![using("sc")],
            true,
        ),
        (
            "status claims the config",
            vec![vm.clone()],
            vec![class_status(VM, Some("other"), Some("sc"))],
            true,
        ),
        (
            "another class, another config",
            vec![with_ref(&vm, "other")],
            vec![using("other")],
            false,
        ),
        (
            "another class, no config in use",
            vec![with_ref(&vm, "other")],
            vec![class_status(VM, None, None)],
            false,
        ),
        ("no sandbox classes", vec![], vec![], false),
    ];
    for (name, spec, status, want) in cases {
        let wp = make_worker_pool_classes("default", "pool", spec, status);
        assert_eq!(
            worker_pool_may_use_sandbox_config(&wp, &config),
            want,
            "{name}"
        );
    }
}

fn cond(status: bool, reason: &str, message: &str, generation: i64, at: Time) -> Condition {
    Condition {
        type_: CONDITION_RESOLVED.into(),
        status: if status { "True" } else { "False" }.into(),
        reason: reason.into(),
        message: message.into(),
        observed_generation: Some(generation),
        last_transition_time: at,
    }
}

#[test]
fn resolve_sandbox_classes_cases() {
    const GEN: i64 = 3;
    let earlier = t(1_577_836_800); // 2020-01-01
    let configs = vec![make_sandbox_config("def", GV, true)];
    let now = super::super::meta::now();
    let resolved_def = |gen, at: &Time| {
        cond(
            true,
            REASON_RESOLVED,
            r#"resolved to SandboxConfig "def""#,
            gen,
            at.clone(),
        )
    };
    let not_found = |msg: &str, gen, at: &Time| {
        cond(false, REASON_SANDBOX_CONFIG_NOT_FOUND, msg, gen, at.clone())
    };
    let entry = |config: Option<&str>, conds: Vec<Condition>| WorkerPoolSandboxClassStatus {
        name: GV,
        config_ref: config.map(SandboxConfigReference::new),
        conditions: conds,
        ..Default::default()
    };
    let cases = vec![
        (
            "first resolve",
            "",
            vec![],
            vec![entry(Some("def"), vec![resolved_def(GEN, &now)])],
            true,
        ),
        (
            "still resolved keeps lastTransitionTime",
            "",
            vec![entry(Some("def"), vec![resolved_def(GEN - 1, &earlier)])],
            vec![entry(Some("def"), vec![resolved_def(GEN, &earlier)])],
            true,
        ),
        (
            "never resolved records no config",
            "missing",
            vec![],
            vec![entry(
                None,
                vec![not_found(r#"SandboxConfig "missing" not found"#, GEN, &now)],
            )],
            false,
        ),
        (
            "stops resolving keeps the config and moves lastTransitionTime",
            "missing",
            vec![entry(Some("def"), vec![resolved_def(GEN - 1, &earlier)])],
            vec![entry(
                Some("def"),
                vec![not_found(
                    r#"SandboxConfig "missing" not found; keeping "def""#,
                    GEN,
                    &now,
                )],
            )],
            false,
        ),
    ];
    for (name, spec_ref, prev, want, want_resolved) in cases {
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.metadata.generation = Some(GEN);
        wp.spec.sandbox_classes[0].config_ref =
            (!spec_ref.is_empty()).then(|| SandboxConfigReference::new(spec_ref));
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: prev,
            ..Default::default()
        });
        let (got, resolved) = resolve_sandbox_classes(&wp, &configs, &now);
        assert_eq!(resolved, want_resolved, "{name}");
        assert_eq!(got, want, "{name}");
    }
}

#[test]
fn claim_sandbox_configs_cases() {
    let resolved = Condition {
        type_: CONDITION_RESOLVED.into(),
        status: "True".into(),
        reason: REASON_RESOLVED.into(),
        message: String::new(),
        observed_generation: None,
        last_transition_time: t(T0),
    };
    let status = |config: Option<&str>, pending: Option<&str>, conds: &[Condition]| {
        vec![WorkerPoolSandboxClassStatus {
            conditions: conds.to_vec(),
            ..class_status(GV, config, pending)
        }]
    };
    let r = std::slice::from_ref(&resolved);
    let cases: Vec<(
        &str,
        Vec<WorkerPoolSandboxClassStatus>,
        Vec<WorkerPoolSandboxClassStatus>,
        Option<Vec<WorkerPoolSandboxClassStatus>>,
    )> = vec![
        (
            "same config: no claim",
            status(Some("x"), None, r),
            status(Some("x"), None, &[]),
            None,
        ),
        (
            "unresolved and nothing kept: no claim",
            vec![],
            status(None, None, &[]),
            None,
        ),
        (
            "new pool: claims, no config in use",
            vec![],
            status(Some("x"), None, &[]),
            Some(status(None, Some("x"), &[])),
        ),
        (
            "switch: claims and keeps the config in use",
            status(Some("y"), None, r),
            status(Some("x"), None, &[]),
            Some(status(Some("y"), Some("x"), r)),
        ),
        (
            "earlier claim for the same config: claimed again",
            status(Some("y"), Some("x"), r),
            status(Some("x"), None, &[]),
            Some(status(Some("y"), Some("x"), r)),
        ),
        (
            "earlier claim for another config: replaced",
            status(Some("y"), Some("x"), r),
            status(Some("z"), None, &[]),
            Some(status(Some("y"), Some("z"), r)),
        ),
        (
            "earlier claim, back to the config in use: no claim",
            status(Some("y"), Some("x"), r),
            status(Some("y"), None, &[]),
            None,
        ),
    ];
    for (name, prev, classes, want) in cases {
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: prev,
            ..Default::default()
        });
        assert_eq!(claim_sandbox_configs(&wp, &classes), want, "{name}");
    }
}

#[test]
fn confirm_sandbox_config_switch_cases() {
    const GEN: i64 = 3;
    let now = super::super::meta::now();
    let resolved_x = cond(
        true,
        REASON_RESOLVED,
        r#"resolved to SandboxConfig "x""#,
        GEN,
        now.clone(),
    );
    let class = WorkerPoolSandboxClassStatus {
        conditions: vec![resolved_x],
        ..class_status(GV, Some("x"), None)
    };
    let on_y = vec![class_status(GV, Some("y"), Some("x"))];
    let x = make_sandbox_config("x", GV, false);
    let mut deleting_x = x.clone();
    deleting_x.metadata.deletion_timestamp = Some(now.clone());
    let configs = vec![make_sandbox_config("y", GV, false), x.clone()];
    let vm_x = make_sandbox_config("x", VM, false);
    let cases: Vec<(
        &str,
        Vec<WorkerPoolSandboxClassStatus>,
        Option<&SandboxConfig>,
        &str,
        &str,
        &str,
    )> = vec![
        (
            "live: switches",
            on_y.clone(),
            Some(&x),
            "x",
            REASON_RESOLVED,
            r#"resolved to SandboxConfig "x""#,
        ),
        (
            "not found: keeps the config in use",
            on_y.clone(),
            None,
            "y",
            REASON_SANDBOX_CONFIG_NOT_FOUND,
            r#"SandboxConfig "x" not found; keeping "y""#,
        ),
        (
            "being deleted: keeps the config in use",
            on_y.clone(),
            Some(&deleting_x),
            "y",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "x" is being deleted; keeping "y""#,
        ),
        (
            "now of another class: keeps the config in use",
            on_y.clone(),
            Some(&vm_x),
            "y",
            REASON_SANDBOX_CLASS_MISMATCH,
            r#"SandboxConfig "x" is for sandbox class "microvm", not "gvisor"; keeping "y""#,
        ),
        (
            "being deleted, new pool: no config",
            vec![],
            Some(&deleting_x),
            "",
            REASON_SANDBOX_CONFIG_DELETING,
            r#"SandboxConfig "x" is being deleted"#,
        ),
    ];
    for (name, prev, live, want_config, want_reason, want_message) in cases {
        let mut wp = make_worker_pool("pool", "default", 1, "worker:v1");
        wp.metadata.generation = Some(GEN);
        wp.status = Some(WorkerPoolStatus {
            sandbox_classes: prev,
            ..Default::default()
        });
        let got = confirm_sandbox_config_switch(&wp, class.clone(), live, &configs, &now);
        assert_eq!(
            got.config_ref
                .as_ref()
                .map(|r| r.name.as_str())
                .unwrap_or(""),
            want_config,
            "{name}"
        );
        assert!(
            got.pending_config_ref.is_none(),
            "{name}: pendingConfigRef left"
        );
        let c = got
            .condition(CONDITION_RESOLVED)
            .unwrap_or_else(|| panic!("{name}: no Resolved condition"));
        assert_eq!(
            (c.reason.as_str(), c.message.as_str(), c.observed_generation),
            (want_reason, want_message, Some(GEN)),
            "{name}"
        );
    }
}

#[test]
fn every_class_has_config_cases() {
    assert!(
        every_class_has_config(&[class_status(GV, Some("def"), None)]),
        "every class has a config"
    );
    assert!(
        !every_class_has_config(&[
            class_status(GV, Some("def"), None),
            class_status(VM, None, None)
        ]),
        "one class has no config"
    );
    assert!(every_class_has_config(&[]), "no classes");
}
