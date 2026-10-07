#![allow(clippy::cloned_ref_to_slice_refs)]

use super::*;
use crate::cpu_features::encode_cpu_features;

fn attr(k: &str, v: &str) -> api::AttributeEntry {
    api::AttributeEntry {
        key: k.into(),
        value: v.into(),
    }
}

fn compat(ver: &str, attrs: &[api::AttributeEntry]) -> api::VersionedSandboxCompat {
    api::VersionedSandboxCompat {
        schema_version: ver.into(),
        attributes: attrs.to_vec(),
    }
}

fn runtime(class: &str, c: Option<api::VersionedSandboxCompat>) -> api::ActorSandboxRuntime {
    api::ActorSandboxRuntime {
        sandbox_class: class.into(),
        version: c,
    }
}

fn wrt(c: api::VersionedSandboxCompat) -> api::WorkerSandboxRuntime {
    api::WorkerSandboxRuntime {
        version: Some(c),
        actor_overhead: None,
    }
}

fn status(
    class: &str,
    def: api::WorkerSandboxRuntime,
    rest: Vec<api::WorkerSandboxRuntime>,
) -> api::WorkerStatus {
    api::WorkerStatus {
        sandbox_classes: vec![api::WorkerSandboxClass {
            sandbox_class: class.into(),
            default_runtime: Some(def),
            restorable_runtimes: rest,
        }],
        ..Default::default()
    }
}

#[test]
fn probe_host_reports_the_architecture() {
    let c = probe_host();
    assert_eq!(c.schema_version, SCHEMA_VERSION_V1);
    assert_eq!(c.attributes.len(), 1);
    assert_eq!(c.attributes[0].key, ATTR_ARCHITECTURE);
    assert_eq!(c.attributes[0].value, host_arch());
}

#[test]
fn matches_runtimes() {
    let arch = |a: &str| attr(ATTR_ARCHITECTURE, a);
    let cpu = attr(ATTR_CPU_FEATURES, "7");
    let amd64 = runtime("gvisor", Some(compat("v1", &[arch("amd64"), cpu.clone()])));
    let reordered = runtime("gvisor", Some(compat("v1", &[cpu.clone(), arch("amd64")])));
    let arm64 = runtime("gvisor", Some(compat("v1", &[arch("arm64"), cpu.clone()])));
    let microvm = runtime("microvm", Some(compat("v1", &[arch("amd64"), cpu.clone()])));
    let v2 = runtime("gvisor", Some(compat("v2", &[arch("amd64"), cpu.clone()])));
    let extra = runtime(
        "gvisor",
        Some(compat(
            "v1",
            &[
                arch("amd64"),
                cpu.clone(),
                attr(ATTR_SANDBOX_BUNDLE_HASH, "sha256:abc"),
            ],
        )),
    );
    let no_version = runtime("gvisor", None);
    for (name, worker, snap, want) in [
        (
            "nil snapshot imposes no constraint",
            Some(&amd64),
            None,
            true,
        ),
        ("exact match", Some(&amd64), Some(&amd64), true),
        (
            "attribute order ignored",
            Some(&amd64),
            Some(&reordered),
            true,
        ),
        (
            "different attribute value fails",
            Some(&arm64),
            Some(&amd64),
            false,
        ),
        (
            "different sandbox class fails",
            Some(&microvm),
            Some(&amd64),
            false,
        ),
        (
            "different schema version fails",
            Some(&v2),
            Some(&amd64),
            false,
        ),
        (
            "attribute only the worker has is ignored",
            Some(&extra),
            Some(&amd64),
            true,
        ),
        (
            "attribute missing on worker fails",
            Some(&amd64),
            Some(&extra),
            false,
        ),
        (
            "nil worker fails when snapshot is stamped",
            None,
            Some(&amd64),
            false,
        ),
        ("nil version fails", Some(&no_version), Some(&amd64), false),
    ] {
        assert_eq!(matches(worker, snap), want, "{name}");
    }
}

#[test]
fn matches_compats() {
    let amd64 = attr(ATTR_ARCHITECTURE, "amd64");
    let arm64 = attr(ATTR_ARCHITECTURE, "arm64");
    let hash = attr(ATTR_SANDBOX_BUNDLE_HASH, "sha256:abc");
    let bits = |v: &str| attr(ATTR_CPU_FEATURES, v);
    let high_snap = encode_cpu_features("amd64", &["pni", "avx2", "avx512_bf16"]).unwrap();
    let superset =
        encode_cpu_features("amd64", &["pni", "sse2", "avx2", "avx_vnni", "avx512_bf16"]).unwrap();
    let missing_one = encode_cpu_features("amd64", &["pni", "sse2", "avx2", "avx_vnni"]).unwrap();
    let c = |v: &str, a: &[api::AttributeEntry]| Some(compat(v, a));
    let cases: Vec<(
        &str,
        Option<api::VersionedSandboxCompat>,
        Option<api::VersionedSandboxCompat>,
        bool,
    )> = vec![
        (
            "equal",
            c("v1", &[amd64.clone()]),
            c("v1", &[amd64.clone()]),
            true,
        ),
        (
            "key only the worker has is ignored",
            c("v1", &[amd64.clone(), bits("7")]),
            c("v1", &[amd64.clone()]),
            true,
        ),
        (
            "key only the snapshot has fails",
            c("v1", &[amd64.clone()]),
            c("v1", &[amd64.clone(), bits("7")]),
            false,
        ),
        (
            "key only the snapshot has fails even at equal count",
            c("v1", &[amd64.clone(), bits("7")]),
            c("v1", &[amd64.clone(), hash.clone()]),
            false,
        ),
        (
            "value differs",
            c("v1", &[arm64.clone()]),
            c("v1", &[amd64.clone()]),
            false,
        ),
        (
            "cpu_features exact match",
            c("v1", &[amd64.clone(), bits(&high_snap)]),
            c("v1", &[amd64.clone(), bits(&high_snap)]),
            true,
        ),
        (
            "cpu_features worker is superset",
            c("v1", &[amd64.clone(), bits(&superset)]),
            c("v1", &[amd64.clone(), bits(&high_snap)]),
            true,
        ),
        (
            "cpu_features worker missing a bit",
            c("v1", &[amd64.clone(), bits(&missing_one)]),
            c("v1", &[amd64.clone(), bits(&high_snap)]),
            false,
        ),
        (
            "cpu_features empty snapshot set matches any worker",
            c("v1", &[amd64.clone(), bits("7")]),
            c("v1", &[amd64.clone(), bits("0")]),
            true,
        ),
        (
            "cpu_features empty worker set matches nothing",
            c("v1", &[amd64.clone(), bits("0")]),
            c("v1", &[amd64.clone(), bits("7")]),
            false,
        ),
        (
            "cpu_features invalid worker bitmask fails",
            c("v1", &[amd64.clone(), bits("not-a-number")]),
            c("v1", &[amd64.clone(), bits("7")]),
            false,
        ),
        (
            "cpu_features invalid snapshot bitmask fails",
            c("v1", &[amd64.clone(), bits("7")]),
            c("v1", &[amd64.clone(), bits("-1")]),
            false,
        ),
        (
            "cpu_features non-canonical bitmask fails",
            c("v1", &[amd64.clone(), bits("07")]),
            c("v1", &[amd64.clone(), bits("7")]),
            false,
        ),
        (
            "cpu_features uppercase hex fails",
            c("v1", &[amd64.clone(), bits("1F")]),
            c("v1", &[amd64.clone(), bits("7")]),
            false,
        ),
        (
            "schema version differs",
            c("v2", &[amd64.clone()]),
            c("v1", &[amd64.clone()]),
            false,
        ),
        (
            "both schema versions empty",
            c("", &[amd64.clone()]),
            c("", &[amd64.clone()]),
            false,
        ),
        ("no attributes on either", c("v1", &[]), c("v1", &[]), true),
        (
            "nil snapshot identity",
            c("v1", &[amd64.clone()]),
            None,
            false,
        ),
        (
            "nil worker identity",
            None,
            c("v1", &[amd64.clone()]),
            false,
        ),
        ("both nil", None, None, false),
    ];
    for (name, worker, snap, want) in cases {
        assert_eq!(
            matches_compat(worker.as_ref(), snap.as_ref()),
            want,
            "{name}"
        );
    }
}

#[test]
fn requirement_prepares_once() {
    let rt = |cpu: &str| {
        runtime(
            "gvisor",
            Some(compat(
                SCHEMA_VERSION_V1,
                &[
                    attr(ATTR_ARCHITECTURE, "amd64"),
                    attr(ATTR_CPU_FEATURES, cpu),
                ],
            )),
        )
    };
    let snap = rt("7");
    let req = Requirement::new(Some(&snap));
    assert_eq!(req.runtime(), Some(&snap));
    for (name, worker, want) in [
        ("worker superset", rt("f"), true),
        ("worker missing a bit", rt("3"), false),
        ("worker mask invalid", rt("zz"), false),
        ("worker superset after misses", rt("7"), true),
    ] {
        assert_eq!(req.matches(Some(&worker)), want, "{name}");
    }
    let st = status(
        "gvisor",
        wrt(rt("3").version.unwrap()),
        vec![wrt(rt("f").version.unwrap())],
    );
    assert_eq!(req.matching_runtime_stamp(Some(&st)), Some(rt("f")));
    assert!(!Requirement::new(Some(&rt("zz"))).matches(Some(&rt("f"))));
    let none = Requirement::default();
    assert!(none.runtime().is_none() && none.matches(None) && none.worker_matches(None));
    assert_eq!(none.runtime_for(Some(&st)), Some(rt("3")));
}

#[test]
fn matching_runtime_and_worker_matches() {
    let c = |arch: &str, hash: &str| {
        let mut a = vec![attr(ATTR_ARCHITECTURE, arch)];
        if !hash.is_empty() {
            a.push(attr(ATTR_SANDBOX_BUNDLE_HASH, hash));
        }
        compat(SCHEMA_VERSION_V1, &a)
    };
    let def = c("amd64", "v2-hash");
    let r1 = c("amd64", "v1-hash");
    let r2 = c("amd64", "v0-hash");
    let def_rt = runtime("gvisor", Some(def.clone()));
    let unmatched = runtime("gvisor", Some(c("arm64", "v2-hash")));
    let st = status("gvisor", wrt(def.clone()), vec![wrt(r1), wrt(r2.clone())]);
    let empty = api::WorkerStatus::default();

    // A missing snapshot matches any worker and returns the default.
    assert!(worker_matches(None, None));
    assert!(worker_matches(Some(&empty), None));
    assert_eq!(matching_runtime(Some(&st), None), Some(def_rt.clone()));
    // The default runtime.
    assert!(worker_matches(Some(&st), Some(&def_rt)));
    assert_eq!(
        matching_runtime(Some(&st), Some(&def_rt)),
        Some(def_rt.clone())
    );
    // A restorable runtime.
    let want = runtime("gvisor", Some(r2.clone()));
    assert!(worker_matches(Some(&st), Some(&want)));
    assert_eq!(matching_runtime(Some(&st), Some(&want)), Some(want.clone()));
    // Neither.
    assert!(!worker_matches(Some(&st), Some(&unmatched)));
    assert_eq!(matching_runtime(Some(&st), Some(&unmatched)), None);
    assert!(!worker_matches(None, Some(&def_rt)));
    // A class the worker does not run.
    let other_class = runtime("microvm", Some(def.clone()));
    assert!(!worker_matches(Some(&st), Some(&other_class)));
    assert_eq!(matching_runtime(Some(&st), Some(&other_class)), None);
    // runtime_for: the match, else the default.
    assert_eq!(runtime_for(Some(&st), Some(&want)), Some(want.clone()));
    assert_eq!(
        runtime_for(Some(&st), Some(&unmatched)),
        Some(def_rt.clone())
    );
    assert_eq!(runtime_for(Some(&st), None), Some(def_rt.clone()));
    assert_eq!(runtime_for(Some(&empty), Some(&unmatched)), None);
    // Cold boot before registration.
    assert_eq!(cold_boot_runtime(Some(&empty)), None);
    assert_eq!(cold_boot_runtime(None), None);
    assert_eq!(cold_boot_runtime(Some(&st)), Some(def_rt));
    assert_eq!(sandbox_class(Some(&st)), "gvisor");
    assert_eq!(sandbox_class(None), "");
}

#[test]
fn worker_runtime_for() {
    let c = |hash: &str| {
        compat(
            SCHEMA_VERSION_V1,
            &[
                attr(ATTR_ARCHITECTURE, "amd64"),
                attr(ATTR_SANDBOX_BUNDLE_HASH, hash),
            ],
        )
    };
    let mut def = wrt(c("new"));
    def.actor_overhead = Some(api::RuntimeOverhead {
        memory_bytes: 4 << 20,
        memory_bytes_per_gib: 0,
    });
    let mut old = wrt(c("old"));
    old.actor_overhead = Some(api::RuntimeOverhead {
        memory_bytes: 16 << 20,
        memory_bytes_per_gib: 0,
    });
    let st = status("microvm", def.clone(), vec![old.clone()]);
    let stamp = |v| runtime("microvm", Some(v));
    for (name, snap, want) in [
        ("cold boot runs the default", None, &def),
        (
            "a restore runs the matching runtime",
            Some(stamp(c("old"))),
            &old,
        ),
        (
            "a snapshot nothing restores cold boots on the default",
            Some(stamp(c("gone"))),
            &def,
        ),
    ] {
        let r = Requirement::new(snap.as_ref());
        let got = r.worker_runtime_for(Some(&st)).unwrap();
        assert_eq!(got, want, "{name}");
        assert_eq!(
            r.runtime_for(Some(&st)).unwrap().version,
            got.version,
            "{name}"
        );
    }
    assert!(Requirement::new(None)
        .worker_runtime_for(Some(&api::WorkerStatus::default()))
        .is_none());
}
