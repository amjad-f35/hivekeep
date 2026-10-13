//! The runtime index: registration, provenance, resolution and its file.

use super::*;
use crate::assets::{AssetEntry, GVISOR_ASSET_NAME};
use tonic::Code;

const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SHA_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn hashed(name: &str, is_default: bool, sha: &str) -> HashedVersion {
    let mut rec = SandboxAssetsRecord {
        sandbox_class: "gvisor".into(),
        ..Default::default()
    };
    rec.assets.insert(
        GVISOR_ASSET_NAME.into(),
        AssetEntry {
            url: format!("gs://bucket/{name}"),
            sha256: sha.into(),
        },
    );
    let hash = rec.bundle_hash();
    HashedVersion {
        name: name.into(),
        is_default,
        rec,
        hash,
    }
}

fn gvisor(config: &str, hashed: Vec<HashedVersion>) -> Vec<AdvertisedClass> {
    vec![AdvertisedClass {
        class: "gvisor".into(),
        config: config.into(),
        hashed,
    }]
}

#[test]
fn index() {
    let x = RuntimeIndex::in_memory();
    let (v1, v2) = (hashed("v1", true, SHA_A), hashed("v2", false, SHA_B));
    x.register("pod-1", &gvisor("config-x", vec![v1.clone(), v2.clone()]))
        .unwrap();
    for v in [&v1, &v2] {
        let rt = x.lookup(&v.hash).unwrap();
        assert_eq!(
            (rt.rec, rt.config.as_str(), rt.version.as_str()),
            (v.rec.clone(), "config-x", v.name.as_str())
        );
    }
    let wr = x.worker("pod-1").expect("pod-1 indexed");
    let cr = &wr["gvisor"];
    assert_eq!(cr.config, "config-x");
    assert_eq!(cr.default_hash, v1.hash);
    assert_eq!(cr.hashes, vec![v1.hash.clone(), v2.hash.clone()]);
    assert!(
        !wr.contains_key("microvm"),
        "advertises a class its pool does not run"
    );

    // The same binaries under another config keep the first provenance.
    let same = hashed("release-1", true, SHA_A);
    assert_eq!(same.hash, v1.hash, "identical binaries hashed differently");
    x.register("pod-2", &gvisor("config-y", vec![same.clone()]))
        .unwrap();
    let rt = x.lookup(&v1.hash).unwrap();
    assert_eq!(
        (rt.config.as_str(), rt.version.as_str()),
        ("config-x", "v1")
    );
    assert_eq!(x.worker("pod-2").unwrap()["gvisor"].default_hash, same.hash);

    // Re-planning replaces what pod-1 advertises; v1 stays resolvable.
    x.register(
        "pod-1",
        &gvisor("config-x", vec![hashed("v2", true, SHA_B)]),
    )
    .unwrap();
    let cr = &x.worker("pod-1").unwrap()["gvisor"];
    assert_eq!(
        (cr.default_hash.clone(), cr.hashes.len()),
        (v2.hash.clone(), 1)
    );
    assert!(
        x.lookup(&v1.hash).is_some(),
        "v1 forgotten when pod-1 stopped advertising it"
    );

    x.forget("pod-2");
    assert!(x.worker("pod-2").is_none());
    assert!(
        x.lookup(&same.hash).is_some(),
        "runtime forgotten with its pod"
    );
    assert!(x.lookup(&format!("sha256:{}0000", &SHA_B[..60])).is_none());
}

/// Making a config's runtimes resolvable changes nothing a worker
/// advertises.
#[test]
fn register_runtimes() {
    let x = RuntimeIndex::in_memory();
    let old = hashed("v1", true, SHA_A);
    x.register("pod-1", &gvisor("config-x", vec![old.clone()]))
        .unwrap();
    let next = hashed("v2", true, SHA_B);
    x.register_runtimes("config-x", std::slice::from_ref(&next))
        .unwrap();
    let rt = x.lookup(&next.hash).unwrap();
    assert_eq!(
        (rt.version.as_str(), rt.config.as_str()),
        ("v2", "config-x")
    );
    assert_eq!(x.worker("pod-1").unwrap()["gvisor"].default_hash, old.hash);
    x.register_runtimes("config-y", &[hashed("v1", true, SHA_C)])
        .unwrap();
    assert!(
        x.worker("pod-2").is_none(),
        "register_runtimes created a worker entry"
    );
}

fn runtime_with(class: &str, attrs: &[(&str, &str)]) -> pb::ActorSandboxRuntime {
    pb::ActorSandboxRuntime {
        sandbox_class: class.into(),
        version: Some(pb::VersionedSandboxCompat {
            schema_version: "v1".into(),
            attributes: attrs
                .iter()
                .map(|(k, v)| pb::AttributeEntry {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        }),
    }
}

#[test]
fn resolve_runtime() {
    let x = RuntimeIndex::in_memory();
    let advertised = hashed("v1", true, SHA_C);
    let placed = runtime_with(
        "gvisor",
        &[
            ("architecture", "amd64"),
            (ATTR_SANDBOX_BUNDLE_HASH, &advertised.hash),
        ],
    );
    assert_eq!(
        x.advertised_runtime(Some(&placed)).unwrap_err().code(),
        Code::FailedPrecondition,
        "before any worker advertises it"
    );
    x.register("pod-1", &gvisor("config-x", vec![advertised.clone()]))
        .unwrap();
    assert_eq!(x.advertised_runtime(Some(&placed)).unwrap(), advertised.rec);

    for (name, rt) in [
        ("no runtime", None),
        ("no attributes", Some(runtime_with("gvisor", &[]))),
        (
            "no bundle hash",
            Some(runtime_with("gvisor", &[("architecture", "amd64")])),
        ),
        (
            "empty bundle hash",
            Some(runtime_with("gvisor", &[(ATTR_SANDBOX_BUNDLE_HASH, "")])),
        ),
    ] {
        assert_eq!(
            x.advertised_runtime(rt.as_ref()).unwrap_err().code(),
            Code::InvalidArgument,
            "{name}"
        );
    }
    let other = runtime_with("microvm", &[(ATTR_SANDBOX_BUNDLE_HASH, &advertised.hash)]);
    assert_eq!(
        x.advertised_runtime(Some(&other)).unwrap_err().code(),
        Code::FailedPrecondition,
        "the hash under another class"
    );
}

#[test]
fn persists() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("sandbox-runtime-index.json");
    let x = RuntimeIndex::open(path.clone()).unwrap();
    let (v1, v2) = (hashed("v1", true, SHA_A), hashed("v2", false, SHA_B));
    x.register_runtimes("config-x", std::slice::from_ref(&v1))
        .unwrap();
    x.register("pod-1", &gvisor("config-x", vec![v1.clone(), v2.clone()]))
        .unwrap();

    let reopened = RuntimeIndex::open(path.clone()).unwrap();
    for v in [&v1, &v2] {
        let rt = reopened
            .lookup(&v.hash)
            .unwrap_or_else(|| panic!("{} not loaded", v.name));
        assert_eq!(
            (rt.config.as_str(), rt.version.as_str()),
            ("config-x", v.name.as_str())
        );
        assert_eq!(
            rt.rec.assets[GVISOR_ASSET_NAME].sha256,
            v.rec.assets[GVISOR_ASSET_NAME].sha256
        );
    }
    assert!(
        reopened.worker("pod-1").is_none(),
        "a worker entry survived the restart"
    );

    // An entry stored under a hash its record does not have is dropped.
    let mut f: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let wrong = format!("sha256:{SHA_C}");
    let runtimes = f["runtimes"].as_object_mut().unwrap();
    let e = runtimes.remove(&v1.hash).unwrap();
    runtimes.insert(wrong.clone(), e);
    std::fs::write(&path, serde_json::to_vec(&f).unwrap()).unwrap();
    let damaged = RuntimeIndex::open(path.clone()).unwrap();
    assert!(
        damaged.lookup(&v2.hash).is_some(),
        "the intact entry was not loaded"
    );
    assert!(damaged.lookup(&wrong).is_none());
    assert!(damaged.lookup(&v1.hash).is_none());

    // Registering again rewrites the file without the bad entry.
    damaged
        .register_runtimes("config-x", std::slice::from_ref(&v1))
        .unwrap();
    let rewritten = RuntimeIndex::open(path.clone()).unwrap();
    assert!(rewritten.lookup(&v1.hash).is_some() && rewritten.lookup(&v2.hash).is_some());
    assert_eq!(
        rewritten.state.read().runtimes.len(),
        2,
        "the bad entry survived the rewrite"
    );

    // A corrupt file starts empty.
    std::fs::write(&path, b"{").unwrap();
    assert_eq!(
        RuntimeIndex::open(path)
            .unwrap()
            .state
            .read()
            .runtimes
            .len(),
        0
    );
}

#[test]
fn persist_failure_is_reported() {
    let d = tempfile::tempdir().unwrap();
    // The directory is never created.
    let x = RuntimeIndex::open(d.path().join("missing/sandbox-runtime-index.json")).unwrap();
    let v = hashed("v1", true, SHA_A);
    assert!(x
        .register_runtimes("config-x", std::slice::from_ref(&v))
        .is_err());
    assert!(
        x.lookup(&v.hash).is_none(),
        "a runtime whose file was not written was indexed"
    );
}
