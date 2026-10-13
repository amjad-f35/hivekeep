use serde_json::{json, Value};

use super::*;

const SHA: &str = "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63";

fn base_pool() -> Value {
    json!({
        "apiVersion": "hivekeep.io/v1", "kind": "WorkerPool",
        "metadata": {"name": "test", "namespace": "default"},
        "spec": {"replicas": 1, "workerImage": "worker:latest", "sandboxClasses": [{"name": "gvisor"}]}
    })
}

fn expect(result: Result<(), ValidationErrors>, want_err: Option<&str>, name: &str) {
    match (result, want_err) {
        (Ok(()), None) => {}
        (Ok(()), Some(want)) => panic!("{name}: unexpected success, expected {want:?}"),
        (Err(e), None) => panic!("{name}: unexpected failure: {e}"),
        (Err(e), Some(want)) => assert!(
            e.to_string().contains(want),
            "{name}: wrong error\n  wanted: {want:?}\n     got: {e}"
        ),
    }
}

#[test]
fn worker_pool_validation() {
    let many = |prefix: &str| -> Value {
        (0..65)
            .map(|i| (format!("{prefix}-{i}"), json!("value")))
            .collect::<serde_json::Map<_, _>>()
            .into()
    };
    let tol = json!({"key": "key", "operator": "Exists", "effect": "NoSchedule"});
    type Mutate = Box<dyn Fn(&mut Value)>;
    let cases: Vec<(&str, Mutate, Option<&str>)> = vec![
        ("base worker pool", Box::new(|_| {}), None),
        ("name of 63 characters", Box::new(|wp| wp["metadata"]["name"] = json!("a".repeat(63))), None),
        ("name of 64 characters", Box::new(|wp| wp["metadata"]["name"] = json!("a".repeat(64))), Some("metadata.name must be at most 63 characters")),
        ("replicas below minimum", Box::new(|wp| wp["spec"]["replicas"] = json!(-1)), Some("spec.replicas: Invalid value: -1: spec.replicas in body should be greater than or equal to 0")),
        ("missing workerImage", Box::new(|wp| wp["spec"]["workerImage"] = json!("")), Some("spec.workerImage: Invalid value: \"\": spec.workerImage in body should be at least 1 chars long")),
        ("missing sandboxClasses", Box::new(|wp| { wp["spec"].as_object_mut().unwrap().remove("sandboxClasses"); }), Some("spec.sandboxClasses: Required value")),
        ("empty sandboxClasses", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([])), Some("spec.sandboxClasses")),
        ("sandboxClass with empty name", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{}])), Some("spec.sandboxClasses[0].name")),
        ("more than one sandboxClass", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "gvisor"}, {"name": "microvm"}])), Some("spec.sandboxClasses: Too many")),
        ("unknown sandboxClass", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "kata"}])), Some("spec.sandboxClasses[0].name: Unsupported value")),
        ("sandboxClass with configRef", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "gvisor", "configRef": {"name": "gvisor-default"}}])), None),
        ("configRef with empty name", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "gvisor", "configRef": {}}])), Some("spec.sandboxClasses[0].configRef.name")),
        ("configRef name of 253 characters", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "gvisor", "configRef": {"name": "a".repeat(253)}}])), None),
        ("configRef name of 254 characters", Box::new(|wp| wp["spec"]["sandboxClasses"] = json!([{"name": "gvisor", "configRef": {"name": "a".repeat(254)}}])), Some("spec.sandboxClasses[0].configRef.name")),
        ("valid template", Box::new(|wp| wp["spec"]["template"] = json!({
            "labels": {"project": "hivekeep", "policy.example.com/profile": "sandbox_host"},
            "annotations": {"policy.example.com/exemption": "sandbox-host"},
            "nodeSelector": {"workload": "hivekeep"},
            "tolerations": [{"key": "gpu", "operator": "Exists", "effect": "NoSchedule"}],
            "resources": {"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "2Gi"}}
        })), None),
        ("invalid worker label key", Box::new(|wp| wp["spec"]["template"] = json!({"labels": {"bad key": "value"}})), Some("label keys must be valid Kubernetes qualified names")),
        ("invalid worker label value", Box::new(|wp| wp["spec"]["template"] = json!({"labels": {"project": "bad value"}})), Some("spec.template.labels.project in body should match")),
        ("reserved hivekeep.io label", Box::new(|wp| wp["spec"]["template"] = json!({"labels": {"hivekeep.io/custom": "value"}})), Some("hivekeep.io and its subdomains are reserved")),
        ("reserved hivekeep.io subdomain label", Box::new(|wp| wp["spec"]["template"] = json!({"labels": {"policy.hivekeep.io/exemption": "value"}})), Some("hivekeep.io and its subdomains are reserved")),
        ("reserved hivekeep.io annotation", Box::new(|wp| wp["spec"]["template"] = json!({"annotations": {"hivekeep.io/custom": "value"}})), Some("hivekeep.io and its subdomains are reserved")),
        ("reserved hivekeep.io subdomain annotation", Box::new(|wp| wp["spec"]["template"] = json!({"annotations": {"policy.hivekeep.io/exemption": "value"}})), Some("hivekeep.io and its subdomains are reserved")),
        ("invalid worker annotation key", Box::new(|wp| wp["spec"]["template"] = json!({"annotations": {"bad key": "value"}})), Some("annotation keys must be valid Kubernetes qualified names")),
        ("too many worker labels", Box::new(move |wp| wp["spec"]["template"] = json!({"labels": many("label")})), Some("spec.template.labels: Too many")),
        ("too many worker annotations", Box::new(move |wp| wp["spec"]["template"] = json!({"annotations": many("annotation")})), Some("spec.template.annotations: Too many")),
        ("too many tolerations", Box::new(move |wp| wp["spec"]["template"] = json!({"tolerations": vec![tol.clone(); 17]})), Some("spec.template.tolerations: Too many")),
        ("extended resource on a pool is not interpreted", Box::new(|wp| {
            wp["spec"]["sandboxClasses"] = json!([{"name": "microvm"}]);
            wp["spec"]["template"] = json!({"resources": {"limits": {"example.com/gpu": "1"}}});
        }), None),
    ];
    for (name, mutate, want) in cases {
        let mut wp = base_pool();
        mutate(&mut wp);
        expect(validate_worker_pool(&wp, None), want, name);
    }
}

#[test]
fn worker_pool_typed_objects_validate() {
    // The typed structs serialize to objects the rules accept.
    let wp = crate::crd::WorkerPool::new(
        "typed",
        crate::crd::WorkerPoolSpec {
            replicas: 0,
            worker_image: "img".into(),
            sandbox_classes: vec![crate::crd::WorkerPoolSandboxClass::default()],
            ..Default::default()
        },
    );
    validate_worker_pool(&serde_json::to_value(wp).unwrap(), None).unwrap();
}

#[test]
fn worker_pool_reserved_metadata_update() {
    let old = base_pool();
    let mut new = old.clone();
    new["spec"]["template"] = json!({"annotations": {"security.hivekeep.io/exemption": "value"}});
    expect(
        validate_worker_pool(&new, Some(&old)),
        Some("hivekeep.io and its subdomains are reserved"),
        "update",
    );
}

#[test]
fn worker_pool_sandbox_classes_update() {
    let old = base_pool();
    let cases: Vec<(&str, Value, Option<&str>)> = vec![
        (
            "change the sandbox class",
            json!([{"name": "microvm"}]),
            Some("sandboxClasses names are immutable"),
        ),
        (
            "set configRef",
            json!([{"name": "gvisor", "configRef": {"name": "gvisor-release1"}}]),
            None,
        ),
    ];
    for (name, classes, want) in cases {
        let mut new = old.clone();
        new["spec"]["sandboxClasses"] = classes;
        expect(validate_worker_pool(&new, Some(&old)), want, name);
    }
}

fn asset(url: &str) -> Value {
    json!({"url": url, "sha256": SHA})
}

fn gvisor_assets() -> Value {
    json!({"amd64": {"gvisor": asset("gs://bucket/gvisor.tar.bz2")}})
}

fn microvm_set(without: Option<&str>) -> Value {
    let mut m = serde_json::Map::new();
    for n in ["cloud-hypervisor", "virtiofsd", "kata-kernel", "kata-image"] {
        if Some(n) != without {
            m.insert(n.into(), asset("gs://bucket/asset"));
        }
    }
    Value::Object(m)
}

fn version(name: &str, assets: Value) -> Value {
    if assets.is_null() {
        json!({"name": name})
    } else {
        json!({"name": name, "assets": assets})
    }
}

fn config(class: &str, assets: Value) -> Value {
    json!({"apiVersion": "hivekeep.io/v1", "kind": "SandboxConfig", "metadata": {"name": "c"},
        "spec": {"sandboxClass": class, "defaultVersion": "v1", "versions": [version("v1", assets)]}})
}

fn with_version(mut sc: Value, v: Value) -> Value {
    sc["spec"]["versions"].as_array_mut().unwrap().push(v);
    sc
}

#[test]
fn sandbox_config_validation() {
    let other = json!({"url": "gs://bucket/other", "sha256": SHA});
    let cases: Vec<(&str, Value, Option<&str>)> = vec![
        (
            "valid gvisor with release tarball",
            config(
                "gvisor",
                json!({"amd64": {"gvisor": asset("gs://b/g")}, "arm64": {"gvisor": asset("gs://b/g")}}),
            ),
            None,
        ),
        (
            "valid microvm with full asset set",
            config("microvm", json!({"amd64": microvm_set(None)})),
            None,
        ),
        (
            "valid microvm arm64 asset set",
            config("microvm", json!({"arm64": microvm_set(None)})),
            None,
        ),
        (
            "microvm missing an asset",
            config("microvm", json!({"amd64": microvm_set(Some("kata-image"))})),
            Some("microvm SandboxConfig must define"),
        ),
        (
            "microvm missing virtiofsd",
            config("microvm", json!({"amd64": microvm_set(Some("virtiofsd"))})),
            Some("microvm SandboxConfig must define"),
        ),
        (
            "gvisor arch missing the gvisor asset",
            config("gvisor", json!({"amd64": {"other": other}})),
            Some("'gvisor' (release tarball) asset"),
        ),
        (
            "gvisor with only a bare runsc asset",
            config("gvisor", json!({"amd64": {"runsc": other}})),
            Some("'gvisor' (release tarball) asset"),
        ),
        (
            "gvisor one arch missing the gvisor asset",
            config(
                "gvisor",
                json!({"amd64": {"gvisor": asset("gs://b/g")}, "arm64": {"other": other}}),
            ),
            Some("'gvisor' (release tarball) asset"),
        ),
        (
            "gvisor with no assets",
            config("gvisor", Value::Null),
            Some("'gvisor' (release tarball) asset"),
        ),
        (
            "asset missing url",
            config("gvisor", json!({"amd64": {"gvisor": {"sha256": SHA}}})),
            Some("url"),
        ),
        (
            "asset missing sha256",
            config(
                "gvisor",
                json!({"amd64": {"gvisor": {"url": "gs://bucket/gvisor.tar.bz2"}}}),
            ),
            Some("sha256"),
        ),
        (
            "asset sha256 not 64 hex",
            config(
                "gvisor",
                json!({"amd64": {"gvisor": {"url": "gs://b/g", "sha256": "deadbeef"}}}),
            ),
            Some("sha256"),
        ),
        (
            "valid gvisor with two versions",
            with_version(
                config("gvisor", gvisor_assets()),
                version("v2", gvisor_assets()),
            ),
            None,
        ),
        (
            "valid non-default disabled version",
            with_version(config("gvisor", gvisor_assets()), {
                let mut v = version("v0", gvisor_assets());
                v["state"] = json!("Disabled");
                v
            }),
            None,
        ),
        (
            "defaultVersion not in versions",
            {
                let mut sc = config("gvisor", gvisor_assets());
                sc["spec"]["defaultVersion"] = json!("v9");
                sc
            },
            Some("defaultVersion must name an entry in versions"),
        ),
        (
            "defaultVersion disabled",
            {
                let mut sc = config("gvisor", gvisor_assets());
                sc["spec"]["versions"][0]["state"] = json!("Disabled");
                sc
            },
            Some("must not be Disabled"),
        ),
        (
            "no versions",
            {
                let mut sc = config("gvisor", gvisor_assets());
                sc["spec"]["versions"] = json!([]);
                sc
            },
            Some("spec.versions"),
        ),
        (
            "duplicate version names",
            with_version(
                config("gvisor", gvisor_assets()),
                version("v1", gvisor_assets()),
            ),
            Some("Duplicate value"),
        ),
        (
            "too many versions",
            {
                let mut sc = config("gvisor", gvisor_assets());
                for i in 0..16 {
                    sc = with_version(sc, version(&format!("extra-{i}"), gvisor_assets()));
                }
                sc
            },
            Some("spec.versions"),
        ),
        (
            "version name not a DNS label",
            {
                let mut sc = config("gvisor", gvisor_assets());
                sc["spec"]["versions"][0]["name"] = json!("V_1");
                sc["spec"]["defaultVersion"] = json!("V_1");
                sc
            },
            Some("spec.versions[0].name"),
        ),
        (
            "gvisor second version missing the gvisor asset",
            with_version(
                config("gvisor", gvisor_assets()),
                version("v2", json!({"amd64": {"other": other}})),
            ),
            Some("'gvisor' (release tarball) asset"),
        ),
        (
            "microvm second version missing an asset",
            with_version(
                config("microvm", json!({"amd64": microvm_set(None)})),
                version("v2", json!({"amd64": microvm_set(Some("kata-kernel"))})),
            ),
            Some("microvm SandboxConfig must define"),
        ),
    ];
    for (name, sc, want) in cases {
        expect(validate_sandbox_config(&sc, None), want, name);
    }
}

#[test]
fn sandbox_config_sandbox_class_default() {
    let mut sc = config("gvisor", gvisor_assets());
    sc["spec"].as_object_mut().unwrap().remove("sandboxClass");
    validate_sandbox_config(&sc, None).unwrap();
    default_sandbox_config(&mut sc);
    assert_eq!(sc["spec"]["sandboxClass"], "gvisor");
    let typed: crate::crd::SandboxConfig = serde_json::from_value(sc).unwrap();
    assert_eq!(typed.spec.sandbox_class, crate::crd::SandboxClass::Gvisor);
}

#[test]
fn sandbox_config_update_removing_default_version() {
    let old = with_version(
        config("gvisor", gvisor_assets()),
        version("v2", gvisor_assets()),
    );
    let mut new = old.clone();
    new["spec"]["versions"].as_array_mut().unwrap().remove(0);
    expect(
        validate_sandbox_config(&new, Some(&old)),
        Some("defaultVersion must name an entry in versions"),
        "drop default",
    );
    new["spec"]["defaultVersion"] = json!("v2");
    expect(
        validate_sandbox_config(&new, Some(&old)),
        None,
        "repoint default",
    );
}

const MANIFESTS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/manifests");

#[test]
fn shipped_sandbox_config_manifests() {
    for file in [
        "sandboxconfig-gvisor.yaml",
        "sandboxconfig-microvm.yaml.tmpl",
    ] {
        let raw = std::fs::read_to_string(format!("{MANIFESTS}/{file}")).unwrap();
        let raw = raw
            .replace("${BUCKET_SCHEME}", "s3")
            .replace("${BUCKET_NAME}", "test-bucket")
            .replace("${ASSET_BUCKET_NAME}", "test-bucket");
        let value: Value = serde_yaml::from_str(&raw).unwrap();
        // Decodes strictly into the typed resource.
        let sc: crate::crd::SandboxConfig = serde_json::from_value(value.clone()).unwrap();
        assert!(
            sc.is_class_default(),
            "{file} is not annotated as its class default"
        );
        assert_eq!(
            serde_json::to_value(&sc).unwrap()["spec"],
            {
                let mut v = value.clone();
                default_sandbox_config(&mut v);
                v["spec"].clone()
            },
            "{file} has fields the typed resource drops"
        );
        validate_sandbox_config(&value, None).unwrap_or_else(|e| panic!("{file}: {e}"));
    }
}

#[test]
fn shipped_policy_matches_rules() {
    // The admission policy shipped for clusters states the same per-class
    // requirements this module enforces.
    let raw =
        std::fs::read_to_string(format!("{MANIFESTS}/sandboxconfig-validation.yaml")).unwrap();
    assert!(raw.contains("apiGroups:   [\"hivekeep.io\"]"));
    assert!(raw.contains(GVISOR_ASSETS_MSG));
    assert!(raw.contains(MICROVM_ASSETS_MSG));
    for a in GVISOR_REQUIRED_ASSETS.iter().chain(MICROVM_REQUIRED_ASSETS) {
        assert!(raw.contains(&format!("'{a}'")), "policy lacks {a}");
    }
}
