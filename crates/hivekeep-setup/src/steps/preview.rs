//! Passing the configured preview gates to every Hivekeep component.

use super::yamldoc::{edit_documents, for_each_container_mut};
use super::Result;
use crate::images::REGISTRY_PLACEHOLDER;
use serde_json::Value;

/// The flag every Hivekeep component binary accepts.
pub const PREVIEW_FLAG: &str = "--preview";

/// Whether a container image is a Hivekeep component (which accepts
/// `--preview`), as the checked-in manifests name it before resolution. The
/// demo workloads are not components.
pub fn is_component_image(image: &str) -> bool {
    match image
        .strip_prefix(REGISTRY_PLACEHOLDER)
        .and_then(|r| r.strip_prefix("/hivekeep-"))
    {
        Some(rest) => !rest.starts_with("demo-"),
        None => false,
    }
}

/// The `--preview` argument for the enabled gates.
pub fn preview_arg(enabled: &[String]) -> String {
    format!("{PREVIEW_FLAG}={}", enabled.join(","))
}

/// Sets `--preview` on every Hivekeep container in the manifest, replacing
/// any `--preview` it already carries. Must run before image resolution,
/// which rewrites the references it keys on. Documents without a component
/// container are left byte for byte.
pub fn inject_preview_arg(manifest: &[u8], gates: &[String]) -> Result<Vec<u8>> {
    let marker = format!("{REGISTRY_PLACEHOLDER}/hivekeep-");
    if !String::from_utf8_lossy(manifest).contains(&marker) {
        return Ok(manifest.to_vec());
    }
    let arg = preview_arg(gates);
    edit_documents(manifest, |obj| {
        Ok(for_each_container_mut(obj, |c| set_preview_arg(c, &arg)))
    })
}

fn set_preview_arg(container: &mut Value, arg: &str) -> bool {
    let image = container.get("image").and_then(Value::as_str).unwrap_or("");
    if !is_component_image(image) {
        return false;
    }
    let prefix = format!("{PREVIEW_FLAG}=");
    let mut args: Vec<Value> = container
        .get("args")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|a| !a.as_str().is_some_and(|s| s.starts_with(&prefix)))
        .collect();
    args.push(Value::String(arg.to_string()));
    container["args"] = Value::Array(args);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{self, test_config, ROUTER_ENVOY};
    use crate::images::{ImageError, ImageResolver};
    use crate::steps::yamldoc::{args_of, decode_all, CONTAINER_LIST_PATHS};
    use crate::steps::Env;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Maps `<kind>/<name>/<container>` to the container's args, with
    /// `hivekeep:` before the key of a component container.
    pub(crate) fn container_args(manifest: &[u8]) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        for doc in decode_all(manifest).unwrap() {
            let kind = doc
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let name = doc
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            for path in CONTAINER_LIST_PATHS {
                let Some(list) = doc
                    .pointer(&format!("/{}", path.join("/")))
                    .and_then(Value::as_array)
                else {
                    continue;
                };
                for c in list {
                    let cname = c.get("name").and_then(Value::as_str).unwrap_or("");
                    let image = c.get("image").and_then(Value::as_str).unwrap_or("");
                    let mut key = format!("{kind}/{name}/{cname}");
                    if is_component_image(image) {
                        key = format!("hivekeep:{key}");
                    }
                    out.insert(key, args_of(c));
                }
            }
        }
        out
    }

    const PREVIEW_TEST_MANIFEST: &str = "# leading comment
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
data:
  image: REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      initContainers:
      - name: sidecar
        image: REGISTRY_PLACEHOLDER/hivekeep-gate:0.1.0
        restartPolicy: Always
        args:
        - \"sdsmint\"
      containers:
      - name: api
        image: REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0
        args:
        - --port=8080  # keep me
        - --preview=Stale
      - name: bare
        image: REGISTRY_PLACEHOLDER/hivekeep-keeper:0.1.0
      - name: envoy
        image: ${ENVOY_DATAPLANE_IMAGE}
        args:
        - -c
      - name: demo
        image: REGISTRY_PLACEHOLDER/hivekeep-demo-counter:0.1.0
---
apiVersion: v1
kind: Pod
metadata:
  name: pod
spec:
  containers:
  - name: ctl
    image: REGISTRY_PLACEHOLDER/hivekeep-controller:0.1.0
";

    #[test]
    fn inject_preview_arg_cases() {
        for (enabled, arg) in [
            (vec!["ALL".to_string()], "--preview=ALL"),
            (vec![], "--preview="),
        ] {
            let out = inject_preview_arg(PREVIEW_TEST_MANIFEST.as_bytes(), &enabled).unwrap();
            let got = container_args(&out);
            let want: BTreeMap<String, Vec<String>> = [
                ("hivekeep:Deployment/api/sidecar", vec!["sdsmint", arg]),
                ("hivekeep:Deployment/api/api", vec!["--port=8080", arg]),
                ("hivekeep:Deployment/api/bare", vec![arg]),
                ("Deployment/api/envoy", vec!["-c"]),
                ("Deployment/api/demo", vec![]),
                ("hivekeep:Pod/pod/ctl", vec![arg]),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into_iter().map(str::to_string).collect()))
            .collect();
            assert_eq!(got, want, "{arg}");
            // Documents without a component container survive byte for
            // byte, comments included; placeholders survive everywhere.
            let text = String::from_utf8(out).unwrap();
            for s in [
                "# leading comment",
                "${ENVOY_DATAPLANE_IMAGE}",
                "kind: ConfigMap",
                "image: REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0\n---",
            ] {
                assert!(text.contains(s), "output lost {s:?}:\n{text}");
            }
        }
    }

    #[test]
    fn inject_preview_arg_leaves_other_manifests_alone() {
        let input = b"apiVersion: v1\nkind: Pod\nmetadata:   {name: x}\nspec:\n  containers:\n  - {name: c, image: busybox}\n";
        for enabled in [vec!["ALL".to_string()], vec![]] {
            assert_eq!(inject_preview_arg(input, &enabled).unwrap(), input.to_vec());
        }
    }

    /// Records the manifest it is asked to resolve and returns it unchanged.
    #[derive(Default)]
    struct RecordingResolver(Mutex<Vec<u8>>);

    #[async_trait]
    impl ImageResolver for RecordingResolver {
        async fn resolve_bytes(&self, manifest: &[u8]) -> std::result::Result<Vec<u8>, ImageError> {
            *self.0.lock() = manifest.to_vec();
            Ok(manifest.to_vec())
        }
    }

    /// Strips every `--preview=` argument, and any args list left empty, from
    /// decoded YAML.
    fn without_preview(v: Value) -> Value {
        match v {
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .filter(|i| !i.as_str().is_some_and(|s| s.starts_with("--preview=")))
                    .map(without_preview)
                    .collect(),
            ),
            Value::Object(m) => {
                let mut out: serde_json::Map<String, Value> = m
                    .into_iter()
                    .map(|(k, v)| (k, without_preview(v)))
                    .collect();
                if out
                    .get("args")
                    .and_then(Value::as_array)
                    .is_some_and(|a| a.is_empty())
                {
                    out.remove("args");
                }
                Value::Object(out)
            }
            other => other,
        }
    }

    #[tokio::test]
    async fn resolve_manifest_injects_preview() {
        if !crate::kustomize::available() {
            eprintln!("skipping: no kustomize renderer");
            return;
        }
        type PathFn = fn(&Env) -> PathBuf;
        struct Source {
            name: &'static str,
            cfg: fn(&mut config::Config),
            path: PathFn,
            direct: bool,
            want: &'static [&'static str],
        }
        let sources = [
            Source {
                name: "base bundle",
                cfg: |c| c.router = ROUTER_ENVOY.into(),
                path: |e| e.system_overlay_path(),
                direct: false,
                want: &[
                    "hivekeep-api",
                    "hivekeep-controller",
                    "gate-router",
                    "keeper",
                ],
            },
            Source {
                name: "kind bundle",
                cfg: |c| {
                    c.router = ROUTER_ENVOY.into();
                    c.kind = true;
                },
                path: |e| e.system_overlay_path(),
                direct: false,
                want: &[
                    "hivekeep-api",
                    "hivekeep-controller",
                    "gate-router",
                    "keeper",
                ],
            },
            Source {
                name: "keeper file",
                cfg: |_| {},
                path: |e| e.cfg.manifest(&["keeper.yaml"]),
                direct: true,
                want: &["keeper"],
            },
            Source {
                name: "podcert file",
                cfg: |_| {},
                path: |e| e.cfg.manifest(&["pod-certificate-controller.yaml"]),
                direct: false,
                want: &["podcertificate-controller"],
            },
            Source {
                name: "podcert size10 overlay",
                cfg: |c| c.cluster_size = config::CLUSTER_SIZE_SIZE10.into(),
                path: |e| e.cfg.manifest(&["podcert-size10"]),
                direct: false,
                want: &["podcertificate-controller"],
            },
            Source {
                name: "router file",
                cfg: |_| {},
                path: |e| e.cfg.manifest(&["gate-router.yaml"]),
                direct: false,
                want: &["gate-router"],
            },
            Source {
                name: "egress file",
                cfg: |_| {},
                path: |e| e.gate_egress_manifest_path(),
                direct: false,
                want: &["gate-egress"],
            },
            Source {
                name: "credential provider file",
                cfg: |_| {},
                path: |e| {
                    e.cfg.manifests_path(&[
                        "egress-credential-injection",
                        "k8s-credential-provider.yaml",
                    ])
                },
                direct: false,
                want: &["k8s-credential-provider"],
            },
        ];
        for src in &sources {
            for enabled in [vec!["ALL".to_string()], vec![]] {
                let arg = preview_arg(&enabled);
                let mut cfg = test_config();
                (src.cfg)(&mut cfg);
                cfg.preview_gates = enabled.clone();
                let resolver = Arc::new(RecordingResolver::default());
                let e = Env::local(cfg).with_resolver(resolver.clone());
                let path = (src.path)(&e);
                if src.direct {
                    e.resolve_manifest(&path).await.unwrap();
                } else {
                    e.render_resolve(&path).await.unwrap();
                }
                let original = if src.direct {
                    crate::kube::read_path(&path).unwrap()
                } else {
                    e.render(&path).unwrap()
                };
                let got = resolver.0.lock().clone();
                let before = decode_all(&original).unwrap();
                let after: Vec<Value> = decode_all(&got)
                    .unwrap()
                    .into_iter()
                    .map(without_preview)
                    .collect();
                assert_eq!(
                    before, after,
                    "{} {arg}: injection changed more than --preview",
                    src.name
                );

                let mut found = std::collections::BTreeSet::new();
                for (key, args) in container_args(&got) {
                    let previews = args.iter().filter(|a| a.starts_with(PREVIEW_FLAG)).count();
                    if !key.starts_with("hivekeep:") {
                        assert_eq!(
                            previews, 0,
                            "{} {arg}: {key} is not a component but got {args:?}",
                            src.name
                        );
                        continue;
                    }
                    assert!(
                        previews == 1 && args.contains(&arg),
                        "{} {key} args = {args:?}, want one {arg}",
                        src.name
                    );
                    found.insert(key.split('/').nth(1).unwrap().to_string());
                }
                for name in src.want {
                    assert!(
                        found.contains(*name),
                        "{} {arg}: no component container in {name} (found {found:?})",
                        src.name
                    );
                }
            }
        }
    }
}
