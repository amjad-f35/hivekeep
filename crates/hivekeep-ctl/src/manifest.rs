//! Manifests: one resource in protobuf JSON form, written as YAML or JSON,
//! parsed strictly so that a misspelt field is an error rather than
//! silently dropped configuration.

use crate::protojson::{from_value, ProtoMessage};
use crate::yaml::manifest_to_json;

/// Parses a manifest holding one `M`. `kind` names it in errors.
pub fn parse<M: ProtoMessage>(data: &str, kind: &str) -> Result<M, String> {
    let json = manifest_to_json(data).map_err(|e| e.to_string())?;
    from_value(json).map_err(|e| format!("invalid {kind}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_proto::api::v1 as pb;

    const TEMPLATE: &str = r#"
metadata:
  hive: demo
  name: counter
containers:
- name: counter
  image: registry.example/counter@sha256:0000000000000000000000000000000000000000000000000000000000000000
  env:
  - name: MODE
    value: fast
resources:
  limits:
    cpuMillicores: "500"
    memoryBytes: 268435456
snapshotConfig:
  goldenSnapshotConfig:
    mode: GOLDEN_SNAPSHOT_MODE_DISABLED
"#;

    #[test]
    fn actor_template() {
        let t: pb::ActorTemplate = parse(TEMPLATE, "ActorTemplate").unwrap();
        let m = t.metadata.unwrap();
        assert_eq!((m.hive.as_str(), m.name.as_str()), ("demo", "counter"));
        assert_eq!(t.containers.len(), 1);
        assert_eq!(t.containers[0].env[0].value, "fast");
        let limits = t.resources.unwrap().limits.unwrap();
        assert_eq!(limits.cpu_millicores, 500);
        assert_eq!(limits.memory_bytes, 256 << 20);
        assert_eq!(
            t.snapshot_config
                .unwrap()
                .golden_snapshot_config
                .unwrap()
                .mode,
            pb::GoldenSnapshotMode::Disabled as i32
        );
    }

    #[test]
    fn snake_case_field_names() {
        let t: pb::ActorTemplate = parse(
            "metadata: {hive: demo, name: c}\nworker_selector:\n  match_labels:\n    pool: fast\n",
            "ActorTemplate",
        )
        .unwrap();
        assert_eq!(t.worker_selector.unwrap().match_labels["pool"], "fast");
    }

    #[test]
    fn json_manifest() {
        let t: pb::ActorTemplate = parse(
            r#"{"metadata": {"hive": "demo", "name": "c"}}"#,
            "ActorTemplate",
        )
        .unwrap();
        assert_eq!(t.metadata.unwrap().name, "c");
    }

    #[test]
    fn errors() {
        for (data, want) in [
            ("", "manifest is empty"),
            ("# nothing\n", "manifest is empty"),
            (
                "metadata: {name: a}\n---\nmetadata: {name: b}\n",
                "more than one document",
            ),
            ("metadata: [", "invalid YAML"),
            ("metadata: {name: a}\nbogus: 1\n", "bogus"),
            ("containers: 3\n", "invalid ActorTemplate"),
            (
                "snapshotConfig: {goldenSnapshotConfig: {mode: NOT_A_MODE}}\n",
                "NOT_A_MODE",
            ),
        ] {
            let err = parse::<pb::ActorTemplate>(data, "ActorTemplate").unwrap_err();
            assert!(err.contains(want), "{data:?}: {err}");
        }
    }
}
