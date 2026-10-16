//! Container Device Interface specs: the edits a device generator asks a
//! runtime to apply to containers that use its devices.

use serde::Deserialize;

/// The part of a CDI spec consumed here.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// The addressable devices.
    #[serde(default)]
    pub devices: Vec<Device>,
    /// Edits applied to any container the spec touches, whichever devices
    /// were named (driver libraries, control nodes, env).
    #[serde(default)]
    pub container_edits: Edits,
}

/// One addressable device. Generators name the same hardware several ways
/// (by index, by UUID, and an `all` device repeating every node).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    /// The device name.
    pub name: String,
    /// What it changes in a container.
    #[serde(default)]
    pub container_edits: Edits,
}

/// The changes a device makes to a container.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edits {
    /// Environment entries.
    #[serde(default)]
    pub env: Vec<String>,
    /// Device nodes to create.
    #[serde(default)]
    pub device_nodes: Vec<DeviceNode>,
    /// Host paths to mount.
    #[serde(default)]
    pub mounts: Vec<Mount>,
    /// Lifecycle hooks.
    #[serde(default)]
    pub hooks: Vec<Hook>,
}

/// A device node. Major and minor are often absent: CDI leaves resolving them
/// to the runtime, which stats the host.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct DeviceNode {
    /// Path in the container.
    pub path: String,
    /// Node type.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Major number.
    #[serde(default)]
    pub major: i64,
    /// Minor number.
    #[serde(default)]
    pub minor: i64,
}

/// A host path mounted into the container.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mount {
    /// Host path.
    pub host_path: String,
    /// Container path.
    pub container_path: String,
    /// Mount type.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Mount options.
    #[serde(default)]
    pub options: Vec<String>,
}

/// A hook the spec asks the runtime to run.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hook {
    /// The lifecycle point.
    pub hook_name: String,
    /// Executable.
    pub path: String,
    /// Arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment.
    #[serde(default)]
    pub env: Vec<String>,
}

/// Parses a CDI spec in JSON form.
pub fn parse(data: &[u8]) -> Result<Spec, String> {
    serde_json::from_slice(data).map_err(|e| format!("parsing CDI spec: {e}"))
}

impl Spec {
    /// The spec-level edits plus those of each named device, in order. A name
    /// the spec does not carry is an error: the caller and the generator
    /// disagree about what the host has. Aliases of one device repeat its
    /// nodes rather than being collapsed.
    pub fn edits_for<S: AsRef<str>>(&self, devices: &[S]) -> Result<Edits, String> {
        let mut e = self.container_edits.clone();
        for want in devices {
            let want = want.as_ref();
            let d = self
                .devices
                .iter()
                .find(|d| d.name == want)
                .ok_or_else(|| format!("CDI spec has no {want:?} device"))?;
            let d = &d.container_edits;
            e.env.extend(d.env.iter().cloned());
            e.device_nodes.extend(d.device_nodes.iter().cloned());
            e.mounts.extend(d.mounts.iter().cloned());
            e.hooks.extend(d.hooks.iter().cloned());
        }
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_GPU_SPEC: &str = r#"{
      "cdiVersion": "0.6.0",
      "kind": "nvidia.com/gpu",
      "containerEdits": {"env": ["COMMON=1"], "deviceNodes": [{"path": "/dev/nvidiactl"}]},
      "devices": [
        {"name": "all", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}, {"path": "/dev/nvidia1"}]}},
        {"name": "0", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}]}},
        {"name": "1", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia1"}]}},
        {"name": "GPU-abcdef", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}]}}
      ]
    }"#;

    fn paths(e: &Edits) -> Vec<&str> {
        e.device_nodes.iter().map(|n| n.path.as_str()).collect()
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse(b"not json").is_err());
    }

    #[test]
    fn edits_for() {
        let spec = parse(TWO_GPU_SPEC.as_bytes()).unwrap();
        let none: [&str; 0] = [];
        for (name, devices, want) in [
            ("no devices", &none[..], vec!["/dev/nvidiactl"]),
            (
                "one device",
                &["1"][..],
                vec!["/dev/nvidiactl", "/dev/nvidia1"],
            ),
            (
                "several devices",
                &["0", "1"][..],
                vec!["/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia1"],
            ),
            (
                "aliases of one device repeat its nodes",
                &["0", "GPU-abcdef"][..],
                vec!["/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia0"],
            ),
        ] {
            assert_eq!(paths(&spec.edits_for(devices).unwrap()), want, "{name}");
        }
        assert!(spec.edits_for(&["7"]).is_err());
    }

    #[test]
    fn edits_for_does_not_accumulate() {
        let spec = parse(TWO_GPU_SPEC.as_bytes()).unwrap();
        spec.edits_for(&["0"]).unwrap();
        assert_eq!(
            paths(&spec.edits_for(&["1"]).unwrap()),
            ["/dev/nvidiactl", "/dev/nvidia1"]
        );
    }
}
