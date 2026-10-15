//! Container Device Interface specs: reading one, and resolving what a named
//! set of its devices asks a container runtime to do.
//!
//! containerd applies CDI to the pod's containers, but an Actor's containers
//! are created by the worker underneath one of them, so the worker reads the
//! spec itself. Applying the edits is sandbox specific; see
//! [`crate::cdi_inject`] for gVisor.

use serde::Deserialize;

use crate::error::{Error, Result};

/// The part of a CDI spec consumed here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// The addressable devices.
    #[serde(default)]
    pub devices: Vec<Device>,
    /// Edits applied to any container the spec touches, whichever devices
    /// were named: driver libraries, control nodes, env.
    #[serde(default)]
    pub container_edits: Edits,
}

/// One addressable device. Generators name the same hardware several ways
/// (per index, per UUID, "all"), so applying every device of a spec would
/// inject each node several times over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    /// The device's name within the spec.
    pub name: String,
    /// What it changes in a container.
    #[serde(default)]
    pub container_edits: Edits,
}

/// The changes a device makes to a container.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edits {
    /// `KEY=value` entries to add.
    #[serde(default)]
    pub env: Vec<String>,
    /// Device nodes to create.
    #[serde(default)]
    pub device_nodes: Vec<DeviceNode>,
    /// Host paths to mount.
    #[serde(default)]
    pub mounts: Vec<Mount>,
    /// Lifecycle hooks to run.
    #[serde(default)]
    pub hooks: Vec<Hook>,
}

/// A device node to create in the container. The numbers are often left
/// out: CDI leaves them to the runtime, which stats the host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DeviceNode {
    /// The node's path, the same on the host and in the container.
    pub path: String,
    /// `c` or `b`; empty for a char device.
    #[serde(default, rename = "type")]
    pub kind: String,
    /// The major number, 0 when unset.
    #[serde(default)]
    pub major: i64,
    /// The minor number.
    #[serde(default)]
    pub minor: i64,
}

/// A host path to mount into the container.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mount {
    /// The source on the host.
    pub host_path: String,
    /// The destination in the container.
    pub container_path: String,
    /// The mount type; empty for a bind mount.
    #[serde(default, rename = "type")]
    pub kind: String,
    /// Mount options.
    #[serde(default)]
    pub options: Vec<String>,
}

/// A lifecycle hook the spec asks the runtime to run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hook {
    /// The OCI hook point, e.g. `createContainer`.
    pub hook_name: String,
    /// The binary.
    pub path: String,
    /// argv, including argv\[0\].
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment.
    #[serde(default)]
    pub env: Vec<String>,
}

/// Reads a CDI spec in JSON form.
pub fn parse(data: &[u8]) -> Result<Spec> {
    serde_json::from_slice(data).map_err(|e| Error::msg(format!("parsing CDI spec: {e}")))
}

impl Spec {
    /// The spec-level edits plus those of the named devices, in order. A name
    /// the spec does not carry is an error: the caller and the generator
    /// disagree about what the host has.
    pub fn edits_for(&self, devices: &[String]) -> Result<Edits> {
        let mut edits = self.container_edits.clone();
        for want in devices {
            let d = self
                .devices
                .iter()
                .find(|d| &d.name == want)
                .ok_or_else(|| Error::msg(format!("CDI spec has no {want:?} device")))?;
            let d = &d.container_edits;
            edits.env.extend(d.env.iter().cloned());
            edits.device_nodes.extend(d.device_nodes.iter().cloned());
            edits.mounts.extend(d.mounts.iter().cloned());
            edits.hooks.extend(d.hooks.iter().cloned());
        }
        Ok(edits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shaped like nvidia-ctk's: an "all" device carrying both nodes, one
    /// per index, and a UUID alias for the same hardware.
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

    fn spec() -> Spec {
        parse(TWO_GPU_SPEC.as_bytes()).unwrap()
    }

    fn node_paths(e: &Edits) -> Vec<&str> {
        e.device_nodes.iter().map(|n| n.path.as_str()).collect()
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse(b"not json").is_err());
    }

    #[test]
    fn edits_for() {
        type Case<'a> = (&'a str, &'a [&'a str], Option<&'a [&'a str]>);
        let cases: [Case; 5] = [
            // Spec-level edits apply whether or not a device was named.
            ("no devices", &[], Some(&["/dev/nvidiactl"])),
            (
                "one device",
                &["1"],
                Some(&["/dev/nvidiactl", "/dev/nvidia1"]),
            ),
            (
                "several devices",
                &["0", "1"],
                Some(&["/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia1"]),
            ),
            // Naming two aliases is the caller's error; the duplicate stays
            // visible rather than quietly collapsed.
            (
                "aliases of one device repeat its nodes",
                &["0", "GPU-abcdef"],
                Some(&["/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia0"]),
            ),
            ("unknown device", &["7"], None),
        ];
        for (name, devices, want) in cases {
            let got = spec().edits_for(&names(devices));
            match want {
                Some(want) => assert_eq!(node_paths(&got.unwrap()), want, "{name}"),
                None => assert!(got.is_err(), "{name}"),
            }
        }
    }

    /// The spec-level edits start every call, so one call's devices must not
    /// leak into the next.
    #[test]
    fn edits_for_does_not_accumulate() {
        let s = spec();
        s.edits_for(&names(&["0"])).unwrap();
        let got = s.edits_for(&names(&["1"])).unwrap();
        assert_eq!(node_paths(&got), ["/dev/nvidiactl", "/dev/nvidia1"]);
    }
}
