//! The sandbox runtime family shared by WorkerPools and SandboxConfigs.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Selects a sandbox runtime family.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum SandboxClass {
    /// The gVisor (`runsc`) runtime. The default.
    #[default]
    Gvisor,
    /// The micro-VM runtime; needs `/dev/kvm` and vhost devices.
    Microvm,
}

impl SandboxClass {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            SandboxClass::Gvisor => hivekeep_core::sandbox_class::GVISOR,
            SandboxClass::Microvm => hivekeep_core::sandbox_class::MICROVM,
        }
    }

    /// Parses a wire name.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            hivekeep_core::sandbox_class::GVISOR => Some(SandboxClass::Gvisor),
            hivekeep_core::sandbox_class::MICROVM => Some(SandboxClass::Microvm),
            _ => None,
        }
    }
}

impl std::fmt::Display for SandboxClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names() {
        for c in [SandboxClass::Gvisor, SandboxClass::Microvm] {
            assert_eq!(SandboxClass::parse(c.as_str()), Some(c));
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
        }
        assert_eq!(SandboxClass::parse("kata"), None);
    }
}
