//! Image references the fixtures run.
//!
//! The suites do not build images: a lane publishes them first and points
//! the harness at them. Each image has its own override variable; otherwise
//! it is `<registry>/<name>:<tag>` from `HK_E2E_IMAGE_REGISTRY` and
//! `HK_E2E_IMAGE_TAG`. The defaults match the installer's unresolved
//! placeholder references, so an unconfigured run fails loudly at image pull.

use crate::sandbox::SandboxClass;

/// Registry the default references live under.
pub const REGISTRY_ENV: &str = "HK_E2E_IMAGE_REGISTRY";
/// Tag of the default references.
pub const TAG_ENV: &str = "HK_E2E_IMAGE_TAG";
/// Registry used when [`REGISTRY_ENV`] is unset.
pub const DEFAULT_REGISTRY: &str = "REGISTRY_PLACEHOLDER";
/// Tag used when [`TAG_ENV`] is unset.
pub const DEFAULT_TAG: &str = "0.1.0";

/// An image a fixture runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Image {
    /// The worker image of a pool of the given class.
    Worker(SandboxClass),
    /// The in-sandbox probe (`hivekeep-e2e-probe`).
    Probe,
    /// The multi-protocol origin (`hivekeep-e2e-testserver`).
    Testserver,
}

impl Image {
    /// The image's repository name.
    pub fn repository(self) -> &'static str {
        match self {
            Image::Worker(SandboxClass::Gvisor) => "hivekeep-sandbox-gvisor",
            Image::Worker(SandboxClass::MicroVm) => "hivekeep-sandbox-microvm",
            Image::Probe => "hivekeep-e2e-probe",
            Image::Testserver => "hivekeep-e2e-testserver",
        }
    }

    /// The variable that overrides this image's whole reference.
    pub fn override_env(self) -> &'static str {
        match self {
            Image::Worker(SandboxClass::Gvisor) => "HK_E2E_WORKER_IMAGE_GVISOR",
            Image::Worker(SandboxClass::MicroVm) => "HK_E2E_WORKER_IMAGE_MICROVM",
            Image::Probe => "HK_E2E_PROBE_IMAGE",
            Image::Testserver => "HK_E2E_TESTSERVER_IMAGE",
        }
    }

    /// The reference to run, from the process environment.
    pub fn reference(self) -> String {
        self.reference_with(|k| std::env::var(k).ok())
    }

    /// The reference to run, from `lookup`.
    pub fn reference_with(self, lookup: impl Fn(&str) -> Option<String>) -> String {
        let get = |k: &str| lookup(k).filter(|v| !v.is_empty());
        if let Some(full) = get(self.override_env()) {
            return full;
        }
        let registry = get(REGISTRY_ENV).unwrap_or_else(|| DEFAULT_REGISTRY.to_string());
        let tag = get(TAG_ENV).unwrap_or_else(|| DEFAULT_TAG.to_string());
        format!(
            "{}/{}:{tag}",
            registry.trim_end_matches('/'),
            self.repository()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn references() {
        let none = |_: &str| None;
        assert_eq!(
            Image::Probe.reference_with(none),
            "REGISTRY_PLACEHOLDER/hivekeep-e2e-probe:0.1.0"
        );
        let env: HashMap<&str, &str> = [(REGISTRY_ENV, "reg.example/hk/"), (TAG_ENV, "dev")].into();
        let lookup = |k: &str| env.get(k).map(|v| v.to_string());
        assert_eq!(
            Image::Worker(SandboxClass::MicroVm).reference_with(lookup),
            "reg.example/hk/hivekeep-sandbox-microvm:dev"
        );
        let env: HashMap<&str, &str> = [("HK_E2E_TESTSERVER_IMAGE", "x/y@sha256:00")].into();
        assert_eq!(
            Image::Testserver.reference_with(|k| env.get(k).map(|v| v.to_string())),
            "x/y@sha256:00"
        );
    }
}
