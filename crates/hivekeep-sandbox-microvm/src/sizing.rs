//! The sandbox's size, from the Actor's declared limits, and what it means
//! for a micro-VM: whole vCPUs and the OCI resource block.

use hivekeep_proto::worker::v1 as pb;

use crate::guest_spec;

/// The cgroup v2 `cpu.max` period (100ms) the CPU quota is expressed against.
pub const CPU_QUOTA_PERIOD_US: i64 = 100_000;

/// A sandbox's target size. Zero means unset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxSize {
    /// CPU limit in millicores.
    pub milli_cpu: i64,
    /// Memory limit in bytes.
    pub memory_bytes: i64,
}

impl SandboxSize {
    /// The size from an Actor's declared limits; negative values are unset.
    pub fn from_limits(limits: Option<&pb::Resources>) -> Self {
        let l = limits.copied().unwrap_or_default();
        SandboxSize {
            milli_cpu: l.cpu_millicores.max(0),
            memory_bytes: l.memory_bytes.max(0),
        }
    }

    /// The CPU limit as whole vCPUs, rounded up so a fractional limit still
    /// gets a core; 0 when unset.
    pub fn vcpus(&self) -> u32 {
        if self.milli_cpu <= 0 {
            return 0;
        }
        (self.milli_cpu.saturating_add(999) / 1000).clamp(1, u32::MAX as i64) as u32
    }

    /// Writes the limits into the spec's resources, leaving unset ones (and
    /// every other existing value) alone.
    pub fn apply_to_oci_spec(&self, spec: &mut guest_spec::Spec) {
        if self.milli_cpu <= 0 && self.memory_bytes <= 0 {
            return;
        }
        let r = spec
            .linux
            .get_or_insert_with(Default::default)
            .resources
            .get_or_insert_with(Default::default);
        if self.milli_cpu > 0 {
            let cpu = r.cpu.get_or_insert_with(Default::default);
            cpu.period = Some(CPU_QUOTA_PERIOD_US as u64);
            cpu.quota = Some(self.milli_cpu * CPU_QUOTA_PERIOD_US / 1000);
        }
        if self.memory_bytes > 0 {
            r.memory.get_or_insert_with(Default::default).limit = Some(self.memory_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest_spec::{Cpu, Linux, Resources, Spec};

    #[test]
    fn from_limits() {
        let s = SandboxSize::from_limits(Some(&pb::Resources {
            cpu_millicores: 1500,
            memory_bytes: 2147483648,
        }));
        assert_eq!((s.milli_cpu, s.memory_bytes), (1500, 2147483648));
    }

    #[test]
    fn from_limits_clamps_negative() {
        let s = SandboxSize::from_limits(Some(&pb::Resources {
            cpu_millicores: -5,
            memory_bytes: -1,
        }));
        assert_eq!(s, SandboxSize::default());
        assert_eq!(SandboxSize::from_limits(None), SandboxSize::default());
    }

    #[test]
    fn vcpus_round_up() {
        for (milli, want) in [
            (0, 0),
            (1, 1),
            (999, 1),
            (1000, 1),
            (1001, 2),
            (2500, 3),
            (4000, 4),
        ] {
            let s = SandboxSize {
                milli_cpu: milli,
                ..Default::default()
            };
            assert_eq!(s.vcpus(), want, "{milli}");
        }
    }

    #[test]
    fn applies_to_the_oci_spec() {
        let mut spec = Spec::default();
        SandboxSize {
            milli_cpu: 2000,
            memory_bytes: 1073741824,
        }
        .apply_to_oci_spec(&mut spec);
        let r = spec.linux.unwrap().resources.unwrap();
        let cpu = r.cpu.unwrap();
        assert_eq!(cpu.period, Some(CPU_QUOTA_PERIOD_US as u64));
        assert_eq!(cpu.quota, Some(2 * CPU_QUOTA_PERIOD_US));
        assert_eq!(r.memory.unwrap().limit, Some(1073741824));
    }

    #[test]
    fn preserves_existing_values_and_skips_unset() {
        let mut spec = Spec {
            linux: Some(Linux {
                resources: Some(Resources {
                    cpu: Some(Cpu {
                        shares: Some(1024),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        SandboxSize {
            memory_bytes: 512,
            ..Default::default()
        }
        .apply_to_oci_spec(&mut spec);
        let r = spec.linux.unwrap().resources.unwrap();
        let cpu = r.cpu.unwrap();
        assert_eq!(cpu.shares, Some(1024));
        assert_eq!(cpu.quota, None);
        assert_eq!(r.memory.unwrap().limit, Some(512));
    }

    #[test]
    fn empty_size_leaves_the_spec_alone() {
        let mut spec = Spec::default();
        SandboxSize::default().apply_to_oci_spec(&mut spec);
        assert!(spec.linux.is_none());
    }
}
