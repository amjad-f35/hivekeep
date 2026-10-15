//! The sandbox's size, from the Actor's declared limits.

use hivekeep_proto::worker::v1 as pb;

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
        let l = limits.cloned().unwrap_or_default();
        SandboxSize {
            milli_cpu: l.cpu_millicores.max(0),
            memory_bytes: l.memory_bytes.max(0),
        }
    }

    /// The CFS quota for the CPU limit, if one is set.
    pub fn cpu_quota_us(&self) -> Option<i64> {
        (self.milli_cpu > 0).then(|| self.milli_cpu * CPU_QUOTA_PERIOD_US / 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_limits_clamps_negatives() {
        let l = pb::Resources {
            cpu_millicores: 1500,
            memory_bytes: -1,
        };
        let s = SandboxSize::from_limits(Some(&l));
        assert_eq!(
            s,
            SandboxSize {
                milli_cpu: 1500,
                memory_bytes: 0
            }
        );
        assert_eq!(s.cpu_quota_us(), Some(150_000));
        assert_eq!(SandboxSize::from_limits(None), SandboxSize::default());
        assert_eq!(SandboxSize::default().cpu_quota_us(), None);
    }
}
