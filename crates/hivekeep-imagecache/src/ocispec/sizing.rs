//! An actor's sandbox size, from its declared limits as carried on the
//! worker RPCs. The gVisor worker writes it into the sandbox container's
//! spec (runsc applies it to the host cgroup leaf); the micro-VM worker
//! sizes the VM from it instead, and a micro-VM container's own cgroup
//! limit comes from its declared resources.

use hivekeep_proto::worker::v1 as pb;

use super::{Cpu, Linux, Memory, Resources, Spec, CPU_QUOTA_PERIOD_US};

/// The sandbox's target size; zero fields are unset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxSize {
    /// CPU limit in milli-cores.
    pub milli_cpu: i64,
    /// Memory limit in bytes.
    pub memory_bytes: i64,
}

impl SandboxSize {
    /// From declared limits; negative values are unset.
    pub fn from_limits(l: Option<&pb::Resources>) -> Self {
        SandboxSize {
            milli_cpu: l.map_or(0, |l| l.cpu_millicores).max(0),
            memory_bytes: l.map_or(0, |l| l.memory_bytes).max(0),
        }
    }

    /// Whole vCPUs for a micro-VM, rounded up (at least one when set); zero
    /// when unset.
    pub fn vcpus(&self) -> u32 {
        if self.milli_cpu <= 0 {
            return 0;
        }
        ((self.milli_cpu + 999) / 1000).max(1) as u32
    }

    /// Writes the limits into `spec`'s `linux.resources`, leaving unset
    /// fields (and anything else already there) untouched.
    pub fn apply_to_oci_spec(&self, spec: &mut Spec) {
        if self.milli_cpu <= 0 && self.memory_bytes <= 0 {
            return;
        }
        let linux = spec.linux.get_or_insert_with(Linux::default);
        let res = linux.resources.get_or_insert_with(Resources::default);
        if self.milli_cpu > 0 {
            let cpu = res.cpu.get_or_insert_with(Cpu::default);
            cpu.period = Some(CPU_QUOTA_PERIOD_US as u64);
            cpu.quota = Some(self.milli_cpu * CPU_QUOTA_PERIOD_US / 1000);
        }
        if self.memory_bytes > 0 {
            res.memory.get_or_insert_with(Memory::default).limit = Some(self.memory_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizing_rules() {
        let s = SandboxSize::from_limits(Some(&pb::Resources {
            cpu_millicores: 1500,
            memory_bytes: -1,
        }));
        assert_eq!(
            s,
            SandboxSize {
                milli_cpu: 1500,
                memory_bytes: 0
            }
        );
        assert_eq!(s.vcpus(), 2);
        assert_eq!(SandboxSize::default().vcpus(), 0);
        let mut spec = Spec::default();
        SandboxSize::default().apply_to_oci_spec(&mut spec);
        assert!(spec.linux.is_none());
        s.apply_to_oci_spec(&mut spec);
        let cpu = spec.linux.unwrap().resources.unwrap().cpu.unwrap();
        assert_eq!((cpu.quota, cpu.period), (Some(150_000), Some(100_000)));
    }
}
