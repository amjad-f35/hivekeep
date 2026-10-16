//! The ceiling an Actor's container limits must fit inside: the guest is
//! sized from the Actor's own limits, so a container limit above it can never
//! bind and would fail somewhere that points nowhere useful.

use crate::guest_spec::Cpu;
use crate::kata::specconv::DEFAULT_CPU_PERIOD_US;

/// The guest's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestEnvelope {
    /// Guest RAM, MiB.
    pub mem_mib: u64,
    /// vCPUs.
    pub vcpus: u32,
}

const MEMORY_REMEDY: &str = "raise spec.resources.limits.memory or lower the container limits";
const CPU_REMEDY: &str = "raise spec.resources.limits.cpu or lower the container limits";

/// One container's limits, by name.
pub struct ContainerLimits<'a> {
    /// The container.
    pub name: &'a str,
    /// Its memory limit; non-positive is unlimited.
    pub memory: Option<i64>,
    /// Its CPU limit.
    pub cpu: Option<&'a Cpu>,
}

/// Refuses limits the guest can never satisfy, summed across the containers
/// since they share one guest. The errors are INVALID_ARGUMENT: the template
/// is immutable, so a retry cannot succeed and it must not read as a fault.
pub fn check_resource_envelope(
    ctrs: &[ContainerLimits<'_>],
    env: GuestEnvelope,
) -> Result<(), tonic::Status> {
    let guest_bytes = (env.mem_mib as i64) * 1024 * 1024;
    let guest_millis = env.vcpus as i64 * 1000;
    let (mut total_bytes, mut total_millis) = (0i64, 0i64);
    for c in ctrs {
        // Non-positive is unlimited, not a claim: summing it would let a
        // negative offset another container's limit.
        if let Some(limit) = c.memory.filter(|l| *l > 0) {
            if limit > guest_bytes {
                return Err(tonic::Status::invalid_argument(format!(
                    "container {:?} asks for {limit} bytes of memory but the guest has {} MiB; {MEMORY_REMEDY}",
                    c.name, env.mem_mib
                )));
            }
            total_bytes = total_bytes.saturating_add(limit);
        }
        let millis = cpu_limit_millis(c.name, c.cpu)?;
        if millis > guest_millis {
            return Err(tonic::Status::invalid_argument(format!(
                "container {:?} asks for {millis}m CPU but the guest has {} vCPU; {CPU_REMEDY}",
                c.name, env.vcpus
            )));
        }
        total_millis = total_millis.saturating_add(millis);
    }
    if total_bytes > guest_bytes {
        return Err(tonic::Status::invalid_argument(format!(
            "the actor's containers ask for {total_bytes} bytes of memory in total but the guest has {} MiB; {MEMORY_REMEDY}",
            env.mem_mib
        )));
    }
    if total_millis > guest_millis {
        return Err(tonic::Status::invalid_argument(format!(
            "the actor's containers ask for {total_millis}m CPU in total but the guest has {} vCPU; {CPU_REMEDY}",
            env.vcpus
        )));
    }
    Ok(())
}

/// A container's CFS quota as millicores. A quota without a period is read
/// against the default period rather than skipped, so it cannot slip past.
pub fn cpu_limit_millis(name: &str, cpu: Option<&Cpu>) -> Result<i64, tonic::Status> {
    let Some(quota) = cpu.and_then(|c| c.quota).filter(|q| *q > 0) else {
        return Ok(0);
    };
    let mut period = DEFAULT_CPU_PERIOD_US as i64;
    if let Some(p) = cpu.and_then(|c| c.period).filter(|p| *p > 0) {
        period = i64::try_from(p).map_err(|_| {
            tonic::Status::invalid_argument(format!(
                "container {name:?} has a cpu period of {p}, which is out of range"
            ))
        })?;
    }
    if quota > i64::MAX / 1000 {
        return Err(tonic::Status::invalid_argument(format!(
            "container {name:?} has a cpu quota of {quota}, which is out of range"
        )));
    }
    Ok(quota * 1000 / period)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: i64 = 1024 * 1024;

    struct Owned {
        name: String,
        memory: Option<i64>,
        cpu: Option<Cpu>,
    }

    fn mem(name: &str, bytes: i64) -> Owned {
        Owned {
            name: name.into(),
            memory: Some(bytes),
            cpu: None,
        }
    }

    fn cpu(name: &str, quota: i64, period: u64) -> Owned {
        Owned {
            name: name.into(),
            memory: None,
            cpu: Some(Cpu {
                shares: None,
                quota: Some(quota),
                period: (period > 0).then_some(period),
            }),
        }
    }

    fn check(ctrs: &[Owned], env: GuestEnvelope) -> Result<(), tonic::Status> {
        let l: Vec<ContainerLimits<'_>> = ctrs
            .iter()
            .map(|c| ContainerLimits {
                name: &c.name,
                memory: c.memory,
                cpu: c.cpu.as_ref(),
            })
            .collect();
        check_resource_envelope(&l, env)
    }

    #[test]
    fn checks_the_envelope() {
        let env = GuestEnvelope {
            mem_mib: 2048,
            vcpus: 1,
        };
        for (name, ctrs, want) in [
            ("within the envelope", vec![mem("ok", 64 * MIB)], None),
            (
                "memory over the guest",
                vec![mem("toobig", 4096 * MIB)],
                Some("toobig"),
            ),
            (
                "memory equal to the whole guest is allowed",
                vec![mem("exact", 2048 * MIB)],
                None,
            ),
            (
                "an unlimited sibling cannot offset limits that overrun",
                vec![
                    mem("a", 1536 * MIB),
                    mem("b", 1024 * MIB),
                    mem("unlimited", -1536 * MIB),
                ],
                Some("in total"),
            ),
            (
                "limits that fit alone but not together",
                vec![mem("a", 1536 * MIB), mem("b", 1024 * MIB)],
                Some("in total"),
            ),
            (
                "cpu over the guest",
                vec![cpu("toofast", 400000, 100000)],
                Some("toofast"),
            ),
            (
                "cpu summed over the guest",
                vec![cpu("a", 60000, 100000), cpu("b", 60000, 100000)],
                Some("in total"),
            ),
            (
                "cpu quota with no period is still checked",
                vec![cpu("noperiod", 400000, 0)],
                Some("noperiod"),
            ),
            (
                "quota large enough to overflow the millis conversion",
                vec![cpu("huge", i64::MAX / 100, 100000)],
                Some("out of range"),
            ),
            (
                "no limits",
                vec![Owned {
                    name: "plain".into(),
                    memory: None,
                    cpu: None,
                }],
                None,
            ),
        ] {
            let got = check(&ctrs, env);
            match want {
                None => assert!(got.is_ok(), "{name}: {got:?}"),
                Some(w) => {
                    let e = got.unwrap_err();
                    assert!(e.message().contains(w), "{name}: {}", e.message());
                    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{name}");
                }
            }
        }
    }

    #[test]
    fn the_memory_error_names_the_memory_limit() {
        let e = check(
            &[mem("hog", 2048 * MIB)],
            GuestEnvelope {
                mem_mib: 1024,
                vcpus: 1,
            },
        )
        .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        for w in ["hog", "1024", "spec.resources.limits.memory"] {
            assert!(e.message().contains(w), "{}", e.message());
        }
    }

    #[test]
    fn the_cpu_error_names_the_cpu_limit() {
        let p = DEFAULT_CPU_PERIOD_US;
        let e = check(
            &[cpu("hog", (2000 * p / 1000) as i64, p)],
            GuestEnvelope {
                mem_mib: 2048,
                vcpus: 1,
            },
        )
        .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert!(e.message().contains("spec.resources.limits.cpu"));
        assert!(!e.message().contains("spec.resources.limits.memory"));
    }

    #[test]
    fn a_container_may_use_the_whole_limit() {
        let mib = crate::vm_boot::resolve_guest_mem_mib(1024 * MIB).unwrap();
        assert!(check(
            &[mem("whole", 1024 * MIB)],
            GuestEnvelope {
                mem_mib: mib,
                vcpus: 1
            }
        )
        .is_ok());
    }
}
