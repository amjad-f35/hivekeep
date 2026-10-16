//! OCI spec to the guest agent's protobuf spec. The agent's JSON field names
//! differ from OCI's, so the conversion is field by field, covering what the
//! agent needs to create and start a container.

use crate::agentpb::grpc as apb;
use crate::guest_spec::Spec;

/// The CFS period assumed for a quota without one: the kernel default, and
/// the period quotas are expressed against.
pub const DEFAULT_CPU_PERIOD_US: u64 = 100_000;

/// Converts `s`. The caller sets the rootfs (`root`) to the shared-tree path.
pub fn spec_to_agent_pb(s: &Spec) -> apb::Spec {
    let mut out = apb::Spec {
        version: s.oci_version.clone(),
        hostname: s.hostname.clone(),
        annotations: s
            .annotations
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        ..Default::default()
    };
    if let Some(p) = &s.process {
        out.process = Some(apb::Process {
            args: p.args.clone(),
            env: p.env.clone(),
            cwd: p.cwd.clone(),
            no_new_privileges: p.no_new_privileges,
            user: Some(apb::User {
                uid: p.user.uid,
                gid: p.user.gid,
                additional_gids: p.user.additional_gids.clone(),
                username: p.user.username.clone(),
            }),
            capabilities: p.capabilities.as_ref().map(|c| apb::LinuxCapabilities {
                bounding: c.bounding.clone(),
                effective: c.effective.clone(),
                inheritable: c.inheritable.clone(),
                permitted: c.permitted.clone(),
                ambient: c.ambient.clone(),
            }),
            rlimits: p
                .rlimits
                .iter()
                .map(|r| apb::PosixRlimit {
                    r#type: r.kind.clone(),
                    hard: r.hard,
                    soft: r.soft,
                })
                .collect(),
            ..Default::default()
        });
    }
    if let Some(r) = &s.root {
        out.root = Some(apb::Root {
            path: r.path.clone(),
            readonly: r.readonly,
        });
    }
    out.mounts = s
        .mounts
        .iter()
        .map(|m| apb::Mount {
            destination: m.destination.clone(),
            source: m.source.clone(),
            r#type: m.kind.clone(),
            options: m.options.clone(),
        })
        .collect();
    if let Some(l) = &s.linux {
        let mut pl = apb::Linux {
            cgroups_path: l.cgroups_path.clone(),
            masked_paths: l.masked_paths.clone(),
            readonly_paths: l.readonly_paths.clone(),
            sysctl: l
                .sysctl
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            ..Default::default()
        };
        // The network, cgroup and time namespaces are the host's business or
        // unsupported in the guest, so they are dropped (the container then
        // shares the guest's network). Every other namespace must lose its
        // host path, which means nothing inside the guest.
        pl.namespaces = l
            .namespaces
            .iter()
            .filter(|n| !matches!(n.kind.as_str(), "network" | "cgroup" | "time"))
            .map(|n| apb::LinuxNamespace {
                r#type: n.kind.clone(),
                path: String::new(),
            })
            .collect();
        if let Some(r) = &l.resources {
            let mut res = apb::LinuxResources {
                devices: r
                    .devices
                    .iter()
                    .map(|d| apb::LinuxDeviceCgroup {
                        allow: d.allow,
                        r#type: d.kind.clone(),
                        major: d.major.unwrap_or(0),
                        minor: d.minor.unwrap_or(0),
                        access: d.access.clone(),
                    })
                    .collect(),
                ..Default::default()
            };
            // The agent enforces only what this carries.
            if let Some(limit) = r.memory.as_ref().and_then(|m| m.limit) {
                res.memory = Some(apb::LinuxMemory {
                    limit,
                    ..Default::default()
                });
            }
            if let Some(c) = &r.cpu {
                let mut cpu = apb::LinuxCpu {
                    shares: c.shares.unwrap_or(0),
                    ..Default::default()
                };
                // A non-positive quota is unlimited in OCI; on the wire a zero
                // would mean no CPU at all, so it stays unset. A live quota
                // needs a period, which OCI makes optional and the wire does
                // not, so it defaults to the CFS period.
                if let Some(q) = c.quota.filter(|q| *q > 0) {
                    cpu.quota = q;
                    cpu.period = DEFAULT_CPU_PERIOD_US;
                }
                if let Some(p) = c.period.filter(|p| *p > 0) {
                    cpu.period = p;
                }
                res.cpu = Some(cpu);
            }
            pl.resources = Some(res);
        }
        out.linux = Some(pl);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest_spec::{Cpu, Linux, Memory, Resources};

    fn with_cpu(cpu: Cpu) -> Spec {
        Spec {
            linux: Some(Linux {
                resources: Some(Resources {
                    cpu: Some(cpu),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn carries_resource_limits() {
        let mut s = with_cpu(Cpu {
            shares: Some(1024),
            quota: Some(20000),
            period: Some(100000),
        });
        s.linux.as_mut().unwrap().resources.as_mut().unwrap().memory = Some(Memory {
            limit: Some(67108864),
        });
        let r = spec_to_agent_pb(&s).linux.unwrap().resources.unwrap();
        assert_eq!(r.memory.unwrap().limit, 67108864);
        let cpu = r.cpu.unwrap();
        assert_eq!((cpu.quota, cpu.period, cpu.shares), (20000, 100000, 1024));
    }

    #[test]
    fn no_limits_add_no_sub_messages() {
        let r = spec_to_agent_pb(&with_cpu(Cpu {
            shares: Some(1024),
            ..Default::default()
        }))
        .linux
        .unwrap()
        .resources
        .unwrap();
        assert!(r.memory.is_none());
        let cpu = r.cpu.unwrap();
        assert_eq!((cpu.shares, cpu.quota, cpu.period), (1024, 0, 0));
    }

    #[test]
    fn a_quota_without_a_period_gets_the_default() {
        let cpu = spec_to_agent_pb(&with_cpu(Cpu {
            quota: Some(20000),
            ..Default::default()
        }))
        .linux
        .unwrap()
        .resources
        .unwrap()
        .cpu
        .unwrap();
        assert_eq!((cpu.quota, cpu.period), (20000, DEFAULT_CPU_PERIOD_US));
    }

    #[test]
    fn non_positive_cpu_values_are_dropped() {
        for (name, quota, period, want_quota, want_period) in [
            (
                "zero period keeps the CFS default",
                20000,
                0,
                20000,
                DEFAULT_CPU_PERIOD_US,
            ),
            ("zero quota is unlimited", 0, 50000, 0, 50000),
            ("negative quota is unlimited", -1, 50000, 0, 50000),
        ] {
            let cpu = spec_to_agent_pb(&with_cpu(Cpu {
                shares: None,
                quota: Some(quota),
                period: Some(period),
            }))
            .linux
            .unwrap()
            .resources
            .unwrap()
            .cpu
            .unwrap();
            assert_eq!((cpu.quota, cpu.period), (want_quota, want_period), "{name}");
        }
    }

    #[test]
    fn forwards_sysctls() {
        let key = "net.ipv4.ip_unprivileged_port_start";
        let s = Spec {
            linux: Some(Linux {
                sysctl: [(key.to_string(), "0".to_string())].into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(spec_to_agent_pb(&s).linux.unwrap().sysctl[key], "0");
    }

    #[test]
    fn drops_host_namespaces_and_paths() {
        let mut l = Linux::default();
        for (k, p) in [
            ("pid", ""),
            ("network", "/var/run/netns/x"),
            ("ipc", "/proc/1/ns/ipc"),
            ("cgroup", ""),
        ] {
            l.namespaces.push(crate::guest_spec::Namespace {
                kind: k.into(),
                path: p.into(),
            });
        }
        let s = Spec {
            linux: Some(l),
            ..Default::default()
        };
        let ns = spec_to_agent_pb(&s).linux.unwrap().namespaces;
        let kinds: Vec<_> = ns.iter().map(|n| n.r#type.as_str()).collect();
        assert_eq!(kinds, ["pid", "ipc"]);
        assert!(ns.iter().all(|n| n.path.is_empty()));
    }
}
