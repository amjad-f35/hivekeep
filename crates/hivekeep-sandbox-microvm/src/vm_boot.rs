//! How a micro-VM is sized and booted: the runtime assets it needs, its RAM
//! floor, the guest kernel command line and the VM config.

use std::collections::HashMap;

use hivekeep_proto::keeper::v1 as kpb;

use crate::ch::{
    BalloonConfig, ConsoleConfig, CpusConfig, FsConfig, MemoryConfig, PayloadConfig,
    PlatformConfig, PmemConfig, RngConfig, VmConfig, VmmInfo, VsockConfig,
};
use crate::kata::config::{with_agent_debug, BASE_KERNEL_PARAMS};
use crate::kata::paths::{serial_log_path, virtiofsd_socket_path, vsock_socket_path, FS_TAG};

/// The VMM binary, as a runtime asset name.
pub const ASSET_CH: &str = "cloud-hypervisor";
/// The guest kernel.
pub const ASSET_KERNEL: &str = "kata-kernel";
/// The guest OS image.
pub const ASSET_IMAGE: &str = "kata-image";
/// The virtiofsd binary.
pub const ASSET_VIRTIOFSD: &str = "virtiofsd";
/// The virtio-pmem device carrying the guest image, in every VM config and
/// so in every snapshot.
pub const GUEST_IMAGE_PMEM_ID: &str = "guest-image";
/// Where the guest image keeps the agent; booted as PID 1 it is the guest's
/// entire userspace.
pub const KATA_AGENT_PATH: &str = "/usr/bin/kata-agent";
/// The smallest guest RAM, and so the smallest Actor memory limit: less makes
/// the guest hang at boot rather than fail. 128MiB is the smallest size with
/// real headroom for the agent and a small workload.
pub const MIN_GUEST_MEM_MIB: u64 = 128;
/// Memory of the worker pod kept from Actors: the worker itself and the page
/// cache of snapshot writes in flight.
pub const MEMORY_RESERVED_BYTES: i64 = 64 << 20;

/// What an Actor costs outside its guest, booked with every Actor: the VMM,
/// virtiofsd, host page tables for guest RAM, and (in the fixed part) the
/// page cache of its snapshot writes.
pub fn actor_overhead() -> kpb::RuntimeOverhead {
    kpb::RuntimeOverhead {
        memory_bytes: 128 << 20,
        memory_bytes_per_gib: 8 << 20,
    }
}

/// The binaries a request runs: the fetched assets when present, else the
/// worker's flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntime {
    /// The VMM.
    pub ch_binary: String,
    /// virtiofsd; empty for the one on PATH.
    pub virtiofsd: String,
}

/// Resolves the request's runtime binaries.
pub fn resolve_runtime(paths: &HashMap<String, String>, ch_flag: &str) -> ResolvedRuntime {
    ResolvedRuntime {
        ch_binary: paths
            .get(ASSET_CH)
            .filter(|p| !p.is_empty())
            .cloned()
            .unwrap_or_else(|| ch_flag.to_string()),
        virtiofsd: paths.get(ASSET_VIRTIOFSD).cloned().unwrap_or_default(),
    }
}

/// The guest kernel and image a request carries, or an error naming both.
pub fn kernel_and_image(paths: &HashMap<String, String>) -> Result<(String, String), String> {
    match (paths.get(ASSET_KERNEL), paths.get(ASSET_IMAGE)) {
        (Some(k), Some(i)) if !k.is_empty() && !i.is_empty() => Ok((k.clone(), i.clone())),
        _ => Err(format!(
            "the micro-VM worker requires {ASSET_KERNEL:?} and {ASSET_IMAGE:?} asset paths"
        )),
    }
}

/// The guest RAM for a declared memory limit: all of it. Unset or below the
/// floor is an error.
pub fn resolve_guest_mem_mib(declared_bytes: i64) -> Result<u64, String> {
    if declared_bytes <= 0 {
        return Err("actor has no memory limit to size the micro-VM's RAM".into());
    }
    let m = declared_bytes as u64 / (1024 * 1024);
    if m < MIN_GUEST_MEM_MIB {
        return Err(format!(
            "actor memory limit {m}MiB is too small for a microVM, the minimum is {MIN_GUEST_MEM_MIB}MiB"
        ));
    }
    Ok(m)
}

/// The guest kernel parameters.
pub fn kernel_params(guest_debug: bool) -> String {
    if guest_debug {
        with_agent_debug(BASE_KERNEL_PARAMS)
    } else {
        BASE_KERNEL_PARAMS.to_string()
    }
}

/// Whether to boot the agent as PID 1. That skips systemd, most of what the
/// guest reads at boot and its snapshot carries, but also chronyd, which
/// repairs the guest clock after a resume: only safe on a VMM that advances
/// the clock itself.
pub fn agent_init(info: &VmmInfo) -> bool {
    if info.advances_guest_clock_on_restore() {
        return true;
    }
    tracing::info!(vmm_version = %info.version, vmm_build_version = %info.build_version, "VMM does not advance the guest clock on restore; booting systemd to keep chronyd");
    false
}

/// The kernel parameters selecting the guest's PID 1. The systemd path names
/// kata's target (else the guest powers off) and masks networkd, since the
/// agent owns eth0.
pub fn init_params(agent_init: bool) -> String {
    if agent_init {
        format!("init={KATA_AGENT_PATH}")
    } else {
        "systemd.unit=kata-containers.target systemd.mask=systemd-networkd.service systemd.mask=systemd-networkd.socket".into()
    }
}

/// The early console of the UART the VMM emulates before virtio-console
/// exists: MMIO on arm64, an ISA port elsewhere.
pub fn earlycon_param() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "earlycon=pl011,mmio,0x09000000"
    } else {
        "earlycon=uart,io,0x3f8,115200"
    }
}

/// What a VM is booted with.
pub struct VmSpec<'a> {
    /// Sandbox id.
    pub id: &'a str,
    /// Guest kernel.
    pub kernel: &'a str,
    /// Guest image.
    pub image: &'a str,
    /// Extra kernel parameters.
    pub kernel_params: &'a str,
    /// Where the virtio console is captured.
    pub console_log: &'a str,
    /// Guest RAM, MiB.
    pub mem_mib: u64,
    /// vCPUs.
    pub vcpus: u32,
    /// Boot the agent as PID 1.
    pub agent_init: bool,
    /// Add the early UART console.
    pub debug: bool,
}

/// The VM config. The guest console is a virtio-console: every byte to an
/// emulated UART traps to the VMM, and that, not the kernel, dominated cold
/// boot. Debug mode adds the UART back (with earlycon) for guests that die
/// before virtio-console probes.
pub fn build_vm_config(s: &VmSpec<'_>) -> VmConfig {
    let mut cmdline = format!(
        "root=/dev/pmem0p1 rootflags=dax,data=ordered,errors=remount-ro ro rootfstype=ext4 panic=1 no_timer_check noreplace-smp console=hvc0 {}",
        init_params(s.agent_init)
    );
    if !s.kernel_params.is_empty() {
        cmdline.push(' ');
        cmdline.push_str(s.kernel_params);
    }
    if cfg!(target_arch = "x86_64") {
        cmdline.push_str(" clocksource=kvm-clock");
    }
    let mut serial = ConsoleConfig {
        mode: "Off".into(),
        file: String::new(),
    };
    if s.debug {
        cmdline.push(' ');
        cmdline.push_str(earlycon_param());
        serial = ConsoleConfig {
            mode: "File".into(),
            file: serial_log_path(s.id).to_string_lossy().into(),
        };
    }
    VmConfig {
        cpus: CpusConfig {
            boot_vcpus: s.vcpus as i32,
            max_vcpus: s.vcpus as i32,
        },
        memory: MemoryConfig {
            size: (s.mem_mib * 1024 * 1024) as i64,
            shared: true,
        },
        payload: PayloadConfig {
            kernel: s.kernel.into(),
            cmdline,
        },
        // DAX keeps the image's file data in the host page cache rather than
        // copied into guest RAM.
        pmem: vec![PmemConfig {
            id: GUEST_IMAGE_PMEM_ID.into(),
            file: s.image.into(),
            discard_writes: true,
        }],
        fs: build_fs_configs(s.id),
        balloon: Some(BalloonConfig {
            size: 0,
            free_page_reporting: true,
        }),
        platform: Some(PlatformConfig {
            num_pci_segments: 2,
        }),
        rng: Some(RngConfig {
            src: "/dev/urandom".into(),
        }),
        console: Some(ConsoleConfig {
            mode: "File".into(),
            file: s.console_log.into(),
        }),
        serial: Some(serial),
        vsock: Some(VsockConfig {
            cid: 3,
            socket: vsock_socket_path(s.id).to_string_lossy().into(),
        }),
    }
}

/// The one virtio-fs device, on the PCI segment reserved for it.
pub fn build_fs_configs(id: &str) -> Vec<FsConfig> {
    vec![FsConfig {
        tag: FS_TAG.into(),
        socket: virtiofsd_socket_path(id).to_string_lossy().into(),
        num_queues: 1,
        queue_size: 1024,
        pci_segment: 1,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kata::paths::console_log_path;

    fn spec<'a>(console: &'a str, debug: bool) -> VmSpec<'a> {
        VmSpec {
            id: "actor-1",
            kernel: "/vmlinux",
            image: "/rootfs.img",
            kernel_params: "",
            console_log: console,
            mem_mib: 256,
            vcpus: 1,
            agent_init: true,
            debug,
        }
    }

    #[test]
    fn guest_memory() {
        const MIB: i64 = 1024 * 1024;
        assert!(resolve_guest_mem_mib(0).is_err(), "unset");
        assert_eq!(resolve_guest_mem_mib(1536 * MIB).unwrap(), 1536);
        assert_eq!(
            resolve_guest_mem_mib(MIN_GUEST_MEM_MIB as i64 * MIB).unwrap(),
            MIN_GUEST_MEM_MIB
        );
        assert!(resolve_guest_mem_mib((MIN_GUEST_MEM_MIB as i64 - 1) * MIB).is_err());
    }

    #[test]
    fn init_selection() {
        assert_eq!(init_params(true), "init=/usr/bin/kata-agent");
        let s = init_params(false);
        for want in [
            "systemd.unit=kata-containers.target",
            "systemd.mask=systemd-networkd.service",
            "systemd.mask=systemd-networkd.socket",
        ] {
            assert!(s.contains(want), "{s}");
        }
        assert!(!s.contains("init="));
    }

    #[test]
    fn console_setup() {
        let console = console_log_path("actor-1").to_string_lossy().to_string();
        let cfg = build_vm_config(&spec(&console, false));
        assert_eq!(
            cfg.console,
            Some(ConsoleConfig {
                mode: "File".into(),
                file: console.clone()
            })
        );
        assert_eq!(cfg.serial.as_ref().unwrap().mode, "Off");
        assert!(cfg.payload.cmdline.contains("console=hvc0"));
        if cfg!(target_arch = "x86_64") {
            assert!(cfg.payload.cmdline.contains("clocksource=kvm-clock"));
        }
        assert!(!cfg.payload.cmdline.contains("earlycon"));
        let dbg = build_vm_config(&spec(&console, true));
        assert_eq!(
            dbg.serial,
            Some(ConsoleConfig {
                mode: "File".into(),
                file: serial_log_path("actor-1").to_string_lossy().into()
            })
        );
        assert!(dbg.payload.cmdline.contains("earlycon="));
    }

    #[test]
    fn debug_kernel_params() {
        let prod = kernel_params(false);
        for forbidden in ["agent.debug_console", "agent.debug_console_vport", "1026"] {
            assert!(!prod.contains(forbidden), "{prod}");
        }
        let dbg = kernel_params(true);
        for want in [
            "agent.log=debug",
            "agent.debug_console",
            "agent.debug_console_vport=1026",
        ] {
            assert!(dbg.contains(want), "{dbg}");
        }
    }

    #[test]
    fn balloon_reports_free_pages() {
        let cfg = build_vm_config(&spec("/console.log", false));
        assert_eq!(
            cfg.balloon,
            Some(BalloonConfig {
                size: 0,
                free_page_reporting: true
            })
        );
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains(r#""balloon":{"size":0,"free_page_reporting":true}"#),
            "{json}"
        );
    }

    #[test]
    fn agent_init_follows_the_vmm() {
        let v = |s: &str| VmmInfo {
            version: s.into(),
            ..Default::default()
        };
        assert!(agent_init(&v("53.0.0")));
        assert!(!agent_init(&v("52.0.0")));
    }

    #[test]
    fn requires_kernel_and_image() {
        let p = |kv: &[(&str, &str)]| {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>()
        };
        for (name, paths) in [
            ("no assets", p(&[])),
            ("no kernel", p(&[(ASSET_IMAGE, "/i")])),
            ("no image", p(&[(ASSET_KERNEL, "/k")])),
            ("empty path", p(&[(ASSET_KERNEL, ""), (ASSET_IMAGE, "/i")])),
        ] {
            let err = kernel_and_image(&paths).unwrap_err();
            assert!(
                err.contains(ASSET_KERNEL) && err.contains(ASSET_IMAGE),
                "{name}: {err}"
            );
        }
    }
}
