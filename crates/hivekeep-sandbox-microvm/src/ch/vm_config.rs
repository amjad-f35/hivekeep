//! The body of `vm.create`: the subset of the VMM's VM config the worker sets
//! to boot the kata guest.

use serde::Serialize;

fn is_zero(v: &i32) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// The VM config.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct VmConfig {
    /// vCPUs.
    pub cpus: CpusConfig,
    /// Guest RAM.
    pub memory: MemoryConfig,
    /// Kernel and command line.
    pub payload: PayloadConfig,
    /// virtio-pmem devices.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pmem: Vec<PmemConfig>,
    /// virtio-fs devices.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fs: Vec<FsConfig>,
    /// The balloon.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balloon: Option<BalloonConfig>,
    /// The entropy device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rng: Option<RngConfig>,
    /// The emulated UART.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<ConsoleConfig>,
    /// The virtio console.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub console: Option<ConsoleConfig>,
    /// The hybrid vsock the agent listens on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vsock: Option<VsockConfig>,
    /// VM-wide platform options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<PlatformConfig>,
}

/// The virtio-balloon device.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct BalloonConfig {
    /// Initial balloon size.
    pub size: i64,
    /// Whether the guest reports free pages back to the host.
    #[serde(skip_serializing_if = "is_false")]
    pub free_page_reporting: bool,
}

/// A virtio-fs device backed by a vhost-user socket.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct FsConfig {
    /// The tag the guest mounts.
    pub tag: String,
    /// The virtiofsd socket.
    pub socket: String,
    /// Request queues.
    #[serde(skip_serializing_if = "is_zero")]
    pub num_queues: i32,
    /// Queue depth.
    #[serde(skip_serializing_if = "is_zero")]
    pub queue_size: i32,
    /// The PCI segment it sits on.
    #[serde(skip_serializing_if = "is_zero")]
    pub pci_segment: i32,
}

/// VM-wide platform options. More than one PCI segment is needed when a
/// device sits on a non-zero one.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PlatformConfig {
    /// PCI segments.
    #[serde(skip_serializing_if = "is_zero")]
    pub num_pci_segments: i32,
}

/// Boot and maximum vCPUs.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct CpusConfig {
    /// vCPUs at boot.
    pub boot_vcpus: i32,
    /// vCPUs at most.
    pub max_vcpus: i32,
}

/// Guest RAM. `shared` backs it with a memfd, which is what lets a snapshot
/// write a sparse image.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct MemoryConfig {
    /// Bytes.
    pub size: i64,
    /// memfd-backed.
    pub shared: bool,
}

/// The guest kernel and its command line.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PayloadConfig {
    /// Kernel path.
    pub kernel: String,
    /// Command line.
    pub cmdline: String,
}

/// A virtio-pmem device.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PmemConfig {
    /// Device id, as recorded in snapshots.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Backing file.
    pub file: String,
    /// Map privately, so guest writes land in a private copy.
    #[serde(skip_serializing_if = "is_false")]
    pub discard_writes: bool,
}

/// The entropy source.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct RngConfig {
    /// Host source.
    pub src: String,
}

/// A serial or console device: mode `Off`, `File` (with `file`), or `Tty`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ConsoleConfig {
    /// Mode.
    pub mode: String,
    /// Capture file in `File` mode.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub file: String,
}

/// The hybrid vsock: guest CID and host socket.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct VsockConfig {
    /// Guest CID.
    pub cid: i64,
    /// Host socket.
    pub socket: String,
}
