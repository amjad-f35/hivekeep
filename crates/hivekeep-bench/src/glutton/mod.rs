//! The glutton benchmark workload: a server, meant to run inside an Actor,
//! that consumes RAM, disk, CPU and file descriptors and gossips with its
//! peers on request, over gRPC or protobuf-over-HTTP (see
//! `proto/hivekeep/glutton/v1/glutton.proto`).

/// The workload's identity to telemetry: metric prefix and service name.
pub const NAME: &str = "glutton";

/// Wire modes the main listener can serve, selected by `--mode`.
pub const MODE_GRPC: &str = "grpc";
/// Protobuf-over-HTTP mode.
pub const MODE_HTTP: &str = "http";

/// Routes the HTTP-mode server answers. [`READYZ_ROUTE`] is served in both
/// modes: the worker holds a resume until it answers 200, so a resume never
/// reports success before the listener is reachable. The test fake answers
/// exactly these paths, which keeps the stand-in and the real server from
/// drifting apart.
pub const READYZ_ROUTE: &str = "/readyz";
/// Ping route.
pub const PING_ROUTE: &str = "/ping";
/// WriteDisk route.
pub const WRITE_DISK_ROUTE: &str = "/writedisk";
/// ReadDisk route.
pub const READ_DISK_ROUTE: &str = "/readdisk";
/// WriteRAM route.
pub const WRITE_RAM_ROUTE: &str = "/writeram";
/// ReadRAM route.
pub const READ_RAM_ROUTE: &str = "/readram";
/// BurnCPU route.
pub const BURN_CPU_ROUTE: &str = "/burncpu";
/// Ingest route.
pub const INGEST_ROUTE: &str = "/ingest";
/// UseCPU route.
pub const USE_CPU_ROUTE: &str = "/usecpu";
/// RunScript route.
pub const RUN_SCRIPT_ROUTE: &str = "/runscript";

#[cfg(test)]
pub(crate) mod fake;

pub mod cpu_load;
pub mod ctx;
pub mod gossip;
pub mod metrics;
pub mod script;
pub mod server;
pub mod service;
pub mod sizes;

pub use ctx::Ctx;
pub use server::{handler, serve, Handler};
pub use service::{Options, Service};
