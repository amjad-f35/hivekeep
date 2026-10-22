//! The probe: a minimal introspection Actor the e2e suites deploy.
//!
//! It reports what the runtime looks like from inside the sandbox, so a
//! suite can assert on real in-Actor state rather than on the configuration
//! the keeper generated. Each endpoint is small and independently
//! assertable:
//!
//! - `/whoami`: identity files from the system-info volume, plus the
//!   identity read through a handle held open since startup.
//! - `/fetch`: an HTTP(S) GET through the Actor's egress path.
//! - `/readfile`, `/writefile`: file access inside the sandbox.
//! - `/lookup`: name resolution as the sandbox sees it.
//! - `/resources`: the compute envelope (CPUs, memory, cgroup limits).
//! - `/capabilities`: the capability sets the kernel reports.
//! - `/healthz`: the wakeup probe, gated on a boot fetch when one is asked for.

mod bootfetch;
mod capabilities;
mod fetch;
mod files;
mod resources;
mod roots;
mod server;
#[cfg(test)]
mod test_support;

use std::net::SocketAddr;

use clap::Parser;

pub use bootfetch::{boot_fetch, RestoreFetch, BOOT_FETCH_TIMEOUT, BOOT_FETCH_URL_ENV};
pub use capabilities::{decode_cap_mask, CapabilitiesResponse, CAPABILITY_NAMES};
pub use fetch::{fetch, parse_fetch_headers, MAX_FETCH_BODY};

/// Where the probe template mounts the system-info volume.
pub const SYSTEM_INFO_DIR: &str = "/run/hivekeep";
/// The Actor's name, from the `actorMetadata` projection.
pub const IDENTITY_FILE: &str = "/run/hivekeep/actor-id";
/// The Actor's hive, from the `actorMetadata` projection.
pub const HIVE_FILE: &str = "/run/hivekeep/hive";
/// The Actor's UID, from the `actorMetadata` projection.
pub const UID_FILE: &str = "/run/hivekeep/actor-uid";
/// The projected trust bundle, present when the template asks for one.
pub const TRUST_FILE: &str = "/run/hivekeep/trust-bundle.pem";

/// Command-line options of the probe.
#[derive(Debug, Parser)]
#[command(
    name = "hivekeep-e2e-probe",
    about = "In-sandbox introspection server for the e2e suites"
)]
pub struct Args {
    /// Address to serve on. The templates' wakeup probes dial port 80.
    #[arg(long, default_value = "0.0.0.0:80")]
    pub listen: SocketAddr,
}

/// Runs the probe with the process arguments.
pub async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    server::serve(args.listen).await
}
