//! The minting SDS server: Envoy asks (over delta SDS on a local socket)
//! for a secret named after the SNI of a connection it is about to
//! intercept, and gets a fresh short-lived leaf for that name signed by
//! the MITM CA.
//!
//! The server never pushes on its own: each leaf carries a resource TTL
//! shorter than its validity, so Envoy drops it and asks again if the name
//! is still in use. Names that are not host names or IP addresses are
//! withdrawn instead of minted.

pub mod delta;
pub mod listen;
pub mod server;

#[cfg(test)]
mod manifest_tests;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_identity::host_cert::{HostCertMinter, DEFAULT_HOST_CERT_TTL};
use hivekeep_identity::state_file::RefreshingCaPool;

pub use server::SdsServer;

/// Type URL of SDS secrets.
pub const SECRET_TYPE_URL: &str =
    "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret";

/// Default leaf lifetime.
pub const DEFAULT_TTL: Duration = DEFAULT_HOST_CERT_TTL;

/// Shortest sensible leaf lifetime (leaves are backdated five minutes).
const MIN_SENSIBLE_TTL: Duration = Duration::from_secs(2 * 60);
/// Longest sensible leaf lifetime.
const MAX_SENSIBLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// `hivekeep-gate sds-mint` flags.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Unix socket to listen on; required, and the only transport offered,
    /// because leaf private keys transit this channel.
    #[arg(long = "uds-path")]
    pub uds_path: Option<PathBuf>,
    /// Path to the CA pool JSON holding the MITM CA.
    #[arg(long = "ca-pool-path")]
    pub ca_pool_path: Option<PathBuf>,
    /// Leaf certificate lifetime; the resource TTL is derived from it, so
    /// an actively used name is re-minted about twice per lifetime.
    #[arg(long = "leaf-cert-ttl", default_value = "15m", value_parser = humantime::parse_duration)]
    pub leaf_cert_ttl: Duration,
}

/// Checks the leaf lifetime is within the sensible band.
pub fn validate_ttl(ttl: Duration) -> Result<(), String> {
    if ttl.is_zero() {
        return Err(format!(
            "--leaf-cert-ttl must be positive, got {}",
            humantime::format_duration(ttl)
        ));
    }
    if ttl < MIN_SENSIBLE_TTL {
        return Err(format!(
            "--leaf-cert-ttl {} is below {}; leaves are back-dated 5m for clock skew, so most of each certificate's validity would already be in the past",
            humantime::format_duration(ttl),
            humantime::format_duration(MIN_SENSIBLE_TTL)
        ));
    }
    if ttl > MAX_SENSIBLE_TTL {
        return Err(format!(
            "--leaf-cert-ttl {} is above {}; a name in steady use would carry the same certificate too long for a short-lived MITM leaf",
            humantime::format_duration(ttl),
            humantime::format_duration(MAX_SENSIBLE_TTL)
        ));
    }
    Ok(())
}

/// Runs the server until `shutdown` resolves.
pub async fn run(
    args: Args,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> anyhow::Result<()> {
    let uds = args
        .uds_path
        .ok_or_else(|| anyhow::anyhow!("--uds-path is required"))?;
    let pool_path = args
        .ca_pool_path
        .ok_or_else(|| anyhow::anyhow!("--ca-pool-path is required"))?;
    validate_ttl(args.leaf_cert_ttl).map_err(anyhow::Error::msg)?;
    let pool = RefreshingCaPool::open(&pool_path)
        .map_err(|e| anyhow::anyhow!("while loading CA pool: {e}"))?;
    let minter = HostCertMinter::new(Arc::new(pool), args.leaf_cert_ttl)
        .map_err(|e| anyhow::anyhow!("building minter: {e}"))?;
    let lis = listen::listen(&uds)?;
    tracing::info!(address = %uds.display(), ttl = %humantime::format_duration(args.leaf_cert_ttl), "sds-mint listening");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(lis);
    tonic::transport::Server::builder()
        .add_service(SdsServer::new(Arc::new(minter)).into_service())
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn validate_ttl_cases() {
        let m = Duration::from_secs(60);
        for (name, ttl, ok) in [
            ("default", DEFAULT_TTL, true),
            ("deployed value", 15 * m, true),
            ("the load-test value", 5 * m, true),
            ("at the band floor", 2 * m, true),
            ("at the band ceiling", 24 * 60 * m, true),
            ("one minute", m, false),
            ("zero", Duration::ZERO, false),
            ("short", Duration::from_secs(30), false),
            ("long", 48 * 60 * m, false),
        ] {
            assert_eq!(validate_ttl(ttl).is_ok(), ok, "{name}");
        }
    }

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        a: Args,
    }

    #[test]
    fn default_ttl_is_the_flag_default() {
        let cli = Cli::try_parse_from(["x"]).unwrap();
        assert_eq!(cli.a.leaf_cert_ttl, DEFAULT_TTL);
        assert!(validate_ttl(DEFAULT_TTL).is_ok());
    }
}
