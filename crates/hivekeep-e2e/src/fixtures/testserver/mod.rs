//! The one binary behind every plain helper pod the suites stand up. A
//! subcommand picks the behavior, so a single image backs all of them and a
//! new origin-style test adds a subcommand rather than an image:
//!
//! ```text
//! hivekeep-e2e-testserver grpc --listen=:50051         cleartext HTTP/2 gRPC echo origin
//! hivekeep-e2e-testserver http --listen=:8080          HTTP(S) origin: /healthz, /echoheaders, POST /fetch, /dial
//! hivekeep-e2e-testserver egressprobe --listen=:8080   client that drives the egress gateway
//! hivekeep-e2e-testserver kill --cmdline-contains=X    SIGKILL matching processes
//! hivekeep-e2e-testserver websocket --listen=:80       WebSocket server answering PING
//! ```
//!
//! Each pod runs exactly one subcommand, so the grpc pod speaks nothing but
//! cleartext HTTP/2, for instance. Given `--tls-cert` and `--tls-key`, `http`
//! and `websocket` also serve HTTPS on :8443 beside their cleartext listener,
//! so one pod answers both schemes for the same name.

pub mod duration;
pub mod egress_client;
pub mod egressprobe;
pub mod grpc;
pub mod http;
pub mod kill;
pub mod serve;
pub mod tls;
pub mod websocket;

use std::time::Duration;

use clap::{Args, Parser, Subcommand};

/// Default egress gateway front door the probe dials.
pub const DEFAULT_EGRESS_GATEWAY_ADDRESS: &str = "gate-egress.hivekeep-system.svc:443";
/// Default trust bundle for the gateway's serving certificate.
pub const DEFAULT_SERVICE_DNS_TRUST_BUNDLE: &str =
    "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem";

#[derive(Debug, Parser)]
#[command(
    name = "hivekeep-e2e-testserver",
    about = "Multi-mode helper server for the e2e suites."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve a cleartext HTTP/2 gRPC echo origin.
    Grpc(GrpcArgs),
    /// Serve an HTTP/1.1 origin answering /healthz, /echoheaders, POST /fetch
    /// and /dial, and with --accept-connect any CONNECT.
    Http(HttpArgs),
    /// Drive the egress gateway with a chosen credential and report the
    /// failing hop.
    Egressprobe(EgressProbeArgs),
    /// SIGKILL every process whose command line contains a string, as a crash
    /// would.
    Kill(KillArgs),
    /// Serve a WebSocket server for e2e tests.
    Websocket(WebsocketArgs),
}

#[derive(Debug, Args)]
struct GrpcArgs {
    /// Address the gRPC server listens on, cleartext HTTP/2.
    #[arg(long, default_value = ":50051")]
    listen: String,
    /// Address for an HTTP/1.1 /readyz listener. Empty serves no HTTP at all,
    /// which the egress fixture wants; the ingress Actor sets it because a
    /// wakeup probe is an HTTP GET.
    #[arg(long, default_value = "")]
    health_listen: String,
}

#[derive(Debug, Args)]
struct HttpArgs {
    /// Address the origin serves cleartext HTTP on.
    #[arg(long, default_value = ":8080")]
    listen: String,
    /// PEM certificate file; with --tls-key, also serves HTTPS on :8443.
    #[arg(long, default_value = "")]
    tls_cert: String,
    /// PEM private key file; with --tls-cert, also serves HTTPS on :8443.
    #[arg(long, default_value = "")]
    tls_key: String,
    /// Response body for /healthz.
    #[arg(long, default_value = "")]
    body: String,
    /// Answer CONNECT requests 200, as a forward proxy would, without
    /// relaying anything.
    #[arg(long)]
    accept_connect: bool,
}

#[derive(Debug, Args)]
struct EgressProbeArgs {
    /// Address the probe's HTTP API listens on.
    #[arg(long, default_value = ":8080")]
    listen: String,
    /// host:port of the egress gateway's CONNECT front door.
    #[arg(long, default_value = DEFAULT_EGRESS_GATEWAY_ADDRESS)]
    gateway_address: String,
    /// PEM trust bundle used to verify the gateway's serving certificate.
    #[arg(long, default_value = DEFAULT_SERVICE_DNS_TRUST_BUNDLE)]
    trust_bundle: String,
    /// Budget for one CONNECT plus inner handshake.
    #[arg(long, default_value = "20s", value_parser = duration::parse_duration)]
    handshake_timeout: Duration,
}

#[derive(Debug, Args)]
struct KillArgs {
    /// Kill processes whose command line contains this.
    #[arg(long, required = true)]
    cmdline_contains: String,
}

#[derive(Debug, Args)]
struct WebsocketArgs {
    /// Address the WebSocket server serves cleartext on.
    #[arg(long, default_value = ":80")]
    listen: String,
    /// PEM certificate file; with --tls-key, also serves HTTPS on :8443.
    #[arg(long, default_value = "")]
    tls_cert: String,
    /// PEM private key file; with --tls-cert, also serves HTTPS on :8443.
    #[arg(long, default_value = "")]
    tls_key: String,
    /// Echo every WebSocket message with its original type and payload.
    #[arg(long)]
    echo: bool,
}

/// Runs the test server with the process arguments.
pub async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Grpc(a) => grpc::run(&a.listen, &a.health_listen).await,
        Command::Http(a) => {
            let mut handler = http::http_handler(a.body);
            if a.accept_connect {
                handler = http::accepting_connect(handler);
            }
            eprintln!("testserver http: listening on {}", a.listen);
            tls::serve_http(handler, &a.listen, &a.tls_cert, &a.tls_key).await
        }
        Command::Egressprobe(a) => {
            eprintln!(
                "testserver egressprobe: listening on {}, gateway {}",
                a.listen, a.gateway_address
            );
            let handler = egressprobe::egress_probe_handler(egressprobe::ProbeConfig {
                gateway_address: a.gateway_address,
                trust_bundle_path: a.trust_bundle,
                handshake_timeout: a.handshake_timeout,
            });
            tls::serve_http(handler, &a.listen, "", "").await
        }
        Command::Kill(a) => kill::run(&a.cmdline_contains),
        Command::Websocket(a) => {
            eprintln!("testserver websocket: listening on {}", a.listen);
            tls::serve_http(
                websocket::websocket_handler(a.echo),
                &a.listen,
                &a.tls_cert,
                &a.tls_key,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subcommands_parse() {
        let cli =
            Cli::try_parse_from(["t", "grpc", "--listen=:80", "--health-listen=:8080"]).unwrap();
        assert!(
            matches!(cli.command, Command::Grpc(GrpcArgs { ref listen, ref health_listen }) if listen == ":80" && health_listen == ":8080")
        );
        let cli = Cli::try_parse_from(["t", "egressprobe"]).unwrap();
        match cli.command {
            Command::Egressprobe(a) => {
                assert_eq!(a.gateway_address, DEFAULT_EGRESS_GATEWAY_ADDRESS);
                assert_eq!(a.handshake_timeout, Duration::from_secs(20));
            }
            other => panic!("parsed {other:?}"),
        }
        assert!(
            Cli::try_parse_from(["t", "kill"]).is_err(),
            "kill requires --cmdline-contains"
        );
        let cli = Cli::try_parse_from([
            "t",
            "http",
            "--accept-connect",
            "--tls-cert=a",
            "--tls-key=b",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Http(HttpArgs {
                accept_connect: true,
                ..
            })
        ));
    }
}
