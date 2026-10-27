//! The subcommands for the peers around the tunnel: certificates, the fake
//! gateway, and the Actors' stand-ins.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use hivekeep_bench_tunnel::admin::{self, Extra};
use hivekeep_bench_tunnel::load::egress_actor;
use hivekeep_bench_tunnel::sink::{Gateway, GatewayConfig, IngressActor};
use hivekeep_bench_tunnel::{pki, workerhost};
use tokio_util::sync::CancellationToken;

#[derive(clap::Args)]
pub struct PkiArgs {
    /// Directory to write the certificates to.
    #[arg(long)]
    dir: PathBuf,
    /// Names and IPs the tunnel's serving certificate is valid for.
    #[arg(long, value_delimiter = ',', default_value = "hk-tunnel-worker")]
    worker_hosts: Vec<String>,
    /// Names and IPs the egress gateway's certificate is valid for.
    #[arg(long, value_delimiter = ',', default_value = "hk-tunnel-sink")]
    gateway_hosts: Vec<String>,
    /// Certificate lifetime.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30days")]
    validity: Duration,
}

pub fn pki(a: PkiArgs) -> anyhow::Result<()> {
    pki::generate(
        &a.dir,
        &pki::Options {
            worker_hosts: a.worker_hosts,
            gateway_hosts: a.gateway_hosts,
            validity: a.validity,
        },
    )?;
    Ok(())
}

#[derive(clap::Args)]
pub struct SinkArgs {
    /// Directory written by `pki`.
    #[arg(long)]
    pki_dir: PathBuf,
    /// Address for the fake egress gateway.
    #[arg(long, default_value = ":9443")]
    gateway_listen: String,
    /// Address for /stats and /healthz.
    #[arg(long, default_value = ":9091")]
    admin: String,
    /// Response body size for requests inside answered tunnels.
    #[arg(long, default_value_t = 16)]
    body_bytes: usize,
}

pub async fn sink(a: SinkArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    let gateway = Gateway::new(&GatewayConfig {
        credential_bundle: a.pki_dir.join(pki::GATEWAY_BUNDLE),
        client_ca: a.pki_dir.join(pki::CA_FILE),
        body_bytes: a.body_bytes,
    })?;
    let gateway_lis = admin::listen_tcp(&a.gateway_listen)
        .await
        .context("gateway listener")?;
    let admin_lis = admin::listen_tcp(&a.admin)
        .await
        .context("admin listener")?;
    tracing::info!(gateway = %gateway_lis.local_addr()?, admin = %a.admin, "sink serving");
    eprintln!(
        "sink serving: gateway {}, admin {}",
        gateway_lis.local_addr()?,
        a.admin
    );
    let handler = gateway.admin_handler();
    tokio::select! {
        r = gateway.serve(gateway_lis, cancel.clone()) => r?,
        r = admin::serve_tcp(admin_lis, handler, cancel.clone()) => r?,
    }
    Ok(())
}

#[derive(clap::Args)]
pub struct IngressActorArgs {
    /// Port to serve.
    #[arg(long, default_value_t = workerhost::ACTOR_PORT)]
    port: u16,
    /// Response body size.
    #[arg(long, default_value_t = 16)]
    body_bytes: usize,
    /// Unix socket to serve the counters on.
    #[arg(long)]
    stats_socket: PathBuf,
}

pub async fn ingress_actor(a: IngressActorArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    let lis = tokio::net::TcpListener::bind(("0.0.0.0", a.port))
        .await
        .context("ingress actor listener")?;
    let actor = IngressActor::new(a.body_bytes);
    let stats_lis = admin::listen_unix(&a.stats_socket).context("stats socket")?;
    let a2 = actor.clone();
    let extra: Extra = Arc::new(move || {
        let mut m = serde_json::Map::new();
        m.insert(
            "ingress_actor".into(),
            serde_json::to_value(a2.stats()).unwrap_or_default(),
        );
        m
    });
    tokio::spawn(admin::serve_unix(
        stats_lis,
        admin::stats_handler(Some(extra)),
        cancel.clone(),
    ));
    actor.serve(lis, cancel).await?;
    Ok(())
}

#[derive(clap::Args)]
pub struct EgressActorArgs {
    /// Unix socket the load generator drives it on.
    #[arg(long)]
    socket: PathBuf,
}

pub async fn egress_actor(a: EgressActorArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    // The sandbox's kernel network stack stands in for the sandbox's own,
    // with Linux's defaults; widen its ports as the containers' are, or a
    // long run of short connections exhausts them on sockets in TIME_WAIT.
    #[cfg(target_os = "linux")]
    for (path, value) in [
        ("/proc/sys/net/ipv4/ip_local_port_range", "1024 65535"),
        ("/proc/sys/net/ipv4/tcp_tw_reuse", "1"),
    ] {
        std::fs::write(path, value).context("tuning the sandbox network")?;
    }
    let lis = admin::listen_unix(&a.socket).context("egress actor socket")?;
    egress_actor::serve(lis, cancel).await?;
    Ok(())
}
