//! `hivekeep-sandbox-gvisor`: serves the `Worker` API on the worker pod's
//! socket and runs each hosted Actor in a gVisor sandbox.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use clap::Parser;
use hivekeep_proto::worker::v1::worker_server::WorkerServer;
use hivekeep_sandbox_gvisor::config::Config;
use hivekeep_sandbox_gvisor::crash_report::KeeperCrashReporter;
use hivekeep_sandbox_gvisor::flags::{FlagStore, DEFAULT_POLL_INTERVAL};
use hivekeep_sandbox_gvisor::grpc::WorkerApi;
use hivekeep_sandbox_gvisor::ports::{CrashReporter, LogUsageSink};
use hivekeep_sandbox_gvisor::register::{self, RegisterError, ReportConfig};
use hivekeep_sandbox_gvisor::{
    actor_path, cgroup, ckpt_cgroup, memory_watch, node_path, ServiceConfig, WorkerService,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

fn main() -> Result<()> {
    let cfg = Config::parse();
    if !cfg!(target_os = "linux") {
        bail!("hivekeep-sandbox-gvisor is only supported on Linux");
    }
    // One TLS provider for the whole process.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rt = tokio::runtime::Runtime::new()?;
    let telemetry = rt.block_on(async { init_telemetry(&cfg.log_level) })?;
    let result = rt.block_on(run(cfg));
    telemetry.shutdown();
    if let Err(e) = result {
        tracing::error!(err = format!("{e:#}"), "Error while executing");
        std::process::exit(1);
    }
    Ok(())
}

/// How much of the pod's memory limit the worker keeps for itself rather
/// than offering to Actors.
const MEMORY_RESERVED_BYTES: i64 = 64 << 20;

/// JSON logs on stdout at `level`, and OTLP export through the keeper's
/// node relay socket when it is mounted (or a configured endpoint).
fn init_telemetry(level: &str) -> Result<hivekeep_telemetry::Telemetry> {
    hivekeep_telemetry::init(hivekeep_telemetry::InitOptions {
        service_name: "hivekeep-sandbox-gvisor".into(),
        log_level: level.to_string(),
        metrics: hivekeep_telemetry::MetricsMode::PushOnly,
        relay_socket: Some(hivekeep_telemetry::otlprelay::DEFAULT_SOCKET_PATH.into()),
        ..Default::default()
    })
    .with_context(|| format!("invalid --log-level {level:?}"))
}

async fn run(cfg: Config) -> Result<()> {
    cfg.validate().map_err(anyhow::Error::msg)?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "worker booting");
    let cancel = CancellationToken::new();
    let _stop_background = cancel.clone().drop_guard();

    // The keeper owns the worker directory; only the socket is ours.
    let worker_dir = node_path::worker_path(&cfg.pod_uid);
    actor_path::create_private_dir(&worker_dir)
        .with_context(|| format!("creating {}", worker_dir.display()))?;

    // Let runsc create sandbox leaves with real accounting under the pod scope.
    cgroup::delegate(&cfg.cgroup_root).context("while setting up cgroup delegation")?;
    ckpt_cgroup::remove_stale_checkpoint_cgroups(&cfg.cgroup_root);

    let sock = node_path::worker_socket_path(&cfg.pod_uid);
    match std::fs::remove_file(&sock) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("while removing {}", sock.display())),
    }
    let listener = tokio::net::UnixListener::bind(&sock).context("while opening unix socket")?;

    let flags = FlagStore::open(
        &cfg.runsc_flags_path(),
        DEFAULT_POLL_INTERVAL,
        cancel.clone(),
    )?;
    let throttle_memory = memory_watch::psi_enabled(&cfg.cgroup_root);
    if !throttle_memory {
        tracing::warn!("Kernel has no cgroup memory pressure (PSI), so sandboxes keep memory.max and an OOM kill in one can restart the worker");
    }
    if cfg.pool_namespace.is_empty() || cfg.pool_name.is_empty() {
        tracing::warn!("Worker pool unknown; usage records will name no pool");
    }
    // gVisor claims every interface of its namespace, so each Actor's
    // runtime namespace is joined to its gateway by a veth pair.
    let tunnel = cfg
        .node
        .start_tunnel(true, cancel.clone())
        .await
        .map_err(anyhow::Error::msg)?;
    let dial = cfg.node.keeper_dial();
    let crash_reporter: Option<Arc<dyn CrashReporter>> =
        match KeeperCrashReporter::new(dial.clone()) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::error!(err = %e, "Crashed actors will not be reported");
                None
            }
        };
    let svc = WorkerService::new(ServiceConfig {
        tunnel: Arc::new(tunnel),
        crash_reporter,
        max_actors: cfg.max_actors,
        cgroup_root: cfg.cgroup_root.clone(),
        throttle_memory,
        flags,
        usage: Some(Arc::new(LogUsageSink {
            pool_namespace: cfg.pool_namespace.clone(),
            pool_name: cfg.pool_name.clone(),
        })),
        ..Default::default()
    });

    // Registration is what lets the control plane place Actors here.
    match cfg.standalone_epoch {
        Some(e) if e > 0 => svc.epoch.store(e),
        Some(e) => bail!("--standalone-epoch must be positive, got {e}"),
        None => {
            let report = ReportConfig {
                dial,
                actors: cfg.max_actors as i32,
                pod_ips: register::parse_pod_ips(&cfg.pod_ips),
                memory_reserved_bytes: MEMORY_RESERVED_BYTES,
                actor_overhead: None,
                sandbox_class: hivekeep_core::sandbox_class::GVISOR.into(),
                runtimes_file: node_path::worker_sandbox_runtimes_path(&cfg.pod_uid),
                capacity_dir: cfg.node.capacity_dir.clone(),
            };
            let (epoch, c) = (svc.epoch.clone(), cancel.clone());
            tokio::spawn(async move {
                match register::report(report, epoch, c).await {
                    Ok(()) | Err(RegisterError::Cancelled) => {}
                    Err(e) => {
                        tracing::error!(err = %e, "Failed to register worker");
                        std::process::exit(1);
                    }
                }
            });
        }
    }

    spawn_usage_sampler(svc.clone(), cfg.usage_sample_interval, cancel.clone());
    if throttle_memory {
        tokio::spawn(svc.clone().watch_memory(cancel.clone()));
    }

    let ready = Arc::new(AtomicBool::new(true));
    tokio::spawn(serve_health(
        cfg.readiness_listen_address.clone(),
        svc.clone(),
        ready.clone(),
        cancel.clone(),
    ));

    // SIGTERM starts the pod's grace period: pass it into the sandboxes so
    // Actors can save state, then stop serving once the drain is done.
    let drained = CancellationToken::new();
    {
        let svc = svc.clone();
        let drained = drained.clone();
        tokio::spawn(async move {
            let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                tracing::error!("Cannot handle SIGTERM");
                return;
            };
            term.recv().await;
            tracing::info!("Received signal; beginning graceful shutdown");
            ready.store(false, Ordering::SeqCst);
            svc.graceful_shutdown().await;
            drained.cancel();
        });
    }

    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    let served = tonic::transport::Server::builder()
        .layer(hivekeep_telemetry::interceptors::GrpcLogLayer::internal())
        .add_service(WorkerServer::new(WorkerApi(svc)))
        .serve_with_incoming_shutdown(incoming, drained.cancelled_owned())
        .await;
    if let Err(e) = std::fs::remove_file(&sock) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::error!(err = %e, "Failed to remove the worker socket on shutdown");
        }
    }
    served.context("while serving")
}

fn spawn_usage_sampler(
    svc: Arc<WorkerService>,
    every: std::time::Duration,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => svc.sweep_usage().await,
            }
        }
    });
}

/// `/healthz` answers while the process runs; `/readyz` once the worker has
/// registered and until it starts draining.
async fn serve_health(
    addr: String,
    svc: Arc<WorkerService>,
    ready: Arc<AtomicBool>,
    cancel: CancellationToken,
) {
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr, err = %e, "Cannot serve health probes");
            return;
        }
    };
    loop {
        let conn = tokio::select! {
            _ = cancel.cancelled() => return,
            c = listener.accept() => c,
        };
        let Ok((mut conn, _)) = conn else { continue };
        let ok = ready.load(Ordering::SeqCst) && svc.epoch.registered();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = conn.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req.split_whitespace().nth(1).unwrap_or("/");
            let (status, body) = match path {
                "/healthz" => ("200 OK", "ok"),
                "/readyz" if ok => ("200 OK", "ok"),
                "/readyz" => ("503 Service Unavailable", "not ready"),
                _ => ("404 Not Found", "not found"),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = conn.write_all(resp.as_bytes()).await;
        });
    }
}
