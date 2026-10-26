//! locust-worker joins a locust master as a regular worker and runs one
//! Hivekeep load class: virtual users that drive Actors through their
//! lifecycle and send traffic through the gate router.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hivekeep_bench::boot;
use hivekeep_bench::loadgen::classes::{agentsession, registry};
use hivekeep_bench::loadgen::client::{
    default_api_endpoint, default_router_url, dial_control, router_http_client, router_server_name,
    router_tls_config, POD_IDENTITY_CRED_BUNDLE, SERVICE_DNS_CA_FILE,
};
use hivekeep_bench::loadgen::dynconfig::{start_poll, subscribe_spawn, ProbabilityUpdater};
use hivekeep_bench::loadgen::events::Events;
use hivekeep_bench::loadgen::locust::{runner::run_once, Runner, ZmqTransport};
use hivekeep_bench::loadgen::userclass::Config;
use hivekeep_bench::loadgen::{metrics, trace};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(
    name = "locust-worker",
    version,
    about = "Runs one Hivekeep load class as a locust worker."
)]
struct Args {
    /// hivekeep-api gRPC dial target.
    #[arg(long, default_value_t = default_api_endpoint())]
    api_endpoint: String,
    /// Gate router HTTPS base URL (no trailing slash).
    #[arg(long, default_value_t = default_router_url())]
    router_url: String,
    /// Hive every Actor this worker creates lives in.
    #[arg(long, default_value = "benchmark")]
    hive: String,
    /// Address of the Prometheus /metrics endpoint.
    #[arg(long, default_value = ":8001")]
    prometheus_addr: String,
    /// Initial runtime config as a JSON object keyed by locust flag name in
    /// snake_case; unset keys keep the class's defaults.
    #[arg(long, default_value = "")]
    config_json: String,
    /// When non-zero, fetch the runtime config from
    /// http://{master-host}:{port}/loadgen-config on each spawn message. The
    /// first fetch must succeed; later failures keep the last values.
    #[arg(long, default_value_t = 0)]
    master_web_port: u16,
    /// With --master-web-port, also fetch the config on this interval (0
    /// stops polling).
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    config_poll_interval: Duration,
    /// Load class to run.
    #[arg(long, default_value = "glutton")]
    user_class: String,
    /// Actors each user creates and cycles through round-robin.
    #[arg(long, default_value_t = 1)]
    actors_per_user: i64,
    /// Validate this agent-session script and exit.
    #[arg(long)]
    check_agentsession_script: Option<PathBuf>,
    /// Actors to create in the batch (spawn benchmark).
    #[arg(long, default_value_t = 100)]
    total_actors: i64,
    /// Actors created concurrently (spawn benchmark).
    #[arg(long, default_value_t = 1)]
    spawn_concurrency: i64,
    /// Per-Actor deadline in seconds (spawn benchmark).
    #[arg(long, default_value_t = 120.0)]
    actor_deadline: f64,
    /// Idle router connections kept per host; at least the user count.
    #[arg(long, default_value_t = 10000)]
    http_max_idle_conns_per_host: usize,
    /// The locust master's host.
    #[arg(long, default_value = "127.0.0.1")]
    master_host: String,
    /// The locust master's port.
    #[arg(long, default_value_t = 5557)]
    master_port: u16,
    /// Run one iteration of the class without a master, then clean up.
    #[arg(long)]
    run_tasks: bool,
}

fn fatal(msg: &str, err: impl std::fmt::Display) -> ExitCode {
    tracing::error!(err = %err, "fatal: {msg}");
    eprintln!("locust-worker: {msg}: {err}");
    ExitCode::FAILURE
}

#[tokio::main]
async fn main() -> ExitCode {
    boot::install_crypto_provider();
    let args = Args::parse();
    if args.actors_per_user < 1 {
        return fatal("--actors-per-user must be >= 1", args.actors_per_user);
    }
    if args.total_actors < 1 {
        return fatal("--total-actors must be >= 1", args.total_actors);
    }
    if args.spawn_concurrency < 1 {
        return fatal("--spawn-concurrency must be >= 1", args.spawn_concurrency);
    }
    if args.actor_deadline <= 0.0 || !args.actor_deadline.is_finite() {
        return fatal("--actor-deadline must be > 0", args.actor_deadline);
    }
    if !args.router_url.starts_with("https://") {
        return fatal("--router-url must use https://", &args.router_url);
    }
    let _telemetry = boot::init_logging("locust-worker");

    if let Some(path) = &args.check_agentsession_script {
        return match agentsession::load_file(path) {
            Ok(s) => {
                let b = agentsession::budgets(&s.steps);
                println!(
                    "agent-session script {:?}: {} steps, declares {} RAM + {} disk, min_actor_memory {}",
                    s.name,
                    s.steps.len(),
                    agentsession::format_size(b.ram),
                    agentsession::format_size(b.disk),
                    agentsession::format_size(s.min_actor_memory)
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }

    let classes = registry();
    let class = args.user_class.to_lowercase();
    let Some(entry) = classes.lookup(&class).cloned() else {
        return fatal(
            "unknown --user-class value",
            format!("{} (known: {})", args.user_class, classes.names().join(",")),
        );
    };

    // The holder is built from the class's own codec, so a --config-json
    // the class cannot run on fails here, and a bad value from the master
    // later is refused in favour of the last good one.
    let holder = Arc::new(entry.holder());
    if !args.config_json.is_empty() {
        if let Err(e) = holder.apply(args.config_json.as_bytes()) {
            return fatal("failed to parse --config-json", e);
        }
    }
    let sampler = Arc::new(trace::Sampler::new(holder.common().trace_probability));
    let tracer = match trace::init("hivekeep-loadgen", sampler.clone()) {
        Ok(t) => t,
        Err(e) => return fatal("failed to initialize tracing", e),
    };

    let (api, _conn) = match dial_control(&args.api_endpoint).await {
        Ok(c) => c,
        Err(e) => return fatal("failed to dial hivekeep-api", e),
    };
    let tls = match router_tls_config(
        SERVICE_DNS_CA_FILE,
        POD_IDENTITY_CRED_BUNDLE,
        &router_server_name(),
    ) {
        Ok(t) => t,
        Err(e) => return fatal("failed to configure router TLS", e),
    };
    let http = match router_http_client(tls, args.http_max_idle_conns_per_host) {
        Ok(c) => c,
        Err(e) => return fatal("failed to build the router client", e),
    };

    let events = Events::new();
    let cancel = CancellationToken::new();
    if args.master_web_port > 0 {
        let url = format!(
            "http://{}:{}/loadgen-config",
            args.master_host, args.master_web_port
        );
        let u = url.clone();
        subscribe_spawn(
            &events,
            url.clone(),
            holder.clone(),
            sampler.clone() as Arc<dyn ProbabilityUpdater>,
            Duration::from_secs(5),
            Arc::new(move |err, fetched| {
                if fetched {
                    tracing::warn!(url = %u, %err, "dynconfig spawn fetch failed; keeping the current values");
                } else {
                    tracing::error!(url = %u, %err, "fatal: first dynconfig fetch failed; exiting so the pod restarts");
                    std::process::exit(1);
                }
            }),
        );
        let u = url.clone();
        start_poll(
            cancel.clone(),
            url.clone(),
            holder.clone(),
            sampler.clone(),
            args.config_poll_interval,
            Duration::from_secs(5),
            Arc::new(
                move |err| tracing::warn!(url = %u, %err, "dynconfig poll failed; keeping the current values"),
            ),
        );
        tracing::info!(%url, poll_interval = ?args.config_poll_interval, "dynconfig fetch enabled");
    }

    let cfg = Arc::new(Config {
        api: Arc::new(api),
        http,
        router_url: args.router_url.clone(),
        hive: args.hive.clone(),
        dyn_config: holder,
        tracer,
        actors_per_user: args.actors_per_user as usize,
        total_actors: args.total_actors as usize,
        spawn_concurrency: args.spawn_concurrency as usize,
        actor_deadline: Duration::from_secs_f64(args.actor_deadline),
    });
    let task = (entry.init)(cfg);
    tracing::info!(user_class = entry.user_class, user_class_flag = %class, "registered load class");

    let metrics_task =
        match boot::serve_metrics(&args.prometheus_addr, metrics::registry().clone()).await {
            Ok(t) => t,
            Err(e) => return fatal("failed to start the metrics listener", e),
        };

    if args.run_tasks {
        run_once(&task).await;
    } else {
        let runner = Runner::new(entry.user_class, task.clone(), events);
        let mut transport = match ZmqTransport::connect(
            &args.master_host,
            args.master_port,
            runner.node_id(),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => return fatal("failed to connect to the locust master", e),
        };
        match runner.run(&mut transport, boot::shutdown_signal()).await {
            Ok(exit) => tracing::info!(?exit, "run ended; suspending and deleting actors"),
            Err(e) => {
                tracing::error!(err = %e, "connection to the master failed; suspending and deleting actors")
            }
        }
    }
    cancel.cancel();
    if tokio::time::timeout(Duration::from_secs(60), task.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("class shutdown did not finish within 60s");
    }
    metrics_task.abort();
    ExitCode::SUCCESS
}
