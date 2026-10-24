//! `sandbox-client`: resumes a sandbox Actor, offers a prompt that runs each
//! line in it, and suspends the Actor when the session ends.

use anyhow::Context;
use clap::Parser;
use hivekeep_demo_sandbox::client::{self, classify, ActorRef, Line};
use hivekeep_proto::api::v1 as pb;
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser, Debug)]
#[command(name = "sandbox-client")]
struct Args {
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
    /// Name of the sandbox Actor.
    #[arg(long)]
    name: String,
    /// Hive the Actor lives in.
    #[arg(long)]
    hive: String,
    /// hivekeep-api gRPC endpoint, e.g. a port-forward to the `api` Service.
    #[arg(long, env = "HK_API_ENDPOINT")]
    api: String,
    /// Bearer token for hivekeep-api.
    #[arg(long, env = "HK_API_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Address of the local gate proxy (`hivectl proxy actor ...`).
    #[arg(long, default_value = "localhost:8000")]
    gate: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::cli(
        "sandbox-client",
        &args.telemetry,
    ))
    .map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    let actor = ActorRef {
        hive: args.hive,
        name: args.name,
    };

    eprintln!("Connecting to hivekeep-api...");
    let mut api = client::connect(&args.api, args.token.as_deref()).await?;
    eprintln!("Resuming actor {actor}...");
    api.resume_actor(pb::ResumeActorRequest {
        actor: Some(actor.to_object_ref()),
    })
    .await
    .context("resuming actor")?;
    eprintln!("Actor resumed.");

    let outcome = repl(&args.gate, &actor).await;

    eprintln!("Suspending actor {actor}...");
    match api
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(actor.to_object_ref()),
            options: None,
        })
        .await
    {
        Ok(_) => eprintln!("Actor suspended."),
        Err(e) => eprintln!("Failed to suspend actor: {e}"),
    }
    outcome
}

async fn repl(gate: &str, actor: &ActorRef) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    println!("Sandbox prompt ready. Type 'exit' to leave.");
    loop {
        print!("sandbox> ");
        std::io::stdout().flush().ok();
        let line = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupt received, shutting down...");
                return Ok(());
            }
            l = lines.next_line() => l.context("reading standard input")?,
        };
        let Some(line) = line else { return Ok(()) };
        match classify(&line) {
            Line::Skip => continue,
            Line::Exit => return Ok(()),
            Line::Run(cmd) => match client::run_command(&http, gate, actor, &cmd).await {
                Ok(out) => {
                    if !out.error.is_empty() {
                        println!("Command error: {}", out.error);
                    }
                    print!("{}{}", out.stdout, out.stderr);
                }
                Err(e) => println!("Error: {e}"),
            },
        }
    }
}
