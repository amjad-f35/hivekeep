//! egress-bench: the workload of the egress scale test, which holds many
//! tunnels open through the egress gateway at a fixed pace and measures what
//! they cost. The actor holds one tunnel per endpoint open and counts what
//! happened; the target is the endpoint every tunnel leads to.

use clap::{Parser, Subcommand};
use hivekeep_bench::egress::{actor, target};

#[derive(Parser)]
#[command(name = "egress-bench", version = hivekeep_bench::boot::version(), about = "The actor and target of the egress scale test")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Hold one tunnel per endpoint open through the gateway and count what happened.
    Actor {
        /// Address to serve the readyz, start, stats, collect and stop routes on.
        #[arg(long, default_value = ":80")]
        listen: String,
    },
    /// Answer every request the actors' tunnels carry with a small fixed body.
    Target(target::TargetOptions),
}

#[tokio::main]
async fn main() {
    hivekeep_bench::boot::install_crypto_provider();
    let _telemetry = hivekeep_bench::boot::init_logging("egress-bench");
    let cli = Cli::parse();
    let res = match cli.command {
        Command::Actor { listen } => actor::run(&listen).await,
        Command::Target(o) => target::run(o).await,
    };
    if let Err(e) = res {
        eprintln!("egress-bench: {e:#}");
        std::process::exit(1);
    }
}
