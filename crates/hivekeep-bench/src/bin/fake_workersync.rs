//! fake-workersync: keeps every WorkerPool's replicas as fake Workers no pod
//! backs, with capacity reported through the fake keeper on their node.
//! The real controller must not run alongside it.

use clap::Parser;
use hivekeep_bench::isolate::fake_workersync::app::{run, Args};

#[tokio::main]
async fn main() {
    hivekeep_bench::boot::install_crypto_provider();
    let args = Args::parse();
    if let Err(err) = run(args).await {
        tracing::error!(err = format!("{err:#}"), "fake-workersync failed");
        eprintln!("fake-workersync: {err:#}");
        std::process::exit(1);
    }
}
