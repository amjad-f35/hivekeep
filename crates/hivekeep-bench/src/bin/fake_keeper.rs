//! fake-keeper: stands in for the keeper in control-plane benchmarks. It
//! answers every keeper call with success after a delay, without running or
//! saving any workload, and relays fake-workersync's capacity reports for
//! the fake Workers on its node. Never run it on a cluster that serves real
//! Actors.

use hivekeep_bench::isolate::fake_keeper::{app, config};

#[tokio::main]
async fn main() {
    hivekeep_bench::boot::install_crypto_provider();
    let argv: Vec<String> = std::env::args().collect();
    let args = match config::parse_with_tuning(&argv, |k| std::env::var(k).ok()) {
        Ok(a) => a,
        Err(e) => e.exit(),
    };
    if let Err(err) = app::run(args).await {
        tracing::error!(err = format!("{err:#}"), "fake-keeper failed");
        eprintln!("fake-keeper: {err:#}");
        std::process::exit(1);
    }
}
