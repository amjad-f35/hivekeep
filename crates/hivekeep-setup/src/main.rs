//! `hivekeep-setup`: installs and tears down Hivekeep on a Kubernetes
//! cluster. See `hivekeep-setup --help`.

use hivekeep_setup::cli::{self, CliError};
use tokio_util::sync::CancellationToken;

fn main() {
    if let Err(e) = real_main() {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}

fn real_main() -> anyhow::Result<()> {
    // Several TLS stacks are linked; pick one explicitly.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("could not install the rustls crypto provider"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Diagnostics go to stderr as text (the run record is the
        // output); HK_LOG_LEVEL / HK_LOG_FORMAT / RUST_LOG adjust them.
        let mut opts = hivekeep_telemetry::InitOptions::cli("hivekeep-setup", &Default::default());
        if std::env::var_os("HK_LOG_LEVEL").is_none() && std::env::var_os("RUST_LOG").is_none() {
            opts.log_level = "warn".into();
        }
        let _telemetry = hivekeep_telemetry::init(opts);
        let cancel = CancellationToken::new();
        let run = cli::execute(std::env::args_os(), cancel.clone());
        tokio::select! {
            r = run => match r {
                Ok(()) => Ok(()),
                // Already printed alongside the record of the run.
                Err(CliError::Reported(_)) => std::process::exit(1),
                Err(e) => Err(e.into()),
            },
            _ = tokio::signal::ctrl_c() => {
                cancel.cancel();
                Err(anyhow::anyhow!("interrupted"))
            }
        }
    })
}
