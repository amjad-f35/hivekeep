//! `hivectl`: manage Actors, templates, Hives, Tags, Workers and policies in
//! a Hivekeep deployment.

use std::process::ExitCode;

use clap::Parser;
use hivekeep_ctl::app;
use hivekeep_ctl::cli::Cli;
use hivekeep_ctl::io::Io;

#[tokio::main]
async fn main() -> ExitCode {
    // ring is the workspace's TLS provider; install it rather than leave
    // the choice to whatever the dependency graph links.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let argv: Vec<String> = std::env::args().collect();
    let cli = match Cli::try_parse_from(&argv) {
        Ok(cli) => cli,
        Err(e) => {
            let report = app::parse_failure_report(&e, &argv);
            if e.use_stderr() {
                eprint!("{report}");
            } else {
                print!("{report}");
            }
            return ExitCode::from(e.exit_code() as u8);
        }
    };
    // Diagnostics go to stderr as text; -v raises the level. HK_LOG_LEVEL,
    // HK_LOG_FORMAT and RUST_LOG still apply when no -v is given.
    let mut telemetry_opts = hivekeep_telemetry::InitOptions::cli("hivectl", &Default::default());
    telemetry_opts.log_level = match cli.global.verbosity() {
        0 if std::env::var_os("HK_LOG_LEVEL").is_some()
            || std::env::var_os("RUST_LOG").is_some() =>
        {
            String::new()
        }
        0 => "warn".into(),
        1 | 2 => "info".into(),
        _ => "debug".into(),
    };
    let _telemetry = hivekeep_telemetry::init(telemetry_opts);
    let result = tokio::select! {
        r = app::run(cli, Io::process()) => r,
        _ = tokio::signal::ctrl_c() => return ExitCode::from(130),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprint!("{}", app::failure_report(&e, &argv));
            ExitCode::FAILURE
        }
    }
}
