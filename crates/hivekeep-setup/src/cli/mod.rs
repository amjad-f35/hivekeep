//! The `hivekeep-setup` command tree and the per-run sequence around every
//! command: parse, validate what needs no configuration, load and report the
//! configuration, connect (unless the command needs no cluster), run, and
//! record the run.

mod annotations;
mod options;
mod record;
mod run;
mod tree;

pub use annotations::{is_deploy, needs_cluster, records_run};
pub use options::{benchmark_options, deploy_options};
pub use record::record_run;
pub use tree::{all_paths, find, register_settings, root, NO_REPORT_DEFAULTS_ENV};

use crate::config::{self, ConfigError, Flags, LoadOptions, ReportOptions};
use crate::gcp::GcpError;
use crate::kube;
use crate::steps::csi::validate_csi_driver;
use crate::steps::{Env, StepError};
use clap::parser::ValueSource;
use clap::ArgMatches;
use std::ffi::OsString;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Step(#[from] StepError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Gcp(#[from] GcpError),
    #[error(transparent)]
    Kube(#[from] kube::KubeError),
    /// A command line that does not parse.
    #[error("{0}")]
    Usage(String),
    /// Already printed, together with the record of the run that failed;
    /// the caller only sets the exit status.
    #[error("{0}")]
    Reported(String),
}

/// Walks the parse to the leaf command: its canonical path (without the
/// program name) and its matches.
fn leaf(matches: &ArgMatches) -> (String, &ArgMatches) {
    let mut words = Vec::new();
    let mut cur = matches;
    while let Some((name, sub)) = cur.subcommand() {
        words.push(name.to_string());
        cur = sub;
        if words.first().map(String::as_str) == Some("gcp") {
            break;
        }
    }
    (words.join(" "), cur)
}

/// The checks that need neither configuration nor a cluster, run before
/// either is touched: a typo must not cost a credential fetch or leave a
/// half-installed cluster.
pub fn validate_args(path: &str, matches: &ArgMatches) -> Result<(), CliError> {
    match path {
        "setup csi" => Ok(validate_csi_driver(&run::csi_driver(matches))?),
        tree_path if tree_path == tree::DEPLOY_SYSTEM_PATH => {
            if matches.value_source("setup-csi") == Some(ValueSource::CommandLine) {
                if let Some(d) = matches.get_one::<String>("setup-csi") {
                    validate_csi_driver(d)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Parses `args` (program name first). Help and version requests are
/// printed here and come back as `None`.
pub fn parse<I, T>(args: I) -> Result<Option<ArgMatches>, CliError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    match root().try_get_matches_from(args) {
        Ok(m) => Ok(Some(m)),
        Err(e) => {
            use clap::error::ErrorKind::*;
            match e.kind() {
                DisplayHelp | DisplayVersion | DisplayHelpOnMissingArgumentOrSubcommand => {
                    let _ = e.print();
                    Ok(None)
                }
                _ => Err(CliError::Usage(
                    e.render().to_string().trim_end().to_string(),
                )),
            }
        }
    }
}

/// Runs one invocation. `cancel` stops retries and waits when cancelled.
pub async fn execute<I, T>(args: I, cancel: CancellationToken) -> Result<(), CliError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let Some(matches) = parse(args)? else {
        return Ok(());
    };
    let (path, sub) = leaf(&matches);

    if path == "gcp" || path.starts_with("gcp ") {
        let gcp_matches = matches.subcommand_matches("gcp").expect("gcp subcommand");
        return Ok(crate::gcp::run(gcp_matches).await?);
    }
    validate_args(&path, sub)?;

    let flags = Flags::from_matches(sub);
    let cfg = config::load(
        &flags,
        LoadOptions {
            config_path: sub.get_one::<String>("config").map(PathBuf::from),
            no_dev_env: sub.get_flag("no-dev-env"),
            root: None,
        },
    )?;
    let resolved = cfg.resolved().clone();

    // Reported before the cluster is touched, so the settings are on screen
    // even when connecting fails.
    let omit_defaults = sub.get_flag("no-report-defaults")
        || std::env::var_os(NO_REPORT_DEFAULTS_ENV).is_some_and(|v| !v.is_empty());
    resolved.report(&mut std::io::stdout(), ReportOptions { omit_defaults });

    let env = match prepare(cfg, &path, cancel).await {
        Ok(env) => env,
        Err(e) => return Err(fail(&path, &resolved, None, e)),
    };
    match run::run(&path, sub, &env).await {
        Ok(()) => {
            record_run(&path, Some(&resolved), &env.hivekeep_version(), false);
            Ok(())
        }
        Err(e) => Err(fail(&path, &resolved, Some(&env), e)),
    }
}

/// Builds the environment the command runs in.
async fn prepare(
    cfg: config::Config,
    path: &str,
    cancel: CancellationToken,
) -> Result<Env, CliError> {
    if !needs_cluster(path) {
        return Ok(Env::local(cfg));
    }
    // Populate the kubeconfig before any client is built.
    cfg.ensure_cluster_credentials().await?;
    if cfg.kind && is_deploy(path) {
        cfg.check_kind_cluster().await?;
    }
    let client = kube::Client::connect(&cfg.kubeconfig, &cfg.context)
        .await
        .map_err(StepError::from)?
        .with_cancellation(cancel);
    Ok(Env::with_client(cfg, client))
}

/// Prints the error, then the record of the failed run, so the retry line is
/// the last thing on screen.
fn fail(path: &str, resolved: &config::Resolved, env: Option<&Env>, e: CliError) -> CliError {
    let msg = e.to_string();
    eprintln!("Error: {msg}");
    let version = env.map(|e| e.hivekeep_version()).unwrap_or_default();
    record_run(path, Some(resolved), &version, true);
    CliError::Reported(msg)
}

#[cfg(test)]
mod tests;
