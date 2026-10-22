//! `hivekeep-apitool validate` checks the public API definition against the
//! project's API style rules.
//!
//! Findings can be exempted in `crates/hivekeep-devtools/exemptions.json`. The
//! matching is strict: both an extra exemption and a missing one fail, so the
//! file always reflects the real state of the API. `validate --update` rewrites
//! it to match every current finding; review its diff like any other change.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};

use hivekeep_devtools::apitool::exemption::{self, Set};
use hivekeep_devtools::apitool::lint;
use hivekeep_devtools::apitool::validate;

#[derive(Parser)]
#[command(
    name = "hivekeep-apitool",
    about = "Validates the Hivekeep API definition against the API style rules"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the API shape rules against api.proto.
    Validate {
        /// Instead of validating, rewrite the exemptions file to match every current finding.
        #[arg(long)]
        update: bool,
        /// Lint this .proto file instead of the embedded API definition.
        #[arg(long)]
        proto: Option<PathBuf>,
        /// Exemptions file (default: found from the workspace root).
        #[arg(long)]
        exemptions: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let _telemetry = hivekeep_telemetry::init(hivekeep_telemetry::InitOptions::cli(
        "hivekeep-apitool",
        &Default::default(),
    ));
    let result = match cli.command {
        None => {
            let _ = Cli::command().print_help();
            Ok(())
        }
        Some(Command::Validate {
            update,
            proto,
            exemptions,
        }) => run_validate(update, proto, exemptions),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run_validate(
    update: bool,
    proto: Option<PathBuf>,
    exemptions_path: Option<PathBuf>,
) -> Result<()> {
    let api = match &proto {
        Some(p) => {
            let source =
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            validate::api_from_source(&source)?
        }
        None => validate::api()?,
    };
    let path = match exemptions_path {
        Some(p) => p,
        None => validate::default_exemptions_path()?,
    };
    let results = validate::all(&api)?;
    let mut out = std::io::stdout().lock();

    if update {
        let current = validate::as_exemptions(&results);
        exemption::save(&path, &current)?;
        writeln!(
            out,
            "wrote {} exemption(s) to {}",
            current.len(),
            path.display()
        )?;
        return Ok(());
    }

    let exemptions = exemption::load(&path)?;
    if let Err(e) = exemption::check_sorted(&exemptions) {
        bail!(
            "{}: {e}; run `hivekeep-apitool validate --update` to restore canonical order",
            path.display()
        );
    }
    let mut remaining = Set::new(&exemptions);

    let service = api
        .services
        .first()
        .map(|s| s.name.as_str())
        .unwrap_or("no service");
    writeln!(
        out,
        "Validating {} ({service}), {} rule(s), {} exemption(s)...\n",
        validate::API_FILE_NAME,
        lint::ALL.len(),
        exemptions.len()
    )?;

    let (mut total, mut failed_rules, mut exempted) = (0usize, 0usize, 0usize);
    for r in &results {
        let mut shown = Vec::new();
        for f in &r.findings {
            if remaining.consume(r.rule.name, &f.subject, &f.message) {
                exempted += 1;
            } else {
                shown.push(f);
            }
        }
        if shown.is_empty() {
            writeln!(out, "  {} {}", pass_icon(), r.rule.name)?;
            continue;
        }
        failed_rules += 1;
        total += shown.len();
        writeln!(out, "  {} {}", fail_icon(), r.rule.name)?;
        writeln!(out, "    {}", r.rule.description)?;
        for f in shown {
            writeln!(out, "      {}: {}", f.subject, f.message)?;
        }
    }
    writeln!(out)?;

    let stale = remaining.unused();
    if !stale.is_empty() {
        writeln!(
            out,
            "{} {} exemption(s) no longer match any finding - remove them by running `hivekeep-apitool validate --update`:",
            fail_icon(),
            stale.len()
        )?;
        for e in &stale {
            writeln!(out, "      [{}] {}: {}", e.rule, e.subject, e.message)?;
        }
        writeln!(out)?;
    }

    if total == 0 && stale.is_empty() {
        writeln!(
            out,
            "{} all {} rules passed ({exempted} finding(s) exempted)",
            pass_icon(),
            lint::ALL.len()
        )?;
        return Ok(());
    }
    if total > 0 {
        writeln!(
            out,
            "Fix the finding(s) above, or if they're intentional for now, run `hivekeep-apitool validate --update` to add them to the exemptions file.\n"
        )?;
    }
    writeln!(
        out,
        "{} {total} finding(s) across {failed_rules} rule(s), {} stale exemption(s)",
        fail_icon(),
        stale.len()
    )?;
    bail!("{total} finding(s), {} stale exemption(s)", stale.len())
}

const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_RED: &str = "\x1b[31m";
const ANSI_RESET: &str = "\x1b[0m";

/// Colors are on unless `NO_COLOR` is set to a non-empty value.
fn colorize(code: &str, s: &str) -> String {
    if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
        s.to_string()
    } else {
        format!("{code}{s}{ANSI_RESET}")
    }
}

fn pass_icon() -> String {
    colorize(ANSI_GREEN, "✓")
}

fn fail_icon() -> String {
    colorize(ANSI_RED, "✗")
}
