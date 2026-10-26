//! Command line of the fake keeper and its benchmark tuning.
//!
//! The installer chooses the fake data plane; a benchmark tunes it through
//! `FAKE_KEEPER_<FLAG>` environment variables (from a tuning ConfigMap). A
//! flag given on the command line wins over its variable.

use std::time::Duration;

use clap::parser::ValueSource;
use clap::{ArgAction, CommandFactory, FromArgMatches, Parser};
use hivekeep_k8s::install_defaults::{spiffe_id, SYSTEM_NAMESPACE};

use crate::isolate::fakeworker::DEFAULT_RELAY_PORT;
use crate::wire::KEEPER_DEFAULT_PORT;

/// The flags a `FAKE_KEEPER_<FLAG>` variable can set.
pub const TUNING_FLAGS: &[&str] = &[
    "delay",
    "delay-run",
    "delay-restore",
    "delay-checkpoint",
    "delay-promote-snapshot",
    "delay-terminate",
    "mint-actor-certificate",
];

/// The environment variable that sets the tuning flag `name`.
pub fn tuning_env(name: &str) -> String {
    format!("FAKE_KEEPER_{}", name.replace('-', "_").to_uppercase())
}

/// A duration that may be negative, as the delay flags take: a negative
/// per-call override means "use `--delay`".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SignedDuration {
    /// Whether the duration is below zero.
    pub negative: bool,
    /// Its magnitude.
    pub magnitude: Duration,
}

impl SignedDuration {
    /// The "use the default" override.
    pub const UNSET: SignedDuration = SignedDuration {
        negative: true,
        magnitude: Duration::from_nanos(1),
    };

    /// A non-negative duration.
    pub fn of(d: Duration) -> SignedDuration {
        SignedDuration {
            negative: false,
            magnitude: d,
        }
    }
}

/// Parses `500ms`, `3s`, `1h30m`, `0` or a negative form such as `-1ns`.
pub fn parse_signed_duration(s: &str) -> Result<SignedDuration, String> {
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let magnitude = if rest == "0" {
        Duration::ZERO
    } else {
        humantime::parse_duration(rest).map_err(|e| format!("invalid duration {s:?}: {e}"))?
    };
    Ok(SignedDuration {
        negative: negative && !magnitude.is_zero(),
        magnitude,
    })
}

/// How long each keeper call takes before it succeeds: the data plane's
/// share of it, not counting an actor certificate mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Delays {
    /// Run.
    pub run: Duration,
    /// Restore.
    pub restore: Duration,
    /// Checkpoint.
    pub checkpoint: Duration,
    /// PromoteSnapshot.
    pub promote_snapshot: Duration,
    /// Terminate.
    pub terminate: Duration,
}

impl Delays {
    /// Every call takes `d`.
    pub fn uniform(d: Duration) -> Delays {
        Delays {
            run: d,
            restore: d,
            checkpoint: d,
            promote_snapshot: d,
            terminate: d,
        }
    }
}

/// Applies each per-call override over the default delay; a negative
/// override uses the default. A negative default is an error.
pub fn resolve_delays(
    def: SignedDuration,
    run: SignedDuration,
    restore: SignedDuration,
    checkpoint: SignedDuration,
    promote_snapshot: SignedDuration,
    terminate: SignedDuration,
) -> Result<Delays, String> {
    if def.negative {
        return Err(format!(
            "delay must not be negative, got -{:?}",
            def.magnitude
        ));
    }
    let pick = |d: SignedDuration| {
        if d.negative {
            def.magnitude
        } else {
            d.magnitude
        }
    };
    Ok(Delays {
        run: pick(run),
        restore: pick(restore),
        checkpoint: pick(checkpoint),
        promote_snapshot: pick(promote_snapshot),
        terminate: pick(terminate),
    })
}

fn default_relay_allowed_id() -> String {
    spiffe_id(SYSTEM_NAMESPACE, "fake-workersync")
}

/// The fake keeper's flags.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "fake-keeper",
    version,
    about = "Stands in for the keeper in control-plane benchmarks: every keeper call succeeds after a delay, and no workload runs."
)]
pub struct Args {
    /// Port to serve the keeper API on; hivekeep-api dials the keeper's
    /// default port.
    #[arg(long, default_value_t = KEEPER_DEFAULT_PORT)]
    pub port: u16,
    /// Port to serve fake-workersync's capacity reports on.
    #[arg(long, default_value_t = DEFAULT_RELAY_PORT)]
    pub relay_port: u16,
    /// The only SPIFFE ID allowed to send capacity reports.
    #[arg(long, default_value_t = default_relay_allowed_id())]
    pub relay_allowed_spiffe_id: String,
    /// Address to serve /healthz and /readyz on.
    #[arg(long, default_value = ":9091")]
    pub health_listen_addr: String,

    /// Credential bundle presented as the serving certificate and as the
    /// client certificate to hivekeep-api.
    #[arg(
        long,
        default_value = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"
    )]
    pub grpc_server_cred_bundle: String,
    /// CA bundle client certificates are verified against.
    #[arg(
        long,
        default_value = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub client_ca_certs: String,
    /// hivekeep-api address (`host:port`) for capacity reports and mints.
    #[arg(long, default_value = "hivekeep-api.hivekeep-system.svc:443")]
    pub api_address: String,
    /// CA bundle hivekeep-api's certificate is verified against.
    #[arg(
        long,
        default_value = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
    )]
    pub api_ca_file: String,
    /// DNS name expected on hivekeep-api's certificate.
    #[arg(long, default_value = "hivekeep-api.hivekeep-system.svc")]
    pub api_server_name: String,

    /// How long each keeper call takes before it succeeds, not counting the
    /// actor certificate mint on Run and Restore.
    #[arg(long, default_value = "0", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay: SignedDuration,
    /// Overrides --delay for Run; negative uses --delay.
    #[arg(long, default_value = "-1ns", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay_run: SignedDuration,
    /// Overrides --delay for Restore; negative uses --delay.
    #[arg(long, default_value = "-1ns", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay_restore: SignedDuration,
    /// Overrides --delay for Checkpoint; negative uses --delay.
    #[arg(long, default_value = "-1ns", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay_checkpoint: SignedDuration,
    /// Overrides --delay for PromoteSnapshot; negative uses --delay.
    #[arg(long, default_value = "-1ns", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay_promote_snapshot: SignedDuration,
    /// Overrides --delay for Terminate; negative uses --delay.
    #[arg(long, default_value = "-1ns", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    pub delay_terminate: SignedDuration,
    /// On each Run and Restore that names an egress gateway, mint the
    /// Actor's certificate from hivekeep-api before the delay, as the worker
    /// does before starting the workload.
    #[arg(long, default_value_t = true, action = ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub mint_actor_certificate: bool,

    /// Minimum log level: debug, info, warn or error.
    #[arg(long, default_value = "info")]
    pub log_level: String,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,
}

impl Args {
    /// The per-call delays these flags give.
    pub fn delays(&self) -> Result<Delays, String> {
        resolve_delays(
            self.delay,
            self.delay_run,
            self.delay_restore,
            self.delay_checkpoint,
            self.delay_promote_snapshot,
            self.delay_terminate,
        )
    }
}

/// Parses `argv` (program name first) with each tuning flag the command
/// line leaves unset taken from its variable, read through `lookup`. A bad
/// value in a variable is an error naming the variable.
pub fn parse_with_tuning(
    argv: &[String],
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Args, clap::Error> {
    let matches = Args::command().try_get_matches_from(argv)?;
    let mut extended = argv.to_vec();
    for name in TUNING_FLAGS {
        let id = name.replace('-', "_");
        if matches.value_source(&id) == Some(ValueSource::CommandLine) {
            continue;
        }
        let Some(value) = lookup(&tuning_env(name)) else {
            continue;
        };
        // Validate the variable alone first, so the error names it.
        let probe = vec![argv[0].clone(), format!("--{name}={value}")];
        if let Err(e) = Args::command().try_get_matches_from(&probe) {
            return Err(Args::command().error(
                clap::error::ErrorKind::ValueValidation,
                format!("{}={value:?}: {}", tuning_env(name), e.kind()),
            ));
        }
        extended.push(format!("--{name}={value}"));
    }
    let matches = Args::command().try_get_matches_from(&extended)?;
    Args::from_arg_matches(&matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn resolve_delays_cases() {
        let s = |d| SignedDuration::of(d);
        let got = resolve_delays(
            s(ms(1000)),
            SignedDuration::UNSET,
            s(ms(2000)),
            s(Duration::ZERO),
            SignedDuration::UNSET,
            SignedDuration::UNSET,
        )
        .unwrap();
        assert_eq!(
            got,
            Delays {
                run: ms(1000),
                restore: ms(2000),
                checkpoint: Duration::ZERO,
                promote_snapshot: ms(1000),
                terminate: ms(1000),
            }
        );
        let neg = parse_signed_duration("-1s").unwrap();
        assert!(resolve_delays(neg, neg, neg, neg, neg, neg).is_err());
    }

    fn argv(extra: &[&str]) -> Vec<String> {
        std::iter::once("fake-keeper")
            .chain(extra.iter().copied())
            .map(String::from)
            .collect()
    }

    // A benchmark tunes the fake keeper through FAKE_KEEPER_* variables; a
    // flag on the command line wins over its variable, and a bad value is an
    // error.
    #[test]
    fn apply_tuning_env() {
        let env: HashMap<&str, &str> = [
            ("FAKE_KEEPER_DELAY", "500ms"),
            ("FAKE_KEEPER_DELAY_RUN", "9s"),
            ("FAKE_KEEPER_MINT_ACTOR_CERTIFICATE", "false"),
            // Not a tuning flag, so not read.
            ("FAKE_KEEPER_PORT", "1"),
        ]
        .into();
        let args = parse_with_tuning(&argv(&["--delay-run=3s"]), |k| {
            env.get(k).map(|v| v.to_string())
        })
        .unwrap();
        assert_eq!(args.delay, SignedDuration::of(ms(500)));
        assert_eq!(args.delay_run, SignedDuration::of(ms(3000)));
        assert!(!args.mint_actor_certificate);
        assert_eq!(
            args.port, KEEPER_DEFAULT_PORT,
            "FAKE_KEEPER_PORT set --port, which is not a tuning flag"
        );

        let bad = parse_with_tuning(&argv(&[]), |k| {
            (k == "FAKE_KEEPER_DELAY").then(|| "fast".to_string())
        });
        assert!(bad.is_err(), "accepted FAKE_KEEPER_DELAY=fast");
    }

    // Every tuning flag is one the fake keeper registers, so a variable can
    // set it rather than failing startup.
    #[test]
    fn tuning_flags_registered() {
        let cmd = Args::command();
        for name in TUNING_FLAGS {
            assert!(
                cmd.get_arguments().any(|a| a.get_long() == Some(name)),
                "{name} is not a flag"
            );
        }
    }

    #[test]
    fn defaults() {
        let args = parse_with_tuning(&argv(&[]), |_| None).unwrap();
        assert_eq!(args.delays().unwrap(), Delays::default());
        assert!(args.mint_actor_certificate);
        assert_eq!(
            args.relay_allowed_spiffe_id,
            "spiffe://cluster.local/ns/hivekeep-system/sa/fake-workersync"
        );
        assert_eq!(
            tuning_env("delay-promote-snapshot"),
            "FAKE_KEEPER_DELAY_PROMOTE_SNAPSHOT"
        );
    }
}
