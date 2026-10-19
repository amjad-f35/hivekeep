//! The command line: `hivectl <verb> <resource> [args] [flags]`.

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::output::OutputFormat;
use crate::refs::SUSPEND_MODE_ASYNC;

/// Manage Actors, their templates, Hives, Tags, Workers and policies in a
/// Hivekeep deployment.
#[derive(Debug, Parser)]
#[command(name = "hivectl", version)]
pub struct Cli {
    /// Connection and output options, accepted by every command.
    #[command(flatten)]
    pub global: GlobalArgs,
    /// The command to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Options shared by every command.
#[derive(Debug, Clone, Args)]
pub struct GlobalArgs {
    /// hivekeep-api gRPC endpoint, as host:port or an http(s):// URI.
    #[arg(long, global = true, env = "HK_ENDPOINT")]
    pub endpoint: Option<String>,
    /// PEM bundle that hivekeep-api's serving certificate must chain to
    /// (defaults to the system roots).
    #[arg(long, global = true, env = "HK_CA_FILE")]
    pub ca_file: Option<PathBuf>,
    /// Name hivekeep-api's certificate must carry (defaults to its in-cluster
    /// Service DNS name).
    #[arg(long, global = true, env = "HK_TLS_SERVER_NAME")]
    pub tls_server_name: Option<String>,
    /// Connect without TLS; for local development only. No token is sent.
    #[arg(long, global = true)]
    pub plaintext: bool,
    /// File holding the bearer token to authenticate with, or - for stdin.
    #[arg(long, global = true, env = "HK_TOKEN_FILE")]
    pub token_file: Option<String>,
    /// Output format: table, json, or yaml.
    #[arg(short = 'o', long, global = true, default_value = "table", value_parser = parse_output)]
    pub output: OutputFormat,
    /// Propagate a trace context with every request and print its trace ID.
    #[arg(long, global = true)]
    pub trace: bool,
    /// Output verbosity; may be repeated (-v, -vv, -vvvvv).
    #[arg(short = 'v', global = true, action = ArgAction::Count)]
    pub verbose_count: u8,
    /// Output verbosity; may be repeated, or given a level (--verbose=4).
    #[arg(
        long = "verbose",
        global = true,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "1",
        action = ArgAction::Append,
        value_name = "LEVEL"
    )]
    pub verbose_levels: Vec<u8>,
    /// Allow the use of preview features enabled on the server.
    #[arg(long, global = true)]
    pub preview: bool,
    /// Kubeconfig for the commands that talk to the cluster (logs, top, admin).
    #[arg(long, global = true)]
    pub kubeconfig: Option<String>,
    /// Kubeconfig context for the commands that talk to the cluster.
    #[arg(long = "context", global = true)]
    pub kube_context: Option<String>,
}

impl GlobalArgs {
    /// The verbosity: one per `-v` or bare `--verbose`, plus any explicit
    /// `--verbose=N` levels.
    pub fn verbosity(&self) -> u8 {
        self.verbose_levels
            .iter()
            .fold(self.verbose_count, |v, l| v.saturating_add(*l))
    }
}

fn parse_output(s: &str) -> Result<OutputFormat, String> {
    s.parse()
}

/// The top-level verbs.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Display one or many resources.
    #[command(subcommand)]
    Get(GetCommand),
    /// Create a resource.
    #[command(subcommand)]
    Create(CreateCommand),
    /// Update a resource.
    #[command(subcommand)]
    Update(UpdateCommand),
    /// Delete a resource.
    #[command(subcommand)]
    Delete(DeleteCommand),
    /// Suspend a resource.
    #[command(subcommand)]
    Suspend(SuspendCommand),
    /// Resume a resource.
    #[command(subcommand)]
    Resume(ResumeCommand),
    /// Revert a resource.
    #[command(subcommand)]
    Revert(RevertCommand),
    /// Print the logs for a resource.
    #[command(subcommand)]
    Logs(LogsCommand),
    /// Display resource (CPU/memory) usage.
    #[command(subcommand)]
    Top(TopCommand),
    /// Drain a resource of its Actors.
    #[command(subcommand)]
    Drain(DrainCommand),
    /// Mark a resource available for placement.
    #[command(subcommand)]
    MarkAvailable(MarkAvailableCommand),
    /// Mark a resource unavailable for placement.
    #[command(subcommand)]
    MarkUnavailable(MarkUnavailableCommand),
    /// Serve a local proxy to a resource.
    #[command(subcommand)]
    Proxy(ProxyCommand),
    /// Administration and debugging commands.
    #[command(subcommand)]
    Admin(AdminCommand),
}

/// Selects one hive, or every hive, for a list.
#[derive(Debug, Clone, Default, Args)]
pub struct HiveScope {
    /// Hive to use (required when getting by name).
    #[arg(short = 'H', long)]
    pub hive: Option<String>,
    /// List across all hives.
    #[arg(short = 'A', long)]
    pub all_hives: bool,
}

/// A hive-scoped resource named by one positional argument.
#[derive(Debug, Clone, Args)]
pub struct HivedName {
    /// The resource name.
    pub name: String,
    /// The resource's hive.
    #[arg(short = 'H', long, required = true)]
    pub hive: String,
}

/// `get` subcommands.
#[derive(Debug, Subcommand)]
pub enum GetCommand {
    /// List all Actors or get one or more Actors.
    #[command(visible_alias = "actors")]
    Actor {
        /// Actors to get; lists when omitted.
        names: Vec<String>,
        #[command(flatten)]
        scope: HiveScope,
    },
    /// List all actor templates or get one or more.
    #[command(visible_alias = "actor-templates")]
    ActorTemplate {
        /// Templates to get; lists when omitted.
        names: Vec<String>,
        #[command(flatten)]
        scope: HiveScope,
    },
    /// List all Hives or get one or more Hives.
    #[command(visible_alias = "hives")]
    Hive {
        /// Hives to get; lists when omitted.
        names: Vec<String>,
    },
    /// List or get Tags.
    #[command(visible_alias = "tags")]
    Tag {
        /// Tags to get; lists when omitted.
        names: Vec<String>,
        #[command(flatten)]
        scope: HiveScope,
    },
    /// List all Workers or get one or more Workers.
    #[command(visible_alias = "workers")]
    Worker {
        /// Workers to get; lists when omitted.
        names: Vec<String>,
        #[command(flatten)]
        filter: WorkerFilter,
    },
    /// List the Actors assigned to a Worker.
    #[command(visible_alias = "assignments")]
    WorkerAssignments {
        /// The Worker.
        worker: String,
    },
    /// Get the egress policy of an Actor.
    #[command(visible_alias = "egress-policies")]
    EgressPolicy(HivedName),
    /// Get the access policy of a Hive, or the global access policy.
    #[command(visible_alias = "access-policies")]
    AccessPolicy {
        #[command(flatten)]
        scope: PolicyScopeArgs,
    },
}

/// Filters for listing Workers.
#[derive(Debug, Clone, Default, Args)]
pub struct WorkerFilter {
    /// Only Workers in this Kubernetes namespace.
    #[arg(short = 'n', long)]
    pub namespace: Option<String>,
    /// Only Workers hosting an Actor in this hive.
    #[arg(short = 'H', long)]
    pub hive: Option<String>,
    /// Only Workers whose pool labels match this selector.
    #[arg(short = 'l', long)]
    pub selector: Option<String>,
    /// Only Workers of this sandbox class (e.g. gvisor, microvm).
    #[arg(long)]
    pub sandbox_class: Option<String>,
}

/// Selects a Hive's access policy or the global one.
#[derive(Debug, Clone, Default, Args)]
pub struct PolicyScopeArgs {
    /// Hive whose access policy to use.
    #[arg(short = 'H', long)]
    pub hive: Option<String>,
    /// Use the global access policy instead of a Hive's.
    #[arg(long)]
    pub global: bool,
}

/// A manifest file argument.
#[derive(Debug, Clone, Args)]
pub struct ManifestArg {
    /// Manifest file holding one JSON or YAML document, or - for stdin.
    #[arg(short = 'f', long, required = true)]
    pub filename: String,
}

/// `create` subcommands.
#[derive(Debug, Subcommand)]
pub enum CreateCommand {
    /// Create an Actor.
    Actor {
        /// The Actor's name.
        name: String,
        /// Hive to create the Actor in.
        #[arg(short = 'H', long, required = true)]
        hive: String,
        /// Template to derive the Actor from, as <hive>/<name>, or <name> in
        /// the Actor's own hive.
        #[arg(long, required = true)]
        template: String,
        /// Tag to initialize the Actor from, as <hive>/<name>, or <name> in
        /// the Actor's own hive.
        #[arg(long)]
        tag: Option<String>,
    },
    /// Create an actor template from a manifest.
    ///
    /// The manifest holds one ActorTemplate in protobuf JSON form, as
    /// `get actor-template <name> -H <hive> -o yaml` prints it. Its hive and
    /// name come from its metadata, and the hive must already exist.
    /// Templates are immutable: delete and recreate to change one.
    ActorTemplate {
        #[command(flatten)]
        manifest: ManifestArg,
    },
    /// Create a Hive.
    Hive {
        /// The Hive's name.
        name: String,
    },
    /// Tag the external snapshot a suspended Actor holds.
    ///
    /// The Tag gets its own copy of that snapshot, so suspending the Actor
    /// again or deleting it cannot collect what the Tag names.
    Tag {
        /// The Tag's name.
        name: String,
        /// The Actor's hive; the Tag is created here.
        #[arg(short = 'H', long, required = true)]
        hive: String,
        /// Suspended Actor whose external snapshot to tag.
        #[arg(long, required = true)]
        actor: String,
        /// Tag scope: hive or published.
        #[arg(long, default_value = "hive")]
        scope: String,
    },
    /// Create an Actor's egress policy from a manifest.
    ///
    /// The manifest is an EgressPolicy as `get egress-policy` prints it with
    /// `-o yaml`. Its metadata may be omitted.
    EgressPolicy {
        #[command(flatten)]
        target: HivedName,
        #[command(flatten)]
        manifest: ManifestArg,
    },
    /// Create an access policy from a manifest.
    ///
    /// The manifest is an AccessPolicy as `get access-policy -o yaml` prints
    /// it. Its metadata may be omitted. Members are principals with a
    /// "user:" prefix.
    AccessPolicy {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[command(flatten)]
        manifest: ManifestArg,
    },
}

/// `update` subcommands.
#[derive(Debug, Subcommand)]
pub enum UpdateCommand {
    /// Update an Actor's template or worker selector.
    Actor {
        #[command(flatten)]
        target: HivedName,
        /// Template to repoint the Actor to, as <hive>/<name>, or <name> in
        /// the Actor's own hive.
        #[arg(long)]
        template: Option<String>,
        /// Comma-separated key=value worker pool selector, or "" to clear it.
        #[arg(long)]
        worker_selector: Option<String>,
    },
    /// Publish or unpublish a Tag.
    Tag {
        #[command(flatten)]
        target: HivedName,
        /// Tag scope: hive or published.
        #[arg(long, required = true)]
        scope: String,
    },
    /// Replace an Actor's egress policy from a manifest.
    ///
    /// The manifest's metadata.uid and metadata.version are required
    /// preconditions: the server rejects the update when they no longer
    /// match the stored policy.
    EgressPolicy {
        #[command(flatten)]
        target: HivedName,
        #[command(flatten)]
        manifest: ManifestArg,
    },
    /// Replace an access policy from a manifest.
    ///
    /// The manifest's metadata.uid and metadata.version are required
    /// preconditions: the server rejects the update when they no longer
    /// match the stored policy.
    AccessPolicy {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[command(flatten)]
        manifest: ManifestArg,
    },
}

/// `--uid` / `--version` guards for a delete.
#[derive(Debug, Clone, Default, Args)]
pub struct DeleteGuards {
    /// Delete only if the policy's metadata.uid matches.
    #[arg(long)]
    pub uid: Option<String>,
    /// Delete only if the policy's metadata.version matches.
    #[arg(long, allow_negative_numbers = true)]
    pub version: Option<i64>,
}

/// `delete` subcommands.
#[derive(Debug, Subcommand)]
pub enum DeleteCommand {
    /// Delete an Actor.
    Actor(HivedName),
    /// Delete an actor template, with its golden Actor and snapshot.
    ActorTemplate(HivedName),
    /// Delete an empty Hive (fails if any Actors remain).
    Hive {
        /// The Hive's name.
        name: String,
    },
    /// Delete a Tag.
    Tag(HivedName),
    /// Delete the egress policy of an Actor.
    EgressPolicy {
        #[command(flatten)]
        target: HivedName,
        #[command(flatten)]
        guards: DeleteGuards,
    },
    /// Delete the access policy of a Hive.
    ///
    /// The global access policy cannot be deleted; update it with no
    /// bindings instead.
    AccessPolicy {
        /// Hive whose access policy to delete.
        #[arg(short = 'H', long, required = true)]
        hive: String,
        #[command(flatten)]
        guards: DeleteGuards,
    },
}

/// `suspend` subcommands.
#[derive(Debug, Subcommand)]
pub enum SuspendCommand {
    /// Suspend an Actor.
    Actor {
        #[command(flatten)]
        target: HivedName,
        /// How the snapshot is made durable: async (return once it is on
        /// the worker's node, upload in the background) or sync (return once
        /// it is in object storage).
        #[arg(long, default_value = SUSPEND_MODE_ASYNC)]
        mode: String,
    },
}

/// `resume` subcommands.
#[derive(Debug, Subcommand)]
pub enum ResumeCommand {
    /// Resume an Actor.
    Actor(HivedName),
}

/// `revert` subcommands.
#[derive(Debug, Subcommand)]
pub enum RevertCommand {
    /// Revert an Actor to SUSPENDED, discarding its current execution.
    ///
    /// The Actor's external snapshot is left untouched, so a later resume
    /// restores from it. Only crashed or running Actors, or suspended Actors
    /// with a node-local snapshot, can be reverted.
    Actor(HivedName),
}

/// `logs` subcommands.
#[derive(Debug, Subcommand)]
pub enum LogsCommand {
    /// Stream logs for an Actor.
    Actor {
        #[command(flatten)]
        target: HivedName,
        /// Stream continuously, following the Actor across Workers.
        #[arg(short = 'f', long)]
        follow: bool,
        /// Only logs from this container; use _sandbox for sandbox
        /// diagnostics.
        #[arg(short = 'c', long, default_value = "")]
        container: String,
        /// Which lines to show: all, containers (container output only), or
        /// lifecycle (only the Actor's lifecycle events).
        #[arg(long, default_value = "all")]
        source: String,
    },
}

/// `top` subcommands.
#[derive(Debug, Subcommand)]
pub enum TopCommand {
    /// Display CPU and memory usage of Workers.
    #[command(visible_alias = "workers")]
    Worker {
        #[command(flatten)]
        filter: WorkerFilter,
    },
}

/// A Worker named by one positional argument.
#[derive(Debug, Clone, Args)]
pub struct WorkerName {
    /// The Worker's name.
    pub name: String,
}

/// `drain` subcommands.
#[derive(Debug, Subcommand)]
pub enum DrainCommand {
    /// Stop placing Actors on a Worker and move its Actors off.
    Worker(WorkerName),
}

/// `mark-available` subcommands.
#[derive(Debug, Subcommand)]
pub enum MarkAvailableCommand {
    /// Return a Worker to service.
    Worker(WorkerName),
}

/// `mark-unavailable` subcommands.
#[derive(Debug, Subcommand)]
pub enum MarkUnavailableCommand {
    /// Take a Worker out of service.
    Worker(WorkerName),
}

/// `proxy` subcommands.
#[derive(Debug, Subcommand)]
pub enum ProxyCommand {
    /// Forward local ports to an Actor through the gate, resuming it if it
    /// is suspended.
    ///
    /// Each port is [local-port:]remote-port; with none, local port 8000 is
    /// forwarded to Actor port 80. A local port of 0 picks a free one.
    Actor {
        /// The Actor's name.
        name: String,
        /// Port mappings.
        ports: Vec<String>,
        /// The Actor's hive.
        #[arg(short = 'H', long, required = true)]
        hive: String,
        /// Local address to listen on.
        #[arg(long, default_value = "127.0.0.1")]
        address: String,
        #[command(flatten)]
        gate: GateArgs,
    },
}

/// How to reach the gate's CONNECT-over-TLS listener.
#[derive(Debug, Clone, Args)]
pub struct GateArgs {
    /// The gate's CONNECT-over-TLS endpoint, as host:port.
    #[arg(long, env = "HK_GATE_ENDPOINT", required = true)]
    pub gate: String,
    /// PEM bundle the gate's serving certificate must chain to.
    #[arg(long, env = "HK_GATE_CA_FILE", required = true)]
    pub gate_ca_file: PathBuf,
    /// Name the gate's certificate must carry (defaults to its Service DNS
    /// name).
    #[arg(long, env = "HK_GATE_SERVER_NAME")]
    pub gate_server_name: Option<String>,
    /// Client certificate chain (PEM) presented to the gate.
    #[arg(long, env = "HK_GATE_CLIENT_CERT", required = true)]
    pub client_cert: PathBuf,
    /// Private key (PEM) of the client certificate.
    #[arg(long, env = "HK_GATE_CLIENT_KEY", required = true)]
    pub client_key: PathBuf,
}

/// Names the Secret an admin pool command works on.
#[derive(Debug, Clone, Args)]
pub struct PoolSecret {
    /// Namespace of the pool Secret.
    #[arg(long, default_value = "default")]
    pub secret_namespace: String,
    /// Name of the pool Secret.
    #[arg(long, required = true)]
    pub name: String,
}

/// `admin` subcommands.
#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Make a Secret holding a new CA pool, for a signing controller.
    MakeCaPool {
        #[command(flatten)]
        secret: PoolSecret,
        /// ID of the initial CA in the pool.
        #[arg(long, default_value = "")]
        ca_id: String,
        /// CA key type: ECDSAP256 or ED25519. ECDSAP256 is more broadly
        /// accepted by external relying parties.
        #[arg(long, default_value = "ECDSAP256")]
        key_type: String,
        /// How long the CA certificate is valid for, e.g. 8760h.
        #[arg(long, default_value = "8760h")]
        validity: String,
    },
    /// Make a Secret holding a new JWT authority pool, for the Actor
    /// identity broker.
    MakeJwtPool {
        #[command(flatten)]
        secret: PoolSecret,
        /// Signing algorithm of the initial key: ES256 or RS256 (4096-bit).
        #[arg(long, default_value = "ES256")]
        alg: String,
        /// ID of the initial key (defaults to the base64url SHA-256 of its
        /// PKIX encoding).
        #[arg(long, default_value = "")]
        key_id: String,
    },
    /// List the keys in a JWT authority pool Secret.
    ListJwtKeys {
        #[command(flatten)]
        secret: PoolSecret,
    },
    /// Add an inactive signing key to a JWT authority pool Secret.
    ///
    /// The key is published to relying parties but signs nothing until
    /// activate-jwt-key makes it the signing key. Activate it only once
    /// relying parties have refetched the key set.
    AddJwtKey {
        #[command(flatten)]
        secret: PoolSecret,
        /// Signing algorithm of the new key: ES256 or RS256 (4096-bit).
        #[arg(long, default_value = "ES256")]
        alg: String,
        /// ID of the new key (defaults to the base64url SHA-256 of its PKIX
        /// encoding).
        #[arg(long, default_value = "")]
        key_id: String,
    },
    /// Make a key in a JWT authority pool Secret the one that signs.
    ActivateJwtKey {
        #[command(flatten)]
        secret: PoolSecret,
        /// ID of the key.
        #[arg(long, required = true)]
        key_id: String,
    },
    /// Remove an inactive key from a JWT authority pool Secret.
    ///
    /// Tokens the key signed stop verifying once relying parties refetch the
    /// key set, so remove it only after the last of them has expired.
    RemoveJwtKey {
        #[command(flatten)]
        secret: PoolSecret,
        /// ID of the key.
        #[arg(long, required = true)]
        key_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("hivectl").chain(args.iter().copied()))
    }

    #[test]
    fn definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn subcommand_is_required() {
        for args in [&[][..], &["get"], &["admin"], &["proxy"]] {
            let kind = parse(args).unwrap_err().kind();
            assert!(
                matches!(
                    kind,
                    ErrorKind::MissingSubcommand
                        | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                ),
                "{args:?}: {kind:?}"
            );
        }
    }

    #[test]
    fn unknown_subcommand_is_suggested() {
        let err = parse(&["get", "actr"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidSubcommand);
        assert!(err.to_string().contains("actor"), "{err}");
    }

    #[test]
    fn global_flags_anywhere() {
        let cli = parse(&[
            "get",
            "actors",
            "-A",
            "-o",
            "yaml",
            "-vv",
            "--endpoint",
            "x:1",
        ])
        .unwrap();
        assert_eq!(cli.global.output, OutputFormat::Yaml);
        assert_eq!(cli.global.verbosity(), 2);
        assert_eq!(cli.global.endpoint.as_deref(), Some("x:1"));
        let cli = parse(&["-o", "json", "get", "hives"]).unwrap();
        assert_eq!(cli.global.output, OutputFormat::Json);
    }

    #[test]
    fn invalid_output_format() {
        let err = parse(&["get", "hives", "-o", "xml"]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Must be one of: table, json, yaml"),
            "{err}"
        );
    }

    #[test]
    fn create_actor_args() {
        let cli = parse(&["create", "actor", "a1", "-H", "team", "--template", "tpl"]).unwrap();
        match cli.command {
            Command::Create(CreateCommand::Actor {
                name,
                hive,
                template,
                tag,
            }) => {
                assert_eq!(
                    (name.as_str(), hive.as_str(), template.as_str()),
                    ("a1", "team", "tpl")
                );
                assert_eq!(tag, None);
            }
            other => panic!("{other:?}"),
        }
        // Missing name, missing flags, extra args.
        for args in [
            &["create", "actor", "-H", "team", "--template", "t"][..],
            &["create", "actor", "a1", "--template", "t"],
            &["create", "actor", "a1", "-H", "team"],
            &[
                "create",
                "actor",
                "a1",
                "a2",
                "-H",
                "team",
                "--template",
                "t",
            ],
        ] {
            assert!(parse(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn a_flag_does_not_swallow_the_next_flag() {
        let err = parse(&["create", "actor", "a1", "-H", "--template", "foo"]).unwrap_err();
        assert!(
            matches!(
                err.kind(),
                ErrorKind::InvalidValue | ErrorKind::MissingRequiredArgument
            ),
            "{:?}",
            err.kind()
        );
    }

    #[test]
    fn hive_commands_take_one_name() {
        assert!(parse(&["create", "hive", "h1"]).is_ok());
        assert!(parse(&["create", "hive"]).is_err());
        assert!(parse(&["create", "hive", "h1", "h2"]).is_err());
        assert!(parse(&["delete", "hive", "h1"]).is_ok());
    }

    #[test]
    fn suspend_mode_defaults_to_async() {
        match parse(&["suspend", "actor", "a", "-H", "h"])
            .unwrap()
            .command
        {
            Command::Suspend(SuspendCommand::Actor { mode, .. }) => assert_eq!(mode, "async"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn actor_template_create_takes_no_name() {
        assert!(parse(&["create", "actor-template", "-f", "m.yaml"]).is_ok());
        assert!(parse(&["create", "actor-template", "name", "-f", "m.yaml"]).is_err());
        assert!(parse(&["create", "actor-template"]).is_err());
    }

    #[test]
    fn egress_policy_args() {
        assert!(parse(&["get", "egress-policy", "a", "-H", "h"]).is_ok());
        assert!(parse(&["get", "egress-policies", "a", "-H", "h"]).is_ok());
        assert!(parse(&["get", "egress-policy", "-H", "h"]).is_err());
        assert!(parse(&["create", "egress-policy", "a", "-H", "h"]).is_err());
        assert!(parse(&["update", "egress-policy", "a", "-H", "h", "-f", "-"]).is_ok());
        match parse(&["delete", "egress-policy", "a", "-H", "h", "--version", "3"])
            .unwrap()
            .command
        {
            Command::Delete(DeleteCommand::EgressPolicy { guards, .. }) => {
                assert_eq!(guards.version, Some(3));
                assert_eq!(guards.uid, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn access_policy_args() {
        assert!(parse(&["get", "access-policy", "--global"]).is_ok());
        assert!(parse(&["get", "access-policy", "-H", "h"]).is_ok());
        assert!(parse(&["get", "access-policy", "extra"]).is_err());
        assert!(parse(&["create", "access-policy", "--global"]).is_err());
        assert!(parse(&["delete", "access-policy"]).is_err());
        assert!(parse(&["delete", "access-policy", "--global"]).is_err());
    }

    #[test]
    fn worker_verbs() {
        assert!(parse(&["drain", "worker", "w1"]).is_ok());
        assert!(parse(&["mark-available", "worker", "w1"]).is_ok());
        assert!(parse(&["mark-unavailable", "worker", "w1"]).is_ok());
        assert!(parse(&["get", "worker-assignments", "w1"]).is_ok());
        assert!(parse(&["top", "workers", "-n", "ns"]).is_ok());
        assert!(parse(&["top", "worker", "w1"]).is_err());
    }

    #[test]
    fn logs_args() {
        match parse(&["logs", "actor", "a", "-H", "h", "-f", "-c", "app"])
            .unwrap()
            .command
        {
            Command::Logs(LogsCommand::Actor {
                follow,
                container,
                source,
                ..
            }) => {
                assert!(follow);
                assert_eq!(container, "app");
                assert_eq!(source, "all");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn admin_args() {
        assert!(parse(&["admin", "make-ca-pool", "--name", "pool"]).is_ok());
        assert!(parse(&["admin", "make-ca-pool"]).is_err());
        assert!(parse(&["admin", "activate-jwt-key", "--name", "p"]).is_err());
        assert!(parse(&["admin", "activate-jwt-key", "--name", "p", "--key-id", "k"]).is_ok());
    }
}
