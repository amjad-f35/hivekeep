//! The `gcp` command tree.

use super::config::*;
use super::project::{
    enable_required_apis, grant_gke_node_permissions, grant_keeper_permissions, resolve_project_id,
    validate_iam_flags, GcloudProjects,
};
use super::runner::{ProcessRunner, Runner};
use super::{bucket, cloudsql, cluster, dashboards, transfer, GcpError};
use crate::{info, installdefaults};
use clap::parser::ValueSource;
use clap::{Arg, ArgAction, ArgMatches, Command};
use std::time::Duration;

/// Delay between retries of IAM writes racing a just-created account.
const RETRY_DELAY: Duration = Duration::from_secs(10);

fn string_arg(name: &'static str, env: &str, default: &str, help: &str) -> Arg {
    let default = get_env(env, default.to_string());
    let help = if env.is_empty() {
        help.to_string()
    } else {
        format!("{help} [env: {env}]")
    };
    Arg::new(name).long(name).default_value(default).help(help)
}

/// A boolean flag that defaults from the environment and can be turned off
/// with `--name=false`.
fn bool_arg(name: &'static str, env: Option<&str>, default: bool, help: &str) -> Arg {
    let (value, help) = match env {
        Some(e) => (get_env(e, default), format!("{help} [env: {e}]")),
        None => (default, help.to_string()),
    };
    Arg::new(name)
        .long(name)
        .help(help)
        .num_args(0..=1)
        .require_equals(true)
        .default_missing_value("true")
        .default_value(if value { "true" } else { "false" })
        .value_parser(clap::value_parser!(bool))
        .action(ArgAction::Set)
}

fn int_arg(name: &'static str, env: &str, help: &str) -> Arg {
    Arg::new(name)
        .long(name)
        .allow_negative_numbers(true)
        .default_value(get_env(env, 0i64).to_string())
        .value_parser(clap::value_parser!(i64))
        .help(format!("{help} [env: {env}]"))
}

fn transfer_gsa_arg() -> Arg {
    string_arg(
        "transfer-gsa-name",
        "SNAPSHOT_TRANSFER_GSA_NAME",
        installdefaults::TRANSFER_SERVICE_ACCOUNT_NAME,
        "Name of the snapshot transfer service account to create",
    )
}

fn machine_type_arg() -> Arg {
    Arg::new("machine-type")
        .long("machine-type")
        .default_value(resolve_machine_type_default())
        .help("Machine type for the node pool [env: NODE_MACHINE_TYPE]")
}

fn cluster_args(
    cmd: Command,
    name: &'static str,
    location: &'static str,
    version: &'static str,
) -> Command {
    cmd.arg(string_arg(name, "CLUSTER_NAME", DEFAULT_CLUSTER_NAME, "Name of the GKE cluster"))
        .arg(string_arg(location, "CLUSTER_LOCATION", DEFAULT_CLUSTER_LOCATION, "Zone or region for the cluster"))
        .arg(string_arg(version, "CLUSTER_VERSION", "", "Kubernetes version"))
        .arg(string_arg("network", "NETWORK", "default", "VPC network name"))
        .arg(string_arg("subnetwork", "SUBNETWORK", "default", "VPC subnetwork name"))
        .arg(machine_type_arg())
        .arg(bool_arg(
            "enable-nested-virtualization",
            Some("ENABLE_NESTED_VIRTUALIZATION"),
            true,
            "Create the node pool with nested virtualization, exposing /dev/kvm for micro-VM workers; needs a \
             machine type that supports it. Turn off with --enable-nested-virtualization=false",
        ))
        .arg(int_arg("boot-disk-size", "BOOT_DISK_SIZE_GB", "Boot disk size in GB for the node pool; 0 = GKE default (100 GB)"))
        .arg(string_arg("boot-disk-type", "BOOT_DISK_TYPE", "", "Boot disk type for the node pool; empty = GKE default"))
}

/// The `gcp` command, to mount under the installer's root.
pub fn command() -> Command {
    let bootstrap = cluster_args(
        Command::new("bootstrap").about("Fully bootstrap the GCP environment").long_about(
            "Runs all setup steps in order: enable APIs, create cluster, create buckets, create the snapshot \
             transfer service account, grant IAM permissions, and create dashboards.",
        ),
        "cluster-name",
        "cluster-location",
        "cluster-version",
    )
    .arg(bool_arg("enable-dataplane-v2", Some("ENABLE_DATAPLANE_V2"), true, "Enable Dataplane V2"))
    .arg(string_arg("bucket-name", "BUCKET_NAME", "", "Name of the GCS bucket for snapshots"))
    .arg(transfer_gsa_arg())
    .arg(string_arg("dashboard-dir", "DASHBOARD_DIR", &default_dashboard_dir(), "Directory containing dashboard JSON files"));

    let create_cluster = cluster_args(
        Command::new("cluster").about("Create GKE cluster"),
        "name",
        "location",
        "version",
    )
    .arg(bool_arg(
        "enable-dataplane-v2",
        Some("ENABLE_DATAPLANE_V2"),
        true,
        "Enable Dataplane V2",
    ));

    let create_bucket = Command::new("bucket")
        .about("Create the GCS snapshot bucket and the micro-VM asset bucket")
        .arg(string_arg(
            "name",
            "BUCKET_NAME",
            "",
            "Name of the GCS snapshot bucket. The asset bucket is named NAME-assets",
        ));

    let create_iam = Command::new("iam")
        .about("Create IAM bindings")
        .arg(string_arg("bucket", "BUCKET_NAME", "", "GCS bucket name (required for bucket bindings)"))
        .arg(bool_arg("gke-nodes", None, true, "Grant GKE nodes permission to pull images"))
        .arg(bool_arg("keeper", None, true, "Grant the keeper project-level permissions"))
        .arg(transfer_gsa_arg())
        .arg(bool_arg(
            "transfer-service-account",
            None,
            true,
            "Create the snapshot transfer service account and let hivekeep-api create tokens for it",
        ))
        .arg(bool_arg(
            "bucket-bindings",
            None,
            true,
            "Grant hivekeep-api and the snapshot transfer service account access to the snapshot bucket, and the \
             keeper read access to the asset bucket",
        ));

    let create_cloudsql = Command::new("cloudsql")
        .about("Create a Cloud SQL PostgreSQL instance for the hivekeep-api store, with IAM database authentication")
        .arg(string_arg("instance", "CLOUDSQL_INSTANCE", DEFAULT_CLOUDSQL_INSTANCE, "Cloud SQL instance name"))
        .arg(string_arg(
            "tier",
            "CLOUDSQL_TIER",
            DEFAULT_CLOUDSQL_TIER,
            "Machine tier: db-custom-<vCPU>-<MB> for enterprise, db-perf-optimized-N-<vCPU> for enterprise-plus",
        ))
        .arg(string_arg(
            "edition",
            "CLOUDSQL_EDITION",
            DEFAULT_CLOUDSQL_EDITION,
            "Instance edition: enterprise | enterprise-plus (enables the local-SSD data cache)",
        ))
        .arg(int_arg(
            "storage-size",
            "CLOUDSQL_STORAGE_GB",
            "Data disk size in GB; 0 = Cloud SQL default (10 GB, auto-resizing). PD IOPS scale with size",
        ))
        .arg(string_arg(
            "gsa-name",
            "CLOUDSQL_GSA_NAME",
            DEFAULT_CLOUDSQL_GSA_NAME,
            "Name of the Google service account to create for Workload Identity + IAM database auth",
        ))
        .arg(transfer_gsa_arg())
        .arg(string_arg("network", "NETWORK", "default", "VPC network name (must match the cluster's)"));

    let create_dashboards = Command::new("dashboards")
        .about("Create Cloud Monitoring dashboards")
        .arg(string_arg(
            "dir",
            "DASHBOARD_DIR",
            &default_dashboard_dir(),
            "Directory containing dashboard JSON files",
        ));

    Command::new("gcp")
        .about("Set up the GCP resources Hivekeep runs on")
        .long_about("Provisions and configures the GCP resources a Hivekeep install requires.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(string_arg("project-id", "PROJECT_ID", "", "GCP Project ID").global(true))
        .arg(
            string_arg(
                "project-number",
                "PROJECT_NUMBER",
                "",
                "GCP Project Number (optional, resolved from --project-id if needed)",
            )
            .global(true),
        )
        .arg(string_arg("region", "GCE_REGION", DEFAULT_REGION, "GCP Region").global(true))
        .subcommand(bootstrap)
        .subcommand(
            Command::new("enable")
                .about("Enable GCP resources")
                .subcommand_required(true)
                .subcommand(Command::new("apis").about("Enable required GCP APIs")),
        )
        .subcommand(
            Command::new("create")
                .about("Create GCP resources")
                .subcommand_required(true)
                .subcommand(create_cluster)
                .subcommand(create_bucket)
                .subcommand(create_iam)
                .subcommand(create_cloudsql)
                .subcommand(create_dashboards),
        )
}

fn s(m: &ArgMatches, name: &str) -> String {
    m.try_get_one::<String>(name)
        .ok()
        .flatten()
        .cloned()
        .unwrap_or_default()
}

fn b(m: &ArgMatches, name: &str) -> bool {
    m.try_get_one::<bool>(name)
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
}

fn i(m: &ArgMatches, name: &str) -> i64 {
    m.try_get_one::<i64>(name)
        .ok()
        .flatten()
        .copied()
        .unwrap_or(0)
}

fn from_command_line(m: &ArgMatches, name: &str) -> bool {
    m.value_source(name) == Some(ValueSource::CommandLine)
}

fn base_config(m: &ArgMatches) -> GcpConfig {
    GcpConfig {
        project_id: s(m, "project-id"),
        project_number: s(m, "project-number"),
        region: s(m, "region"),
        ..Default::default()
    }
}

fn apply_cluster_flags(
    cfg: &mut GcpConfig,
    m: &ArgMatches,
    name: &str,
    location: &str,
    version: &str,
) {
    cfg.cluster_name = s(m, name);
    cfg.cluster_location = s(m, location);
    cfg.cluster_version = s(m, version);
    cfg.network = s(m, "network");
    cfg.subnetwork = s(m, "subnetwork");
    cfg.machine_type = s(m, "machine-type");
    cfg.enable_dataplane_v2 = b(m, "enable-dataplane-v2");
    cfg.enable_nested_virtualization = b(m, "enable-nested-virtualization");
    cfg.boot_disk_size_gb = i32::try_from(i(m, "boot-disk-size")).unwrap_or(-1);
    cfg.boot_disk_type = s(m, "boot-disk-type");
    warn_deprecated_machine_type_env(from_command_line(m, "machine-type"));
}

/// Runs the parsed `gcp` command against real gcloud.
pub async fn run(matches: &ArgMatches) -> Result<(), GcpError> {
    run_with(matches, &ProcessRunner).await
}

/// Runs the parsed `gcp` command with `runner`.
pub async fn run_with(matches: &ArgMatches, runner: &dyn Runner) -> Result<(), GcpError> {
    let mut cfg = base_config(matches);
    match matches.subcommand() {
        Some(("bootstrap", m)) => {
            apply_cluster_flags(
                &mut cfg,
                m,
                "cluster-name",
                "cluster-location",
                "cluster-version",
            );
            cfg.bucket_name = s(m, "bucket-name");
            cfg.transfer_gsa_name = s(m, "transfer-gsa-name");
            cfg.dashboard_dir = s(m, "dashboard-dir");
            bootstrap(runner, &mut cfg).await
        }
        Some(("enable", m)) => match m.subcommand() {
            Some(("apis", _)) => {
                resolve_project_id(&mut cfg)?;
                enable_required_apis(runner, &cfg)
            }
            _ => Err(GcpError::Usage("enable needs a subcommand".into())),
        },
        Some(("create", m)) => match m.subcommand() {
            Some(("cluster", m)) => {
                apply_cluster_flags(&mut cfg, m, "name", "location", "version");
                resolve_project_id(&mut cfg)?;
                cluster::create_cluster_idempotent(runner, &mut cfg)
            }
            Some(("bucket", m)) => {
                cfg.bucket_name = s(m, "name");
                bucket::validate_bucket_flags(&mut cfg, &GcloudProjects(runner))?;
                bucket::create_buckets(runner, &cfg)
            }
            Some(("iam", m)) => {
                cfg.bucket_name = s(m, "bucket");
                cfg.transfer_gsa_name = s(m, "transfer-gsa-name");
                create_iam(
                    runner,
                    &mut cfg,
                    b(m, "gke-nodes"),
                    b(m, "keeper"),
                    b(m, "transfer-service-account"),
                    b(m, "bucket-bindings"),
                )
                .await
            }
            Some(("cloudsql", m)) => {
                cfg.cloudsql_instance = s(m, "instance");
                cfg.cloudsql_tier = s(m, "tier");
                cfg.cloudsql_edition = s(m, "edition");
                cfg.cloudsql_storage_gb = i(m, "storage-size");
                cfg.cloudsql_gsa_name = s(m, "gsa-name");
                cfg.transfer_gsa_name = s(m, "transfer-gsa-name");
                cfg.network = s(m, "network");
                resolve_project_id(&mut cfg)?;
                // An explicit region prevents silent cross-region latency and
                // egress costs.
                let region_env = std::env::var("GCE_REGION").unwrap_or_default();
                if !from_command_line(matches, "region")
                    && !from_command_line(m, "region")
                    && region_env.is_empty()
                {
                    return Err(GcpError::Usage(
                        "--region is required (or set GCE_REGION): the instance must be in the cluster's region".into(),
                    ));
                }
                cloudsql::provision(runner, &cfg, RETRY_DELAY).await
            }
            Some(("dashboards", m)) => {
                cfg.dashboard_dir = s(m, "dir");
                resolve_project_id(&mut cfg)?;
                dashboards::create_monitoring_dashboards(runner, &cfg)
            }
            _ => Err(GcpError::Usage("create needs a subcommand".into())),
        },
        _ => Err(GcpError::Usage("gcp needs a subcommand".into())),
    }
}

fn api_server_principal(cfg: &GcpConfig) -> String {
    workload_identity_principal(cfg, installdefaults::API_SERVER_SERVICE_ACCOUNT)
}

async fn create_iam(
    runner: &dyn Runner,
    cfg: &mut GcpConfig,
    gke_nodes: bool,
    keeper: bool,
    transfer_account: bool,
    bucket_bindings: bool,
) -> Result<(), GcpError> {
    validate_iam_flags(cfg, bucket_bindings, &GcloudProjects(runner))?;
    if gke_nodes {
        grant_gke_node_permissions(runner, cfg)?;
    }
    if keeper {
        grant_keeper_permissions(runner, cfg)?;
    }
    // Before the bucket bindings, which grant the account: IAM refuses a
    // binding for an account that does not exist.
    if transfer_account {
        transfer::setup_transfer_service_account(
            runner,
            cfg,
            &[api_server_principal(cfg)],
            RETRY_DELAY,
        )
        .await?;
    }
    if bucket_bindings {
        bucket::create_iam_policy_bindings(runner, cfg, RETRY_DELAY).await?;
    }
    Ok(())
}

async fn bootstrap(runner: &dyn Runner, cfg: &mut GcpConfig) -> Result<(), GcpError> {
    resolve_project_id(cfg)?;
    if cfg.bucket_name.is_empty() {
        return Err(GcpError::Usage("--bucket-name is required".into()));
    }
    bucket::check_asset_bucket_name(cfg)?;
    info!("Starting full bootstrap...");
    info!("Step 1/8: Enabling required APIs...");
    enable_required_apis(runner, cfg)?;
    info!("Step 2/8: Creating GKE cluster...");
    cluster::create_cluster_idempotent(runner, cfg)?;
    info!("Step 3/8: Creating GCS buckets for snapshots and micro-VM assets...");
    super::project::resolve_project(cfg, &GcloudProjects(runner))?;
    bucket::create_buckets(runner, cfg)?;
    info!("Step 4/8: Granting GKE node permissions...");
    grant_gke_node_permissions(runner, cfg)?;
    info!("Step 5/8: Granting keeper permissions...");
    grant_keeper_permissions(runner, cfg)?;
    info!("Step 6/8: Creating the snapshot transfer service account...");
    transfer::setup_transfer_service_account(
        runner,
        cfg,
        &[api_server_principal(cfg)],
        RETRY_DELAY,
    )
    .await?;
    info!("Step 7/8: Creating IAM policy bindings for the buckets...");
    bucket::create_iam_policy_bindings(runner, cfg, RETRY_DELAY).await?;
    info!("Step 8/8: Creating monitoring dashboards...");
    dashboards::create_monitoring_dashboards(runner, cfg)?;
    info!("Bootstrap completed successfully.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::runner::FakeRunner;

    fn parse(args: &[&str]) -> ArgMatches {
        let mut full = vec!["gcp"];
        full.extend_from_slice(args);
        command().try_get_matches_from(full).unwrap()
    }

    #[test]
    fn command_tree_is_valid() {
        command().debug_assert();
    }

    #[test]
    fn bool_flags_turn_off_with_equals_false() {
        let m = parse(&["create", "iam", "--gke-nodes=false", "--keeper"]);
        let (_, create) = m.subcommand().unwrap();
        let (_, iam) = create.subcommand().unwrap();
        assert!(!b(iam, "gke-nodes"));
        assert!(b(iam, "keeper"));
        assert!(b(iam, "bucket-bindings"));
    }

    #[tokio::test]
    async fn bootstrap_requires_a_bucket_before_doing_anything() {
        let runner = FakeRunner::new();
        let m = parse(&["bootstrap", "--project-id=p", "--bucket-name="]);
        let err = run_with(&m, &runner).await.unwrap_err();
        assert_eq!(err.to_string(), "--bucket-name is required");
        assert!(runner.calls().is_empty());
    }

    #[tokio::test]
    async fn enable_apis_enables_every_required_service() {
        let runner = FakeRunner::new();
        run_with(
            &parse(&["enable", "apis", "--project-id=projects/p"]),
            &runner,
        )
        .await
        .unwrap();
        let line = &runner.command_lines()[0];
        assert!(
            line.starts_with("gcloud services enable artifactregistry.googleapis.com"),
            "{line}"
        );
        assert!(line.ends_with("--project=p"), "{line}");
    }
}
