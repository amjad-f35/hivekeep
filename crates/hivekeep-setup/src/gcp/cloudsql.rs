//! A Cloud SQL PostgreSQL instance for the hivekeep-api store, with IAM
//! database authentication through Workload Identity.

use super::config::GcpConfig;
use super::policy::add_service_account_binding;
use super::project::{enable_services, get_project_policy, set_project_policy};
use super::runner::{Gcloud, Runner};
use super::transfer::{
    ensure_service_account, get_service_account_policy, set_service_account_policy,
};
use super::GcpError;
use crate::{info, installdefaults};
use serde::Deserialize;
use std::time::Duration;

/// The store database. Must match the database the bundled PostgreSQL
/// creates and hivekeep-api's connection strings name.
pub const CLOUDSQL_DATABASE: &str = "hivekeeppg";

/// The Kubernetes identity that connects to the database.
pub const API_SERVER_NAMESPACE: &str = installdefaults::SYSTEM_NAMESPACE;
pub const API_SERVER_KSA: &str = installdefaults::API_SERVER_SERVICE_ACCOUNT;

pub fn cloudsql_gsa_email(cfg: &GcpConfig) -> String {
    format!(
        "{}@{}.iam.gserviceaccount.com",
        cfg.cloudsql_gsa_name, cfg.project_id
    )
}

/// The IAM database username of a service account: its email without the
/// `.gserviceaccount.com` suffix.
pub fn cloudsql_database_user(gsa_email: &str) -> String {
    gsa_email
        .strip_suffix(".gserviceaccount.com")
        .unwrap_or(gsa_email)
        .to_string()
}

/// The classic Workload Identity member of hivekeep-api's KSA. Federated
/// `principal://` identities cannot be Cloud SQL IAM users, so Cloud SQL
/// needs the KSA-to-GSA link.
pub fn workload_identity_member(project_id: &str) -> String {
    format!("serviceAccount:{project_id}.svc.id.goog[{API_SERVER_NAMESPACE}/{API_SERVER_KSA}]")
}

pub fn private_network_url(cfg: &GcpConfig) -> String {
    format!(
        "projects/{}/global/networks/{}",
        cfg.project_id, cfg.network
    )
}

/// The creation request for a new instance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InstanceSpec {
    pub name: String,
    pub region: String,
    pub database_version: String,
    pub tier: String,
    /// `ENTERPRISE` or `ENTERPRISE_PLUS`.
    pub edition: String,
    pub data_cache_enabled: bool,
    /// 0 leaves the Cloud SQL default (10 GB, auto-resizing).
    pub data_disk_size_gb: i64,
    pub ipv4_enabled: bool,
    pub private_network: String,
    /// `(name, value)` database flags.
    pub database_flags: Vec<(String, String)>,
    pub backups_enabled: bool,
    pub point_in_time_recovery: bool,
    pub deletion_protection: bool,
}

/// Maps the flags onto a creation request. The shape of an existing
/// instance is never touched.
pub fn cloudsql_instance_spec(cfg: &GcpConfig) -> Result<InstanceSpec, GcpError> {
    let (edition, data_cache) = match cfg.cloudsql_edition.as_str() {
        // Enterprise accepts db-custom-<vCPU>-<MB> tiers.
        "" | "enterprise" => ("ENTERPRISE", false),
        // Enterprise Plus accepts db-perf-optimized-N-<vCPU> tiers; its
        // local-SSD data cache extends the effective cache beyond RAM.
        "enterprise-plus" => ("ENTERPRISE_PLUS", true),
        other => {
            return Err(GcpError::Usage(format!(
                "unknown --edition {other:?} (want enterprise|enterprise-plus)"
            )))
        }
    };
    Ok(InstanceSpec {
        name: cfg.cloudsql_instance.clone(),
        region: cfg.region.clone(),
        database_version: "POSTGRES_18".into(),
        tier: cfg.cloudsql_tier.clone(),
        edition: edition.into(),
        data_cache_enabled: data_cache,
        data_disk_size_gb: cfg.cloudsql_storage_gb.max(0),
        // Private services access only.
        ipv4_enabled: false,
        private_network: private_network_url(cfg),
        database_flags: vec![("cloudsql.iam_authentication".into(), "on".into())],
        // The store is the control plane's state: daily backups with
        // point-in-time recovery, and a guard against accidental deletion.
        backups_enabled: true,
        point_in_time_recovery: true,
        deletion_protection: true,
    })
}

/// The `gcloud sql instances create` arguments for a spec.
pub fn create_instance_args(spec: &InstanceSpec, cfg: &GcpConfig) -> Vec<String> {
    let mut a = vec![
        "sql".to_string(),
        "instances".into(),
        "create".into(),
        spec.name.clone(),
        format!("--project={}", cfg.project_id),
        format!("--region={}", spec.region),
        format!("--database-version={}", spec.database_version),
        format!("--tier={}", spec.tier),
        format!(
            "--edition={}",
            spec.edition.to_lowercase().replace('_', "-")
        ),
        format!("--network={}", spec.private_network),
        "--no-assign-ip".into(),
        format!(
            "--database-flags={}",
            spec.database_flags
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "--backup".into(),
        "--enable-point-in-time-recovery".into(),
        "--deletion-protection".into(),
    ];
    if spec.data_cache_enabled {
        a.push("--enable-data-cache".into());
    }
    if spec.data_disk_size_gb > 0 {
        a.push(format!("--storage-size={}GB", spec.data_disk_size_gb));
    }
    a
}

/// Verifies the VPC has a private services access peering, which private-IP
/// Cloud SQL needs. Creating it is a one-time-per-VPC step left to gcloud.
pub fn check_private_services_access(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let out = Gcloud { runner }.raw(&[
        "services".into(),
        "vpc-peerings".into(),
        "list".into(),
        format!("--network={}", cfg.network),
        format!("--project={}", cfg.project_id),
        "--format=json".into(),
    ])?;
    if out.success {
        let peerings: Vec<serde_json::Value> =
            serde_json::from_str(&out.stdout).unwrap_or_default();
        let has_range = peerings.iter().any(|p| {
            p.get("reservedPeeringRanges")
                .and_then(|r| r.as_array())
                .is_some_and(|r| !r.is_empty())
        });
        if has_range {
            return Ok(());
        }
    } else {
        crate::warn!(
            "could not list service networking connections: {}",
            out.stderr.trim()
        );
    }
    Err(GcpError::Other(format!(
        "network {net:?} has no private services access peering, which Cloud SQL private IP requires. Create it once per VPC:\n\n  \
         gcloud compute addresses create google-managed-services-{net} \\\n    --global --purpose=VPC_PEERING --prefix-length=16 --network={net} --project={proj}\n  \
         gcloud services vpc-peerings connect \\\n    --service=servicenetworking.googleapis.com \\\n    --ranges=google-managed-services-{net} --network={net} --project={proj}",
        net = cfg.network,
        proj = cfg.project_id
    )))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DescribedInstance {
    #[serde(default)]
    settings: Option<DescribedSettings>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DescribedSettings {
    #[serde(default)]
    database_flags: Vec<DescribedFlag>,
}

#[derive(Debug, Default, Deserialize)]
struct DescribedFlag {
    #[serde(default)]
    name: String,
    #[serde(default)]
    value: String,
}

/// Creates the instance, or verifies an existing one has IAM authentication
/// on.
pub fn create_cloudsql_instance(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let g = Gcloud { runner };
    info!(
        "Checking if Cloud SQL instance {} exists",
        cfg.cloudsql_instance
    );
    let out = g.raw(&[
        "sql".into(),
        "instances".into(),
        "describe".into(),
        cfg.cloudsql_instance.clone(),
        format!("--project={}", cfg.project_id),
        "--format=json".into(),
    ])?;
    if out.success {
        let inst: DescribedInstance = serde_json::from_str(&out.stdout).unwrap_or_default();
        let iam_on = inst
            .settings
            .map(|s| {
                s.database_flags
                    .iter()
                    .any(|f| f.name == "cloudsql.iam_authentication" && f.value == "on")
            })
            .unwrap_or(false);
        if !iam_on {
            return Err(GcpError::Other(format!(
                "instance {i} exists but cloudsql.iam_authentication is off; enable it (this restarts the instance):\n\n  \
                 gcloud sql instances patch {i} --database-flags=cloudsql.iam_authentication=on --project={p}",
                i = cfg.cloudsql_instance,
                p = cfg.project_id
            )));
        }
        info!("Cloud SQL instance exists with IAM authentication on. Skipping create.");
        return Ok(());
    }
    if !out.is_not_found() {
        return Err(GcpError::Other(format!(
            "get instance: {}",
            out.stderr.trim()
        )));
    }
    check_private_services_access(runner, cfg)?;
    let spec = cloudsql_instance_spec(cfg)?;
    info!(
        "Creating Cloud SQL instance {} in {} ({}, {}); this takes several minutes...",
        spec.name, spec.region, spec.tier, spec.edition
    );
    g.run("create instance", &create_instance_args(&spec, cfg))?;
    Ok(())
}

pub fn create_cloudsql_database(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let g = Gcloud { runner };
    let base = |verb: &str| -> Vec<String> {
        vec![
            "sql".into(),
            "databases".into(),
            verb.into(),
            CLOUDSQL_DATABASE.into(),
            format!("--instance={}", cfg.cloudsql_instance),
            format!("--project={}", cfg.project_id),
        ]
    };
    let out = g.raw(&base("describe"))?;
    if out.success {
        info!("Database {CLOUDSQL_DATABASE} exists. Skipping create.");
        return Ok(());
    }
    if !out.is_not_found() {
        return Err(GcpError::Other(format!(
            "get database: {}",
            out.stderr.trim()
        )));
    }
    info!("Creating database {CLOUDSQL_DATABASE}...");
    g.run("create database", &base("create"))?;
    Ok(())
}

/// Creates the Cloud SQL service account and lets hivekeep-api's KSA act as
/// it, so the proxy sidecar's credentials resolve to the IAM database user.
pub fn create_cloudsql_gsa(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let email = ensure_service_account(
        runner,
        cfg,
        &cfg.cloudsql_gsa_name,
        "hivekeep-api Cloud SQL access",
    )?;
    let mut policy = get_service_account_policy(runner, cfg, &email)?;
    if !add_service_account_binding(
        &mut policy,
        "roles/iam.workloadIdentityUser",
        &workload_identity_member(&cfg.project_id),
    ) {
        info!("Workload Identity binding exists. Skipping.");
        return Ok(());
    }
    info!("Binding Workload Identity user to service account {email}...");
    set_service_account_policy(runner, cfg, &email, &policy)
}

/// Grants the service account the roles to open connector tunnels and log
/// in with IAM authentication, retrying while a fresh account propagates.
pub async fn grant_cloudsql_project_roles(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    retry_delay: Duration,
) -> Result<(), GcpError> {
    let member = format!("serviceAccount:{}", cloudsql_gsa_email(cfg));
    let mut attempt = 1;
    loop {
        let mut policy = get_project_policy(runner, cfg)?;
        let a = policy.add_binding("roles/cloudsql.client", &member);
        let b = policy.add_binding("roles/cloudsql.instanceUser", &member);
        if !a && !b {
            info!("IAM policy already has required Cloud SQL permissions. Skipping update.");
            return Ok(());
        }
        info!("Setting IAM policy (grant Cloud SQL permissions to {member})...");
        match set_project_policy(runner, cfg, &policy) {
            Ok(()) => return Ok(()),
            Err(e) if attempt >= 6 => return Err(e),
            Err(e) => {
                crate::warn!("setting IAM policy failed (attempt {attempt}), retrying: {e}");
                tokio::time::sleep(retry_delay).await;
                attempt += 1;
            }
        }
    }
}

pub fn create_cloudsql_iam_user(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let g = Gcloud { runner };
    let user = cloudsql_database_user(&cloudsql_gsa_email(cfg));
    let out = g.run(
        "list database users",
        &[
            "sql".into(),
            "users".into(),
            "list".into(),
            format!("--instance={}", cfg.cloudsql_instance),
            format!("--project={}", cfg.project_id),
            "--format=value(name)".into(),
        ],
    )?;
    if out.lines().any(|l| l.trim() == user) {
        info!("IAM database user {user} exists. Skipping create.");
        return Ok(());
    }
    info!("Creating IAM database user {user}...");
    // The API takes the truncated form, which is also the name the database
    // sees.
    g.run(
        "create IAM database user",
        &[
            "sql".into(),
            "users".into(),
            "create".into(),
            user,
            format!("--instance={}", cfg.cloudsql_instance),
            format!("--project={}", cfg.project_id),
            "--type=cloud_iam_service_account".into(),
        ],
    )?;
    Ok(())
}

/// The instructions printed once the instance is provisioned.
pub fn next_steps(cfg: &GcpConfig) -> String {
    format!(
        "\nCloud SQL is provisioned. Two steps remain:\n\n\
         1. Create the PostgreSQL owner and read/write roles, grant the IAM database\n   \
            user membership, and configure schema privileges before deploying hivekeep-api.\n\n\
         2. Deploy hivekeep-api against it:\n\n     \
            export HK_API_POSTGRES_CLOUDSQL_INSTANCE={}:{}:{}\n     \
            export HK_API_POSTGRES_CLOUDSQL_GSA={}\n     \
            hivekeep-setup deploy hivekeep-system --credential-provider='{{\"name\":\"k8s.io\"}}'\n",
        cfg.project_id,
        cfg.region,
        cfg.cloudsql_instance,
        cloudsql_gsa_email(cfg)
    )
}

/// Every Cloud SQL step, in order.
pub async fn provision(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    retry_delay: Duration,
) -> Result<(), GcpError> {
    enable_services(
        runner,
        cfg,
        &[
            "sqladmin.googleapis.com",
            "servicenetworking.googleapis.com",
        ],
    )?;
    create_cloudsql_instance(runner, cfg)?;
    create_cloudsql_database(runner, cfg)?;
    create_cloudsql_gsa(runner, cfg)?;
    // hivekeep-api's pod runs as this account once Cloud SQL is on, so its
    // credential exchange creates transfer account tokens with it.
    super::transfer::setup_transfer_service_account(
        runner,
        cfg,
        &[format!("serviceAccount:{}", cloudsql_gsa_email(cfg))],
        retry_delay,
    )
    .await?;
    grant_cloudsql_project_roles(runner, cfg, retry_delay).await?;
    create_cloudsql_iam_user(runner, cfg)?;
    crate::log::info(&next_steps(cfg));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> GcpConfig {
        GcpConfig {
            project_id: "my-proj".into(),
            region: "us-central1".into(),
            network: "default".into(),
            cloudsql_instance: "hivekeep-pg".into(),
            cloudsql_tier: "db-custom-2-8192".into(),
            cloudsql_gsa_name: "hivekeep-api".into(),
            ..Default::default()
        }
    }

    #[test]
    fn cloudsql_identity_derivations() {
        let c = base();
        assert_eq!(
            cloudsql_gsa_email(&c),
            "hivekeep-api@my-proj.iam.gserviceaccount.com"
        );
        // What operators GRANT to and what the installer puts in the DSN.
        assert_eq!(
            cloudsql_database_user(&cloudsql_gsa_email(&c)),
            "hivekeep-api@my-proj.iam"
        );
        assert_eq!(
            workload_identity_member(&c.project_id),
            "serviceAccount:my-proj.svc.id.goog[hivekeep-system/hivekeep-api]"
        );
        assert_eq!(
            private_network_url(&c),
            "projects/my-proj/global/networks/default"
        );
    }

    #[test]
    fn cloudsql_instance_spec_enterprise_defaults() {
        let spec = cloudsql_instance_spec(&base()).unwrap();
        assert_eq!(spec.edition, "ENTERPRISE");
        assert!(!spec.data_cache_enabled);
        assert_eq!(spec.data_disk_size_gb, 0);
        assert_eq!(
            spec.database_flags[0],
            ("cloudsql.iam_authentication".into(), "on".into())
        );
        assert!(spec.backups_enabled && spec.point_in_time_recovery);
        assert!(spec.deletion_protection);
        let a = create_instance_args(&spec, &base());
        assert!(!a.iter().any(|x| x.starts_with("--storage-size")));
        assert!(a.contains(&"--edition=enterprise".to_string()));
    }

    #[test]
    fn cloudsql_instance_spec_enterprise_plus_enables_data_cache() {
        let c = GcpConfig {
            cloudsql_edition: "enterprise-plus".into(),
            cloudsql_tier: "db-perf-optimized-N-16".into(),
            cloudsql_storage_gb: 5000,
            ..base()
        };
        let spec = cloudsql_instance_spec(&c).unwrap();
        assert_eq!(spec.edition, "ENTERPRISE_PLUS");
        assert!(spec.data_cache_enabled);
        assert_eq!(spec.data_disk_size_gb, 5000);
        let a = create_instance_args(&spec, &c);
        assert!(a.contains(&"--enable-data-cache".to_string()));
        assert!(a.contains(&"--edition=enterprise-plus".to_string()));
        assert!(a.contains(&"--storage-size=5000GB".to_string()));
    }

    #[test]
    fn cloudsql_instance_spec_unknown_edition_rejected() {
        let c = GcpConfig {
            cloudsql_edition: "hyperscale".into(),
            ..base()
        };
        let err = cloudsql_instance_spec(&c).unwrap_err();
        assert!(err.to_string().contains("--edition"), "{err}");
    }

    #[test]
    fn existing_instance_without_iam_auth_is_refused() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["sql", "instances", "describe"],
            crate::gcp::runner::Output::ok(r#"{"settings":{"databaseFlags":[]}}"#),
        );
        let err = create_cloudsql_instance(&runner, &base()).unwrap_err();
        assert!(
            err.to_string()
                .contains("cloudsql.iam_authentication is off"),
            "{err}"
        );
    }
}
