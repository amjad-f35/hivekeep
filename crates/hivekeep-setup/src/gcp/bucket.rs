//! The snapshot bucket and the micro-VM asset bucket, and who may use them.

use super::config::{workload_identity_principal, GcpConfig};
use super::policy::Policy;
use super::project::{parse_policy, resolve_project, write_policy, ProjectLookup};
use super::runner::{Gcloud, Runner};
use super::transfer::transfer_service_account_member;
use super::GcpError;
use crate::{info, installdefaults};
use serde::Deserialize;
use std::time::Duration;

/// The longest bucket name GCS accepts.
pub const MAX_BUCKET_NAME_LENGTH: usize = 63;

/// Fails when the asset bucket's name (the snapshot bucket's plus a suffix)
/// is longer than GCS allows.
pub fn check_asset_bucket_name(cfg: &GcpConfig) -> Result<(), GcpError> {
    let asset = cfg.asset_bucket_name();
    if asset.len() > MAX_BUCKET_NAME_LENGTH {
        return Err(GcpError::Usage(format!(
            "bucket name {:?} is too long: the asset bucket {asset:?} would have {} characters, and GCS allows {MAX_BUCKET_NAME_LENGTH}",
            cfg.bucket_name,
            asset.len()
        )));
    }
    Ok(())
}

/// Checks `create bucket`'s flags and resolves the project number.
pub fn validate_bucket_flags(
    cfg: &mut GcpConfig,
    lookup: &dyn ProjectLookup,
) -> Result<(), GcpError> {
    if cfg.bucket_name.is_empty() {
        return Err(GcpError::Usage("--name is required".into()));
    }
    check_asset_bucket_name(cfg)?;
    resolve_project(cfg, lookup)
}

#[derive(Debug, Default, Deserialize)]
struct BucketAttrs {
    #[serde(default, alias = "projectNumber", alias = "project_number")]
    project_number: serde_json::Value,
    #[serde(default)]
    location: String,
    #[serde(
        default,
        alias = "uniform_bucket_level_access",
        alias = "uniformBucketLevelAccess"
    )]
    ubla: Option<bool>,
}

fn value_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Creates both buckets.
pub fn create_buckets(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    create_bucket(runner, cfg, &cfg.bucket_name)?;
    create_bucket(runner, cfg, &cfg.asset_bucket_name())
}

/// Creates a bucket in the region with uniform access, or verifies an
/// existing one belongs to this project and region and turns uniform access
/// on. Bucket names are global, so an existing one may be someone else's.
pub fn create_bucket(runner: &dyn Runner, cfg: &GcpConfig, name: &str) -> Result<(), GcpError> {
    let g = Gcloud { runner };
    let url = format!("gs://{name}");
    info!("Checking if bucket {name} exists");
    let out = g.raw(&[
        "storage".into(),
        "buckets".into(),
        "describe".into(),
        url.clone(),
        "--format=json".into(),
    ])?;
    if !out.success {
        if !out.is_not_found() {
            return Err(GcpError::Other(format!(
                "getting bucket {name}: {}",
                out.stderr.trim()
            )));
        }
        info!("Bucket {name} does not exist. Creating...");
        g.run(
            &format!("create bucket {name}"),
            &[
                "storage".into(),
                "buckets".into(),
                "create".into(),
                url,
                format!("--project={}", cfg.project_id),
                format!("--location={}", cfg.region),
                "--uniform-bucket-level-access".into(),
            ],
        )?;
        return Ok(());
    }
    let attrs: BucketAttrs = serde_json::from_str(&out.stdout)
        .map_err(|e| GcpError::Other(format!("parse bucket {name}: {e}")))?;
    let project = value_string(&attrs.project_number);
    if project != cfg.project_number {
        return Err(GcpError::Other(format!(
            "bucket {name} belongs to project number {project}, but expected {} (it may be owned by another GCP project)",
            cfg.project_number
        )));
    }
    if !attrs.location.eq_ignore_ascii_case(&cfg.region) {
        return Err(GcpError::Other(format!(
            "bucket {name} is in location {}, but expected {}",
            attrs.location, cfg.region
        )));
    }
    if attrs.ubla != Some(true) {
        info!("Updating uniform-bucket-level-access on {name}");
        g.run(
            &format!("update bucket {name} ubla"),
            &[
                "storage".into(),
                "buckets".into(),
                "update".into(),
                url,
                "--uniform-bucket-level-access".into(),
            ],
        )?;
    } else {
        info!("uniform-bucket-level-access is already correct on {name}");
    }
    Ok(())
}

/// A role on a bucket for an IAM member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketGrant {
    pub bucket: String,
    pub member: String,
    pub role: String,
}

/// The roles granted on the two buckets. hivekeep-api copies snapshots for
/// tags and deletes the unreferenced ones (objectAdmin, plus bucketViewer to
/// resolve the bucket); the transfer account gets the same, downscoped per
/// transfer by the credential exchange; the keeper only reads micro-VM
/// sandbox assets with its own identity.
pub fn bucket_grants(cfg: &GcpConfig) -> Vec<BucketGrant> {
    let api = workload_identity_principal(cfg, installdefaults::API_SERVER_SERVICE_ACCOUNT);
    let transfer = transfer_service_account_member(cfg);
    let g = |bucket: &str, member: &str, role: &str| BucketGrant {
        bucket: bucket.into(),
        member: member.into(),
        role: role.into(),
    };
    vec![
        g(&cfg.bucket_name, &api, "roles/storage.objectAdmin"),
        g(&cfg.bucket_name, &api, "roles/storage.bucketViewer"),
        g(&cfg.bucket_name, &transfer, "roles/storage.objectAdmin"),
        g(&cfg.bucket_name, &transfer, "roles/storage.bucketViewer"),
        g(
            &cfg.asset_bucket_name(),
            &workload_identity_principal(cfg, installdefaults::KEEPER_SERVICE_ACCOUNT),
            "roles/storage.objectViewer",
        ),
    ]
}

/// Adds the grants on the named bucket that the policy lacks; reports
/// whether it added any.
pub fn add_grants(policy: &mut Policy, name: &str, grants: &[BucketGrant]) -> bool {
    let mut changed = false;
    for g in grants.iter().filter(|g| g.bucket == name) {
        if policy.has_unconditional_binding(&g.role, &g.member) {
            continue;
        }
        info!("Adding {} to {} on bucket {name}", g.role, g.member);
        policy.add_binding(&g.role, &g.member);
        changed = true;
    }
    changed
}

fn get_bucket_policy(runner: &dyn Runner, name: &str) -> Result<Policy, GcpError> {
    let out = Gcloud { runner }.raw(&[
        "storage".into(),
        "buckets".into(),
        "get-iam-policy".into(),
        format!("gs://{name}"),
        "--format=json".into(),
    ])?;
    if !out.success {
        if out.is_not_found() {
            return Err(GcpError::Other(format!(
                "bucket {name} does not exist, run `create bucket` first: {}",
                out.stderr.trim()
            )));
        }
        return Err(GcpError::Other(format!(
            "get bucket {name} iam policy: {}",
            out.stderr.trim()
        )));
    }
    parse_policy(&out.stdout)
}

/// Grants [`bucket_grants`]. Every policy is read before any is written, so
/// a missing bucket fails before anything is granted. Writes are retried: a
/// service account created moments earlier can take a while to propagate.
pub async fn create_iam_policy_bindings(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    retry_delay: Duration,
) -> Result<(), GcpError> {
    let names = [cfg.bucket_name.clone(), cfg.asset_bucket_name()];
    let mut policies = Vec::new();
    for name in &names {
        policies.push(get_bucket_policy(runner, name)?);
    }
    let grants = bucket_grants(cfg);
    for (name, mut policy) in names.iter().zip(policies) {
        let mut attempt = 1;
        loop {
            if !add_grants(&mut policy, name, &grants) {
                info!("IAM policy is already correct on bucket {name}");
                break;
            }
            info!("Setting IAM policy for bucket {name}");
            let file = write_policy(&policy)?;
            let out = Gcloud { runner }.raw(&[
                "storage".into(),
                "buckets".into(),
                "set-iam-policy".into(),
                format!("gs://{name}"),
                file.path().display().to_string(),
                "--format=none".into(),
            ])?;
            if out.success {
                break;
            }
            if attempt >= 6 {
                return Err(GcpError::Other(format!(
                    "set bucket {name} iam policy: {}",
                    out.stderr.trim()
                )));
            }
            crate::warn!(
                "setting bucket {name} IAM policy failed (attempt {attempt}), retrying: {}",
                out.stderr.trim()
            );
            tokio::time::sleep(retry_delay).await;
            policy = get_bucket_policy(runner, name)?;
            attempt += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::project::tests::FakeLookup;

    fn cfg(id: &str, nr: &str, bucket: &str) -> GcpConfig {
        GcpConfig {
            project_id: id.into(),
            project_number: nr.into(),
            bucket_name: bucket.into(),
            ..Default::default()
        }
    }

    #[test]
    fn validate_bucket_flags_cases() {
        let too_long = "b".repeat(57);
        let too_long_err = format!(
            "bucket name \"{too_long}\" is too long: the asset bucket \"{too_long}-assets\" would have 64 characters, and GCS allows 63"
        );
        let cases: Vec<(&str, GcpConfig, String)> = vec![
            (
                "missing bucket name",
                cfg("test-project", "", ""),
                "--name is required".into(),
            ),
            (
                "bucket name too long for the asset bucket",
                cfg("test-project", "", &too_long),
                too_long_err,
            ),
            (
                "longest bucket name the asset bucket allows",
                cfg("test-project", "", &"b".repeat(56)),
                String::new(),
            ),
            (
                "missing project id",
                cfg("", "", "test-bucket"),
                "--project-id is required".into(),
            ),
            (
                "missing project id when only project number provided",
                cfg("", "123456", "test-bucket"),
                "--project-id is required".into(),
            ),
            (
                "all required flags present with both id and number",
                cfg("test-project", "123456", "test-bucket"),
                String::new(),
            ),
            (
                "all required flags present with only project id",
                cfg("test-project", "", "test-bucket"),
                String::new(),
            ),
        ];
        for (name, mut c, want) in cases {
            let res = validate_bucket_flags(&mut c, &FakeLookup::number("projects/123456"));
            if want.is_empty() {
                res.unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(c.project_number, "123456", "{name}");
            } else {
                assert_eq!(res.unwrap_err().to_string(), want, "{name}");
            }
        }
    }

    /// What each member may reach: the keeper reads the asset bucket only,
    /// and the transfer account, like hivekeep-api, reads and writes the
    /// snapshot bucket only.
    #[test]
    fn bucket_grants_per_member() {
        let mut c = cfg("proj", "123", "snaps");
        c.transfer_gsa_name = "hivekeep-snapshot-transfer".into();
        let grants_of = |member: &str| -> Vec<BucketGrant> {
            bucket_grants(&c)
                .into_iter()
                .filter(|g| g.member == member)
                .collect()
        };
        let keeper = workload_identity_principal(&c, "hivekeep-keeper");
        let transfer = "serviceAccount:hivekeep-snapshot-transfer@proj.iam.gserviceaccount.com";
        let g = |b: &str, m: &str, r: &str| BucketGrant {
            bucket: b.into(),
            member: m.into(),
            role: r.into(),
        };
        assert_eq!(
            grants_of(&keeper),
            vec![g("snaps-assets", &keeper, "roles/storage.objectViewer")]
        );
        assert_eq!(
            grants_of(transfer),
            vec![
                g("snaps", transfer, "roles/storage.objectAdmin"),
                g("snaps", transfer, "roles/storage.bucketViewer"),
            ]
        );
    }

    #[test]
    fn add_grants_only_touches_the_named_bucket() {
        let c = cfg("proj", "123", "snaps");
        let member = |sa: &str| {
            format!("principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/proj.svc.id.goog/subject/ns/hivekeep-system/sa/{sa}")
        };
        let mut policy = Policy::default();
        assert!(add_grants(&mut policy, "snaps-assets", &bucket_grants(&c)));
        assert_eq!(
            policy.members("roles/storage.objectViewer"),
            vec![member("hivekeep-keeper")]
        );
        assert!(policy.members("roles/storage.objectAdmin").is_empty());
        assert!(!add_grants(&mut policy, "snaps-assets", &bucket_grants(&c)));
    }

    #[tokio::test]
    async fn bindings_fail_before_granting_when_a_bucket_is_missing() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["storage", "buckets", "get-iam-policy", "gs://snaps-assets"],
            crate::gcp::runner::Output::fail(
                "ERROR: (gcloud.storage) HTTPError 404: bucket not found",
            ),
        );
        let c = cfg("proj", "123", "snaps");
        let err = create_iam_policy_bindings(&runner, &c, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("run `create bucket` first"),
            "{err}"
        );
        assert!(!runner
            .command_lines()
            .iter()
            .any(|l| l.contains("set-iam-policy")));
    }

    #[test]
    fn existing_bucket_in_another_project_is_refused() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["storage", "buckets", "describe"],
            crate::gcp::runner::Output::ok(r#"{"project_number":"999","location":"US-WEST1","uniform_bucket_level_access":true}"#),
        );
        let mut c = cfg("proj", "123", "snaps");
        c.region = "us-west1".into();
        let err = create_bucket(&runner, &c, "snaps").unwrap_err();
        assert!(
            err.to_string().contains("belongs to project number 999"),
            "{err}"
        );
    }
}
