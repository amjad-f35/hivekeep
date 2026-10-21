//! Resolving the project, enabling APIs, and project-level IAM.

use super::config::{workload_identity_principal, GcpConfig};
use super::policy::{add_project_iam_binding, Policy};
use super::runner::{Gcloud, Runner};
use super::GcpError;
use crate::{info, installdefaults};

/// Normalises the project flags and requires a project ID.
pub fn resolve_project_id(cfg: &mut GcpConfig) -> Result<(), GcpError> {
    cfg.project_id = cfg.project_id.trim_start_matches("projects/").to_string();
    cfg.project_number = cfg
        .project_number
        .trim_start_matches("projects/")
        .to_string();
    if cfg.project_id.is_empty() {
        return Err(GcpError::Usage("--project-id is required".into()));
    }
    Ok(())
}

/// Looks a project's number up by its resource name (`projects/<id>`).
pub trait ProjectLookup {
    fn project_number(&self, name: &str) -> Result<String, GcpError>;
}

/// Asks Resource Manager through gcloud.
pub struct GcloudProjects<'a>(pub &'a dyn Runner);

impl ProjectLookup for GcloudProjects<'_> {
    fn project_number(&self, name: &str) -> Result<String, GcpError> {
        let id = name.trim_start_matches("projects/");
        let out = Gcloud { runner: self.0 }.run(
            "describe project",
            &[
                "projects".into(),
                "describe".into(),
                id.into(),
                "--format=value(projectNumber)".into(),
            ],
        )?;
        Ok(out.trim().to_string())
    }
}

/// Ensures both the project ID and number are set, looking the number up
/// when it was not given.
pub fn resolve_project(cfg: &mut GcpConfig, lookup: &dyn ProjectLookup) -> Result<(), GcpError> {
    resolve_project_id(cfg)?;
    if !cfg.project_number.is_empty() {
        return Ok(());
    }
    let name = format!("projects/{}", cfg.project_id);
    let number = lookup
        .project_number(&name)
        .map_err(|e| GcpError::Other(format!("resolve project info for {name}: {e}")))?;
    cfg.project_number = number.trim_start_matches("projects/").to_string();
    info!(
        "Resolved GCP project number {} for {}",
        cfg.project_number, cfg.project_id
    );
    Ok(())
}

/// The APIs every install needs.
pub const REQUIRED_APIS: &[&str] = &[
    "artifactregistry.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "networkconnectivity.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "telemetry.googleapis.com",
];

/// Enables services in one batch.
pub fn enable_services(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    services: &[&str],
) -> Result<(), GcpError> {
    info!("Batch enabling services: {}", services.join(", "));
    let mut a = vec!["services".to_string(), "enable".to_string()];
    a.extend(services.iter().map(|s| s.to_string()));
    a.push(format!("--project={}", cfg.project_id));
    Gcloud { runner }.run("enable services", &a)?;
    info!("Successfully enabled required services.");
    Ok(())
}

pub fn enable_required_apis(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    enable_services(runner, cfg, REQUIRED_APIS)
}

/// Reads the project's IAM policy.
pub fn get_project_policy(runner: &dyn Runner, cfg: &GcpConfig) -> Result<Policy, GcpError> {
    let out = Gcloud { runner }.run(
        "get project iam policy",
        &[
            "projects".into(),
            "get-iam-policy".into(),
            cfg.project_id.clone(),
            "--format=json".into(),
        ],
    )?;
    parse_policy(&out)
}

/// Writes the project's IAM policy.
pub fn set_project_policy(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    policy: &Policy,
) -> Result<(), GcpError> {
    let file = write_policy(policy)?;
    Gcloud { runner }.run(
        "set project iam policy",
        &[
            "projects".into(),
            "set-iam-policy".into(),
            cfg.project_id.clone(),
            file.path().display().to_string(),
            "--format=none".into(),
        ],
    )?;
    Ok(())
}

pub fn parse_policy(json: &str) -> Result<Policy, GcpError> {
    if json.trim().is_empty() {
        return Ok(Policy::default());
    }
    serde_json::from_str(json).map_err(|e| GcpError::Other(format!("parse iam policy: {e}")))
}

/// Stages a policy in a temporary file for `set-iam-policy`.
pub fn write_policy(policy: &Policy) -> Result<tempfile::NamedTempFile, GcpError> {
    let file = tempfile::Builder::new()
        .prefix("hivekeep-iam-")
        .suffix(".json")
        .tempfile()
        .map_err(|e| GcpError::Other(format!("stage iam policy: {e}")))?;
    let body = serde_json::to_vec_pretty(policy).map_err(|e| GcpError::Other(e.to_string()))?;
    std::fs::write(file.path(), body)
        .map_err(|e| GcpError::Other(format!("stage iam policy: {e}")))?;
    Ok(file)
}

/// Grants `role` to `member` on the project unless it already holds it.
fn grant_project_role(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    role: &str,
    member: &str,
    what: &str,
) -> Result<(), GcpError> {
    let mut policy = get_project_policy(runner, cfg)?;
    if !add_project_iam_binding(&mut policy, role, member) {
        info!(
            "IAM policy already has the required {what} permissions; skipping update ({})",
            cfg.project_id
        );
        return Ok(());
    }
    info!(
        "Setting IAM policy (grant {what} permissions) on {}...",
        cfg.project_id
    );
    set_project_policy(runner, cfg, &policy)
}

/// Lets GKE nodes pull images.
pub fn grant_gke_node_permissions(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let member = format!(
        "serviceAccount:{}-compute@developer.gserviceaccount.com",
        cfg.project_number
    );
    grant_project_role(
        runner,
        cfg,
        "roles/artifactregistry.reader",
        &member,
        "GKE node",
    )
}

/// Lets the keeper pull images. It gets no storage role here; see
/// [`super::bucket::bucket_grants`].
pub fn grant_keeper_permissions(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let member = workload_identity_principal(cfg, installdefaults::KEEPER_SERVICE_ACCOUNT);
    grant_project_role(
        runner,
        cfg,
        "roles/artifactregistry.reader",
        &member,
        "keeper",
    )
}

/// Checks every flag `create iam` needs before any grant runs: each grant is
/// a separate policy write, so a flag validated midway would leave earlier
/// grants already applied.
pub fn validate_iam_flags(
    cfg: &mut GcpConfig,
    bucket_bindings: bool,
    lookup: &dyn ProjectLookup,
) -> Result<(), GcpError> {
    resolve_project(cfg, lookup)?;
    if bucket_bindings && cfg.bucket_name.is_empty() {
        return Err(GcpError::Usage(
            "--bucket is required for bucket bindings".into(),
        ));
    }
    if bucket_bindings {
        return super::bucket::check_asset_bucket_name(cfg);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;

    /// A project lookup that records the names it was asked for.
    pub struct FakeLookup {
        pub result: Result<String, String>,
        pub calls: RefCell<Vec<String>>,
    }

    impl FakeLookup {
        pub fn number(n: &str) -> Self {
            Self {
                result: Ok(n.into()),
                calls: RefCell::default(),
            }
        }
        pub fn failing(msg: &str) -> Self {
            Self {
                result: Err(msg.into()),
                calls: RefCell::default(),
            }
        }
    }

    impl ProjectLookup for FakeLookup {
        fn project_number(&self, name: &str) -> Result<String, GcpError> {
            self.calls.borrow_mut().push(name.into());
            self.result.clone().map_err(GcpError::Other)
        }
    }

    fn cfg(id: &str, number: &str) -> GcpConfig {
        GcpConfig {
            project_id: id.into(),
            project_number: number.into(),
            ..Default::default()
        }
    }

    struct Case {
        name: &'static str,
        cfg: GcpConfig,
        lookup: FakeLookup,
        want_id: &'static str,
        want_nr: &'static str,
        want_err: &'static str,
        want_called: Option<&'static str>,
    }

    #[test]
    fn resolve_project_cases() {
        let cases = vec![
            Case {
                name: "missing project ID",
                cfg: cfg("", ""),
                lookup: FakeLookup::number(""),
                want_id: "",
                want_nr: "",
                want_err: "--project-id is required",
                want_called: None,
            },
            Case {
                name: "only project number provided",
                cfg: cfg("", "987654"),
                lookup: FakeLookup::number(""),
                want_id: "",
                want_nr: "",
                want_err: "--project-id is required",
                want_called: None,
            },
            Case {
                name: "both provided",
                cfg: cfg("my-proj", "123456"),
                lookup: FakeLookup::number("x"),
                want_id: "my-proj",
                want_nr: "123456",
                want_err: "",
                want_called: None,
            },
            Case {
                name: "only project ID provided resolves project number",
                cfg: cfg("my-proj", ""),
                lookup: FakeLookup::number("projects/987654"),
                want_id: "my-proj",
                want_nr: "987654",
                want_err: "",
                want_called: Some("projects/my-proj"),
            },
            Case {
                name: "projects prefix trimmed",
                cfg: cfg("projects/my-proj", ""),
                lookup: FakeLookup::number("987654"),
                want_id: "my-proj",
                want_nr: "987654",
                want_err: "",
                want_called: Some("projects/my-proj"),
            },
            Case {
                name: "resolution API error",
                cfg: cfg("nonexistent-proj", ""),
                lookup: FakeLookup::failing("project not found"),
                want_id: "",
                want_nr: "",
                want_err: "resolve project info for projects/nonexistent-proj: project not found",
                want_called: Some("projects/nonexistent-proj"),
            },
        ];
        for mut c in cases {
            let res = resolve_project(&mut c.cfg, &c.lookup);
            let calls = c.lookup.calls.borrow().clone();
            if !c.want_err.is_empty() {
                let err = res.unwrap_err().to_string();
                assert!(err.contains(c.want_err), "{}: {err}", c.name);
                continue;
            }
            res.unwrap();
            match c.want_called {
                Some(n) => assert_eq!(calls, vec![n.to_string()], "{}", c.name),
                None => assert!(calls.is_empty(), "{}", c.name),
            }
            assert_eq!(c.cfg.project_id, c.want_id, "{}", c.name);
            assert_eq!(c.cfg.project_number, c.want_nr, "{}", c.name);
        }
    }

    #[test]
    fn resolve_project_id_cases() {
        let cases: [(&str, GcpConfig, &str, &str, &str); 5] = [
            (
                "missing project ID",
                cfg("", ""),
                "",
                "",
                "--project-id is required",
            ),
            (
                "only project number provided",
                cfg("", "987654"),
                "",
                "",
                "--project-id is required",
            ),
            (
                "both provided",
                cfg("my-proj", "123456"),
                "my-proj",
                "123456",
                "",
            ),
            (
                "only project ID provided skips API call",
                cfg("my-proj", ""),
                "my-proj",
                "",
                "",
            ),
            (
                "projects prefix trimmed with only project ID",
                cfg("projects/my-proj", ""),
                "my-proj",
                "",
                "",
            ),
        ];
        for (name, mut c, want_id, want_nr, want_err) in cases {
            let res = resolve_project_id(&mut c);
            if !want_err.is_empty() {
                assert!(res.unwrap_err().to_string().contains(want_err), "{name}");
                continue;
            }
            res.unwrap();
            assert_eq!(c.project_id, want_id, "{name}");
            assert_eq!(c.project_number, want_nr, "{name}");
        }
    }

    #[test]
    fn validate_iam_flags_cases() {
        let too_long = "b".repeat(57);
        let too_long_err = format!(
            "bucket name \"{too_long}\" is too long: the asset bucket \"{too_long}-assets\" would have 64 characters, and GCS allows 63"
        );
        let with_bucket = |id: &str, nr: &str, b: &str| GcpConfig {
            bucket_name: b.into(),
            ..cfg(id, nr)
        };
        let cases: Vec<(&str, GcpConfig, bool, String)> = vec![
            (
                "missing project id",
                cfg("", ""),
                true,
                "--project-id is required".into(),
            ),
            (
                "missing project id when only project number provided",
                cfg("", "123"),
                true,
                "--project-id is required".into(),
            ),
            (
                "only project id provided, bucket missing",
                cfg("test-project", ""),
                true,
                "--bucket is required for bucket bindings".into(),
            ),
            (
                "missing bucket while bucket bindings are requested",
                cfg("test-project", "123"),
                true,
                "--bucket is required for bucket bindings".into(),
            ),
            (
                "bucket name too long for the asset bucket",
                with_bucket("test-project", "", &too_long),
                true,
                too_long_err,
            ),
            (
                "bucket not required when bucket bindings are disabled",
                cfg("test-project", "123"),
                false,
                String::new(),
            ),
            (
                "all required flags present with both id and number",
                with_bucket("test-project", "123", "test-bucket"),
                true,
                String::new(),
            ),
            (
                "all required flags present with only project id",
                with_bucket("test-project", "", "test-bucket"),
                true,
                String::new(),
            ),
        ];
        for (name, mut c, bindings, want) in cases {
            let res = validate_iam_flags(&mut c, bindings, &FakeLookup::number("projects/123"));
            if want.is_empty() {
                res.unwrap_or_else(|e| panic!("{name}: {e}"));
            } else {
                assert_eq!(res.unwrap_err().to_string(), want, "{name}");
            }
        }
    }

    #[test]
    fn add_project_iam_binding_cloudsql_roles() {
        let mut policy = Policy::default();
        let member = "serviceAccount:hivekeep-api@my-proj.iam.gserviceaccount.com";
        for role in ["roles/cloudsql.client", "roles/cloudsql.instanceUser"] {
            assert!(
                add_project_iam_binding(&mut policy, role, member),
                "{role} first add"
            );
            assert!(
                !add_project_iam_binding(&mut policy, role, member),
                "{role} repeat add"
            );
        }
    }

    #[test]
    fn grant_is_skipped_when_already_bound() {
        let runner = crate::gcp::runner::FakeRunner::new();
        let c = cfg("p", "42");
        runner.respond(
            &["projects", "get-iam-policy"],
            crate::gcp::runner::Output::ok(
                r#"{"bindings":[{"role":"roles/artifactregistry.reader","members":["serviceAccount:42-compute@developer.gserviceaccount.com"]}],"etag":"x"}"#,
            ),
        );
        grant_gke_node_permissions(&runner, &c).unwrap();
        assert!(!runner
            .command_lines()
            .iter()
            .any(|l| l.contains("set-iam-policy")));
        grant_keeper_permissions(&runner, &c).unwrap();
        assert!(runner
            .command_lines()
            .iter()
            .any(|l| l.contains("set-iam-policy")));
    }
}
