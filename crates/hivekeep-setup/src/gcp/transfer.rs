//! The snapshot transfer service account, and Google service accounts in
//! general.

use super::config::GcpConfig;
use super::policy::{add_service_account_binding, Policy};
use super::project::{parse_policy, write_policy};
use super::runner::{Gcloud, Runner};
use super::GcpError;
use crate::info;
use std::time::Duration;

/// Lets a member create tokens for a service account, which hivekeep-api's
/// credential exchange does for the transfer account.
pub const TOKEN_CREATOR_ROLE: &str = "roles/iam.serviceAccountTokenCreator";

pub fn transfer_gsa_email(cfg: &GcpConfig) -> String {
    format!(
        "{}@{}.iam.gserviceaccount.com",
        cfg.transfer_gsa_name, cfg.project_id
    )
}

/// The transfer account as an IAM member.
pub fn transfer_service_account_member(cfg: &GcpConfig) -> String {
    format!("serviceAccount:{}", transfer_gsa_email(cfg))
}

/// Creates the service account `account_id` unless it exists.
pub fn ensure_service_account(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    account_id: &str,
    display_name: &str,
) -> Result<String, GcpError> {
    let g = Gcloud { runner };
    let email = format!("{account_id}@{}.iam.gserviceaccount.com", cfg.project_id);
    let out = g.raw(&[
        "iam".into(),
        "service-accounts".into(),
        "describe".into(),
        email.clone(),
        format!("--project={}", cfg.project_id),
        "--format=json".into(),
    ])?;
    if out.success {
        info!("Service account {email} exists. Skipping create.");
        return Ok(email);
    }
    if !out.is_not_found() {
        return Err(GcpError::Other(format!(
            "get service account {email}: {}",
            out.stderr.trim()
        )));
    }
    info!("Creating service account {email}...");
    g.run(
        &format!("create service account {email}"),
        &[
            "iam".into(),
            "service-accounts".into(),
            "create".into(),
            account_id.into(),
            format!("--project={}", cfg.project_id),
            format!("--display-name={display_name}"),
        ],
    )?;
    Ok(email)
}

pub fn get_service_account_policy(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    email: &str,
) -> Result<Policy, GcpError> {
    let out = Gcloud { runner }.run(
        &format!("get service account {email} iam policy"),
        &[
            "iam".into(),
            "service-accounts".into(),
            "get-iam-policy".into(),
            email.into(),
            format!("--project={}", cfg.project_id),
            "--format=json".into(),
        ],
    )?;
    parse_policy(&out)
}

pub fn set_service_account_policy(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    email: &str,
    policy: &Policy,
) -> Result<(), GcpError> {
    let file = write_policy(policy)?;
    Gcloud { runner }.run(
        "set service account iam policy",
        &[
            "iam".into(),
            "service-accounts".into(),
            "set-iam-policy".into(),
            email.into(),
            file.path().display().to_string(),
            format!("--project={}", cfg.project_id),
            "--format=none".into(),
        ],
    )?;
    Ok(())
}

fn grant_token_creator(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    email: &str,
    members: &[String],
) -> Result<(), GcpError> {
    let mut policy = get_service_account_policy(runner, cfg, email)?;
    let mut changed = false;
    for m in members {
        if add_service_account_binding(&mut policy, TOKEN_CREATOR_ROLE, m) {
            info!("Granting token creator on {email} to {m}");
            changed = true;
        }
    }
    if !changed {
        info!("Service account {email} IAM policy is already correct");
        return Ok(());
    }
    set_service_account_policy(runner, cfg, email, &policy)
}

/// Creates the transfer account unless it exists, and lets each member
/// create tokens for it. Grants are retried with a fresh policy read, since a
/// just-created account (this one or a member) can take a while to
/// propagate.
pub async fn setup_transfer_service_account(
    runner: &dyn Runner,
    cfg: &GcpConfig,
    members: &[String],
    retry_delay: Duration,
) -> Result<(), GcpError> {
    let email = ensure_service_account(
        runner,
        cfg,
        &cfg.transfer_gsa_name,
        "Hivekeep snapshot transfers",
    )?;
    let mut attempt = 1;
    loop {
        match grant_token_creator(runner, cfg, &email, members) {
            Ok(()) => return Ok(()),
            Err(e) if attempt >= 6 => return Err(e),
            Err(e) => {
                crate::warn!(
                    "granting token creator on {email} failed (attempt {attempt}), retrying: {e}"
                );
                tokio::time::sleep(retry_delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::policy::Binding;

    #[test]
    fn add_service_account_binding_cases() {
        let member = "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/proj.svc.id.goog/subject/ns/hivekeep-system/sa/hivekeep-api";
        let b = |members: &[&str], cond: bool| Binding {
            role: TOKEN_CREATOR_ROLE.into(),
            members: members.iter().map(|s| s.to_string()).collect(),
            condition: cond.then(|| serde_json::json!({"expression": "false"})),
        };
        let cases: Vec<(&str, Vec<Binding>, bool, Vec<&str>)> = vec![
            ("no binding yet", vec![], true, vec![member]),
            (
                "joins the role's binding",
                vec![b(&["user:a@example.com"], false)],
                true,
                vec!["user:a@example.com", member],
            ),
            (
                "already a member",
                vec![b(&[member], false)],
                false,
                vec![member],
            ),
            (
                "a conditional binding does not count",
                vec![b(&[member], true)],
                true,
                vec![member],
            ),
        ];
        for (name, bindings, want_changed, want_members) in cases {
            let mut policy = Policy {
                bindings,
                ..Default::default()
            };
            assert_eq!(
                add_service_account_binding(&mut policy, TOKEN_CREATOR_ROLE, member),
                want_changed,
                "{name}"
            );
            assert_eq!(policy.members(TOKEN_CREATOR_ROLE), want_members, "{name}");
        }
    }

    #[test]
    fn transfer_service_account_member_format() {
        let c = GcpConfig {
            project_id: "proj".into(),
            transfer_gsa_name: "xfer".into(),
            ..Default::default()
        };
        assert_eq!(
            transfer_service_account_member(&c),
            "serviceAccount:xfer@proj.iam.gserviceaccount.com"
        );
    }

    #[tokio::test]
    async fn setup_creates_the_account_when_missing() {
        let runner = crate::gcp::runner::FakeRunner::new();
        runner.respond(
            &["iam", "service-accounts", "describe"],
            crate::gcp::runner::Output::fail("NOT_FOUND: Unknown service account"),
        );
        let c = GcpConfig {
            project_id: "proj".into(),
            transfer_gsa_name: "xfer".into(),
            ..Default::default()
        };
        setup_transfer_service_account(&runner, &c, &["user:a@example.com".into()], Duration::ZERO)
            .await
            .unwrap();
        let lines = runner.command_lines();
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("gcloud iam service-accounts create xfer")),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l
                    .contains("service-accounts set-iam-policy xfer@proj.iam.gserviceaccount.com")),
            "{lines:?}"
        );
    }
}
