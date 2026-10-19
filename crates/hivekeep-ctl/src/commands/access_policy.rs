//! Access policy commands, for a Hive's policy or the global one. Each has
//! at most one policy, named `default`; without one, it grants nothing.

use std::fmt;

use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::cli::DeleteGuards;
use crate::client::Api;
use crate::commands::egress_policy::delete_options;
use crate::context::{rpc_error, Ctx};
use crate::error::{status_text, CliError, Result};
use crate::io::Sink;
use crate::output::Printer;
use crate::refs::global_ref;
use crate::{manifest, print};

/// The parent of an access policy: one Hive, or the global singleton.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyScope {
    /// The deployment-wide policy.
    Global,
    /// A Hive's policy.
    Hive(String),
}

impl PolicyScope {
    /// Requires exactly one of `--hive` and `--global`.
    pub fn from_flags(hive: Option<&str>, global: bool) -> Result<Self> {
        match (hive.filter(|h| !h.is_empty()), global) {
            (Some(_), true) => Err(CliError::usage(
                "--hive and --global are mutually exclusive",
            )),
            (None, false) => Err(CliError::usage("one of --hive or --global is required")),
            (Some(h), false) => Ok(PolicyScope::Hive(h.to_string())),
            (None, true) => Ok(PolicyScope::Global),
        }
    }

    /// The Hive's name, empty for the global scope.
    pub fn hive(&self) -> &str {
        match self {
            PolicyScope::Global => "",
            PolicyScope::Hive(h) => h,
        }
    }

    fn hive_ref(&self) -> Option<pb::ObjectRef> {
        Some(global_ref(self.hive()))
    }
}

impl fmt::Display for PolicyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyScope::Global => f.write_str("global"),
            PolicyScope::Hive(h) => write!(f, "hive {h:?}"),
        }
    }
}

/// Parses an access policy manifest and checks its metadata: the name is
/// always `default` (filled in when omitted) and the hive always empty,
/// whatever the scope.
pub fn from_manifest(data: &str) -> std::result::Result<pb::AccessPolicy, String> {
    let mut policy: pb::AccessPolicy = manifest::parse(data, "AccessPolicy")?;
    let m = policy.metadata.get_or_insert_with(Default::default);
    if m.name.is_empty() {
        m.name = SINGLETON_NAME.to_string();
    }
    if m.name != SINGLETON_NAME {
        return Err(format!(
            "manifest metadata.name {:?} must be {SINGLETON_NAME:?}",
            m.name
        ));
    }
    if !m.hive.is_empty() {
        return Err(format!(
            "manifest metadata.hive must be empty, got {:?}; choose the hive with --hive",
            m.hive
        ));
    }
    Ok(policy)
}

/// Settles a NotFound from an access policy RPC. For a Hive the server
/// answers NotFound both for a missing Hive and for a Hive without a policy,
/// so this reads the Hive. `Ok(())` means the policy is what is missing.
pub async fn require_scope(api: &Api, scope: &PolicyScope) -> Result<()> {
    let PolicyScope::Hive(hive) = scope else {
        return Ok(());
    };
    match api
        .get_hive(pb::GetHiveRequest {
            hive: scope.hive_ref(),
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(s) if s.code() == Code::NotFound => {
            Err(CliError::failed(format!("hive {hive:?} not found")))
        }
        Err(s) => Err(rpc_error(format!("failed to get hive {hive:?}"), &s)),
    }
}

/// Runs the access policy commands against one scope.
pub struct Runner<'a> {
    /// The client.
    pub api: &'a Api,
    /// The scope.
    pub scope: PolicyScope,
    /// Renders results.
    pub printer: Printer,
    /// Command output.
    pub out: Sink,
    /// Diagnostics.
    pub err: Sink,
}

impl Runner<'_> {
    fn print(&self, policy: &pb::AccessPolicy) -> Result<()> {
        print::access_policy::print(
            &self.printer,
            &mut self.out.clone(),
            self.scope.hive(),
            policy,
        )
    }

    /// Gets and prints the policy; no policy is reported on stderr, not as
    /// a failure.
    pub async fn get(&self) -> Result<()> {
        let name = SINGLETON_NAME.to_string();
        let result = match &self.scope {
            PolicyScope::Global => {
                self.api
                    .get_global_access_policy(pb::GetGlobalAccessPolicyRequest { name })
                    .await
            }
            PolicyScope::Hive(_) => {
                self.api
                    .get_hive_access_policy(pb::GetHiveAccessPolicyRequest {
                        hive: self.scope.hive_ref(),
                        name,
                    })
                    .await
            }
        };
        match result {
            Ok(p) => self.print(&p),
            Err(s) if s.code() == Code::NotFound => {
                require_scope(self.api, &self.scope).await?;
                self.err
                    .line(&format!("{} has no access policy", self.scope));
                Ok(())
            }
            Err(s) => Err(rpc_error(
                format!("failed to get {} access policy", self.scope),
                &s,
            )),
        }
    }

    /// Creates the policy.
    pub async fn create(&self, policy: pb::AccessPolicy) -> Result<()> {
        let result = match &self.scope {
            PolicyScope::Global => {
                self.api
                    .create_global_access_policy(pb::CreateGlobalAccessPolicyRequest {
                        access_policy: Some(policy),
                    })
                    .await
            }
            PolicyScope::Hive(_) => {
                self.api
                    .create_hive_access_policy(pb::CreateHiveAccessPolicyRequest {
                        hive: self.scope.hive_ref(),
                        access_policy: Some(policy),
                    })
                    .await
            }
        };
        match result {
            Ok(p) => self.print(&p),
            Err(s) if s.code() == Code::AlreadyExists => Err(CliError::failed(format!(
                "{} already has an access policy; change it with \"hivectl update access-policy\": {}",
                self.scope,
                status_text(&s)
            ))),
            Err(s) => Err(rpc_error(format!("failed to create {} access policy", self.scope), &s)),
        }
    }

    /// Replaces the policy.
    pub async fn update(&self, policy: pb::AccessPolicy) -> Result<()> {
        let result = match &self.scope {
            PolicyScope::Global => {
                self.api
                    .update_global_access_policy(pb::UpdateGlobalAccessPolicyRequest {
                        access_policy: Some(policy),
                    })
                    .await
            }
            PolicyScope::Hive(_) => {
                self.api
                    .update_hive_access_policy(pb::UpdateHiveAccessPolicyRequest {
                        hive: self.scope.hive_ref(),
                        access_policy: Some(policy),
                    })
                    .await
            }
        };
        match result {
            Ok(p) => self.print(&p),
            Err(s) if s.code() == Code::NotFound => {
                require_scope(self.api, &self.scope).await?;
                Err(CliError::failed(format!(
                    "{} has no access policy to update; create it with \"hivectl create access-policy\"",
                    self.scope
                )))
            }
            Err(s) if s.code() == Code::Aborted => Err(CliError::failed(format!(
                "manifest's uid and version preconditions do not match those of the stored {} access \
                 policy (changed since it was read, or read from another scope); re-run \
                 \"get access-policy -o yaml\" and reapply the edit: {}",
                self.scope,
                status_text(&s)
            ))),
            Err(s) => Err(rpc_error(format!("failed to update {} access policy", self.scope), &s)),
        }
    }

    /// Deletes a Hive's policy.
    pub async fn delete(&self, options: Option<pb::DeleteOptions>) -> Result<()> {
        let result = self
            .api
            .delete_hive_access_policy(pb::DeleteHiveAccessPolicyRequest {
                hive: self.scope.hive_ref(),
                name: SINGLETON_NAME.into(),
                options,
            })
            .await;
        match result {
            Ok(_) => {
                self.out
                    .line(&format!("access policy for {} deleted", self.scope));
                Ok(())
            }
            Err(s) if s.code() == Code::NotFound => {
                require_scope(self.api, &self.scope).await?;
                Err(CliError::failed(format!(
                    "{} has no access policy",
                    self.scope
                )))
            }
            Err(s) => Err(rpc_error(
                format!("failed to delete {} access policy", self.scope),
                &s,
            )),
        }
    }
}

fn load(ctx: &mut Ctx, filename: &str) -> Result<pb::AccessPolicy> {
    let data = ctx.read_manifest(filename)?;
    from_manifest(&data).map_err(|e| {
        CliError::failed(format!(
            "failed to parse access policy manifest {filename:?}: {e}"
        ))
    })
}

fn runner<'a>(ctx: &Ctx, api: &'a Api, scope: PolicyScope) -> Runner<'a> {
    Runner {
        api,
        scope,
        printer: ctx.printer,
        out: ctx.out(),
        err: ctx.err(),
    }
}

/// `get access-policy`.
pub async fn get(ctx: &mut Ctx, hive: Option<&str>, global: bool) -> Result<()> {
    let scope = PolicyScope::from_flags(hive, global)?;
    let api = ctx.api().await?;
    runner(ctx, &api, scope).get().await
}

/// `create access-policy -f <manifest>`.
pub async fn create(ctx: &mut Ctx, hive: Option<&str>, global: bool, filename: &str) -> Result<()> {
    let scope = PolicyScope::from_flags(hive, global)?;
    let policy = load(ctx, filename)?;
    let api = ctx.api().await?;
    runner(ctx, &api, scope).create(policy).await
}

/// `update access-policy -f <manifest>`.
pub async fn update(ctx: &mut Ctx, hive: Option<&str>, global: bool, filename: &str) -> Result<()> {
    let scope = PolicyScope::from_flags(hive, global)?;
    let policy = load(ctx, filename)?;
    // Checked before dialing, so an unusable manifest costs no connection.
    let m = policy.metadata.clone().unwrap_or_default();
    if m.uid.is_empty() || m.version == 0 {
        return Err(CliError::failed(format!(
            "manifest for the {scope} access policy lacks the metadata.uid and metadata.version \
             preconditions that \"get access-policy -o yaml\" prints"
        )));
    }
    let api = ctx.api().await?;
    runner(ctx, &api, scope).update(policy).await
}

/// `delete access-policy -H <hive>`.
pub async fn delete(ctx: &mut Ctx, hive: &str, guards: &DeleteGuards) -> Result<()> {
    let options = delete_options(guards)?;
    let scope = PolicyScope::Hive(hive.to_string());
    let api = ctx.api().await?;
    runner(ctx, &api, scope).delete(options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes() {
        assert_eq!(
            PolicyScope::from_flags(Some("team-a"), false).unwrap(),
            PolicyScope::Hive("team-a".into())
        );
        assert_eq!(
            PolicyScope::from_flags(None, true).unwrap(),
            PolicyScope::Global
        );
        let err = PolicyScope::from_flags(Some("a"), true).unwrap_err();
        assert!(err.message().contains("mutually exclusive"));
        let err = PolicyScope::from_flags(None, false).unwrap_err();
        assert!(err
            .message()
            .contains("one of --hive or --global is required"));
        assert_eq!(PolicyScope::Global.to_string(), "global");
        assert_eq!(PolicyScope::Hive("a".into()).to_string(), "hive \"a\"");
    }

    #[test]
    fn manifests() {
        let p = from_manifest(
            "bindings:\n- role: owner\n  members:\n  - user:system:serviceaccount:team-a:alice\n",
        )
        .unwrap();
        assert_eq!(p.metadata.as_ref().unwrap().name, "default");
        assert_eq!(
            p.bindings[0].members[0],
            "user:system:serviceaccount:team-a:alice"
        );

        let p =
            from_manifest("metadata: {name: default, uid: u-1, version: \"2\"}\nbindings: []\n")
                .unwrap();
        assert_eq!(p.metadata.unwrap().version, 2);

        for (data, want) in [
            ("metadata: {name: other}\n", "must be \"default\""),
            ("metadata: {hive: team-a}\n", "metadata.hive must be empty"),
            (
                "bindings: [{role: owner, members: [a], extra: 1}]\n",
                "extra",
            ),
            ("", "manifest is empty"),
        ] {
            let err = from_manifest(data).unwrap_err();
            assert!(err.contains(want), "{data:?}: {err}");
        }
    }
}
