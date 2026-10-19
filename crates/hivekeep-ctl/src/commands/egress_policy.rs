//! Egress policy commands. Each Actor has at most one egress policy, named
//! `default`; without one, the gate refuses all of the Actor's egress.

use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::cli::DeleteGuards;
use crate::client::Api;
use crate::context::{rpc_error, Ctx};
use crate::error::{status_text, CliError, Result};
use crate::io::Sink;
use crate::output::Printer;
use crate::refs::hived_ref;
use crate::{manifest, print};

/// Parses an egress policy manifest.
pub fn from_manifest(data: &str) -> std::result::Result<pb::EgressPolicy, String> {
    manifest::parse(data, "EgressPolicy")
}

/// Defaults the manifest's empty metadata.hive and metadata.name, then
/// checks that both match the command line: `--hive` and the singleton name.
pub fn override_metadata(policy: &mut pb::EgressPolicy, hive: &str) -> Result<()> {
    let m = policy.metadata.get_or_insert_with(Default::default);
    if m.hive.is_empty() {
        m.hive = hive.to_string();
    }
    if m.name.is_empty() {
        m.name = SINGLETON_NAME.to_string();
    }
    if m.hive != hive {
        return Err(CliError::failed(format!(
            "manifest metadata.hive {:?} does not match --hive {hive:?}",
            m.hive
        )));
    }
    if m.name != SINGLETON_NAME {
        return Err(CliError::failed(format!(
            "manifest metadata.name {:?} must be {SINGLETON_NAME:?}",
            m.name
        )));
    }
    Ok(())
}

/// Rejects a manifest lacking the uid and version preconditions an update
/// needs. Whether they still match is for the server to decide.
pub fn require_preconditions(policy: &pb::EgressPolicy, actor: &pb::ObjectRef) -> Result<()> {
    let m = policy.metadata.clone().unwrap_or_default();
    if m.uid.is_empty() || m.version == 0 {
        return Err(CliError::failed(format!(
            "manifest for actor {:?} in hive {:?} lacks the metadata.uid and metadata.version \
             preconditions that \"get egress-policy -o yaml\" prints",
            actor.name, actor.hive
        )));
    }
    Ok(())
}

/// The delete options for the `--uid`/`--version` guards, rejecting a
/// guard the server would read as "skip the check".
pub fn delete_options(guards: &DeleteGuards) -> Result<Option<pb::DeleteOptions>> {
    if guards.uid.is_none() && guards.version.is_none() {
        return Ok(None);
    }
    if guards.uid.as_deref() == Some("") {
        return Err(CliError::usage("--uid must not be empty"));
    }
    if let Some(v) = guards.version {
        if v < 1 {
            return Err(CliError::usage(format!(
                "--version must be at least 1, got {v}"
            )));
        }
    }
    Ok(Some(pb::DeleteOptions {
        uid: guards.uid.clone().unwrap_or_default(),
        version: guards.version.unwrap_or_default(),
    }))
}

/// Settles a NotFound from an egress policy RPC, which the server answers
/// both for a missing Actor and for an Actor without a policy. Returns
/// `Ok(())` when the Actor exists, so the policy is what is missing.
pub async fn require_actor(api: &Api, actor: &pb::ObjectRef) -> Result<()> {
    match api
        .get_actor(pb::GetActorRequest {
            actor: Some(actor.clone()),
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(s) if s.code() == Code::NotFound => Err(CliError::failed(format!(
            "actor {:?} in hive {:?} not found",
            actor.name, actor.hive
        ))),
        Err(s) => Err(rpc_error(
            format!(
                "failed to get actor {:?} in hive {:?}",
                actor.name, actor.hive
            ),
            &s,
        )),
    }
}

fn subject(actor: &pb::ObjectRef) -> String {
    format!("actor {:?} in hive {:?}", actor.name, actor.hive)
}

/// Gets and prints the policy; no policy is reported on `err`, not as a
/// failure.
pub async fn get_with(
    api: &Api,
    actor: &pb::ObjectRef,
    printer: &Printer,
    out: &Sink,
    err: &Sink,
) -> Result<()> {
    let result = api
        .get_actor_egress_policy(pb::GetActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            name: SINGLETON_NAME.into(),
        })
        .await;
    match result {
        Ok(policy) => print::egress_policy::print(printer, &mut out.clone(), &actor.name, &policy),
        Err(s) if s.code() == Code::NotFound => {
            require_actor(api, actor).await?;
            err.line(&format!("{} has no egress policy", subject(actor)));
            Ok(())
        }
        Err(s) => Err(rpc_error(
            format!("failed to get egress policy for {}", subject(actor)),
            &s,
        )),
    }
}

/// Creates the policy and prints it.
pub async fn create_with(
    api: &Api,
    actor: &pb::ObjectRef,
    policy: pb::EgressPolicy,
    printer: &Printer,
    out: &Sink,
) -> Result<()> {
    let created = api
        .create_actor_egress_policy(pb::CreateActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            egress_policy: Some(policy),
        })
        .await
        .map_err(|s| {
            rpc_error(
                format!("failed to create egress policy for {}", subject(actor)),
                &s,
            )
        })?;
    print::egress_policy::print(printer, &mut out.clone(), &actor.name, &created)
}

/// Replaces the policy and prints the result.
pub async fn update_with(
    api: &Api,
    actor: &pb::ObjectRef,
    policy: pb::EgressPolicy,
    printer: &Printer,
    out: &Sink,
) -> Result<()> {
    let result = api
        .update_actor_egress_policy(pb::UpdateActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            egress_policy: Some(policy),
        })
        .await;
    match result {
        Ok(updated) => print::egress_policy::print(printer, &mut out.clone(), &actor.name, &updated),
        Err(s) if s.code() == Code::NotFound => {
            require_actor(api, actor).await?;
            Err(CliError::failed(format!(
                "{} has no egress policy to update; create it with \"hivectl create egress-policy\"",
                subject(actor)
            )))
        }
        Err(s) if s.code() == Code::Aborted => Err(CliError::failed(format!(
            "manifest's uid and version preconditions do not match those of the stored egress policy \
             for {} (changed since it was read, or read from another actor's policy); re-run \
             \"get egress-policy -o yaml\" and reapply the edit: {}",
            subject(actor),
            status_text(&s)
        ))),
        Err(s) => Err(rpc_error(format!("failed to update egress policy for {}", subject(actor)), &s)),
    }
}

/// Deletes the policy.
pub async fn delete_with(
    api: &Api,
    actor: &pb::ObjectRef,
    options: Option<pb::DeleteOptions>,
    out: &Sink,
) -> Result<()> {
    let result = api
        .delete_actor_egress_policy(pb::DeleteActorEgressPolicyRequest {
            actor: Some(actor.clone()),
            name: SINGLETON_NAME.into(),
            options,
        })
        .await;
    match result {
        Ok(_) => {
            out.line(&format!("egress policy for {} deleted", subject(actor)));
            Ok(())
        }
        Err(s) if s.code() == Code::NotFound => {
            require_actor(api, actor).await?;
            Err(CliError::failed(format!(
                "{} has no egress policy",
                subject(actor)
            )))
        }
        Err(s) => Err(rpc_error(
            format!("failed to delete egress policy for {}", subject(actor)),
            &s,
        )),
    }
}

fn load(ctx: &mut Ctx, filename: &str, hive: &str) -> Result<pb::EgressPolicy> {
    let data = ctx.read_manifest(filename)?;
    let mut policy = from_manifest(&data).map_err(|e| {
        CliError::failed(format!(
            "failed to parse egress policy manifest {filename:?}: {e}"
        ))
    })?;
    override_metadata(&mut policy, hive)?;
    Ok(policy)
}

/// `get egress-policy <actor>`.
pub async fn get(ctx: &mut Ctx, actor: &str, hive: &str) -> Result<()> {
    let api = ctx.api().await?;
    get_with(
        &api,
        &hived_ref(hive, actor),
        &ctx.printer,
        &ctx.out(),
        &ctx.err(),
    )
    .await
}

/// `create egress-policy <actor> -f <manifest>`.
pub async fn create(ctx: &mut Ctx, actor: &str, hive: &str, filename: &str) -> Result<()> {
    let policy = load(ctx, filename, hive)?;
    let api = ctx.api().await?;
    create_with(
        &api,
        &hived_ref(hive, actor),
        policy,
        &ctx.printer,
        &ctx.out(),
    )
    .await
}

/// `update egress-policy <actor> -f <manifest>`.
pub async fn update(ctx: &mut Ctx, actor: &str, hive: &str, filename: &str) -> Result<()> {
    let policy = load(ctx, filename, hive)?;
    let actor = hived_ref(hive, actor);
    // Checked before dialing, so an unusable manifest costs no connection.
    require_preconditions(&policy, &actor)?;
    let api = ctx.api().await?;
    update_with(&api, &actor, policy, &ctx.printer, &ctx.out()).await
}

/// `delete egress-policy <actor>`.
pub async fn delete(ctx: &mut Ctx, actor: &str, hive: &str, guards: &DeleteGuards) -> Result<()> {
    let options = delete_options(guards)?;
    let api = ctx.api().await?;
    delete_with(&api, &hived_ref(hive, actor), options, &ctx.out()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"
metadata:
  hive: team-a
  name: default
  uid: 2f1e3a0c-6c55-4b39-9a1e-1d1a1c1b1a10
  version: "4"
rules:
- https:
    hostnames: ["api.example.com"]
"#;

    #[test]
    fn manifest_parses() {
        let p = from_manifest(MANIFEST).unwrap();
        assert_eq!(p.rules.len(), 1);
        assert_eq!(p.metadata.unwrap().version, 4);
        assert!(from_manifest("rules: 3\n")
            .unwrap_err()
            .contains("invalid EgressPolicy"));
        assert!(from_manifest("bogus: 1\n").is_err());
    }

    #[test]
    fn metadata_defaults_and_checks() {
        let mut p = pb::EgressPolicy::default();
        override_metadata(&mut p, "team-a").unwrap();
        let m = p.metadata.clone().unwrap();
        assert_eq!((m.hive.as_str(), m.name.as_str()), ("team-a", "default"));

        let mut p = from_manifest(MANIFEST).unwrap();
        override_metadata(&mut p, "team-a").unwrap();

        let mut p = from_manifest(MANIFEST).unwrap();
        let err = override_metadata(&mut p, "team-b").unwrap_err();
        assert!(err.message().contains("does not match --hive"), "{err}");

        let mut p = from_manifest("metadata: {name: other}\n").unwrap();
        let err = override_metadata(&mut p, "team-a").unwrap_err();
        assert!(err.message().contains("must be \"default\""), "{err}");
    }

    #[test]
    fn preconditions() {
        let actor = hived_ref("team-a", "a1");
        assert!(require_preconditions(&from_manifest(MANIFEST).unwrap(), &actor).is_ok());
        for data in [
            "rules: []\n",
            "metadata: {uid: u}\n",
            "metadata: {version: 3}\n",
        ] {
            let p = from_manifest(data).unwrap();
            let err = require_preconditions(&p, &actor).unwrap_err();
            assert!(err.message().contains("lacks the metadata.uid"), "{err}");
        }
    }

    #[test]
    fn delete_guards() {
        let g = |uid: Option<&str>, version: Option<i64>| DeleteGuards {
            uid: uid.map(String::from),
            version,
        };
        assert_eq!(delete_options(&g(None, None)).unwrap(), None);
        assert_eq!(
            delete_options(&g(Some("u"), Some(2))).unwrap(),
            Some(pb::DeleteOptions {
                uid: "u".into(),
                version: 2
            })
        );
        assert_eq!(
            delete_options(&g(Some("u"), None)).unwrap(),
            Some(pb::DeleteOptions {
                uid: "u".into(),
                version: 0
            })
        );
        assert!(delete_options(&g(Some(""), None))
            .unwrap_err()
            .message()
            .contains("--uid"));
        assert!(delete_options(&g(None, Some(0)))
            .unwrap_err()
            .message()
            .contains("at least 1"));
        assert!(delete_options(&g(None, Some(-1)))
            .unwrap_err()
            .message()
            .contains("got -1"));
    }
}
