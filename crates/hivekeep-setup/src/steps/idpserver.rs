//! The actor identity provider (hivekeep-idp-server), which publishes the
//! keys of the actor JWT pool under the install's issuer.

use super::yamldoc::edit_documents;
use super::{Env, Result, StepError};
use serde_json::Value;
use std::path::PathBuf;

const IDP_ISSUER_FLAG: &str = "--issuer";
/// Deployment and container name of the identity provider.
pub const IDP_SERVER: &str = "hivekeep-idp-server";

impl Env {
    /// The hivekeep-idp-server manifest.
    pub(crate) fn idp_server_manifest_path(&self) -> PathBuf {
        self.cfg.manifest(&["hivekeep-idp-server.yaml"])
    }

    /// Applies hivekeep-idp-server on its own.
    pub(crate) async fn deploy_idp_server(&self) -> Result<()> {
        let manifest = self.render_idp_server()?;
        self.resolve_and_apply_bytes(&manifest).await
    }

    /// Renders the hivekeep-idp-server manifest with the install's issuer,
    /// as the system bundle carries it.
    pub(crate) fn render_idp_server(&self) -> Result<Vec<u8>> {
        let manifest = self.render(&self.idp_server_manifest_path())?;
        set_idp_server_issuer(&manifest, &self.cfg.actor_jwt_issuer)
    }
}

/// Sets `--issuer` on the hivekeep-idp-server container, replacing any
/// `--issuer` it already carries.
pub fn set_idp_server_issuer(manifest: &[u8], issuer: &str) -> Result<Vec<u8>> {
    let mut found = false;
    let mut missing_container = false;
    let out = edit_documents(manifest, |obj| {
        if found
            || obj.get("kind").and_then(Value::as_str) != Some("Deployment")
            || obj.pointer("/metadata/name").and_then(Value::as_str) != Some(IDP_SERVER)
        {
            return Ok(false);
        }
        found = true;
        let Some(containers) = obj
            .pointer_mut("/spec/template/spec/containers")
            .and_then(Value::as_array_mut)
        else {
            missing_container = true;
            return Ok(false);
        };
        let Some(c) = containers
            .iter_mut()
            .find(|c| c.get("name").and_then(Value::as_str) == Some(IDP_SERVER))
        else {
            missing_container = true;
            return Ok(false);
        };
        let prefix = format!("{IDP_ISSUER_FLAG}=");
        let mut args: Vec<Value> = c
            .get("args")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|a| !a.as_str().is_some_and(|s| s.starts_with(&prefix)))
            .collect();
        args.push(Value::String(format!("{prefix}{issuer}")));
        c["args"] = Value::Array(args);
        Ok(true)
    })?;
    if !found {
        return Err(StepError::msg(format!(
            "the manifest has no deployment/{IDP_SERVER}"
        )));
    }
    if missing_container {
        return Err(StepError::msg(format!(
            "deployment/{IDP_SERVER} has no {IDP_SERVER} container"
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_config, ROUTER_ENVOY};
    use crate::kube::{decode_manifest_bytes, ObjectExt};
    use k8s_openapi::api::apps::v1::Deployment;
    use k8s_openapi::api::core::v1::PodSpec;
    use std::collections::BTreeMap;

    fn skip() -> bool {
        if !crate::kustomize::available() {
            eprintln!("skipping: no kustomize renderer");
            return true;
        }
        false
    }

    #[test]
    fn idp_server_gets_the_issuer_and_shares_the_api_server_pool() {
        if skip() {
            return;
        }
        const ISSUER: &str = "https://idp.example.com";
        for kind in [false, true] {
            let mut cfg = test_config();
            cfg.router = ROUTER_ENVOY.into();
            cfg.kind = kind;
            let e = Env::local(cfg);
            let mut rendered = e.render(&e.system_overlay_path()).unwrap();
            // Twice, so the second call has an --issuer to replace.
            for _ in 0..2 {
                rendered = set_idp_server_issuer(&rendered, ISSUER).unwrap();
            }
            let mut pods: BTreeMap<String, PodSpec> = BTreeMap::new();
            for obj in decode_manifest_bytes(&rendered).unwrap() {
                if obj.kind() != "Deployment" {
                    continue;
                }
                let dep: Deployment = serde_json::from_value(obj.clone()).unwrap();
                pods.insert(
                    obj.name().to_string(),
                    dep.spec.unwrap().template.spec.unwrap(),
                );
            }
            let api = pods
                .get("hivekeep-api")
                .unwrap_or_else(|| panic!("kind={kind}: no hivekeep-api"));
            let idp = pods
                .get(IDP_SERVER)
                .unwrap_or_else(|| panic!("kind={kind}: no {IDP_SERVER}"));
            let args = idp.containers[0].args.clone().unwrap_or_default();
            let issuer_args: Vec<&String> =
                args.iter().filter(|a| a.starts_with("--issuer=")).collect();
            assert_eq!(
                issuer_args,
                vec![&format!("--issuer={ISSUER}")],
                "kind={kind}"
            );
            assert!(
                idp.containers[0].env.as_ref().is_none_or(|e| e.is_empty()),
                "kind={kind}: idp sets env"
            );
            let volume = |spec: &PodSpec| {
                spec.volumes
                    .as_ref()
                    .and_then(|v| v.iter().find(|v| v.name == "actor-id-jwt-pool").cloned())
            };
            assert!(
                volume(api).is_some(),
                "kind={kind}: hivekeep-api has no actor-id-jwt-pool volume"
            );
            assert_eq!(
                volume(api),
                volume(idp),
                "kind={kind}: actor-id-jwt-pool volume differs"
            );
        }
    }

    #[test]
    fn render_idp_server_sets_the_issuer() {
        const ISSUER: &str = "https://idp.example.com";
        let mut cfg = test_config();
        cfg.actor_jwt_issuer = ISSUER.into();
        let e = Env::local(cfg);
        let rendered = e.render_idp_server().unwrap();
        let objs = decode_manifest_bytes(&rendered).unwrap();
        let idp = objs
            .iter()
            .find(|o| o.kind() == "Deployment" && o.name() == IDP_SERVER)
            .unwrap();
        let dep: Deployment = serde_json::from_value(idp.clone()).unwrap();
        let args = dep.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .unwrap_or_default();
        assert!(args.contains(&format!("--issuer={ISSUER}")), "{args:?}");
    }

    #[test]
    fn set_idp_server_issuer_without_the_deployment() {
        let manifest = b"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: hivekeep-api\n";
        assert!(set_idp_server_issuer(manifest, "https://idp.example.com").is_err());
    }
}
