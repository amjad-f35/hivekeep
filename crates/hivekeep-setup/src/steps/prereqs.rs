//! Creating what hivekeep-api and the podcertificate controller need before
//! they start, skipping anything already present.

use super::create::{
    CONFIG_MAP_API_AUTHN, CONFIG_MAP_API_CONFIG, SECRET_ACTOR_ID_CA_CERTS, SECRET_ACTOR_ID_CA_POOL,
    SECRET_POD_IDENTITY_CA, SECRET_POSTGRES_CA, SECRET_SERVICE_DNS_CA,
};
use super::env::{NAMESPACE_POD_CERT, TRUST_BUNDLE_TIMEOUT};
use super::{Env, Result, StepError};
use crate::log;

/// The ClusterTrustBundles the podcertificate controller publishes once it
/// runs. Workloads project them, so nothing depending on pod certificates can
/// start before they exist.
pub const TRUST_BUNDLE_NAMES: [&str; 3] = [
    "podidentity.podcert.hivekeep.io:identity:primary-bundle",
    "servicedns.podcert.hivekeep.io:identity:primary-bundle",
    "postgres.podcert.hivekeep.io:identity:primary-bundle",
];

/// Appended to a trust bundle timeout: the controller rolled out, so a
/// missing bundle means it runs without producing, and these are the places
/// to look.
const TRUST_BUNDLE_DIAGNOSTICS: &str =
    "The podcertificate-controller pod is likely Ready but not producing bundles
(missing CA-pool secret, crash-looping after first Ready, RBAC denial, or a
name mismatch after an upgrade). Investigate with:
  kubectl get clustertrustbundles -A
  kubectl -n podcertificate-controller-system logs deploy/podcertificate-controller --tail=200
  kubectl -n podcertificate-controller-system get pods,secrets,configmaps
  kubectl -n podcertificate-controller-system get events --sort-by=.lastTimestamp | tail -20";

impl Env {
    /// Creates the secrets and config hivekeep-api needs, skipping anything
    /// present. The PostgreSQL settings and an explicitly requested log
    /// level are rewritten every time.
    pub async fn ensure_api_server_prerequisites(&self) -> Result<()> {
        log::step("ensure_apiserver_prerequisites");
        // Not skipped when present: an existing pool may still need its
        // public keys published.
        self.create_jwt_authority_pool_secret().await?;
        let k = self.kube()?;
        let ns = self.namespace();
        if !k.secret_exists(ns, SECRET_ACTOR_ID_CA_POOL).await? {
            self.create_actor_id_ca_pool_secret().await?;
        }
        // Derived from the pool above, so it comes after it.
        if !k.secret_exists(ns, SECRET_ACTOR_ID_CA_CERTS).await? {
            self.create_actor_id_ca_certs_secret().await?;
        }
        self.create_api_server_env_vars().await?;
        self.ensure_api_config().await?;
        if !k.config_map_exists(ns, CONFIG_MAP_API_AUTHN).await? {
            return self.create_api_authentication_config().await;
        }
        Ok(())
    }

    /// Writes the config file when missing or when a log level was asked
    /// for; otherwise an existing (possibly hand-edited) one is kept.
    pub(crate) async fn ensure_api_config(&self) -> Result<()> {
        if !self.cfg.api_log_level.is_empty() {
            return self.create_api_config().await;
        }
        if !self
            .kube()?
            .config_map_exists(self.namespace(), CONFIG_MAP_API_CONFIG)
            .await?
        {
            return self.create_api_config().await;
        }
        Ok(())
    }

    /// Creates the podcertificate signer pools if any is missing.
    pub async fn ensure_pod_certificate_cas(&self) -> Result<()> {
        for name in [
            SECRET_SERVICE_DNS_CA,
            SECRET_POD_IDENTITY_CA,
            SECRET_POSTGRES_CA,
        ] {
            if !self.kube()?.secret_exists(NAMESPACE_POD_CERT, name).await? {
                return self.create_pod_certificate_controller_cas().await;
            }
        }
        Ok(())
    }

    /// Blocks until the podcertificate controller has published every
    /// signer bundle.
    pub async fn wait_for_pod_certificate_trust_bundles(&self) -> Result<()> {
        log::info("Waiting for podcertificate ClusterTrustBundles to be ready...");
        self.kube()?
            .wait_cluster_trust_bundles(
                &TRUST_BUNDLE_NAMES,
                self.cfg.wait_timeout(TRUST_BUNDLE_TIMEOUT),
            )
            .await
            .map_err(|e| StepError::msg(format!("{e}\n{TRUST_BUNDLE_DIAGNOSTICS}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::steps::cluster_fixture::{config_map_data, config_map_object, fake_env};

    /// The level a textproto config sets.
    fn level(textproto: &str) -> String {
        let i = textproto.find("level:").expect("a level") + "level:".len();
        let rest = textproto[i..].trim_start();
        rest.trim_start_matches('"')
            .split('"')
            .next()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn ensure_api_config_cases() {
        let hand_edited = r#"logging { level: "warn" }"#;
        for (name, existing, log_level, want) in [
            ("new install defaults to info", false, "", "info"),
            (
                "new install takes the requested level",
                false,
                "debug",
                "debug",
            ),
            ("existing config is kept", true, "", "warn"),
            (
                "requested level overwrites existing config",
                true,
                "error",
                "error",
            ),
        ] {
            let objs = if existing {
                vec![config_map_object(
                    "hivekeep-system",
                    CONFIG_MAP_API_CONFIG,
                    &[("apiconfig.textproto", hand_edited)],
                )]
            } else {
                vec![]
            };
            let (e, f) = fake_env(
                Config {
                    api_log_level: log_level.into(),
                    ..Default::default()
                },
                objs,
            );
            e.ensure_api_config().await.unwrap();
            let data = config_map_data(&f, "hivekeep-system", CONFIG_MAP_API_CONFIG);
            let text = data
                .get("apiconfig.textproto")
                .and_then(|v| v.as_str())
                .expect(name);
            assert_eq!(level(text), want, "{name}");
        }
    }
}
