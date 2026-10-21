//! Running hivekeep-api against Cloud SQL through the Auth Proxy sidecar:
//! folding the operator's intent over what the cluster records, the
//! passwordless DSN, the proxy's configuration, and adding or removing the
//! sidecar.

use super::create::CONFIG_MAP_API_ENV_VARS;
use super::{Env, Result, StepError};
use crate::config::{
    CloudSqlConfig, CLOUDSQL_IP_TYPE_PRIVATE, CLOUDSQL_IP_TYPE_PSC, CLOUDSQL_IP_TYPE_PUBLIC,
};
use crate::log;
use std::collections::BTreeMap;

/// Keys the proxy sidecar reads from the hivekeep-api-envvars ConfigMap.
/// The proxy takes any flag from a `CSQL_PROXY_`-prefixed variable; the
/// instance connection name is expanded into its args.
pub(crate) const ENV_CLOUDSQL_INSTANCE: &str = "HK_API_POSTGRES_CLOUDSQL_INSTANCE";
pub(crate) const ENV_CSQL_IAM_AUTHN: &str = "CSQL_PROXY_AUTO_IAM_AUTHN";
pub(crate) const ENV_CSQL_PRIVATE_IP: &str = "CSQL_PROXY_PRIVATE_IP";
pub(crate) const ENV_CSQL_PSC: &str = "CSQL_PROXY_PSC";

/// Links the hivekeep-api ServiceAccount to the Google service account the
/// proxy authenticates as.
pub(crate) const WORKLOAD_IDENTITY_ANNOTATION: &str = "iam.gke.io/gcp-service-account";

/// The initContainer name in `install/cloudsql/proxy-sidecar-patch.yaml`,
/// and the marker that a sidecar is installed.
pub(crate) const CLOUDSQL_PROXY_CONTAINER: &str = "cloud-sql-proxy";

/// Trimmed off the service account email to get the IAM database user.
const GSA_EMAIL_SUFFIX: &str = ".gserviceaccount.com";

/// Name of the hivekeep-api Deployment and ServiceAccount.
pub(crate) const API_SERVER: &str = "hivekeep-api";

/// Database the bundled PostgreSQL and the proxy DSN name.
pub(crate) const POSTGRES_DATABASE: &str = "hivekeeppg";

/// The Cloud SQL configuration of this run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CloudSqlSettings {
    /// Empty when Cloud SQL is not in use.
    pub instance: String,
    pub gsa: String,
    pub iam_auth: String,
    pub ip_type: String,
    /// The instance came from the cluster rather than the configuration;
    /// only then are the other settings inherited too.
    pub adopted: bool,
}

/// Folds the configured intent over the settings the cluster records. The
/// record is consulted only when the instance was not supplied at all, so a
/// redeploy that does not mention Cloud SQL regresses nothing to defaults.
pub(crate) fn cloud_sql_settings_from(
    c: &CloudSqlConfig,
    recorded: &BTreeMap<String, String>,
) -> CloudSqlSettings {
    let mut s = CloudSqlSettings {
        instance: c.instance.clone(),
        iam_auth: if c.iam_auth.is_empty() {
            "true".into()
        } else {
            c.iam_auth.clone()
        },
        ip_type: if c.ip_type.is_empty() {
            CLOUDSQL_IP_TYPE_PRIVATE.into()
        } else {
            c.ip_type.clone()
        },
        ..Default::default()
    };
    if c.instance_set {
        return s;
    }
    let get = |k: &str| recorded.get(k).cloned().unwrap_or_default();
    s.instance = get(ENV_CLOUDSQL_INSTANCE);
    if s.instance.is_empty() {
        return s;
    }
    s.adopted = true;
    if c.iam_auth.is_empty() && !get(ENV_CSQL_IAM_AUTHN).is_empty() {
        s.iam_auth = get(ENV_CSQL_IAM_AUTHN);
    }
    if c.ip_type.is_empty() {
        // The proxy defaults to the public address, so neither key recorded
        // means public, not the private default above.
        if !get(ENV_CSQL_PSC).is_empty() {
            s.ip_type = CLOUDSQL_IP_TYPE_PSC.into();
        } else if get(ENV_CSQL_PRIVATE_IP).is_empty() {
            s.ip_type = CLOUDSQL_IP_TYPE_PUBLIC.into();
        }
    }
    s
}

/// The DSN for Cloud SQL through the proxy: plaintext to the sidecar on pod
/// loopback, with the proxy owning TLS and IAM authentication. It is
/// passwordless, so it only logs in when the proxy injects an IAM token.
pub(crate) fn cloud_sql_dsn(s: &CloudSqlSettings) -> Result<String> {
    if s.iam_auth == "false" {
        return Err(StepError::msg(
            "HK_API_POSTGRES_CLOUDSQL_IAM_AUTH=false disables automatic IAM database authentication, so a \
             passwordless DSN cannot be synthesized; set HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING explicitly \
             (host=127.0.0.1 to stay on the proxy)",
        ));
    }
    if s.gsa.is_empty() {
        return Err(StepError::msg(
            "HK_API_POSTGRES_CLOUDSQL_INSTANCE requires HK_API_POSTGRES_CLOUDSQL_GSA (or an explicit \
             HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING)",
        ));
    }
    let user = s.gsa.strip_suffix(GSA_EMAIL_SUFFIX).unwrap_or(&s.gsa);
    Ok(format!(
        "user={user} host=127.0.0.1 port=5432 dbname={POSTGRES_DATABASE} sslmode=disable"
    ))
}

/// The proxy's configuration for the ConfigMap; empty without Cloud SQL,
/// which prunes keys a previous Cloud SQL install left. Health checks use
/// 9801 because hivekeep-api's own servers hold 9090 and 9091.
pub(crate) fn cloud_sql_env_vars(s: &CloudSqlSettings) -> BTreeMap<String, String> {
    if s.instance.is_empty() {
        return BTreeMap::new();
    }
    let mut data: BTreeMap<String, String> = [
        (ENV_CLOUDSQL_INSTANCE, s.instance.as_str()),
        (ENV_CSQL_IAM_AUTHN, s.iam_auth.as_str()),
        ("CSQL_PROXY_PORT", "5432"),
        ("CSQL_PROXY_HEALTH_CHECK", "true"),
        ("CSQL_PROXY_HTTP_ADDRESS", "0.0.0.0"),
        ("CSQL_PROXY_HTTP_PORT", "9801"),
        ("CSQL_PROXY_STRUCTURED_LOGS", "true"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    match s.ip_type.as_str() {
        CLOUDSQL_IP_TYPE_PRIVATE => {
            data.insert(ENV_CSQL_PRIVATE_IP.into(), "true".into());
        }
        CLOUDSQL_IP_TYPE_PSC => {
            data.insert(ENV_CSQL_PSC.into(), "true".into());
        }
        _ => {}
    }
    data
}

impl Env {
    /// The hivekeep-api-envvars ConfigMap data; empty when it is absent.
    pub(crate) async fn recorded_api_server_env_vars(&self) -> Result<BTreeMap<String, String>> {
        let cm = self
            .kube()?
            .get_config_map(self.namespace(), CONFIG_MAP_API_ENV_VARS)
            .await?;
        Ok(cm.and_then(|c| c.data).unwrap_or_default())
    }

    /// Only "is Cloud SQL in play", which the bundled-or-external choice
    /// turns on.
    pub async fn resolve_cloud_sql_instance(&self) -> Result<String> {
        if self.cfg.cloud_sql.instance_set {
            return Ok(self.cfg.cloud_sql.instance.clone());
        }
        Ok(self
            .recorded_api_server_env_vars()
            .await?
            .get(ENV_CLOUDSQL_INSTANCE)
            .cloned()
            .unwrap_or_default())
    }

    /// The full Cloud SQL configuration, reading the cluster for whatever the
    /// configuration left unspecified.
    pub(crate) async fn resolve_cloud_sql(&self) -> Result<CloudSqlSettings> {
        let recorded = if self.cfg.cloud_sql.instance_set {
            BTreeMap::new()
        } else {
            self.recorded_api_server_env_vars().await?
        };
        let mut s = cloud_sql_settings_from(&self.cfg.cloud_sql, &recorded);
        if s.instance.is_empty() {
            return Ok(s);
        }
        if s.adopted {
            log::info(&format!(
                "Cloud SQL config adopted from cluster: {}",
                s.instance
            ));
        }
        s.gsa = self.cfg.cloud_sql.gsa.clone();
        if s.gsa.is_empty() {
            s.gsa = self
                .kube()?
                .service_account_annotation(
                    self.namespace(),
                    API_SERVER,
                    WORKLOAD_IDENTITY_ANNOTATION,
                )
                .await?;
        }
        Ok(s)
    }

    /// Adds or removes the proxy sidecar and the Workload Identity
    /// annotation. Runs after the Deployment is applied, which resets the pod
    /// template to the sidecar-free base. Removal fires only on an
    /// explicitly empty instance.
    pub async fn reconcile_cloud_sql_proxy_sidecar(&self) -> Result<()> {
        let s = self.resolve_cloud_sql().await?;
        let k = self.kube()?;
        let ns = self.namespace();
        if !s.instance.is_empty() {
            log::step("reconcile_cloudsql_proxy_sidecar (add)");
            if !s.gsa.is_empty() {
                k.set_service_account_annotation(
                    ns,
                    API_SERVER,
                    WORKLOAD_IDENTITY_ANNOTATION,
                    &s.gsa,
                )
                .await?;
            }
            let path = self.cfg.manifest(&["cloudsql", "proxy-sidecar-patch.yaml"]);
            let patch = std::fs::read_to_string(&path).map_err(|e| {
                StepError::msg(format!("reading the Cloud SQL proxy sidecar patch: {e}"))
            })?;
            return Ok(k.patch_deployment(ns, API_SERVER, &patch).await?);
        }
        if !self.cloud_sql_proxy_installed().await? {
            return Ok(());
        }
        log::step("reconcile_cloudsql_proxy_sidecar (remove)");
        let remove = format!(
            r#"{{"spec":{{"template":{{"spec":{{"initContainers":[{{"name":"{CLOUDSQL_PROXY_CONTAINER}","$patch":"delete"}}]}}}}}}}}"#
        );
        k.patch_deployment(ns, API_SERVER, &remove).await?;
        Ok(
            k.set_service_account_annotation(ns, API_SERVER, WORKLOAD_IDENTITY_ANNOTATION, "")
                .await?,
        )
    }

    /// Whether hivekeep-api currently runs the proxy sidecar.
    async fn cloud_sql_proxy_installed(&self) -> Result<bool> {
        let Some(dep) = self
            .kube()?
            .get_deployment(self.namespace(), API_SERVER)
            .await?
        else {
            return Ok(false);
        };
        Ok(dep
            .spec
            .and_then(|s| s.template.spec)
            .and_then(|p| p.init_containers)
            .unwrap_or_default()
            .iter()
            .any(|c| c.name == CLOUDSQL_PROXY_CONTAINER))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_config, Config};
    use crate::steps::cluster_fixture::{fake_env, service_account_object};

    fn rec(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn settings(instance: &str, iam: &str, ip: &str, adopted: bool) -> CloudSqlSettings {
        CloudSqlSettings {
            instance: instance.into(),
            iam_auth: iam.into(),
            ip_type: ip.into(),
            adopted,
            ..Default::default()
        }
    }

    #[test]
    fn cloud_sql_settings_from_cases() {
        let set = |i: &str| CloudSqlConfig {
            instance: i.into(),
            instance_set: true,
            ..Default::default()
        };
        let cases: Vec<(
            &str,
            CloudSqlConfig,
            BTreeMap<String, String>,
            CloudSqlSettings,
        )> = vec![
            (
                "unset and nothing recorded",
                CloudSqlConfig::default(),
                rec(&[]),
                settings("", "true", "private", false),
            ),
            (
                "explicit instance defaults to private IAM auth",
                set("p:r:i"),
                rec(&[]),
                settings("p:r:i", "true", "private", false),
            ),
            (
                "explicitly empty instance ignores the record",
                set(""),
                rec(&[(ENV_CLOUDSQL_INSTANCE, "p:r:i"), (ENV_CSQL_PSC, "true")]),
                settings("", "true", "private", false),
            ),
            (
                "adopted instance inherits private IP",
                CloudSqlConfig::default(),
                rec(&[
                    (ENV_CLOUDSQL_INSTANCE, "p:r:i"),
                    (ENV_CSQL_PRIVATE_IP, "true"),
                    (ENV_CSQL_IAM_AUTHN, "false"),
                ]),
                settings("p:r:i", "false", "private", true),
            ),
            (
                "adopted instance inherits PSC",
                CloudSqlConfig::default(),
                rec(&[
                    (ENV_CLOUDSQL_INSTANCE, "p:r:i"),
                    (ENV_CSQL_PSC, "true"),
                    (ENV_CSQL_IAM_AUTHN, "true"),
                ]),
                settings("p:r:i", "true", "psc", true),
            ),
            (
                "adopted instance with no IP key is public",
                CloudSqlConfig::default(),
                rec(&[(ENV_CLOUDSQL_INSTANCE, "p:r:i")]),
                settings("p:r:i", "true", "public", true),
            ),
            (
                "explicit settings outrank the record",
                CloudSqlConfig {
                    ip_type: "public".into(),
                    iam_auth: "false".into(),
                    ..Default::default()
                },
                rec(&[(ENV_CLOUDSQL_INSTANCE, "p:r:i"), (ENV_CSQL_PSC, "true")]),
                settings("p:r:i", "false", "public", true),
            ),
        ];
        for (name, cfg, recorded, want) in cases {
            assert_eq!(cloud_sql_settings_from(&cfg, &recorded), want, "{name}");
        }
    }

    #[tokio::test]
    async fn resolve_cloud_sql_adopts_gsa() {
        let sa = service_account_object(
            "hivekeep-system",
            API_SERVER,
            &[(WORKLOAD_IDENTITY_ANNOTATION, "hk@p.iam.gserviceaccount.com")],
        );
        let cfg = Config {
            cloud_sql: CloudSqlConfig {
                instance: "p:r:i".into(),
                instance_set: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let (e, _f) = fake_env(cfg, vec![sa]);
        assert_eq!(
            e.resolve_cloud_sql().await.unwrap().gsa,
            "hk@p.iam.gserviceaccount.com"
        );
    }

    #[test]
    fn cloud_sql_dsn_cases() {
        let mut s = settings("p:r:i", "true", "", false);
        s.gsa = "hk@p.iam.gserviceaccount.com".into();
        assert_eq!(
            cloud_sql_dsn(&s).unwrap(),
            format!(
                "user=hk@p.iam host=127.0.0.1 port=5432 dbname={POSTGRES_DATABASE} sslmode=disable"
            )
        );
        let mut off = s.clone();
        off.iam_auth = "false".into();
        assert!(cloud_sql_dsn(&off)
            .unwrap_err()
            .to_string()
            .contains("HK_API_POSTGRES_CLOUDSQL_IAM_AUTH=false"));
        let no_gsa = settings("p:r:i", "true", "", false);
        assert!(cloud_sql_dsn(&no_gsa)
            .unwrap_err()
            .to_string()
            .contains("HK_API_POSTGRES_CLOUDSQL_GSA"));
    }

    #[test]
    fn cloud_sql_env_vars_cases() {
        assert!(cloud_sql_env_vars(&CloudSqlSettings::default()).is_empty());
        for (ip, extra) in [
            ("private", Some(ENV_CSQL_PRIVATE_IP)),
            ("psc", Some(ENV_CSQL_PSC)),
            ("public", None),
        ] {
            let got = cloud_sql_env_vars(&settings("p:r:i", "true", ip, false));
            let mut want = vec![
                ENV_CLOUDSQL_INSTANCE,
                ENV_CSQL_IAM_AUTHN,
                "CSQL_PROXY_HEALTH_CHECK",
                "CSQL_PROXY_HTTP_ADDRESS",
                "CSQL_PROXY_HTTP_PORT",
                "CSQL_PROXY_PORT",
                "CSQL_PROXY_STRUCTURED_LOGS",
            ];
            want.extend(extra);
            want.sort();
            let keys: Vec<&str> = got.keys().map(String::as_str).collect();
            assert_eq!(keys, want, "{ip}");
            assert_eq!(got[ENV_CLOUDSQL_INSTANCE], "p:r:i");
        }
    }

    #[test]
    fn proxy_sidecar_patch_names_the_container() {
        let cfg = test_config();
        let patch =
            std::fs::read_to_string(cfg.manifest(&["cloudsql", "proxy-sidecar-patch.yaml"]))
                .unwrap();
        let v: serde_json::Value = serde_yaml::from_str(&patch).unwrap();
        let inits = v
            .pointer("/spec/template/spec/initContainers")
            .and_then(|i| i.as_array())
            .cloned()
            .unwrap_or_default();
        let proxy = inits
            .iter()
            .find(|c| c.get("name").and_then(|n| n.as_str()) == Some(CLOUDSQL_PROXY_CONTAINER))
            .expect("the patch declares the proxy initContainer");
        // A plain initContainer would run to completion and block the pod.
        assert_eq!(
            proxy.get("restartPolicy").and_then(|r| r.as_str()),
            Some("Always")
        );
    }
}
