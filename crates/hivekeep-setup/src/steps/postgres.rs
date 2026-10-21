//! The bundled PostgreSQL: choosing it over an external database, rendering
//! it at the configured size and StorageClass, and creating the fixed
//! development identities inside it.

use super::cloudsql::POSTGRES_DATABASE;
use super::{Env, Result, StepError};
use crate::config::{
    DEFAULT_POSTGRES_OWNER_ROLE, DEFAULT_POSTGRES_READ_WRITE_ROLE, DEFAULT_POSTGRES_SCHEMA,
    SIZE10_POSTGRES_POOL_PARAMS,
};
use crate::kube::{self, describe, Gvk, Object, ObjectExt, WorkloadKind};
use crate::log;
use hivekeep_store::postgres::setup;
use serde_json::Value;
use std::path::PathBuf;

/// Where hivekeep-api reaches the bundled PostgreSQL.
pub(crate) const BUNDLED_POSTGRES_HOST: &str = "postgres.hivekeep-system.svc:5432";

/// The size10 container: no CPU limit (it would only throttle checkpoints on
/// the dedicated node), memory request equal to limit. The size10 config
/// patch is tuned to these numbers.
pub(crate) const SIZE10_POSTGRES_CPU_REQUEST: &str = "80";
pub(crate) const SIZE10_POSTGRES_MEMORY: &str = "140Gi";

/// Selects the pool cordoning pins the bundled PostgreSQL to.
pub(crate) const POSTGRES_POOL_SELECTOR: &str = "hivekeep.io/workloadType=hivekeep-postgres";

/// A bundled connection presenting a certificate for its own login user.
pub(crate) fn bundled_postgres_dsn(user: &str) -> String {
    let bundle = format!("/run/postgres.podcert.hivekeep.io/{user}.pem");
    format!(
        "postgresql://{user}@{BUNDLED_POSTGRES_HOST}/{POSTGRES_DATABASE}?sslmode=verify-full\
         &sslrootcert=/run/servicedns.podcert.hivekeep.io/trust-bundle.pem&sslcert={bundle}&sslkey={bundle}"
    )
}

/// Whether a connection string names the bundled PostgreSQL.
pub(crate) fn uses_bundled_postgres(dsn: &str) -> bool {
    url::Url::parse(dsn).ok().is_some_and(|u| {
        let host = u.host_str().unwrap_or("");
        match u.port() {
            Some(p) => format!("{host}:{p}") == BUNDLED_POSTGRES_HOST,
            None => false,
        }
    })
}

/// Where hivekeep-api's store comes from. It gates both applying the
/// StatefulSet and waiting on it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PostgresPlan {
    pub bundled: bool,
    /// What replaces the bundled database, for the log line.
    pub external: String,
}

fn containers_mut(obj: &mut Object) -> Option<&mut Vec<Value>> {
    obj.pointer_mut("/spec/template/spec/containers")
        .and_then(|c| c.as_array_mut())
}

/// Sets the StorageClass of the StatefulSet's `data` claim.
pub(crate) fn set_postgres_storage_class(objs: &mut [Object], class: &str) -> Result<()> {
    let Some(ss) = objs
        .iter_mut()
        .find(|o| o.kind() == "StatefulSet" && o.name() == "postgres")
    else {
        return Err(StepError::msg(
            "the postgres manifest has no statefulset/postgres to set the StorageClass on",
        ));
    };
    let what = describe(ss);
    let claims = ss
        .pointer_mut("/spec/volumeClaimTemplates")
        .and_then(|c| c.as_array_mut());
    if let Some(claim) = claims.and_then(|c| {
        c.iter_mut()
            .find(|c| c.pointer("/metadata/name").and_then(|n| n.as_str()) == Some("data"))
    }) {
        *claim.at_mut(&["spec", "storageClassName"]) = Value::String(class.into());
        return Ok(());
    }
    Err(StepError::msg(format!(
        "{what} has no data volume claim to set the StorageClass on"
    )))
}

/// Resizes the bundled objects for a dedicated node: the keys the size10
/// config patch names replace the ConfigMap's (others stay with the base),
/// and the container gets the size10 resources.
pub(crate) fn apply_postgres_size10_overrides(objs: &mut [Object], conf_patch: &str) -> Result<()> {
    let patch: Value = serde_yaml::from_str(conf_patch).map_err(|e| {
        StepError::msg(format!(
            "while parsing the size10 postgres config patch: {e}"
        ))
    })?;
    let data = patch
        .get("data")
        .and_then(|d| d.as_object())
        .cloned()
        .unwrap_or_default();
    if data
        .get("postgresql.conf")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .is_empty()
    {
        return Err(StepError::msg(
            "the size10 postgres config patch carries no postgresql.conf",
        ));
    }
    let ns = super::env::NAMESPACE_SYSTEM;
    let cm = objs.iter().position(|o| {
        o.obj_namespace() == ns && o.kind() == "ConfigMap" && o.name() == "postgres-config"
    });
    let ss = objs.iter().position(|o| {
        o.obj_namespace() == ns && o.kind() == "StatefulSet" && o.name() == "postgres"
    });
    let Some(cm) = cm else {
        return Err(StepError::msg(
            "the postgres manifest has no configmap/postgres-config to resize",
        ));
    };
    let Some(ss) = ss else {
        return Err(StepError::msg(
            "the postgres manifest has no statefulset/postgres to resize",
        ));
    };
    for (k, v) in data {
        *objs[cm].at_mut(&["data", &k]) = v;
    }
    let what = describe(&objs[ss]);
    let Some(container) = containers_mut(&mut objs[ss]).and_then(|c| c.first_mut()) else {
        return Err(StepError::msg(format!(
            "{what} has no containers to resize"
        )));
    };
    if let Some(limits) = container
        .pointer_mut("/resources/limits")
        .and_then(|l| l.as_object_mut())
    {
        limits.remove("cpu");
    }
    *container.at_mut(&["resources", "requests", "cpu"]) = SIZE10_POSTGRES_CPU_REQUEST.into();
    *container.at_mut(&["resources", "requests", "memory"]) = SIZE10_POSTGRES_MEMORY.into();
    *container.at_mut(&["resources", "limits", "memory"]) = SIZE10_POSTGRES_MEMORY.into();
    Ok(())
}

impl Env {
    /// The bundled connection strings. The bundled database's identities
    /// are fixed, so custom roles or schema are refused.
    pub(crate) fn postgres_read_write_connection_strings(&self) -> Result<(String, String)> {
        let c = &self.cfg;
        if c.postgres_read_write_role != DEFAULT_POSTGRES_READ_WRITE_ROLE
            || c.postgres_owner_role != DEFAULT_POSTGRES_OWNER_ROLE
            || c.postgres_schema_name() != DEFAULT_POSTGRES_SCHEMA
        {
            return Err(StepError::msg(format!(
                "bundled PostgreSQL requires roles {DEFAULT_POSTGRES_READ_WRITE_ROLE:?} and \
                 {DEFAULT_POSTGRES_OWNER_ROLE:?} and schema {DEFAULT_POSTGRES_SCHEMA:?}"
            )));
        }
        let mut rw = bundled_postgres_dsn(setup::READ_WRITE_USER);
        if c.size10() {
            rw.push_str(SIZE10_POSTGRES_POOL_PARAMS);
        }
        Ok((rw, bundled_postgres_dsn(setup::OWNER_USER)))
    }

    /// Creates the fixed development identities before hivekeep-api starts.
    /// Administrator credentials stay inside the PostgreSQL pod.
    async fn setup_bundled_postgres(&self) -> Result<()> {
        log::step("setup_bundled_postgres");
        let mut command: Vec<String> = [
            "psql",
            "--no-psqlrc",
            "--set=ON_ERROR_STOP=1",
            "--username",
            "postgres",
            "--dbname",
            POSTGRES_DATABASE,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        command.extend(setup::SetupConfig::default().psql_args());
        let out = self
            .kube()?
            .cluster()
            .exec(
                self.namespace(),
                "postgres-0",
                "postgres",
                &command,
                Some(setup::SetupConfig::script().into_bytes()),
            )
            .await;
        out.map(|_| ())
            .map_err(|e| StepError::msg(format!("setting up bundled PostgreSQL identities: {e}")))
    }

    pub(crate) async fn wait_and_setup_bundled_postgres(&self) -> Result<()> {
        self.kube()?
            .rollout_status(
                WorkloadKind::StatefulSet,
                self.namespace(),
                "postgres",
                self.cfg.rollout_timeout,
            )
            .await?;
        self.setup_bundled_postgres().await
    }

    /// Bundled database, or an external one named by DSN or by a Cloud SQL
    /// instance (possibly adopted from the cluster).
    pub async fn plan_postgres(&self) -> Result<PostgresPlan> {
        if !self.cfg.postgres_read_write_connection_string.is_empty() {
            return Ok(PostgresPlan {
                bundled: false,
                external: "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING".into(),
            });
        }
        let instance = self.resolve_cloud_sql_instance().await?;
        if !instance.is_empty() {
            return Ok(PostgresPlan {
                bundled: false,
                external: format!("Cloud SQL instance {instance}"),
            });
        }
        Ok(PostgresPlan {
            bundled: true,
            external: String::new(),
        })
    }

    /// Applies, waits for and sets up the bundled PostgreSQL, or logs that
    /// an external database replaces it.
    pub async fn deploy_postgres_plan(&self, plan: &PostgresPlan) -> Result<()> {
        if !plan.bundled {
            log::step(&format!(
                "Skipping bundled PostgreSQL: external database configured ({})",
                plan.external
            ));
            return Ok(());
        }
        self.apply_postgres().await?;
        self.wait_and_setup_bundled_postgres().await
    }

    /// The bundled manifest: the kind overlay trims it to a laptop.
    pub(crate) fn postgres_manifest_path(&self) -> PathBuf {
        if self.cfg.kind {
            self.cfg.manifest(&["kind", "postgres"])
        } else {
            self.cfg.manifest(&["postgres", "postgres.yaml"])
        }
    }

    /// Renders and applies the bundled StatefulSet at the selected size.
    pub(crate) async fn apply_postgres(&self) -> Result<()> {
        self.require_postgres_pool().await?;
        let objs = self.postgres_objects()?;
        Ok(self.kube()?.apply(&objs).await?)
    }

    /// The bundled objects with StorageClass and size applied to the render
    /// itself, so one apply means one rollout.
    pub(crate) fn postgres_objects(&self) -> Result<Vec<Object>> {
        let manifest = self.render(&self.postgres_manifest_path())?;
        let mut objs = kube::decode_manifest_bytes(&manifest)?;
        if !self.cfg.postgres_storage_class.is_empty() {
            log::step(&format!(
                "Using StorageClass {} for the bundled PostgreSQL volume",
                self.cfg.postgres_storage_class
            ));
            set_postgres_storage_class(&mut objs, &self.cfg.postgres_storage_class)?;
        }
        if self.cfg.size10() {
            log::step("apply_postgres_size10_overrides");
            let conf = std::fs::read_to_string(
                self.cfg
                    .manifest(&["postgres-size10", "postgres-config-patch.yaml"]),
            )
            .map_err(|e| {
                StepError::msg(format!("while reading the size10 postgres config: {e}"))
            })?;
            apply_postgres_size10_overrides(&mut objs, &conf)?;
        }
        Ok(objs)
    }

    /// Under cordoning the StatefulSet deletes its pod before the
    /// replacement schedules, so a cluster with no postgres node is refused
    /// before anything is applied.
    pub(crate) async fn require_postgres_pool(&self) -> Result<()> {
        if !self.cfg.cordon_system_infra_components {
            return Ok(());
        }
        let nodes = self
            .kube()?
            .list(
                &Gvk::new("", "v1", "Node"),
                "",
                Some(POSTGRES_POOL_SELECTOR),
            )
            .await
            .map_err(|e| {
                StepError::msg(format!(
                    "while listing the {POSTGRES_POOL_SELECTOR} nodes: {e}"
                ))
            })?;
        if nodes.is_empty() {
            return Err(StepError::msg(format!(
                "--cordon-system-infra-components pins the bundled PostgreSQL to nodes labeled \
                 {POSTGRES_POOL_SELECTOR}, and the cluster has none: create a one-node pool labeled and tainted \
                 {POSTGRES_POOL_SELECTOR}:NoSchedule in the zone of the postgres volume first (see \
                 docs/architecture.md#system-infra-isolation)"
            )));
        }
        Ok(())
    }

    /// Deploys the single-replica PostgreSQL StatefulSet on its own,
    /// including the podcertificate controller that issues its serving
    /// certificate.
    pub async fn deploy_postgres(&self) -> Result<()> {
        log::step("deploy_postgres");
        self.ensure_system_namespace().await?;
        self.deploy_pod_certificate_controller().await?;
        self.deploy_postgres_plan(&PostgresPlan {
            bundled: true,
            external: String::new(),
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_config, CloudSqlConfig, Config, CLUSTER_SIZE_SIZE10};
    use crate::steps::cloudsql::ENV_CLOUDSQL_INSTANCE;
    use crate::steps::cluster_fixture::{config_map_object, fake_env, node_object};
    use crate::steps::create::CONFIG_MAP_API_ENV_VARS;
    use std::path::Path;

    fn require_kustomize() -> bool {
        let ok = crate::kustomize::available();
        if !ok {
            eprintln!("skipping: no kustomize renderer on PATH");
        }
        ok
    }

    fn env(cfg: Config) -> Env {
        Env::local(cfg)
    }

    fn postgres_objects_for(kind: bool) -> Vec<Object> {
        let e = env(Config {
            kind,
            ..test_config()
        });
        let manifest = e.render(&e.postgres_manifest_path()).unwrap();
        kube::decode_manifest_bytes(&manifest).unwrap()
    }

    fn find<'a>(objs: &'a [Object], kind: &str, name: &str) -> Option<&'a Object> {
        objs.iter().find(|o| o.kind() == kind && o.name() == name)
    }

    fn size10_patch() -> String {
        std::fs::read_to_string(
            test_config().manifest(&["postgres-size10", "postgres-config-patch.yaml"]),
        )
        .unwrap()
    }

    #[test]
    fn postgres_manifest_path_selects_kind_overlay() {
        if !require_kustomize() {
            return;
        }
        let objs = postgres_objects_for(true);
        let ss = find(&objs, "StatefulSet", "postgres")
            .expect("kind overlay renders statefulset/postgres");
        let cpu = ss
            .pointer("/spec/template/spec/containers/0/resources/requests/cpu")
            .and_then(|v| v.as_str());
        assert_eq!(cpu, Some("250m"));
    }

    #[test]
    fn apply_postgres_size10_overrides_over_real_manifest() {
        let mut objs = postgres_objects_for(false);
        let base = find(&objs, "ConfigMap", "postgres-config")
            .expect("configmap/postgres-config")
            .clone();
        apply_postgres_size10_overrides(&mut objs, &size10_patch()).unwrap();
        let cm = find(&objs, "ConfigMap", "postgres-config").unwrap();
        let conf = cm
            .pointer("/data/postgresql.conf")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(conf.contains("max_connections = 500"), "{conf}");
        for key in ["pg_hba.conf", "reload-tls.sh"] {
            assert_eq!(cm["data"][key], base["data"][key], "{key} changed");
        }
        for want in [
            "ssl = on",
            "hba_file = ",
            "ssl_ca_file = '/run/postgres.podcert.hivekeep.io/trust-bundle.pem'",
        ] {
            assert!(conf.contains(want), "size10 postgresql.conf lacks {want:?}");
        }
        let ss = find(&objs, "StatefulSet", "postgres").unwrap();
        let res = ss
            .pointer("/spec/template/spec/containers/0/resources")
            .unwrap();
        assert_eq!(res["requests"]["cpu"], SIZE10_POSTGRES_CPU_REQUEST);
        assert_eq!(res["requests"]["memory"], SIZE10_POSTGRES_MEMORY);
        assert_eq!(res["limits"]["memory"], SIZE10_POSTGRES_MEMORY);
        assert!(res["limits"].get("cpu").is_none(), "limits.cpu kept");
    }

    #[test]
    fn apply_postgres_size10_overrides_rejects_missing_objects() {
        let objs = postgres_objects_for(false);
        let mut without: Vec<Object> = objs
            .iter()
            .filter(|o| o.kind() != "StatefulSet")
            .cloned()
            .collect();
        assert!(apply_postgres_size10_overrides(&mut without, &size10_patch()).is_err());
        let mut all = objs.clone();
        assert!(apply_postgres_size10_overrides(&mut all, "data: {}\n").is_err());
    }

    fn data_claim_storage_class(objs: &[Object]) -> Option<String> {
        let ss = find(objs, "StatefulSet", "postgres").expect("statefulset/postgres");
        let claims = ss
            .pointer("/spec/volumeClaimTemplates")
            .and_then(|c| c.as_array())
            .expect("claims");
        let claim = claims
            .iter()
            .find(|c| c.pointer("/metadata/name").and_then(|n| n.as_str()) == Some("data"))
            .expect("data claim");
        claim
            .pointer("/spec/storageClassName")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    #[test]
    fn set_postgres_storage_class_over_real_manifests() {
        let kinds: &[bool] = if crate::kustomize::available() {
            &[false, true]
        } else {
            &[false]
        };
        for &kind in kinds {
            let mut objs = postgres_objects_for(kind);
            assert_eq!(
                data_claim_storage_class(&objs),
                None,
                "kind={kind}: manifest already names one"
            );
            set_postgres_storage_class(&mut objs, "dynamic-rwo").unwrap();
            assert_eq!(
                data_claim_storage_class(&objs).as_deref(),
                Some("dynamic-rwo"),
                "kind={kind}"
            );
        }
    }

    #[test]
    fn set_postgres_storage_class_rejects_missing_objects() {
        let objs = postgres_objects_for(false);
        let mut without: Vec<Object> = objs
            .iter()
            .filter(|o| o.kind() != "StatefulSet")
            .cloned()
            .collect();
        assert!(set_postgres_storage_class(&mut without, "x").is_err());
        let mut objs = objs;
        let ss = objs.iter_mut().find(|o| o.kind() == "StatefulSet").unwrap();
        ss["spec"]
            .as_object_mut()
            .unwrap()
            .remove("volumeClaimTemplates");
        assert!(set_postgres_storage_class(&mut objs, "x").is_err());
    }

    #[test]
    fn postgres_objects_storage_class() {
        for (name, class, size10, want) in [
            ("unset keeps the cluster default", "", false, None),
            ("set", "dynamic-rwo", false, Some("dynamic-rwo")),
            ("set with size10", "dynamic-rwo", true, Some("dynamic-rwo")),
        ] {
            let mut cfg = Config {
                postgres_storage_class: class.into(),
                ..test_config()
            };
            if size10 {
                cfg.cluster_size = CLUSTER_SIZE_SIZE10.into();
            }
            let objs = env(cfg).postgres_objects().unwrap();
            assert_eq!(data_claim_storage_class(&objs).as_deref(), want, "{name}");
        }
    }

    #[tokio::test]
    async fn plan_postgres_cases() {
        let set = |i: &str| CloudSqlConfig {
            instance: i.into(),
            instance_set: true,
            ..Default::default()
        };
        let bundled = PostgresPlan {
            bundled: true,
            external: String::new(),
        };
        let ext = |s: &str| PostgresPlan {
            bundled: false,
            external: s.into(),
        };
        for (name, conn, cloud, recorded, want) in [
            (
                "no external database",
                "",
                CloudSqlConfig::default(),
                None,
                bundled.clone(),
            ),
            (
                "explicit DSN",
                "postgresql://user@db.example.com:5432/db",
                CloudSqlConfig::default(),
                None,
                ext("HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING"),
            ),
            (
                "Cloud SQL instance from the environment",
                "",
                set("p:r:i"),
                None,
                ext("Cloud SQL instance p:r:i"),
            ),
            (
                "Cloud SQL instance adopted from the cluster",
                "",
                CloudSqlConfig::default(),
                Some("p:r:i"),
                ext("Cloud SQL instance p:r:i"),
            ),
            (
                "explicitly empty instance ignores the cluster record",
                "",
                set(""),
                Some("p:r:i"),
                bundled.clone(),
            ),
        ] {
            let objs = recorded
                .map(|i| {
                    vec![config_map_object(
                        "hivekeep-system",
                        CONFIG_MAP_API_ENV_VARS,
                        &[(ENV_CLOUDSQL_INSTANCE, i)],
                    )]
                })
                .unwrap_or_default();
            let cfg = Config {
                postgres_read_write_connection_string: conn.into(),
                cloud_sql: cloud,
                ..Default::default()
            };
            let (e, _f) = fake_env(cfg, objs);
            assert_eq!(e.plan_postgres().await.unwrap(), want, "{name}");
        }
    }

    #[test]
    fn bundled_postgres_manifest_exists() {
        let cfg = test_config();
        assert!(cfg.manifest(&["postgres", "postgres.yaml"]).is_file());
        // Not at the top level, where the bundle render would apply it even for
        // external databases.
        assert!(!cfg.manifest(&["postgres.yaml"]).exists());
    }

    #[tokio::test]
    async fn require_postgres_pool_cases() {
        let infra = || {
            node_object(
                "infra-1",
                &[("hivekeep.io/workloadType", "hivekeep-system-infra")],
            )
        };
        let pg = || node_object("pg-1", &[("hivekeep.io/workloadType", "hivekeep-postgres")]);
        for (name, cordon, nodes, want_err) in [
            ("not cordoned", false, vec![infra()], false),
            (
                "cordoned with a postgres node",
                true,
                vec![infra(), pg()],
                false,
            ),
            (
                "cordoned without a postgres node",
                true,
                vec![infra()],
                true,
            ),
            ("cordoned with no nodes", true, vec![], true),
        ] {
            let (e, _f) = fake_env(
                Config {
                    cordon_system_infra_components: cordon,
                    ..Default::default()
                },
                nodes,
            );
            let err = e.require_postgres_pool().await.err();
            assert_eq!(err.is_some(), want_err, "{name}");
            if let Some(err) = err {
                assert!(
                    err.to_string().contains("docs/architecture.md"),
                    "{name}: {err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_postgres_requires_postgres_pool() {
        let (e, _f) = fake_env(
            Config {
                cordon_system_infra_components: true,
                ..test_config()
            },
            vec![],
        );
        let err = e.apply_postgres().await.unwrap_err();
        assert!(err.to_string().contains(POSTGRES_POOL_SELECTOR), "{err}");
    }

    #[test]
    fn bundled_postgres_identity_configuration() {
        let e = env(Config {
            postgres_read_write_role: DEFAULT_POSTGRES_READ_WRITE_ROLE.into(),
            postgres_owner_role: DEFAULT_POSTGRES_OWNER_ROLE.into(),
            ..Default::default()
        });
        let (rw, owner) = e.postgres_read_write_connection_strings().unwrap();
        assert_eq!(rw, bundled_postgres_dsn(setup::READ_WRITE_USER));
        assert_eq!(owner, bundled_postgres_dsn(setup::OWNER_USER));
        assert!(uses_bundled_postgres(&rw));
        assert!(!uses_bundled_postgres("postgres://runtime@database/db"));
    }

    #[test]
    fn bundled_postgres_requires_fixed_identity() {
        let e = env(Config {
            postgres_read_write_role: "custom".into(),
            postgres_owner_role: DEFAULT_POSTGRES_OWNER_ROLE.into(),
            ..Default::default()
        });
        assert!(e.postgres_read_write_connection_strings().is_err());
    }

    #[test]
    fn bundled_postgres_manifest_authentication() {
        let text = std::fs::read_to_string(test_config().manifest(&["postgres", "postgres.yaml"]))
            .unwrap();
        for want in [
            "local all all trust",
            "hostssl all postgres all reject",
            &format!(
                "hostssl {POSTGRES_DATABASE} {},{} all cert",
                setup::OWNER_USER,
                setup::READ_WRITE_USER
            ),
            "name: POSTGRES_HOST_AUTH_METHOD",
            "value: trust",
        ] {
            assert!(text.contains(want), "postgres manifest lacks {want:?}");
        }
    }

    /// The signer projecting the file at `path` into the pod: a pod
    /// certificate (checking it requests the right username) or a trust
    /// bundle.
    fn projected_file_signer(pod: &Value, path: &str, trust: bool) -> String {
        let p = Path::new(path);
        let dir = p.parent().unwrap().to_str().unwrap();
        let base = p.file_name().unwrap().to_str().unwrap();
        let empty = vec![];
        let containers = pod
            .get("containers")
            .and_then(|c| c.as_array())
            .unwrap_or(&empty);
        let volumes = pod
            .get("volumes")
            .and_then(|c| c.as_array())
            .unwrap_or(&empty);
        for c in containers {
            for m in c
                .get("volumeMounts")
                .and_then(|m| m.as_array())
                .unwrap_or(&empty)
            {
                if m.get("mountPath").and_then(|v| v.as_str()) != Some(dir) {
                    continue;
                }
                for v in volumes {
                    if v.get("name") != m.get("name") {
                        continue;
                    }
                    for s in v
                        .pointer("/projected/sources")
                        .and_then(|s| s.as_array())
                        .unwrap_or(&empty)
                    {
                        if trust {
                            if let Some(ctb) = s.get("clusterTrustBundle") {
                                if ctb.get("path").and_then(|v| v.as_str()) == Some(base) {
                                    return ctb["signerName"].as_str().unwrap_or("").to_string();
                                }
                            }
                        } else if let Some(pc) = s.get("podCertificate") {
                            if pc.get("credentialBundlePath").and_then(|v| v.as_str()) == Some(base)
                            {
                                let user = pc
                                    .pointer(
                                        "/userAnnotations/postgres.podcert.hivekeep.io~1username",
                                    )
                                    .and_then(|v| v.as_str());
                                assert_eq!(
                                    user,
                                    Some(base.trim_end_matches(".pem")),
                                    "projection for {path}"
                                );
                                return pc["signerName"].as_str().unwrap_or("").to_string();
                            }
                        }
                    }
                }
            }
        }
        panic!("no projected {path} in pod");
    }

    #[test]
    fn postgres_certificate_projections() {
        let e = env(Config {
            postgres_read_write_role: DEFAULT_POSTGRES_READ_WRITE_ROLE.into(),
            postgres_owner_role: DEFAULT_POSTGRES_OWNER_ROLE.into(),
            ..test_config()
        });
        let manifest = e.render(&e.cfg.manifest(&["hivekeep-api.yaml"])).unwrap();
        let objs = kube::decode_manifest_bytes(&manifest).unwrap();
        let api = find(&objs, "Deployment", "hivekeep-api").expect("API deployment");
        let pod = api.pointer("/spec/template/spec").unwrap();
        let (rw, owner) = e.postgres_read_write_connection_strings().unwrap();
        let mut seen = std::collections::HashSet::new();
        for (dsn, user) in [(rw, setup::READ_WRITE_USER), (owner, setup::OWNER_USER)] {
            let u = url::Url::parse(&dsn).unwrap();
            let q: std::collections::HashMap<String, String> =
                u.query_pairs().into_owned().collect();
            assert_eq!(u.username(), user);
            assert!(u.password().is_none());
            assert_eq!(q["sslmode"], "verify-full");
            let cert = &q["sslcert"];
            assert!(
                seen.insert(cert.clone()),
                "owner and runtime share a certificate"
            );
            assert_eq!(cert, &q["sslkey"]);
            assert_eq!(
                Path::new(cert).file_name().unwrap().to_str().unwrap(),
                format!("{user}.pem")
            );
            for param in ["sslcert", "sslkey"] {
                assert_eq!(
                    projected_file_signer(pod, &q[param], false),
                    "postgres.podcert.hivekeep.io/identity"
                );
            }
            assert_eq!(
                projected_file_signer(pod, &q["sslrootcert"], true),
                "servicedns.podcert.hivekeep.io/identity"
            );
        }
        let kinds: &[bool] = if crate::kustomize::available() {
            &[false, true]
        } else {
            &[false]
        };
        for &kind in kinds {
            let objs = postgres_objects_for(kind);
            let db = find(&objs, "StatefulSet", "postgres").unwrap();
            let pod = db.pointer("/spec/template/spec").unwrap();
            assert_eq!(
                projected_file_signer(
                    pod,
                    "/run/postgres.podcert.hivekeep.io/trust-bundle.pem",
                    true
                ),
                "postgres.podcert.hivekeep.io/identity"
            );
        }
    }
}
