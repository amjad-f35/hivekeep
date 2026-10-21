//! How hivekeep-api reaches its PostgreSQL store and what it is told about
//! the install: the connection Secret, the envvars ConfigMap, the external
//! server CA, the environment digest that turns a changed source into a
//! rollout, and the bootstrap-owner flag.

use super::cloudsql::{cloud_sql_dsn, cloud_sql_env_vars, API_SERVER};
use super::create::{CONFIG_MAP_API_ENV_VARS, SECRET_API_ENV_VARS, SECRET_POSTGRES_SERVER_CA};
use super::postgres::uses_bundled_postgres;
use super::{Env, Result, StepError};
use crate::installdefaults;
use crate::kube::{self, describe, Object, ObjectExt};
use crate::log;
use hivekeep_core::preview::{Gate, ALL_GATES};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Digest of hivekeep-api's environment on its pod template. An envFrom
/// source changing rolls no pods on its own, so the digest is what turns a
/// new DSN into a restart.
pub(crate) const ENV_HASH_ANNOTATION: &str = "hivekeep.io/env-hash";

/// The hivekeep-api flag [`set_api_server_authz_args`] owns.
const API_SERVER_AUTHZ_FLAG: &str = "--authz-bootstrap-owners";

pub(crate) const ENV_READ_WRITE_DSN: &str = "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING";
pub(crate) const ENV_OWNER_DSN: &str = "HK_API_POSTGRES_OWNER_CONNECTION_STRING";
pub(crate) const ENV_SCHEMA: &str = "HK_API_POSTGRES_SCHEMA";
pub(crate) const ENV_READ_WRITE_ROLE: &str = "HK_API_POSTGRES_READ_WRITE_ROLE";
pub(crate) const ENV_OWNER_ROLE: &str = "HK_API_POSTGRES_OWNER_ROLE";
pub(crate) const ENV_POOL_MAX_CONNS: &str = "HK_API_POSTGRES_POOL_MAX_CONNS";
pub(crate) const ENV_ACTOR_JWT_ISSUER: &str = "HK_API_ACTOR_JWT_ISSUER";
pub(crate) const ENV_TRANSFER_SERVICE_ACCOUNT: &str = "HK_API_SNAPSHOT_TRANSFER_SERVICE_ACCOUNT";
pub(crate) const ENV_SNAPSHOT_LOCATION: &str = "HK_API_SNAPSHOT_LOCATION";

/// A warning when the shared client ServiceAccount is a bootstrap owner:
/// it is the default so demos, benchmarks and e2e work, but everyone who can
/// mint its token is then a global owner.
pub(crate) fn shared_bootstrap_owner_warning(
    bootstrap_owners: &str,
    namespace: &str,
) -> Option<String> {
    let shared = installdefaults::client_principal(namespace);
    bootstrap_owners.split(',').any(|o| o.trim() == shared).then(|| {
        format!(
            "{shared} is a bootstrap owner, so anyone who can mint a token for it is a global owner of this \
             install. It is the default so the demos, benchmarks and e2e tests work; for a shared or production \
             install, set --api-authz-bootstrap-owners (HK_API_AUTHZ_BOOTSTRAP_OWNERS) to your operators' own \
             principals. See docs/authorization.md."
        )
    })
}

/// Validates the cluster-wide snapshot location. It may be empty only when
/// the PerTemplateSnapshotLocation preview gate is on, in which case every
/// template sets its own.
pub(crate) fn cluster_snapshot_location(location: &str, gates: &[String]) -> Result<String> {
    if !location.is_empty() {
        hivekeep_core::snapshot::validate_snapshot_location(location)
            .map_err(|e| StepError::msg(format!("invalid --snapshot-location: {e}")))?;
        return Ok(location.to_string());
    }
    if !preview_gate_enabled(gates, Gate::PerTemplateSnapshotLocation) {
        return Err(StepError::msg(format!(
            "--snapshot-location (SNAPSHOT_LOCATION) must be set unless --preview={} is enabled",
            Gate::PerTemplateSnapshotLocation.name()
        )));
    }
    Ok(String::new())
}

/// Whether a `--preview` value enables `gate`.
pub(crate) fn preview_gate_enabled(gates: &[String], gate: Gate) -> bool {
    gates.iter().any(|g| g == ALL_GATES || g == gate.name())
}

/// The Secret payload: both connection strings and the schema. A DSN can
/// carry a password, which is why this is a Secret and not the ConfigMap.
pub(crate) fn build_api_server_env_vars(
    read_write: &str,
    owner: &str,
    schema: &str,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ENV_READ_WRITE_DSN.to_string(), read_write.to_string()),
        (ENV_OWNER_DSN.to_string(), owner.to_string()),
        (ENV_SCHEMA.to_string(), schema.to_string()),
    ])
}

/// Digests the environment sources. Only changes matter; the two sources
/// are separated so a key cannot move between them unnoticed.
pub(crate) fn env_hash(
    config_map: &BTreeMap<String, String>,
    secret: &BTreeMap<String, Vec<u8>>,
) -> String {
    let mut h = Sha256::new();
    h.update(b"configmap\n");
    for (k, v) in config_map {
        h.update(format!("{k}={v}\n").as_bytes());
    }
    h.update(b"secret\n");
    for (k, v) in secret {
        h.update(k.as_bytes());
        h.update(b"=");
        h.update(v);
        h.update(b"\n");
    }
    hex::encode(h.finalize())
}

fn api_server_deployment(objs: &mut [Object]) -> Option<&mut Object> {
    objs.iter_mut()
        .find(|o| o.kind() == "Deployment" && o.name() == API_SERVER)
}

/// Sets the bootstrap owners flag on the hivekeep-api container, replacing
/// any the manifest carries. A changed flag changes the pod template, so it
/// rolls the Deployment.
pub(crate) fn set_api_server_authz_args(objs: &mut [Object], bootstrap_owners: &str) -> Result<()> {
    let Some(dep) = api_server_deployment(objs) else {
        return Err(StepError::msg(format!(
            "the manifest has no deployment/{API_SERVER} to set authorization flags on"
        )));
    };
    let what = describe(dep);
    let Some(containers) = dep
        .pointer_mut("/spec/template/spec/containers")
        .and_then(|c| c.as_array_mut())
    else {
        return Err(StepError::msg(format!("{what} has no containers")));
    };
    let Some(container) = containers
        .iter_mut()
        .find(|c| c.get("name").and_then(|n| n.as_str()) == Some(API_SERVER))
    else {
        return Err(StepError::msg(format!(
            "{what} has no {API_SERVER} container"
        )));
    };
    let mut args: Vec<String> = match container.get("args") {
        None | Some(Value::Null) => vec![],
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| StepError::msg(format!("{what}: non-string arg")))
            })
            .collect::<Result<_>>()?,
        Some(_) => return Err(StepError::msg(format!("{what}: args is not a list"))),
    };
    args.retain(|a| {
        a != API_SERVER_AUTHZ_FLAG && !a.starts_with(&format!("{API_SERVER_AUTHZ_FLAG}="))
    });
    if !bootstrap_owners.trim_matches([' ', ',']).is_empty() {
        args.push(format!("{API_SERVER_AUTHZ_FLAG}={bootstrap_owners}"));
    }
    container["args"] = Value::Array(args.into_iter().map(Value::String).collect());
    Ok(())
}

/// Sets the digest on the hivekeep-api pod template. Without that
/// Deployment the manifest is refused: applying it unstamped would stop a
/// changed environment from rolling.
pub(crate) fn stamp_api_server_env_hash(objs: &mut [Object], hash: &str) -> Result<()> {
    let Some(dep) = api_server_deployment(objs) else {
        return Err(StepError::msg(format!(
            "the manifest has no deployment/{API_SERVER} to stamp with {ENV_HASH_ANNOTATION}"
        )));
    };
    *dep.at_mut(&[
        "spec",
        "template",
        "metadata",
        "annotations",
        ENV_HASH_ANNOTATION,
    ]) = Value::String(hash.into());
    Ok(())
}

impl Env {
    /// Reconciles how hivekeep-api reaches PostgreSQL: both DSNs and the
    /// schema into the Secret, roles, Cloud SQL, the actor JWT issuer, the
    /// transfer account and the snapshot location into the ConfigMap, and an
    /// external server CA into postgres-server-ca. It does not restart
    /// hivekeep-api; [`Env::update_api_server_env_vars`] does.
    pub async fn create_api_server_env_vars(&self) -> Result<()> {
        log::step("create_api_server_env_vars");
        let snapshot_location =
            cluster_snapshot_location(&self.cfg.snapshot_location, &self.cfg.preview_gates)?;
        self.cfg.require_transfer_service_account()?;
        let k = self.kube()?;
        let ns = self.namespace();
        k.ensure_namespace(ns).await?;

        let c = &self.cfg;
        let mut read_write = c.postgres_read_write_connection_string.clone();
        let mut owner = c.postgres_owner_connection_string.clone();
        let mut read_write_role = c.postgres_read_write_role.clone();
        let mut owner_role = c.postgres_owner_role.clone();
        let mut pool_max_conns = c.postgres_pool_max_conns.clone();

        let cloudsql = self.resolve_cloud_sql().await?;
        if read_write.is_empty() && cloudsql.adopted {
            // Fill missing credentials from the adopted configuration.
            let (rec_rw, rec_owner) = self.recorded_connection_strings().await?;
            read_write = rec_rw;
            if owner.is_empty() {
                owner = rec_owner;
            }
        }
        if read_write.is_empty() {
            if !cloudsql.instance.is_empty() {
                read_write = cloud_sql_dsn(&cloudsql)?;
            } else {
                self.refuse_bundled_over_external().await?;
                (read_write, owner) = self.postgres_read_write_connection_strings()?;
            }
        }
        if owner.is_empty() {
            self.refuse_dropping_owner_login().await?;
            owner = read_write.clone();
        }
        let mut schema = c.postgres_schema_name().to_string();
        if cloudsql.adopted {
            let recorded = self.recorded_api_server_env_vars().await?;
            let rec = |key: &str| recorded.get(key).cloned().unwrap_or_default();
            if !c.postgres_read_write_role_set && !rec(ENV_READ_WRITE_ROLE).is_empty() {
                read_write_role = rec(ENV_READ_WRITE_ROLE);
            }
            if !c.postgres_owner_role_set && !rec(ENV_OWNER_ROLE).is_empty() {
                owner_role = rec(ENV_OWNER_ROLE);
            }
            if pool_max_conns.is_empty() {
                pool_max_conns = rec(ENV_POOL_MAX_CONNS);
            }
            if c.postgres_schema.is_empty() {
                if let Some(secret) = k.get_secret(ns, SECRET_API_ENV_VARS).await? {
                    if let Some(v) = secret.data.and_then(|d| d.get(ENV_SCHEMA).cloned()) {
                        if !v.0.is_empty() {
                            schema = String::from_utf8_lossy(&v.0).into_owned();
                        }
                    }
                }
            }
        }
        let mut config_vars = cloud_sql_env_vars(&cloudsql);
        config_vars.insert(ENV_READ_WRITE_ROLE.into(), read_write_role);
        config_vars.insert(ENV_OWNER_ROLE.into(), owner_role);
        if !pool_max_conns.is_empty() {
            config_vars.insert(ENV_POOL_MAX_CONNS.into(), pool_max_conns);
        }
        config_vars.insert(ENV_ACTOR_JWT_ISSUER.into(), c.actor_jwt_issuer.clone());
        // Written empty on kind, so the sidecar's argument is never the
        // literal $(HK_API_SNAPSHOT_TRANSFER_SERVICE_ACCOUNT).
        config_vars.insert(
            ENV_TRANSFER_SERVICE_ACCOUNT.into(),
            c.transfer_service_account(),
        );
        if !snapshot_location.is_empty() {
            config_vars.insert(ENV_SNAPSHOT_LOCATION.into(), snapshot_location);
        }
        k.apply_config_map(ns, CONFIG_MAP_API_ENV_VARS, &config_vars)
            .await?;
        k.apply_secret(
            ns,
            SECRET_API_ENV_VARS,
            &build_api_server_env_vars(&read_write, &owner, &schema),
        )
        .await?;
        self.apply_postgres_server_ca().await
    }

    /// [`Env::create_api_server_env_vars`] for an install not being
    /// redeployed: it also stamps the new digest on the running Deployment,
    /// which rolls it onto the new values.
    pub async fn update_api_server_env_vars(&self) -> Result<()> {
        self.create_api_server_env_vars().await?;
        self.annotate_api_server_env_hash().await
    }

    /// Stops a run that names no database from moving an install off its
    /// external PostgreSQL: records hold no connection strings.
    async fn refuse_bundled_over_external(&self) -> Result<()> {
        // An explicitly empty Cloud SQL instance asks for the bundled one.
        if self.cfg.cloud_sql.instance_set {
            return Ok(());
        }
        let (recorded, _) = self.recorded_connection_strings().await?;
        if recorded.is_empty() || uses_bundled_postgres(&recorded) {
            return Ok(());
        }
        Err(StepError::msg(format!(
            "{API_SERVER} uses an external PostgreSQL, but {ENV_READ_WRITE_DSN} is not set"
        )))
    }

    /// Stops a run that names no owner connection string from replacing a
    /// separate owner login with the read-write one. The bundled database's
    /// owner login is the installer's own, so it does not count.
    async fn refuse_dropping_owner_login(&self) -> Result<()> {
        let (rw, owner) = self.recorded_connection_strings().await?;
        if owner == rw || uses_bundled_postgres(&owner) {
            return Ok(());
        }
        Err(StepError::msg(format!(
            "{API_SERVER} uses a separate PostgreSQL owner login, but {ENV_OWNER_DSN} is not set; set it to the \
             read-write connection string to use one login"
        )))
    }

    /// The connection strings the cluster's Secret holds; the owner falls
    /// back to the read-write one.
    pub(crate) async fn recorded_connection_strings(&self) -> Result<(String, String)> {
        let Some(secret) = self
            .kube()?
            .get_secret(self.namespace(), SECRET_API_ENV_VARS)
            .await?
        else {
            return Ok((String::new(), String::new()));
        };
        let data = secret.data.unwrap_or_default();
        let get = |k: &str| {
            data.get(k)
                .map(|b| String::from_utf8_lossy(&b.0).into_owned())
                .unwrap_or_default()
        };
        let rw = get(ENV_READ_WRITE_DSN);
        let mut owner = get(ENV_OWNER_DSN);
        if owner.is_empty() {
            owner = rw.clone();
        }
        Ok((rw, owner))
    }

    /// Publishes an external PostgreSQL's server CA, which hivekeep-api
    /// mounts at /run/postgres-server-ca/server-ca.pem for sslmode=verify-ca.
    async fn apply_postgres_server_ca(&self) -> Result<()> {
        let path = &self.cfg.postgres_server_ca_file;
        if path.is_empty() {
            return Ok(());
        }
        let pem = std::fs::read_to_string(path)
            .map_err(|e| StepError::msg(format!("reading HK_API_POSTGRES_SERVER_CA_FILE: {e}")))?;
        let data = BTreeMap::from([("server-ca.pem".to_string(), pem)]);
        Ok(self
            .kube()?
            .apply_secret(self.namespace(), SECRET_POSTGRES_SERVER_CA, &data)
            .await?)
    }

    /// Guards a standalone env-vars rewrite on a cluster whose Deployment
    /// predates the DSN moving into the Secret: it would be left without a
    /// DSN on its next restart.
    pub async fn ensure_env_vars_safe_standalone(&self) -> Result<()> {
        let Some(dep) = self
            .kube()?
            .get_deployment(self.namespace(), API_SERVER)
            .await?
        else {
            return Ok(());
        };
        let refs_secret = dep
            .spec
            .and_then(|s| s.template.spec)
            .and_then(|p| p.containers.into_iter().next())
            .and_then(|c| c.env_from)
            .unwrap_or_default()
            .iter()
            .any(|src| {
                src.secret_ref
                    .as_ref()
                    .is_some_and(|r| r.name == SECRET_API_ENV_VARS)
            });
        if refs_secret {
            return Ok(());
        }
        Err(StepError::msg(format!(
            "the running {API_SERVER} Deployment does not reference the {SECRET_API_ENV_VARS} Secret; rewriting \
             the env vars alone would leave it without a DSN on its next restart. Run `hivekeep-setup deploy \
             api-server` instead, which also updates the Deployment"
        )))
    }

    /// Stamps the running Deployment with the digest, so a changed DSN
    /// starts a rollout.
    pub(crate) async fn annotate_api_server_env_hash(&self) -> Result<()> {
        let k = self.kube()?;
        if k.get_deployment(self.namespace(), API_SERVER)
            .await?
            .is_none()
        {
            // The deploy that creates it stamps the digest.
            return Ok(());
        }
        let hash = self.api_server_env_hash().await?;
        let patch = serde_json::json!({"spec": {"template": {"metadata": {"annotations": {ENV_HASH_ANNOTATION: hash}}}}});
        Ok(
            k.patch_deployment(self.namespace(), API_SERVER, &patch.to_string())
                .await?,
        )
    }

    /// Applies a manifest carrying the hivekeep-api Deployment, stamped with
    /// its current environment digest and bootstrap owners, so a new image
    /// and a new environment are one rollout.
    pub async fn apply_with_api_server_env_hash(&self, manifest: &[u8]) -> Result<()> {
        let mut objs = kube::decode_manifest_bytes(manifest)?;
        let hash = self.api_server_env_hash().await?;
        stamp_api_server_env_hash(&mut objs, &hash)?;
        set_api_server_authz_args(&mut objs, &self.cfg.api_authz_bootstrap_owners)?;
        if let Some(msg) =
            shared_bootstrap_owner_warning(&self.cfg.api_authz_bootstrap_owners, self.namespace())
        {
            log::warn(&msg);
        }
        Ok(self.kube()?.apply(&objs).await?)
    }

    /// The digest of hivekeep-api's environment as it is in the cluster.
    pub(crate) async fn api_server_env_hash(&self) -> Result<String> {
        let k = self.kube()?;
        let cm = k
            .get_config_map(self.namespace(), CONFIG_MAP_API_ENV_VARS)
            .await?;
        let secret = k.get_secret(self.namespace(), SECRET_API_ENV_VARS).await?;
        let cm_data = cm.and_then(|c| c.data).unwrap_or_default();
        let secret_data: BTreeMap<String, Vec<u8>> = secret
            .and_then(|s| s.data)
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.0))
            .collect();
        Ok(env_hash(&cm_data, &secret_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CloudSqlConfig, Config, CLUSTER_SIZE_SIZE10, DEFAULT_POSTGRES_OWNER_ROLE,
        DEFAULT_POSTGRES_READ_WRITE_ROLE, SIZE10_POSTGRES_POOL_PARAMS,
    };
    use crate::kube::fake::FakeCluster;
    use crate::steps::cloudsql::WORKLOAD_IDENTITY_ANNOTATION;
    use crate::steps::cluster_fixture::*;
    use crate::steps::postgres::bundled_postgres_dsn;
    use hivekeep_store::postgres::setup;
    use serde_json::json;

    const NS: &str = "hivekeep-system";
    const TEST_TRANSFER_GSA_NAME: &str = "hivekeep-snapshot-transfer";
    const TEST_PROJECT_ID: &str = "proj";
    const TEST_TRANSFER_ACCOUNT: &str = "hivekeep-snapshot-transfer@proj.iam.gserviceaccount.com";

    fn base_cfg() -> Config {
        Config {
            transfer_gsa_name: TEST_TRANSFER_GSA_NAME.into(),
            project_id: TEST_PROJECT_ID.into(),
            snapshot_location: "gs://hivekeep-snapshots/".into(),
            ..Default::default()
        }
    }

    fn bundled_roles(cfg: Config) -> Config {
        Config {
            postgres_read_write_role: DEFAULT_POSTGRES_READ_WRITE_ROLE.into(),
            postgres_owner_role: DEFAULT_POSTGRES_OWNER_ROLE.into(),
            ..cfg
        }
    }

    /// The namespace, an empty envvars ConfigMap and the Secret.
    fn seeded(secret_data: &[(&str, Vec<u8>)]) -> Vec<Object> {
        vec![
            namespace_object(NS),
            config_map_object(NS, CONFIG_MAP_API_ENV_VARS, &[]),
            secret_object(NS, SECRET_API_ENV_VARS, secret_data),
        ]
    }

    fn cm(f: &FakeCluster, key: &str) -> Option<String> {
        config_map_data(f, NS, CONFIG_MAP_API_ENV_VARS)
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    fn sd(f: &FakeCluster, key: &str) -> Option<String> {
        string_data(f, NS, SECRET_API_ENV_VARS, key)
    }

    #[test]
    fn env_hash_moves_with_values_only() {
        let cm = BTreeMap::from([
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
        ]);
        let secret = BTreeMap::from([("DSN".to_string(), b"postgresql://h/db".to_vec())]);
        let base = env_hash(&cm, &secret);
        assert_eq!(base, env_hash(&cm.clone(), &secret), "order dependence");
        assert_ne!(
            base,
            env_hash(
                &cm,
                &BTreeMap::from([("DSN".to_string(), b"postgresql://other/db".to_vec())])
            )
        );
        assert_ne!(
            base,
            env_hash(
                &BTreeMap::from([("A".to_string(), "1".to_string())]),
                &secret
            )
        );
        assert_ne!(
            env_hash(
                &BTreeMap::from([("X".to_string(), "1".to_string())]),
                &BTreeMap::new()
            ),
            env_hash(
                &BTreeMap::new(),
                &BTreeMap::from([("X".to_string(), b"1".to_vec())])
            ),
            "the two sources are indistinguishable"
        );
    }

    #[tokio::test]
    async fn create_api_server_env_vars_postgres_identities() {
        let cases: Vec<(&str, Config, String, String, &str, &str)> = vec![
            (
                "bundled identities",
                bundled_roles(base_cfg()),
                bundled_postgres_dsn(setup::READ_WRITE_USER),
                bundled_postgres_dsn(setup::OWNER_USER),
                DEFAULT_POSTGRES_READ_WRITE_ROLE,
                DEFAULT_POSTGRES_OWNER_ROLE,
            ),
            (
                "size10 bundled identities",
                bundled_roles(Config {
                    cluster_size: CLUSTER_SIZE_SIZE10.into(),
                    ..base_cfg()
                }),
                bundled_postgres_dsn(setup::READ_WRITE_USER) + SIZE10_POSTGRES_POOL_PARAMS,
                bundled_postgres_dsn(setup::OWNER_USER),
                DEFAULT_POSTGRES_READ_WRITE_ROLE,
                DEFAULT_POSTGRES_OWNER_ROLE,
            ),
            (
                "external one login",
                Config {
                    postgres_read_write_connection_string: "postgres://operator@database/db".into(),
                    postgres_read_write_role: "tenant_readwrite".into(),
                    postgres_owner_role: "tenant_owner".into(),
                    ..base_cfg()
                },
                "postgres://operator@database/db".into(),
                "postgres://operator@database/db".into(),
                "tenant_readwrite",
                "tenant_owner",
            ),
            (
                "external separate logins",
                Config {
                    postgres_read_write_connection_string: "postgres://runtime@database/db".into(),
                    postgres_owner_connection_string: "postgres://owner@database/db".into(),
                    postgres_read_write_role: "tenant_readwrite".into(),
                    postgres_owner_role: "tenant_owner".into(),
                    ..base_cfg()
                },
                "postgres://runtime@database/db".into(),
                "postgres://owner@database/db".into(),
                "tenant_readwrite",
                "tenant_owner",
            ),
            (
                "Cloud SQL one login",
                Config {
                    postgres_read_write_role: "tenant_readwrite".into(),
                    postgres_owner_role: "tenant_owner".into(),
                    cloud_sql: CloudSqlConfig {
                        instance: "p:r:i".into(),
                        instance_set: true,
                        gsa: "svc@p.iam.gserviceaccount.com".into(),
                        ..Default::default()
                    },
                    ..base_cfg()
                },
                String::new(),
                String::new(),
                "tenant_readwrite",
                "tenant_owner",
            ),
        ];
        for (name, cfg, want_rw, want_owner, rw_role, owner_role) in cases {
            let cloud = !cfg.cloud_sql.instance.is_empty();
            let (e, f) = fake_env(cfg, seeded(&[]));
            e.create_api_server_env_vars()
                .await
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            let rw = sd(&f, ENV_READ_WRITE_DSN).unwrap_or_default();
            let owner = sd(&f, ENV_OWNER_DSN).unwrap_or_default();
            if cloud {
                assert!(
                    rw.contains("svc@p.iam") && rw == owner,
                    "{name}: {rw:?}, {owner:?}"
                );
            } else {
                assert_eq!((rw, owner), (want_rw, want_owner), "{name}");
            }
            assert_eq!(
                cm(&f, ENV_READ_WRITE_ROLE).as_deref(),
                Some(rw_role),
                "{name}"
            );
            assert_eq!(
                cm(&f, ENV_OWNER_ROLE).as_deref(),
                Some(owner_role),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn create_api_server_env_vars_rejects_custom_bundled_identity() {
        let cfg = Config {
            postgres_read_write_role: "tenant_readwrite".into(),
            postgres_owner_role: "tenant_owner".into(),
            ..base_cfg()
        };
        let (e, _f) = fake_env(cfg, seeded(&[]));
        let err = e.create_api_server_env_vars().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("bundled PostgreSQL requires roles"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn create_api_server_env_vars_keeps_external_postgres() {
        for (name, cloud, recorded, want_err) in [
            (
                "fresh install",
                CloudSqlConfig::default(),
                String::new(),
                false,
            ),
            (
                "bundled install",
                CloudSqlConfig::default(),
                bundled_postgres_dsn(setup::READ_WRITE_USER),
                false,
            ),
            (
                "external install",
                CloudSqlConfig::default(),
                "postgres://runtime@database/db".to_string(),
                true,
            ),
            (
                "leaving Cloud SQL",
                CloudSqlConfig {
                    instance_set: true,
                    ..Default::default()
                },
                "user=svc@p.iam host=127.0.0.1 port=5432 dbname=db sslmode=disable".to_string(),
                false,
            ),
        ] {
            let cfg = bundled_roles(Config {
                cloud_sql: cloud,
                ..base_cfg()
            });
            let (e, f) = fake_env(
                cfg,
                seeded(&[(ENV_READ_WRITE_DSN, recorded.clone().into_bytes())]),
            );
            let res = e.create_api_server_env_vars().await;
            if !want_err {
                res.unwrap_or_else(|err| panic!("{name}: {err}"));
                continue;
            }
            let err = res.unwrap_err();
            assert!(
                err.to_string()
                    .contains("HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING is not set"),
                "{name}: {err}"
            );
            assert_eq!(
                secret_data(&f, NS, SECRET_API_ENV_VARS, ENV_READ_WRITE_DSN),
                Some(recorded),
                "{name}"
            );
            assert_eq!(sd(&f, ENV_READ_WRITE_DSN), None, "{name}: Secret rewritten");
        }
    }

    #[tokio::test]
    async fn create_api_server_env_vars_keeps_owner_login() {
        let rw = "postgres://runtime@database/db";
        let separate = "postgres://owner@database/db";
        for (name, owner, rec_rw, rec_owner, want_err) in [
            (
                "separate owner not given",
                "",
                rw.to_string(),
                separate.to_string(),
                true,
            ),
            (
                "separate owner given",
                separate,
                rw.to_string(),
                separate.to_string(),
                false,
            ),
            ("one login", "", rw.to_string(), rw.to_string(), false),
            (
                "moving off the bundled database",
                "",
                bundled_postgres_dsn(setup::READ_WRITE_USER),
                bundled_postgres_dsn(setup::OWNER_USER),
                false,
            ),
        ] {
            let cfg = Config {
                postgres_read_write_connection_string: rw.into(),
                postgres_owner_connection_string: owner.into(),
                ..base_cfg()
            };
            let (e, f) = fake_env(
                cfg,
                seeded(&[
                    (ENV_READ_WRITE_DSN, rec_rw.into_bytes()),
                    (ENV_OWNER_DSN, rec_owner.clone().into_bytes()),
                ]),
            );
            let res = e.create_api_server_env_vars().await;
            if !want_err {
                res.unwrap_or_else(|err| panic!("{name}: {err}"));
                continue;
            }
            let err = res.unwrap_err();
            assert!(
                err.to_string()
                    .contains("HK_API_POSTGRES_OWNER_CONNECTION_STRING is not set"),
                "{name}: {err}"
            );
            assert_eq!(
                secret_data(&f, NS, SECRET_API_ENV_VARS, ENV_OWNER_DSN),
                Some(rec_owner),
                "{name}"
            );
            assert_eq!(sd(&f, ENV_OWNER_DSN), None, "{name}: Secret rewritten");
        }
    }

    #[tokio::test]
    async fn create_api_server_env_vars_pool_size() {
        let cfg = Config {
            postgres_read_write_connection_string: "postgres://runtime@postgres/db".into(),
            postgres_pool_max_conns: "20".into(),
            ..base_cfg()
        };
        let (e, f) = fake_env(cfg, seeded(&[]));
        e.create_api_server_env_vars().await.unwrap();
        assert_eq!(cm(&f, ENV_POOL_MAX_CONNS).as_deref(), Some("20"));
        assert_eq!(
            sd(&f, ENV_READ_WRITE_DSN).as_deref(),
            Some("postgres://runtime@postgres/db")
        );
    }

    #[tokio::test]
    async fn create_api_server_env_vars_actor_jwt_issuer() {
        let cfg = Config {
            postgres_read_write_connection_string: "postgres://runtime@postgres/db".into(),
            actor_jwt_issuer: "https://idp.example.com/prod".into(),
            ..base_cfg()
        };
        let (e, f) = fake_env(cfg, seeded(&[]));
        e.create_api_server_env_vars().await.unwrap();
        assert_eq!(
            cm(&f, ENV_ACTOR_JWT_ISSUER).as_deref(),
            Some("https://idp.example.com/prod")
        );
    }

    #[tokio::test]
    async fn create_api_server_env_vars_adopts_postgres_identity() {
        let recorded = "user=svc@p.iam host=127.0.0.1 dbname=db";
        let explicit_owner = "user=new-owner@p.iam host=127.0.0.1 dbname=db";
        let cases = [
            (
                "preserve recorded identity",
                bundled_roles(base_cfg()),
                "tenant_readwrite",
                "tenant_owner",
                recorded,
                "tenant_schema",
                "20",
            ),
            (
                "explicit overrides win",
                bundled_roles(Config {
                    postgres_read_write_role_set: true,
                    postgres_owner_role_set: true,
                    postgres_owner_connection_string: explicit_owner.into(),
                    postgres_schema: "other_schema".into(),
                    postgres_pool_max_conns: "30".into(),
                    ..base_cfg()
                }),
                DEFAULT_POSTGRES_READ_WRITE_ROLE,
                DEFAULT_POSTGRES_OWNER_ROLE,
                explicit_owner,
                "other_schema",
                "30",
            ),
        ];
        for (name, cfg, want_rw, want_owner, want_owner_dsn, want_schema, want_pool) in cases {
            let objs = vec![
                namespace_object(NS),
                config_map_object(
                    NS,
                    CONFIG_MAP_API_ENV_VARS,
                    &[
                        ("HK_API_POSTGRES_CLOUDSQL_INSTANCE", "p:r:i"),
                        (ENV_READ_WRITE_ROLE, "tenant_readwrite"),
                        (ENV_OWNER_ROLE, "tenant_owner"),
                        (ENV_POOL_MAX_CONNS, "20"),
                    ],
                ),
                secret_object(
                    NS,
                    SECRET_API_ENV_VARS,
                    &[
                        (ENV_READ_WRITE_DSN, recorded.as_bytes().to_vec()),
                        (ENV_OWNER_DSN, recorded.as_bytes().to_vec()),
                        (ENV_SCHEMA, b"tenant_schema".to_vec()),
                    ],
                ),
                service_account_object(
                    NS,
                    API_SERVER,
                    &[(
                        WORKLOAD_IDENTITY_ANNOTATION,
                        "svc@p.iam.gserviceaccount.com",
                    )],
                ),
            ];
            let (e, f) = fake_env(cfg, objs);
            e.create_api_server_env_vars()
                .await
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(
                cm(&f, ENV_READ_WRITE_ROLE).as_deref(),
                Some(want_rw),
                "{name}"
            );
            assert_eq!(
                cm(&f, ENV_OWNER_ROLE).as_deref(),
                Some(want_owner),
                "{name}"
            );
            assert_eq!(
                cm(&f, ENV_POOL_MAX_CONNS).as_deref(),
                Some(want_pool),
                "{name}"
            );
            assert_eq!(sd(&f, ENV_SCHEMA).as_deref(), Some(want_schema), "{name}");
            assert_eq!(
                sd(&f, ENV_READ_WRITE_DSN).as_deref(),
                Some(recorded),
                "{name}"
            );
            assert_eq!(
                sd(&f, ENV_OWNER_DSN).as_deref(),
                Some(want_owner_dsn),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn recorded_connection_strings_cases() {
        for (name, data, want_owner) in [
            (
                "one DSN",
                vec![(ENV_READ_WRITE_DSN, b"readwrite".to_vec())],
                "readwrite",
            ),
            (
                "separate DSNs",
                vec![
                    (ENV_READ_WRITE_DSN, b"readwrite".to_vec()),
                    (ENV_OWNER_DSN, b"owner".to_vec()),
                ],
                "owner",
            ),
        ] {
            let (e, _f) = fake_env(
                base_cfg(),
                vec![secret_object(NS, SECRET_API_ENV_VARS, &data)],
            );
            let (rw, owner) = e.recorded_connection_strings().await.unwrap();
            assert!(!rw.is_empty(), "{name}");
            assert_eq!(owner, want_owner, "{name}");
        }
    }

    /// A hivekeep-api Deployment whose first container pulls in the named
    /// Secrets through envFrom.
    fn api_server_deployment(secret_refs: &[&str]) -> Object {
        let env_from: Vec<Value> = secret_refs
            .iter()
            .map(|n| json!({"secretRef": {"name": n}}))
            .collect();
        json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": API_SERVER, "namespace": NS},
            "spec": {"selector": {}, "template": {"spec": {"containers": [{"name": API_SERVER, "envFrom": env_from}]}}}
        })
    }

    #[tokio::test]
    async fn ensure_env_vars_safe_standalone_cases() {
        for (name, dep, want_err) in [
            ("fresh install has no Deployment yet", None, false),
            (
                "Deployment already reads the Secret",
                Some(api_server_deployment(&[SECRET_API_ENV_VARS])),
                false,
            ),
            (
                "Deployment predates the Secret",
                Some(api_server_deployment(&[])),
                true,
            ),
        ] {
            let (e, _f) = fake_env(base_cfg(), dep.into_iter().collect());
            match e.ensure_env_vars_safe_standalone().await {
                Err(err) => {
                    assert!(want_err, "{name}: {err}");
                    assert!(
                        err.to_string().contains(SECRET_API_ENV_VARS),
                        "{name}: {err}"
                    );
                }
                Ok(()) => assert!(!want_err, "{name}: want an error"),
            }
        }
    }

    fn stamped(f: &FakeCluster) -> Option<String> {
        f.object(
            &crate::kube::Gvk::new("apps", "v1", "Deployment"),
            NS,
            API_SERVER,
        )
        .and_then(|d| {
            d.pointer(&format!(
                "/spec/template/metadata/annotations/{}",
                ENV_HASH_ANNOTATION.replace('/', "~1")
            ))
            .cloned()
        })
        .and_then(|v| v.as_str().map(str::to_string))
    }

    #[tokio::test]
    async fn annotate_api_server_env_hash_cases() {
        let (e, _f) = fake_env(base_cfg(), vec![]);
        e.annotate_api_server_env_hash().await.unwrap();

        let data = vec![(ENV_READ_WRITE_DSN, b"postgresql://h/db".to_vec())];
        let (e, f) = fake_env(
            base_cfg(),
            vec![
                api_server_deployment(&[SECRET_API_ENV_VARS]),
                secret_object(NS, SECRET_API_ENV_VARS, &data),
            ],
        );
        e.annotate_api_server_env_hash().await.unwrap();
        let want = env_hash(
            &BTreeMap::new(),
            &data
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        );
        assert_eq!(stamped(&f), Some(want));
    }

    #[tokio::test]
    async fn only_a_standalone_env_update_rolls_the_api_server() {
        let new_env = || {
            let cfg = Config {
                postgres_read_write_connection_string: "postgres://runtime@postgres/db".into(),
                ..base_cfg()
            };
            let mut objs = seeded(&[]);
            objs.push(api_server_deployment(&[SECRET_API_ENV_VARS]));
            fake_env(cfg, objs)
        };
        let (e, f) = new_env();
        e.create_api_server_env_vars().await.unwrap();
        assert_eq!(stamped(&f), None, "create patched the running Deployment");
        let (e, f) = new_env();
        e.update_api_server_env_vars().await.unwrap();
        assert!(
            stamped(&f).is_some(),
            "update left the Deployment unstamped"
        );
    }

    #[test]
    fn stamp_api_server_env_hash_cases() {
        let mut objs = kube::decode_manifest(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: hivekeep-api\n  namespace: hivekeep-system\nspec:\n  \
             template:\n    metadata:\n      annotations:\n        prometheus.io/scrape: \"true\"\n---\napiVersion: apps/v1\n\
             kind: Deployment\nmetadata:\n  name: hivekeep-controller\n  namespace: hivekeep-system\nspec:\n  template:\n    \
             metadata: {}\n",
        )
        .unwrap();
        stamp_api_server_env_hash(&mut objs, "digest").unwrap();
        let ann = |o: &Object| {
            o.pointer("/spec/template/metadata/annotations")
                .cloned()
                .unwrap_or(Value::Null)
        };
        assert_eq!(ann(&objs[0])[ENV_HASH_ANNOTATION], "digest");
        assert_eq!(ann(&objs[0])["prometheus.io/scrape"], "true");
        assert!(ann(&objs[1]).get(ENV_HASH_ANNOTATION).is_none());
        assert!(stamp_api_server_env_hash(&mut objs[1..], "digest").is_err());
    }

    #[test]
    fn set_api_server_authz_args_cases() {
        let manifest = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: hivekeep-api\n  namespace: hivekeep-system\n\
spec:\n  template:\n    spec:\n      containers:\n      - name: objectstore-plugin\n        args: [\"--keep\"]\n      \
- name: hivekeep-api\n        args:\n        - --config-file=/run/apiconfig.hivekeep.io/apiconfig.textproto\n        \
- --authz-bootstrap-owners=stale\n";
        let cfg_flag = "--config-file=/run/apiconfig.hivekeep.io/apiconfig.textproto";
        for (name, owners, want) in [
            (
                "replaces the owners",
                "alice,bob",
                vec![cfg_flag, "--authz-bootstrap-owners=alice,bob"],
            ),
            ("no owners drops the flag", "", vec![cfg_flag]),
        ] {
            let mut objs = kube::decode_manifest(manifest).unwrap();
            set_api_server_authz_args(&mut objs, owners).unwrap();
            let args =
                |i: usize| objs[0]["spec"]["template"]["spec"]["containers"][i]["args"].clone();
            assert_eq!(args(1), json!(want), "{name}");
            assert_eq!(
                args(0),
                json!(["--keep"]),
                "{name}: other container changed"
            );
        }
        let mut objs = kube::decode_manifest(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: hivekeep-controller\n",
        )
        .unwrap();
        assert!(set_api_server_authz_args(&mut objs, "alice").is_err());
    }

    #[tokio::test]
    async fn create_api_server_env_vars_omits_authz() {
        let cfg = bundled_roles(Config {
            api_authz_bootstrap_owners: "system:serviceaccount:hivekeep-system:hivekeep-client"
                .into(),
            ..base_cfg()
        });
        let (e, f) = fake_env(cfg, seeded(&[]));
        e.create_api_server_env_vars().await.unwrap();
        assert_eq!(cm(&f, "HK_API_AUTHZ_BOOTSTRAP_OWNERS"), None);
    }

    #[test]
    fn shared_bootstrap_owner_warning_cases() {
        let shared = "system:serviceaccount:hivekeep-system:hivekeep-client";
        for (name, owners, warn) in [
            ("default owner", shared.to_string(), true),
            ("listed among others", format!("alice, {shared} ,bob"), true),
            ("own principals only", "alice,bob".to_string(), false),
            (
                "another namespace's client",
                "system:serviceaccount:other:hivekeep-client".to_string(),
                false,
            ),
            ("prefix is not a match", format!("{shared}-x"), false),
        ] {
            let got = shared_bootstrap_owner_warning(&owners, NS);
            assert_eq!(got.is_some(), warn, "{name}");
            if let Some(msg) = got {
                for want in [
                    shared,
                    "--api-authz-bootstrap-owners",
                    "HK_API_AUTHZ_BOOTSTRAP_OWNERS",
                ] {
                    assert!(msg.contains(want), "{name}: {msg} lacks {want}");
                }
            }
        }
    }

    #[tokio::test]
    async fn create_api_server_env_vars_snapshot_location() {
        for (name, location, gates, want, want_err) in [
            ("set", "gs://hivekeep-snapshots/prefix/", vec![], Some("gs://hivekeep-snapshots/prefix/"), None),
            ("set with the gate on", "gs://hivekeep-snapshots/prefix/", vec!["PerTemplateSnapshotLocation"], Some("gs://hivekeep-snapshots/prefix/"), None),
            ("unset when the gate is on", "", vec!["PerTemplateSnapshotLocation"], None, None),
            ("unset when every gate is on", "", vec!["ALL"], None, None),
            (
                "an error when unset and the gate is off",
                "",
                vec!["ExternalVolumes"],
                None,
                Some("--snapshot-location (SNAPSHOT_LOCATION) must be set unless --preview=PerTemplateSnapshotLocation is enabled"),
            ),
            ("an error when invalid", "gs:///no-bucket", vec![], None, Some("invalid --snapshot-location")),
        ] {
            let cfg = Config {
                postgres_read_write_connection_string: "postgres://runtime@postgres/db".into(),
                snapshot_location: location.into(),
                preview_gates: gates.iter().map(|g| g.to_string()).collect(),
                ..base_cfg()
            };
            let (e, f) = fake_env(cfg, seeded(&[]));
            let res = e.create_api_server_env_vars().await;
            if let Some(want_err) = want_err {
                let err = res.unwrap_err();
                assert!(err.to_string().contains(want_err), "{name}: {err}");
                continue;
            }
            res.unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(cm(&f, ENV_SNAPSHOT_LOCATION).as_deref(), want, "{name}");
        }
    }

    #[tokio::test]
    async fn create_api_server_env_vars_transfer_service_account() {
        for (name, cfg, want, want_err) in [
            ("GKE", base_cfg(), Some(TEST_TRANSFER_ACCOUNT), false),
            (
                "kind",
                Config {
                    kind: true,
                    snapshot_location: "gs://hivekeep-snapshots/".into(),
                    ..Default::default()
                },
                Some(""),
                false,
            ),
            (
                "GKE without an account",
                Config {
                    snapshot_location: "gs://hivekeep-snapshots/".into(),
                    ..Default::default()
                },
                None,
                true,
            ),
        ] {
            let cfg = Config {
                postgres_read_write_connection_string: "postgres://runtime@postgres/db".into(),
                ..cfg
            };
            let (e, f) = fake_env(cfg, seeded(&[]));
            let res = e.create_api_server_env_vars().await;
            if want_err {
                let err = res.unwrap_err();
                assert!(
                    err.to_string().contains("SNAPSHOT_TRANSFER_GSA_NAME"),
                    "{name}: {err}"
                );
                assert!(
                    config_map_data(&f, NS, CONFIG_MAP_API_ENV_VARS).is_empty(),
                    "{name}: ConfigMap written"
                );
                continue;
            }
            res.unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(
                cm(&f, ENV_TRANSFER_SERVICE_ACCOUNT).as_deref(),
                want,
                "{name}"
            );
        }
    }
}
