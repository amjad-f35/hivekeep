//! Generating the Secrets and ConfigMaps the control plane reads: the CA
//! and JWT signing pools, the hivekeep-api config file and its
//! authentication config.

use super::env::NAMESPACE_POD_CERT;
use super::{Env, Result, StepError};
use crate::kube::Gvk;
use crate::log;
use base64::Engine;
use hivekeep_identity::ca::{Ca, CaPool};
use hivekeep_identity::jwt::authority::{JwtAlgorithm, JwtPool, JwtSigner};
use hivekeep_identity::jwt::jwks::key_set_json;
use hivekeep_identity::key::KeyAlgorithm;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use std::collections::BTreeMap;
use std::time::Duration;

/// Secret and ConfigMap names the control plane reads.
pub const SECRET_ACTOR_ID_JWT_POOL: &str = "actor-id-jwt-pool";
pub const SECRET_ACTOR_ID_CA_POOL: &str = "actor-id-ca-pool";
pub const SECRET_ACTOR_ID_CA_CERTS: &str = "actor-id-ca-certs";
pub const SECRET_SERVICE_DNS_CA: &str = "service-dns-ca-pool";
pub const SECRET_POD_IDENTITY_CA: &str = "pod-identity-ca-pool";
pub const SECRET_POSTGRES_CA: &str = "postgres-ca-pool";
pub const SECRET_EGRESS_MITM_CA_POOL: &str = "egress-mitm-ca-pool";
pub const SECRET_API_ENV_VARS: &str = "hivekeep-api-secret-envvars";
pub const SECRET_POSTGRES_SERVER_CA: &str = "postgres-server-ca";
pub const CONFIG_MAP_API_ENV_VARS: &str = "hivekeep-api-envvars";
pub const CONFIG_MAP_API_AUTHN: &str = "hivekeep-api-authentication";
pub const CONFIG_MAP_API_CONFIG: &str = "hivekeep-api-config";

/// Identifier of the first CA in a new pool.
pub(crate) const POOL_KEY_ID: &str = "1";

/// How long a generated root is valid.
pub(crate) const CA_VALIDITY: Duration = Duration::from_secs(365 * 24 * 3600);

/// Log level a new install's hivekeep-api gets when none is asked for.
const DEFAULT_API_LOG_LEVEL: &str = "info";

/// Default issuer of a cluster that publishes no discoverable one.
pub(crate) const IN_CLUSTER_ISSUER: &str = "https://kubernetes.default.svc";

const TLS_CERT_KEY: &str = "tls.crt";
const TLS_PRIVATE_KEY_KEY: &str = "tls.key";

/// Secret payloads of a freshly generated CA pool: the serialised pool, with
/// its single CA active for signing, and the root and its key under the
/// standard TLS keys. The TLS keys are not redundant: consumers that speak
/// TLS rather than the pool format can mount them directly.
pub(crate) fn new_ca_pool_secret_data(
    id: &str,
    algorithm: KeyAlgorithm,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let ca = Ca::generate(id, algorithm, CA_VALIDITY)
        .map_err(|e| StepError::msg(format!("while generating CA {id:?}: {e}")))?;
    let cert = ca.root_pem();
    let key = ca.key_pem();
    let pool = CaPool::new(vec![ca], id)
        .to_json()
        .map_err(|e| StepError::msg(format!("while marshaling the pool for CA {id:?}: {e}")))?;
    Ok(BTreeMap::from([
        ("pool".to_string(), pool),
        (TLS_CERT_KEY.to_string(), cert.into_bytes()),
        (TLS_PRIVATE_KEY_KEY.to_string(), key.into_bytes()),
    ]))
}

fn jwt_algorithm(name: &str) -> Result<JwtAlgorithm> {
    match name {
        "ES256" => Ok(JwtAlgorithm::Es256),
        "RS256" => Ok(JwtAlgorithm::Rs256),
        other => Err(StepError::msg(format!(
            "unsupported actor JWT algorithm {other:?}: want ES256 or RS256"
        ))),
    }
}

/// The public keys of a serialised JWT pool, as a JWKS.
pub(crate) fn public_jwks(pool: &[u8]) -> Result<Vec<u8>> {
    let pool = JwtPool::from_json(pool)
        .map_err(|e| StepError::msg(format!("while parsing the JWT pool: {e}")))?;
    let keys = pool
        .verification_keys()
        .map_err(|e| StepError::msg(e.to_string()))?;
    key_set_json(&keys).map_err(|e| StepError::msg(e.to_string()))
}

/// Secret payloads of a new JWT pool with one active authority keyed by its
/// thumbprint, plus `jwks`, its public keys, for verifiers that must not see
/// the pool.
pub(crate) fn new_jwt_pool_secret_data(algorithm: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    let pool = JwtPool::generate(jwt_algorithm(algorithm)?, None)
        .map_err(|e| StepError::msg(format!("while generating the JWT pool: {e}")))?;
    let bytes = pool
        .to_json()
        .map_err(|e| StepError::msg(format!("while generating the JWT pool: {e}")))?;
    let jwks = public_jwks(&bytes)
        .map_err(|e| e.context("while publishing the JWT pool's public keys"))?;
    Ok(BTreeMap::from([
        ("pool".to_string(), bytes),
        ("jwks".to_string(), jwks),
    ]))
}

/// hivekeep-api's config file.
pub(crate) fn build_api_config(log_level: &str) -> String {
    let escaped = log_level.replace('\\', "\\\\").replace('"', "\\\"");
    format!("logging: {{\n  level: \"{escaped}\"\n}}\n")
}

/// hivekeep-api's authentication config. An in-cluster issuer is not
/// reachable over public discovery, so the server is pointed at its own
/// projected service account CA and token for the OIDC handshake. No
/// trailing newline.
pub(crate) fn build_authentication_config(issuer: &str) -> String {
    let mut config = format!(
        "jwtProviders:\n- name: kubernetes\n  issuer: {issuer}\n  audiences: [hivekeep-api.hivekeep-system.svc]\n"
    );
    if issuer == IN_CLUSTER_ISSUER || issuer == format!("{IN_CLUSTER_ISSUER}.cluster.local") {
        config.push_str(
            "  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n  \
             discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token\n",
        );
    }
    config.trim_end_matches('\n').to_string()
}

impl Env {
    /// Generates the actor-identity JWT signing pool. An existing pool is
    /// kept and its `jwks` brought in step with it.
    pub async fn create_jwt_authority_pool_secret(&self) -> Result<()> {
        log::step("create_jwt_authority_pool_secret");
        self.create_jwt_pool(self.namespace(), SECRET_ACTOR_ID_JWT_POOL)
            .await
    }

    /// Generates the actor-identity CA pool. External relying parties trust
    /// this root, so it is ECDSA P-256, more broadly accepted than Ed25519.
    pub async fn create_actor_id_ca_pool_secret(&self) -> Result<()> {
        log::step("create_actor_id_ca_pool_secret");
        self.create_ca_pool(
            self.namespace(),
            SECRET_ACTOR_ID_CA_POOL,
            KeyAlgorithm::EcdsaP256,
        )
        .await
    }

    /// Generates the egress MITM CA pool.
    pub async fn create_egress_mitm_ca_pool_secret(&self) -> Result<()> {
        log::step("create_egress_mitm_ca_pool_secret");
        self.create_ca_pool(
            self.namespace(),
            SECRET_EGRESS_MITM_CA_POOL,
            KeyAlgorithm::EcdsaP256,
        )
        .await
    }

    /// Creates the egress MITM CA pool unless it exists. The egress gateway
    /// mounts it.
    pub async fn ensure_egress_mitm_ca_pool_secret(&self) -> Result<()> {
        if self
            .kube()?
            .secret_exists(self.namespace(), SECRET_EGRESS_MITM_CA_POOL)
            .await?
        {
            return Ok(());
        }
        self.create_egress_mitm_ca_pool_secret().await
    }

    /// Generates the signer pools the podcertificate controller issues from.
    pub async fn create_pod_certificate_controller_cas(&self) -> Result<()> {
        log::step("create_podcertificate_controller_cas");
        self.kube()?.ensure_namespace(NAMESPACE_POD_CERT).await?;
        for name in [
            SECRET_SERVICE_DNS_CA,
            SECRET_POD_IDENTITY_CA,
            SECRET_POSTGRES_CA,
        ] {
            self.create_ca_pool(NAMESPACE_POD_CERT, name, KeyAlgorithm::Ed25519)
                .await?;
        }
        Ok(())
    }

    /// Publishes the actor-identity root alone. The egress gateway verifies
    /// actor client certificates against it, but the pool Secret also holds
    /// the signing key.
    pub async fn create_actor_id_ca_certs_secret(&self) -> Result<()> {
        log::step("create_actor_id_ca_certs_secret");
        let k = self.kube()?;
        let root = k
            .ca_pool_root_pem(self.namespace(), SECRET_ACTOR_ID_CA_POOL)
            .await
            .map_err(|e| {
                StepError::from(e).context(format!("while building {SECRET_ACTOR_ID_CA_CERTS}"))
            })?;
        let data = BTreeMap::from([(
            "ca.crt".to_string(),
            String::from_utf8_lossy(&root).into_owned(),
        )]);
        Ok(
            k.apply_secret(self.namespace(), SECRET_ACTOR_ID_CA_CERTS, &data)
                .await?,
        )
    }

    /// Writes hivekeep-api's config file.
    pub async fn create_api_config(&self) -> Result<()> {
        log::step("create_api_config");
        let k = self.kube()?;
        k.ensure_namespace(self.namespace()).await?;
        let level = if self.cfg.api_log_level.is_empty() {
            DEFAULT_API_LOG_LEVEL
        } else {
            &self.cfg.api_log_level
        };
        let api_config = build_api_config(level);
        log::info(&format!("{CONFIG_MAP_API_CONFIG} apiconfig.textproto:"));
        for line in api_config.trim_end_matches('\n').split('\n') {
            log::info(&format!("  | {line}"));
        }
        let data = BTreeMap::from([("apiconfig.textproto".to_string(), api_config)]);
        Ok(
            k.apply_config_map(self.namespace(), CONFIG_MAP_API_CONFIG, &data)
                .await?,
        )
    }

    /// Writes the default authentication config, trusting the cluster's
    /// service account issuer.
    pub async fn create_api_authentication_config(&self) -> Result<()> {
        log::step("create_api_authentication_config");
        let k = self.kube()?;
        k.ensure_namespace(self.namespace()).await?;
        let authn = build_authentication_config(&self.jwt_issuer().await);
        // A wrong issuer fails as an opaque 401 much later, so show it.
        log::info(&format!("{CONFIG_MAP_API_AUTHN} authentication.yaml:"));
        for line in authn.split('\n') {
            log::info(&format!("  | {line}"));
        }
        let data = BTreeMap::from([("authentication.yaml".to_string(), authn)]);
        Ok(
            k.apply_config_map(self.namespace(), CONFIG_MAP_API_AUTHN, &data)
                .await?,
        )
    }

    /// The service account token issuer hivekeep-api trusts: the explicit
    /// setting, else the GKE form of the cluster coordinates, else what the
    /// cluster's OpenID discovery reports, else the in-cluster default.
    pub(crate) async fn jwt_issuer(&self) -> String {
        let c = &self.cfg;
        if !c.expected_jwt_issuer.is_empty() {
            return c.expected_jwt_issuer.clone();
        }
        if !c.project_id.is_empty() && !c.cluster_location.is_empty() && !c.cluster_name.is_empty()
        {
            return format!(
                "https://container.googleapis.com/v1/projects/{}/locations/{}/clusters/{}",
                c.project_id, c.cluster_location, c.cluster_name
            );
        }
        if let Ok(k) = self.kube() {
            let issuer = k.oidc_issuer().await;
            if !issuer.is_empty() {
                return issuer;
            }
        }
        IN_CLUSTER_ISSUER.to_string()
    }

    /// Generates a CA pool into a Secret, leaving an existing one alone:
    /// regenerating would rotate the root out from under every certificate
    /// already issued from it.
    async fn create_ca_pool(
        &self,
        namespace: &str,
        name: &str,
        algorithm: KeyAlgorithm,
    ) -> Result<()> {
        if self.kube()?.secret_exists(namespace, name).await? {
            log::info(&format!(
                "  CA pool {namespace}/{name} already exists; keeping it"
            ));
            return Ok(());
        }
        let data = new_ca_pool_secret_data(POOL_KEY_ID, algorithm).map_err(|e| {
            e.context(format!(
                "while generating the CA pool for {namespace}/{name}"
            ))
        })?;
        self.create_pool_secret(namespace, name, "kubernetes.io/tls", data)
            .await
    }

    /// Generates a JWT pool into a Secret. An existing pool is preserved:
    /// rotating the key would invalidate every token minted from it.
    pub(crate) async fn create_jwt_pool(&self, namespace: &str, name: &str) -> Result<()> {
        if let Some(existing) = self.kube()?.get_secret(namespace, name).await? {
            log::info(&format!(
                "  JWT authority pool {namespace}/{name} already exists; keeping it"
            ));
            return self.publish_jwt_pool_keys(namespace, name, &existing).await;
        }
        let data = new_jwt_pool_secret_data(&self.cfg.actor_jwt_algorithm).map_err(|e| {
            e.context(format!(
                "while building the JWT pool for {namespace}/{name}"
            ))
        })?;
        self.create_pool_secret(namespace, name, "Opaque", data)
            .await
    }

    /// Recomputes a pool Secret's `jwks` from its pool: missing on a Secret
    /// from before the key existed, or stale after another tool rewrote the
    /// pool (which would keep a removed key trusted). A matching Secret is
    /// left alone.
    async fn publish_jwt_pool_keys(
        &self,
        namespace: &str,
        name: &str,
        secret: &Secret,
    ) -> Result<()> {
        let data = secret.data.clone().unwrap_or_default();
        let pool = data.get("pool").map(|b| b.0.clone()).unwrap_or_default();
        let jwks = public_jwks(&pool).map_err(|e| {
            e.context(format!(
                "while publishing the public keys of {namespace}/{name}"
            ))
        })?;
        if data.get("jwks").map(|b| b.0.as_slice()) == Some(jwks.as_slice()) {
            return Ok(());
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(&jwks);
        let patch = crate::kube::Patch::Merge(serde_json::json!({"data": {"jwks": encoded}}));
        self.kube()?
            .patch(&Gvk::of::<Secret>(), namespace, name, &patch)
            .await
            .map_err(|e| {
                StepError::from(e).context(format!("while writing jwks to {namespace}/{name}"))
            })?;
        log::info(&format!(
            "  published the public keys of {namespace}/{name} as jwks"
        ));
        Ok(())
    }

    /// Creates (never applies) a pool Secret: it holds generated key
    /// material, and a concurrent run must not overwrite a pool another run
    /// just generated.
    async fn create_pool_secret(
        &self,
        namespace: &str,
        name: &str,
        secret_type: &str,
        data: BTreeMap<String, Vec<u8>>,
    ) -> Result<()> {
        let secret = Secret {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(name.into()),
                namespace: Some(namespace.into()),
                ..Default::default()
            },
            type_: Some(secret_type.into()),
            data: Some(data.into_iter().map(|(k, v)| (k, ByteString(v))).collect()),
            ..Default::default()
        };
        let mut obj = serde_json::to_value(&secret).map_err(|e| StepError::msg(e.to_string()))?;
        obj["apiVersion"] = "v1".into();
        obj["kind"] = "Secret".into();
        self.kube()?.create(&obj).await.map_err(|e| {
            StepError::from(e).context(format!("while creating pool secret {namespace}/{name}"))
        })?;
        log::info(&format!("  created pool secret {namespace}/{name}"));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::steps::cluster_fixture::{fake_env, secret_object};
    use hivekeep_identity::key::{KeyKind, PrivateKey, PublicKey};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use x509_parser::prelude::{FromDer, X509Certificate};

    #[test]
    fn build_api_server_env_vars_keys() {
        let got = super::super::apienv::build_api_server_env_vars(
            "postgresql://rw@postgres/db",
            "postgresql://o@postgres/db",
            "public",
        );
        let keys: Vec<&str> = got.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "HK_API_POSTGRES_OWNER_CONNECTION_STRING",
                "HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
                "HK_API_POSTGRES_SCHEMA"
            ]
        );
        assert_eq!(
            got["HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING"],
            "postgresql://rw@postgres/db"
        );
        assert_eq!(
            got["HK_API_POSTGRES_OWNER_CONNECTION_STRING"],
            "postgresql://o@postgres/db"
        );
        assert_eq!(got["HK_API_POSTGRES_SCHEMA"], "public");
    }

    #[test]
    fn build_authentication_config_cases() {
        let external = "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c";
        assert_eq!(
            build_authentication_config(external),
            format!("jwtProviders:\n- name: kubernetes\n  issuer: {external}\n  audiences: [hivekeep-api.hivekeep-system.svc]")
        );
        for issuer in [
            IN_CLUSTER_ISSUER.to_string(),
            format!("{IN_CLUSTER_ISSUER}.cluster.local"),
        ] {
            assert_eq!(
                build_authentication_config(&issuer),
                format!(
                    "jwtProviders:\n- name: kubernetes\n  issuer: {issuer}\n  audiences: [hivekeep-api.hivekeep-system.svc]\n  \
                     certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n  \
                     discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token"
                )
            );
        }
    }

    fn root_of(pool: &[u8]) -> (String, Vec<u8>, String) {
        let pool = CaPool::from_json(pool).unwrap();
        assert_eq!(pool.cas().len(), 1);
        let ca = &pool.cas()[0];
        (
            ca.id().to_string(),
            ca.root_der().to_vec(),
            pool.active().unwrap().id().to_string(),
        )
    }

    fn key_kind(der: &[u8]) -> KeyKind {
        let (_, cert) = X509Certificate::from_der(der).unwrap();
        PublicKey::from_spki_der(cert.public_key().raw.to_vec())
            .unwrap()
            .kind()
    }

    #[test]
    fn new_ca_pool_secret_data_shape() {
        for (name, alg, want) in [
            ("pod certificates", KeyAlgorithm::Ed25519, KeyKind::Ed25519),
            (
                "actor identity",
                KeyAlgorithm::EcdsaP256,
                KeyKind::EcdsaP256,
            ),
            ("egress mitm", KeyAlgorithm::EcdsaP256, KeyKind::EcdsaP256),
        ] {
            let data = new_ca_pool_secret_data(POOL_KEY_ID, alg).unwrap();
            let keys: Vec<&str> = data.keys().map(String::as_str).collect();
            assert_eq!(keys, ["pool", "tls.crt", "tls.key"], "{name}");
            let (id, root, active) = root_of(&data["pool"]);
            assert_eq!(id, POOL_KEY_ID, "{name}");
            assert_eq!(active, POOL_KEY_ID, "{name}");
            assert_eq!(key_kind(&root), want, "{name}");
            let (_, cert) = X509Certificate::from_der(&root).unwrap();
            assert!(cert.is_ca(), "{name}: root is not a CA");
            let validity =
                cert.validity().not_after.timestamp() - cert.validity().not_before.timestamp();
            assert_eq!(validity as u64, CA_VALIDITY.as_secs(), "{name}");
            // The TLS pair must be usable together.
            let key = PrivateKey::from_pem(&data["tls.key"]).unwrap();
            assert_eq!(
                key.public_key().spki_der(),
                cert.public_key().raw,
                "{name}: key does not match cert"
            );
            assert!(String::from_utf8_lossy(&data["tls.crt"]).contains("BEGIN CERTIFICATE"));
        }
    }

    #[tokio::test]
    async fn create_actor_id_ca_pool_secret_is_ecdsa() {
        let (e, _f) = fake_env(Config::default(), vec![]);
        e.create_actor_id_ca_pool_secret().await.unwrap();
        let s = e
            .kube()
            .unwrap()
            .get_secret(e.namespace(), SECRET_ACTOR_ID_CA_POOL)
            .await
            .unwrap()
            .unwrap();
        let (_, root, _) = root_of(&s.data.unwrap()["pool"].0);
        assert_eq!(key_kind(&root), KeyKind::EcdsaP256);
    }

    #[test]
    fn new_jwt_pool_secret_data_shape() {
        for alg in ["ES256", "RS256"] {
            let data = new_jwt_pool_secret_data(alg).unwrap();
            let keys: Vec<&str> = data.keys().map(String::as_str).collect();
            assert_eq!(keys, ["jwks", "pool"]);
            assert_eq!(public_jwks(&data["pool"]).unwrap(), data["jwks"]);
            let pool = JwtPool::from_json(&data["pool"]).unwrap();
            assert_eq!(pool.authorities().len(), 1);
            let a = &pool.authorities()[0];
            assert_eq!(a.algorithm().as_str(), alg);
            let thumbprint = a.verification_key().public_key.thumbprint().unwrap();
            assert_eq!(a.id(), thumbprint, "key ID is the thumbprint");
            assert_eq!(pool.active_id(), thumbprint, "the authority is active");
        }
        assert!(new_jwt_pool_secret_data("HS256").is_err());
    }

    #[tokio::test]
    async fn ensure_pod_certificate_cas_keeps_existing() {
        let ns = serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name": NAMESPACE_POD_CERT}});
        let (e, f) = fake_env(Config::default(), vec![ns]);
        e.create_pod_certificate_controller_cas().await.unwrap();
        let k = e.kube().unwrap();
        let before = k
            .get_secret(NAMESPACE_POD_CERT, SECRET_SERVICE_DNS_CA)
            .await
            .unwrap()
            .unwrap();
        k.delete_named(
            &Gvk::of::<Secret>(),
            NAMESPACE_POD_CERT,
            SECRET_POSTGRES_CA,
            None,
        )
        .await
        .unwrap();
        e.ensure_pod_certificate_cas().await.unwrap();
        for name in [
            SECRET_SERVICE_DNS_CA,
            SECRET_POD_IDENTITY_CA,
            SECRET_POSTGRES_CA,
        ] {
            let s = k
                .get_secret(NAMESPACE_POD_CERT, name)
                .await
                .unwrap()
                .unwrap();
            let pool = &s.data.as_ref().unwrap()["pool"].0;
            if name == SECRET_SERVICE_DNS_CA {
                assert_eq!(
                    pool,
                    &before.data.as_ref().unwrap()["pool"].0,
                    "existing service DNS CA rotated"
                );
            }
            assert!(!CaPool::from_json(pool).unwrap().cas().is_empty());
        }
        drop(f);
    }

    #[tokio::test]
    async fn create_jwt_pool_publishes_keys_of_existing_pool() {
        let pool = JwtPool::generate(JwtAlgorithm::Es256, None)
            .unwrap()
            .to_json()
            .unwrap();
        let want = public_jwks(&pool).unwrap();
        for (name, data, want_updates) in [
            ("pool from before jwks", vec![("pool", pool.clone())], 1),
            (
                "pool with a stale jwks",
                vec![("pool", pool.clone()), ("jwks", br#"{"keys":[]}"#.to_vec())],
                1,
            ),
            (
                "pool with jwks",
                vec![("pool", pool.clone()), ("jwks", want.clone())],
                0,
            ),
        ] {
            let secret = secret_object("hivekeep-system", SECRET_ACTOR_ID_JWT_POOL, &data);
            let (e, f) = fake_env(Config::default(), vec![secret]);
            let updates = Arc::new(AtomicUsize::new(0));
            let u = updates.clone();
            f.prepend_reactor("patch", "Secret", move |_| {
                u.fetch_add(1, Ordering::SeqCst);
                None
            });
            e.create_jwt_pool("hivekeep-system", SECRET_ACTOR_ID_JWT_POOL)
                .await
                .unwrap();
            let got = e
                .kube()
                .unwrap()
                .get_secret("hivekeep-system", SECRET_ACTOR_ID_JWT_POOL)
                .await
                .unwrap()
                .unwrap();
            let got = got.data.unwrap();
            assert_eq!(got["pool"].0, pool, "{name}");
            assert_eq!(got["jwks"].0, want, "{name}");
            assert_eq!(updates.load(Ordering::SeqCst), want_updates, "{name}");
        }
    }
}
