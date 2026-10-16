//! Authentication of API clients.
//!
//! A client that presented a certificate (verified by the TLS layer) is the
//! identity in its first URI SAN. Anything else must send
//! `authorization: Bearer <JWT>` from one of the configured issuers. The
//! [`Authenticator`] runs as a tonic interceptor and stores the
//! [`Principal`] and peer certificates in the request extensions.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use hivekeep_identity::jwt::jwks::{parse_key_set, KeyWithId};
use hivekeep_identity::jwt::verifier::{KeySource, Verifier};
use hivekeep_identity::IdentityError;
use parking_lot::RwLock;
use serde::Deserialize;
use tonic::{Request, Status};

use crate::context::{PeerCertificates, Principal};
use crate::peer_cert;

/// The authentication config file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthenticationConfig {
    /// Trusted token issuers.
    #[serde(default)]
    pub jwt_providers: Vec<JwtProviderConfig>,
}

/// One trusted OIDC issuer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JwtProviderConfig {
    /// A name for logs.
    pub name: String,
    /// The `iss` the provider's tokens carry; its discovery document lives
    /// under it.
    pub issuer: String,
    /// Accepted audiences; a token must name one.
    #[serde(default)]
    pub audiences: Vec<String>,
    /// PEM CA bundle for reaching the issuer, if not publicly trusted.
    #[serde(default)]
    pub certificate_authority_file: String,
    /// A bearer token file sent with discovery requests, if required.
    #[serde(default)]
    pub discovery_token_file: String,
}

/// Why the config is unusable.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AuthnConfigError(pub String);

/// Reads, parses (YAML or JSON, unknown fields refused) and validates the
/// config at `path`.
pub fn load_config(path: &str) -> Result<AuthenticationConfig, AuthnConfigError> {
    let bytes = std::fs::read(path)
        .map_err(|e| AuthnConfigError(format!("read authentication config: {e}")))?;
    let cfg: AuthenticationConfig = serde_yaml::from_slice(&bytes)
        .map_err(|e| AuthnConfigError(format!("parse authentication config: {e}")))?;
    validate_config(&cfg)?;
    Ok(cfg)
}

/// Checks a config without I/O.
pub fn validate_config(cfg: &AuthenticationConfig) -> Result<(), AuthnConfigError> {
    if cfg.jwt_providers.is_empty() {
        return Err(AuthnConfigError(
            "at least one JWT provider is required".into(),
        ));
    }
    let mut names = std::collections::HashSet::new();
    let mut issuers = std::collections::HashSet::new();
    for (i, p) in cfg.jwt_providers.iter().enumerate() {
        let field = format!("jwtProviders[{i}]");
        if p.name.is_empty() {
            return Err(AuthnConfigError(format!("{field}.name is required")));
        }
        if !names.insert(p.name.clone()) {
            return Err(AuthnConfigError(format!(
                "duplicate JWT provider name {:?}",
                p.name
            )));
        }
        let ok = url::Url::parse(&p.issuer)
            .map(|u| {
                u.scheme() == "https"
                    && u.host_str().is_some_and(|h| !h.is_empty())
                    && u.query().is_none()
                    && u.fragment().is_none()
            })
            .unwrap_or(false);
        if !ok {
            return Err(AuthnConfigError(format!(
                "{field}.issuer must be an HTTPS URL without query or fragment"
            )));
        }
        if !issuers.insert(p.issuer.clone()) {
            return Err(AuthnConfigError(format!(
                "duplicate JWT provider issuer {:?}",
                p.issuer
            )));
        }
        if p.audiences.is_empty() {
            return Err(AuthnConfigError(format!(
                "{field}.audiences must contain at least one audience"
            )));
        }
        if p.audiences.iter().any(String::is_empty) {
            return Err(AuthnConfigError(format!(
                "{field}.audiences must not contain an empty audience"
            )));
        }
    }
    Ok(())
}

/// Verifies bearer tokens of one issuer.
#[derive(Clone)]
pub struct JwtProvider {
    /// Name for logs.
    pub name: String,
    /// The issuer.
    pub issuer: String,
    /// The verifier.
    pub verifier: Arc<Verifier>,
}

/// The authentication interceptor.
#[derive(Clone, Default)]
pub struct Authenticator {
    providers: Arc<Vec<JwtProvider>>,
}

/// Rejects a server configuration with no providers or duplicate issuers.
pub fn validate_providers(providers: &[JwtProvider]) -> Result<(), AuthnConfigError> {
    if providers.is_empty() {
        return Err(AuthnConfigError(
            "at least one JWT provider is required".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (i, p) in providers.iter().enumerate() {
        if p.name.is_empty() || p.issuer.is_empty() {
            return Err(AuthnConfigError(format!(
                "JWT provider {i} requires a name, issuer, and verifier"
            )));
        }
        if !seen.insert(&p.issuer) {
            return Err(AuthnConfigError(format!(
                "duplicate JWT provider issuer {:?}",
                p.issuer
            )));
        }
    }
    Ok(())
}

/// The issuer of a JWT, unverified, for picking the provider.
pub fn unverified_issuer(token: &str) -> Result<String, String> {
    let mut parts = token.split('.');
    let (Some(_), Some(payload), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("malformed JWT".into());
    };
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| e.to_string())?;
    #[derive(Deserialize)]
    struct Iss {
        #[serde(default)]
        iss: String,
    }
    let iss: Iss = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
    if iss.iss.is_empty() {
        return Err("token issuer is required".into());
    }
    Ok(iss.iss)
}

/// The bearer token in `authorization`, if any.
pub fn bearer_token<T>(req: &Request<T>) -> Option<String> {
    let v = req.metadata().get("authorization")?.to_str().ok()?;
    let tok = v.strip_prefix("Bearer ")?.trim();
    (!tok.is_empty()).then(|| tok.to_string())
}

impl Authenticator {
    /// An authenticator over `providers`.
    pub fn new(providers: Vec<JwtProvider>) -> Self {
        Authenticator {
            providers: Arc::new(providers),
        }
    }

    /// Authenticates `req`, recording what it found in its extensions.
    pub fn authenticate<T>(&self, req: &mut Request<T>) -> Result<(), Status> {
        if req.extensions().get::<PeerCertificates>().is_none() {
            if let Some(certs) = req.peer_certs() {
                let ders: Vec<Vec<u8>> = certs.iter().map(|c| c.as_ref().to_vec()).collect();
                req.extensions_mut()
                    .insert(PeerCertificates(Arc::new(ders)));
            }
        }
        let id = req
            .extensions()
            .get::<PeerCertificates>()
            .and_then(|c| c.leaf())
            .and_then(peer_cert::first_uri_san);
        if let Some(id) = id {
            req.extensions_mut().insert(Principal::mtls(id));
            return Ok(());
        }
        let principal = self.verify_bearer(req)?;
        req.extensions_mut().insert(principal);
        Ok(())
    }

    fn verify_bearer<T>(&self, req: &Request<T>) -> Result<Principal, Status> {
        let token =
            bearer_token(req).ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let issuer = unverified_issuer(&token)
            .map_err(|_| Status::unauthenticated("invalid bearer token"))?;
        let Some(p) = self.providers.iter().find(|p| p.issuer == issuer) else {
            return Err(Status::unauthenticated(format!(
                "token issuer {issuer:?} not trusted"
            )));
        };
        match p.verifier.verify(&token, SystemTime::now()) {
            Ok(t) => Ok(Principal::jwt(t.subject, p.issuer.clone())),
            Err(e) => {
                tracing::debug!(provider = %p.name, error = %e, "JWT verification failed");
                Err(Status::unauthenticated("invalid bearer token"))
            }
        }
    }
}

impl tonic::service::Interceptor for Authenticator {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        self.authenticate(&mut req)?;
        Ok(req)
    }
}

/// An issuer's keys fetched over HTTPS through OIDC discovery and refreshed
/// in the background; [`KeySource::keys`] serves the last fetched set.
pub struct HttpJwksSource {
    issuer: String,
    keys: RwLock<Option<Vec<KeyWithId>>>,
    client: reqwest::Client,
    token_file: Option<PathBuf>,
}

/// How often the background refresh runs.
pub const JWKS_REFRESH_PERIOD: Duration = Duration::from_secs(60);

impl HttpJwksSource {
    /// A source for `issuer`, trusting `ca_file` if set and sending the token
    /// in `token_file` if set.
    pub fn new(
        issuer: &str,
        ca_file: &str,
        token_file: &str,
    ) -> Result<Arc<Self>, AuthnConfigError> {
        let mut b = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if !ca_file.is_empty() {
            let pem = std::fs::read(ca_file)
                .map_err(|e| AuthnConfigError(format!("reading {ca_file}: {e}")))?;
            for c in reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| AuthnConfigError(e.to_string()))?
            {
                b = b.add_root_certificate(c);
            }
        }
        let client = b.build().map_err(|e| AuthnConfigError(e.to_string()))?;
        Ok(Arc::new(HttpJwksSource {
            issuer: issuer.trim_end_matches('/').to_string(),
            keys: RwLock::new(None),
            client,
            token_file: (!token_file.is_empty()).then(|| PathBuf::from(token_file)),
        }))
    }

    async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        let mut req = self.client.get(url);
        if let Some(p) = &self.token_file {
            let t =
                std::fs::read_to_string(p).map_err(|e| format!("reading {}: {e}", p.display()))?;
            req = req.bearer_auth(t.trim());
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("GET {url}: {}", resp.status()));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| e.to_string())
    }

    /// Fetches the key set now.
    pub async fn refresh(&self) -> Result<(), String> {
        #[derive(Deserialize)]
        struct Discovery {
            jwks_uri: String,
        }
        let doc = self
            .get(&format!("{}/.well-known/openid-configuration", self.issuer))
            .await?;
        let d: Discovery = serde_json::from_slice(&doc).map_err(|e| e.to_string())?;
        let jwks = self.get(&d.jwks_uri).await?;
        let keys = parse_key_set(&jwks, &d.jwks_uri).map_err(|e| e.to_string())?;
        *self.keys.write() = Some(keys);
        Ok(())
    }

    /// Refreshes every [`JWKS_REFRESH_PERIOD`] until `stop`.
    pub fn spawn_refresh(self: &Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = me.refresh().await {
                    tracing::warn!(issuer = %me.issuer, error = %e, "refreshing JWT keys failed");
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(JWKS_REFRESH_PERIOD) => {}
                }
            }
        });
    }
}

impl KeySource for HttpJwksSource {
    fn keys(&self) -> hivekeep_identity::Result<Vec<KeyWithId>> {
        self.keys
            .read()
            .clone()
            .ok_or_else(|| IdentityError::Keys(format!("keys of {} not fetched yet", self.issuer)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::request_as;
    use hivekeep_identity::jwt::authority::{JwtAlgorithm, JwtPool, JwtSigner};
    use hivekeep_identity::jwt::claims::Claims;
    use hivekeep_identity::jwt::verifier::SignerKeySource;

    fn provider(issuer: &str, signer: Arc<dyn JwtSigner>) -> JwtProvider {
        JwtProvider {
            name: "p".into(),
            issuer: issuer.into(),
            verifier: Arc::new(Verifier::new(
                issuer,
                vec!["hivekeep-api".into()],
                Arc::new(SignerKeySource(signer)),
            )),
        }
    }

    fn token(signer: &dyn JwtSigner, issuer: &str, aud: &str) -> String {
        let c = Claims::for_actor(
            issuer,
            "h",
            "a",
            "u",
            vec![aud.into()],
            Duration::from_secs(600),
            SystemTime::now(),
        )
        .unwrap();
        signer.sign_jwt(&c).unwrap()
    }

    #[test]
    fn bearer_tokens() {
        let signer: Arc<dyn JwtSigner> =
            Arc::new(JwtPool::generate(JwtAlgorithm::Es256, None).unwrap());
        let a = Authenticator::new(vec![provider("https://idp.example", signer.clone())]);

        let mut req = request_as((), None, None);
        assert_eq!(
            a.authenticate(&mut req).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        let mut req = Request::new(());
        let t = token(signer.as_ref(), "https://idp.example", "hivekeep-api");
        req.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
        a.authenticate(&mut req).unwrap();
        let p = req.extensions().get::<Principal>().unwrap();
        assert_eq!(p.issuer, "https://idp.example");
        assert!(p.id.ends_with("h/a"));

        let mut req = Request::new(());
        let t = token(signer.as_ref(), "https://idp.example", "other");
        req.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
        assert!(a.authenticate(&mut req).is_err());

        let mut req = Request::new(());
        let t = token(signer.as_ref(), "https://evil.example", "hivekeep-api");
        req.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
        assert!(a
            .authenticate(&mut req)
            .unwrap_err()
            .message()
            .contains("not trusted"));

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer abc".parse().unwrap());
        assert!(a.authenticate(&mut req).is_err());
        assert!(validate_providers(&[]).is_err());
    }

    #[test]
    fn mtls_identity_wins() {
        let ca = crate::keeper_auth::testing::pod_ca();
        let chain = crate::keeper_auth::testing::pod_cert(&ca, "ns", "sa", "n");
        let mut req = request_as((), None, Some(chain));
        Authenticator::default().authenticate(&mut req).unwrap();
        assert_eq!(
            req.extensions().get::<Principal>().unwrap().id,
            "spiffe://cluster.local/ns/ns/sa/sa"
        );
    }

    #[test]
    fn config_validation() {
        let ok = AuthenticationConfig {
            jwt_providers: vec![JwtProviderConfig {
                name: "a".into(),
                issuer: "https://idp".into(),
                audiences: vec!["x".into()],
                ..Default::default()
            }],
        };
        validate_config(&ok).unwrap();
        let mut bad = ok.clone();
        bad.jwt_providers[0].issuer = "http://idp".into();
        assert!(validate_config(&bad).is_err());
        let mut bad = ok.clone();
        bad.jwt_providers[0].audiences.clear();
        assert!(validate_config(&bad).is_err());
        let mut bad = ok.clone();
        bad.jwt_providers.push(ok.jwt_providers[0].clone());
        assert!(validate_config(&bad).unwrap_err().0.contains("duplicate"));
        assert!(validate_config(&AuthenticationConfig::default()).is_err());

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.yaml");
        std::fs::write(
            &p,
            "jwtProviders:\n- name: a\n  issuer: https://idp\n  audiences: [x]\n",
        )
        .unwrap();
        assert_eq!(load_config(p.to_str().unwrap()).unwrap(), ok);
        std::fs::write(
            &p,
            "jwtProviders:\n- name: a\n  issuer: https://idp\n  audiences: [x]\n  bogus: 1\n",
        )
        .unwrap();
        assert!(load_config(p.to_str().unwrap()).is_err());
    }
}
