//! Where S3 credentials come from: the environment, a web identity token
//! (IRSA-style federation through STS), or the instance metadata service.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::Method;
use parking_lot::Mutex;
use serde::Deserialize;
use url::Url;

use super::sigv4::Credentials;
use crate::error::{Error, Result};
use crate::http::{encode_component, Request, ReqwestTransport, Transport};

/// The default instance metadata endpoint.
pub const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";

/// Hands out signing credentials.
#[async_trait]
pub trait CredentialsProvider: Send + Sync + 'static {
    /// The current credentials.
    async fn credentials(&self) -> Result<Credentials>;
}

/// Fixed credentials.
#[derive(Debug, Clone)]
pub struct StaticCredentials(pub Credentials);

impl StaticCredentials {
    /// A provider of `access_key_id` and `secret`.
    pub fn new(access_key_id: &str, secret: &str) -> Self {
        StaticCredentials(Credentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret.into(),
            session_token: None,
        })
    }
}

#[async_trait]
impl CredentialsProvider for StaticCredentials {
    async fn credentials(&self) -> Result<Credentials> {
        Ok(self.0.clone())
    }
}

/// Temporary credentials fetched by `fetch` and reused until five minutes
/// before they expire.
struct Refreshing<F> {
    fetch: F,
    cur: Mutex<Option<(Credentials, DateTime<Utc>)>>,
}

#[async_trait]
trait Fetch: Send + Sync + 'static {
    async fn fetch(&self) -> Result<(Credentials, DateTime<Utc>)>;
}

#[async_trait]
impl<F: Fetch> CredentialsProvider for Refreshing<F> {
    async fn credentials(&self) -> Result<Credentials> {
        if let Some((c, exp)) = self.cur.lock().clone() {
            if exp - chrono::Duration::minutes(5) > Utc::now() {
                return Ok(c);
            }
        }
        let (c, exp) = self.fetch.fetch().await?;
        *self.cur.lock() = Some((c.clone(), exp));
        Ok(c)
    }
}

struct WebIdentity {
    token_file: String,
    role_arn: String,
    session_name: String,
    sts: Url,
    transport: Arc<dyn Transport>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StsCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResult {
    credentials: StsCredentials,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResponse {
    assume_role_with_web_identity_result: AssumeRoleResult,
}

#[async_trait]
impl Fetch for WebIdentity {
    async fn fetch(&self) -> Result<(Credentials, DateTime<Utc>)> {
        let token = std::fs::read_to_string(&self.token_file)
            .map_err(|e| Error::from(e).context("reading the web identity token"))?;
        let q = [
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", self.role_arn.as_str()),
            ("RoleSessionName", self.session_name.as_str()),
            ("WebIdentityToken", token.trim()),
        ];
        let body = q
            .iter()
            .map(|(k, v)| format!("{k}={}", encode_component(v)))
            .collect::<Vec<_>>()
            .join("&");
        let resp = self
            .transport
            .send(
                Request::new(Method::POST, self.sts.clone())
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(body),
            )
            .await?;
        let status = resp.status.as_u16();
        let body = resp.bytes().await?;
        if status != 200 {
            return Err(Error::other(format!(
                "STS AssumeRoleWithWebIdentity answered HTTP {status}"
            )));
        }
        let r: AssumeRoleResponse = quick_xml::de::from_str(&String::from_utf8_lossy(&body))
            .map_err(|e| Error::other(format!("STS response: {e}")))?;
        let c = r.assume_role_with_web_identity_result.credentials;
        let exp = DateTime::parse_from_rfc3339(&c.expiration)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now() + chrono::Duration::minutes(15));
        Ok((
            Credentials {
                access_key_id: c.access_key_id,
                secret_access_key: c.secret_access_key,
                session_token: Some(c.session_token),
            },
            exp,
        ))
    }
}

struct Imds {
    base: Url,
    transport: Arc<dyn Transport>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ImdsCredentials {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
}

#[async_trait]
impl Fetch for Imds {
    async fn fetch(&self) -> Result<(Credentials, DateTime<Utc>)> {
        let url = |p: &str| self.base.join(p).map_err(|e| Error::other(e.to_string()));
        let resp = self
            .transport
            .send(
                Request::new(Method::PUT, url("latest/api/token")?)
                    .header("x-aws-ec2-metadata-token-ttl-seconds", "21600"),
            )
            .await?;
        let token = String::from_utf8_lossy(&resp.bytes().await?)
            .trim()
            .to_string();
        let get = |u: Url| Request::new(Method::GET, u).header("x-aws-ec2-metadata-token", &token);
        let role = self
            .transport
            .send(get(url("latest/meta-data/iam/security-credentials/")?))
            .await?
            .bytes()
            .await?;
        let role = String::from_utf8_lossy(&role)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        if role.is_empty() {
            return Err(Error::other("the instance has no IAM role"));
        }
        let body = self
            .transport
            .send(get(url(&format!(
                "latest/meta-data/iam/security-credentials/{role}"
            ))?))
            .await?
            .bytes()
            .await?;
        let c: ImdsCredentials = serde_json::from_slice(&body)
            .map_err(|e| Error::other(format!("instance credentials: {e}")))?;
        let exp = DateTime::parse_from_rfc3339(&c.expiration)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now() + chrono::Duration::minutes(15));
        Ok((
            Credentials {
                access_key_id: c.access_key_id,
                secret_access_key: c.secret_access_key,
                session_token: Some(c.token),
            },
            exp,
        ))
    }
}

/// The provider the standard environment describes, first match wins:
/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (with `AWS_SESSION_TOKEN`);
/// `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`; the instance metadata
/// service. Shared config and credentials files are not read.
pub fn from_env() -> Result<Arc<dyn CredentialsProvider>> {
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    if let (Some(ak), Some(sk)) = (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
        return Ok(Arc::new(StaticCredentials(Credentials {
            access_key_id: ak,
            secret_access_key: sk,
            session_token: env("AWS_SESSION_TOKEN"),
        })));
    }
    let transport: Arc<dyn Transport> =
        Arc::new(ReqwestTransport::new(Some(Duration::from_secs(10)))?);
    if let (Some(token_file), Some(role_arn)) =
        (env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN"))
    {
        let region = env("AWS_REGION")
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        let sts = env("AWS_ENDPOINT_URL_STS")
            .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com/"));
        return Ok(Arc::new(Refreshing {
            fetch: WebIdentity {
                token_file,
                role_arn,
                session_name: env("AWS_ROLE_SESSION_NAME")
                    .unwrap_or_else(|| "hivekeep-objectstore".into()),
                sts: Url::parse(&sts).map_err(|e| Error::other(format!("STS endpoint: {e}")))?,
                transport,
            },
            cur: Mutex::new(None),
        }));
    }
    let base =
        env("AWS_EC2_METADATA_SERVICE_ENDPOINT").unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.into());
    let base = if base.ends_with('/') {
        base
    } else {
        format!("{base}/")
    };
    Ok(Arc::new(Refreshing {
        fetch: Imds {
            base: Url::parse(&base).map_err(|e| Error::other(format!("metadata endpoint: {e}")))?,
            transport,
        },
        cur: Mutex::new(None),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{transport_fn, Response};

    #[tokio::test]
    async fn web_identity_exchanges_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let tf = dir.path().join("token");
        std::fs::write(&tf, "jwt\n").unwrap();
        let t = transport_fn(|req: Request| {
            Box::pin(async move {
                let body = String::from_utf8_lossy(&req.body).into_owned();
                assert!(body.contains("WebIdentityToken=jwt"), "{body}");
                assert!(
                    body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr"),
                    "{body}"
                );
                Ok(Response::new(
                    200,
                    "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
                     <AccessKeyId>AK</AccessKeyId><SecretAccessKey>SK</SecretAccessKey>\
                     <SessionToken>ST</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration>\
                     </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>",
                ))
            })
        });
        let p = Refreshing {
            fetch: WebIdentity {
                token_file: tf.display().to_string(),
                role_arn: "arn:aws:iam::1:role/r".into(),
                session_name: "s".into(),
                sts: Url::parse("http://sts.test/").unwrap(),
                transport: t,
            },
            cur: Mutex::new(None),
        };
        let c = p.credentials().await.unwrap();
        assert_eq!(c.access_key_id, "AK");
        assert_eq!(c.session_token.as_deref(), Some("ST"));
    }
}
