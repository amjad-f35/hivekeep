//! The storage client a snapshot transfer runs with.
//!
//! The node plugin asks once per transfer with the actor JWT the request
//! carries, and again, forced, after storage refused a request, which means
//! the credential behind the client no longer covers the actor. On GCS the
//! source is the credential exchange, trading the JWT for a token scoped to
//! the actor; on S3 it is [`Ambient`], which ignores the JWT.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::Status;

use crate::error::{Error, Result};
use crate::storage::{ObjectStorage, Uploader};

/// A storage credential and when it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// The access token.
    pub access_token: String,
    /// Its expiry.
    pub expiry: SystemTime,
}

/// The least remaining lifetime a cached [`Token`] is served with, on both
/// sides of the exchange, so a transfer never starts on a token about to
/// expire; there is no mid-transfer refresh. A shorter token still goes to
/// the caller that asked, but is not cached.
pub const MIN_SERVE_LIFETIME: Duration = Duration::from_secs(20 * 60);

/// How much life `t` has left at `now`.
pub fn lifetime_left(t: &Token, now: SystemTime) -> Duration {
    t.expiry.duration_since(now).unwrap_or(Duration::ZERO)
}

/// Supplies the storage client a transfer runs with.
#[async_trait]
pub trait Credentials: Send + Sync + 'static {
    /// The client for the actor `actor_jwt` names.
    async fn client(&self, actor_jwt: &str, force: bool) -> Result<Arc<dyn ObjectStorage>, Status>;
}

/// The pod's own client for every transfer, whatever the JWT.
pub struct Ambient(pub Arc<dyn ObjectStorage>);

#[async_trait]
impl Credentials for Ambient {
    async fn client(&self, _: &str, _: bool) -> Result<Arc<dyn ObjectStorage>, Status> {
        Ok(self.0.clone())
    }
}

/// Whether storage turned a request down for lack of authorization. With a
/// per-actor credential that means it is stale: the actor's prefixes changed
/// or the token expired mid-transfer.
pub fn refused(err: &Error) -> bool {
    err.describe()
        .is_some_and(|d| d.http_status == 401 || d.http_status == 403)
}

struct Current {
    client: Option<Arc<dyn ObjectStorage>>,
    generation: u64,
}

/// One FetchSnapshot or UploadSnapshot: the client its object requests run
/// with, obtained on first use and replaced when a request is refused.
/// Object requests run concurrently, so a replacement one of them forces
/// serves the others.
pub struct Transfer {
    creds: Arc<dyn Credentials>,
    actor_jwt: String,
    budget: Uploader,
    cur: Mutex<Current>,
}

impl Transfer {
    /// A transfer for `actor_jwt` on `budget`.
    pub fn new(creds: Arc<dyn Credentials>, actor_jwt: &str, budget: Uploader) -> Self {
        Transfer {
            creds,
            actor_jwt: actor_jwt.to_string(),
            budget,
            cur: Mutex::new(Current {
                client: None,
                generation: 0,
            }),
        }
    }

    /// Replaces the client. NotFound or Aborted from the source means the
    /// actor is not this node's to transfer for (gone, recreated or
    /// elsewhere); it is reported as FailedPrecondition, since the keeper
    /// reads NotFound from a transfer as a missing snapshot object.
    async fn obtain(&self, cur: &mut Current, force: bool) -> Result<()> {
        match self.creds.client(&self.actor_jwt, force).await {
            Ok(c) => {
                cur.client = Some(c);
                cur.generation += 1;
                Ok(())
            }
            Err(s) if matches!(s.code(), tonic::Code::NotFound | tonic::Code::Aborted) => {
                Err(Status::failed_precondition(format!(
                    "the actor cannot be transferred from this node: {}",
                    s.message()
                ))
                .into())
            }
            Err(s) => Err(s.into()),
        }
    }

    async fn current(&self) -> Result<(Arc<dyn ObjectStorage>, u64)> {
        let mut cur = self.cur.lock().await;
        if cur.client.is_none() {
            self.obtain(&mut cur, false).await?;
        }
        Ok((cur.client.clone().expect("obtained"), cur.generation))
    }

    async fn replace(&self, generation: u64) -> Result<Arc<dyn ObjectStorage>> {
        let mut cur = self.cur.lock().await;
        if cur.generation == generation {
            self.obtain(&mut cur, true).await?;
        }
        Ok(cur.client.clone().expect("obtained"))
    }

    /// Runs one object request. A request storage refuses is retried once
    /// with a replaced client; a second refusal is final, and the caller sees
    /// PermissionDenied: the actor's credential does not cover the request.
    pub async fn run<F, Fut>(&self, op: F) -> Result<()>
    where
        F: Fn(Arc<dyn ObjectStorage>, Uploader) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let (client, generation) = self.current().await?;
        match op(client.clone(), self.budget.with_client(client)).await {
            Err(e) if refused(&e) => {}
            other => return other,
        }
        let client = self.replace(generation).await?;
        match op(client.clone(), self.budget.with_client(client)).await {
            Err(e) if refused(&e) => Err(Status::permission_denied(format!(
                "storage refused the request with the actor's credential: {e}"
            ))
            .into()),
            other => other,
        }
    }
}

/// Wraps an error as the plugin's status.
pub fn status_of(e: Error) -> Status {
    super::status::to_status(e)
}
