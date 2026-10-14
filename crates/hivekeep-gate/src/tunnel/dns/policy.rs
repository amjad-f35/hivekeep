//! Where the relay gets an Actor's egress policy, and the per-query check
//! built on it.

use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::evaluator::Evaluator;
use super::protocol::Question;

/// A boxed error from a policy source.
pub type PolicyError = Box<dyn std::error::Error + Send + Sync>;

/// Supplies the current egress policy of the Actor a relay serves.
///
/// Returning the same `Arc` for an unchanged policy lets the relay reuse
/// its compiled form; `None` means the Actor has no policy, so nothing
/// resolves.
#[async_trait]
pub trait PolicySource: Send + Sync {
    /// The current policy.
    async fn egress_policy(&self) -> Result<Option<Arc<pb::EgressPolicy>>, PolicyError>;
}

/// Decides whether a (canonicalized) question may be forwarded.
#[async_trait]
pub trait QueryPolicy: Send + Sync {
    /// Whether `q` may be forwarded upstream.
    async fn allow(&self, q: &Question) -> bool;
}

/// Allows every query.
pub struct AllowAll;

#[async_trait]
impl QueryPolicy for AllowAll {
    async fn allow(&self, _: &Question) -> bool {
        true
    }
}

/// A [`QueryPolicy`] from a synchronous predicate.
pub struct FnPolicy<F>(pub F);

#[async_trait]
impl<F: Fn(&Question) -> bool + Send + Sync> QueryPolicy for FnPolicy<F> {
    async fn allow(&self, q: &Question) -> bool {
        (self.0)(q)
    }
}

/// The last policy seen and its compiled form.
type CachedEvaluator = (Option<Arc<pb::EgressPolicy>>, Arc<Evaluator>);

/// Checks queries against a [`PolicySource`], recompiling only when the
/// source hands out a different policy. A fetch in progress is abandoned
/// (and the query denied) when `cancel` fires.
pub struct EgressPolicyCheck {
    source: Option<Arc<dyn PolicySource>>,
    cancel: CancellationToken,
    cache: Mutex<Option<CachedEvaluator>>,
}

impl EgressPolicyCheck {
    /// A check over `source`; without a source every query is denied.
    pub fn new(source: Option<Arc<dyn PolicySource>>, cancel: CancellationToken) -> Self {
        EgressPolicyCheck {
            source,
            cancel,
            cache: Mutex::new(None),
        }
    }

    fn evaluator(&self, policy: Option<Arc<pb::EgressPolicy>>) -> Arc<Evaluator> {
        let mut cache = self.cache.lock();
        if let Some((cached, eval)) = cache.as_ref() {
            let same = match (cached, &policy) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if same {
                return eval.clone();
            }
        }
        let eval = Arc::new(Evaluator::new(policy.as_deref()));
        *cache = Some((policy, eval.clone()));
        eval
    }
}

#[async_trait]
impl QueryPolicy for EgressPolicyCheck {
    async fn allow(&self, q: &Question) -> bool {
        let Some(source) = &self.source else {
            return false;
        };
        let fetched = tokio::select! {
            r = source.egress_policy() => r,
            _ = self.cancel.cancelled() => return false,
        };
        match fetched {
            Ok(policy) => self.evaluator(policy).allows_hostname(&q.name_str()),
            Err(err) => {
                tracing::debug!(%err, "dns relay denied a query; the Actor's egress policy is unavailable");
                false
            }
        }
    }
}
