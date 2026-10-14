//! An Actor's egress policy as the DNS relay sees it: fetched from the
//! keeper, cached briefly, shared between concurrent queries, and kept
//! (stale) when a refresh fails so a keeper blip does not break name
//! resolution.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;

use super::dns::{PolicyError, PolicySource};

/// How long a fetched policy is reused.
pub const EGRESS_POLICY_TTL: Duration = Duration::from_secs(5);
/// How long one fetch may take.
pub const EGRESS_POLICY_FETCH_TIMEOUT: Duration = Duration::from_secs(2);
/// How often a persistently stale policy is warned about.
pub const EGRESS_POLICY_STALE_WARN_INTERVAL: Duration = Duration::from_secs(5 * 60);

type Policy = Option<Arc<pb::EgressPolicy>>;
type FetchResult = Result<Policy, Arc<str>>;

/// Fetches the policy once.
pub type Fetch =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Option<pb::EgressPolicy>, String>> + Send + Sync>;

/// Log severities used for policy state changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Routine.
    Debug,
    /// State changes.
    Info,
    /// Persistent staleness.
    Warn,
    /// The first failure.
    Error,
}

/// Receives log records (replaced in tests).
pub type Log = Arc<dyn Fn(Level, &str) + Send + Sync>;

/// Messages logged on policy state changes.
pub mod msg {
    /// No policy has been fetched and the fetch failed.
    pub const FETCH_FAILED: &str =
        "Could not fetch the Actor's egress policy; refusing its DNS queries";
    /// A refresh failed; the last policy stays in use.
    pub const REFRESH_FAILED: &str =
        "Could not refresh the Actor's egress policy; using stale cached policy";
    /// The Actor has no policy.
    pub const NO_POLICY: &str = "Actor has no egress policy; blocking all DNS queries";
    /// The Actor has a policy.
    pub const HAS_POLICY: &str = "Actor has an egress policy";
}

#[derive(Default)]
struct State {
    fetched: bool,
    policy: Policy,
    expires: Option<Instant>,
    failing: bool,
    next_stale_warn: Option<Instant>,
    inflight: Option<Shared<BoxFuture<'static, FetchResult>>>,
}

struct Inner {
    fetch: Fetch,
    ttl: Duration,
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
    log: Log,
    state: Mutex<State>,
}

/// The cached egress policy of one Actor.
#[derive(Clone)]
pub struct ActorEgressPolicy {
    inner: Arc<Inner>,
}

/// Logs through `tracing`, tagged with the Actor.
pub fn tracing_log(actor: ActorRef) -> Log {
    Arc::new(move |level, m| {
        let (hive, name) = (&actor.hive, &actor.name);
        match level {
            Level::Debug => tracing::debug!(hk.hive = %hive, hk.actor.name = %name, "{m}"),
            Level::Info => tracing::info!(hk.hive = %hive, hk.actor.name = %name, "{m}"),
            Level::Warn => tracing::warn!(hk.hive = %hive, hk.actor.name = %name, "{m}"),
            Level::Error => tracing::error!(hk.hive = %hive, hk.actor.name = %name, "{m}"),
        }
    })
}

impl ActorEgressPolicy {
    /// A source fetching with `fetch`, using the wall clock.
    pub fn new(fetch: Fetch, log: Log) -> Self {
        Self::with_clock(fetch, EGRESS_POLICY_TTL, Arc::new(Instant::now), log)
    }

    /// A source with an injected clock.
    pub fn with_clock(
        fetch: Fetch,
        ttl: Duration,
        now: Arc<dyn Fn() -> Instant + Send + Sync>,
        log: Log,
    ) -> Self {
        ActorEgressPolicy {
            inner: Arc::new(Inner {
                fetch,
                ttl,
                now,
                log,
                state: Mutex::new(State::default()),
            }),
        }
    }

    fn cached(&self) -> Option<Policy> {
        let st = self.inner.state.lock();
        let now = (self.inner.now)();
        (st.fetched && st.expires.is_some_and(|e| now < e)).then(|| st.policy.clone())
    }

    /// The current policy; concurrent callers share one fetch, and a
    /// caller that stops waiting does not cancel it.
    pub async fn get(&self) -> FetchResult {
        if let Some(p) = self.cached() {
            return Ok(p);
        }
        let shared = {
            let mut st = self.inner.state.lock();
            match &st.inflight {
                Some(f) => f.clone(),
                None => {
                    let this = self.clone();
                    let task = tokio::spawn(async move {
                        if let Some(p) = this.cached() {
                            this.inner.state.lock().inflight = None;
                            return Ok(p);
                        }
                        let r = match tokio::time::timeout(
                            EGRESS_POLICY_FETCH_TIMEOUT,
                            (this.inner.fetch)(),
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_) => Err("fetching the Actor's egress policy timed out".into()),
                        };
                        let out = this.record(r);
                        this.inner.state.lock().inflight = None;
                        out
                    });
                    let f =
                        async move { task.await.unwrap_or_else(|e| Err(Arc::from(e.to_string()))) }
                            .boxed()
                            .shared();
                    st.inflight = Some(f.clone());
                    f
                }
            }
        };
        shared.await
    }

    fn record(&self, r: Result<Option<pb::EgressPolicy>, String>) -> FetchResult {
        let mut st = self.inner.state.lock();
        let now = (self.inner.now)();
        let log = &self.inner.log;
        match r {
            Err(err) => {
                if !st.fetched {
                    let level = if st.failing {
                        Level::Debug
                    } else {
                        Level::Error
                    };
                    st.failing = true;
                    log(level, msg::FETCH_FAILED);
                    return Err(Arc::from(err));
                }
                let level = if !st.failing {
                    st.failing = true;
                    st.next_stale_warn = Some(now + EGRESS_POLICY_STALE_WARN_INTERVAL);
                    Level::Error
                } else if st.next_stale_warn.is_none_or(|w| now >= w) {
                    st.next_stale_warn = Some(now + EGRESS_POLICY_STALE_WARN_INTERVAL);
                    Level::Warn
                } else {
                    Level::Debug
                };
                log(level, msg::REFRESH_FAILED);
                Ok(st.policy.clone())
            }
            Ok(policy) => {
                let policy = policy.map(Arc::new);
                if st.failing || !st.fetched || policy.is_none() != st.policy.is_none() {
                    log(
                        Level::Info,
                        if policy.is_none() {
                            msg::NO_POLICY
                        } else {
                            msg::HAS_POLICY
                        },
                    );
                }
                // Keep the same Arc when the policy did not change, so the
                // relay's compiled form stays valid.
                let unchanged = matches!((&st.policy, &policy), (Some(a), Some(b)) if a == b);
                if !unchanged {
                    st.policy = policy;
                }
                st.failing = false;
                st.next_stale_warn = None;
                st.fetched = true;
                st.expires = Some(now + self.inner.ttl);
                Ok(st.policy.clone())
            }
        }
    }
}

#[async_trait]
impl PolicySource for ActorEgressPolicy {
    async fn egress_policy(&self) -> Result<Option<Arc<pb::EgressPolicy>>, PolicyError> {
        self.get().await.map_err(|e| e.to_string().into())
    }
}

/// Asks the keeper for `actor`'s policy.
pub async fn get_egress_policy<T>(
    client: &mut pb::worker_support_client::WorkerSupportClient<T>,
    actor: &ActorRef,
) -> Result<Option<pb::EgressPolicy>, tonic::Status>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let resp = client
        .get_actor_egress_policy(pb::GetActorEgressPolicyRequest {
            actor_hive: actor.hive.clone(),
            actor_name: actor.name.clone(),
        })
        .await?;
    Ok(resp.into_inner().egress_policy)
}

#[cfg(test)]
#[path = "policy_source_tests.rs"]
mod tests;
