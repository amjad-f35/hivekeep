//! Compiled egress policies by Actor, cached briefly so the CONNECT and
//! the requests inside its tunnel share one lookup.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::resource::{ActorRef, SINGLETON_NAME};
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use tokio::time::Instant;

use super::api::EgressApi;
use crate::egress_policy::Policy;
use crate::singleflight::Group;

/// Default lifetime of a cached policy.
pub const DEFAULT_POLICY_CACHE_TTL: Duration = Duration::from_secs(10);
/// How long one fetch may take.
const POLICY_FETCH_TIMEOUT: Duration = Duration::from_secs(4);
/// Entries kept before expired ones are swept.
pub const MIN_CLEANUP_SIZE: usize = 64;

/// A failed lookup.
#[derive(Debug, Clone)]
pub enum PolicyError {
    /// The Actor has no policy.
    NoPolicy,
    /// The control plane failed.
    Fetch(tonic::Status),
}

type Fetched = Result<Option<Arc<Policy>>, tonic::Status>;

struct Entries {
    map: HashMap<ActorRef, (Option<Arc<Policy>>, Instant)>,
    cleanup_at: usize,
}

/// The cache.
pub struct PolicyCache {
    api: Arc<dyn EgressApi>,
    ttl: Duration,
    entries: Arc<Mutex<Entries>>,
    flight: Group<ActorRef, Fetched>,
}

impl PolicyCache {
    /// A cache over `api`; a zero `ttl` disables caching.
    pub fn new(api: Arc<dyn EgressApi>, ttl: Duration) -> Self {
        PolicyCache {
            api,
            ttl,
            entries: Arc::new(Mutex::new(Entries {
                map: HashMap::new(),
                cleanup_at: MIN_CLEANUP_SIZE,
            })),
            flight: Group::default(),
        }
    }

    fn cached(
        entries: &Mutex<Entries>,
        ttl: Duration,
        r: &ActorRef,
    ) -> Option<Option<Arc<Policy>>> {
        if ttl.is_zero() {
            return None;
        }
        let e = entries.lock();
        e.map
            .get(r)
            .filter(|(_, exp)| Instant::now() < *exp)
            .map(|(p, _)| p.clone())
    }

    /// The Actor's compiled policy.
    pub async fn get(&self, r: &ActorRef) -> Result<Arc<Policy>, PolicyError> {
        let found = match Self::cached(&self.entries, self.ttl, r) {
            Some(p) => p,
            None => {
                let (api, entries, ttl, key) =
                    (self.api.clone(), self.entries.clone(), self.ttl, r.clone());
                self.flight
                    .run(
                        r.clone(),
                        move || async move {
                            if let Some(p) = Self::cached(&entries, ttl, &key) {
                                return Ok(p);
                            }
                            fetch(&*api, &entries, ttl, key).await
                        },
                        Err(tonic::Status::internal("policy fetch panicked")),
                    )
                    .await
                    .map_err(PolicyError::Fetch)?
            }
        };
        found.ok_or(PolicyError::NoPolicy)
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.lock().map.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

async fn fetch(
    api: &dyn EgressApi,
    entries: &Mutex<Entries>,
    ttl: Duration,
    r: ActorRef,
) -> Fetched {
    let req = pb::GetActorEgressPolicyRequest {
        actor: Some(r.to_object_ref()),
        name: SINGLETON_NAME.into(),
    };
    let resp =
        match tokio::time::timeout(POLICY_FETCH_TIMEOUT, api.get_actor_egress_policy(req)).await {
            Ok(r) => r,
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "fetching the egress policy timed out",
            )),
        };
    let policy = match resp {
        Ok(p) => {
            let (compiled, errs) = Policy::compile(&p);
            for err in errs {
                tracing::warn!(actor = %r, %err, "egress policy has an entry this gateway cannot enforce; it will not match anything");
            }
            Some(Arc::new(compiled))
        }
        Err(s) if s.code() == tonic::Code::NotFound => None,
        Err(s) => {
            return Err(tonic::Status::new(
                s.code(),
                format!("fetching egress policy for {r}: {}", s.message()),
            ));
        }
    };
    if !ttl.is_zero() {
        let mut e = entries.lock();
        let now = Instant::now();
        e.map.insert(r, (policy.clone(), now + ttl));
        if e.map.len() >= e.cleanup_at {
            e.map.retain(|_, (_, exp)| now < *exp);
            e.cleanup_at = MIN_CLEANUP_SIZE.max(2 * e.map.len());
        }
    }
    Ok(policy)
}

#[cfg(test)]
#[path = "policy_cache_tests.rs"]
mod tests;
