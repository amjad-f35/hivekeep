//! Actor JWTs injected into egress requests: minted by the control plane,
//! reused until a third of their lifetime remains, and minted once for
//! concurrent requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use prost::Message;
use tokio::time::Instant;

use super::api::EgressApi;
use crate::singleflight::Group;

type MintResult = Result<String, tonic::Status>;
/// Cached tokens by request encoding, with when reuse ends.
type Tokens = Arc<Mutex<HashMap<Vec<u8>, (String, Instant)>>>;

/// Mints and caches Actor JWTs.
pub struct Minter {
    api: Arc<dyn EgressApi>,
    tokens: Tokens,
    flight: Group<Vec<u8>, MintResult>,
}

impl Minter {
    /// A minter calling `api`.
    pub fn new(api: Arc<dyn EgressApi>) -> Self {
        Minter {
            api,
            tokens: Arc::default(),
            flight: Group::default(),
        }
    }

    fn cached(tokens: &Mutex<HashMap<Vec<u8>, (String, Instant)>>, key: &[u8]) -> Option<String> {
        let mut t = tokens.lock();
        match t.get(key) {
            Some((jwt, until)) if Instant::now() < *until => Some(jwt.clone()),
            Some(_) => {
                t.remove(key);
                None
            }
            None => None,
        }
    }

    /// A JWT for `actor` with the audiences and lifetime of `src`.
    pub async fn token(&self, actor: &ActorRef, src: &pb::ActorJwtSource) -> MintResult {
        let mut audiences = src.audiences.clone();
        audiences.sort();
        let req = pb::MintActorJwtRequest {
            actor: Some(actor.to_object_ref()),
            audiences,
            expiration_seconds: src.expiration_seconds,
        };
        let key = req.encode_to_vec();
        if let Some(jwt) = Self::cached(&self.tokens, &key) {
            return Ok(jwt);
        }
        let (api, tokens, k2) = (self.api.clone(), self.tokens.clone(), key.clone());
        let actor = actor.clone();
        self.flight
            .run(
                key,
                move || async move {
                    if let Some(jwt) = Self::cached(&tokens, &k2) {
                        return Ok(jwt);
                    }
                    let lifetime = Duration::from_secs(req.expiration_seconds.max(0) as u64);
                    let resp = api.mint_actor_jwt(req).await.map_err(|s| {
                        tonic::Status::new(
                            s.code(),
                            format!("minting an Actor JWT for {actor}: {}", s.message()),
                        )
                    })?;
                    let expires = resp
                        .expire_time
                        .and_then(|t| {
                            SystemTime::UNIX_EPOCH.checked_add(Duration::new(
                                t.seconds.max(0) as u64,
                                t.nanos.max(0) as u32,
                            ))
                        })
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    let remaining = expires
                        .duration_since(SystemTime::now())
                        .unwrap_or_default();
                    let reuse = remaining.saturating_sub(lifetime / 3);
                    if !reuse.is_zero() {
                        tokens
                            .lock()
                            .insert(k2, (resp.actor_jwt.clone(), Instant::now() + reuse));
                    }
                    Ok(resp.actor_jwt)
                },
                Err(tonic::Status::internal("Actor JWT mint panicked")),
            )
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Notify;

    /// A control plane minting `jwt-<n>` tokens.
    #[derive(Default)]
    pub(crate) struct FakeMint {
        pub calls: AtomicU32,
        pub last: Mutex<Option<pb::MintActorJwtRequest>>,
        pub err: Mutex<Option<tonic::Status>>,
        pub gate: Option<Arc<Notify>>,
    }

    impl EgressApi for Arc<FakeMint> {
        fn get_actor(
            &self,
            _: pb::GetActorRequest,
        ) -> BoxFuture<'static, Result<pb::Actor, tonic::Status>> {
            Box::pin(async { Err(tonic::Status::unimplemented("")) })
        }
        fn get_actor_egress_policy(
            &self,
            _: pb::GetActorEgressPolicyRequest,
        ) -> BoxFuture<'static, Result<pb::EgressPolicy, tonic::Status>> {
            Box::pin(async { Err(tonic::Status::unimplemented("")) })
        }
        fn mint_actor_jwt(
            &self,
            req: pb::MintActorJwtRequest,
        ) -> BoxFuture<'static, Result<pb::MintActorJwtResponse, tonic::Status>> {
            let this = self.clone();
            Box::pin(async move {
                let n = this.calls.fetch_add(1, Ordering::SeqCst) + 1;
                *this.last.lock() = Some(req.clone());
                if let Some(g) = &this.gate {
                    g.notified().await;
                }
                if let Some(e) = this.err.lock().clone() {
                    return Err(e);
                }
                let exp = SystemTime::now() + Duration::from_secs(req.expiration_seconds as u64);
                let d = exp.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                Ok(pb::MintActorJwtResponse {
                    actor_jwt: format!("jwt-{n}"),
                    expire_time: Some(prost_types::Timestamp {
                        seconds: d.as_secs() as i64,
                        nanos: d.subsec_nanos() as i32,
                    }),
                })
            })
        }
    }

    fn src(lifetime: i64, auds: &[&str]) -> pb::ActorJwtSource {
        pb::ActorJwtSource {
            audiences: auds.iter().map(|s| s.to_string()).collect(),
            expiration_seconds: lifetime,
        }
    }

    fn actor() -> ActorRef {
        ActorRef::new("default", "my-actor")
    }

    #[tokio::test(start_paused = true)]
    async fn reuse_token_until_a_third_of_its_lifetime_remains() {
        let ctl = Arc::new(FakeMint::default());
        let m = Minter::new(Arc::new(ctl.clone()));
        let s = src(900, &["https://b.example", "https://a.example"]);
        assert_eq!(m.token(&actor(), &s).await.unwrap(), "jwt-1");
        let last = ctl.last.lock().clone().unwrap();
        assert_eq!(
            last.audiences,
            vec!["https://a.example", "https://b.example"]
        );
        assert_eq!(last.expiration_seconds, 900);
        assert_eq!(last.actor.unwrap(), actor().to_object_ref());
        // Wall-clock expiry is computed once at mint; the reuse window runs
        // on the (paused) runtime clock.
        tokio::time::advance(Duration::from_secs(599)).await;
        assert_eq!(m.token(&actor(), &s).await.unwrap(), "jwt-1");
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(m.token(&actor(), &s).await.unwrap(), "jwt-2");
    }

    #[tokio::test]
    async fn key_on_actor_audiences_and_lifetime() {
        let ctl = Arc::new(FakeMint::default());
        let m = Minter::new(Arc::new(ctl));
        let other = ActorRef::new("default", "other-actor");
        for (a, s, want) in [
            (actor(), src(900, &["a", "b"]), "jwt-1"),
            (actor(), src(900, &["b", "a"]), "jwt-1"),
            (actor(), src(900, &["a"]), "jwt-2"),
            (actor(), src(600, &["a", "b"]), "jwt-3"),
            (other, src(900, &["a", "b"]), "jwt-4"),
            (actor(), src(900, &["a b"]), "jwt-5"),
        ] {
            assert_eq!(m.token(&a, &s).await.unwrap(), want);
        }
    }

    #[tokio::test]
    async fn do_not_cache_errors() {
        let ctl = Arc::new(FakeMint::default());
        *ctl.err.lock() = Some(tonic::Status::unavailable("hivekeep-api is down"));
        let m = Minter::new(Arc::new(ctl.clone()));
        let s = src(900, &["a"]);
        assert_eq!(
            m.token(&actor(), &s).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        *ctl.err.lock() = None;
        assert_eq!(m.token(&actor(), &s).await.unwrap(), "jwt-2");
    }

    #[tokio::test]
    async fn collapse_concurrent_mints() {
        let gate = Arc::new(Notify::new());
        let ctl = Arc::new(FakeMint {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let m = Arc::new(Minter::new(Arc::new(ctl.clone())));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let m = m.clone();
                tokio::spawn(async move { m.token(&actor(), &src(900, &["a"])).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        gate.notify_waiters();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(ctl.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn mint_outlives_canceled_caller() {
        let gate = Arc::new(Notify::new());
        let ctl = Arc::new(FakeMint {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let m = Arc::new(Minter::new(Arc::new(ctl.clone())));
        let m1 = m.clone();
        let t = tokio::spawn(async move { m1.token(&actor(), &src(900, &["a"])).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.abort();
        assert!(t.await.unwrap_err().is_cancelled());
        gate.notify_waiters();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(m.token(&actor(), &src(900, &["a"])).await.unwrap(), "jwt-1");
        assert_eq!(ctl.calls.load(Ordering::SeqCst), 1);
    }
}
