//! The per-actor JWT every snapshot transfer carries, minted by the control
//! plane and cached until shortly before it expires.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use tonic::{Code, Status};

use crate::clock::SharedClock;
use crate::context::Ctx;
use crate::control::WorkerServiceApi;

/// The audience object storage expects on an actor JWT.
pub const ACTOR_JWT_AUDIENCE: &str = "objectstore.hivekeep.io";

/// A cached JWT is refreshed once it has less than this left.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);
/// How many actors' JWTs are cached.
const CACHE_SIZE: usize = 10_000;

/// The actor a transfer acts for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransferActor {
    /// The actor's hive.
    pub hive: String,
    /// The actor's name.
    pub name: String,
    /// The actor's UID.
    pub uid: String,
}

#[derive(Clone)]
struct Cached {
    jwt: String,
    expiry: DateTime<Utc>,
}

/// Mints and caches actor JWTs.
pub struct WorkerActorJwts {
    workers: Arc<dyn WorkerServiceApi>,
    clock: SharedClock,
    cache: Mutex<(HashMap<String, Cached>, VecDeque<String>)>,
}

impl WorkerActorJwts {
    /// A cache minting through `workers`.
    pub fn new(workers: Arc<dyn WorkerServiceApi>, clock: SharedClock) -> Self {
        WorkerActorJwts {
            workers,
            clock,
            cache: Mutex::new((HashMap::new(), VecDeque::new())),
        }
    }

    /// The actor's JWT, minted anew when the cached one is close to
    /// expiring. A control plane that cannot mint one now is Unavailable;
    /// one that no longer lets the actor be transferred from this node
    /// (NotFound, Aborted) is FailedPrecondition; anything else Internal.
    pub async fn get(&self, ctx: &Ctx, actor: &TransferActor) -> Result<String, Status> {
        if let Some(c) = self.cache.lock().0.get(&actor.uid).cloned() {
            if (c.expiry - self.clock.now()).to_std().unwrap_or_default() > REFRESH_MARGIN {
                return Ok(c.jwt);
            }
        }
        let res = self
            .workers
            .mint_worker_actor_jwt(
                ctx,
                api::MintWorkerActorJwtRequest {
                    actor: Some(api::ObjectRef {
                        hive: actor.hive.clone(),
                        name: actor.name.clone(),
                    }),
                    actor_uid: actor.uid.clone(),
                    audiences: vec![ACTOR_JWT_AUDIENCE.to_string()],
                },
            )
            .await;
        let resp = match res {
            Ok(r) => r,
            Err(e) => {
                let who = format!("{}/{}", actor.hive, actor.name);
                return Err(match e.code() {
                    Code::Unavailable => Status::unavailable(format!(
                        "while minting the actor JWT for {who}: {}",
                        e.message()
                    )),
                    Code::NotFound | Code::Aborted => Status::failed_precondition(format!(
                        "actor {who} cannot be transferred from this node: {}",
                        e.message()
                    )),
                    _ => Status::internal(format!(
                        "while minting the actor JWT for {who}: {}",
                        e.message()
                    )),
                });
            }
        };
        if resp.actor_jwt.is_empty() {
            return Err(Status::internal(format!(
                "the control plane minted an empty actor JWT for {}/{}",
                actor.hive, actor.name
            )));
        }
        let expiry = resp
            .expire_time
            .and_then(|t| DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32))
            .unwrap_or(DateTime::UNIX_EPOCH);
        let mut c = self.cache.lock();
        let (map, order) = &mut *c;
        if map
            .insert(
                actor.uid.clone(),
                Cached {
                    jwt: resp.actor_jwt.clone(),
                    expiry,
                },
            )
            .is_none()
        {
            order.push_back(actor.uid.clone());
            while order.len() > CACHE_SIZE {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                }
            }
        }
        Ok(resp.actor_jwt)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::clock::MockClock;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A WorkerService that mints JWTs and records the other calls.
    #[derive(Default)]
    pub(crate) struct FakeWorkers {
        pub(crate) mints: AtomicUsize,
        pub(crate) mint_err: Mutex<Option<Status>>,
        pub(crate) expiry: Mutex<Option<DateTime<Utc>>>,
        pub(crate) calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl WorkerServiceApi for FakeWorkers {
        async fn acquire_worker_epoch(
            &self,
            _: &Ctx,
            req: api::AcquireWorkerEpochRequest,
        ) -> Result<api::AcquireWorkerEpochResponse, Status> {
            self.calls
                .lock()
                .push(format!("acquire:{}", req.worker.unwrap_or_default().name));
            Ok(api::AcquireWorkerEpochResponse { epoch: 7 })
        }
        async fn register_worker(
            &self,
            _: &Ctx,
            req: api::RegisterWorkerRequest,
        ) -> Result<(), Status> {
            self.calls
                .lock()
                .push(format!("register:{}", req.worker.unwrap_or_default().name));
            Ok(())
        }
        async fn set_worker_sandbox_runtimes(
            &self,
            _: &Ctx,
            _: api::SetWorkerSandboxRuntimesRequest,
        ) -> Result<(), Status> {
            self.calls.lock().push("runtimes".into());
            Ok(())
        }
        async fn mint_worker_actor_certificate(
            &self,
            _: &Ctx,
            _: api::MintWorkerActorCertificateRequest,
        ) -> Result<api::MintWorkerActorCertificateResponse, Status> {
            Ok(api::MintWorkerActorCertificateResponse {
                actor_certificates: vec![b"cert".to_vec()],
            })
        }
        async fn mint_worker_actor_jwt(
            &self,
            _: &Ctx,
            req: api::MintWorkerActorJwtRequest,
        ) -> Result<api::MintWorkerActorJwtResponse, Status> {
            let n = self.mints.fetch_add(1, Ordering::SeqCst);
            if let Some(e) = self.mint_err.lock().clone() {
                return Err(e);
            }
            let exp = self
                .expiry
                .lock()
                .unwrap_or(DateTime::from_timestamp(4_000_000_000, 0).unwrap());
            Ok(api::MintWorkerActorJwtResponse {
                actor_jwt: format!("jwt-{}-{n}", req.actor_uid),
                expire_time: Some(crate::snapshot_tracker::timestamp(exp)),
            })
        }
        async fn request_actor_suspend(
            &self,
            _: &Ctx,
            _: api::RequestActorSuspendRequest,
        ) -> Result<(), Status> {
            self.calls.lock().push("suspend".into());
            Ok(())
        }
        async fn report_actor_snapshot_state(
            &self,
            _: &Ctx,
            _: api::ReportActorSnapshotStateRequest,
        ) -> Result<(), Status> {
            self.calls.lock().push("report".into());
            Ok(())
        }
        async fn report_actor_crashed(
            &self,
            _: &Ctx,
            _: api::ReportActorCrashedRequest,
        ) -> Result<(), Status> {
            self.calls.lock().push("crashed".into());
            Ok(())
        }
    }

    fn actor() -> TransferActor {
        TransferActor {
            hive: "h".into(),
            name: "a".into(),
            uid: "u1".into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn caches_until_close_to_expiry() {
        let fake = Arc::new(FakeWorkers::default());
        let now = DateTime::from_timestamp(1_000_000, 0).unwrap();
        *fake.expiry.lock() = Some(now + chrono::Duration::minutes(10));
        let jwts = WorkerActorJwts::new(fake.clone(), Arc::new(MockClock::new(now)));
        let ctx = Ctx::background();
        let a = jwts.get(&ctx, &actor()).await.unwrap();
        let b = jwts.get(&ctx, &actor()).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(fake.mints.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(6 * 60)).await;
        let c = jwts.get(&ctx, &actor()).await.unwrap();
        assert_ne!(a, c, "refreshed inside the margin");
    }

    #[tokio::test]
    async fn maps_mint_failures() {
        let fake = Arc::new(FakeWorkers::default());
        let jwts = WorkerActorJwts::new(fake.clone(), Arc::new(crate::clock::SystemClock));
        for (code, want) in [
            (Code::Unavailable, Code::Unavailable),
            (Code::NotFound, Code::FailedPrecondition),
            (Code::Aborted, Code::FailedPrecondition),
            (Code::PermissionDenied, Code::Internal),
        ] {
            *fake.mint_err.lock() = Some(Status::new(code, "x"));
            assert_eq!(
                jwts.get(&Ctx::background(), &actor())
                    .await
                    .unwrap_err()
                    .code(),
                want
            );
        }
    }
}

#[cfg(test)]
#[path = "worker_jwt_tests.rs"]
mod behavior_tests;
