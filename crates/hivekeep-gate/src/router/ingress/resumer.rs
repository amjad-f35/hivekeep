//! Resuming an Actor before routing to it.
//!
//! Concurrent requests for one Actor share a single *flight* (one
//! `ResumeActor` call, retried as needed) and its outcome. Retryable
//! conditions (a concurrent operation; with parking enabled, also no
//! capacity, a transient state or an unavailable control plane) are
//! retried with backoff until the flight's budget elapses. The budget
//! bounds retries, never an attempt in flight: a resume that overshoots it
//! is still served.
//!
//! A request only takes a parking-lot slot once its flight has had to
//! retry; requests answered on the first attempt never park.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::errors::{parking_full, shed_outcome};
use super::parking::{
    ParkOutcome, ParkedRequestConfig, ParkingLot, DEFAULT_PARKED_REQUEST_RETRY_FACTOR,
    DEFAULT_PARKED_REQUEST_RETRY_INTERVAL, DEFAULT_PARKED_REQUEST_RETRY_JITTER,
};
use crate::attr;
use crate::router::extproc::RouteError;

/// Retry budget when parking is disabled (only concurrent-resume
/// conflicts are retried then).
pub const FAIL_FAST_RESUME_BUDGET: Duration = Duration::from_secs(15);

/// The control-plane call the resumer makes.
pub trait ActorApi: Send + Sync + 'static {
    /// Resumes an Actor (a no-op for a running one).
    fn resume_actor(
        &self,
        req: pb::ResumeActorRequest,
    ) -> BoxFuture<'static, Result<pb::ResumeActorResponse, tonic::Status>>;
}

impl ActorApi for pb::control_client::ControlClient<tonic::transport::Channel> {
    fn resume_actor(
        &self,
        req: pb::ResumeActorRequest,
    ) -> BoxFuture<'static, Result<pb::ResumeActorResponse, tonic::Status>> {
        let mut c = self.clone();
        Box::pin(async move {
            pb::control_client::ControlClient::resume_actor(&mut c, req)
                .await
                .map(|r| r.into_inner())
        })
    }
}

/// Why a resume failed.
#[derive(Debug, Clone)]
pub enum ResumeError {
    /// The control plane said no.
    Status(tonic::Status),
    /// The budget elapsed while the flight was still retrying; carries the
    /// last retryable error.
    BudgetExhausted(tonic::Status),
    /// The caller went away.
    Canceled,
    /// The caller's deadline passed.
    Timeout,
    /// A routing decision (e.g. the lot was full).
    Route(RouteError),
    /// Anything else.
    Other(String),
}

impl ResumeError {
    /// The gRPC code, looking through budget exhaustion.
    pub fn code(&self) -> Option<tonic::Code> {
        match self {
            ResumeError::Status(s) | ResumeError::BudgetExhausted(s) => Some(s.code()),
            ResumeError::Route(r) => r.code,
            _ => None,
        }
    }

    /// How a park ending with this error is labeled.
    pub fn park_outcome(err: Option<&ResumeError>) -> ParkOutcome {
        match err {
            None => ParkOutcome::Served,
            Some(ResumeError::BudgetExhausted(_)) => ParkOutcome::BudgetExhausted,
            Some(ResumeError::Canceled) => ParkOutcome::Canceled,
            Some(ResumeError::Timeout) => ParkOutcome::Timeout,
            Some(_) => ParkOutcome::Error,
        }
    }
}

/// How a request's resume went, for the route-duration metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// The Actor was already running.
    None,
    /// This request started the resume.
    Triggered,
    /// This request joined another's resume.
    Joined,
    /// Unknown (the resume failed).
    Unknown,
}

impl ResumeOutcome {
    /// The label value.
    pub fn as_str(self) -> &'static str {
        match self {
            ResumeOutcome::None => attr::resume::NONE,
            ResumeOutcome::Triggered => attr::resume::TRIGGERED,
            ResumeOutcome::Joined => attr::resume::JOINED,
            ResumeOutcome::Unknown => attr::resume::UNKNOWN,
        }
    }
}

/// Exponential backoff without cap or attempt limit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    /// The first delay.
    pub interval: Duration,
    /// Multiplier per step.
    pub factor: f64,
    /// Random fraction added per step.
    pub jitter: f64,
}

impl Backoff {
    /// The delay of the next step, advancing the backoff.
    pub fn step(&mut self) -> Duration {
        let d = self.interval;
        if self.factor != 0.0 {
            self.interval = self.interval.mul_f64(self.factor);
        }
        if self.jitter > 0.0 {
            d + d.mul_f64(rand::rng().random::<f64>() * self.jitter)
        } else {
            d
        }
    }
}

/// The caller's side of a request: its cancellation and deadline.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    /// Fires when the client goes away.
    pub cancel: Option<CancellationToken>,
    /// When the client stops waiting.
    pub deadline: Option<Instant>,
}

struct CallResult {
    actor: Option<pb::Actor>,
    resumed: bool,
    leader_id: u64,
    err: Option<ResumeError>,
}

#[derive(Default, Clone)]
struct FlightState {
    retrying: bool,
    last_retry: Option<tonic::Status>,
    result: Option<Arc<CallResult>>,
}

type Flight = Arc<watch::Sender<FlightState>>;

/// Resumes Actors, sharing flights between concurrent callers.
pub struct ActorResumer {
    api: Arc<dyn ActorApi>,
    flights: Mutex<HashMap<String, Flight>>,
    lot: Option<Arc<ParkingLot>>,
    park_enabled: bool,
    budget: Duration,
    backoff: Backoff,
    next_id: AtomicU64,
}

async fn wait_ctx(ctx: &RequestCtx) -> ResumeError {
    let cancel = async {
        match &ctx.cancel {
            Some(c) => c.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let deadline = async {
        match ctx.deadline {
            Some(d) => tokio::time::sleep_until(d).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = cancel => ResumeError::Canceled,
        _ = deadline => ResumeError::Timeout,
    }
}

fn caller_result(res: &CallResult, req_id: u64) -> Result<(pb::Actor, ResumeOutcome), ResumeError> {
    if let Some(e) = &res.err {
        return Err(e.clone());
    }
    let outcome = match (res.resumed, res.leader_id == req_id) {
        (false, _) => ResumeOutcome::None,
        (true, true) => ResumeOutcome::Triggered,
        (true, false) => ResumeOutcome::Joined,
    };
    Ok((res.actor.clone().unwrap_or_default(), outcome))
}

impl ActorResumer {
    /// A resumer with parking disabled (only conflicts are retried).
    pub fn new(api: Arc<dyn ActorApi>) -> Self {
        ActorResumer {
            api,
            flights: Mutex::new(HashMap::new()),
            lot: None,
            park_enabled: false,
            budget: FAIL_FAST_RESUME_BUDGET,
            backoff: Backoff {
                interval: DEFAULT_PARKED_REQUEST_RETRY_INTERVAL,
                factor: DEFAULT_PARKED_REQUEST_RETRY_FACTOR,
                jitter: DEFAULT_PARKED_REQUEST_RETRY_JITTER,
            },
            next_id: AtomicU64::new(0),
        }
    }

    /// Applies parking settings.
    pub fn with_parking(mut self, cfg: ParkedRequestConfig) -> Self {
        let cfg = cfg.normalized();
        self.park_enabled = cfg.enabled();
        if self.park_enabled {
            self.budget = cfg.budget;
        }
        self.backoff = Backoff {
            interval: cfg.retry_interval,
            factor: cfg.retry_factor,
            jitter: cfg.retry_jitter,
        };
        self
    }

    /// Uses `lot` for parked callers.
    pub fn with_lot(mut self, lot: Arc<ParkingLot>) -> Self {
        self.lot = Some(lot);
        self
    }

    /// The backoff retries use.
    pub fn backoff(&self) -> Backoff {
        self.backoff
    }

    fn retryable(&self, s: &tonic::Status) -> bool {
        match s.code() {
            tonic::Code::Aborted => true,
            tonic::Code::ResourceExhausted
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unavailable => self.park_enabled,
            _ => false,
        }
    }

    /// Resumes `actor`, joining a flight already in progress.
    pub async fn resume_actor(
        self: &Arc<Self>,
        actor: &ActorRef,
        ctx: &RequestCtx,
    ) -> Result<(pb::Actor, ResumeOutcome), ResumeError> {
        let req_id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let key = actor.to_string();
        let span =
            tracing::info_span!("ResumeActor", hk.hive = %actor.hive, hk.actor.name = %actor.name);
        let flight = {
            let mut flights = self.flights.lock();
            match flights.get(&key) {
                Some(f) => f.clone(),
                None => {
                    let f: Flight = Arc::new(watch::channel(FlightState::default()).0);
                    flights.insert(key.clone(), f.clone());
                    let this = self.clone();
                    let (f2, a2) = (f.clone(), actor.clone());
                    tokio::spawn(tracing::Instrument::instrument(
                        async move { this.run_flight(f2, key, a2, req_id).await },
                        span,
                    ));
                    f
                }
            }
        };
        self.await_flight(&flight, actor, req_id, ctx).await
    }

    async fn run_flight(&self, f: Flight, key: String, actor: ActorRef, req_id: u64) {
        let deadline = Instant::now() + self.budget;
        let mut backoff = self.backoff;
        let mut last_retry: Option<tonic::Status>;
        let result = loop {
            let req = pb::ResumeActorRequest {
                actor: Some(actor.to_object_ref()),
            };
            match self.api.resume_actor(req).await {
                Ok(resp) => {
                    break CallResult {
                        actor: resp.actor,
                        resumed: resp.resumed,
                        leader_id: req_id,
                        err: None,
                    }
                }
                Err(s) if self.retryable(&s) => {
                    f.send_modify(|st| {
                        st.retrying = true;
                        st.last_retry = Some(s.clone());
                    });
                    last_retry = Some(s);
                }
                Err(s) => {
                    break CallResult {
                        actor: None,
                        resumed: false,
                        leader_id: req_id,
                        err: Some(ResumeError::Status(s)),
                    }
                }
            }
            let exhausted = || CallResult {
                actor: None,
                resumed: false,
                leader_id: req_id,
                err: last_retry.clone().map(ResumeError::BudgetExhausted),
            };
            if Instant::now() >= deadline {
                break exhausted();
            }
            let delay = backoff.step();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = tokio::time::sleep_until(deadline) => break exhausted(),
            }
        };
        self.flights.lock().remove(&key);
        f.send_modify(|st| st.result = Some(Arc::new(result)));
    }

    async fn await_flight(
        &self,
        f: &Flight,
        actor: &ActorRef,
        req_id: u64,
        ctx: &RequestCtx,
    ) -> Result<(pb::Actor, ResumeOutcome), ResumeError> {
        let mut rx = f.subscribe();
        let interrupted = wait_ctx(ctx);
        tokio::pin!(interrupted);
        // Wait for the first attempt to resolve or to need a retry.
        let last_retry = loop {
            {
                let st = rx.borrow_and_update();
                if let Some(r) = &st.result {
                    return caller_result(r, req_id);
                }
                if st.retrying {
                    break st.last_retry.clone();
                }
            }
            tokio::select! {
                changed = rx.changed() => if changed.is_err() {
                    return Err(ResumeError::Other("resume flight ended without a result".into()));
                },
                e = &mut interrupted => return Err(e),
            }
        };
        let slot = match &self.lot {
            Some(lot) => match lot.enter(shed_outcome(last_retry.as_ref())) {
                Some(s) => Some(s),
                None => {
                    return Err(ResumeError::Route(parking_full(
                        &actor.to_string(),
                        last_retry.as_ref(),
                    )))
                }
            },
            None => None,
        };
        let res = loop {
            if let Some(r) = rx.borrow_and_update().result.clone() {
                break caller_result(&r, req_id);
            }
            tokio::select! {
                changed = rx.changed() => if changed.is_err() {
                    break Err(ResumeError::Other("resume flight ended without a result".into()));
                },
                e = &mut interrupted => break Err(e),
            }
        };
        if let Some(slot) = slot {
            slot.release(ResumeError::park_outcome(res.as_ref().err()));
        }
        res
    }
}

#[cfg(test)]
#[path = "resumer_tests.rs"]
pub(crate) mod tests;
