//! One started loop: a task per endpoint sending GETs on a fixed schedule,
//! and what they measured — the counters under one lock, the distributions
//! in its recorder.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::api::{StartRequest, DEFAULT_REQUEST_TIMEOUT_MS, NO_LOOP_MESSAGE};
use super::classify::{classify, RequestError};
use super::client::{tls_config, DialFn, EndpointClient, Trace};
use super::hist::Distribution;
use super::recorder::Recorder;
use super::stats::{Endpoint, Stats};

/// Why a read of a loop could not be answered.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CollectError {
    /// The loop has been stopped.
    #[error("{NO_LOOP_MESSAGE}")]
    NoLoop,
    /// A window id that is neither the last cut nor the next.
    #[error("window {got} is out of sequence: the last cut is {last}, the next window is {}", last + 1)]
    Sequence {
        /// The id asked for.
        got: i64,
        /// The last cut's id.
        last: i64,
    },
    /// The recorder refused the cut.
    #[error("{0}")]
    Recorder(String),
}

struct State {
    /// Set by stop before it releases the recorder, so a collect that raced
    /// the stop answers 404 instead of reading a released recorder.
    stopped: bool,
    /// Counters; elapsed and rounds are derived at snapshot, distributions
    /// at collect.
    stats: Stats,
    /// Start of each endpoint's most recent request.
    last: Vec<Option<Instant>>,
    /// The least-visited endpoint's request count, kept current.
    rounds: i64,
    /// The last cut's id (0 before the first) and its answer, so the same id
    /// asked again is answered without a new cut.
    window: i64,
    cut: Stats,
    rec: Recorder,
}

/// A running loop.
pub struct LoopRun {
    cancel: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    started: Instant,
    state: Mutex<State>,
}

/// The served form of a kept Stats: the per-endpoint slice only when asked.
pub fn view(mut s: Stats, endpoints: bool) -> Stats {
    if !endpoints {
        s.endpoints = Vec::new();
    }
    s
}

impl LoopRun {
    /// A loop over `req`'s endpoints with a fresh recorder and no tasks yet.
    pub fn new(req: &StartRequest) -> Arc<LoopRun> {
        let n = req.endpoints.max(0) as usize;
        Arc::new(LoopRun {
            cancel: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
            started: Instant::now(),
            state: Mutex::new(State {
                stopped: false,
                stats: Stats {
                    scheme: req.url_scheme().to_string(),
                    endpoints: vec![Endpoint::default(); n],
                    ..Default::default()
                },
                last: vec![None; n],
                rounds: 0,
                window: 0,
                cut: Stats::default(),
                rec: Recorder::new(),
            }),
        })
    }

    /// Starts a loop over `urls`, one per endpoint, verifying HTTPS against
    /// `roots` (the platform's when `None`) and dialing through `dial` (a
    /// resolving TCP dial when `None`).
    pub fn start(
        req: &StartRequest,
        urls: &[String],
        roots: Option<rustls::RootCertStore>,
        dial: Option<DialFn>,
    ) -> Result<Arc<LoopRun>, String> {
        let run = LoopRun::new(req);
        let tls = tls_config(roots);
        let timeout_ms = if req.request_timeout_ms == 0 {
            DEFAULT_REQUEST_TIMEOUT_MS
        } else {
            req.request_timeout_ms
        };
        let timeout = Duration::from_millis(timeout_ms as u64);
        let interval = Duration::from_millis(req.interval_ms as u64);
        let offset = Duration::from_millis(req.offset_ms as u64);
        let period = interval * urls.len() as u32;
        let mut clients = Vec::with_capacity(urls.len());
        for u in urls {
            clients.push(EndpointClient::new(u, dial.clone(), tls.clone())?);
        }
        let mut tasks = run.tasks.lock();
        for (i, client) in clients.into_iter().enumerate() {
            let r = run.clone();
            let start_after = offset + interval * i as u32;
            tasks.push(tokio::spawn(async move {
                r.endpoint(client, i, timeout, start_after, period).await
            }));
        }
        drop(tasks);
        Ok(run)
    }

    /// Waits `offset`, opens endpoint `i`, then requests it once per period
    /// until cancelled. At most one request is in flight per endpoint, and
    /// the period is counted from the start of the open: a request that
    /// outlasts the period delays only this endpoint, whose next request
    /// starts as soon as the slow one returns.
    async fn endpoint(
        &self,
        mut client: EndpointClient,
        i: usize,
        timeout: Duration,
        offset: Duration,
        period: Duration,
    ) {
        if !offset.is_zero() {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(offset) => {}
            }
        }
        if self.cancel.is_cancelled() {
            return;
        }
        let mut ticker = (!period.is_zero()).then(|| {
            let mut t = tokio::time::interval_at(Instant::now() + period, period);
            // A tick missed during a slow request fires once, at once; later
            // ticks keep the original schedule.
            t.set_missed_tick_behavior(MissedTickBehavior::Skip);
            t
        });
        self.request(&mut client, i, timeout).await;
        if self.cancel.is_cancelled() {
            return; // stopped mid-open: not an open
        }
        {
            let mut st = self.state.lock();
            st.stats.opened += 1;
            if st.stats.opened == st.stats.endpoints.len() as i64 {
                st.stats.opening_done = self.started.elapsed();
                st.stats.requests_at_opening_done = st.stats.requests;
            }
        }
        while !self.cancel.is_cancelled() {
            match ticker.as_mut() {
                Some(t) => {
                    tokio::select! {
                        _ = self.cancel.cancelled() => return,
                        _ = t.tick() => {}
                    }
                }
                None => tokio::task::yield_now().await,
            }
            self.request(&mut client, i, timeout).await;
        }
    }

    async fn request(&self, client: &mut EndpointClient, i: usize, timeout: Duration) {
        let start = Instant::now();
        {
            let mut st = self.state.lock();
            if let Some(last) = st.last[i] {
                let gap = start - last;
                st.stats.longest_gap = st.stats.longest_gap.max(gap);
                st.stats.endpoints[i].longest_gap = st.stats.endpoints[i].longest_gap.max(gap);
            }
            st.last[i] = Some(start);
        }
        let mut trace = Trace::default();
        let res = tokio::select! {
            _ = self.cancel.cancelled() => {
                // Stopped mid-request: the failure is the stop, not the egress path.
                client.reset();
                return;
            }
            r = tokio::time::timeout(timeout, client.get(&mut trace)) => r,
        };
        let latency = start.elapsed();
        let (status, err) = match res {
            Ok(Ok(status)) => (status, None),
            Ok(Err(e)) => (0, Some(e)),
            Err(_) => {
                client.reset();
                (0, Some(RequestError::Timeout))
            }
        };
        if self.cancel.is_cancelled() {
            return;
        }
        self.record(i, latency, status, err.as_ref(), &trace);
    }

    /// Counts one finished request on endpoint `i`.
    pub fn record(
        &self,
        i: usize,
        latency: Duration,
        status: u16,
        err: Option<&RequestError>,
        trace: &Trace,
    ) {
        let mut guard = self.state.lock();
        let st = &mut *guard;
        let s = &mut st.stats;
        let first = s.endpoints[i].requests == 0;
        s.requests += 1;
        s.endpoints[i].requests += 1;
        if s.endpoints[i].requests - 1 == st.rounds {
            // Only the least-visited endpoint can raise the minimum.
            st.rounds = s.endpoints.iter().map(|e| e.requests).min().unwrap_or(0);
        }
        if trace.new_conn {
            s.new_conns += 1;
        }
        if let (Some(a), Some(b)) = (trace.dns_start, trace.dns_done) {
            st.rec.record(Distribution::Dns, (b - a).as_secs_f64());
        }
        let tls_started = trace.tls_start.is_some();
        if let (Some(a), Some(b)) = (trace.tls_start, trace.tls_done) {
            st.rec.record(Distribution::Tls, (b - a).as_secs_f64());
        }
        let mut class = classify(err, status);
        if !class.is_empty()
            && tls_started
            && trace.tls_done.is_none()
            && !class.starts_with("tls: ")
        {
            // A failure inside the handshake, such as the gateway closing a
            // connection whose SNI no rule allows, is told apart from the
            // same error in an HTTP exchange.
            class = format!("tls handshake: {class}");
        }
        if class.is_empty() {
            s.successes += 1;
            let d = if first {
                Distribution::Opening
            } else {
                Distribution::Latency
            };
            st.rec.record(d, latency.as_secs_f64());
            return;
        }
        *s.errors.entry(class).or_default() += 1;
        s.endpoints[i].errors += 1;
    }

    fn snapshot_locked(&self, st: &State, endpoints: bool) -> Stats {
        let mut s = st.stats.clone();
        s.elapsed = self.started.elapsed();
        s.rounds = st.rounds;
        if !endpoints {
            s.endpoints = Vec::new();
        }
        s
    }

    /// The counters alone, for `GET /stats`; `None` once stopped.
    pub fn snapshot(&self, endpoints: bool) -> Option<Stats> {
        let st = self.state.lock();
        (!st.stopped).then(|| self.snapshot_locked(&st, endpoints))
    }

    /// The id the next cut must carry.
    pub fn next_window(&self) -> i64 {
        self.state.lock().window + 1
    }

    /// Answers `POST /collect` for window `w` (at least 1). The id after the
    /// last cut cuts a new window — counters and distributions under the
    /// lock every record takes, so no sample lands between the two; the
    /// last cut's id answers that cut again.
    pub fn collect(&self, w: i64, endpoints: bool) -> Result<Stats, CollectError> {
        let mut st = self.state.lock();
        if st.stopped {
            return Err(CollectError::NoLoop);
        }
        if w == st.window {
            return Ok(view(st.cut.clone(), endpoints));
        }
        if w != st.window + 1 {
            return Err(CollectError::Sequence {
                got: w,
                last: st.window,
            });
        }
        let s = self.collect_locked(&mut st)?;
        st.window = w;
        st.cut = s.clone();
        Ok(view(s, endpoints))
    }

    fn collect_locked(&self, st: &mut State) -> Result<Stats, CollectError> {
        let mut s = self.snapshot_locked(st, true);
        s.distributions = st
            .rec
            .collect()
            .map_err(|e| CollectError::Recorder(e.to_string()))?;
        Ok(s)
    }

    /// Cancels the loop, waits for every endpoint task to exit, and returns
    /// the final Stats (endpoints included) with the tail window. It then
    /// releases the recorder; a collect that raced the stop answers 404.
    pub async fn stop(&self) -> Result<Stats, CollectError> {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for t in tasks {
            let _ = t.await;
        }
        let mut st = self.state.lock();
        st.stopped = true;
        let s = self.collect_locked(&mut st);
        st.rec.shutdown();
        s
    }

    /// Test access to the counters and the recorder.
    #[cfg(test)]
    pub(crate) fn with_state<T>(
        &self,
        f: impl FnOnce(&mut Stats, &mut i64, &mut Recorder) -> T,
    ) -> T {
        let mut guard = self.state.lock();
        let st = &mut *guard;
        f(&mut st.stats, &mut st.rounds, &mut st.rec)
    }
}
