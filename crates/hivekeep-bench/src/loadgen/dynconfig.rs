//! The worker's runtime-mutable settings: the locust flags an operator can
//! change in the master's web form.
//!
//! The locust wire protocol only carries the user count and spawn rate, so
//! these come over an HTTP side channel, the master's `/loadgen-config`
//! endpoint, as a JSON object keyed by locust flag name in snake_case.
//!
//! This module does not know what the keys mean. A worker runs exactly one
//! user class, known at launch, and the class hands the [`Holder`] its
//! [`Codec`]: the typed struct it reads its knobs from, with the defaults an
//! unset key falls back to and the rules a fetched value must pass. The
//! holder decodes each payload over those defaults (so a payload stands on
//! its own), refuses one the class cannot run on in favour of the last good
//! config, and hands the class its config back through [`Holder::get`].
//! Keys the class does not name are ignored.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::duration::Seconds;
use super::events::Events;

/// A config payload the class (or the worker's common slice) refuses.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct Refused(pub String);

/// A decoded class config: any comparable, printable value.
pub trait ClassConfig: Any + fmt::Debug + Send + Sync {
    /// Whether `other` is the same type and value.
    fn same_as(&self, other: &dyn ClassConfig) -> bool;
    /// Upcast for downcasting.
    fn as_any(&self) -> &dyn Any;
}

impl<T: Any + fmt::Debug + PartialEq + Send + Sync> ClassConfig for T {
    fn same_as(&self, other: &dyn ClassConfig) -> bool {
        other.as_any().downcast_ref::<T>() == Some(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// How a user class's config comes to be: the value a worker starts from,
/// and the value a payload puts in force.
pub trait Codec: Send + Sync + 'static {
    /// The config before any payload: the class's defaults.
    fn initial(&self) -> Arc<dyn ClassConfig>;
    /// The config `payload`, a JSON object, puts in force, validated. A key
    /// absent from it, or null in it, is at the class's default, not at the
    /// value before. Anything but a JSON object, an empty body included, is
    /// refused.
    fn decode(&self, payload: &[u8]) -> Result<Arc<dyn ClassConfig>, Refused>;
}

/// A codec's rule check; `Err` says what is wrong.
pub type Validate<T> = fn(&T) -> Result<(), String>;

/// The [`Codec`] over a class's knobs struct `T`, whose serde names are the
/// keys it reads (embedded groups such as [`WaitTime`] use
/// `#[serde(flatten)]`). Decoding lays the payload over `defaults` and runs
/// `validate` on the result.
pub struct Typed<T> {
    /// The value every unset key falls back to.
    pub defaults: T,
    /// The rules a decoded value must pass.
    pub validate: Option<Validate<T>>,
}

impl<T> Typed<T> {
    /// A codec with `defaults` and no rules.
    pub fn new(defaults: T) -> Self {
        Typed {
            defaults,
            validate: None,
        }
    }

    /// Adds the rules a decoded value must pass.
    pub fn with_validate(mut self, validate: Validate<T>) -> Self {
        self.validate = Some(validate);
        self
    }
}

/// Parses `payload` as a JSON object.
fn payload_object(payload: &[u8]) -> Result<Map<String, Value>, Refused> {
    match serde_json::from_slice::<Value>(payload) {
        Ok(Value::Object(m)) => Ok(m),
        Ok(other) => Err(Refused(format!(
            "decode config: want a JSON object, got {}",
            kind(&other)
        ))),
        Err(e) => Err(Refused(format!("decode config: {e}"))),
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

impl<T> Typed<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    /// The typed decode: `payload` laid over the defaults, then validated.
    pub fn decode_typed(&self, payload: &[u8]) -> Result<T, Refused> {
        let overlay = payload_object(payload)?;
        let base = match serde_json::to_value(&self.defaults) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        };
        let merged = merge(&base, overlay.iter());
        let next: T = match serde_json::from_value(Value::Object(merged)) {
            Ok(v) => v,
            Err(e) => {
                // Name the key: the first one that fails on its own.
                let key = overlay
                    .iter()
                    .find(|kv| {
                        serde_json::from_value::<T>(Value::Object(merge(
                            &base,
                            std::iter::once(*kv),
                        )))
                        .is_err()
                    })
                    .map(|(k, _)| k.as_str());
                return Err(Refused(match key {
                    Some(k) => format!("decode config: {k}: {e}"),
                    None => format!("decode config: {e}"),
                }));
            }
        };
        if let Some(validate) = self.validate {
            validate(&next).map_err(Refused)?;
        }
        Ok(next)
    }
}

/// `base` with every non-null entry of `overlay` laid over it.
fn merge<'a>(
    base: &Map<String, Value>,
    overlay: impl Iterator<Item = (&'a String, &'a Value)>,
) -> Map<String, Value> {
    let mut out = base.clone();
    for (k, v) in overlay {
        if !v.is_null() {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

impl<T> Codec for Typed<T>
where
    T: Serialize + DeserializeOwned + Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
{
    fn initial(&self) -> Arc<dyn ClassConfig> {
        Arc::new(self.defaults.clone())
    }

    fn decode(&self, payload: &[u8]) -> Result<Arc<dyn ClassConfig>, Refused> {
        Ok(Arc::new(self.decode_typed(payload)?))
    }
}

/// The class config of a class that reads no knobs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NoKnobs {}

/// The slice of the payload the worker itself reads, whatever the class.
/// Every holder decodes it alongside the class's config.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Common {
    /// Fraction of iterations whose spans are sampled.
    pub trace_probability: f64,
}

fn validate_common(c: &Common) -> Result<(), String> {
    if !(0.0..=1.0).contains(&c.trace_probability) {
        return Err(format!(
            "trace_probability must be between 0.0 and 1.0, got: {:.6}",
            c.trace_probability
        ));
    }
    Ok(())
}

fn common_codec() -> Typed<Common> {
    Typed::new(Common::default()).with_validate(validate_common)
}

/// The pair a holder holds at one instant.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The class's config, of the codec's type.
    pub class: Arc<dyn ClassConfig>,
    /// The worker's common slice.
    pub common: Common,
}

/// Holds the running class's config and the worker's [`Common`] slice,
/// swapped together so readers always see a consistent pair.
pub struct Holder {
    codec: Box<dyn Codec>,
    current: RwLock<Snapshot>,
}

impl fmt::Debug for Holder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Holder")
            .field("current", &*self.current.read())
            .finish()
    }
}

impl Holder {
    /// A holder at `codec`'s defaults.
    pub fn new(codec: Box<dyn Codec>) -> Self {
        let class = codec.initial();
        Holder {
            codec,
            current: RwLock::new(Snapshot {
                class,
                common: Common::default(),
            }),
        }
    }

    /// A holder for a class that reads no knobs.
    pub fn without_knobs() -> Self {
        Self::new(Box::new(Typed::new(NoKnobs {})))
    }

    /// For tests: a holder fixed at `cfg`, with no rules.
    pub fn fixed<T>(cfg: T) -> Self
    where
        T: Serialize + DeserializeOwned + Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    {
        Self::new(Box::new(Typed::new(cfg)))
    }

    /// Puts `payload` in force, decoded over the defaults, and returns the
    /// snapshot it left (not one a concurrent apply stored since) and
    /// whether anything changed. A payload the class or the common slice
    /// refuses leaves the holder as it was.
    pub fn apply(&self, payload: &[u8]) -> Result<(Snapshot, bool), Refused> {
        let class = self.codec.decode(payload)?;
        let common = common_codec().decode_typed(payload)?;
        let mut cur = self.current.write();
        if cur.common == common && cur.class.same_as(class.as_ref()) {
            return Ok((cur.clone(), false));
        }
        *cur = Snapshot { class, common };
        Ok((cur.clone(), true))
    }

    /// The class's current config, untyped.
    pub fn load(&self) -> Arc<dyn ClassConfig> {
        self.current.read().class.clone()
    }

    /// The worker's current common slice.
    pub fn common(&self) -> Common {
        self.current.read().common
    }

    /// The class's current config. Asking for a type other than the
    /// codec's is a programming error and panics with both types named.
    pub fn get<T: Clone + 'static>(&self) -> T {
        let cfg = self.load();
        match cfg.as_any().downcast_ref::<T>() {
            Some(v) => v.clone(),
            None => panic!(
                "dynconfig: holder carries {:?}, get asked for {}",
                cfg,
                std::any::type_name::<T>()
            ),
        }
    }
}

/// [`Holder::get`] on an optional holder: no holder reads as `T`'s default.
pub fn get<T: Clone + Default + 'static>(holder: Option<&Holder>) -> T {
    holder.map(Holder::get).unwrap_or_default()
}

/// The gap between one iteration of a user and the next, drawn uniformly
/// from `[min_wait, max_wait]`. Flatten it into a class's knobs struct and
/// call [`WaitTime::validate`] from the class's rules.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WaitTime {
    /// Lower bound.
    #[serde(rename = "min_wait_time")]
    pub min_wait: Seconds,
    /// Upper bound.
    #[serde(rename = "max_wait_time")]
    pub max_wait: Seconds,
}

impl WaitTime {
    /// Rejects negative or inverted bounds.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_wait.is_negative() {
            return Err(format!(
                "min_wait_time cannot be negative: {}",
                self.min_wait
            ));
        }
        if self.max_wait.is_negative() {
            return Err(format!(
                "max_wait_time cannot be negative: {}",
                self.max_wait
            ));
        }
        if self.max_wait < self.min_wait {
            return Err(format!(
                "max_wait_time ({}) cannot be less than min_wait_time ({})",
                self.max_wait, self.min_wait
            ));
        }
        Ok(())
    }

    /// A wait from the window; an inverted or empty window yields the
    /// lower bound.
    pub fn draw(&self) -> Duration {
        uniform(self.min_wait.duration(), self.max_wait.duration())
    }
}

/// A uniform draw from `[lo, hi]`; an inverted or empty range yields `lo`.
pub fn uniform(lo: Duration, hi: Duration) -> Duration {
    if hi <= lo {
        return lo;
    }
    lo + (hi - lo).mul_f64(rand::random::<f64>())
}

/// Resume mode: a ResumeActor call before the traffic.
pub const RESUME_MODE_EXPLICIT: &str = "explicit";
/// Resume mode: no wake call; the first request through the gate wakes the
/// Actor while it is held.
pub const RESUME_MODE_IMPLICIT: &str = "implicit";
/// Lifecycle mode: synchronous suspend (snapshot uploaded before it returns).
pub const LIFECYCLE_MODE_SYNC: &str = "sync";
/// Lifecycle mode: asynchronous suspend (snapshot uploaded in the background).
pub const LIFECYCLE_MODE_ASYNC: &str = "async";
/// Lifecycle mode: the Actor is never suspended between iterations.
pub const LIFECYCLE_MODE_NONE: &str = "none";

/// How a class takes its Actors off a worker and brings them back. An empty
/// string is each class's own default.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lifecycle {
    /// `explicit`, `implicit`, or empty.
    pub resume_mode: String,
    /// `sync`, `async`, `none`, or empty.
    pub lifecycle_mode: String,
}

impl Lifecycle {
    /// Rejects unknown modes.
    pub fn validate(&self) -> Result<(), String> {
        let r = self.resume_mode.as_str();
        if !r.is_empty() && r != RESUME_MODE_EXPLICIT && r != RESUME_MODE_IMPLICIT {
            return Err(format!(
                "invalid resume_mode {r:?}: must be {RESUME_MODE_EXPLICIT:?} or {RESUME_MODE_IMPLICIT:?}"
            ));
        }
        let l = self.lifecycle_mode.as_str();
        if !l.is_empty()
            && l != LIFECYCLE_MODE_SYNC
            && l != LIFECYCLE_MODE_ASYNC
            && l != LIFECYCLE_MODE_NONE
        {
            return Err(format!(
                "invalid lifecycle_mode {l:?}: must be {LIFECYCLE_MODE_SYNC:?}, {LIFECYCLE_MODE_ASYNC:?}, or {LIFECYCLE_MODE_NONE:?}"
            ));
        }
        Ok(())
    }
}

/// GETs `url` and returns its body; a non-200 answer is an error.
pub async fn fetch(url: &str, timeout: Duration) -> Result<Vec<u8>, String> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if resp.status() != reqwest::StatusCode::OK {
        return Err(format!("GET {url}: status {}", resp.status().as_u16()));
    }
    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| format!("read {url}: {e}"))
}

/// What the dynconfig loop pushes the trace probability into.
pub trait ProbabilityUpdater: Send + Sync {
    /// Sets the sampling probability.
    fn update_probability(&self, p: f64);
}

impl ProbabilityUpdater for super::trace::Sampler {
    fn update_probability(&self, p: f64) {
        super::trace::Sampler::update_probability(self, p)
    }
}

/// Fetches `url` into `holder` and, on a change, pushes the trace
/// probability to `sampler` and logs the config now in force.
async fn fetch_and_apply(
    trigger: &str,
    url: &str,
    timeout: Duration,
    holder: &Holder,
    sampler: &dyn ProbabilityUpdater,
) -> Result<(), String> {
    let body = fetch(url, timeout).await?;
    let (applied, changed) = holder.apply(&body).map_err(|e| format!("{url}: {e}"))?;
    if !changed {
        return Ok(());
    }
    sampler.update_probability(applied.common.trace_probability);
    tracing::info!(
        trigger,
        trace_probability = applied.common.trace_probability,
        config = ?applied.class,
        "dynconfig applied"
    );
    Ok(())
}

/// Fetches `url` every `interval` until `cancel` fires, applying each change
/// to `holder` and `sampler`. Returns at once; an interval of zero starts
/// nothing.
///
/// The spawn subscription alone is not enough: the master sends a spawn
/// message only when the user count or spawn rate changes, so a load-shape
/// step that only changes the sample rate reaches the worker through this
/// poll. `on_error` gets each failed fetch and refused payload; it must not
/// end the process, since the holder keeps the last good value. Only
/// changes are logged.
pub fn start_poll(
    cancel: CancellationToken,
    url: String,
    holder: Arc<Holder>,
    sampler: Arc<dyn ProbabilityUpdater>,
    interval: Duration,
    fetch_timeout: Duration,
    on_error: Arc<dyn Fn(String) + Send + Sync>,
) {
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let res = tokio::select! {
                _ = cancel.cancelled() => return,
                r = fetch_and_apply("poll", &url, fetch_timeout, &holder, sampler.as_ref()) => r,
            };
            if let Err(e) = res {
                on_error(e);
            }
        }
    });
}

/// Fetches `url` on every spawn message published on `events`, applying
/// the result to `holder` and `sampler`. `on_error` gets each failed fetch
/// or refused payload, with `fetched` true when an earlier spawn fetch was
/// applied (the holder then has a value from the master and the worker can
/// keep running); with `fetched` false the worker only has its command-line
/// values and callers typically exit.
pub fn subscribe_spawn(
    events: &Events,
    url: String,
    holder: Arc<Holder>,
    sampler: Arc<dyn ProbabilityUpdater>,
    fetch_timeout: Duration,
    on_error: Arc<dyn Fn(String, bool) + Send + Sync>,
) {
    let fetched = Arc::new(std::sync::atomic::AtomicBool::new(false));
    events.subscribe_spawn(Arc::new(move |_count, _rate| {
        let (url, holder, sampler, on_error, fetched) = (
            url.clone(),
            holder.clone(),
            sampler.clone(),
            on_error.clone(),
            fetched.clone(),
        );
        Box::pin(async move {
            use std::sync::atomic::Ordering;
            match fetch_and_apply("spawn", &url, fetch_timeout, &holder, sampler.as_ref()).await {
                Ok(()) => fetched.store(true, Ordering::SeqCst),
                Err(e) => on_error(e, fetched.load(Ordering::SeqCst)),
            }
        })
    }));
}

#[cfg(test)]
#[path = "dynconfig_tests.rs"]
mod tests;
