//! The Actor events: lifecycle transitions and usage samples, a closed
//! vocabulary of named records.
//!
//! One call writes each record as an event to the configured [`EventSink`]
//! (an OTLP log exporter, say) when the sink takes it, and to the stdout
//! [`RecordHandler`] otherwise, or to both when [`set_console`] asks. Both
//! forms come from the same call, so they cannot drift. This is not a
//! bridge for ordinary component logs, which stay on their own path.
//!
//! The event form carries no trace context as attributes: the sink receives
//! it beside the record and sets the record's own trace fields.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use crate::attr;

mod async_handler;

pub use crate::actor_log::TraceContext;
pub use async_handler::AsyncHandler;

/// The instrumentation scope of every Actor event; consumers select a
/// stream by event name.
pub const SCOPE_NAME: &str = "hivekeep.actor_event";

/// An event severity on the OpenTelemetry 1–24 scale (0 is unset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Severity(pub u8);

impl Severity {
    /// Unset.
    pub const UNDEFINED: Severity = Severity(0);
    /// TRACE.
    pub const TRACE: Severity = Severity(1);
    /// DEBUG.
    pub const DEBUG: Severity = Severity(5);
    /// INFO.
    pub const INFO: Severity = Severity(9);
    /// INFO3, a sub-level of INFO.
    pub const INFO3: Severity = Severity(11);
    /// WARN.
    pub const WARN: Severity = Severity(13);
    /// ERROR.
    pub const ERROR: Severity = Severity(17);
    /// FATAL.
    pub const FATAL: Severity = Severity(21);
}

/// A stdout record's level. There are four to the event scale's 24, so a
/// sub-level collapses onto the range it sits in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Debug and below.
    Debug,
    /// Info.
    Info,
    /// Warn.
    Warn,
    /// Error and above.
    Error,
}

/// One name of the vocabulary. The body and severity live here rather than
/// at a call site, so a record's two forms cannot differ. `keys` are on
/// every record; `conditional` keys are present exactly when the
/// registry's condition holds (absent means not measured, zero means
/// measured as zero).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// The record's event name.
    pub name: &'static str,
    /// The display body.
    pub body: &'static str,
    /// The severity.
    pub severity: Severity,
    /// Keys on every record.
    pub keys: &'static [&'static str],
    /// Keys present only when their condition holds.
    pub conditional: &'static [&'static str],
}

impl Event {
    /// The stdout level of this event.
    pub fn level(&self) -> Level {
        match self.severity {
            s if s >= Severity::ERROR => Level::Error,
            s if s >= Severity::WARN => Level::Warn,
            s if s >= Severity::INFO => Level::Info,
            _ => Level::Debug,
        }
    }
}

const LIFECYCLE_KEYS: &[&str] = &[
    attr::HIVE_KEY,
    attr::ACTOR_NAME_KEY,
    attr::ACTOR_UID_KEY,
    attr::TEMPLATE_HIVE_KEY,
    attr::TEMPLATE_NAME_KEY,
    attr::ACTOR_OPERATION_NAME_KEY,
    attr::ACTOR_STATE_KEY,
];

/// An Actor reached a new state; `hk.actor.state` says which.
pub const STATE_CHANGED: Event = Event {
    name: "hk.actor.state_changed",
    body: "Actor state changed",
    severity: Severity::INFO,
    keys: LIFECYCLE_KEYS,
    conditional: &[],
};

/// An Actor was lost. Its own name because of its severity.
pub const CRASHED: Event = Event {
    name: "hk.actor.crashed",
    body: "Actor crashed",
    severity: Severity::ERROR,
    keys: LIFECYCLE_KEYS,
    conditional: &[],
};

/// A worker's measurement of one Actor. The measurements are absent while
/// the Actor is not measurable, and the peak also when the source has none.
pub const USAGE_SAMPLED: Event = Event {
    name: "hk.actor.usage_sampled",
    body: "Actor usage sampled",
    severity: Severity::INFO,
    keys: &[
        attr::HIVE_KEY,
        attr::ACTOR_NAME_KEY,
        attr::ACTOR_UID_KEY,
        attr::TEMPLATE_HIVE_KEY,
        attr::TEMPLATE_NAME_KEY,
        attr::WORKER_POOL_NAMESPACE_KEY,
        attr::WORKER_POOL_NAME_KEY,
        attr::SANDBOX_CLASS_KEY,
        attr::STATS_SOURCE_KEY,
        attr::STATS_KIND_KEY,
        attr::ACTOR_EPOCH_KEY,
    ],
    conditional: &[
        attr::STATS_MEMORY_USAGE_KEY,
        attr::STATS_MEMORY_PEAK_KEY,
        attr::STATS_MEMORY_WORKING_SET_KEY,
        attr::STATS_CPU_TIME_KEY,
    ],
};

/// The whole vocabulary, checked against the event registry.
pub const EVENTS: [Event; 3] = [STATE_CHANGED, CRASHED, USAGE_SAMPLED];

/// A caller-side attribute value, as a structured logger takes it.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A string.
    Str(String),
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A float.
    F64(f64),
    /// A bool.
    Bool(bool),
    /// A duration.
    Duration(Duration),
    /// Anything else, by its display form.
    Other(String),
}

impl Value {
    /// The display form the stdout record writes.
    pub fn as_display(&self) -> String {
        match self {
            Value::Str(s) | Value::Other(s) => s.clone(),
            Value::I64(i) => i.to_string(),
            Value::U64(u) => u.to_string(),
            Value::F64(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Duration(d) => d.as_nanos().to_string(),
        }
    }
}

/// One caller-side attribute.
#[derive(Debug, Clone, PartialEq)]
pub struct Attr {
    /// The key.
    pub key: String,
    /// The value.
    pub value: Value,
}

impl Attr {
    /// A string attribute.
    pub fn str(key: impl Into<String>, v: impl Into<String>) -> Attr {
        Attr {
            key: key.into(),
            value: Value::Str(v.into()),
        }
    }
    /// A signed integer attribute.
    pub fn i64(key: impl Into<String>, v: i64) -> Attr {
        Attr {
            key: key.into(),
            value: Value::I64(v),
        }
    }
    /// An unsigned integer attribute.
    pub fn u64(key: impl Into<String>, v: u64) -> Attr {
        Attr {
            key: key.into(),
            value: Value::U64(v),
        }
    }
    /// A float attribute.
    pub fn f64(key: impl Into<String>, v: f64) -> Attr {
        Attr {
            key: key.into(),
            value: Value::F64(v),
        }
    }
}

/// The Actor identity attributes, in the order every event lists them.
pub fn identity_attrs(a: &crate::resource::ActorAttribution) -> Vec<Attr> {
    attr::actor_log_attrs(a)
        .into_iter()
        .map(|(k, v)| Attr::str(k, v))
        .collect()
}

/// An event-form attribute value: the kinds an OTLP log record holds.
#[derive(Debug, Clone, PartialEq)]
pub enum RecordValue {
    /// A string.
    Str(String),
    /// An integer.
    I64(i64),
    /// A float.
    F64(f64),
    /// A bool.
    Bool(bool),
}

impl RecordValue {
    /// The display form, matching [`Value::as_display`].
    pub fn as_display(&self) -> String {
        match self {
            RecordValue::Str(s) => s.clone(),
            RecordValue::I64(i) => i.to_string(),
            RecordValue::F64(f) => f.to_string(),
            RecordValue::Bool(b) => b.to_string(),
        }
    }
}

impl From<&Value> for RecordValue {
    /// Keeps the kind the stdout form writes. Unsigned values clamp to
    /// `i64::MAX` rather than wrap; durations are nanoseconds.
    fn from(v: &Value) -> Self {
        match v {
            Value::Str(s) | Value::Other(s) => RecordValue::Str(s.clone()),
            Value::I64(i) => RecordValue::I64(*i),
            Value::U64(u) => RecordValue::I64(i64::try_from(*u).unwrap_or(i64::MAX)),
            Value::F64(f) => RecordValue::F64(*f),
            Value::Bool(b) => RecordValue::Bool(*b),
            Value::Duration(d) => RecordValue::I64(i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)),
        }
    }
}

/// The event form of one record.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    /// The event name.
    pub event_name: &'static str,
    /// When the thing happened.
    pub timestamp: SystemTime,
    /// The severity; its text is the stdout format's concern and not set.
    pub severity: Severity,
    /// The display body.
    pub body: &'static str,
    /// Everything machine-readable.
    pub attributes: Vec<(String, RecordValue)>,
}

/// Builds the event form of a record.
pub fn build_record(ev: &Event, t: SystemTime, attrs: &[Attr]) -> EventRecord {
    EventRecord {
        event_name: ev.name,
        timestamp: t,
        severity: ev.severity,
        body: ev.body,
        attributes: attrs
            .iter()
            .map(|a| (a.key.clone(), RecordValue::from(&a.value)))
            .collect(),
    }
}

/// Where event forms go: an OTLP log exporter, typically.
pub trait EventSink: Send + Sync {
    /// Whether the sink takes `ev` now; `false` sends the record to stdout.
    fn enabled(&self, ev: &Event) -> bool;
    /// Writes one record, joined to `trace` when the caller has one.
    fn emit(&self, rec: EventRecord, trace: Option<&TraceContext>);
}

/// A sink that takes nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSink;

impl EventSink for NoSink {
    fn enabled(&self, _: &Event) -> bool {
        false
    }
    fn emit(&self, _: EventRecord, _: Option<&TraceContext>) {}
}

/// The stdout form of one record.
#[derive(Debug, Clone, PartialEq)]
pub struct StdoutRecord {
    /// When the thing happened.
    pub time: SystemTime,
    /// The level.
    pub level: Level,
    /// The display body.
    pub message: String,
    /// The attributes, as given.
    pub attrs: Vec<Attr>,
}

/// Where stdout forms go.
pub trait RecordHandler: Send + Sync {
    /// Whether records at `level` are written.
    fn enabled(&self, level: Level) -> bool;
    /// Writes one record.
    fn handle(&self, rec: StdoutRecord);
}

/// Writes stdout records as JSON lines at or above a level.
#[derive(Debug, Clone, Copy)]
pub struct JsonStdout {
    /// The lowest level written.
    pub min_level: Level,
}

impl RecordHandler for JsonStdout {
    fn enabled(&self, level: Level) -> bool {
        level >= self.min_level
    }
    fn handle(&self, rec: StdoutRecord) {
        use std::io::Write;
        let mut m = serde_json::Map::new();
        let time: chrono::DateTime<chrono::Utc> = rec.time.into();
        m.insert("time".into(), time.to_rfc3339().into());
        m.insert(
            "level".into(),
            format!("{:?}", rec.level).to_uppercase().into(),
        );
        m.insert("msg".into(), rec.message.into());
        for a in rec.attrs {
            let v = match RecordValue::from(&a.value) {
                RecordValue::Str(s) => s.into(),
                RecordValue::I64(i) => i.into(),
                RecordValue::F64(f) => f.into(),
                RecordValue::Bool(b) => b.into(),
            };
            m.insert(a.key, v);
        }
        if let Ok(mut b) = serde_json::to_vec(&m) {
            b.push(b'\n');
            let _ = std::io::stdout().lock().write_all(&b);
        }
    }
}

static DEFAULT_HANDLER: RwLock<Option<Arc<dyn RecordHandler>>> = RwLock::new(None);
static DEFAULT_SINK: RwLock<Option<Arc<dyn EventSink>>> = RwLock::new(None);
static CONSOLE: AtomicBool = AtomicBool::new(false);

/// Replaces the process-wide stdout handler (JSON at info by default) and
/// returns the previous one.
pub fn set_default_handler(h: Option<Arc<dyn RecordHandler>>) -> Option<Arc<dyn RecordHandler>> {
    std::mem::replace(
        &mut *DEFAULT_HANDLER.write().unwrap_or_else(|e| e.into_inner()),
        h,
    )
}

fn default_handler() -> Arc<dyn RecordHandler> {
    DEFAULT_HANDLER
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| {
            Arc::new(JsonStdout {
                min_level: Level::Info,
            })
        })
}

/// Installs the process-wide event sink the free [`log`] functions use.
pub fn set_default_sink(s: Option<Arc<dyn EventSink>>) {
    *DEFAULT_SINK.write().unwrap_or_else(|e| e.into_inner()) = s;
}

/// Whether a record's stdout form is also written when its event form is.
/// Set once at startup.
pub fn set_console(on: bool) {
    CONSOLE.store(on, Ordering::SeqCst);
}

/// Writes records through one sink and one stdout handler.
#[derive(Clone)]
pub struct Emitter {
    sink: Arc<dyn EventSink>,
    /// `None` is the process-wide default handler at call time.
    stdout: Option<Arc<dyn RecordHandler>>,
}

impl Emitter {
    /// Events to `sink`, stdout records through the default handler.
    pub fn new(sink: Arc<dyn EventSink>) -> Self {
        Emitter { sink, stdout: None }
    }

    /// Like [`Emitter::new`] with stdout records through `stdout`, for a
    /// stream whose stdout form must not follow the default level or block
    /// the caller.
    pub fn new_to(sink: Arc<dyn EventSink>, stdout: Arc<dyn RecordHandler>) -> Self {
        Emitter {
            sink,
            stdout: Some(stdout),
        }
    }

    /// Writes `ev`, stamped `t` (when the thing happened, such as a usage
    /// sample's read time) on both forms.
    pub fn log_at(&self, trace: Option<&TraceContext>, ev: &Event, t: SystemTime, attrs: &[Attr]) {
        if self.emit(trace, ev, t, attrs) && !CONSOLE.load(Ordering::SeqCst) {
            return;
        }
        let level = ev.level();
        let h = self.stdout.clone().unwrap_or_else(default_handler);
        if h.enabled(level) {
            h.handle(StdoutRecord {
                time: t,
                level,
                message: ev.body.into(),
                attrs: attrs.to_vec(),
            });
        }
    }

    /// [`Emitter::log_at`] now.
    pub fn log(&self, trace: Option<&TraceContext>, ev: &Event, attrs: &[Attr]) {
        self.log_at(trace, ev, SystemTime::now(), attrs)
    }

    /// Writes the event form and reports whether it did.
    pub fn emit(
        &self,
        trace: Option<&TraceContext>,
        ev: &Event,
        t: SystemTime,
        attrs: &[Attr],
    ) -> bool {
        if !self.sink.enabled(ev) {
            return false;
        }
        self.sink
            .emit(build_record(ev, t, attrs), trace.filter(|t| t.is_valid()));
        true
    }
}

/// The emitter over the process-wide sink (none until one is installed).
pub fn default_emitter() -> Emitter {
    let sink = DEFAULT_SINK
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| Arc::new(NoSink));
    Emitter::new(sink)
}

/// Records `ev` through the process-wide sink.
pub fn log(trace: Option<&TraceContext>, ev: &Event, attrs: &[Attr]) {
    default_emitter().log(trace, ev, attrs)
}

/// Records `ev` at `t` through the process-wide sink.
pub fn log_at(trace: Option<&TraceContext>, ev: &Event, t: SystemTime, attrs: &[Attr]) {
    default_emitter().log_at(trace, ev, t, attrs)
}

#[cfg(test)]
mod tests;
