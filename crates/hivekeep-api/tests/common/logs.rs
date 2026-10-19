//! Captures tracing events, and Actor event records, emitted while a test
//! future runs.

use std::cell::RefCell;
use std::sync::{Arc, Once};

use hivekeep_core::actor_event::{self, Level, RecordHandler, StdoutRecord};

use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::subscriber::with_default;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// One captured event.
#[derive(Debug, Clone, Default)]
pub struct Event {
    pub target: String,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl Event {
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

struct Visitor<'a>(&'a mut Event);

impl Visit for Visitor<'_> {
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        let s = format!("{v:?}");
        if f.name() == "message" {
            self.0.message = s;
        } else {
            self.0
                .fields
                .push((f.name().to_string(), s.trim_matches('"').to_string()));
        }
    }
    fn record_str(&mut self, f: &Field, v: &str) {
        if f.name() == "message" {
            self.0.message = v.to_string();
        } else {
            self.0.fields.push((f.name().to_string(), v.to_string()));
        }
    }
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Event>>>);

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, e: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut ev = Event {
            target: e.metadata().target().to_string(),
            ..Default::default()
        };
        e.record(&mut Visitor(&mut ev));
        self.0.lock().push(ev);
    }
}

/// The `target` of a captured Actor event record; its fields are the
/// record's attributes, by attribute key.
pub const ACTOR_EVENTS: &str = actor_event::SCOPE_NAME;

thread_local! {
    static ACTOR_RECORDS: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
}

/// Collects Actor event records written on a thread that is capturing.
/// Captures run their future on the calling thread, so parallel tests do
/// not see each other's records.
struct ThreadRecords;

impl RecordHandler for ThreadRecords {
    fn enabled(&self, _: Level) -> bool {
        true
    }
    fn handle(&self, rec: StdoutRecord) {
        ACTOR_RECORDS.with(|r| {
            if let Some(v) = r.borrow_mut().as_mut() {
                v.push(Event {
                    target: ACTOR_EVENTS.to_string(),
                    message: rec.message,
                    fields: rec
                        .attrs
                        .iter()
                        .map(|a| (a.key.clone(), a.value.as_display()))
                        .collect(),
                });
            }
        });
    }
}

/// Runs `f` on a fresh current-thread runtime with a capturing subscriber,
/// returning its result and the events (tracing events, then the Actor
/// event records).
pub fn capture<T>(f: impl std::future::Future<Output = T>) -> (T, Vec<Event>) {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        actor_event::set_default_handler(Some(Arc::new(ThreadRecords)));
    });
    ACTOR_RECORDS.with(|r| *r.borrow_mut() = Some(Vec::new()));
    let cap = Capture::default();
    let sub = tracing_subscriber::registry().with(cap.clone());
    let out = with_default(sub, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    });
    let mut events = cap.0.lock().clone();
    events.extend(
        ACTOR_RECORDS
            .with(|r| r.borrow_mut().take())
            .unwrap_or_default(),
    );
    (out, events)
}
