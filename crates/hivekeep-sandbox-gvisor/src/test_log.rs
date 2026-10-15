//! Captures tracing events in tests, so a test can check the fields a log
//! collector would see.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// One captured event: its level and every field by name, `message`
/// included, rendered as text.
#[derive(Debug, Clone)]
pub struct Record {
    pub level: Level,
    pub fields: BTreeMap<String, String>,
}

impl Record {
    /// A field's value, or "" when absent.
    pub fn get(&self, name: &str) -> &str {
        self.fields.get(name).map(String::as_str).unwrap_or("")
    }
}

/// The events captured so far.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<Record>>>);

impl Captured {
    pub fn records(&self) -> Vec<Record> {
        self.0.lock().clone()
    }

    /// How many events carry exactly `message`.
    pub fn count(&self, message: &str) -> usize {
        self.0
            .lock()
            .iter()
            .filter(|r| r.get("message") == message)
            .count()
    }

    /// The events carrying exactly `message`.
    pub fn with_message(&self, message: &str) -> Vec<Record> {
        self.0
            .lock()
            .iter()
            .filter(|r| r.get("message") == message)
            .cloned()
            .collect()
    }
}

struct Visitor<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Visitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

struct CaptureLayer(Captured);

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Visitor(&mut fields));
        self.0 .0.lock().push(Record {
            level: *event.metadata().level(),
            fields,
        });
    }
}

/// Captures every event on this thread until the guard drops. Use with a
/// current-thread runtime so spawned tasks log on the same thread.
pub fn capture() -> (Captured, tracing::subscriber::DefaultGuard) {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
    let guard = tracing::subscriber::set_default(subscriber);
    (captured, guard)
}
