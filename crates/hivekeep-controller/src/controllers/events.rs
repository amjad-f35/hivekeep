//! Kubernetes Events about the objects the controllers manage.
//!
//! [`EventRecorder`] is fire-and-forget: a failed Event write is logged and
//! never fails a reconcile. [`KubeEventRecorder`] writes `events.k8s.io/v1`
//! Events; [`RecordingEventRecorder`] keeps them in memory for tests.

use std::fmt;

use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, PostParams};
use kube::{Client, Resource, ResourceExt};
use parking_lot::Mutex;

/// Whether an Event reports normal operation or a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Normal operation.
    Normal,
    /// Something needs attention.
    Warning,
}

impl EventType {
    /// The wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Normal => "Normal",
            EventType::Warning => "Warning",
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One Event to publish.
#[derive(Debug, Clone, PartialEq)]
pub struct EventSpec {
    /// The object the Event is about.
    pub regarding: ObjectReference,
    /// A second object involved, if any.
    pub related: Option<ObjectReference>,
    /// Normal or Warning.
    pub type_: EventType,
    /// Machine-readable reason, UpperCamelCase.
    pub reason: String,
    /// What the controller was doing.
    pub action: String,
    /// Human-readable description.
    pub note: String,
}

/// Publishes Events.
pub trait EventRecorder: Send + Sync {
    /// Publishes `event`. Never blocks on the API server.
    fn publish(&self, event: EventSpec);

    /// Publishes an Event about `regarding`.
    fn event(
        &self,
        regarding: ObjectReference,
        related: Option<ObjectReference>,
        type_: EventType,
        reason: &str,
        action: &str,
        note: String,
    ) {
        self.publish(EventSpec {
            regarding,
            related,
            type_,
            reason: reason.to_string(),
            action: action.to_string(),
            note,
        });
    }
}

/// An [`ObjectReference`] to `obj`.
pub fn object_reference<K: Resource<DynamicType = ()>>(obj: &K) -> ObjectReference {
    ObjectReference {
        api_version: Some(K::api_version(&()).into_owned()),
        kind: Some(K::kind(&()).into_owned()),
        name: obj.meta().name.clone(),
        namespace: obj.meta().namespace.clone(),
        uid: obj.meta().uid.clone(),
        resource_version: obj.meta().resource_version.clone(),
        field_path: None,
    }
}

/// Writes `events.k8s.io/v1` Events through the API server.
pub struct KubeEventRecorder {
    client: Client,
    controller: String,
    instance: String,
}

impl KubeEventRecorder {
    /// A recorder that reports as `controller`, from this host.
    pub fn new(client: Client, controller: impl Into<String>) -> Self {
        let controller = controller.into();
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
        let instance = format!("{controller}-{host}");
        KubeEventRecorder {
            client,
            controller,
            instance,
        }
    }

    fn build(&self, ev: &EventSpec, now: Timestamp) -> Event {
        let namespace = ev
            .regarding
            .namespace
            .clone()
            .filter(|ns| !ns.is_empty())
            .unwrap_or_else(|| "default".into());
        let name = format!(
            "{}.{:x}",
            ev.regarding.name.as_deref().unwrap_or("object"),
            now.as_nanosecond()
        );
        Event {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(namespace),
                ..Default::default()
            },
            event_time: Some(MicroTime(now)),
            action: Some(ev.action.clone()),
            reason: Some(ev.reason.clone()),
            note: Some(truncate_note(&ev.note)),
            type_: Some(ev.type_.as_str().into()),
            regarding: Some(ev.regarding.clone()),
            related: ev.related.clone(),
            reporting_controller: Some(self.controller.clone()),
            reporting_instance: Some(self.instance.clone()),
            ..Default::default()
        }
    }
}

/// The API server rejects notes over 1 KiB.
fn truncate_note(note: &str) -> String {
    const MAX: usize = 1024;
    if note.len() <= MAX {
        return note.to_string();
    }
    let mut end = MAX;
    while !note.is_char_boundary(end) {
        end -= 1;
    }
    note[..end].to_string()
}

impl EventRecorder for KubeEventRecorder {
    fn publish(&self, ev: EventSpec) {
        let event = self.build(&ev, Timestamp::now());
        let api: Api<Event> =
            Api::namespaced(self.client.clone(), &event.namespace().unwrap_or_default());
        tokio::spawn(async move {
            if let Err(err) = api.create(&PostParams::default(), &event).await {
                tracing::warn!(%err, reason = %ev.reason, "Failed to write Event");
            }
        });
    }
}

/// Keeps Events in memory, formatted `"<type> <reason> <note>"`.
#[derive(Default)]
pub struct RecordingEventRecorder {
    events: Mutex<Vec<EventSpec>>,
}

impl RecordingEventRecorder {
    /// An empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the Events recorded so far, formatted.
    pub fn drain(&self) -> Vec<String> {
        std::mem::take(&mut *self.events.lock())
            .into_iter()
            .map(|e| format!("{} {} {}", e.type_, e.reason, e.note))
            .collect()
    }

    /// Takes the Events recorded so far.
    pub fn drain_specs(&self) -> Vec<EventSpec> {
        std::mem::take(&mut *self.events.lock())
    }
}

impl EventRecorder for RecordingEventRecorder {
    fn publish(&self, event: EventSpec) {
        self.events.lock().push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_k8s::fake::FakeApiServer;

    fn reference() -> ObjectReference {
        ObjectReference {
            api_version: Some("hivekeep.io/v1".into()),
            kind: Some("WorkerPool".into()),
            name: Some("pool".into()),
            namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn kube_recorder_writes_events() {
        let fake = FakeApiServer::new();
        let rec = KubeEventRecorder::new(fake.client(), "workerpool-controller");
        rec.event(
            reference(),
            None,
            EventType::Warning,
            "Broken",
            "Resolve",
            "it broke".into(),
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let evs = fake.list::<Event>();
            if let Some(ev) = evs.first() {
                assert_eq!(ev.metadata.namespace.as_deref(), Some("ns"));
                assert_eq!(ev.type_.as_deref(), Some("Warning"));
                assert_eq!(ev.reason.as_deref(), Some("Broken"));
                assert_eq!(ev.action.as_deref(), Some("Resolve"));
                assert_eq!(ev.note.as_deref(), Some("it broke"));
                assert_eq!(
                    ev.regarding.as_ref().and_then(|r| r.name.as_deref()),
                    Some("pool")
                );
                assert_eq!(
                    ev.reporting_controller.as_deref(),
                    Some("workerpool-controller")
                );
                assert!(ev.event_time.is_some());
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "no Event written");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn recording_recorder_formats() {
        let rec = RecordingEventRecorder::new();
        rec.event(
            reference(),
            None,
            EventType::Normal,
            "Done",
            "Act",
            "note".into(),
        );
        assert_eq!(rec.drain(), vec!["Normal Done note"]);
        assert!(rec.drain().is_empty());
    }

    #[test]
    fn long_notes_are_truncated() {
        let long = "é".repeat(1000);
        let t = truncate_note(&long);
        assert!(t.len() <= 1024);
        assert!(long.starts_with(&t));
    }
}
