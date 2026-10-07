//! Structured JSON logging for Actor sandboxes, shared by the worker
//! runtimes: an Actor container's stdout and stderr forwarded line by line,
//! annotated with the Actor's identity labels from [`crate::attr`], and
//! synthetic lifecycle records.
//!
//! Only lifecycle records carry trace context: one forwarder covers a whole
//! container stream, so it cannot tell which request produced a line. An
//! Actor that writes its own `trace_id`/`span_id` keeps them, verbatim like
//! the rest of its fields.

use std::io::{BufRead, Write};
use std::sync::Mutex;

use serde_json::{Map, Value};

use crate::attr::{
    actor_log_labels, LOG_SPAN_ID_FIELD, LOG_TRACE_FLAGS_FIELD, LOG_TRACE_ID_FIELD,
    RESERVED_NAMESPACE,
};
use crate::resource::ActorAttribution;

/// The plain spelling of the label group.
pub const LABELS_KEY_PLAIN: &str = "labels";
/// The spelling Cloud Logging promotes into `LogEntry.labels`.
pub const LABELS_KEY_GCE: &str = "logging.googleapis.com/labels";

/// The label group's spelling for this environment, chosen in one place.
pub fn labels_key_for(on_gce: bool) -> &'static str {
    if on_gce {
        LABELS_KEY_GCE
    } else {
        LABELS_KEY_PLAIN
    }
}

/// Selects the Cloud Logging spelling of the label group
/// ([`LABELS_KEY_GCE`]) for a node whose log agent promotes it.
pub const GCE_LABELS_ENV: &str = "HK_ACTOR_LOG_GCE_LABELS";

/// Whether [`GCE_LABELS_ENV`] asks for the Cloud Logging spelling (`true`
/// or `1`).
pub fn gce_labels_from_env() -> bool {
    std::env::var(GCE_LABELS_ENV).is_ok_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "True"))
}

/// A writer whose writes never interleave across threads.
pub struct SyncedWriter<W: Write> {
    w: Mutex<W>,
}

impl<W: Write> SyncedWriter<W> {
    /// Wraps `w`.
    pub fn new(w: W) -> Self {
        SyncedWriter { w: Mutex::new(w) }
    }

    /// Writes all of `buf` in one critical section.
    pub fn write_all(&self, buf: &[u8]) -> std::io::Result<()> {
        self.w
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .write_all(buf)
    }

    /// The wrapped writer.
    pub fn into_inner(self) -> W {
        self.w.into_inner().unwrap_or_else(|e| e.into_inner())
    }
}

/// A sampled span's identity, as a lifecycle record joins it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// The trace ID; all zero is invalid.
    pub trace_id: [u8; 16],
    /// The span ID; all zero is invalid.
    pub span_id: [u8; 8],
    /// The W3C trace flags.
    pub flags: u8,
}

impl TraceContext {
    /// Whether both IDs are set.
    pub fn is_valid(&self) -> bool {
        self.trace_id != [0; 16] && self.span_id != [0; 8]
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// Writes Actor records as JSON lines to one writer.
pub struct ActorLogger<W: Write> {
    writer: SyncedWriter<W>,
    labels_key: &'static str,
}

impl<W: Write> ActorLogger<W> {
    /// A logger writing to `w`, with the label group spelled for GCE when
    /// `on_gce`.
    pub fn new(w: W, on_gce: bool) -> Self {
        ActorLogger {
            writer: SyncedWriter::new(w),
            labels_key: labels_key_for(on_gce),
        }
    }

    /// The label group's key.
    pub fn labels_key(&self) -> &'static str {
        self.labels_key
    }

    /// The wrapped writer.
    pub fn into_inner(self) -> W {
        self.writer.into_inner()
    }

    /// Writes a lifecycle record, joined to `trace` when it is valid.
    pub fn emit_lifecycle_log(
        &self,
        trace: Option<&TraceContext>,
        msg: &str,
        a: &ActorAttribution,
    ) {
        let mut m = Map::new();
        m.insert("time".into(), now_rfc3339().into());
        m.insert("message".into(), msg.into());
        m.insert(self.labels_key.into(), labels_value(a, ""));
        if let Some(t) = trace.filter(|t| t.is_valid()) {
            m.insert(LOG_TRACE_ID_FIELD.into(), hex(&t.trace_id).into());
            m.insert(LOG_SPAN_ID_FIELD.into(), hex(&t.span_id).into());
            m.insert(
                LOG_TRACE_FLAGS_FIELD.into(),
                format!("{:02x}", t.flags).into(),
            );
        }
        self.write(&m);
    }

    /// Forwards every line of `r` until EOF or a read error. A line holding
    /// exactly one JSON object keeps its fields (minus anything in the
    /// reserved namespace, with any label groups folded into one sanitized
    /// group); anything else becomes the `message` of a new record. Either
    /// way the Actor's identity labels, with `container_name`, are added.
    pub fn wrap_container_logs<R: BufRead>(
        &self,
        mut r: R,
        a: &ActorAttribution,
        container_name: &str,
    ) {
        let mut line = Vec::new();
        loop {
            line.clear();
            let n = match r.read_until(b'\n', &mut line) {
                Ok(n) => n,
                Err(_) => break,
            };
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            self.emit_container_line(&line, a, container_name);
            if n == 0 {
                break;
            }
        }
    }

    /// Forwards one line of a container's output (without its newline);
    /// an empty line is dropped. See [`ActorLogger::wrap_container_logs`]
    /// for how the record is built. For a forwarder that receives the
    /// stream in chunks and splits the lines itself.
    pub fn emit_container_line(&self, line: &[u8], a: &ActorAttribution, container_name: &str) {
        if line.is_empty() {
            return;
        }
        let rec = match parse_object(line) {
            Some(mut m) => {
                if !m.contains_key("time") {
                    m.insert("time".into(), now_rfc3339().into());
                }
                m.retain(|k, _| !k.starts_with(RESERVED_NAMESPACE));
                let mut labels = self.fold_label_groups(&mut m);
                if let Value::Object(ids) = labels_value(a, container_name) {
                    labels.extend(ids);
                }
                m.insert(self.labels_key.into(), Value::Object(labels));
                m
            }
            None => {
                let mut m = Map::new();
                m.insert("time".into(), now_rfc3339().into());
                m.insert(
                    "message".into(),
                    String::from_utf8_lossy(line).into_owned().into(),
                );
                m.insert(self.labels_key.into(), labels_value(a, container_name));
                m
            }
        };
        self.write(&rec);
    }

    fn write(&self, m: &Map<String, Value>) {
        if let Ok(mut b) = serde_json::to_vec(m) {
            b.push(b'\n');
            let _ = self.writer.write_all(&b);
        }
    }

    /// Reduces the record to one sanitized label group under this logger's
    /// key. Both spellings are folded whichever is ours: an Actor can set
    /// either, and the one we do not write is not inert (off GCE a forged
    /// Cloud Logging group is exactly the one promoted). Keys our group
    /// already holds win.
    fn fold_label_groups(&self, m: &mut Map<String, Value>) -> Map<String, Value> {
        let mut labels = sanitize_labels(m.get(self.labels_key));
        for key in [LABELS_KEY_PLAIN, LABELS_KEY_GCE] {
            if key == self.labels_key {
                continue;
            }
            for (k, v) in sanitize_labels(m.get(key)) {
                labels.entry(k).or_insert(v);
            }
            m.remove(key);
        }
        labels
    }
}

fn labels_value(a: &ActorAttribution, container_name: &str) -> Value {
    Value::Object(
        actor_log_labels(a, container_name)
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect(),
    )
}

/// The line as a JSON object, or `None` when it is anything else: not JSON,
/// a non-object value (`null` included), or an object with trailing data.
fn parse_object(line: &[u8]) -> Option<Map<String, Value>> {
    let mut it = serde_json::Deserializer::from_slice(line).into_iter::<Value>();
    let first = it.next()?.ok()?;
    if it.next().is_some() {
        return None;
    }
    match first {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

/// Drops reserved keys from an Actor's label group and stringifies the
/// rest: one non-string value costs a promoted record all its labels.
fn sanitize_labels(v: Option<&Value>) -> Map<String, Value> {
    let Some(Value::Object(labels)) = v else {
        return Map::new();
    };
    labels
        .iter()
        .filter(|(k, _)| !k.starts_with(RESERVED_NAMESPACE))
        .map(|(k, v)| (k.clone(), Value::String(label_value_string(v))))
        .collect()
}

fn label_value_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
