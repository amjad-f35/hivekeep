//! JSON log lines with trace correlation and redacted protobuf values.
//!
//! [`JsonLayer`] is a `tracing` layer that writes one JSON object per event:
//! `time`, `level`, `msg`, the fields of every enclosing span (outermost
//! first, so a span works like a logger with bound attributes), the event's
//! own fields, and the active OpenTelemetry span under the field names the
//! OpenTelemetry spec fixes for non-OTLP log formats, so a collector can lift
//! them onto the record's own trace fields.
//!
//! Protobuf messages are logged through [`ProtoValue`], which masks every
//! `debug_redact` field before anything is rendered (see
//! [`crate::protoredact`]). The layer embeds a recorded value that renders as
//! a JSON object (or `null`) as structured JSON, so a message logged with
//! `resp = %ProtoValue::of(..)` appears as a nested object rather than an
//! escaped string.
//!
//! Not covered: a message logged with `?msg` (its `Debug` output is not
//! redacted and prints every field), or formatted into a string before the
//! call. Log messages through [`ProtoValue`].

use std::fmt;
use std::io::Write;

use opentelemetry::trace::TraceContextExt;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Value as Json;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::attrs::{LOG_SPAN_ID_FIELD, LOG_TRACE_FLAGS_FIELD, LOG_TRACE_ID_FIELD};
use crate::protoredact;

/// A protobuf message prepared for a log line: redacted and rendered once,
/// when it is built.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtoValue(Json);

impl ProtoValue {
    /// A generated message of type `desc`, with `debug_redact` fields masked.
    pub fn of<M: prost::Message>(desc: &MessageDescriptor, msg: &M) -> Self {
        Self::dynamic(&protoredact::to_dynamic(desc, msg))
    }

    /// A generated message whose type is looked up by full name in the
    /// shared descriptor pool. An unknown type logs as `null`: a log line
    /// must never be the reason a secret leaks, so there is no unredacted
    /// fallback.
    pub fn named<M: prost::Message>(full_name: &str, msg: &M) -> Self {
        match protoredact::message_descriptor(full_name) {
            Some(desc) => Self::of(&desc, msg),
            None => ProtoValue(Json::Null),
        }
    }

    /// A dynamic message, with `debug_redact` fields masked.
    pub fn dynamic(msg: &DynamicMessage) -> Self {
        ProtoValue(protoredact::to_json(&protoredact::redacted(msg)))
    }

    /// An absent message, logged as `null`.
    pub fn none() -> Self {
        ProtoValue(Json::Null)
    }

    /// An arbitrary, already safe, string, logged as is.
    pub fn text(s: impl Into<String>) -> Self {
        ProtoValue(Json::String(s.into()))
    }

    /// The rendered JSON.
    pub fn json(&self) -> &Json {
        &self.0
    }
}

impl fmt::Display for ProtoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The JSON formatting layer. Build it with [`JsonLayer::new`].
pub struct JsonLayer<W> {
    make_writer: W,
    with_time: bool,
}

impl<W> JsonLayer<W>
where
    W: for<'a> MakeWriter<'a> + 'static,
{
    /// A layer writing each line to a writer made by `make_writer`.
    pub fn new(make_writer: W) -> Self {
        JsonLayer {
            make_writer,
            with_time: true,
        }
    }

    /// Leaves the `time` field out, for output compared line by line.
    pub fn without_time(mut self) -> Self {
        self.with_time = false;
        self
    }
}

/// The fields of one line, in the order they were added; a repeated key
/// replaces the earlier value in place.
#[derive(Default)]
struct Line(Vec<(String, Json)>);

impl Line {
    fn insert(&mut self, key: String, value: Json) {
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.0.push((key, value)),
        }
    }

    fn remove(&mut self, key: &str) -> Option<Json> {
        let i = self.0.iter().position(|(k, _)| k == key)?;
        Some(self.0.remove(i).1)
    }

    fn render(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128);
        out.push(b'{');
        for (i, (k, v)) in self.0.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            let _ = serde_json::to_writer(&mut out, k);
            out.push(b':');
            let _ = serde_json::to_writer(&mut out, v);
        }
        out.extend_from_slice(b"}\n");
        out
    }
}

/// Fields bound on a span, stored in its extensions.
struct SpanFields(Fields);

/// Recorded fields in recording order.
type Fields = Line;

struct JsonVisitor<'a>(&'a mut Fields);

impl JsonVisitor<'_> {
    fn put(&mut self, field: &Field, v: Json) {
        self.0.insert(field.name().to_string(), v);
    }
}

/// A recorded value that renders as a JSON object, or as `null`, is
/// embedded structurally: that is the shape [`ProtoValue`] renders to.
fn structured_or_string(text: String) -> Json {
    let t = text.trim();
    if t == "null" {
        return Json::Null;
    }
    if t.starts_with('{') && t.ends_with('}') {
        if let Ok(v @ Json::Object(_)) = serde_json::from_str::<Json>(t) {
            return v;
        }
    }
    Json::String(text)
}

impl Visit for JsonVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(
            field,
            serde_json::Number::from_f64(value).map_or(Json::Null, Json::Number),
        );
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Json::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Json::from(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Json::Bool(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Json::String(value.to_string()));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Json::String(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, structured_or_string(format!("{value:?}")));
    }
}

/// The active span context as log fields, or nothing when it is not valid:
/// a trace ID without a span ID names a request but not the operation within
/// it.
fn trace_fields() -> Option<[(&'static str, String); 3]> {
    let cx = opentelemetry::Context::current();
    let span = cx.span();
    let sc = span.span_context();
    if !sc.is_valid() {
        return None;
    }
    Some([
        (LOG_TRACE_ID_FIELD, sc.trace_id().to_string()),
        (LOG_SPAN_ID_FIELD, sc.span_id().to_string()),
        (
            LOG_TRACE_FLAGS_FIELD,
            format!("{:02x}", sc.trace_flags().to_u8()),
        ),
    ])
}

impl<S, W> Layer<S> for JsonLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = Fields::default();
        attrs.record(&mut JsonVisitor(&mut fields));
        span.extensions_mut().insert(SpanFields(fields));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(SpanFields(fields)) = ext.get_mut::<SpanFields>() {
            values.record(&mut JsonVisitor(fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut line = Line::default();
        if self.with_time {
            line.insert(
                "time".into(),
                Json::String(
                    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                ),
            );
        }
        line.insert(
            "level".into(),
            Json::String(event.metadata().level().to_string()),
        );
        line.insert("msg".into(), Json::String(String::new()));

        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(SpanFields(fields)) = span.extensions().get::<SpanFields>() {
                    for (k, v) in &fields.0 {
                        line.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        let mut fields = Fields::default();
        event.record(&mut JsonVisitor(&mut fields));
        if let Some(msg) = fields.remove("message") {
            let msg = match msg {
                Json::String(s) => s,
                other => other.to_string(),
            };
            line.insert("msg".into(), Json::String(msg));
        }
        for (k, v) in fields.0 {
            line.insert(k, v);
        }

        if let Some(trace) = trace_fields() {
            for (k, v) in trace {
                line.insert(k.into(), Json::String(v));
            }
        }

        let _ = self.make_writer.make_writer().write_all(&line.render());
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hivekeep_proto::api::v1 as api;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    const TEST_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const TEST_SPAN_ID: &str = "00f067aa0ba902b7";

    /// A shared in-memory writer for tests.
    #[derive(Clone, Default)]
    pub(crate) struct Buf(pub Arc<Mutex<Vec<u8>>>);

    impl Buf {
        pub(crate) fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
        pub(crate) fn clear(&self) {
            self.0.lock().unwrap().clear();
        }
    }

    impl Write for Buf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    /// Runs `f` with a JSON layer writing to the returned buffer as the
    /// thread's subscriber.
    pub(crate) fn capture(f: impl FnOnce()) -> String {
        let buf = Buf::default();
        let subscriber =
            tracing_subscriber::registry().with(JsonLayer::new(buf.clone()).without_time());
        tracing::subscriber::with_default(subscriber, f);
        buf.text()
    }

    fn parse(line: &str) -> serde_json::Map<String, Json> {
        match serde_json::from_str(line.trim()).unwrap() {
            Json::Object(m) => m,
            other => panic!("not an object: {other}"),
        }
    }

    fn span_context(flags: TraceFlags) -> SpanContext {
        SpanContext::new(
            TraceId::from_hex(TEST_TRACE_ID).unwrap(),
            SpanId::from_hex(TEST_SPAN_ID).unwrap(),
            flags,
            false,
            TraceState::default(),
        )
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn handle_trace_correlation() {
        let cases: Vec<(&str, Option<SpanContext>, Option<(&str, &str, &str)>)> = vec![
            ("no span in context", None, None),
            (
                "invalid span context contributes nothing",
                Some(SpanContext::empty_context()),
                None,
            ),
            (
                "sampled span",
                Some(span_context(TraceFlags::SAMPLED)),
                Some((TEST_TRACE_ID, TEST_SPAN_ID, "01")),
            ),
            (
                "unsampled span still correlates",
                Some(span_context(TraceFlags::default())),
                Some((TEST_TRACE_ID, TEST_SPAN_ID, "00")),
            ),
        ];
        for (name, sc, want) in cases {
            let out = capture(|| {
                let _guard = sc.map(|sc| {
                    opentelemetry::Context::new()
                        .with_remote_span_context(sc)
                        .attach()
                });
                tracing::info!("something happened");
            });
            let rec = parse(&out);
            match want {
                None => {
                    for f in [LOG_TRACE_ID_FIELD, LOG_SPAN_ID_FIELD, LOG_TRACE_FLAGS_FIELD] {
                        assert!(!rec.contains_key(f), "{name}: {f} present");
                    }
                }
                Some((t, s, fl)) => {
                    assert_eq!(rec[LOG_TRACE_ID_FIELD], t, "{name}");
                    assert_eq!(rec[LOG_SPAN_ID_FIELD], s, "{name}");
                    assert_eq!(rec[LOG_TRACE_FLAGS_FIELD], fl, "{name}");
                }
            }
        }
    }

    #[test]
    fn handle_bound_fields_keep_trace_fields_top_level() {
        let out = capture(|| {
            let _g = opentelemetry::Context::new()
                .with_remote_span_context(span_context(TraceFlags::SAMPLED))
                .attach();
            let span = tracing::info_span!("component", component = "keeper");
            let _e = span.enter();
            tracing::info!(id = "abc", "something happened");
        });
        let rec = parse(&out);
        assert_eq!(rec[LOG_TRACE_ID_FIELD], TEST_TRACE_ID);
        assert_eq!(rec["component"], "keeper");
        assert_eq!(rec["id"], "abc");
        assert_eq!(rec["msg"], "something happened");
    }

    fn mint(token: &str) -> api::MintActorJwtResponse {
        api::MintActorJwtResponse {
            actor_jwt: token.into(),
            ..Default::default()
        }
    }

    #[test]
    fn handle_redacts_proto_values_anywhere_in_the_record() {
        const TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl";
        let resp = mint(TOKEN);
        let r#ref = api::ObjectRef {
            hive: "demo-sandbox".into(),
            name: "agent".into(),
        };
        let tpl = api::ActorTemplate {
            containers: vec![api::Container {
                name: "c".into(),
                env: vec![api::EnvVar {
                    name: "API_KEY".into(),
                    value: "sk-secret".into(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = capture(|| {
            let span = tracing::info_span!(
                "bound",
                bound = %ProtoValue::named("hivekeep.api.v1.ActorTemplate", &tpl)
            );
            let _e = span.enter();
            tracing::info!(
                resp = %ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &resp),
                actor = %ProtoValue::named("hivekeep.api.v1.ObjectRef", &r#ref),
                host = "example.com",
                n = 3,
                "test"
            );
        });
        for leak in [TOKEN, "sk-secret"] {
            assert!(!out.contains(leak), "log contains {leak}: {out}");
        }
        for want in [
            r#""resp":{"actor_jwt":"[REDACTED]"}"#,
            r#""bound":{"containers":[{"env":[{"name":"API_KEY","value":"[REDACTED]"}],"name":"c"}]}"#,
            r#""actor":{"hive":"demo-sandbox","name":"agent"}"#,
            r#""host":"example.com""#,
            r#""n":3"#,
        ] {
            assert!(out.contains(want), "log missing {want}\n got: {out}");
        }
        assert_eq!(resp.actor_jwt, TOKEN, "the logged message was mutated");
    }

    #[test]
    fn redact_value_reports_what_it_changes() {
        // A clean message renders exactly as its unredacted JSON; a
        // sensitive one renders without the secret.
        let clean = api::ObjectRef {
            hive: "h".into(),
            name: "n".into(),
        };
        let desc = protoredact::message_descriptor("hivekeep.api.v1.ObjectRef").unwrap();
        let plain = protoredact::to_json(&protoredact::to_dynamic(&desc, &clean));
        assert_eq!(ProtoValue::of(&desc, &clean).json(), &plain);
        let envless = api::ActorTemplate {
            containers: vec![api::Container {
                name: "c".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let v = ProtoValue::named("hivekeep.api.v1.ActorTemplate", &envless);
        assert_eq!(v.to_string(), r#"{"containers":[{"name":"c"}]}"#);
        let sensitive = ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &mint("token"));
        assert!(!sensitive.to_string().contains("token"));
        assert_eq!(ProtoValue::none().to_string(), "null");
        assert_eq!(
            ProtoValue::named("no.such.Type", &mint("token")).to_string(),
            "null"
        );
    }

    #[test]
    fn handle_covers_bound_fields_and_nils_together() {
        const TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.token";
        let out = capture(|| {
            let outer = tracing::info_span!(
                "outer",
                bound = %ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &mint(TOKEN))
            );
            let _o = outer.enter();
            tracing::info!(nilresp = %ProtoValue::none(), plain = "keep", "x");
        });
        assert!(!out.contains(TOKEN));
        let rec = parse(&out);
        assert_eq!(rec["msg"], "x");
        assert_eq!(rec["level"], "INFO");
        assert_eq!(rec["bound"], serde_json::json!({"actor_jwt": "[REDACTED]"}));
        assert_eq!(rec["nilresp"], Json::Null);
        assert_eq!(rec["plain"], "keep");
    }

    struct Counting(Arc<Mutex<usize>>);
    impl fmt::Display for Counting {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            *self.0.lock().unwrap() += 1;
            write!(f, "once")
        }
    }

    #[test]
    fn handle_resolves_values_once() {
        for with_proto in [false, true] {
            let calls = Arc::new(Mutex::new(0));
            let c = Counting(calls.clone());
            capture(|| {
                if with_proto {
                    tracing::info!(v = %c, a = "b", resp = %ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &mint("t")), "x");
                } else {
                    tracing::info!(v = %c, a = "b", "x");
                }
            });
            assert_eq!(*calls.lock().unwrap(), 1, "with_proto={with_proto}");
        }
    }

    #[test]
    fn handle_leaves_records_without_protos_alone() {
        let out = capture(|| {
            tracing::info!(a = "b", n = 1, nilproto = %ProtoValue::none(), text = "{not json", "x");
        });
        assert_eq!(
            out,
            "{\"level\":\"INFO\",\"msg\":\"x\",\"a\":\"b\",\"n\":1,\"nilproto\":null,\"text\":\"{not json\"}\n"
        );
    }

    #[test]
    fn handle_tolerates_messages_with_nothing_to_mask() {
        for (msg, want) in [(mint(""), r#""v":{}"#), (mint("token"), "[REDACTED]")] {
            let out = capture(|| {
                tracing::info!(v = %ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &msg), "x");
            });
            assert!(
                !out.contains("token") && out.contains(want),
                "got {out}, want {want}"
            );
        }
    }
}
