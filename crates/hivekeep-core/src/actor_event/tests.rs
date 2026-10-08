use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;
use crate::attr::{actor_state, operation, stats_kind, stats_source};
use crate::resource::{ActorAttribution, ActorRef};

const HIVE: &str = "hk-demo-counter";
const ACTOR: &str = "counter-1";
const UID: &str = "8f2a1c4e6b0d47f1";
const TEMPLATE: &str = "counter";

/// Serializes the tests that swap the process-wide handler or console flag.
static GLOBALS: Mutex<()> = Mutex::new(());

fn globals() -> MutexGuard<'static, ()> {
    GLOBALS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Restores the default handler when dropped.
struct DefaultHandler(Option<Arc<dyn RecordHandler>>);

impl DefaultHandler {
    fn set(h: Arc<dyn RecordHandler>) -> Self {
        DefaultHandler(set_default_handler(Some(h)))
    }
}

impl Drop for DefaultHandler {
    fn drop(&mut self) {
        set_default_handler(self.0.take());
    }
}

fn attribution() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new(HIVE, ACTOR),
        uid: UID.into(),
        template_hive: HIVE.into(),
        template_name: TEMPLATE.into(),
    }
}

fn state_changed_attrs(state: &str) -> Vec<Attr> {
    let mut a = identity_attrs(&attribution());
    a.push(Attr::str(attr::ACTOR_OPERATION_NAME_KEY, operation::RESUME));
    a.push(Attr::str(attr::ACTOR_STATE_KEY, state));
    a
}

fn crashed_attrs() -> Vec<Attr> {
    state_changed_attrs(actor_state::CRASHED)
}

fn usage_base(source: &str) -> Vec<Attr> {
    let mut a = identity_attrs(&attribution());
    a.push(Attr::str(attr::WORKER_POOL_NAMESPACE_KEY, "hk-system"));
    a.push(Attr::str(attr::WORKER_POOL_NAME_KEY, "default"));
    a.push(Attr::str(attr::SANDBOX_CLASS_KEY, "gvisor"));
    a.push(Attr::str(attr::STATS_SOURCE_KEY, source));
    a.push(Attr::str(attr::STATS_KIND_KEY, stats_kind::PERIODIC));
    a
}

fn usage_sampled_attrs() -> Vec<Attr> {
    let mut a = usage_base(stats_source::CGROUP);
    a.push(Attr::u64(attr::STATS_MEMORY_USAGE_KEY, 40 << 20));
    a.push(Attr::u64(attr::STATS_MEMORY_PEAK_KEY, 48 << 20));
    a.push(Attr::u64(attr::STATS_MEMORY_WORKING_SET_KEY, 32 << 20));
    a.push(Attr::f64(attr::STATS_CPU_TIME_KEY, 1.5));
    a.push(Attr::i64(attr::ACTOR_EPOCH_KEY, 1_699_999_990_000_000_000));
    a
}

/// An Actor that is not measurable yet: required keys only.
fn usage_pending_attrs() -> Vec<Attr> {
    let mut a = usage_base(stats_source::UNSPECIFIED);
    a.push(Attr::i64(attr::ACTOR_EPOCH_KEY, 1_699_999_990_000_000_000));
    a
}

fn record_attrs(rec: &EventRecord) -> BTreeMap<String, String> {
    rec.attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.as_display()))
        .collect()
}

/// Collects event forms, with the trace each came with.
#[derive(Default)]
struct MemSink {
    on: bool,
    records: Mutex<Vec<(EventRecord, Option<TraceContext>)>>,
}

impl MemSink {
    fn on() -> Arc<Self> {
        Arc::new(MemSink {
            on: true,
            ..Default::default()
        })
    }
    fn records(&self) -> Vec<(EventRecord, Option<TraceContext>)> {
        self.records.lock().unwrap().clone()
    }
}

impl EventSink for MemSink {
    fn enabled(&self, _: &Event) -> bool {
        self.on
    }
    fn emit(&self, rec: EventRecord, trace: Option<&TraceContext>) {
        self.records.lock().unwrap().push((rec, trace.copied()));
    }
}

/// Keeps stdout forms.
#[derive(Default)]
struct Capture {
    min: Option<Level>,
    records: Mutex<Vec<StdoutRecord>>,
}

impl Capture {
    fn records(&self) -> Vec<StdoutRecord> {
        self.records.lock().unwrap().clone()
    }
}

impl RecordHandler for Capture {
    fn enabled(&self, level: Level) -> bool {
        self.min.is_none_or(|m| level >= m)
    }
    fn handle(&self, rec: StdoutRecord) {
        self.records.lock().unwrap().push(rec);
    }
}

#[test]
fn build_record_cases() {
    let now = UNIX_EPOCH + Duration::from_secs(1_789_473_600);
    struct Case {
        name: &'static str,
        event: Event,
        attrs: Vec<Attr>,
        want_name: &'static str,
        want_body: &'static str,
        want_sev: Severity,
        want_vals: Vec<(&'static str, &'static str)>,
        want_absent: &'static [&'static str],
    }
    let cases = vec![
        Case {
            name: "state changed",
            event: STATE_CHANGED,
            attrs: state_changed_attrs(actor_state::RUNNING),
            want_name: "hk.actor.state_changed",
            want_body: "Actor state changed",
            want_sev: Severity::INFO,
            want_vals: vec![
                (attr::ACTOR_STATE_KEY, actor_state::RUNNING),
                (attr::ACTOR_OPERATION_NAME_KEY, operation::RESUME),
                (attr::ACTOR_UID_KEY, UID),
            ],
            want_absent: &[],
        },
        Case {
            name: "deleted is a state, not a name of its own",
            event: STATE_CHANGED,
            attrs: state_changed_attrs(actor_state::DELETED),
            want_name: "hk.actor.state_changed",
            want_body: "Actor state changed",
            want_sev: Severity::INFO,
            want_vals: vec![(attr::ACTOR_STATE_KEY, actor_state::DELETED)],
            want_absent: &[],
        },
        Case {
            name: "crashed",
            event: CRASHED,
            attrs: crashed_attrs(),
            want_name: "hk.actor.crashed",
            want_body: "Actor crashed",
            want_sev: Severity::ERROR,
            want_vals: vec![(attr::ACTOR_STATE_KEY, actor_state::CRASHED)],
            want_absent: &[],
        },
        Case {
            name: "usage sampled",
            event: USAGE_SAMPLED,
            attrs: usage_sampled_attrs(),
            want_name: "hk.actor.usage_sampled",
            want_body: "Actor usage sampled",
            want_sev: Severity::INFO,
            want_vals: vec![
                (attr::STATS_KIND_KEY, stats_kind::PERIODIC),
                (attr::ACTOR_EPOCH_KEY, "1699999990000000000"),
                (attr::STATS_CPU_TIME_KEY, "1.5"),
                (attr::ACTOR_UID_KEY, UID),
            ],
            want_absent: &[],
        },
        Case {
            name: "usage sampled while pending carries no measurements",
            event: USAGE_SAMPLED,
            attrs: usage_pending_attrs(),
            want_name: "hk.actor.usage_sampled",
            want_body: "Actor usage sampled",
            want_sev: Severity::INFO,
            want_vals: vec![(attr::STATS_SOURCE_KEY, stats_source::UNSPECIFIED)],
            want_absent: USAGE_SAMPLED.conditional,
        },
    ];
    for c in cases {
        let rec = build_record(&c.event, now, &c.attrs);
        assert_eq!(rec.event_name, c.want_name, "{}", c.name);
        assert_eq!(rec.body, c.want_body, "{}", c.name);
        assert_eq!(rec.severity, c.want_sev, "{}", c.name);
        assert_eq!(rec.timestamp, now, "{}", c.name);
        let got = record_attrs(&rec);
        for (k, v) in &c.want_vals {
            assert_eq!(got.get(*k).map(String::as_str), Some(*v), "{}: {k}", c.name);
        }
        // The name promises a shape: every required key, nothing undeclared.
        for k in c.event.keys {
            assert!(got.contains_key(*k), "{}: missing declared key {k}", c.name);
        }
        for k in got.keys() {
            assert!(
                c.event.keys.contains(&k.as_str()) || c.event.conditional.contains(&k.as_str()),
                "{}: undeclared key {k}",
                c.name
            );
        }
        for k in c.want_absent {
            assert!(!got.contains_key(*k), "{}: {k} must be absent", c.name);
        }
    }
}

#[test]
fn event_level() {
    let lvl = |s| {
        Event {
            severity: s,
            ..STATE_CHANGED
        }
        .level()
    };
    for (name, sev, want) in [
        (
            "unset falls to the quietest level",
            Severity::UNDEFINED,
            Level::Debug,
        ),
        ("trace", Severity::TRACE, Level::Debug),
        ("debug", Severity::DEBUG, Level::Debug),
        ("info", Severity::INFO, Level::Info),
        ("warn", Severity::WARN, Level::Warn),
        ("error", Severity::ERROR, Level::Error),
        ("fatal is as high as it goes", Severity::FATAL, Level::Error),
        ("a sub-level keeps its range", Severity::INFO3, Level::Info),
    ] {
        assert_eq!(lvl(sev), want, "{name}");
    }
    assert_eq!(STATE_CHANGED.level(), Level::Info);
    assert_eq!(CRASHED.level(), Level::Error);
    assert_eq!(USAGE_SAMPLED.level(), Level::Info);
}

/// One call, one record: the event form when the sink takes it, the stdout
/// form when it does not, with the same fields either way.
#[test]
fn log_writes_one_copy() {
    let _g = globals();
    for (name, ev, attrs) in [
        (
            "state changed",
            STATE_CHANGED,
            state_changed_attrs(actor_state::RUNNING),
        ),
        ("crashed", CRASHED, crashed_attrs()),
        ("usage sampled", USAGE_SAMPLED, usage_sampled_attrs()),
    ] {
        let stdout = Arc::new(Capture::default());
        let _d = DefaultHandler::set(stdout.clone());

        let sink = MemSink::on();
        Emitter::new(sink.clone()).log(None, &ev, &attrs);
        let events = sink.records();
        assert_eq!(
            (events.len(), stdout.records().len()),
            (1, 0),
            "{name}: sink on"
        );
        let rec = &events[0].0;
        assert_eq!(rec.body, ev.body);
        assert_eq!(rec.severity, ev.severity);

        Emitter::new(Arc::new(NoSink)).log(None, &ev, &attrs);
        let out = stdout.records();
        assert_eq!(out.len(), 1, "{name}: sink off");
        assert_eq!(out[0].message, ev.body);
        assert_eq!(out[0].level, ev.level());
        let stdout_attrs: BTreeMap<String, String> = out[0]
            .attrs
            .iter()
            .map(|a| (a.key.clone(), a.value.as_display()))
            .collect();
        assert_eq!(stdout_attrs, record_attrs(rec), "{name}");
    }
}

/// Trace context travels beside the record, never as its attributes.
#[test]
fn emit_carries_trace_context() {
    let sink = MemSink::on();
    let trace = TraceContext {
        trace_id: [7; 16],
        span_id: [9; 8],
        flags: 1,
    };
    assert!(Emitter::new(sink.clone()).emit(
        Some(&trace),
        &STATE_CHANGED,
        SystemTime::now(),
        &state_changed_attrs(actor_state::RUNNING)
    ));
    let got = sink.records();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, Some(trace));
    for (k, _) in &got[0].0.attributes {
        assert!(
            ![
                attr::LOG_TRACE_ID_FIELD,
                attr::LOG_SPAN_ID_FIELD,
                attr::LOG_TRACE_FLAGS_FIELD
            ]
            .contains(&k.as_str()),
            "trace context as attribute {k}"
        );
    }
}

#[test]
fn emit_is_a_no_op_without_a_sink() {
    // No test installs a process-wide sink: this must neither panic nor
    // claim to have written.
    assert!(!default_emitter().emit(
        None,
        &STATE_CHANGED,
        SystemTime::now(),
        &state_changed_attrs(actor_state::RUNNING)
    ));
}

#[test]
fn build_record_keeps_value_kinds() {
    for (name, v, want) in [
        (
            "string",
            Value::Str("v".into()),
            RecordValue::Str("v".into()),
        ),
        ("int", Value::I64(7), RecordValue::I64(7)),
        ("uint", Value::U64(7), RecordValue::I64(7)),
        (
            "uint above i64 clamps",
            Value::U64(u64::MAX),
            RecordValue::I64(i64::MAX),
        ),
        ("float", Value::F64(1.5), RecordValue::F64(1.5)),
        ("bool", Value::Bool(true), RecordValue::Bool(true)),
        (
            "duration is nanoseconds, as in the stdout form",
            Value::Duration(Duration::from_millis(1500)),
            RecordValue::I64(1_500_000_000),
        ),
        (
            "anything else is its display form",
            Value::Other("{}".into()),
            RecordValue::Str("{}".into()),
        ),
    ] {
        let rec = build_record(
            &STATE_CHANGED,
            SystemTime::now(),
            &[Attr {
                key: "k".into(),
                value: v.clone(),
            }],
        );
        assert_eq!(rec.attributes[0].1, want, "{name}");
        // The two forms print alike, except where the event form clamps.
        if !matches!(v, Value::U64(u) if u > i64::MAX as u64) {
            assert_eq!(rec.attributes[0].1.as_display(), v.as_display(), "{name}");
        }
    }
}

/// The caller's time, not the write time, stamps both forms.
#[test]
fn log_at_stamps_the_record() {
    let _g = globals();
    let stdout = Arc::new(Capture::default());
    let _d = DefaultHandler::set(stdout.clone());
    let sink = MemSink::on();
    let at = UNIX_EPOCH + Duration::from_secs(1_790_337_600);
    Emitter::new(sink.clone()).log_at(None, &USAGE_SAMPLED, at, &usage_sampled_attrs());
    Emitter::new(Arc::new(NoSink)).log_at(None, &USAGE_SAMPLED, at, &usage_sampled_attrs());
    let (out, events) = (stdout.records(), sink.records());
    assert_eq!((out.len(), events.len()), (1, 1));
    assert_eq!(out[0].time, at);
    assert_eq!(events[0].0.timestamp, at);
}

/// Console mode: both forms from one call, with the same time.
#[test]
fn set_console_writes_both() {
    let _g = globals();
    let stdout = Arc::new(Capture::default());
    let _d = DefaultHandler::set(stdout.clone());
    set_console(true);
    let sink = MemSink::on();
    Emitter::new(sink.clone()).log(
        None,
        &STATE_CHANGED,
        &state_changed_attrs(actor_state::RUNNING),
    );
    set_console(false);
    let (out, events) = (stdout.records(), sink.records());
    assert_eq!((out.len(), events.len()), (1, 1));
    assert_eq!(out[0].time, events[0].0.timestamp);
}

/// Blocks every write until released.
struct Gate {
    release: Mutex<bool>,
    cv: std::sync::Condvar,
    msgs: Mutex<Vec<String>>,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Gate {
            release: Mutex::new(false),
            cv: std::sync::Condvar::new(),
            msgs: Mutex::new(Vec::new()),
        })
    }
    fn open(&self) {
        *self.release.lock().unwrap() = true;
        self.cv.notify_all();
    }
    fn msgs(&self) -> Vec<String> {
        self.msgs.lock().unwrap().clone()
    }
}

impl RecordHandler for Gate {
    fn enabled(&self, _: Level) -> bool {
        true
    }
    fn handle(&self, rec: StdoutRecord) {
        let mut open = self.release.lock().unwrap();
        while !*open {
            open = self.cv.wait(open).unwrap();
        }
        drop(open);
        self.msgs.lock().unwrap().push(rec.message);
    }
}

fn msg(m: &str) -> StdoutRecord {
    StdoutRecord {
        time: SystemTime::now(),
        level: Level::Info,
        message: m.into(),
        attrs: Vec::new(),
    }
}

#[test]
fn async_handler_writes_in_order() {
    let inner = Arc::new(Capture::default());
    let h = AsyncHandler::new(inner.clone(), 8);
    for m in ["one", "two", "three"] {
        h.handle(msg(m));
    }
    h.close();
    let got: Vec<String> = inner.records().into_iter().map(|r| r.message).collect();
    assert_eq!(got, ["one", "two", "three"]);
}

/// The writer holds its first record; the queue fills, and the overflow is
/// counted instead of blocking the caller.
#[test]
fn async_handler_drops_when_full() {
    let inner = Gate::new();
    let h = Arc::new(AsyncHandler::new(inner.clone(), 2));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    {
        let h = h.clone();
        std::thread::spawn(move || {
            for _ in 0..10 {
                h.handle(msg("m"));
            }
            let _ = done_tx.send(());
        });
    }
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("handle blocked on a full queue");
    inner.open();
    h.close();
    let msgs = inner.msgs();
    assert!(
        msgs.iter().any(|m| m == async_handler::DROP_REPORT),
        "{msgs:?}"
    );
    let written = msgs.iter().filter(|m| *m == "m").count();
    assert!((1..=3).contains(&written), "wrote {written} of 10");
}

#[test]
fn async_handler_after_close() {
    let inner = Arc::new(Capture::default());
    let h = AsyncHandler::new(inner.clone(), 1);
    h.close();
    h.close();
    h.handle(msg("late"));
    assert_eq!(
        inner.records().len(),
        1,
        "a record handled after close was lost"
    );
}

/// The stdout form goes to the given handler, so the default's level does
/// not reach it.
#[test]
fn new_to_bypasses_the_default() {
    let _g = globals();
    let _d = DefaultHandler::set(Arc::new(Capture {
        min: Some(Level::Error),
        ..Default::default()
    }));
    let quiet = Arc::new(Capture::default());
    Emitter::new_to(Arc::new(NoSink), quiet.clone()).log(
        None,
        &USAGE_SAMPLED,
        &usage_sampled_attrs(),
    );
    assert_eq!(quiet.records().len(), 1);
}

/// The drop report goes through the records' own handler, so a quiet
/// default cannot hide it.
#[test]
fn async_handler_reports_drops_through_its_handler() {
    let _g = globals();
    let _d = DefaultHandler::set(Arc::new(Capture {
        min: Some(Level::Error),
        ..Default::default()
    }));
    let inner = Gate::new();
    let h = AsyncHandler::new(inner.clone(), 1);
    for _ in 0..5 {
        h.handle(msg("m"));
    }
    inner.open();
    h.close();
    assert!(inner.msgs().iter().any(|m| m == async_handler::DROP_REPORT));
}

/// The vocabulary and the event registry agree both ways, so an event has
/// to be declared before it ships.
#[test]
fn events_match_the_registry() {
    #[derive(serde::Deserialize)]
    struct Registry {
        groups: Vec<Group>,
    }
    #[derive(serde::Deserialize)]
    struct Group {
        #[serde(rename = "type")]
        kind: String,
        name: String,
        attributes: Vec<GroupAttr>,
    }
    #[derive(serde::Deserialize)]
    struct GroupAttr {
        #[serde(rename = "ref")]
        key: String,
        requirement_level: serde_yaml::Value,
    }
    let reg: Registry = serde_yaml::from_str(include_str!("../../registry/events.yaml")).unwrap();
    let mut declared: BTreeMap<String, (Vec<String>, Vec<String>)> = BTreeMap::new();
    for g in reg.groups.into_iter().filter(|g| g.kind == "event") {
        let mut shape = (Vec::new(), Vec::new());
        for a in g.attributes {
            match &a.requirement_level {
                serde_yaml::Value::String(s) => {
                    assert_eq!(s, "required", "{}: {} is {s:?}", g.name, a.key);
                    shape.0.push(a.key);
                }
                serde_yaml::Value::Mapping(m) => {
                    let cond = m
                        .get("conditionally_required")
                        .and_then(serde_yaml::Value::as_str)
                        .unwrap_or("");
                    assert!(
                        !cond.is_empty(),
                        "{}: {} has no stated condition",
                        g.name,
                        a.key
                    );
                    shape.1.push(a.key);
                }
                other => panic!("{}: {} has requirement level {other:?}", g.name, a.key),
            }
        }
        declared.insert(g.name, shape);
    }
    for ev in EVENTS {
        let (mut req, mut cond) = declared
            .remove(ev.name)
            .unwrap_or_else(|| panic!("{} has no registry group", ev.name));
        let mut keys: Vec<String> = ev.keys.iter().map(|k| k.to_string()).collect();
        let mut conditional: Vec<String> = ev.conditional.iter().map(|k| k.to_string()).collect();
        req.sort();
        cond.sort();
        keys.sort();
        conditional.sort();
        assert_eq!(req, keys, "{} required keys", ev.name);
        assert_eq!(cond, conditional, "{} conditional keys", ev.name);
    }
    assert!(
        declared.is_empty(),
        "registry declares events nothing emits: {:?}",
        declared.keys()
    );
}
