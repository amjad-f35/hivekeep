use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::attr::{
    ACTOR_CONTAINER_NAME_KEY, ACTOR_NAME_KEY, ACTOR_UID_KEY, HIVE_KEY, TEMPLATE_HIVE_KEY,
    TEMPLATE_NAME_KEY,
};
use crate::resource::ActorRef;

const HIVE: &str = "default";
const ACTOR: &str = "act-1";
const UID: &str = "uid-1";
const TEMPLATE_HIVE: &str = "tmpl-ns";
const TEMPLATE: &str = "tmpl-1";
const CONTAINER: &str = "ctr-1";

fn attribution() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new(HIVE, ACTOR),
        uid: UID.into(),
        template_hive: TEMPLATE_HIVE.into(),
        template_name: TEMPLATE.into(),
    }
}

/// Runs `input` through a logger and returns the one record and the
/// logger's label key.
fn wrap(input: &str, on_gce: bool) -> (Map<String, Value>, &'static str) {
    let al = ActorLogger::new(Vec::new(), on_gce);
    al.wrap_container_logs(input.as_bytes(), &attribution(), CONTAINER);
    let key = al.labels_key();
    (decode_line(&al.into_inner()), key)
}

fn decode_line(out: &[u8]) -> Map<String, Value> {
    let text = std::str::from_utf8(out).unwrap();
    assert_eq!(text.lines().count(), 1, "{text}");
    serde_json::from_str(text.trim_end()).unwrap()
}

fn label_group<'a>(m: &'a Map<String, Value>, key: &str) -> &'a Map<String, Value> {
    m.get(key)
        .expect("missing labels group")
        .as_object()
        .expect("labels group is not an object")
}

fn identity_labels() -> BTreeMap<&'static str, &'static str> {
    [
        (HIVE_KEY, HIVE),
        (ACTOR_NAME_KEY, ACTOR),
        (ACTOR_UID_KEY, UID),
        (ACTOR_CONTAINER_NAME_KEY, CONTAINER),
        (TEMPLATE_HIVE_KEY, TEMPLATE_HIVE),
        (TEMPLATE_NAME_KEY, TEMPLATE),
    ]
    .into()
}

fn assert_labels(labels: &Map<String, Value>, want: &BTreeMap<&str, &str>) {
    for (k, v) in want {
        assert_eq!(
            labels.get(*k).and_then(Value::as_str),
            Some(*v),
            "label {k}"
        );
    }
}

#[test]
fn wrap_container_logs_plain_text() {
    let (m, key) = wrap("Test application log output\n", false);
    assert_eq!(m["message"], "Test application log output");
    assert!(!m.contains_key("level"), "plain text gets no guessed level");
    assert_labels(label_group(&m, key), &identity_labels());
}

#[test]
fn wrap_container_logs_json_input() {
    // A large integer and an existing time: neither may be rewritten.
    let input = r#"{"level":"info","msg":"Started container","custom_attr":"value","count":1234567890123456789,"time":"2026-05-16T01:03:37Z"}"#;
    let (m, key) = wrap(&format!("{input}\n"), false);
    assert_eq!(m["msg"], "Started container");
    assert_eq!(m["level"], "info");
    assert_eq!(m["custom_attr"], "value");
    assert_eq!(m["time"], "2026-05-16T01:03:37Z");
    assert_eq!(m["count"].as_u64(), Some(1234567890123456789));
    assert_labels(label_group(&m, key), &identity_labels());
}

/// An Actor joins a trace only by writing the fields itself.
#[test]
fn wrap_container_logs_actor_trace_context_passthrough() {
    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let span_id = "00f067aa0ba902b7";
    let input = format!(
        r#"{{"msg":"handled","{LOG_TRACE_ID_FIELD}":"{trace_id}","{LOG_SPAN_ID_FIELD}":"{span_id}"}}"#
    );
    let (m, _) = wrap(&format!("{input}\n"), false);
    assert_eq!(m[LOG_TRACE_ID_FIELD], trace_id);
    assert_eq!(m[LOG_SPAN_ID_FIELD], span_id);
}

#[test]
fn synced_writer_concurrency() {
    const WORKERS: usize = 10;
    const WRITES: usize = 100;
    const LEN: usize = 10;
    let sw = Arc::new(SyncedWriter::new(Vec::new()));
    let handles: Vec<_> = (0..WORKERS)
        .map(|_| {
            let sw = sw.clone();
            std::thread::spawn(move || {
                for _ in 0..WRITES {
                    let mut line = "a".repeat(LEN);
                    line.push('\n');
                    sw.write_all(line.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let out = Arc::try_unwrap(sw).ok().unwrap().into_inner();
    let text = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), WORKERS * WRITES);
    assert!(lines.iter().all(|l| l.len() == LEN), "interleaved write");
}

#[test]
fn wrap_container_logs_merges_labels() {
    let (m, key) = wrap(
        "{\"level\":\"info\",\"msg\":\"App log\",\"labels\":{\"app\":\"my-app\",\"version\":\"v1\"}}\n",
        false,
    );
    let mut want = identity_labels();
    want.insert("app", "my-app");
    want.insert("version", "v1");
    assert_labels(label_group(&m, key), &want);
}

/// An Actor cannot write into the reserved namespace: neither forged
/// identity labels nor any other `hk.*` key.
#[test]
fn wrap_container_logs_reserved_namespace() {
    let input = format!(
        r#"{{"level":"info","msg":"App log","{ACTOR_NAME_KEY}":"top-level-forgery","hk.tenant":"forged","labels":{{"{ACTOR_NAME_KEY}":"malicious-name","{ACTOR_UID_KEY}":"malicious-uid","hk.tenant":"forged","app":"my-app"}}}}"#
    );
    let (m, key) = wrap(&format!("{input}\n"), false);
    for k in [ACTOR_NAME_KEY, "hk.tenant"] {
        assert!(!m.contains_key(k), "reserved top-level key {k} survived");
    }
    let labels = label_group(&m, key);
    assert!(!labels.contains_key("hk.tenant"));
    let mut want = identity_labels();
    want.insert("app", "my-app");
    assert_labels(labels, &want);
}

/// Both label spellings fold into one sanitized group, whichever this logger
/// writes.
#[test]
fn wrap_container_logs_foreign_label_group() {
    for (on_gce, foreign) in [(true, LABELS_KEY_PLAIN), (false, LABELS_KEY_GCE)] {
        let input = format!(
            r#"{{"msg":"App log","{foreign}":{{"{ACTOR_NAME_KEY}":"forged-name","hk.tenant":"forged","app":"my-app"}}}}"#
        );
        let (m, key) = wrap(&format!("{input}\n"), on_gce);
        assert!(!m.contains_key(foreign), "foreign group {foreign} survived");
        let labels = label_group(&m, key);
        assert!(!labels.contains_key("hk.tenant"));
        let mut want = identity_labels();
        want.insert("app", "my-app");
        assert_labels(labels, &want);
        assert_eq!(labels.len(), want.len(), "{labels:?}");
    }
}

/// Label values are stringified: one non-string value costs a promoted
/// record all its labels.
#[test]
fn wrap_container_logs_non_string_label_value() {
    let (m, key) = wrap(
        r#"{"msg":"App log","labels":{"replicas":3,"enabled":true,"nested":{"a":1},"empty":null}}
"#,
        false,
    );
    let labels = label_group(&m, key);
    assert!(labels.values().all(Value::is_string), "{labels:?}");
    assert_labels(
        labels,
        &[
            ("replicas", "3"),
            ("enabled", "true"),
            ("nested", r#"{"a":1}"#),
            ("empty", ""),
        ]
        .into(),
    );
}

#[test]
fn wrap_container_logs_json_null() {
    let (m, key) = wrap("null\n", false);
    assert_eq!(m["message"], "null");
    assert_labels(label_group(&m, key), &identity_labels());
}

#[test]
fn wrap_container_logs_trailing_garbage() {
    let line = r#"{"count": 1} garbage"#;
    let (m, key) = wrap(&format!("{line}\n"), false);
    assert_eq!(m["message"], line);
    assert_labels(label_group(&m, key), &identity_labels());
}

#[test]
fn emit_lifecycle_log_cases() {
    let trace_id: [u8; 16] = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47,
        0x36,
    ];
    let span_id: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
    let sampled = TraceContext {
        trace_id,
        span_id,
        flags: 1,
    };
    let unsampled = TraceContext {
        flags: 0,
        ..sampled
    };
    let invalid = TraceContext {
        trace_id: [0; 16],
        span_id: [0; 8],
        flags: 0,
    };
    let cases: [(&str, Option<TraceContext>, Option<&str>); 4] = [
        ("no span", None, None),
        ("an invalid span contributes nothing", Some(invalid), None),
        ("sampled span", Some(sampled), Some("01")),
        (
            "unsampled span still correlates",
            Some(unsampled),
            Some("00"),
        ),
    ];
    for (name, trace, flags) in cases {
        let al = ActorLogger::new(Vec::new(), false);
        al.emit_lifecycle_log(trace.as_ref(), "Actor restored", &attribution());
        let key = al.labels_key();
        let m = decode_line(&al.into_inner());
        assert_eq!(m["message"], "Actor restored", "{name}");
        assert!(
            m.get("time").is_some_and(Value::is_string),
            "{name}: no timestamp"
        );
        match flags {
            None => {
                for f in [LOG_TRACE_ID_FIELD, LOG_SPAN_ID_FIELD, LOG_TRACE_FLAGS_FIELD] {
                    assert!(!m.contains_key(f), "{name}: {f} present");
                }
            }
            Some(fl) => {
                assert_eq!(
                    m[LOG_TRACE_ID_FIELD], "4bf92f3577b34da6a3ce929d0e0e4736",
                    "{name}"
                );
                assert_eq!(m[LOG_SPAN_ID_FIELD], "00f067aa0ba902b7", "{name}");
                assert_eq!(m[LOG_TRACE_FLAGS_FIELD], fl, "{name}");
            }
        }
        // A lifecycle record is Actor-scoped: no container produced it.
        let labels = label_group(&m, key);
        assert!(!labels.contains_key(ACTOR_CONTAINER_NAME_KEY), "{name}");
        let mut want = identity_labels();
        want.remove(ACTOR_CONTAINER_NAME_KEY);
        assert_labels(labels, &want);
        assert_eq!(labels.len(), want.len(), "{name}");
    }
}

#[test]
fn labels_key_spellings() {
    assert_eq!(labels_key_for(false), "labels");
    assert_eq!(labels_key_for(true), "logging.googleapis.com/labels");
}

#[test]
fn emit_container_line_matches_the_stream_wrapper() {
    let al = ActorLogger::new(Vec::new(), false);
    al.emit_container_line(b"", &attribution(), CONTAINER);
    al.emit_container_line(br#"{"msg":"hi","level":"warn"}"#, &attribution(), CONTAINER);
    let m = decode_line(&al.into_inner());
    assert_eq!(m["msg"], "hi");
    assert_eq!(m["level"], "warn");
    assert_labels(label_group(&m, LABELS_KEY_PLAIN), &identity_labels());
}
