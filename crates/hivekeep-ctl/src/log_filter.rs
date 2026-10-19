//! Selecting and cleaning an Actor's lines from a worker pod's log stream.
//!
//! A worker pod's stream interleaves the JSON lines of every Actor it has
//! hosted. Each line carries the emitting Actor (hive and name) in its
//! labels; container output also carries the container's name, while the
//! lifecycle events the worker emits around start, checkpoint, restore and
//! termination carry none, which is what tells the two apart.

use std::fmt;
use std::io::Write;

use hivekeep_core::resource::ActorRef;
use serde_json::{Map, Value};

use crate::protojson::sort_keys;
use crate::verbosity::{Logger, TRACE};

/// Label naming the emitting Actor's hive.
pub const HIVE_LABEL: &str = "hk.hive";
/// Label naming the emitting Actor.
pub const ACTOR_NAME_LABEL: &str = "hk.actor.name";
/// Label naming the emitting container; absent on lifecycle events.
pub const CONTAINER_NAME_LABEL: &str = "hk.actor.container.name";
/// Prefix reserved for platform labels. Every label under it is stripped
/// from output, so an Actor cannot have a label of its own printed as if it
/// were platform attribution.
pub const RESERVED_PREFIX: &str = "hk.";
/// The pseudo-container the sandbox's own diagnostics are attributed to.
pub const SANDBOX_CONTAINER: &str = "_sandbox";

/// Keys under which a line may carry its labels.
const LABEL_KEYS: [&str; 2] = ["logging.googleapis.com/labels", "labels"];

/// Which class of lines `--source` selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSource {
    /// Everything.
    All,
    /// Container output only.
    Containers,
    /// Lifecycle events only.
    Lifecycle,
}

impl fmt::Display for LogSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LogSource::All => "all",
            LogSource::Containers => "containers",
            LogSource::Lifecycle => "lifecycle",
        })
    }
}

/// Selects the lines to display: all of an Actor's, one class of them, or
/// only one container's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilter {
    target: ActorRef,
    source: LogSource,
    container: String,
}

impl LogFilter {
    /// Builds the filter from the `--source` and `--container` values,
    /// rejecting combinations that could never match.
    pub fn new(target: ActorRef, source: &str, container: &str) -> Result<Self, String> {
        let source = match source {
            "all" => LogSource::All,
            "containers" => LogSource::Containers,
            "lifecycle" => {
                if !container.is_empty() {
                    return Err("--container cannot be combined with --source=lifecycle: \
                                lifecycle events are not emitted by a container"
                        .into());
                }
                LogSource::Lifecycle
            }
            other => {
                return Err(format!(
                    "invalid --source {other:?}: must be one of all, containers, lifecycle"
                ))
            }
        };
        Ok(LogFilter {
            target,
            source,
            container: container.to_string(),
        })
    }

    /// The Actor whose lines are selected.
    pub fn target(&self) -> &ActorRef {
        &self.target
    }

    /// Whether a line from `emitter`'s `container` (empty for a lifecycle
    /// event) is selected.
    pub fn matches(&self, emitter: &ActorRef, container: &str) -> bool {
        // Actor names are unique only within a hive, and a worker pod may
        // host Actors of several hives over time, so match on both.
        if emitter != &self.target || self.target.hive.is_empty() || self.target.name.is_empty() {
            return false;
        }
        match self.source {
            LogSource::Lifecycle => return container.is_empty(),
            LogSource::Containers if container.is_empty() => return false,
            _ => {}
        }
        if self.container.is_empty() {
            container != SANDBOX_CONTAINER
        } else {
            container == self.container
        }
    }
}

fn labels_of(m: &Map<String, Value>) -> Option<&Map<String, Value>> {
    LABEL_KEYS.iter().find_map(|k| {
        m.get(*k).and_then(Value::as_object).filter(|l| {
            l.get(ACTOR_NAME_LABEL)
                .and_then(Value::as_str)
                .is_some_and(|n| !n.is_empty())
        })
    })
}

/// The cleaned line for display, or `None` when `filter` does not select
/// `line`. Platform labels are removed, keys are sorted, and `time` leads.
pub fn render_line(line: &str, filter: &LogFilter, log: &Logger) -> Option<String> {
    // Under lifecycle nearly every line is a container's, and a busy Actor
    // writes many, so skip them before decoding. Only a container line
    // carries this key; an Actor cannot forge it, since the platform strips
    // reserved keys from an Actor's own labels.
    if filter.source == LogSource::Lifecycle
        && line.contains(&format!("\"{CONTAINER_NAME_LABEL}\""))
    {
        return None;
    }
    let mut m = match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(m)) => m,
        Ok(_) => {
            log.log(TRACE, "Skipping non-object log line");
            return None;
        }
        Err(e) => {
            log.log(TRACE, format!("Skipping non-JSON log line: {e}"));
            return None;
        }
    };

    let (emitter, container) = match labels_of(&m) {
        Some(labels) => {
            let s = |k: &str| {
                labels
                    .get(k)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            (
                ActorRef::new(s(HIVE_LABEL), s(ACTOR_NAME_LABEL)),
                s(CONTAINER_NAME_LABEL),
            )
        }
        None => (ActorRef::new("", ""), String::new()),
    };
    if !filter.matches(&emitter, &container) {
        return None;
    }

    for key in LABEL_KEYS {
        if let Some(Value::Object(labels)) = m.get_mut(key) {
            labels.retain(|k, _| !k.starts_with(RESERVED_PREFIX));
            if labels.is_empty() {
                m.remove(key);
            }
        }
    }
    let time = m.remove("time");
    let body = sort_keys(Value::Object(m)).to_string();
    Some(match time {
        None => body,
        Some(t) if body == "{}" => format!("{{\"time\":{t}}}"),
        Some(t) => format!("{{\"time\":{t},{}", &body[1..]),
    })
}

/// Writes `line` to `out` if `filter` selects it, and reports whether it did.
pub fn display_line(line: &str, filter: &LogFilter, out: &mut dyn Write, log: &Logger) -> bool {
    match render_line(line, filter, log) {
        Some(text) => writeln!(out, "{text}").is_ok(),
        None => false,
    }
}

/// Splits the RFC 3339 timestamp kubectl prefixes to a line with
/// `--timestamps` from the line itself. A line without one is returned
/// whole, with `None`.
pub fn split_timestamp(line: &str) -> (Option<chrono::DateTime<chrono::FixedOffset>>, &str) {
    if let Some((stamp, rest)) = line.split_once(' ') {
        if let Ok(t) = chrono::DateTime::parse_from_rfc3339(stamp) {
            return (Some(t), rest);
        }
    }
    (None, line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::SharedBuf;

    fn target() -> ActorRef {
        ActorRef::new("space-1", "act-1")
    }

    fn labels(hive: Option<&str>, name: &str, container: Option<&str>, extra: &str) -> String {
        let mut parts = Vec::new();
        if let Some(h) = hive {
            parts.push(format!("\"hk.hive\":\"{h}\""));
        }
        parts.push(format!("\"hk.actor.name\":\"{name}\""));
        if let Some(c) = container {
            parts.push(format!("\"hk.actor.container.name\":\"{c}\""));
        }
        if !extra.is_empty() {
            parts.push(extra.to_string());
        }
        format!("{{{}}}", parts.join(","))
    }

    fn line(fields: &str, labels_json: &str) -> String {
        format!("{{{fields},\"logging.googleapis.com/labels\":{labels_json}}}")
    }

    fn check(line: &str, target: ActorRef, source: &str, container: &str, want: Option<&str>) {
        let f = LogFilter::new(target, source, container).unwrap();
        let mut buf = SharedBuf::default();
        let (sink, err) = crate::io::Sink::buffer();
        let shown = display_line(line, &f, &mut buf, &Logger::new(1, sink));
        assert_eq!(shown, want.is_some(), "{line}");
        assert_eq!(buf.contents().trim(), want.unwrap_or(""), "{line}");
        assert_eq!(err.contents(), "");
    }

    #[test]
    fn matching_and_cleaning() {
        let own = labels(Some("space-1"), "act-1", None, "");
        check(
            &line(
                r#""time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count""#,
                &own,
            ),
            target(),
            "all",
            "",
            Some(r#"{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}"#),
        );
        check(
            &line(r#""level":"error","msg":"Failed""#, &own),
            target(),
            "all",
            "",
            Some(r#"{"level":"error","msg":"Failed"}"#),
        );
        // The plain "labels" key works too.
        check(
            &format!(r#"{{"time":"2026-05-16T01:03:38Z","msg":"Count","labels":{own}}}"#),
            target(),
            "all",
            "",
            Some(r#"{"time":"2026-05-16T01:03:38Z","msg":"Count"}"#),
        );
        // Other keys are sorted after time.
        check(
            &line(
                r#""time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","traceID":"abc-123","err":"timeout""#,
                &own,
            ),
            target(),
            "all",
            "",
            Some(
                r#"{"time":"2026-05-16T01:03:38Z","err":"timeout","level":"info","msg":"Hello","traceID":"abc-123"}"#,
            ),
        );
        check(
            &line(r#""message":"login failed","code":401"#, &own),
            target(),
            "all",
            "",
            Some(r#"{"code":401,"message":"login failed"}"#),
        );
        // Application labels stay; reserved ones go, forged or not.
        check(
            &line(
                r#""time":"2026-05-16T01:03:38Z","msg":"Hello""#,
                &labels(
                    Some("space-1"),
                    "act-1",
                    None,
                    r#""hk.tenant":"forged","app":"my-app""#,
                ),
            ),
            target(),
            "all",
            "",
            Some(
                r#"{"time":"2026-05-16T01:03:38Z","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}"#,
            ),
        );
    }

    #[test]
    fn non_matching() {
        let msg = r#""time":"2026-05-16T01:03:38Z","message":"Hello world""#;
        check(
            &line(msg, &labels(Some("space-1"), "act-2", None, "")),
            target(),
            "all",
            "",
            None,
        );
        check(
            &line(msg, &labels(Some("space-2"), "act-1", None, "")),
            target(),
            "all",
            "",
            None,
        );
        check(
            &line(msg, &labels(None, "act-1", None, "")),
            target(),
            "all",
            "",
            None,
        );
        check(
            &line(msg, &labels(Some(""), "act-1", None, "")),
            ActorRef::new("", "act-1"),
            "all",
            "",
            None,
        );
        check("not a json line", target(), "all", "", None);
    }

    #[test]
    fn containers_and_sources() {
        let msg = r#""time":"2026-05-16T01:03:38Z","msg":"hi""#;
        let want = Some(r#"{"time":"2026-05-16T01:03:38Z","msg":"hi"}"#);
        let counter = line(msg, &labels(Some("space-1"), "act-1", Some("counter"), ""));
        let sidecar = line(msg, &labels(Some("space-1"), "act-1", Some("sidecar"), ""));
        let sandbox = line(msg, &labels(Some("space-1"), "act-1", Some("_sandbox"), ""));
        let lifecycle = line(msg, &labels(Some("space-1"), "act-1", None, ""));
        let other_lifecycle = line(msg, &labels(Some("space-1"), "act-2", None, ""));

        check(&counter, target(), "all", "", want);
        check(&counter, target(), "all", "counter", want);
        check(&sidecar, target(), "all", "counter", None);
        check(&sandbox, target(), "all", "", None);
        check(&sandbox, target(), "all", "_sandbox", want);
        check(&sandbox, target(), "all", "counter", None);
        check(&lifecycle, target(), "all", "", want);
        check(&lifecycle, target(), "all", "counter", None);
        check(&lifecycle, target(), "lifecycle", "", want);
        check(&counter, target(), "lifecycle", "", None);
        check(&other_lifecycle, target(), "lifecycle", "", None);
        check(&sidecar, target(), "containers", "", want);
        check(&lifecycle, target(), "containers", "", None);
        check(&sidecar, target(), "containers", "counter", None);
    }

    /// One line through `display_line`: the target, `--source` (empty
    /// meaning the default, `all`), `--container`, and what is shown.
    struct Case {
        name: &'static str,
        line: &'static str,
        target: (&'static str, &'static str),
        source: &'static str,
        container: &'static str,
        want: Option<&'static str>,
    }

    const S1: (&str, &str) = ("space-1", "act-1");

    #[test]
    fn display_cases() {
        let cases = [
            Case {
                name: "matching actor, JSON log with RFC3339Nano",
                line: r#"{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}"#,
                ),
            },
            Case {
                name: "matching actor, plain text log",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","message":"Hello"}"#),
            },
            Case {
                name: "matching actor, JSON log with no timestamp",
                line: r#"{"level":"error","msg":"Failed","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(r#"{"level":"error","msg":"Failed"}"#),
            },
            Case {
                name: "matching actor, fallback to standard labels key",
                line: r#"{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}"#,
                ),
            },
            Case {
                name: "non-matching actor",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-2"}}"#,
                target: S1,
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "same actor name in a different hive",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"hk.hive":"space-2","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "matching actor name without hive label",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "empty target hive does not match empty hive label",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"hk.hive":"","hk.actor.name":"act-1"}}"#,
                target: ("", "act-1"),
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "empty target actor name does not match empty name label",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":""}}"#,
                target: ("space-1", ""),
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "invalid json line",
                line: "not a json line",
                target: S1,
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "matching actor, flat JSON log",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","traceID":"abc-123","err":"timeout","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38Z","err":"timeout","level":"info","msg":"Hello","traceID":"abc-123"}"#,
                ),
            },
            Case {
                name: "matching actor, severity and message keys",
                line: r#"{"time":"2026-05-16T01:03:38Z","severity":"error","message":"Disk full","custom_tag":"alert","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38Z","custom_tag":"alert","message":"Disk full","severity":"error"}"#,
                ),
            },
            Case {
                name: "matching actor, 2-field structured log without time",
                line: r#"{"message":"login failed","code":401,"logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(r#"{"code":401,"message":"login failed"}"#),
            },
            Case {
                name: "matching actor, JSON log with custom application labels",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","app":"my-app"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38Z","level":"info","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}"#,
                ),
            },
            Case {
                // The worker drops these when it writes the line; they are
                // stripped here too, so one that got in some other way is
                // never printed as platform attribution.
                name: "matching actor, label in the reserved namespace is stripped",
                line: r#"{"time":"2026-05-16T01:03:38Z","msg":"Hello","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.tenant":"forged","app":"my-app"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38Z","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}"#,
                ),
            },
            Case {
                name: "no filter shows a container line",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"counter"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}"#),
            },
            Case {
                name: "container filter matches the named container",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"counter"}}"#,
                target: S1,
                source: "",
                container: "counter",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}"#),
            },
            Case {
                name: "no filter excludes sandbox diagnostics",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _sandbox]","labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"_sandbox"}}"#,
                target: S1,
                source: "",
                container: "",
                want: None,
            },
            Case {
                name: "explicit sandbox filter shows runsc diagnostics",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _sandbox]","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"_sandbox"}}"#,
                target: S1,
                source: "",
                container: "_sandbox",
                want: Some(
                    r#"{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _sandbox]"}"#,
                ),
            },
            Case {
                name: "application filter excludes sandbox diagnostics",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _sandbox]","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"_sandbox"}}"#,
                target: S1,
                source: "",
                container: "counter",
                want: None,
            },
            Case {
                name: "no filter shows lifecycle events",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started","labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started"}"#),
            },
            Case {
                name: "container filter excludes a different container",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"sidecar"}}"#,
                target: S1,
                source: "",
                container: "counter",
                want: None,
            },
            Case {
                name: "container filter excludes a lifecycle event",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "",
                container: "counter",
                want: None,
            },
            Case {
                name: "source=all shows a lifecycle event",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "all",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started"}"#),
            },
            Case {
                name: "source=lifecycle shows a lifecycle event",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor checkpointing","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "lifecycle",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","message":"Actor checkpointing"}"#),
            },
            Case {
                name: "source=lifecycle excludes a container line",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"counter"}}"#,
                target: S1,
                source: "lifecycle",
                container: "",
                want: None,
            },
            Case {
                name: "source=lifecycle excludes another actor's lifecycle event",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-2"}}"#,
                target: S1,
                source: "lifecycle",
                container: "",
                want: None,
            },
            Case {
                name: "source=containers shows any container's line",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"sidecar"}}"#,
                target: S1,
                source: "containers",
                container: "",
                want: Some(r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}"#),
            },
            Case {
                name: "source=containers excludes a lifecycle event",
                line: r#"{"time":"2026-05-16T01:03:38Z","message":"Actor restored","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1"}}"#,
                target: S1,
                source: "containers",
                container: "",
                want: None,
            },
            Case {
                name: "source=containers with container filter keeps only the named container",
                line: r#"{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"hk.hive":"space-1","hk.actor.name":"act-1","hk.actor.container.name":"sidecar"}}"#,
                target: S1,
                source: "containers",
                container: "counter",
                want: None,
            },
        ];
        for c in cases {
            let source = if c.source.is_empty() { "all" } else { c.source };
            let f = LogFilter::new(ActorRef::new(c.target.0, c.target.1), source, c.container)
                .unwrap_or_else(|e| panic!("{}: {e}", c.name));
            let mut buf = SharedBuf::default();
            let (sink, err) = crate::io::Sink::buffer();
            let shown = display_line(c.line, &f, &mut buf, &Logger::new(1, sink));
            assert_eq!(shown, c.want.is_some(), "{}", c.name);
            assert_eq!(buf.contents().trim(), c.want.unwrap_or(""), "{}", c.name);
            assert_eq!(err.contents(), "", "{}", c.name);
        }
    }

    #[test]
    fn filter_flags_build_the_filter() {
        for (source, container, want) in [
            ("all", "", LogSource::All),
            ("containers", "", LogSource::Containers),
            ("lifecycle", "", LogSource::Lifecycle),
            ("all", "counter", LogSource::All),
            ("containers", "counter", LogSource::Containers),
        ] {
            let f = LogFilter::new(target(), source, container).unwrap();
            assert_eq!(
                f,
                LogFilter {
                    target: target(),
                    source: want,
                    container: container.to_string(),
                }
            );
        }
    }

    #[test]
    fn split_kubelet_timestamp_cases() {
        let (t, rest) = split_timestamp(r#"2026-05-16T01:03:40.300000000Z {"msg":"hi"}"#);
        assert_eq!(rest, r#"{"msg":"hi"}"#);
        assert_eq!(
            t.unwrap(),
            chrono::DateTime::parse_from_rfc3339("2026-05-16T01:03:40.3Z").unwrap()
        );
        for line in [
            r#"{"msg":"hi there"}"#,
            "plain",
            r#"not-a-time {"msg":"hi"}"#,
        ] {
            assert_eq!(split_timestamp(line), (None, line));
        }
    }

    #[test]
    fn filter_flags() {
        for (source, container, err) in [
            ("", "", Some("invalid --source \"\"")),
            ("all", "", None),
            ("containers", "", None),
            ("lifecycle", "", None),
            ("all", "counter", None),
            ("containers", "counter", None),
            (
                "lifecycle",
                "counter",
                Some("--container cannot be combined with --source=lifecycle"),
            ),
            ("supervisor", "", Some("invalid --source \"supervisor\"")),
            ("Lifecycle", "", Some("invalid --source \"Lifecycle\"")),
        ] {
            match (LogFilter::new(target(), source, container), err) {
                (Ok(_), None) => {}
                (Err(e), Some(w)) => assert!(e.contains(w), "{e}"),
                (got, want) => panic!("{source}/{container}: {got:?} vs {want:?}"),
            }
        }
    }

    #[test]
    fn timestamps() {
        let (t, rest) = split_timestamp("2026-05-16T01:03:38.602878302Z {\"msg\":\"x\"}");
        assert_eq!(rest, "{\"msg\":\"x\"}");
        assert_eq!(t.unwrap().timestamp_subsec_nanos(), 602_878_302);
        assert_eq!(
            split_timestamp("{\"msg\":\"x y\"}"),
            (None, "{\"msg\":\"x y\"}")
        );
        assert_eq!(split_timestamp("nospace"), (None, "nospace"));
        assert_eq!(split_timestamp("2026-13-99 x"), (None, "2026-13-99 x"));
    }
}
