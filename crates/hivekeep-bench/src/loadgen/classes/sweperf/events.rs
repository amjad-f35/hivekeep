//! The structured lines a session logs about its cycles and tasks. Each
//! goes to `tracing` with typed fields; tests also capture them on the
//! current thread to check what a script reading the log would see.

/// Logs `msg` at `level` with the named fields. A field set to `None` is
/// left out of the line.
macro_rules! event {
    ($lvl:ident, $msg:literal $(, $k:ident = $v:expr)* $(,)?) => {{
        $(let $k = $v;)*
        tracing::$lvl!($($k,)* $msg);
        #[cfg(test)]
        $crate::loadgen::classes::sweperf::events::capture(
            $msg,
            vec![$((stringify!($k), serde_json::json!($k))),*],
        );
    }};
}
pub(crate) use event;

#[cfg(test)]
pub use sink::{capture, install, Guard};

#[cfg(test)]
mod sink {
    use std::cell::RefCell;

    use serde_json::{Map, Value};

    use super::super::test_support::Logs;

    thread_local! {
        static SINK: RefCell<Option<Logs>> = const { RefCell::new(None) };
    }

    /// Records a line into this thread's capture, if one is installed.
    /// Numbers are kept as floats, as a JSON reader sees them.
    pub fn capture(msg: &str, fields: Vec<(&str, Value)>) {
        SINK.with(|s| {
            if let Some(logs) = &*s.borrow() {
                let mut m = Map::new();
                m.insert("msg".into(), Value::String(msg.into()));
                for (k, v) in fields {
                    let v = match v {
                        Value::Null => continue,
                        Value::Number(n) => serde_json::json!(n.as_f64().unwrap_or_default()),
                        other => other,
                    };
                    m.insert(k.into(), v);
                }
                logs.push(m);
            }
        });
    }

    /// Removes the capture when dropped.
    pub struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            SINK.with(|s| *s.borrow_mut() = None);
        }
    }

    /// Captures this thread's lines into `logs` until the guard drops.
    pub fn install(logs: Logs) -> Guard {
        SINK.with(|s| *s.borrow_mut() = Some(logs));
        Guard
    }
}
