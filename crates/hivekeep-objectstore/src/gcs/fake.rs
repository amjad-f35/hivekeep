//! An in-process GCS for tests: uploads (single and resumable), XML reads,
//! compose, rewrite, delete and list over in-memory buckets, with scripted
//! 429s, held uploads and request counting.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Method;
use parking_lot::Mutex;
use tokio::sync::watch;
use url::Url;

use super::{GcsClient, GcsConn};
use crate::gcp::Anonymous;
use crate::http::{transport_fn, Request, Response, Transport, TransportError};

pub const BASE: &str = "http://gcs.test/";

#[derive(Default)]
struct State {
    objects: BTreeMap<String, Bytes>,
    fail: HashMap<String, usize>,
    attempts: HashMap<String, usize>,
    sessions: HashMap<String, (String, Vec<u8>)>,
    next_session: usize,
    delete_delay: Option<Duration>,
}

/// The fake.
#[derive(Clone)]
pub struct FakeGcs {
    state: Arc<Mutex<State>>,
    gate: Arc<watch::Sender<bool>>,
    held: Arc<AtomicUsize>,
    done: Arc<AtomicUsize>,
    deletes_in_flight: Arc<AtomicUsize>,
    peak_deletes: Arc<AtomicUsize>,
}

fn json_error(code: u16) -> Response {
    Response::new(
        code,
        format!(r#"{{"error":{{"code":{code},"message":"error","errors":[{{"reason":"rateLimitExceeded"}}]}}}}"#),
    )
    .with_header("content-type", "application/json")
}

fn unescape(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

impl FakeGcs {
    pub fn new() -> Self {
        let (gate, _) = watch::channel(true);
        FakeGcs {
            state: Arc::default(),
            gate: Arc::new(gate),
            held: Arc::default(),
            done: Arc::default(),
            deletes_in_flight: Arc::default(),
            peak_deletes: Arc::default(),
        }
    }

    /// Answers the first `n` requests of `kind` with 429.
    pub fn fail(&self, kind: &str, n: usize) {
        self.state.lock().fail.insert(kind.to_string(), n);
    }

    pub fn attempts(&self, kind: &str) -> usize {
        self.state.lock().attempts.get(kind).copied().unwrap_or(0)
    }

    pub fn set_delete_delay(&self, d: Duration) {
        self.state.lock().delete_delay = Some(d);
    }

    pub fn peak_deletes(&self) -> usize {
        self.peak_deletes.load(Ordering::SeqCst)
    }

    /// Holds data-carrying upload requests until released.
    pub fn hold_uploads(&self) {
        self.gate.send_replace(false);
    }

    pub fn release_uploads(&self) {
        self.gate.send_replace(true);
    }

    /// Data-carrying upload requests received.
    pub fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    /// Data-carrying upload requests completed.
    pub fn uploads_done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    pub fn insert(&self, bucket: &str, object: &str, data: Vec<u8>) {
        self.state
            .lock()
            .objects
            .insert(format!("{bucket}/{object}"), data.into());
    }

    pub fn get(&self, bucket: &str, object: &str) -> Option<Bytes> {
        self.state
            .lock()
            .objects
            .get(&format!("{bucket}/{object}"))
            .cloned()
    }

    pub fn keys(&self) -> Vec<String> {
        self.state.lock().objects.keys().cloned().collect()
    }

    pub fn transport(&self) -> Arc<dyn Transport> {
        let me = self.clone();
        transport_fn(move |req| {
            let me = me.clone();
            Box::pin(async move { me.serve(req).await })
        })
    }

    pub fn conn(&self) -> GcsConn {
        GcsConn::new(
            self.transport(),
            Arc::new(Anonymous),
            Url::parse(BASE).unwrap(),
        )
    }

    /// A client with no pool.
    pub fn client(&self) -> GcsClient {
        GcsClient::from_parts(self.conn(), vec![])
    }

    fn count(&self, kind: &str) -> bool {
        let mut st = self.state.lock();
        *st.attempts.entry(kind.to_string()).or_default() += 1;
        let left = st.fail.entry(kind.to_string()).or_default();
        if *left > 0 {
            if *left != usize::MAX {
                *left -= 1;
            }
            return true;
        }
        false
    }

    async fn gated(&self) {
        self.held.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.gate.subscribe();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    async fn serve(&self, req: Request) -> Result<Response, TransportError> {
        let path = req.url.path().to_string();
        let ok = |body: &str| {
            Response::new(200, body.to_string()).with_header("content-type", "application/json")
        };
        // Uploads.
        if let Some(rest) = path.strip_prefix("/upload/storage/v1/b/") {
            if self.count("upload") {
                return Ok(json_error(429));
            }
            let bucket = unescape(rest.trim_end_matches("/o"));
            let name = req.query("name").unwrap_or_default();
            if req.query("uploadType").as_deref() == Some("resumable") {
                let mut st = self.state.lock();
                st.next_session += 1;
                let id = st.next_session.to_string();
                st.sessions
                    .insert(id.clone(), (format!("{bucket}/{name}"), Vec::new()));
                return Ok(Response::new(200, "")
                    .with_header("location", format!("{BASE}upload-session/{id}")));
            }
            self.gated().await;
            self.state
                .lock()
                .objects
                .insert(format!("{bucket}/{name}"), req.body.clone());
            return Ok(ok(&format!(r#"{{"name":"{name}"}}"#)));
        }
        if let Some(id) = path.strip_prefix("/upload-session/") {
            if self.count("upload") {
                return Ok(json_error(429));
            }
            self.gated().await;
            let range = req
                .header_str("content-range")
                .unwrap_or_default()
                .to_string();
            let total = range.rsplit('/').next().unwrap_or("*").to_string();
            let mut st = self.state.lock();
            let Some((key, data)) = st.sessions.get_mut(id) else {
                return Ok(Response::new(404, ""));
            };
            if !range.starts_with("bytes */") {
                let start: usize = range
                    .trim_start_matches("bytes ")
                    .split('-')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                data.truncate(start);
                data.extend_from_slice(&req.body);
            }
            if total == "*" {
                return Ok(Response::new(308, ""));
            }
            let (key, data) = (key.clone(), std::mem::take(data));
            st.sessions.remove(id);
            st.objects.insert(key, data.into());
            return Ok(ok("{}"));
        }
        if let Some(rest) = path.strip_prefix("/storage/v1/b/") {
            let (bucket, rest) = rest.split_once('/').unwrap_or((rest, ""));
            let bucket = unescape(bucket);
            if rest == "o" && req.method == Method::GET {
                if self.count("list") {
                    return Ok(json_error(429));
                }
                let prefix = req.query("prefix").unwrap_or_default();
                let lead = format!("{bucket}/");
                let items: Vec<String> = self
                    .keys()
                    .into_iter()
                    .filter_map(|k| k.strip_prefix(&lead).map(str::to_string))
                    .filter(|n| n.starts_with(&prefix))
                    .map(|n| format!(r#"{{"name":{}}}"#, serde_json::to_string(&n).unwrap()))
                    .collect();
                return Ok(ok(&format!(r#"{{"items":[{}]}}"#, items.join(","))));
            }
            let Some(obj) = rest.strip_prefix("o/") else {
                return Ok(Response::new(400, ""));
            };
            if req.method == Method::POST && obj.ends_with("/compose") {
                if self.count("compose") {
                    return Ok(json_error(429));
                }
                let dest = unescape(obj.trim_end_matches("/compose"));
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let sources = body["sourceObjects"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if sources.len() > super::write::MAX_COMPOSE_SOURCES {
                    return Ok(Response::new(400, "bad compose"));
                }
                let mut st = self.state.lock();
                let mut data = Vec::new();
                for s in sources {
                    let name = s["name"].as_str().unwrap_or_default();
                    match st.objects.get(&format!("{bucket}/{name}")) {
                        Some(b) => data.extend_from_slice(b),
                        None => return Ok(json_error(404)),
                    }
                }
                st.objects.insert(format!("{bucket}/{dest}"), data.into());
                return Ok(ok("{}"));
            }
            if req.method == Method::POST && obj.contains("/rewriteTo/b/") {
                if self.count("copy") {
                    return Ok(json_error(429));
                }
                let (src, dst) = obj.split_once("/rewriteTo/b/").unwrap();
                let (dst_bucket, dst) = dst.split_once("/o/").unwrap_or((dst, ""));
                let mut st = self.state.lock();
                let Some(data) = st
                    .objects
                    .get(&format!("{bucket}/{}", unescape(src)))
                    .cloned()
                else {
                    return Ok(json_error(404));
                };
                st.objects
                    .insert(format!("{}/{}", unescape(dst_bucket), unescape(dst)), data);
                return Ok(ok(r#"{"done":true}"#));
            }
            if req.method == Method::DELETE {
                let n = self.deletes_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_deletes.fetch_max(n, Ordering::SeqCst);
                let delay = self.state.lock().delete_delay;
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                self.deletes_in_flight.fetch_sub(1, Ordering::SeqCst);
                if self.count("delete") {
                    return Ok(json_error(429));
                }
                let found = self
                    .state
                    .lock()
                    .objects
                    .remove(&format!("{bucket}/{}", unescape(obj)));
                return Ok(if found.is_some() {
                    Response::new(204, "")
                } else {
                    json_error(404)
                });
            }
        }
        // XML reads: /<bucket>/<object>.
        if req.method == Method::GET {
            if self.count("get") {
                return Ok(json_error(429));
            }
            let key = unescape(path.trim_start_matches('/'));
            let Some(data) = self.state.lock().objects.get(&key).cloned() else {
                return Ok(Response::new(404, "<Error><Code>NoSuchKey</Code></Error>"));
            };
            let range = req
                .header_str("range")
                .unwrap_or_default()
                .trim_start_matches("bytes=")
                .to_string();
            let (from, to) = range.split_once('-').unwrap_or(("0", ""));
            let from: usize = from.parse().unwrap_or(0);
            if data.is_empty() {
                return Ok(Response::new(416, ""));
            }
            let to: usize = to
                .parse::<usize>()
                .unwrap_or(data.len() - 1)
                .min(data.len() - 1);
            return Ok(Response::new(206, data.slice(from..=to))
                .with_header("content-range", format!("bytes {from}-{to}/{}", data.len())));
        }
        Ok(Response::new(
            400,
            format!("unexpected request {} {path}", req.method),
        ))
    }
}
