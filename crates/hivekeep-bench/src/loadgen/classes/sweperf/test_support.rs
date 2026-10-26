//! Stand-ins for the sweperf tests: a Control API fake, an async HTTP
//! sandbox, log capture and metric readers.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use hivekeep_proto::api::v1 as api;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use serde_json::{Map, Value};
use tonic::{Request, Response, Status};

use crate::loadgen::control::ControlApi;
use crate::loadgen::dynconfig::Holder;
use crate::loadgen::metrics;
use crate::loadgen::trace::Tracer;
use crate::loadgen::userclass::Config;
use crate::wire::SERVER_ELAPSED_HEADER;

use super::knobs::Knobs;

/// A hook run at the start of each DeleteActor.
pub type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Records calls by name; each method fails with its configured error.
#[derive(Default)]
pub struct FakeControl {
    pub s: Mutex<FakeState>,
}

#[derive(Default)]
pub struct FakeState {
    pub calls: Vec<String>,
    pub create_hive_err: Option<Status>,
    pub create_actor_err: Option<Status>,
    pub resume_err: Option<Status>,
    pub suspend_err: Option<Status>,
    pub delete_err: Option<Status>,
    pub suspend_reqs: Vec<api::SuspendActorRequest>,
    /// Makes ResumeActor take this long.
    pub resume_delay: Duration,
    /// The server-elapsed trailer every ResumeActor carries.
    pub resume_elapsed_us: Option<String>,
    pub before_delete: Option<Hook>,
}

impl FakeControl {
    pub fn with<R>(&self, f: impl FnOnce(&mut FakeState) -> R) -> R {
        f(&mut self.s.lock())
    }

    pub fn calls(&self) -> Vec<String> {
        self.s.lock().calls.clone()
    }

    pub fn count(&self, call: &str) -> usize {
        self.s.lock().calls.iter().filter(|c| *c == call).count()
    }
}

#[async_trait]
impl ControlApi for FakeControl {
    async fn create_hive(
        &self,
        _req: Request<api::CreateHiveRequest>,
    ) -> Result<Response<api::Hive>, Status> {
        let mut s = self.s.lock();
        s.calls.push("CreateHive".into());
        match s.create_hive_err.clone() {
            Some(e) => Err(e),
            None => Ok(Response::new(api::Hive::default())),
        }
    }

    async fn create_actor(
        &self,
        _req: Request<api::CreateActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let mut s = self.s.lock();
        s.calls.push("CreateActor".into());
        match s.create_actor_err.clone() {
            Some(e) => Err(e),
            None => Ok(Response::new(api::Actor::default())),
        }
    }

    async fn resume_actor(
        &self,
        _req: Request<api::ResumeActorRequest>,
    ) -> Result<Response<api::ResumeActorResponse>, Status> {
        let delay = {
            let mut s = self.s.lock();
            s.calls.push("ResumeActor".into());
            s.resume_delay
        };
        tokio::time::sleep(delay).await;
        let s = self.s.lock();
        let elapsed = s.resume_elapsed_us.clone();
        match s.resume_err.clone() {
            Some(mut e) => {
                if let Some(us) = elapsed {
                    e.metadata_mut()
                        .insert(SERVER_ELAPSED_HEADER, us.parse().unwrap());
                }
                Err(e)
            }
            None => {
                let mut r = Response::new(api::ResumeActorResponse::default());
                if let Some(us) = elapsed {
                    r.metadata_mut()
                        .insert(SERVER_ELAPSED_HEADER, us.parse().unwrap());
                }
                Ok(r)
            }
        }
    }

    async fn suspend_actor(
        &self,
        req: Request<api::SuspendActorRequest>,
    ) -> Result<Response<api::SuspendActorResponse>, Status> {
        let mut s = self.s.lock();
        s.calls.push("SuspendActor".into());
        s.suspend_reqs.push(req.into_inner());
        match s.suspend_err.clone() {
            Some(e) => Err(e),
            None => Ok(Response::new(api::SuspendActorResponse::default())),
        }
    }

    async fn delete_actor(
        &self,
        _req: Request<api::DeleteActorRequest>,
    ) -> Result<Response<api::Actor>, Status> {
        let hook = self.s.lock().before_delete.clone();
        if let Some(h) = hook {
            h().await;
        }
        let mut s = self.s.lock();
        s.calls.push("DeleteActor".into());
        match s.delete_err.clone() {
            Some(e) => Err(e),
            None => Ok(Response::new(api::Actor::default())),
        }
    }

    async fn get_actor_template(
        &self,
        _req: Request<api::GetActorTemplateRequest>,
    ) -> Result<Response<api::ActorTemplate>, Status> {
        Err(Status::unimplemented("not faked"))
    }
}

/// A request as the sandbox sees it.
#[derive(Debug, Clone)]
pub struct Req {
    pub path: String,
    pub query: HashMap<String, String>,
    pub raw_query: String,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

/// What the sandbox answers.
pub struct Resp {
    pub status: u16,
    pub body: String,
}

impl Resp {
    pub fn json(v: Value) -> Self {
        Resp {
            status: 200,
            body: v.to_string(),
        }
    }
    pub fn text(body: &str) -> Self {
        Resp {
            status: 200,
            body: body.into(),
        }
    }
    pub fn status(status: u16, body: &str) -> Self {
        Resp {
            status,
            body: body.into(),
        }
    }
}

pub type Handler = Arc<dyn Fn(Req) -> BoxFuture<'static, Resp> + Send + Sync>;

/// A sandbox stand-in whose handler may wait. Dropping it stops it.
pub struct Sandbox {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Sandbox {
    pub async fn start(handler: Handler) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let handler = handler.clone();
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(|b| b.to_bytes())
                                .unwrap_or_default();
                            let raw_query = parts.uri.query().unwrap_or("").to_string();
                            let query = url::form_urlencoded::parse(raw_query.as_bytes())
                                .into_owned()
                                .collect();
                            let r = handler(Req {
                                path: parts.uri.path().to_string(),
                                query,
                                raw_query,
                                headers: parts.headers,
                                body,
                            })
                            .await;
                            let mut resp = hyper::Response::new(Full::new(Bytes::from(r.body)));
                            *resp.status_mut() = http::StatusCode::from_u16(r.status).unwrap();
                            Ok::<_, Infallible>(resp)
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Sandbox {
            url: format!("http://{addr}"),
            task,
        }
    }
}

/// A handler from a synchronous closure.
pub fn sync_handler(f: impl Fn(Req) -> Resp + Send + Sync + 'static) -> Handler {
    let f = Arc::new(f);
    Arc::new(move |r| {
        let f = f.clone();
        Box::pin(async move { f(r) })
    })
}

/// A handler that answers nothing but 404.
pub fn not_found() -> Handler {
    sync_handler(|_| Resp::status(404, "404 page not found"))
}

/// Answers liveness, accepts every `/execute` and reports the job done
/// with 1200 ms of container time.
pub fn async_job_handler() -> Handler {
    sync_handler(|r| match (r.path.as_str(), r.query.get("job_id")) {
        ("/execute", _) => Resp::json(serde_json::json!({"job_id": "job-1", "status": "ACCEPTED"})),
        ("/status", Some(_)) => Resp::json(
            serde_json::json!({"job_id": "job-1", "status": "COMPLETED", "exit_code": 0, "execution_duration_ms": 1200}),
        ),
        ("/status", None) => Resp::json(serde_json::json!({"status": "up"})),
        _ => Resp::status(404, "404 page not found"),
    })
}

/// A test harness: config, fake, sandbox.
pub struct Harness {
    pub cfg: Arc<Config>,
    pub fake: Arc<FakeControl>,
    pub class: String,
    _sandbox: Sandbox,
}

/// A harness whose sandbox runs `handler`, with knobs `k`; metrics go
/// under `class`, unique per test.
pub async fn harness(handler: Handler, k: Knobs, class: &str) -> Harness {
    let sandbox = Sandbox::start(handler).await;
    let fake = Arc::new(FakeControl::default());
    let cfg = Config {
        api: fake.clone(),
        http: reqwest::Client::new(),
        router_url: sandbox.url.clone(),
        hive: "benchmark-test".into(),
        dyn_config: Arc::new(Holder::fixed(k)),
        tracer: Tracer::never("test-sweperf"),
        actors_per_user: 1,
        total_actors: 1,
        spawn_concurrency: 1,
        actor_deadline: Duration::from_secs(1),
    };
    Harness {
        cfg: Arc::new(cfg),
        fake,
        class: class.to_string(),
        _sandbox: sandbox,
    }
}

/// One stats row's observations.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Sample {
    pub count: f64,
    pub sum_ms: f64,
}

fn label<'a>(labels: &'a str, key: &str) -> Option<&'a str> {
    let start = labels.find(&format!("{key}=\""))? + key.len() + 2;
    let end = labels[start..].find('"')? + start;
    Some(&labels[start..end])
}

/// Every `locust_request_duration_milliseconds` row of `class`, by name
/// and status, summed over methods.
pub fn read_stats(class: &str) -> HashMap<(String, String), Sample> {
    let mut out: HashMap<(String, String), Sample> = HashMap::new();
    for line in metrics::render().lines() {
        let (field, rest) =
            if let Some(r) = line.strip_prefix("locust_request_duration_milliseconds_count{") {
                ("count", r)
            } else if let Some(r) = line.strip_prefix("locust_request_duration_milliseconds_sum{") {
                ("sum", r)
            } else {
                continue;
            };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        if label(labels, "user_class") != Some(class) {
            continue;
        }
        let key = (
            label(labels, "name").unwrap_or("").to_string(),
            label(labels, "status").unwrap_or("").to_string(),
        );
        let v: f64 = value.trim().parse().unwrap();
        let e = out.entry(key).or_default();
        if field == "count" {
            e.count += v;
        } else {
            e.sum_ms += v;
        }
    }
    out
}

/// What one row observed after `before`.
pub fn recorded_since(
    before: &HashMap<(String, String), Sample>,
    class: &str,
    name: &str,
    status: &str,
) -> Sample {
    let key = (name.to_string(), status.to_string());
    let now = read_stats(class).get(&key).copied().unwrap_or_default();
    let was = before.get(&key).copied().unwrap_or_default();
    Sample {
        count: now.count - was.count,
        sum_ms: now.sum_ms - was.sum_ms,
    }
}

/// The `locust_users` gauge of `class`.
pub fn users_gauge(class: &str) -> f64 {
    let prefix = format!("locust_users{{user_class=\"{class}\"}} ");
    metrics::render()
        .lines()
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim().parse().unwrap()))
        .unwrap_or(0.0)
}

/// Captured log lines: each a map of its fields with the message as `msg`.
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<Map<String, Value>>>>);

impl Logs {
    pub fn push(&self, m: Map<String, Value>) {
        self.0.lock().push(m);
    }

    pub fn lines(&self) -> Vec<Map<String, Value>> {
        self.0.lock().clone()
    }

    pub fn with_msg(&self, msg: &str) -> Vec<Map<String, Value>> {
        self.lines()
            .into_iter()
            .filter(|l| l.get("msg").and_then(Value::as_str) == Some(msg))
            .collect()
    }

    pub fn only(&self, msg: &str) -> Map<String, Value> {
        let got = self.with_msg(msg);
        assert_eq!(got.len(), 1, "{} {msg:?} lines, want 1: {got:?}", got.len());
        got.into_iter().next().unwrap()
    }
}

/// Captures every sweperf line logged on this thread until the guard
/// drops. Tests using it run on the current-thread runtime, so every task
/// they spawn logs here.
pub fn capture_logs() -> (Logs, super::events::Guard) {
    let logs = Logs::default();
    let guard = super::events::install(logs.clone());
    (logs, guard)
}

/// A `Fn` hook from an async block factory.
pub fn hook<F, Fut>(f: F) -> Hook
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    Arc::new(move || Box::pin(f()))
}
