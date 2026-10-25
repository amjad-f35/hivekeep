//! The actor's control routes: start, poll, cut windows of and stop the
//! loop. At most one loop runs at a time.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use super::api::*;
use super::client::{DialFn, MAX_BODY_BYTES};
use super::endpoints::endpoint_url;
use super::run::{view, CollectError, LoopRun};
use super::stats::Stats;
use crate::egress::json;

/// Maps an endpoint number and scheme to the URL the loop requests.
pub type EndpointUrlFn = Arc<dyn Fn(usize, &str) -> String + Send + Sync>;

/// One answer of the actor.
#[derive(Debug, Clone)]
pub struct Answer {
    /// HTTP status.
    pub status: u16,
    /// Headers, in order.
    pub headers: Vec<(String, String)>,
    /// Body.
    pub body: Vec<u8>,
}

impl Answer {
    /// The first value of header `name`, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn error(status: u16, msg: &str) -> Answer {
        Answer {
            status,
            headers: vec![
                ("Content-Type".into(), "text/plain; charset=utf-8".into()),
                ("X-Content-Type-Options".into(), "nosniff".into()),
            ],
            body: format!("{msg}\n").into_bytes(),
        }
    }

    fn json(s: &Stats) -> Answer {
        let mut body = json::to_vec(s).unwrap_or_default();
        body.push(b'\n');
        Answer {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body,
        }
    }
}

#[derive(Default)]
struct Inner {
    run: Option<Arc<LoopRun>>,
    /// The last stopped loop's Stats, endpoints included, kept until the
    /// next start so a repeated stop answers it again.
    final_stats: Option<Stats>,
}

/// The actor's route handler.
pub struct ActorServer {
    /// The URL the loop requests for an endpoint. Tests point it at local
    /// servers.
    pub endpoint_url: EndpointUrlFn,
    /// Verifies HTTPS endpoints; `None` uses the platform roots, which
    /// `SSL_CERT_FILE` extends with the gateway's interception CA.
    pub root_cas: Option<rustls::RootCertStore>,
    /// Opens the loop's connections; `None` dials TCP after a lookup.
    pub dial: Option<DialFn>,
    inner: Mutex<Inner>,
}

impl Default for ActorServer {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorServer {
    /// The handler as it runs in the cluster.
    pub fn new() -> Self {
        ActorServer {
            endpoint_url: Arc::new(endpoint_url),
            root_cas: None,
            dial: None,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Installs `run` as the running loop (tests).
    #[cfg(test)]
    pub(crate) async fn set_run(&self, run: Arc<LoopRun>) {
        self.inner.lock().await.run = Some(run);
    }

    /// Answers one request. `target` is the path with its query.
    pub async fn handle(&self, method: &str, target: &str, body: &[u8]) -> Answer {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let query: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let want = match path {
            READYZ_ROUTE | STATS_ROUTE => "GET",
            START_ROUTE | COLLECT_ROUTE | STOP_ROUTE => "POST",
            _ => "",
        };
        let mut a = if want.is_empty() {
            Answer::error(404, "404 page not found")
        } else if method != want && !(want == "GET" && method == "HEAD") {
            let mut a = Answer::error(405, "Method Not Allowed");
            a.headers.push((
                "Allow".into(),
                if want == "GET" {
                    "GET, HEAD".into()
                } else {
                    want.into()
                },
            ));
            a
        } else {
            let endpoints = query.get("endpoints").map(String::as_str) == Some("1");
            match path {
                READYZ_ROUTE => Answer {
                    status: 200,
                    headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                    body: b"ok\n".to_vec(),
                },
                START_ROUTE => self.start(body).await,
                STATS_ROUTE => self.stats(endpoints).await,
                COLLECT_ROUTE => {
                    self.collect(query.get(WINDOW_PARAM).map(String::as_str), endpoints)
                        .await
                }
                _ => self.stop(endpoints).await,
            }
        };
        a.headers
            .push((PROTOCOL_HEADER.into(), PROTOCOL_VERSION.into()));
        a
    }

    async fn start(&self, body: &[u8]) -> Answer {
        let body = &body[..body.len().min(MAX_BODY_BYTES)];
        let req = match serde_json::Deserializer::from_slice(body)
            .into_iter::<StartRequest>()
            .next()
        {
            Some(Ok(r)) => r,
            Some(Err(e)) => return Answer::error(400, &format!("decoding start request: {e}")),
            None => return Answer::error(400, "decoding start request: EOF"),
        };
        if let Err(e) = req.validate() {
            return Answer::error(400, &e);
        }
        let mut inner = self.inner.lock().await;
        if inner.run.is_some() {
            return Answer::error(
                409,
                &format!("a loop is already running; POST {STOP_ROUTE} first"),
            );
        }
        let urls: Vec<String> = (0..req.endpoints as usize)
            .map(|i| (self.endpoint_url)(i, req.url_scheme()))
            .collect();
        match LoopRun::start(&req, &urls, self.root_cas.clone(), self.dial.clone()) {
            Ok(run) => {
                inner.run = Some(run);
                inner.final_stats = None;
                Answer {
                    status: 204,
                    headers: Vec::new(),
                    body: Vec::new(),
                }
            }
            Err(e) => Answer::error(500, &format!("starting loop: {e}")),
        }
    }

    async fn current(&self) -> Option<Arc<LoopRun>> {
        self.inner.lock().await.run.clone()
    }

    async fn stats(&self, endpoints: bool) -> Answer {
        match self.current().await.and_then(|r| r.snapshot(endpoints)) {
            Some(s) => Answer::json(&s),
            None => Answer::error(404, NO_LOOP_MESSAGE),
        }
    }

    async fn collect(&self, raw: Option<&str>, endpoints: bool) -> Answer {
        let Some(run) = self.current().await else {
            return Answer::error(404, NO_LOOP_MESSAGE);
        };
        let raw = raw.unwrap_or("");
        let window = match raw.parse::<i64>() {
            Ok(w) if w >= 1 => w,
            _ => {
                return Answer::error(
                    400,
                    &format!(
                    "{WINDOW_PARAM} must be a positive integer, got {raw:?}; the next window is {}",
                    run.next_window()
                ),
                )
            }
        };
        match run.collect(window, endpoints) {
            Ok(s) => Answer::json(&s),
            Err(CollectError::NoLoop) => Answer::error(404, NO_LOOP_MESSAGE),
            Err(e @ CollectError::Sequence { .. }) => Answer::error(409, &e.to_string()),
            Err(e) => Answer::error(500, &format!("collecting: {e}")),
        }
    }

    async fn stop(&self, endpoints: bool) -> Answer {
        // Held across the wait so a start cannot slip in before the loop exits.
        let mut inner = self.inner.lock().await;
        let Some(run) = inner.run.take() else {
            return match &inner.final_stats {
                Some(s) => Answer::json(&view(s.clone(), endpoints)),
                None => Answer::error(404, NO_LOOP_MESSAGE),
            };
        };
        match run.stop().await {
            Ok(s) => {
                inner.final_stats = Some(s.clone());
                Answer::json(&view(s, endpoints))
            }
            Err(e) => Answer::error(500, &format!("collecting: {e}")),
        }
    }
}

/// Serves `server`'s routes on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    server: Arc<ActorServer>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    loop {
        let (stream, _) = tokio::select! {
            _ = &mut shutdown => return,
            r = listener.accept() => match r {
                Ok(c) => c,
                Err(_) => continue,
            },
        };
        let server = server.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let server = server.clone();
                async move {
                    let method = req.method().as_str().to_string();
                    let target = req
                        .uri()
                        .path_and_query()
                        .map(|p| p.as_str().to_string())
                        .unwrap_or_default();
                    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
                        .collect()
                        .await
                    {
                        Ok(b) => b.to_bytes(),
                        Err(_) => Bytes::new(),
                    };
                    let a = server.handle(&method, &target, &body).await;
                    let mut resp = hyper::Response::new(Full::new(Bytes::from(a.body)));
                    *resp.status_mut() =
                        hyper::StatusCode::from_u16(a.status).unwrap_or(hyper::StatusCode::OK);
                    for (k, v) in a.headers {
                        if let (Ok(k), Ok(v)) = (
                            hyper::header::HeaderName::try_from(k),
                            hyper::header::HeaderValue::try_from(v),
                        ) {
                            resp.headers_mut().append(k, v);
                        }
                    }
                    Ok::<_, Infallible>(resp)
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}
