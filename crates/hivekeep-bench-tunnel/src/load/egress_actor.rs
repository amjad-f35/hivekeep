//! Egress has to start inside an Actor's sandbox, where the sandbox's
//! redirect hands every outbound connection to the tunnel. The egress actor
//! runs there and executes egress modes on the load generator's behalf; the
//! two talk over a Unix socket, which reaches across network namespaces.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::{Method, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use serde::{Deserialize, Serialize};
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use super::modes::{execute, Hook, Mode, Options, Targets, MAX_ERROR_SAMPLES};
use crate::admin::{self, Handler};
use crate::report::Load;
use crate::{Error, Result};

/// Where the egress actor's answered traffic goes. Nothing answers it: the
/// tunnel takes every connection, and the fake gateway answers by port.
pub const ANSWER_DESTINATION: &str = "192.0.2.10:80";
/// Where the egress actor's discarded traffic goes.
pub const DISCARD_DESTINATION: &str = "192.0.2.10:9";

/// The line the egress actor writes when EgressIdle holds its connections,
/// so the generator can read the tunnel then.
pub const HOLDING: &str = "holding";

/// A run's options on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub mode: Mode,
    pub rate: f64,
    pub duration_ns: u64,
    pub warmup_ns: u64,
    pub workers: usize,
    pub connections: usize,
    pub new_connection_per_request: bool,
    pub payload_bytes: usize,
}

/// A run's result on the wire.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunResult {
    pub load: Load,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

impl RunRequest {
    fn from_options(o: &Options) -> Result<Self> {
        Ok(RunRequest {
            mode: o.mode.ok_or_else(|| Error::msg("no mode"))?,
            rate: o.rate,
            duration_ns: o.duration.as_nanos() as u64,
            warmup_ns: o.warmup.as_nanos() as u64,
            workers: o.workers,
            connections: o.connections,
            new_connection_per_request: o.new_connection_per_request,
            payload_bytes: o.payload_bytes,
        })
    }

    fn options(&self) -> Options {
        Options {
            mode: Some(self.mode),
            rate: self.rate,
            duration: Duration::from_nanos(self.duration_ns),
            warmup: Duration::from_nanos(self.warmup_ns),
            workers: self.workers,
            connections: self.connections,
            new_connection_per_request: self.new_connection_per_request,
            payload_bytes: self.payload_bytes,
            while_holding: None,
        }
    }
}

/// Serves egress runs on `lis` until `cancel`, sending traffic to the
/// fixed destinations the sandbox redirect captures.
pub async fn serve(lis: UnixListener, cancel: CancellationToken) -> std::io::Result<()> {
    let targets = Targets {
        egress: ANSWER_DESTINATION.into(),
        egress_discard: DISCARD_DESTINATION.into(),
        ..Default::default()
    };
    serve_with(lis, targets, cancel).await
}

/// [`serve`] with explicit destinations.
pub async fn serve_with(
    lis: UnixListener,
    targets: Targets,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    admin::serve_unix(lis, handler(Arc::new(targets)), cancel).await
}

fn handler(targets: Arc<Targets>) -> Handler {
    Arc::new(move |req| {
        let targets = targets.clone();
        Box::pin(async move {
            // The egress actor generates the egress load, so its CPU is the
            // generator's.
            if let Some(resp) = admin::stats_route(&req, None) {
                return resp;
            }
            if req.method() != Method::POST || req.uri().path() != "/run" {
                return admin::not_found();
            }
            let body = match req.into_body().collect().await {
                Ok(b) => b.to_bytes(),
                Err(e) => {
                    return admin::respond(StatusCode::BAD_REQUEST, "text/plain", e.to_string())
                }
            };
            let run: RunRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return admin::respond(StatusCode::BAD_REQUEST, "text/plain", e.to_string())
                }
            };
            let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(4);
            tokio::spawn(async move {
                let mut opts = run.options();
                let holding = tx.clone();
                let hook: Hook = Arc::new(move || {
                    let holding = holding.clone();
                    Box::pin(async move {
                        let _ = holding.send(Bytes::from(format!("{HOLDING}\n"))).await;
                    })
                });
                opts.while_holding = Some(hook);
                // A generator that hangs up ends the run.
                let gone = CancellationToken::new();
                let result = tokio::select! {
                    r = execute(&targets, &opts, &gone) => r,
                    _ = tx.closed() => { gone.cancel(); return; }
                };
                let res = match result {
                    Ok(load) => RunResult {
                        load,
                        error: String::new(),
                    },
                    Err(e) => RunResult {
                        load: Load::default(),
                        error: e.to_string(),
                    },
                };
                let mut line = serde_json::to_vec(&res).unwrap_or_default();
                line.push(b'\n');
                let _ = tx.send(Bytes::from(line)).await;
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
                .map(|b| Ok::<_, std::io::Error>(Frame::data(b)));
            http::Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "application/x-ndjson")
                .body(BodyExt::boxed(StreamBody::new(stream)))
                .unwrap_or_else(|_| admin::not_found())
        })
    })
}

/// Runs `opts` on the egress actor at `socket`.
pub async fn execute_on(socket: &Path, opts: &Options, cancel: &CancellationToken) -> Result<Load> {
    let body = serde_json::to_vec(&RunRequest::from_options(opts)?)?;
    let run = async {
        let resp = admin::Client::unix(socket)
            .send(Method::POST, "/run", Bytes::from(body))
            .await
            .map_err(|e| Error::msg(format!("egress actor: {e}")))?;
        if resp.status() != StatusCode::OK {
            return Err(Error::msg(format!(
                "egress actor: HTTP {}",
                resp.status().as_u16()
            )));
        }
        let mut body = resp.into_body();
        let mut buf = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| Error::msg(format!("egress actor: {e}")))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            buf.extend_from_slice(&data);
            while let Some(i) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=i).collect();
                let line = &line[..line.len() - 1];
                if line == HOLDING.as_bytes() {
                    if let Some(h) = &opts.while_holding {
                        h().await;
                    }
                    continue;
                }
                let res: RunResult = serde_json::from_slice(line)
                    .map_err(|e| Error::msg(format!("egress actor: {e}")))?;
                if !res.error.is_empty() {
                    return Err(Error::msg(format!("egress actor: {}", res.error)));
                }
                return Ok(res.load);
            }
        }
        Err(Error::msg("egress actor: no result"))
    };
    tokio::select! {
        r = run => r,
        _ = cancel.cancelled() => Err(Error::msg("egress actor: canceled")),
    }
}

/// Splits an EgressIdle run evenly across the egress actors at `sockets`,
/// one per sandbox. A sandbox has one address, so it can hold at most ~64k
/// egress connections at a time: the redirect makes every one of them look
/// the same but for the source port. The hook runs once, after every actor
/// holds its connections.
pub async fn execute_on_all(
    sockets: &[PathBuf],
    opts: &Options,
    cancel: &CancellationToken,
) -> Result<Load> {
    let n = sockets.len().max(1);
    let waiting = Arc::new(AtomicUsize::new(n));
    let runs = sockets.iter().enumerate().map(|(i, socket)| {
        let mut sub = opts.clone();
        sub.rate = opts.rate / n as f64;
        sub.connections = opts.connections / n + usize::from(i < opts.connections % n);
        let (waiting, parent) = (waiting.clone(), opts.while_holding.clone());
        sub.while_holding = Some(Arc::new(move || {
            let (waiting, parent) = (waiting.clone(), parent.clone());
            Box::pin(async move {
                if waiting.fetch_sub(1, Ordering::SeqCst) == 1 {
                    if let Some(h) = parent {
                        h().await;
                    }
                }
            })
        }));
        async move { execute_on(socket, &sub, cancel).await }
    });
    let results = futures::future::join_all(runs).await;
    let mut loads = Vec::new();
    let mut errs = Vec::new();
    for r in results {
        match r {
            Ok(l) => loads.push(l),
            Err(e) => errs.push(e.to_string()),
        }
    }
    if !errs.is_empty() {
        return Err(Error::msg(errs.join("\n")));
    }
    Ok(merge_loads(&loads))
}

/// Adds up loads that ran side by side; latency percentiles are the worst.
pub fn merge_loads(loads: &[Load]) -> Load {
    let mut m = Load::default();
    for l in loads {
        m.offered += l.offered;
        m.measured_seconds = m.measured_seconds.max(l.measured_seconds);
        m.scheduled += l.scheduled;
        m.skipped += l.skipped;
        m.completed += l.completed;
        m.errors += l.errors;
        m.bytes += l.bytes;
        m.latency = m.latency.worst(l.latency);
        m.send_lag = m.send_lag.worst(l.send_lag);
        m.error_samples.extend(l.error_samples.iter().cloned());
    }
    m.error_samples.truncate(MAX_ERROR_SAMPLES);
    if m.measured_seconds > 0.0 {
        m.achieved = m.completed as f64 / m.measured_seconds;
        m.mbytes_per_sec = m.bytes as f64 / m.measured_seconds / 1e6;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::IngressActor;

    /// A plain HTTP server standing in for the far end of the sandbox's
    /// redirect.
    async fn target() -> (String, CancellationToken) {
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap().to_string();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move { IngressActor::new(2).serve(lis, c).await });
        (addr, cancel)
    }

    fn egress_actor(dir: &Path, name: &str, targets: Targets) -> (PathBuf, CancellationToken) {
        let socket = dir.join(name);
        let lis = admin::listen_unix(&socket).unwrap();
        let cancel = CancellationToken::new();
        tokio::spawn(serve_with(lis, targets, cancel.clone()));
        (socket, cancel)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn egress_actor_runs_modes() {
        let (addr, _t) = target().await;
        let dir = tempfile::tempdir().unwrap();
        let (socket, _a) = egress_actor(
            dir.path(),
            "egress-actor.sock",
            Targets {
                egress: addr.clone(),
                egress_discard: addr,
                ..Default::default()
            },
        );
        let cancel = CancellationToken::new();

        let opts = Options {
            rate: 100.0,
            duration: Duration::from_millis(300),
            workers: 4,
            ..Options::new(Mode::EgressHttp)
        };
        let l = execute_on(&socket, &opts, &cancel).await.unwrap();
        assert!(
            l.completed > 0 && l.errors == 0,
            "EgressHttp through the egress actor: completed {}, errors {}: {:?}",
            l.completed,
            l.errors,
            l.error_samples
        );

        let held = Arc::new(AtomicUsize::new(0));
        let h = held.clone();
        let opts = Options {
            rate: 100.0,
            duration: Duration::from_millis(500),
            workers: 4,
            connections: 5,
            while_holding: Some(Arc::new(move || {
                h.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {})
            })),
            ..Options::new(Mode::EgressIdle)
        };
        let l = execute_on(&socket, &opts, &cancel).await.unwrap();
        assert_eq!(
            (held.load(Ordering::SeqCst), l.completed),
            (1, 5),
            "EgressIdle through the egress actor"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn egress_actors_split_and_add_up() {
        let (addr, _t) = target().await;
        let dir = tempfile::tempdir().unwrap();
        let mut sockets = Vec::new();
        let mut keep = Vec::new();
        for i in 0..3 {
            let (s, c) = egress_actor(
                dir.path(),
                &format!("egress-actor-{i}.sock"),
                Targets {
                    egress: addr.clone(),
                    ..Default::default()
                },
            );
            sockets.push(s);
            keep.push(c);
        }
        let held = Arc::new(AtomicUsize::new(0));
        let h = held.clone();
        let opts = Options {
            rate: 300.0,
            duration: Duration::from_millis(500),
            workers: 4,
            connections: 10,
            while_holding: Some(Arc::new(move || {
                h.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {})
            })),
            ..Options::new(Mode::EgressIdle)
        };
        let l = execute_on_all(&sockets, &opts, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            (held.load(Ordering::SeqCst), l.completed, l.errors),
            (1, 10, 0),
            "EgressIdle across 3 egress actors"
        );
    }

    #[test]
    fn merge_takes_sums_and_worst_latency() {
        let a = Load {
            offered: 1.0,
            completed: 2,
            measured_seconds: 1.0,
            latency: crate::report::Percentiles {
                p99: 3.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let b = Load {
            offered: 1.0,
            completed: 4,
            measured_seconds: 2.0,
            latency: crate::report::Percentiles {
                p99: 1.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let m = merge_loads(&[a, b]);
        assert_eq!(
            (
                m.offered,
                m.completed,
                m.measured_seconds,
                m.latency.p99,
                m.achieved
            ),
            (2.0, 6, 2.0, 3.0, 3.0)
        );
    }
}
