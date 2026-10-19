//! `logs actor`: an Actor's lines from the log stream of the worker pod
//! hosting it. With `--follow`, the stream is followed across suspends,
//! resumes and moves to other Workers.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, SecondsFormat};
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::api::v1 as pb;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tonic::Code;

use crate::client::Api;
use crate::context::{rpc_error, Ctx};
use crate::error::{status_text, CliError, Result};
use crate::io::Sink;
use crate::kubectl::Kubectl;
use crate::log_filter::{display_line, split_timestamp, LogFilter};
use crate::print::enum_name;
use crate::verbosity::{Logger, BASIC, TRACE};

/// What to read from a pod's log.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogOptions {
    /// Keep the stream open for new lines.
    pub follow: bool,
    /// Prefix every line with its RFC 3339 timestamp.
    pub timestamps: bool,
    /// Only lines at or after this time (second precision).
    pub since: Option<DateTime<FixedOffset>>,
}

/// An open log stream: lines, or one final error.
pub struct LogStream {
    rx: mpsc::Receiver<std::result::Result<String, String>>,
    // Keeps the producer (a kubectl process) alive while the stream is.
    _guard: Option<Box<dyn Send>>,
}

impl LogStream {
    /// A stream fed by `rx`.
    pub fn new(rx: mpsc::Receiver<std::result::Result<String, String>>) -> Self {
        LogStream { rx, _guard: None }
    }

    /// The next line; `None` at the end of the stream.
    pub async fn next(&mut self) -> Option<std::result::Result<String, String>> {
        self.rx.recv().await
    }
}

/// A source of pod logs.
pub trait PodLogs: Sync {
    /// Opens the log of `pod` in `namespace`.
    fn stream(
        &self,
        namespace: &str,
        pod: &str,
        opts: &LogOptions,
    ) -> impl Future<Output = std::result::Result<LogStream, String>> + Send;
}

/// Reads pod logs through `kubectl logs`.
pub struct KubectlPodLogs(pub Kubectl);

impl PodLogs for KubectlPodLogs {
    async fn stream(
        &self,
        namespace: &str,
        pod: &str,
        opts: &LogOptions,
    ) -> std::result::Result<LogStream, String> {
        let mut args = vec![
            "logs".to_string(),
            "-n".into(),
            namespace.into(),
            pod.into(),
        ];
        if opts.follow {
            args.push("--follow".into());
        }
        if opts.timestamps {
            args.push("--timestamps".into());
        }
        if let Some(t) = opts.since {
            args.push(format!(
                "--since-time={}",
                t.to_rfc3339_opts(SecondsFormat::Secs, true)
            ));
        }
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let mut child = self
            .0
            .tokio_command(&refs)
            .spawn()
            .map_err(|e| format!("running kubectl: {e}"))?;
        let stdout = child.stdout.take().ok_or("kubectl has no stdout")?;
        let stderr = child.stderr.take().ok_or("kubectl has no stderr")?;
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(l)) => {
                        if tx.send(Ok(l)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        let mut msg = String::new();
                        let _ = tokio::io::AsyncReadExt::read_to_string(
                            &mut BufReader::new(stderr),
                            &mut msg,
                        )
                        .await;
                        let status = child.wait().await;
                        if !status.map(|s| s.success()).unwrap_or(false) && !msg.trim().is_empty() {
                            let _ = tx.send(Err(msg.trim().to_string())).await;
                        }
                        break;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string())).await;
                        break;
                    }
                }
            }
        });
        Ok(LogStream::new(rx))
    }
}

/// Where an Actor currently runs, if it does.
fn running_on(actor: &pb::Actor) -> Option<(String, String)> {
    let status = actor.status.as_ref()?;
    if status.state != pb::ActorState::Running as i32 {
        return None;
    }
    let w = status.worker_assignment.as_ref()?;
    (!w.worker_pod.is_empty() && !w.worker_namespace.is_empty())
        .then(|| (w.worker_namespace.clone(), w.worker_pod.clone()))
}

fn state_of(actor: &pb::Actor) -> i32 {
    actor.status.as_ref().map(|s| s.state).unwrap_or_default()
}

fn pod_of(actor: &pb::Actor) -> String {
    actor
        .status
        .as_ref()
        .and_then(|s| s.worker_assignment.as_ref())
        .map(|w| w.worker_pod.clone())
        .unwrap_or_default()
}

/// Prints an Actor's logs once, or follows them.
pub struct LogsRunner<P> {
    /// The control plane, to find the Actor's worker pod.
    pub api: Api,
    /// Pod log source.
    pub logs: P,
    /// Selects the lines shown.
    pub filter: LogFilter,
    /// Where lines go.
    pub out: Sink,
    /// Where notices go.
    pub err: Sink,
    /// Diagnostics.
    pub log: Logger,
    /// Interval between checks while the Actor is not running.
    pub poll_interval: Duration,
    /// Pause before reopening an ended stream.
    pub reconnect_interval: Duration,
    /// Interval between checks that the Actor is still on the streamed pod.
    pub monitor_interval: Duration,
}

impl<P: PodLogs> LogsRunner<P> {
    fn actor(&self) -> &ActorRef {
        self.filter.target()
    }

    async fn get_actor(&self) -> std::result::Result<pb::Actor, tonic::Status> {
        get_actor(&self.api, self.actor()).await
    }

    /// Prints what the Actor's current pod has logged, then returns.
    pub async fn run_once(&self) -> Result<()> {
        let actor = self
            .get_actor()
            .await
            .map_err(|s| rpc_error("failed to get actor", &s))?;
        let (namespace, pod) = running_on(&actor).ok_or_else(|| {
            CliError::failed(format!(
                "actor {} is not currently running on any worker pod",
                self.actor()
            ))
        })?;
        let mut stream = self
            .logs
            .stream(&namespace, &pod, &LogOptions::default())
            .await
            .map_err(|e| CliError::failed(format!("failed to stream logs from pod {pod}: {e}")))?;
        let mut out = self.out.clone();
        while let Some(item) = stream.next().await {
            match item {
                Ok(line) => {
                    display_line(&line, &self.filter, &mut out, &self.log);
                }
                Err(e) => return Err(CliError::failed(format!("error reading log stream: {e}"))),
            }
        }
        Ok(())
    }

    /// Follows the Actor's logs until it is deleted (or the future is
    /// dropped). Reconnects resume after the last line shown.
    pub async fn follow(&self) -> Result<()> {
        let mut last_pod = String::new();
        // The kubelet's timestamp on the last line read from `last_pod`: the
        // cursor for a reconnect to it. It does not depend on what the line
        // says, and an Actor's own clock can run ahead of the node's.
        let mut cursor: Option<DateTime<FixedOffset>> = None;
        let mut waiting_state: Option<i32> = None;

        loop {
            let actor = match self.get_actor().await {
                Ok(a) => a,
                Err(s) if s.code() == Code::NotFound => {
                    return Err(CliError::failed(format!(
                        "actor {} not found: {}",
                        self.actor(),
                        status_text(&s)
                    )));
                }
                Err(s) => {
                    self.log.log(
                        BASIC,
                        format!(
                            "Failed to get actor {}: {}; retrying in {:?}",
                            self.actor(),
                            status_text(&s),
                            self.poll_interval
                        ),
                    );
                    tokio::time::sleep(self.poll_interval).await;
                    continue;
                }
            };

            let Some((namespace, pod)) = running_on(&actor) else {
                let state = state_of(&actor);
                if waiting_state != Some(state) {
                    self.log.log(
                        BASIC,
                        format!(
                            "Actor {} is not running (state={}); waiting for worker assignment...",
                            self.actor(),
                            enum_name::<pb::ActorState>(state)
                        ),
                    );
                    waiting_state = Some(state);
                }
                tokio::time::sleep(self.poll_interval).await;
                continue;
            };
            waiting_state = None;

            // The Actor resumed on another pod. The cursor still bounds how
            // far back the new stream starts, but its lines are all new and
            // that node's clock is not this one's, so they are not compared.
            let switched = pod != last_pod;
            if switched {
                self.err.line(&format!(
                    "Actor is currently running on pod {namespace}/{pod}"
                ));
                last_pod = pod.clone();
            }

            let opts = LogOptions {
                follow: true,
                timestamps: true,
                since: cursor,
            };
            let mut stream = match self.logs.stream(&namespace, &pod, &opts).await {
                Ok(s) => s,
                Err(e) => {
                    self.log.log(
                        BASIC,
                        format!(
                            "Failed to stream logs from pod {namespace}/{pod}: {e}; retrying in {:?}",
                            self.poll_interval
                        ),
                    );
                    tokio::time::sleep(self.poll_interval).await;
                    continue;
                }
            };

            let (left_tx, mut left_rx) = oneshot::channel::<()>();
            let monitor = tokio::spawn(monitor_pod(
                self.api.clone(),
                self.actor().clone(),
                pod.clone(),
                self.monitor_interval,
                self.log.clone(),
                left_tx,
            ));

            let mut out = self.out.clone();
            loop {
                let item = tokio::select! {
                    item = stream.next() => item,
                    _ = &mut left_rx => break,
                };
                match item {
                    None => break,
                    Some(Ok(raw)) => {
                        let (stamp, line) = split_timestamp(&raw);
                        match stamp {
                            Some(t) => {
                                // The since-time has second precision, so a
                                // reconnect re-reads the cursor's second;
                                // lines up to the cursor were already shown.
                                if !switched && cursor.is_some_and(|c| t <= c) {
                                    continue;
                                }
                                cursor = Some(t);
                            }
                            None => self
                                .log
                                .log(TRACE, "Log line had no kubelet timestamp prefix"),
                        }
                        display_line(line, &self.filter, &mut out, &self.log);
                    }
                    Some(Err(e)) => {
                        self.err
                            .line(&format!("Error reading log stream: {e}. Reconnecting..."));
                        break;
                    }
                }
            }
            monitor.abort();
            drop(stream);
            tokio::time::sleep(self.reconnect_interval).await;
        }
    }
}

async fn get_actor(api: &Api, actor: &ActorRef) -> std::result::Result<pb::Actor, tonic::Status> {
    api.get_actor(pb::GetActorRequest {
        actor: Some(actor.to_object_ref()),
    })
    .await
}

/// Signals `left` once the Actor is no longer running on `pod`: suspended,
/// or moved to another Worker.
async fn monitor_pod(
    api: Api,
    actor: ActorRef,
    pod: String,
    interval: Duration,
    log: Logger,
    left: oneshot::Sender<()>,
) {
    loop {
        tokio::time::sleep(interval).await;
        match get_actor(&api, &actor).await {
            Err(s) => log.log(
                BASIC,
                format!(
                    "Migration monitor failed to get actor {actor}: {}",
                    status_text(&s)
                ),
            ),
            Ok(a) => {
                if running_on(&a).map(|(_, p)| p) != Some(pod.clone()) {
                    log.log(
                        BASIC,
                        format!(
                            "Actor {actor} left pod {pod} (state={}, pod={}); closing current log stream",
                            enum_name::<pb::ActorState>(state_of(&a)),
                            pod_of(&a)
                        ),
                    );
                    let _ = left.send(());
                    return;
                }
            }
        }
    }
}

/// `logs actor <name>`.
pub async fn run(
    ctx: &mut Ctx,
    name: &str,
    hive: &str,
    follow: bool,
    container: &str,
    source: &str,
) -> Result<()> {
    // A bad flag combination is reported as such, before any connection.
    let filter =
        LogFilter::new(ActorRef::new(hive, name), source, container).map_err(CliError::usage)?;
    let api = ctx.api().await?;
    let runner = LogsRunner {
        api,
        logs: KubectlPodLogs(ctx.kubectl()),
        filter,
        out: ctx.out(),
        err: ctx.err(),
        log: ctx.log.clone(),
        poll_interval: Duration::from_secs(2),
        reconnect_interval: Duration::from_secs(1),
        monitor_interval: Duration::from_secs(2),
    };
    if follow {
        runner.follow().await
    } else {
        runner.run_once().await
    }
}
