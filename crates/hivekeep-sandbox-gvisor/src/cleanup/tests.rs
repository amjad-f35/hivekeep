use super::*;
use crate::runsc::Output;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};

/// Stands in for runsc: `containers` is what runsc has a record of, `calls`
/// every command. A container can be set to exit on a signal.
#[derive(Default)]
struct FakeRunsc {
    containers: Mutex<BTreeSet<String>>,
    calls: Mutex<Vec<String>>,
    exit_on: Option<&'static str>,
    wait_err: Option<&'static str>,
    kill_err: HashMap<&'static str, &'static str>,
    exited: tokio::sync::Notify,
    has_exited: std::sync::atomic::AtomicBool,
}

impl FakeRunsc {
    fn with(names: &[&str]) -> Self {
        let f = FakeRunsc::default();
        f.containers
            .lock()
            .extend(names.iter().map(|s| s.to_string()));
        f
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    fn signals(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|c| c.strip_prefix("kill counter ").map(str::to_string))
            .collect()
    }
}

#[async_trait]
impl ContainerRuntime for FakeRunsc {
    async fn create(&self, _: &CancellationToken, _: &Output, _: &str, _: &[String]) -> Result<()> {
        unimplemented!()
    }
    async fn start(&self, _: &CancellationToken, _: &Output, _: &str) -> Result<()> {
        unimplemented!()
    }
    async fn checkpoint(
        &self,
        _: &CancellationToken,
        _: &str,
        _: &str,
        _: &[String],
    ) -> Result<()> {
        unimplemented!()
    }
    async fn fs_checkpoint(
        &self,
        _: &CancellationToken,
        _: &str,
        _: &str,
        _: &[String],
    ) -> Result<()> {
        unimplemented!()
    }
    async fn restore(&self, _: &CancellationToken, _: &Output, _: &str, _: &str) -> Result<()> {
        unimplemented!()
    }
    async fn delete(&self, _: &CancellationToken, name: &str) -> Result<()> {
        self.calls.lock().push(format!("delete {name}"));
        if self.containers.lock().remove(name) {
            Ok(())
        } else {
            Err(Error::msg("exit status 128"))
        }
    }
    async fn state(&self, _: &CancellationToken, name: &str) -> Result<()> {
        self.calls.lock().push(format!("state {name}"));
        if self.containers.lock().contains(name) {
            Ok(())
        } else {
            Err(Error::msg("exit status 128"))
        }
    }
    async fn list(&self, _: &CancellationToken) -> Result<Vec<String>> {
        self.calls.lock().push("list".into());
        Ok(self.containers.lock().iter().cloned().collect())
    }
    async fn kill(&self, _: &CancellationToken, name: &str, signal: &str) -> Result<()> {
        self.calls.lock().push(format!("kill {name} {signal}"));
        if self.exit_on == Some(signal) {
            self.has_exited
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.exited.notify_waiters();
        }
        match self.kill_err.get(signal) {
            Some(e) => Err(Error::msg(*e)),
            None => Ok(()),
        }
    }
    async fn wait(&self, cancel: &CancellationToken, name: &str) -> Result<()> {
        if self.exit_on.is_none() && self.wait_err.is_none() && name != "counter" {
            self.calls.lock().push(format!("wait {name}"));
            return Ok(());
        }
        let exited = self.exited.notified();
        tokio::pin!(exited);
        exited.as_mut().enable();
        if !self.has_exited.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::select! {
                _ = exited => {}
                _ = cancel.cancelled() => return Err(Error::Cancelled),
            }
        }
        match self.wait_err {
            Some(e) => Err(Error::msg(e)),
            None => Ok(()),
        }
    }
    async fn wait_restore(&self, _: &CancellationToken, _: &str) -> Result<()> {
        unimplemented!()
    }
    async fn wait_fs_restore(&self, _: &CancellationToken, _: &str) -> Result<()> {
        unimplemented!()
    }
}

fn app() -> Vec<pb::Container> {
    vec![pb::Container {
        name: "app".into(),
        ..Default::default()
    }]
}

fn tok() -> CancellationToken {
    CancellationToken::new()
}

#[tokio::test]
async fn cleanup_deletes_every_container_runsc_knows() {
    let f = FakeRunsc::with(&["app", "_sandbox"]);
    cleanup_containers(&f, &app(), &tok()).await.unwrap();
    // Happy path: no `runsc list` at all.
    assert_eq!(
        f.calls(),
        [
            "state app",
            "state _sandbox",
            "delete app",
            "delete _sandbox"
        ]
    );
    assert!(f.containers.lock().is_empty());
    cleanup_containers(&f, &app(), &tok()).await.unwrap();
}

#[tokio::test]
async fn cleanup_skips_containers_runsc_has_no_record_of() {
    let f = FakeRunsc::with(&["app"]);
    cleanup_containers(&f, &app(), &tok()).await.unwrap();
    assert_eq!(
        f.calls(),
        ["state app", "state _sandbox", "list", "delete app"]
    );
}

#[tokio::test]
async fn cleanup_succeeds_when_everything_is_gone() {
    let f = FakeRunsc::with(&[]);
    cleanup_containers(&f, &app(), &tok()).await.unwrap();
    assert_eq!(f.calls(), ["state app", "list", "state _sandbox", "list"]);
}

#[tokio::test]
async fn stopping_leaves_the_sandbox_to_cleanup() {
    let f = FakeRunsc::with(&[]);
    stop_containers(&f, &app(), &tok()).await;
    assert_eq!(f.calls(), ["kill app SIGKILL", "wait app"]);
}

#[tokio::test]
async fn delete_goes_in_reverse() {
    let f = FakeRunsc::with(&["_sandbox", "app"]);
    delete_containers(
        &f,
        &["_sandbox".into(), "app".into(), "gone".into()],
        "Run",
        &tok(),
    )
    .await;
    assert_eq!(f.calls(), ["delete gone", "delete app", "delete _sandbox"]);
}

const KILL_TIMEOUT: Duration = Duration::from_millis(20);

async fn run_kill(f: &FakeRunsc, deadline_in: i64) -> Result<()> {
    let deadline = if deadline_in >= 0 {
        Instant::now() + Duration::from_millis(deadline_in as u64)
    } else {
        Instant::now() - Duration::from_millis((-deadline_in) as u64)
    };
    kill_container(f, "counter", deadline, KILL_TIMEOUT, &tok()).await
}

#[tokio::test]
async fn kill_container_escalation() {
    struct Case {
        name: &'static str,
        exit_on: Option<&'static str>,
        wait_err: Option<&'static str>,
        kill_err: &'static [(&'static str, &'static str)],
        deadline_ms: i64,
        signals: &'static [&'static str],
        err: Option<&'static str>,
    }
    let cases = [
        Case {
            name: "exits on SIGTERM",
            exit_on: Some("SIGTERM"),
            wait_err: None,
            kill_err: &[],
            deadline_ms: 60_000,
            signals: &["SIGTERM"],
            err: None,
        },
        Case {
            name: "ignores SIGTERM, killed at the deadline",
            exit_on: Some("SIGKILL"),
            wait_err: None,
            kill_err: &[],
            deadline_ms: 20,
            signals: &["SIGTERM", "SIGKILL"],
            err: None,
        },
        Case {
            name: "deadline already spent",
            exit_on: Some("SIGKILL"),
            wait_err: None,
            kill_err: &[],
            deadline_ms: -1000,
            signals: &["SIGTERM", "SIGKILL"],
            err: None,
        },
        Case {
            name: "survives SIGKILL",
            exit_on: None,
            wait_err: None,
            kill_err: &[],
            deadline_ms: 20,
            signals: &["SIGTERM", "SIGKILL"],
            err: Some("failed to exit even after SIGKILL"),
        },
        Case {
            name: "undeliverable SIGTERM",
            exit_on: None,
            wait_err: None,
            kill_err: &[("SIGTERM", "sandbox gone")],
            deadline_ms: 60_000,
            signals: &["SIGTERM"],
            err: Some("failed to propagate SIGTERM"),
        },
        Case {
            name: "failed wait",
            exit_on: Some("SIGTERM"),
            wait_err: Some("runsc wait exploded"),
            kill_err: &[],
            deadline_ms: 60_000,
            signals: &["SIGTERM"],
            err: Some("wait failed"),
        },
    ];
    for c in cases {
        let f = FakeRunsc {
            exit_on: c.exit_on,
            wait_err: c.wait_err,
            kill_err: c.kill_err.iter().copied().collect(),
            ..Default::default()
        };
        let got = run_kill(&f, c.deadline_ms).await;
        match (c.err, &got) {
            (None, Ok(())) => {}
            (Some(want), Err(e)) => assert!(e.to_string().contains(want), "{}: {e}", c.name),
            _ => panic!("{}: got {got:?}", c.name),
        }
        assert_eq!(f.signals(), c.signals, "{}", c.name);
    }
}

#[tokio::test]
async fn kill_container_honors_cancellation() {
    let f = std::sync::Arc::new(FakeRunsc::default());
    let cancel = tok();
    let (f2, c2) = (f.clone(), cancel.clone());
    tokio::spawn(async move {
        while f2.signals().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        c2.cancel();
    });
    let err = kill_container(
        &*f,
        "counter",
        Instant::now() + Duration::from_secs(60),
        KILL_TIMEOUT,
        &cancel,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Cancelled), "{err}");
    assert_eq!(f.signals(), ["SIGTERM"]);
}
