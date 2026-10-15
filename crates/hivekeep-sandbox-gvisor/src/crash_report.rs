//! Telling the control plane, through the keeper, that an Actor's sandbox
//! died on its own.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::ports::CrashReporter;
use async_trait::async_trait;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::keeper::v1 as kpb;

use crate::keeper_dial::KeeperDialConfig;
use crate::register::{INITIAL_BACKOFF, MAX_BACKOFF, REPORT_TIMEOUT};

/// The control plane's limit on a crash message.
pub const MAX_CRASH_MESSAGE_BYTES: usize = 4096;

/// Keeps `message` within the control plane's limit, on a character
/// boundary.
pub fn truncate_message(message: &str) -> String {
    if message.len() <= MAX_CRASH_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut cut = MAX_CRASH_MESSAGE_BYTES;
    while !message.is_char_boundary(cut) {
        cut -= 1;
    }
    message[..cut].to_string()
}

/// The keeper call a crash report makes.
#[async_trait]
pub trait CrashSink: Send + Sync {
    /// Sends one report.
    async fn report_actor_crashed(
        &self,
        req: kpb::ReportActorCrashedRequest,
    ) -> std::result::Result<(), tonic::Status>;
}

/// Reports until the control plane answers. A crash stays true, so it is
/// worth retrying for as long as the caller allows; a refusal (the Actor
/// moved on, or an operation in flight owns it) is an answer and is logged.
pub async fn report_crashed_with_retry(
    sink: &dyn CrashSink,
    a: &ActorAttribution,
    message: &str,
    mut backoff: Duration,
) -> Result<()> {
    let req = kpb::ReportActorCrashedRequest {
        actor_hive: a.actor.hive.clone(),
        actor_name: a.actor.name.clone(),
        actor_uid: a.uid.clone(),
        message: truncate_message(message),
    };
    loop {
        let err = match sink.report_actor_crashed(req.clone()).await {
            Ok(()) => return Ok(()),
            Err(s) => s,
        };
        match err.code() {
            tonic::Code::NotFound | tonic::Code::FailedPrecondition => {
                tracing::info!(actor_uid = %a.uid, err = %err, "Control plane declined the actor crash report");
                return Ok(());
            }
            tonic::Code::Unavailable
            | tonic::Code::Aborted
            | tonic::Code::ResourceExhausted
            | tonic::Code::DeadlineExceeded => {}
            _ => return Err(Error::msg(format!("reporting actor crash: {err}"))),
        }
        tracing::warn!(in_ = ?backoff, err = %err, "Retrying actor crash report");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Reports crashes to the keeper over its worker-support socket.
#[derive(Debug, Clone)]
pub struct KeeperCrashReporter {
    dial: KeeperDialConfig,
}

impl KeeperCrashReporter {
    /// Loads the credentials once, so a misconfiguration surfaces at startup.
    pub fn new(dial: KeeperDialConfig) -> std::io::Result<Arc<Self>> {
        dial.tls_config()?;
        Ok(Arc::new(KeeperCrashReporter { dial }))
    }
}

#[async_trait]
impl CrashSink for KeeperCrashReporter {
    async fn report_actor_crashed(
        &self,
        req: kpb::ReportActorCrashedRequest,
    ) -> std::result::Result<(), tonic::Status> {
        let mut c = self
            .dial
            .connect()
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
        let mut r = tonic::Request::new(req);
        r.set_timeout(REPORT_TIMEOUT);
        c.report_actor_crashed(r).await.map(drop)
    }
}

#[async_trait]
impl CrashReporter for KeeperCrashReporter {
    async fn report_crashed(&self, a: &ActorAttribution, message: &str) -> Result<()> {
        report_crashed_with_retry(self, a, message, INITIAL_BACKOFF).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_core::resource::ActorRef;
    use parking_lot::Mutex;

    struct FakeSink {
        errs: Mutex<Vec<tonic::Status>>,
        got: Mutex<Vec<kpb::ReportActorCrashedRequest>>,
    }

    #[async_trait]
    impl CrashSink for FakeSink {
        async fn report_actor_crashed(
            &self,
            req: kpb::ReportActorCrashedRequest,
        ) -> std::result::Result<(), tonic::Status> {
            self.got.lock().push(req);
            let mut e = self.errs.lock();
            if e.is_empty() {
                Ok(())
            } else {
                Err(e.remove(0))
            }
        }
    }

    fn actor() -> ActorAttribution {
        ActorAttribution {
            actor: ActorRef::new("team-a", "actor-1"),
            uid: "01234567-89ab-cdef-0123-456789abcdef".into(),
            template_hive: String::new(),
            template_name: String::new(),
        }
    }

    #[tokio::test]
    async fn reports_and_retries() {
        for (name, errs, want_err, want_calls) in [
            ("accepted", vec![], false, 1),
            (
                "retries until delivered",
                vec![tonic::Status::unavailable(""), tonic::Status::aborted("")],
                false,
                3,
            ),
            (
                "refusal is an answer",
                vec![tonic::Status::failed_precondition("")],
                false,
                1,
            ),
            (
                "bad request is not retried",
                vec![tonic::Status::invalid_argument("")],
                true,
                1,
            ),
        ] {
            let sink = FakeSink {
                errs: Mutex::new(errs),
                got: Mutex::default(),
            };
            let r = report_crashed_with_retry(
                &sink,
                &actor(),
                "cloud-hypervisor exited",
                Duration::from_millis(1),
            )
            .await;
            assert_eq!(r.is_err(), want_err, "{name}");
            let got = sink.got.lock();
            assert_eq!(got.len(), want_calls, "{name}");
            assert_eq!(
                got[0],
                kpb::ReportActorCrashedRequest {
                    actor_hive: "team-a".into(),
                    actor_name: "actor-1".into(),
                    actor_uid: actor().uid,
                    message: "cloud-hypervisor exited".into(),
                }
            );
        }
    }

    #[tokio::test]
    async fn gives_up_with_its_deadline() {
        let sink = FakeSink {
            errs: Mutex::new((0..1000).map(|_| tonic::Status::unavailable("")).collect()),
            got: Mutex::default(),
        };
        let r = tokio::time::timeout(
            Duration::from_millis(50),
            report_crashed_with_retry(&sink, &actor(), "x", Duration::from_millis(1)),
        )
        .await;
        assert!(r.is_err(), "the retry outlived its deadline");
    }

    #[test]
    fn truncates_on_a_character_boundary() {
        let long = "é".repeat(MAX_CRASH_MESSAGE_BYTES);
        let got = truncate_message(&long);
        assert!(got.len() <= MAX_CRASH_MESSAGE_BYTES);
        assert!(long.starts_with(&got));
    }
}
