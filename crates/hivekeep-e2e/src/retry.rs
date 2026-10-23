//! Retrying API calls through the transient states a busy control plane
//! reports.

use std::future::Future;
use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as pb;
use tonic::{Code, Status};

use crate::clients::Clients;
use crate::harness::T;

/// How long [`retry_aborted`] keeps retrying.
pub const ABORTED_RETRY_WAIT: Duration = Duration::from_secs(30);
/// How long [`resume_actor_await_capacity`] waits for a worker to free up.
/// Neighbouring suites hold workers for the length of a test, so this
/// covers several of their Actor lifetimes.
pub const RESUME_CAPACITY_WAIT: Duration = Duration::from_secs(90);

/// Calls `rpc` until it returns anything but `Aborted`, or the budget runs
/// out. The control plane returns Aborted while another operation holds the
/// Actor's lease (such as applying the keeper's report of a background
/// snapshot upload), which can come at any time after an asynchronous
/// suspend.
pub async fn retry_aborted<R, F, Fut>(t: &T, what: &str, mut rpc: F) -> Result<R, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, Status>>,
{
    retry_codes(
        t,
        what,
        &[Code::Aborted],
        ABORTED_RETRY_WAIT,
        Duration::from_secs(1),
        &mut rpc,
    )
    .await
}

async fn retry_codes<R, F, Fut>(
    t: &T,
    what: &str,
    codes: &[Code],
    budget: Duration,
    pause: Duration,
    rpc: &mut F,
) -> Result<R, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, Status>>,
{
    let deadline = Instant::now() + budget;
    loop {
        match rpc().await {
            Err(s) if codes.contains(&s.code()) && Instant::now() < deadline => {
                t.log(format!("{what}: {}: {}; retrying", s.code(), s.message()));
                tokio::time::sleep(pause).await;
            }
            other => return other,
        }
    }
}

/// Resumes the Actor, retrying while its pool is saturated
/// (`ResourceExhausted`, which the control plane expects callers to wait
/// out) or its lease is held (`Aborted`). Suites sharing pools make
/// saturation a normal state here, not a failure. Each retry is logged so a
/// pass that had to wait stays visible.
pub async fn resume_actor_await_capacity(
    t: &T,
    clients: &Clients,
    req: pb::ResumeActorRequest,
) -> Result<pb::ResumeActorResponse, Status> {
    let actor = req.actor.clone().unwrap_or_default();
    let what = format!("ResumeActor {}/{}", actor.hive, actor.name);
    let mut rpc = || {
        let mut api = clients.api();
        let req = req.clone();
        async move { api.resume_actor(req).await.map(|r| r.into_inner()) }
    };
    retry_codes(
        t,
        &what,
        &[Code::ResourceExhausted, Code::Aborted],
        RESUME_CAPACITY_WAIT,
        Duration::from_secs(2),
        &mut rpc,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::run_test;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_only_the_listed_codes() {
        run_test("Retry", |t| async move {
            let calls = AtomicUsize::new(0);
            let mut rpc = || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        Err(Status::aborted("lease held"))
                    } else {
                        Ok(n)
                    }
                }
            };
            let got = retry_codes(
                &t,
                "op",
                &[Code::Aborted],
                Duration::from_secs(5),
                Duration::from_millis(1),
                &mut rpc,
            )
            .await;
            assert_eq!(got.unwrap(), 2);

            let mut other = || async { Err::<(), _>(Status::not_found("gone")) };
            let got = retry_codes(
                &t,
                "op",
                &[Code::Aborted],
                Duration::from_secs(5),
                Duration::from_millis(1),
                &mut other,
            )
            .await;
            assert_eq!(got.unwrap_err().code(), Code::NotFound);

            let mut forever = || async { Err::<(), _>(Status::aborted("still")) };
            let got = retry_codes(
                &t,
                "op",
                &[Code::Aborted],
                Duration::from_millis(5),
                Duration::from_millis(1),
                &mut forever,
            )
            .await;
            assert_eq!(got.unwrap_err().code(), Code::Aborted);
        })
        .await;
    }
}
