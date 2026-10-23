//! Request parking end to end: a one-worker pool is oversubscribed by two
//! Actors, so a request for the suspended one parks until the worker frees
//! (ParkThenServed) or the park budget runs out (BudgetExhaustion). It runs
//! with the router's default parking settings (5 s budget); behaviour that
//! depends on other settings is covered by unit tests, since the shared
//! router cannot be reconfigured per test.

mod ops_support;

use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::dataplane::{Dataplane, ParkingObserver};
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::sandbox::fixture_name;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::template_ref;
use hivekeep_proto::api::v1 as pb;
use ops_support::*;

/// The park budget the deployed router runs with (its default). The timing
/// assertions are windows around it: wide enough for jitter, narrow enough
/// to prove parking happened and that the router, not a proxy timeout,
/// gave the verdict.
const ROUTER_PARK_BUDGET: Duration = Duration::from_secs(5);

fn parking_hive() -> String {
    format!("{}-parking", fixture_name("hivekeep-e2e"))
}

async fn wait_for_parked_count(t: &T, parking: &ParkingObserver, cond: impl Fn(i64) -> bool) {
    if let Err(e) = parking.wait_for_count(cond).await {
        t.fatal(format!("waiting for parking gauge: {e}"));
    }
}

async fn whoami(
    rc: &RouterClient,
    name: &str,
) -> Result<hivekeep_e2e::router_client::RouterResponse, String> {
    rc.get(&ActorRef::new(parking_hive(), name), "/whoami")
        .await
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn request_parking() {
    e2e_test("TestRequestParking", |t| async move {
        let hive = parking_hive();
        // One worker, two Actors: the minimal deterministic oversubscription.
        let bucket = bucket(&t);
        let manifests = FixtureManifests {
            pool: "fixtures/probe/probe-parking.yaml.tmpl",
            template: "fixtures/probe/probe-parking-template.yaml.tmpl",
        };
        let (_, templates) = deploy_hive_fixture(
            &t,
            &clients(),
            manifests,
            &bucket,
            "parking",
            &[],
            FixtureOptions::default(),
        )
        .await;
        let at = templates
            .into_iter()
            .next()
            .unwrap_or_else(|| t.fatal("parking fixture created no template"));
        let (actor_a, actor_b) = ("parked-a", "parked-b");
        for name in [actor_a, actor_b] {
            create_actor(&t, &hive, name, template_ref(&at)).await;
        }
        let rc = router(&t).await;
        let dataplane = Dataplane::current();
        let parking = dataplane
            .new_parking_observer(&clients().kube())
            .await
            .unwrap_or_else(|e| t.fatal(format!("creating parking observer: {e}")));

        let (rc, parking, hive) = (&rc, &parking, hive.as_str());
        t.run("ParkThenServed", |t| async move {
            let t = &t;
            // Occupy the only worker with Actor A.
            resume_once(t, hive, actor_a).await;
            wait_for_state(t, hive, actor_a, pb::ActorState::Running, Duration::from_secs(60), Duration::from_secs(1)).await;

            // Requesting B parks: the pool is full. Freeing the worker is
            // asynchronous (the suspend call returns before the suspend
            // completes, and a micro-VM snapshot upload can outlast the park
            // budget), so a budget-exhausted 503 while the suspend is still
            // in flight is correct and the request is retried; a stranded
            // worker fails every attempt.
            let mut attempt = 1;
            let (resp, elapsed) = loop {
                let start = Instant::now();
                let result = if attempt == 1 {
                    // Free the worker only once the request is observably
                    // parked: the gauge, not a sleep, synchronizes.
                    let (r, ()) = tokio::join!(whoami(rc, actor_b), async {
                        wait_for_parked_count(t, parking, |active| active >= 1).await;
                        suspend_actor(t, hive, actor_a).await;
                    });
                    r
                } else {
                    whoami(rc, actor_b).await
                };
                let elapsed = start.elapsed();
                let resp = result.unwrap_or_else(|e| t.fatal(format!("parked request failed transport-level: {e}")));
                if dataplane.is_retryable_parking_budget_exhaustion(resp.status, &resp.text()) && attempt < 3 {
                    t.log(format!("attempt {attempt} budget-exhausted while the worker was still freeing (HTTP {} after {elapsed:?}); retrying", resp.status));
                    attempt += 1;
                    continue;
                }
                break (resp, elapsed);
            };
            if resp.status != 200 {
                t.fatal(format!("parked request: status = {} (body {:?}), want 200", resp.status, resp.text()));
            }
            // Served by the Actor it named, not whichever held the worker.
            if !resp.text().contains(actor_b) {
                t.error(format!("parked request body = {:?}, want the probe to name {actor_b:?}", resp.text()));
            }
            // No upper bound: a 200 proves the router answered before the
            // proxy's timeout, and a slow restore under contention passes.
            t.log(format!("parked request served after {elapsed:?}"));

            // B must really converge and serve a warm follow-up; an Actor
            // stranded in RESUMING with its worker claimed would 503 it.
            wait_for_state(t, hive, actor_b, pb::ActorState::Running, Duration::from_secs(60), Duration::from_secs(1)).await;
            let follow = whoami(rc, actor_b).await.unwrap_or_else(|e| t.fatal(format!("follow-up request failed transport-level: {e}")));
            if follow.status != 200 {
                t.error(format!("follow-up request: status = {} (body {:?}), want 200 from the resumed actor", follow.status, follow.text()));
            }
            // The slot is released once served.
            wait_for_parked_count(t, parking, |active| active == 0).await;
        })
        .await;

        t.run("BudgetExhaustion", |t| async move {
            let t = &t;
            // B runs on the only worker and nothing frees it; A is suspended.
            // Requesting A parks for the whole budget and gets the capacity
            // verdict from the router.
            let start = Instant::now();
            let resp = whoami(rc, actor_a).await;
            let elapsed = start.elapsed();
            let resp = resp.unwrap_or_else(|e| t.fatal(format!("budget-exhausted request failed transport-level: {e}")));
            let want = dataplane.parking_budget_status();
            if resp.status != want {
                t.fatal(format!("status = {} (body {:?}), want {want}", resp.status, resp.text()));
            }
            if want == 503 {
                if !resp.text().contains("no worker has room for the actor") {
                    t.error(format!("body = {:?}, want the router's capacity verdict", resp.text()));
                }
                let ct = resp.headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
                if ct != "text/plain" {
                    t.error(format!("content-type = {ct:?}, want text/plain"));
                }
            }
            // Fail-fast would answer in milliseconds; a proxy timeout lands
            // at budget + 5 s.
            if elapsed < ROUTER_PARK_BUDGET - Duration::from_secs(1) {
                t.error(format!("503 after {elapsed:?}: too fast, the request did not park for the budget"));
            }
            if elapsed > ROUTER_PARK_BUDGET + Duration::from_secs(4) {
                t.error(format!("503 after {elapsed:?}: too slow, likely a proxy timeout rather than the router's verdict"));
            }
            t.log(format!("budget exhausted after {elapsed:?} with HTTP {want}"));
        })
        .await;
    });
}

/// A plain resume (no capacity retries): the pool must have room for A.
async fn resume_once(t: &T, hive: &str, name: &str) {
    if let Err(e) = clients()
        .api()
        .resume_actor(pb::ResumeActorRequest {
            actor: Some(object_ref(hive, name)),
        })
        .await
    {
        t.fatal(format!("failed to resume actor {name:?}: {e}"));
    }
}
