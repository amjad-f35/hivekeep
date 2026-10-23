//! A sandbox is sized to the limits its ActorTemplate declares, as seen from
//! inside the running sandbox.

mod ops_support;

use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::sandbox::fixture_name;
use hivekeep_e2e::suite::{clients, e2e_test};
use ops_support::*;
use serde::Deserialize;

const SIZING_TEMPLATE: &str = "probe-sized";
// The limits in probe-sized-template.yaml.tmpl; the suite asserts the
// sandbox observes exactly these.
const WANT_CPU: i64 = 2;
const WANT_MEM_BYTES: i64 = 512 * 1024 * 1024;

/// The fixture's hive (and pool namespace), suffixed per sandbox class.
fn sizing_hive() -> String {
    format!("{}-sizing", fixture_name("hivekeep-e2e"))
}

/// The probe's `/resources` reply.
#[derive(Debug, Default, Deserialize)]
struct ResourcesResponse {
    #[serde(default)]
    num_cpu: i64,
    #[serde(default)]
    mem_total_bytes: i64,
    #[serde(default)]
    mem_total_error: String,
    #[serde(default)]
    cpu_max: String,
    #[serde(default)]
    memory_max: String,
}

/// An ActorTemplate declaring resource limits yields a sandbox sized to
/// them: resume an Actor and ask it what compute envelope it sees. gVisor
/// sizes its kernel from the CPU quota and cgroup memory limit, the micro-VM
/// sizes the guest itself; the assertions hold for both.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_sizing_sandbox_observes_declared_limits() {
    e2e_test(
        "TestActorSizing_SandboxObservesDeclaredLimits",
        |t| async move {
            let bucket = bucket(&t);
            let hive = sizing_hive();
            let manifests = FixtureManifests {
                pool: "fixtures/probe/probe-sized.yaml.tmpl",
                template: "fixtures/probe/probe-sized-template.yaml.tmpl",
            };
            deploy_hive_fixture(
                &t,
                &clients(),
                manifests,
                &bucket,
                "sizing",
                &[],
                FixtureOptions::default(),
            )
            .await;

            let id = "sized-actor";
            create_actor(&t, &hive, id, object_ref(&hive, SIZING_TEMPLATE)).await;
            // Resume from the golden snapshot (the restore path).
            resume_actor(&t, &hive, id).await;
            let rc = router(&t).await;
            let got: ResourcesResponse = get_json(&t, &rc, &hive, id, "/resources").await;
            t.log(format!(
            "sandbox /resources: num_cpu={} mem_total_bytes={} cpu_max={:?} memory_max={:?} mem_total_error={:?}",
            got.num_cpu, got.mem_total_bytes, got.cpu_max, got.memory_max, got.mem_total_error
        ));
            // CPU: either runtime must expose exactly the declared count.
            if got.num_cpu != WANT_CPU {
                t.error(format!("sandbox NumCPU = {}, want {WANT_CPU} (declared limits.cpu={WANT_CPU}): sandbox not sized to actor limits", got.num_cpu));
            }
            // Memory: bounded by the limit. Both runtimes report somewhat less
            // (reserved overhead, guest kernel), never more; allow 10% above for
            // accounting and half the limit below for those overheads.
            if !got.mem_total_error.is_empty() {
                t.error(format!(
                    "probe could not read MemTotal: {}",
                    got.mem_total_error
                ));
            } else if got.mem_total_bytes > WANT_MEM_BYTES * 11 / 10 {
                t.error(format!("sandbox MemTotal = {} bytes, want <= ~{WANT_MEM_BYTES} (declared limits.memory=512Mi): memory limit not applied", got.mem_total_bytes));
            } else if got.mem_total_bytes < WANT_MEM_BYTES / 2 {
                t.error(format!(
                    "sandbox MemTotal = {} bytes, unexpectedly far below the declared 512Mi limit",
                    got.mem_total_bytes
                ));
            }
        },
    );
}

#[test]
fn hive_is_suffixed_per_class() {
    assert!(sizing_hive().starts_with("hivekeep-e2e") && sizing_hive().ends_with("-sizing"));
    let _ = NEEDS_CLUSTER;
}
