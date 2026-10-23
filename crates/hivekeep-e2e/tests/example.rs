//! The template every suite starts from: one test per file, run through the
//! shared suite setup (cluster clients and preflight checks), which skips
//! the whole binary when no cluster is configured.

use hivekeep_e2e::suite::e2e_test;

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn example() {
    e2e_test("TestExample", |t| async move {
        t.log("TestExample PASSED");
    });
}
