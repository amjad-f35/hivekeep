//! Runtime authorization of hivekeep-api on an install whose bootstrap
//! owners include the client ServiceAccount the e2e clients use, as the
//! default install does.
//!
//! The API's unit tests cover which role may make which call. This suite
//! covers what only an installed system shows: a user granted access
//! through an AccessPolicy using Hivekeep end to end, and the security
//! boundaries that depend on the install, real credentials, stored state,
//! and policy changes reaching every API replica.

mod flow;
mod helpers;
mod security_regression;

use hivekeep_e2e::env::check_env;
use hivekeep_e2e::suite::e2e_test;

/// Names the snapshot bucket the fixture templates store into.
const BUCKET_ENV: &str = "HK_E2E_BUCKET_NAME";

/// Deploys one fixture, the expensive part, and runs every check against
/// it. The subtests run in order: each sets the fixture hive's
/// AccessPolicy for its own principals.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn authz() {
    e2e_test("TestAuthz", |t| async move {
        let env =
            check_env(&[BUCKET_ENV]).unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let f = helpers::deploy_fixture(&t, &env[BUCKET_ENV]).await;
        let f = &f;
        t.run("BasicFlow", |t| async move {
            flow::test_basic_flow(&t, f).await
        })
        .await;
        t.run("SecurityRegression", |t| async move {
            security_regression::test_security_regression(&t, f).await
        })
        .await;
    });
}
