//! Pointing the bundled Kubernetes Secrets credential provider at the e2e
//! fixture Secret for the duration of a test.

use crate::cmd::{find_repo_root, kubectl};
use crate::harness::T;
use crate::install;
use crate::manifest::fixture_path;

/// Holds the fixture Secret; the only namespace the fixture policy lets the
/// probe hives resolve.
pub const CREDENTIAL_SECRETS_NAMESPACE: &str = "hivekeep-e2e-credinject-secrets";
/// Resolves to [`CREDENTIAL_INJECTION_TOKEN`] through the provider, for the
/// hives the fixture policy allows.
pub const CREDENTIAL_INJECTION_URI: &str =
    "hk-secret://k8s.io/default/hivekeep-e2e-credinject-secrets/api-token/token";
/// The fixture Secret's value: what an injected header carries after any
/// prefix.
pub const CREDENTIAL_INJECTION_TOKEN: &str = "e2e-cred-inject-token";

/// The fixture manifest (Secret plus provider policy), relative to this
/// crate.
pub const CREDINJECT_FIXTURE_MANIFEST: &str = "fixtures/credinject/credinject.yaml";
/// The policy the install ships, relative to the workspace root; put back
/// when a test passes.
pub const CREDENTIAL_PROVIDER_POLICY_MANIFEST: &str =
    "crates/hivekeep-setup/manifests/egress-credential-injection/namespace-policy.yaml";

/// Applies the credinject fixture and restarts the provider (it reads its
/// policy once at startup). When the test passes the fixture is removed, the
/// shipped policy put back and the provider restarted again; a failed test
/// keeps the fixture so the provider's logs can be read against it.
///
/// The provider itself and the gate's side of the connection are the
/// install's to configure; on a cluster installed without them the rollout
/// wait below fails.
pub fn configure_credential_provider(t: &T) {
    let root =
        find_repo_root().unwrap_or_else(|e| t.fatal(format!("finding the workspace root: {e}")));
    // The manifests pin the canonical namespace.
    let ns = install::SYSTEM_NAMESPACE;
    let deployment = format!("deployment/{}", install::CREDENTIAL_PROVIDER_DEPLOYMENT);
    let fixture = fixture_path(CREDINJECT_FIXTURE_MANIFEST)
        .to_string_lossy()
        .into_owned();
    let shipped = root
        .join(CREDENTIAL_PROVIDER_POLICY_MANIFEST)
        .to_string_lossy()
        .into_owned();
    kubectl(t, &["apply", "-f", &fixture]);
    let d = deployment.clone();
    t.cleanup(move |t| async move {
        if t.failed() {
            return;
        }
        kubectl(&t, &["delete", "--ignore-not-found", "-f", &fixture]);
        kubectl(&t, &["apply", "-f", &shipped]);
        // Not waiting keeps the new pod's startup out of the run; the old
        // pod is replaced once the new one is ready.
        kubectl(&t, &["-n", ns, "rollout", "restart", &d]);
    });
    kubectl(t, &["-n", ns, "rollout", "restart", &deployment]);
    kubectl(
        t,
        &["-n", ns, "rollout", "status", &deployment, "--timeout=3m"],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifests_exist() {
        assert!(fixture_path(CREDINJECT_FIXTURE_MANIFEST).exists());
        assert!(find_repo_root()
            .unwrap()
            .join(CREDENTIAL_PROVIDER_POLICY_MANIFEST)
            .exists());
        let fixture = std::fs::read_to_string(fixture_path(CREDINJECT_FIXTURE_MANIFEST)).unwrap();
        assert!(fixture.contains(CREDENTIAL_SECRETS_NAMESPACE));
        assert!(
            fixture.contains(CREDENTIAL_INJECTION_TOKEN)
                || fixture.contains(&crate::kube_util::b64(
                    CREDENTIAL_INJECTION_TOKEN.as_bytes()
                ))
        );
    }
}
