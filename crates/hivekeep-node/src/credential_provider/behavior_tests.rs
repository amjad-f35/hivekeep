//! Behaviour of the credential provider config loader, image matching, and
//! the keychain running real plugin executables.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use super::*;
use crate::clock::Clock;
use crate::context::Ctx;
use crate::test_support::LogCapture;

// ---------------------------------------------------------------- helpers

/// A clock that moves only when told to.
struct ManualClock(Mutex<DateTime<Utc>>);

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(ManualClock(Mutex::new(
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        )))
    }
    fn advance(&self, d: Duration) {
        let mut t = self.0.lock();
        *t += chrono::Duration::from_std(d).unwrap();
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock()
    }
}

fn testdata(script: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/credential_provider")
        .join(script)
}

/// Links `<dir>/<name>` to a checked-in script. Linking avoids writing and
/// then executing a file while other tests fork (ETXTBSY).
fn link_plugin(dir: &Path, name: &str, script: &str) {
    std::os::unix::fs::symlink(testdata(script), dir.join(name)).unwrap();
}

/// A plugin that records each request and replies with `response`. Returns
/// the requests file.
fn fake_plugin(dir: &Path, name: &str, response: &str) -> PathBuf {
    std::fs::write(dir.join(format!("{name}.response")), response).unwrap();
    link_plugin(dir, name, "fake-provider.sh");
    dir.join(format!("{name}.requests"))
}

fn failing_plugin(dir: &Path, name: &str) {
    link_plugin(dir, name, "failing-provider.sh");
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let p = dir.join("config.yaml");
    std::fs::write(&p, contents).unwrap();
    p
}

fn count_requests(requests: &Path) -> usize {
    std::fs::read_to_string(requests)
        .unwrap()
        .split_whitespace()
        .count()
}

const GCP_LIKE_CONFIG: &str = r#"kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: fake-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages:
    - "gcr.io"
    - "*.gcr.io"
    - "*.pkg.dev"
    args:
    - get-credentials
    defaultCacheDuration: 1m
"#;

const TWO_PROVIDER_CONFIG: &str = r#"kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: first-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
  - name: second-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
"#;

fn keychain(dir: &Path, config: &str) -> Keychain {
    Keychain::new(&write_config(dir, config), dir).unwrap()
}

async fn resolve(kc: &Keychain, image: &str) -> Option<AuthConfig> {
    kc.resolve(&Ctx::background(), image).await.unwrap()
}

async fn usernames(kc: &Keychain, image: &str) -> Vec<String> {
    kc.candidates(&Ctx::background(), image)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.username)
        .collect()
}

// ----------------------------------------------------------------- config

const GKE_CONFIG: &str = r#"kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: auth-provider-gcp
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages:
    - "container.cloud.google.com"
    - "gcr.io"
    - "*.gcr.io"
    - "*.pkg.dev"
    args:
    - get-credentials
    - --v=3
    defaultCacheDuration: 1m
"#;

#[test]
fn load_config_cloud_node() {
    let d = tempfile::tempdir().unwrap();
    let plugins = load_config(
        &write_config(d.path(), GKE_CONFIG),
        Path::new("/home/kubernetes/bin"),
    )
    .unwrap();
    assert_eq!(plugins.len(), 1);
    let p = &plugins[0];
    assert_eq!(
        p.path,
        PathBuf::from("/home/kubernetes/bin/auth-provider-gcp")
    );
    assert_eq!(p.default_cache_duration(), Duration::from_secs(60));
    assert_eq!(p.args(), ["get-credentials", "--v=3"]);
    for (image, want) in [
        ("gcr.io/proj/img", true),
        ("us.gcr.io/proj/img", true),
        ("us-central1-docker.pkg.dev/proj/repo/img", true),
        ("docker.io/library/busybox", false),
        ("quay.io/proj/img", false),
    ] {
        assert_eq!(p.claims(image).unwrap(), want, "{image}");
    }
}

#[test]
fn load_config_rejects_invalid_config() {
    let cases = [
        (
            "wrong kind",
            "kind: KubeletConfiguration\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: p\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n",
        ),
        (
            "wrong config apiVersion",
            "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1beta1\nproviders:\n  - name: p\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n",
        ),
        ("not yaml", "this: is: not: valid: yaml:\n"),
    ];
    for (name, config) in cases {
        let d = tempfile::tempdir().unwrap();
        assert!(
            load_config(&write_config(d.path(), config), d.path()).is_err(),
            "{name}: accepted"
        );
    }
}

#[test]
fn load_config_skips_unusable_providers() {
    let cases = [
        (
            "missing name",
            "  - apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"quay.io\"]\n    defaultCacheDuration: 1m\n",
        ),
        (
            "name escapes the bin dir",
            "  - name: ../../bin/sh\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"quay.io\"]\n    defaultCacheDuration: 1m\n",
        ),
        (
            "no matchImages",
            "  - name: bad\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    defaultCacheDuration: 1m\n",
        ),
        (
            "missing defaultCacheDuration",
            "  - name: bad\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"quay.io\"]\n",
        ),
        (
            "unsupported exec apiVersion",
            "  - name: bad\n    apiVersion: credentialprovider.kubelet.k8s.io/v1beta1\n    matchImages: [\"quay.io\"]\n    defaultCacheDuration: 1m\n",
        ),
        (
            "service account token attributes",
            "  - name: bad\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"quay.io\"]\n    defaultCacheDuration: 1m\n    tokenAttributes:\n      serviceAccountTokenAudience: aud\n      cacheType: Token\n      requireServiceAccount: true\n",
        ),
        (
            "duplicate of the keeper",
            "  - name: keeper\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"quay.io\"]\n    defaultCacheDuration: 1m\n",
        ),
    ];
    for (name, bad) in cases {
        let d = tempfile::tempdir().unwrap();
        let config = format!(
            "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: keeper\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n{bad}"
        );
        let plugins = load_config(&write_config(d.path(), &config), d.path())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let names: Vec<_> = plugins.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["keeper"], "{name}");
        assert!(plugins[0].claims("gcr.io/proj/img").unwrap(), "{name}");
    }
}

#[test]
fn load_config_empty_when_no_provider_usable() {
    let cases = [
        (
            "no providers declared",
            "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders: []\n",
        ),
        (
            "only provider is unusable",
            "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: bad\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n    tokenAttributes:\n      serviceAccountTokenAudience: aud\n      cacheType: Token\n      requireServiceAccount: true\n",
        ),
    ];
    for (name, config) in cases {
        let d = tempfile::tempdir().unwrap();
        let plugins = load_config(&write_config(d.path(), config), d.path())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(plugins.is_empty(), "{name}");
    }
}

#[test]
fn load_config_missing_file() {
    let d = tempfile::tempdir().unwrap();
    assert!(load_config(&d.path().join("absent.yaml"), Path::new("/bin")).is_err());
}

// ---------------------------------------------------------------- matching

#[test]
fn matches_image_cases() {
    let cases = [
        ("exact registry", "gcr.io", "gcr.io/proj/img", true),
        ("registry only, no repo", "gcr.io", "gcr.io", true),
        ("different registry", "gcr.io", "quay.io/proj/img", false),
        ("wildcard subdomain", "*.gcr.io", "us.gcr.io/proj/img", true),
        (
            "wildcard does not match bare domain",
            "*.gcr.io",
            "gcr.io/proj/img",
            false,
        ),
        (
            "wildcard spans one segment only",
            "*.gcr.io",
            "a.b.gcr.io/proj/img",
            false,
        ),
        (
            "pkg.dev regional",
            "*.pkg.dev",
            "us-central1-docker.pkg.dev/proj/repo/img",
            true,
        ),
        (
            "partial subdomain glob",
            "app*.k8s.io",
            "appfoo.k8s.io/img",
            true,
        ),
        ("top level domain glob", "k8s.*", "k8s.io/img", true),
        (
            "multiple globs",
            "*.*.registry.io",
            "a.b.registry.io/img",
            true,
        ),
        (
            "path prefix matches",
            "registry.io/path",
            "registry.io/path/deeper/img",
            true,
        ),
        (
            "path prefix must match exactly",
            "registry.io/path",
            "registry.io/other/img",
            false,
        ),
        (
            "trailing slash in pattern",
            "registry.io/team/",
            "registry.io/team/img",
            true,
        ),
        (
            "path is a string prefix",
            "registry.io/team",
            "registry.io/teammate/img",
            true,
        ),
        (
            "image shallower than pattern",
            "registry.io/a/b/c",
            "registry.io/a",
            false,
        ),
        (
            "matching port",
            "registry.io:8080/path",
            "registry.io:8080/path/img",
            true,
        ),
        (
            "mismatched port",
            "registry.io:8080/path",
            "registry.io:9090/path/img",
            false,
        ),
        (
            "pattern port, image none",
            "registry.io:8080",
            "registry.io/img",
            false,
        ),
        (
            "image port, pattern none",
            "registry.io",
            "registry.io:8080/img",
            false,
        ),
        (
            "localhost registry",
            "localhost:5000",
            "localhost:5000/img",
            true,
        ),
    ];
    for (name, glob, image, want) in cases {
        assert_eq!(matches_image(glob, image).unwrap(), want, "{name}");
    }
}

#[test]
fn matching_auth_keys_cases() {
    let cases: [(&str, &[&str], &str, &[&str]); 5] = [
        ("no keys", &[], "gcr.io/proj/img", &[]),
        ("no matching key", &["quay.io"], "gcr.io/proj/img", &[]),
        (
            "single match",
            &["*.pkg.dev"],
            "us-docker.pkg.dev/proj/repo/img",
            &["*.pkg.dev"],
        ),
        (
            "concrete key before wildcard",
            &["*.pkg.dev", "us-docker.pkg.dev"],
            "us-docker.pkg.dev/proj/repo/img",
            &["us-docker.pkg.dev", "*.pkg.dev"],
        ),
        (
            "longer path before shorter",
            &["gcr.io", "gcr.io/proj", "quay.io"],
            "gcr.io/proj/img",
            &["gcr.io/proj", "gcr.io"],
        ),
    ];
    for (name, keys, image, want) in cases {
        let auth: BTreeMap<String, ()> = keys.iter().map(|k| (k.to_string(), ())).collect();
        let got = matching_auth_keys(auth.keys(), image).unwrap();
        assert_eq!(got, want, "{name}");
    }
}

#[test]
fn registry_of_cases() {
    for (image, want) in [
        ("gcr.io/proj/img", "gcr.io"),
        ("us-docker.pkg.dev/proj/repo/img", "us-docker.pkg.dev"),
        ("localhost:5000/img", "localhost:5000"),
        ("gcr.io", "gcr.io"),
    ] {
        assert_eq!(registry_of(image), want, "{image}");
    }
}

#[test]
fn repository_of_strips_tag_and_digest() {
    for (image, want) in [
        ("gcr.io/proj/img:latest", "gcr.io/proj/img"),
        ("gcr.io/proj/img@sha256:abc", "gcr.io/proj/img"),
        ("gcr.io/proj/img:v1@sha256:abc", "gcr.io/proj/img"),
        ("localhost:5000/img", "localhost:5000/img"),
        ("localhost:5000/img:1", "localhost:5000/img"),
        ("gcr.io", "gcr.io"),
    ] {
        assert_eq!(repository_of(image), want, "{image}");
    }
}

// ---------------------------------------------------------------- keychain

#[tokio::test]
async fn keychain_resolves_matching_image() {
    let d = tempfile::tempdir().unwrap();
    let requests = fake_plugin(
        d.path(),
        "fake-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"*.pkg.dev":{"username":"_token","password":"ya29.fake"}}}"#,
    );
    let kc = keychain(d.path(), GCP_LIKE_CONFIG);
    let got = resolve(&kc, "us-central1-docker.pkg.dev/proj/repo/img:latest")
        .await
        .unwrap();
    assert_eq!(
        (got.username.as_str(), got.password.as_str()),
        ("_token", "ya29.fake")
    );

    let raw = std::fs::read_to_string(requests).unwrap();
    let req: serde_json::Value = serde_json::from_str(raw.trim()).unwrap();
    assert_eq!(req["kind"], "CredentialProviderRequest");
    assert_eq!(req["apiVersion"], SUPPORTED_API_VERSION);
    // The repository without its tag: the protocol's cache granularity.
    assert_eq!(req["image"], "us-central1-docker.pkg.dev/proj/repo/img");
}

#[tokio::test]
async fn keychain_skips_unmatched_image() {
    let d = tempfile::tempdir().unwrap();
    // Would fail loudly if it ever ran.
    failing_plugin(d.path(), "fake-provider");
    let kc = keychain(d.path(), GCP_LIKE_CONFIG);
    assert_eq!(resolve(&kc, "docker.io/library/busybox:latest").await, None);
}

#[tokio::test]
async fn keychain_anonymous_when_plugin_returns_no_auth() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "fake-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry"}"#,
    );
    let kc = keychain(d.path(), GCP_LIKE_CONFIG);
    assert_eq!(resolve(&kc, "gcr.io/proj/img:latest").await, None);
}

#[tokio::test]
async fn keychain_skips_failing_plugin() {
    type Setup = fn(&Path);
    let cases: [(&str, Setup, &str); 2] = [
        (
            "plugin exits non-zero",
            |d| failing_plugin(d, "fake-provider"),
            "metadata server unreachable",
        ),
        (
            "response at an unrequested apiVersion",
            |d| {
                fake_plugin(
                    d,
                    "fake-provider",
                    r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1beta1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"u","password":"p"}}}"#,
                );
            },
            "v1beta1",
        ),
    ];
    for (name, setup, want_log) in cases {
        let logs = LogCapture::start();
        let d = tempfile::tempdir().unwrap();
        setup(d.path());
        let kc = keychain(d.path(), GCP_LIKE_CONFIG);
        assert_eq!(resolve(&kc, "gcr.io/proj/img:latest").await, None, "{name}");
        assert!(
            logs.text().contains(want_log),
            "{name}: logs do not mention {want_log:?}:\n{}",
            logs.text()
        );
    }
}

#[tokio::test]
async fn keychain_falls_through_to_second_provider() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "first-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{}}"#,
    );
    fake_plugin(
        d.path(),
        "second-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"second","password":"creds"}}}"#,
    );
    let kc = keychain(d.path(), TWO_PROVIDER_CONFIG);
    assert_eq!(
        resolve(&kc, "gcr.io/proj/img:latest")
            .await
            .unwrap()
            .username,
        "second"
    );
}

#[tokio::test]
async fn keychain_candidates_combine_providers() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "first-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"first","password":"p"},"*.io":{"username":"shared","password":"p"}}}"#,
    );
    fake_plugin(
        d.path(),
        "second-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"second","password":"p"},"gcr.io/proj":{"username":"specific","password":"p"},"*.io":{"username":"shared","password":"p"}}}"#,
    );
    let kc = keychain(d.path(), TWO_PROVIDER_CONFIG);
    assert_eq!(
        usernames(&kc, "gcr.io/proj/img:latest").await,
        ["specific", "first", "second", "shared"]
    );
    assert_eq!(
        resolve(&kc, "gcr.io/proj/img:latest")
            .await
            .unwrap()
            .username,
        "specific"
    );
}

#[tokio::test]
async fn keychain_candidates_deduplicate() {
    let d = tempfile::tempdir().unwrap();
    for name in ["first-provider", "second-provider"] {
        fake_plugin(
            d.path(),
            name,
            r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"same","password":"p"}}}"#,
        );
    }
    let kc = keychain(d.path(), TWO_PROVIDER_CONFIG);
    assert_eq!(usernames(&kc, "gcr.io/proj/img").await, ["same"]);
}

#[tokio::test]
async fn keychain_candidates_skip_invalid_auth_key() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "first-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"first","password":"p"}}}"#,
    );
    fake_plugin(
        d.path(),
        "second-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io:badport":{"username":"second","password":"p"}}}"#,
    );
    let kc = keychain(d.path(), TWO_PROVIDER_CONFIG);
    assert_eq!(usernames(&kc, "gcr.io/proj/img").await, ["first"]);
}

#[tokio::test]
async fn keychain_candidates_skip_failing_provider() {
    let d = tempfile::tempdir().unwrap();
    failing_plugin(d.path(), "first-provider");
    fake_plugin(
        d.path(),
        "second-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"second","password":"p"}}}"#,
    );
    let kc = keychain(d.path(), TWO_PROVIDER_CONFIG);
    assert_eq!(usernames(&kc, "gcr.io/proj/img").await, ["second"]);
}

#[tokio::test]
async fn keychain_caching() {
    let cases = [
        (
            "registry key reuses across repositories",
            "Registry",
            "",
            "gcr.io/proj/one:latest",
            "gcr.io/proj/two:latest",
            1,
        ),
        (
            "registry key does not span registries",
            "Registry",
            "",
            "gcr.io/proj/one:latest",
            "us.gcr.io/proj/one:latest",
            2,
        ),
        (
            "image key does not span repositories",
            "Image",
            "",
            "gcr.io/proj/one:latest",
            "gcr.io/proj/two:latest",
            2,
        ),
        (
            "image key reuses for the same repository",
            "Image",
            "",
            "gcr.io/proj/one:latest",
            "gcr.io/proj/one:other",
            1,
        ),
        (
            "global key spans registries",
            "Global",
            "",
            "gcr.io/proj/one:latest",
            "us.gcr.io/other/two:latest",
            1,
        ),
        // Zero is the providers' back-compat default rather than a considered
        // answer, so it is raised to the minimum instead of running the
        // plugin on every pull.
        (
            "zero duration is cached anyway",
            "Registry",
            r#""cacheDuration": "0s","#,
            "gcr.io/proj/one:latest",
            "gcr.io/proj/one:latest",
            1,
        ),
        (
            "unknown key type disables caching",
            "Nonsense",
            "",
            "gcr.io/proj/one:latest",
            "gcr.io/proj/one:latest",
            2,
        ),
    ];
    for (name, key_type, duration, first, second, want_execs) in cases {
        let d = tempfile::tempdir().unwrap();
        let requests = fake_plugin(
            d.path(),
            "fake-provider",
            &format!(
                r#"{{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"{key_type}",{duration}"auth":{{"*.gcr.io":{{"username":"u","password":"p"}},"gcr.io":{{"username":"u","password":"p"}}}}}}"#
            ),
        );
        let kc = keychain(d.path(), GCP_LIKE_CONFIG);
        for image in [first, second] {
            kc.resolve(&Ctx::background(), image).await.unwrap();
        }
        assert_eq!(count_requests(&requests), want_execs, "{name}");
    }
}

#[tokio::test]
async fn keychain_cache_expires() {
    let d = tempfile::tempdir().unwrap();
    let requests = fake_plugin(
        d.path(),
        "fake-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"u","password":"p"}}}"#,
    );
    let clock = ManualClock::new();
    let kc = Keychain::with_clock(
        &write_config(d.path(), GCP_LIKE_CONFIG),
        d.path(),
        clock.clone(),
    )
    .unwrap();
    let image = "gcr.io/proj/img:latest";
    resolve(&kc, image).await;
    // The default cache duration is one minute; just short of it still hits.
    clock.advance(Duration::from_secs(59));
    resolve(&kc, image).await;
    assert_eq!(count_requests(&requests), 1);
    clock.advance(Duration::from_secs(2));
    resolve(&kc, image).await;
    assert_eq!(count_requests(&requests), 2);
}

#[tokio::test]
async fn keychain_cached_entry_selects_per_image() {
    // (name, cacheKeyType, auth map, image -> wanted username)
    type Case<'a> = (&'a str, &'a str, &'a str, &'a [(&'a str, &'a str)]);
    let cases: [Case; 3] = [
        (
            "global key across registries",
            "Global",
            r#"{"gcr.io":{"username":"gcr","password":"p"},"us.gcr.io":{"username":"us","password":"p"}}"#,
            &[("gcr.io/proj/img", "gcr"), ("us.gcr.io/proj/img", "us")],
        ),
        (
            "registry key across repositories",
            "Registry",
            r#"{"gcr.io/a":{"username":"a","password":"p"},"gcr.io/b":{"username":"b","password":"p"}}"#,
            &[("gcr.io/a/img", "a"), ("gcr.io/b/img", "b")],
        ),
        (
            "global key with no entry for the image",
            "Global",
            r#"{"gcr.io":{"username":"gcr","password":"p"}}"#,
            &[("gcr.io/proj/img", "gcr"), ("us.gcr.io/proj/img", "")],
        ),
    ];
    for (name, key_type, auth, refs) in cases {
        let d = tempfile::tempdir().unwrap();
        let requests = fake_plugin(
            d.path(),
            "fake-provider",
            &format!(
                r#"{{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"{key_type}","auth":{auth}}}"#
            ),
        );
        let kc = keychain(d.path(), GCP_LIKE_CONFIG);
        // The first image fills the entry; the second picks its own key.
        for (image, want) in refs {
            let got = resolve(&kc, image)
                .await
                .map(|a| a.username)
                .unwrap_or_default();
            assert_eq!(got, *want, "{name}: {image}");
        }
        assert_eq!(count_requests(&requests), 1, "{name}");
    }
}

#[tokio::test]
async fn keychain_sweeps_expired_entries() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "fake-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Image","cacheDuration":"1m","auth":{"gcr.io":{"username":"u","password":"p"}}}"#,
    );
    let clock = ManualClock::new();
    let kc = Keychain::with_clock(
        &write_config(d.path(), GCP_LIKE_CONFIG),
        d.path(),
        clock.clone(),
    )
    .unwrap();
    for i in 0..5 {
        resolve(&kc, &format!("gcr.io/proj/img{i}")).await;
        clock.advance(Duration::from_secs(3600));
    }
    assert_eq!(kc.plugins()[0].cache_len(), 1);
}

#[tokio::test]
async fn keychain_passes_configured_env() {
    let d = tempfile::tempdir().unwrap();
    // The plugin echoes one configured and one inherited variable.
    link_plugin(d.path(), "fake-provider", "env-provider.sh");
    std::env::set_var("FROM_PROCESS", "inherited");
    let kc = keychain(
        d.path(),
        "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: fake-provider\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n    env:\n    - name: FROM_CONFIG\n      value: configured\n",
    );
    let got = resolve(&kc, "gcr.io/proj/img:latest").await.unwrap();
    assert_eq!(got.username, "configured");
    assert_eq!(got.password, "inherited");
}

#[tokio::test]
async fn keychain_passes_configured_args() {
    let d = tempfile::tempdir().unwrap();
    link_plugin(d.path(), "fake-provider", "args-provider.sh");
    let kc = keychain(
        d.path(),
        "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: fake-provider\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n    args:\n    - get-credentials\n    - --v=3\n",
    );
    let got = resolve(&kc, "gcr.io/proj/img:latest").await.unwrap();
    assert_eq!(
        (got.username.as_str(), got.password.as_str()),
        ("get-credentials", "--v=3")
    );
}

#[tokio::test]
async fn keychain_respects_context_cancellation() {
    let d = tempfile::tempdir().unwrap();
    link_plugin(d.path(), "fake-provider", "sleep-provider.sh");
    let kc = keychain(d.path(), GCP_LIKE_CONFIG);
    let ctx = Ctx::background();
    ctx.cancel();
    let started = std::time::Instant::now();
    assert!(kc.resolve(&ctx, "gcr.io/proj/img:latest").await.is_err());
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[tokio::test]
async fn keychain_skips_unusable_provider_and_keeps_rest() {
    let d = tempfile::tempdir().unwrap();
    fake_plugin(
        d.path(),
        "usable-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"u","password":"p"}}}"#,
    );
    let kc = keychain(
        d.path(),
        "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n  - name: unusable-provider\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n    tokenAttributes:\n      serviceAccountTokenAudience: aud\n      cacheType: Token\n      requireServiceAccount: true\n  - name: usable-provider\n    apiVersion: credentialprovider.kubelet.k8s.io/v1\n    matchImages: [\"gcr.io\"]\n    defaultCacheDuration: 1m\n",
    );
    let got = resolve(&kc, "gcr.io/proj/img:latest").await.unwrap();
    assert_eq!((got.username.as_str(), got.password.as_str()), ("u", "p"));
}

#[tokio::test]
async fn keychain_honors_short_cache_duration() {
    let d = tempfile::tempdir().unwrap();
    let requests = fake_plugin(
        d.path(),
        "fake-provider",
        r#"{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","cacheDuration":"30s","auth":{"gcr.io":{"username":"u","password":"p"}}}"#,
    );
    let clock = ManualClock::new();
    let kc = Keychain::with_clock(
        &write_config(d.path(), GCP_LIKE_CONFIG),
        d.path(),
        clock.clone(),
    )
    .unwrap();
    let image = "gcr.io/proj/img:latest";
    resolve(&kc, image).await;
    // Past the plugin's 30s but inside the minimum: honouring the plugin
    // means running it again here.
    clock.advance(Duration::from_secs(45));
    resolve(&kc, image).await;
    assert_eq!(count_requests(&requests), 2);
}
