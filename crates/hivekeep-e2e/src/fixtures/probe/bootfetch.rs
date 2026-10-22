//! The boot fetch and the wakeup probe it gates.
//!
//! When [`BOOT_FETCH_URL_ENV`] names a URL, the probe fetches it through the
//! egress gateway, trusting only the projected bundle, before it listens. A
//! probe that cannot exits and never answers its wakeup probe, so a golden
//! snapshot of it proves the Actor had egress while it booted. A restore
//! from that golden resumes past the boot fetch with the restored Actor's
//! identity, so [`RestoreFetch`] keeps `/healthz` at 503 until that identity
//! has fetched too.

use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use parking_lot::Mutex;

use super::fetch::{get, MAX_FETCH_BODY};
use super::roots;

/// Names a URL the probe must fetch before it listens.
pub const BOOT_FETCH_URL_ENV: &str = "PROBE_BOOT_FETCH_URL";

/// Bounds the boot fetch, inside the template's wakeup-probe timeout.
pub const BOOT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// GETs `url` once, trusting only the bundle at `trust_bundle`, and fails
/// unless it answers 200. It does not retry: the Actor's EgressPolicy exists
/// before the Actor resumes, so a failure is the finding.
pub async fn boot_fetch(url: &str, trust_bundle: &str) -> Result<(), String> {
    let bundle = std::fs::read(trust_bundle).map_err(|e| format!("reading trust bundle: {e}"))?;
    let store = roots::roots_from_pem(&bundle)
        .ok_or_else(|| format!("no certificates parsed from {trust_bundle}"))?;
    let tls = roots::client_config(store)?;
    let mut handshake = None;
    let result = tokio::time::timeout(
        BOOT_FETCH_TIMEOUT,
        get(
            url,
            None,
            tls,
            &HeaderMap::new(),
            MAX_FETCH_BODY,
            &mut handshake,
        ),
    )
    .await;
    let res = match result {
        Err(_) => Err(format!("timed out after {BOOT_FETCH_TIMEOUT:?}")),
        Ok(r) => r,
    }
    .map_err(|e| format!("boot fetch of {url}: {e}"))?;
    if res.status != 200 {
        return Err(format!("boot fetch of {url}: status {}", res.status));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct State {
    /// The identity that has fetched.
    fetched: String,
    /// The identity a background fetch is running for, if any.
    fetching: String,
}

/// Answers the wakeup probe once the Actor named in the identity file has
/// fetched the URL. The prober times out each request quickly, so the fetch
/// runs in the background while `/healthz` answers 503.
#[derive(Debug)]
pub struct RestoreFetch {
    url: String,
    identity_file: String,
    trust_bundle: String,
    state: Mutex<State>,
}

impl RestoreFetch {
    /// A gate for `url` whose identity `fetched` has already fetched (the
    /// Actor that booted).
    pub fn new(
        url: impl Into<String>,
        identity_file: impl Into<String>,
        trust_bundle: impl Into<String>,
        fetched: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(RestoreFetch {
            url: url.into(),
            identity_file: identity_file.into(),
            trust_bundle: trust_bundle.into(),
            state: Mutex::new(State {
                fetched: fetched.into(),
                fetching: String::new(),
            }),
        })
    }

    /// The `/healthz` answer: 200 once the current identity has fetched,
    /// otherwise 503, starting a fetch for it if none is running.
    pub fn healthz(self: &Arc<Self>) -> (u16, String) {
        let id = match std::fs::read(&self.identity_file) {
            Ok(b) => String::from_utf8_lossy(&b).into_owned(),
            Err(e) => return (503, e.to_string()),
        };
        let mut state = self.state.lock();
        if id == state.fetched {
            return (200, String::new());
        }
        if state.fetching != id {
            state.fetching = id.clone();
            let this = Arc::clone(self);
            let fetch_id = id.clone();
            tokio::spawn(async move { this.fetch(&fetch_id).await });
        }
        (503, format!("fetching {} as {id}", self.url))
    }

    /// Fetches as `id`, recording success.
    pub async fn fetch(&self, id: &str) {
        let result = boot_fetch(&self.url, &self.trust_bundle).await;
        let mut state = self.state.lock();
        if state.fetching == id {
            state.fetching.clear();
        }
        match result {
            Err(e) => eprintln!("probe: restore fetch as {id}: {e}"),
            Ok(()) => {
                eprintln!("probe: restore fetch of {} as {id} succeeded", self.url);
                state.fetched = id.to_string();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
    use std::time::Instant;

    use super::super::test_support::{self, Origin};
    use super::*;

    // The EgressPolicy exists before the Actor resumes, so one request and
    // its failure is the whole boot fetch.
    #[tokio::test]
    async fn boot_fetch_cases() {
        for (name, status, want_err) in [
            ("origin answers 200", 200u16, ""),
            ("origin denies", 403, "status 403"),
        ] {
            let code = Arc::new(AtomicU16::new(status));
            let c = code.clone();
            let origin =
                Origin::https(move |_| test_support::text(c.load(Ordering::SeqCst), String::new()))
                    .await;
            let dir = tempfile::tempdir().unwrap();
            let bundle = origin.write_trust_bundle(dir.path());
            let err = boot_fetch(&origin.url(), &bundle).await;
            match (want_err, err) {
                ("", Err(e)) => panic!("{name}: boot_fetch() = {e}, want Ok"),
                ("", Ok(())) => {}
                (want, Ok(())) => panic!("{name}: boot_fetch() = Ok, want {want:?}"),
                (want, Err(e)) => assert!(e.contains(want), "{name}: {e}"),
            }
            assert_eq!(origin.hits(), 1, "{name}: origin requests");
        }
    }

    fn write_identity(path: &std::path::Path, id: &str) {
        std::fs::write(path, id).unwrap();
    }

    // A restored Actor resumes past the boot fetch with its own identity, so
    // its wakeup probe fails until that identity has fetched.
    #[tokio::test]
    async fn restore_fetch_gates_healthz_on_identity() {
        let origin = Origin::https(|_| test_support::text(200, String::new())).await;
        let dir = tempfile::tempdir().unwrap();
        let identity = dir.path().join("actor-id");
        let f = RestoreFetch::new(
            origin.url(),
            identity.to_str().unwrap(),
            origin.write_trust_bundle(dir.path()),
            "golden",
        );

        write_identity(&identity, "golden");
        assert_eq!(f.healthz().0, 200, "healthz as the Actor that booted");
        write_identity(&identity, "restored");
        assert_eq!(
            f.healthz().0,
            503,
            "healthz as a restored Actor before it fetched"
        );
        f.fetch("restored").await;
        assert_eq!(
            f.healthz().0,
            200,
            "healthz as a restored Actor after it fetched"
        );
    }

    // A failed restore fetch leaves healthz at 503, and the next wakeup probe
    // starts another fetch.
    #[tokio::test]
    async fn restore_fetch_retries_on_next_probe() {
        let deny = Arc::new(AtomicBool::new(true));
        let d = deny.clone();
        let origin = Origin::https(move |_| {
            test_support::text(
                if d.load(Ordering::SeqCst) { 403 } else { 200 },
                String::new(),
            )
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let identity = dir.path().join("actor-id");
        write_identity(&identity, "restored");
        let f = RestoreFetch::new(
            origin.url(),
            identity.to_str().unwrap(),
            origin.write_trust_bundle(dir.path()),
            "golden",
        );

        f.fetch("restored").await;
        assert_eq!(f.healthz().0, 503, "healthz after a failed fetch");
        deny.store(false, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(10);
        while f.healthz().0 != 200 {
            assert!(
                Instant::now() < deadline,
                "healthz did not answer 200 after the origin allowed the fetch"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
