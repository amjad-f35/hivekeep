//! One served PodCertificateRequest version, used for both the watch cache
//! and status writes.

use std::sync::Arc;
use std::time::Duration;

use hivekeep_k8s::cert_api::{self, CertVersion, DiscoverError, VersionedApi};
use hivekeep_k8s::informer::Informer;
use kube::api::{DynamicObject, PostParams};
use kube::runtime::watcher;
use kube::Client;

use super::PodCertificateRequest;

/// How often the cache re-delivers every request to its handlers.
pub const RESYNC_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);

/// Cached and live access to PodCertificateRequests through the version
/// the cluster serves (v1 preferred). Requests are handed out in the beta
/// representation either way.
pub struct PcrClient {
    client: Client,
    version: CertVersion,
    informer: Arc<Informer<DynamicObject>>,
}

impl PcrClient {
    /// Discovers the served version. Falls back to v1beta1 only when v1 is
    /// absent; any other discovery error is returned.
    #[allow(clippy::result_large_err)] // the error type belongs to hivekeep-k8s
    pub async fn new(client: Client) -> Result<Self, DiscoverError> {
        let version =
            cert_api::discover(&client, "podcertificaterequests", "PodCertificateRequest").await?;
        Ok(Self::with_version(client, version))
    }

    /// A client for a known version.
    pub fn with_version(client: Client, version: CertVersion) -> Self {
        let api = VersionedApi::<PodCertificateRequest>::all(client.clone(), version);
        let informer = Informer::with_resync(
            api.dynamic().clone(),
            watcher::Config::default(),
            Some(RESYNC_PERIOD),
        );
        PcrClient {
            client,
            version,
            informer,
        }
    }

    /// The version used on the wire.
    pub fn version(&self) -> CertVersion {
        self.version
    }

    /// The watch cache, shared by every controller using this client. The
    /// caller runs it once (`informer().spawn()`).
    pub fn informer(&self) -> &Arc<Informer<DynamicObject>> {
        &self.informer
    }

    /// A request from the cache, or `None` if it is not cached.
    pub fn get_cached(&self, namespace: &str, name: &str) -> Option<PodCertificateRequest> {
        let obj = self.informer.get(namespace, name)?;
        match VersionedApi::<PodCertificateRequest>::from_dynamic((*obj).clone()) {
            Ok(pcr) => Some(pcr),
            Err(err) => {
                tracing::warn!(%err, namespace, name, "Undecodable PodCertificateRequest in cache");
                None
            }
        }
    }

    /// Writes the request's status through the cached version.
    pub async fn update_status(
        &self,
        pcr: &PodCertificateRequest,
    ) -> Result<PodCertificateRequest, kube::Error> {
        let namespace = pcr.metadata.namespace.as_deref().unwrap_or_default();
        let name = pcr.metadata.name.as_deref().unwrap_or_default();
        VersionedApi::<PodCertificateRequest>::namespaced(
            self.client.clone(),
            namespace,
            self.version,
        )
        .replace_status(name, &PostParams::default(), pcr)
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use hivekeep_k8s::api_errors::is_conflict;
    use hivekeep_k8s::fake::{FakeApiServer, Reaction};
    use hivekeep_k8s::informer::InformerEvent;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
    use k8s_openapi::ByteString;

    use super::*;
    use crate::test_support::{next_event, PCR_RESOURCE};

    #[tokio::test]
    async fn discovery() {
        struct Case {
            name: &'static str,
            stable: bool,
            beta: bool,
            error: Option<(u16, &'static str)>,
            want: Option<CertVersion>,
        }
        let case = |name, stable, beta, error, want| Case {
            name,
            stable,
            beta,
            error,
            want,
        };
        for tc in [
            case("prefer stable", true, true, None, Some(CertVersion::V1)),
            case("stable only", true, false, None, Some(CertVersion::V1)),
            case(
                "beta fallback",
                false,
                true,
                None,
                Some(CertVersion::V1beta1),
            ),
            case("neither served", false, false, None, None),
            case(
                "missing group version",
                false,
                true,
                Some((404, "NotFound")),
                Some(CertVersion::V1beta1),
            ),
            case("forbidden", false, true, Some((403, "Forbidden")), None),
            case(
                "server error",
                false,
                true,
                Some((500, "InternalError")),
                None,
            ),
        ] {
            let fake = FakeApiServer::new();
            for version in [CertVersion::V1, CertVersion::V1beta1] {
                // Stable discovery exists on older clusters for CSRs alone.
                let mut resources = vec![(
                    "certificatesigningrequests",
                    "CertificateSigningRequest",
                    false,
                )];
                if (version == CertVersion::V1 && tc.stable)
                    || (version == CertVersion::V1beta1 && tc.beta)
                {
                    resources.push((PCR_RESOURCE, "PodCertificateRequest", true));
                }
                fake.serve(&version.api_version(), &resources);
            }
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = calls.clone();
            let error = tc.error;
            fake.prepend_reactor("discover", "*", move |_| {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                match error {
                    Some((code, reason)) if n == 1 => {
                        Reaction::fail(code, reason, "discovery failed")
                    }
                    _ => Reaction::Pass,
                }
            });
            let got = PcrClient::new(fake.client()).await;
            let calls = calls.load(Ordering::SeqCst);
            match tc.want {
                None => {
                    assert!(got.is_err(), "{}: expected discovery failure", tc.name);
                    if tc.error.is_some() {
                        assert_eq!(calls, 1, "{}: fell back after discovery failure", tc.name);
                    }
                }
                Some(want) => {
                    let client = got.unwrap_or_else(|e| panic!("{}: {e}", tc.name));
                    assert_eq!(client.version(), want, "{}", tc.name);
                    if want == CertVersion::V1 {
                        assert_eq!(calls, 1, "{}: unnecessary beta discovery", tc.name);
                    }
                }
            }
        }
    }

    fn request(version: CertVersion) -> PodCertificateRequest {
        let mut pcr = PodCertificateRequest::default();
        pcr.metadata.name = Some("request".into());
        pcr.metadata.namespace = Some("test".into());
        let spec = &mut pcr.spec;
        spec.signer_name = "example.com/signer".into();
        spec.pod_name = "pod".into();
        spec.pod_uid = "pod-uid".into();
        spec.service_account_name = "sa".into();
        spec.service_account_uid = "sa-uid".into();
        spec.node_name = "node".into();
        spec.node_uid = "node-uid".into();
        spec.max_expiration_seconds = Some(3600);
        spec.unverified_user_annotations =
            Some([("example.com/key".to_string(), "value".to_string())].into());
        if version == CertVersion::V1beta1 {
            // Legacy requests carry only the PKIX key.
            spec.pkix_public_key = Some(ByteString(b"legacy-key".to_vec()));
        } else {
            spec.stub_pkc_s10_request = ByteString(b"csr".to_vec());
        }
        pcr
    }

    #[tokio::test]
    async fn informer_and_status() {
        for version in [CertVersion::V1, CertVersion::V1beta1] {
            let fake = FakeApiServer::new();
            fake.serve(
                &version.api_version(),
                &[(PCR_RESOURCE, "PodCertificateRequest", true)],
            );
            let client = PcrClient::new(fake.client()).await.unwrap();
            assert_eq!(client.version(), version);
            let pcr = fake.insert(&request(version));

            let mut events = client.informer().events();
            let task = client.informer().spawn();
            assert!(client.get_cached("test", "missing").is_none());

            assert!(matches!(
                next_event(&mut events).await,
                InformerEvent::Added(_)
            ));
            let got = client
                .get_cached("test", "request")
                .expect("cached after add");
            assert_eq!(
                got.spec, pcr.spec,
                "{version:?}: list conversion lost fields"
            );
            assert_eq!(got.metadata.uid, pcr.metadata.uid);
            assert_eq!(got.metadata.resource_version, pcr.metadata.resource_version);

            // A change through the wire version reaches the cache.
            let api =
                VersionedApi::<PodCertificateRequest>::namespaced(fake.client(), "test", version);
            let mut changed = got.clone();
            changed
                .metadata
                .labels
                .get_or_insert_default()
                .insert("changed".into(), "yes".into());
            let changed = api
                .replace("request", &PostParams::default(), &changed)
                .await
                .unwrap();
            assert!(matches!(
                next_event(&mut events).await,
                InformerEvent::Updated { .. }
            ));
            let mut got = client.get_cached("test", "request").unwrap();
            assert_eq!(
                got.metadata.resource_version, changed.metadata.resource_version,
                "watch update lost resource version"
            );

            let now = Time(k8s_openapi::jiff::Timestamp::from_second(1_800_000_000).unwrap());
            let status = &mut got.status.get_or_insert_default();
            status.conditions = Some(vec![Condition {
                type_: "Issued".into(),
                status: "True".into(),
                reason: "Issued".into(),
                message: String::new(),
                last_transition_time: now.clone(),
                observed_generation: None,
            }]);
            status.certificate_chain = Some("certificate".into());
            status.not_before = Some(now.clone());
            status.begin_refresh_at = Some(now.clone());
            status.not_after = Some(now);
            fake.clear_actions();
            client.update_status(&got).await.unwrap();
            let writes: Vec<_> = fake
                .actions()
                .into_iter()
                .filter(|a| a.resource == PCR_RESOURCE && a.verb == "update")
                .collect();
            assert_eq!(writes.len(), 1);
            assert_eq!(writes[0].version, version.as_str(), "wrong status endpoint");
            assert_eq!(
                writes[0].subresource.as_deref(),
                Some("status"),
                "wrong status endpoint"
            );
            let stored: PodCertificateRequest = fake.get("test", "request").unwrap();
            assert_eq!(stored.status, got.status, "status conversion lost fields");
            assert_eq!(stored.spec, got.spec);

            fake.prepend_reactor("update", PCR_RESOURCE, |_| {
                Reaction::fail(409, "Conflict", "changed")
            });
            let err = client.update_status(&got).await.unwrap_err();
            assert!(is_conflict(&err), "lost conflict error: {err}");
            task.abort();
        }
    }
}
