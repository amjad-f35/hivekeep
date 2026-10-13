//! Access to `certificates.k8s.io` resources served at v1 or v1beta1.
//!
//! ClusterTrustBundle and PodCertificateRequest are served at v1 on newer
//! clusters and v1beta1 on older ones, with identical schemas. Discovery
//! picks one version per cluster (preferring v1); objects are always handed
//! out in the typed v1beta1 representation, whatever version is used on the
//! wire.

use std::marker::PhantomData;

use kube::api::{Api, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams};
use kube::discovery::ApiResource;
use kube::Client;
use serde::{de::DeserializeOwned, Serialize};

/// The certificates API group.
pub const GROUP: &str = "certificates.k8s.io";

/// A served version of the certificates group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertVersion {
    /// `certificates.k8s.io/v1`.
    V1,
    /// `certificates.k8s.io/v1beta1`.
    V1beta1,
}

impl CertVersion {
    /// The version string.
    pub fn as_str(self) -> &'static str {
        match self {
            CertVersion::V1 => "v1",
            CertVersion::V1beta1 => "v1beta1",
        }
    }

    /// `group/version`.
    pub fn api_version(self) -> String {
        format!("{GROUP}/{}", self.as_str())
    }
}

/// Discovery failures.
#[derive(Debug, thiserror::Error)]
pub enum DiscoverError {
    /// Neither version serves the resource.
    #[error("neither v1 nor v1beta1 {0} is served")]
    NotServed(&'static str),
    /// Discovery itself failed (anything but a 404).
    #[error("discover {kind} {version}: {source}")]
    Discovery {
        /// The resource kind.
        kind: &'static str,
        /// The version being discovered.
        version: &'static str,
        /// The API error.
        #[source]
        source: Box<kube::Error>,
    },
}

/// Finds the version serving `plural`, preferring v1. Only a 404 for a
/// group version falls through to the next one; any other discovery error
/// is returned at once.
pub async fn discover(
    client: &Client,
    plural: &str,
    kind: &'static str,
) -> Result<CertVersion, DiscoverError> {
    for v in [CertVersion::V1, CertVersion::V1beta1] {
        match client.list_api_group_resources(&v.api_version()).await {
            Ok(list) => {
                if list.resources.iter().any(|r| r.name == plural) {
                    return Ok(v);
                }
            }
            Err(e) if crate::api_errors::is_not_found(&e) => continue,
            Err(source) => {
                return Err(DiscoverError::Discovery {
                    kind,
                    version: v.as_str(),
                    source: Box::new(source),
                })
            }
        }
    }
    Err(DiscoverError::NotServed(kind))
}

/// A typed handle on a certificates resource at a discovered version. `K`
/// is the k8s-openapi v1beta1 type, used as the in-memory representation.
pub struct VersionedApi<K> {
    api: Api<DynamicObject>,
    version: CertVersion,
    resource: ApiResource,
    _k: PhantomData<fn() -> K>,
}

impl<K> Clone for VersionedApi<K> {
    fn clone(&self) -> Self {
        VersionedApi {
            api: self.api.clone(),
            version: self.version,
            resource: self.resource.clone(),
            _k: PhantomData,
        }
    }
}

impl<K> VersionedApi<K>
where
    K: kube::Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone,
{
    fn resource(version: CertVersion) -> ApiResource {
        ApiResource {
            group: GROUP.into(),
            version: version.as_str().into(),
            api_version: version.api_version(),
            kind: K::kind(&()).into_owned(),
            plural: K::plural(&()).into_owned(),
        }
    }

    /// Cluster-scoped access.
    pub fn all(client: Client, version: CertVersion) -> Self {
        let resource = Self::resource(version);
        VersionedApi {
            api: Api::all_with(client, &resource),
            version,
            resource,
            _k: PhantomData,
        }
    }

    /// Namespaced access.
    pub fn namespaced(client: Client, namespace: &str, version: CertVersion) -> Self {
        let resource = Self::resource(version);
        VersionedApi {
            api: Api::namespaced_with(client, namespace, &resource),
            version,
            resource,
            _k: PhantomData,
        }
    }

    /// The version used on the wire.
    pub fn version(&self) -> CertVersion {
        self.version
    }

    /// The dynamic API underneath, for watches.
    pub fn dynamic(&self) -> &Api<DynamicObject> {
        &self.api
    }

    /// The wire resource description.
    pub fn api_resource(&self) -> &ApiResource {
        &self.resource
    }

    /// Converts a wire object to the typed representation.
    pub fn from_dynamic(obj: DynamicObject) -> Result<K, kube::Error> {
        let mut v = serde_json::to_value(obj).map_err(kube::Error::SerdeError)?;
        v["apiVersion"] = K::api_version(&()).as_ref().into();
        v["kind"] = K::kind(&()).as_ref().into();
        serde_json::from_value(v).map_err(kube::Error::SerdeError)
    }

    fn to_dynamic(&self, obj: &K) -> Result<DynamicObject, kube::Error> {
        let mut v = serde_json::to_value(obj).map_err(kube::Error::SerdeError)?;
        v["apiVersion"] = self.version.api_version().into();
        v["kind"] = K::kind(&()).as_ref().into();
        serde_json::from_value(v).map_err(kube::Error::SerdeError)
    }

    /// Reads an object.
    pub async fn get(&self, name: &str) -> Result<K, kube::Error> {
        Self::from_dynamic(self.api.get(name).await?)
    }

    /// Lists objects.
    pub async fn list(&self, lp: &ListParams) -> Result<Vec<K>, kube::Error> {
        self.api
            .list(lp)
            .await?
            .items
            .into_iter()
            .map(Self::from_dynamic)
            .collect()
    }

    /// Creates an object.
    pub async fn create(&self, pp: &PostParams, obj: &K) -> Result<K, kube::Error> {
        Self::from_dynamic(self.api.create(pp, &self.to_dynamic(obj)?).await?)
    }

    /// Replaces an object (resourceVersion-checked when set).
    pub async fn replace(&self, name: &str, pp: &PostParams, obj: &K) -> Result<K, kube::Error> {
        Self::from_dynamic(self.api.replace(name, pp, &self.to_dynamic(obj)?).await?)
    }

    /// Replaces an object's status.
    pub async fn replace_status(
        &self,
        name: &str,
        pp: &PostParams,
        obj: &K,
    ) -> Result<K, kube::Error> {
        Self::from_dynamic(
            self.api
                .replace_status(name, pp, &self.to_dynamic(obj)?)
                .await?,
        )
    }

    /// Patches an object.
    pub async fn patch<P: Serialize + std::fmt::Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K, kube::Error> {
        Self::from_dynamic(self.api.patch(name, pp, patch).await?)
    }

    /// Patches an object's status.
    pub async fn patch_status<P: Serialize + std::fmt::Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K, kube::Error> {
        Self::from_dynamic(self.api.patch_status(name, pp, patch).await?)
    }

    /// Deletes an object.
    pub async fn delete(&self, name: &str, dp: &DeleteParams) -> Result<(), kube::Error> {
        self.api.delete(name, dp).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::{FakeApiServer, Reaction};

    #[tokio::test]
    async fn discovery() {
        struct Case {
            name: &'static str,
            stable: bool,
            beta: bool,
            missing_stable: bool,
            err: Option<(u16, &'static str)>,
            want: Option<CertVersion>,
        }
        let c = |name, stable, beta, missing_stable, err, want| Case {
            name,
            stable,
            beta,
            missing_stable,
            err,
            want,
        };
        for tc in [
            c("both", true, true, false, None, Some(CertVersion::V1)),
            c(
                "stable only",
                true,
                false,
                false,
                None,
                Some(CertVersion::V1),
            ),
            c(
                "beta only",
                false,
                true,
                false,
                None,
                Some(CertVersion::V1beta1),
            ),
            c(
                "missing stable group",
                false,
                true,
                true,
                None,
                Some(CertVersion::V1beta1),
            ),
            c("neither", false, false, false, None, None),
            c(
                "forbidden",
                false,
                true,
                false,
                Some((403, "Forbidden")),
                None,
            ),
            c(
                "unavailable",
                false,
                true,
                false,
                Some((503, "ServiceUnavailable")),
                None,
            ),
        ] {
            let fake = FakeApiServer::new();
            for v in [CertVersion::V1, CertVersion::V1beta1] {
                if v == CertVersion::V1 && tc.missing_stable {
                    continue;
                }
                let mut res = vec![(
                    "certificatesigningrequests",
                    "CertificateSigningRequest",
                    false,
                )];
                if (v == CertVersion::V1 && tc.stable) || (v == CertVersion::V1beta1 && tc.beta) {
                    res.push(("clustertrustbundles", "ClusterTrustBundle", false));
                }
                fake.serve(&v.api_version(), &res);
            }
            if let Some((code, reason)) = tc.err {
                fake.prepend_reactor("discover", "*", move |_| {
                    Reaction::fail(code, reason, "denied")
                });
            }
            let got = discover(&fake.client(), "clustertrustbundles", "ClusterTrustBundle").await;
            let calls = fake
                .actions()
                .iter()
                .filter(|a| a.verb == "discover")
                .count();
            match tc.want {
                Some(want) => assert_eq!(got.unwrap(), want, "{}", tc.name),
                None => {
                    let err = got.expect_err(tc.name);
                    if tc.err.is_some() {
                        assert_eq!(calls, 1, "{}: fell back after discovery error", tc.name);
                    }
                    assert_eq!(
                        matches!(err, DiscoverError::NotServed(_)),
                        tc.err.is_none(),
                        "{}: {err}",
                        tc.name
                    );
                }
            }
        }
    }
}
