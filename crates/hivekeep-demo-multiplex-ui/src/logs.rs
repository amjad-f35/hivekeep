//! Tailing worker pod logs through the Kubernetes API.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};

/// Lines of log returned per pod.
pub const TAIL_LINES: i64 = 25;

/// Where pod logs come from.
#[async_trait]
pub trait LogSource: Send + Sync {
    /// The last [`TAIL_LINES`] lines of `pod`'s default container.
    async fn tail(&self, pod: &str) -> Result<String, String>;
}

/// [`LogSource`] over a kube client scoped to one namespace.
pub struct KubeLogs {
    pods: Api<Pod>,
}

impl KubeLogs {
    /// Connects using in-cluster credentials or the local kubeconfig.
    /// Returns `None` (logs disabled) when neither is usable, so the
    /// dashboard still starts without a cluster context.
    pub async fn connect(namespace: &str) -> Option<Self> {
        match kube::Client::try_default().await {
            Ok(client) => Some(Self {
                pods: Api::namespaced(client, namespace),
            }),
            Err(e) => {
                tracing::warn!(error = %e, "no Kubernetes context available; pod logs disabled");
                None
            }
        }
    }
}

#[async_trait]
impl LogSource for KubeLogs {
    async fn tail(&self, pod: &str) -> Result<String, String> {
        let params = LogParams {
            tail_lines: Some(TAIL_LINES),
            ..Default::default()
        };
        self.pods
            .logs(pod, &params)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Validates the pod segment of `/api/logs/<pod>`: surrounding slashes are
/// ignored, and an empty name or one with an inner slash is refused.
pub fn parse_pod_ref(raw: &str) -> Option<&str> {
    let pod = raw.trim_matches('/');
    if pod.is_empty() || pod.contains('/') {
        None
    } else {
        Some(pod)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_refs() {
        for (raw, want) in [
            ("w-1", Some("w-1")),
            ("/w-1/", Some("w-1")),
            ("", None),
            ("/", None),
            ("a/b", None),
        ] {
            assert_eq!(parse_pod_ref(raw), want, "{raw:?}");
        }
    }
}
