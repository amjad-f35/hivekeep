//! Waiting for pods.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;

use crate::harness::T;
use crate::kube_util::{describe_pod_state, is_pod_ready};

/// Blocks until the pod passes its readiness probe; fails `t` with the
/// pod's last observed state if it does not within `timeout`. Reporting the
/// container's own waiting or terminated reason is the point of the poll:
/// it is the difference between "ImagePullBackOff" and an unexplained
/// refused connection later on.
pub async fn wait_for_pod_ready(t: &T, namespace: &str, name: &str, timeout: Duration) {
    let pods: Api<Pod> = Api::namespaced(crate::suite::clients().kube(), namespace);
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    while Instant::now() < deadline {
        match pods.get(name).await {
            Err(e) => last = e.to_string(),
            Ok(pod) if is_pod_ready(&pod) => {
                t.log(format!("pod {namespace}/{name} is ready"));
                return;
            }
            Ok(pod) => last = describe_pod_state(&pod),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    t.fatal(format!(
        "timed out after {timeout:?} waiting for pod {namespace}/{name} to become ready: {last}"
    ))
}
