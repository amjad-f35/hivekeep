//! Port forwarding to a Service's pods through the Kubernetes API, for
//! tools and tests that reach in-cluster components from outside.
//!
//! A local listener on `127.0.0.1` accepts connections and carries each over
//! its own API-server port-forward stream to the chosen pod.

use std::io;

use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ListParams};
use kube::Client;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Why a forward could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum PortForwardError {
    /// A Kubernetes API call failed.
    #[error("{context}: {source}")]
    Api {
        /// What was being done.
        context: String,
        /// The API error.
        source: Box<kube::Error>,
    },
    /// The Service or its pods do not allow the forward.
    #[error("{0}")]
    Unusable(String),
    /// The local listener failed.
    #[error("port forwarding: {0}")]
    Io(#[from] io::Error),
}

/// A running forward; dropping it (or calling [`Forward::stop`]) stops
/// accepting new connections.
#[derive(Debug)]
pub struct Forward {
    /// The local port on `127.0.0.1`.
    pub local_port: u16,
    stop: CancellationToken,
}

impl Forward {
    /// Stops the forward.
    pub fn stop(&self) {
        self.stop.cancel();
    }
}

impl Drop for Forward {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Whether a pod is running, not terminating, and Ready.
pub fn is_pod_ready(pod: &Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return false;
    }
    let Some(status) = &pod.status else {
        return false;
    };
    if status.phase.as_deref() != Some("Running") {
        return false;
    }
    status
        .conditions
        .iter()
        .flatten()
        .find(|c| c.type_ == "Ready")
        .is_some_and(|c| c.status == "True")
}

/// The container port that Service port `service_port` targets on `pod`.
pub fn resolve_target_port(
    svc: &Service,
    pod: &Pod,
    service_port: i32,
) -> Result<i32, PortForwardError> {
    let name = svc.metadata.name.clone().unwrap_or_default();
    let ports = svc
        .spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .cloned()
        .unwrap_or_default();
    let Some(sp) = ports.into_iter().find(|p| p.port == service_port) else {
        return Err(PortForwardError::Unusable(format!(
            "service {name} has no port {service_port}"
        )));
    };
    match sp.target_port {
        None => Ok(sp.port),
        Some(IntOrString::Int(0)) => Ok(sp.port),
        Some(IntOrString::Int(p)) => Ok(p),
        Some(IntOrString::String(named)) => {
            let found = pod
                .spec
                .iter()
                .flat_map(|s| s.containers.iter())
                .flat_map(|c| c.ports.iter().flatten())
                .filter(|cp| cp.name.as_deref() == Some(named.as_str()))
                .map(|cp| cp.container_port)
                .next_back()
                .unwrap_or(0);
            if found == 0 {
                let pod_name = pod.metadata.name.clone().unwrap_or_default();
                return Err(PortForwardError::Unusable(format!(
                    "named targetPort {named:?} not found on pod {pod_name}"
                )));
            }
            Ok(found)
        }
    }
}

async fn first_ready_pod(
    client: &Client,
    namespace: &str,
    service: &str,
) -> Result<(Pod, Service), PortForwardError> {
    let svc = Api::<Service>::namespaced(client.clone(), namespace)
        .get(service)
        .await
        .map_err(|source| PortForwardError::Api {
            context: format!("getting {service} service"),
            source: Box::new(source),
        })?;
    let selector = svc
        .spec
        .as_ref()
        .and_then(|s| s.selector.clone())
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(PortForwardError::Unusable(format!(
            "service {service} has no selector"
        )));
    }
    let label = selector
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&label))
        .await
        .map_err(|source| PortForwardError::Api {
            context: format!("listing {service} pods"),
            source: Box::new(source),
        })?;
    pods.items
        .into_iter()
        .find(is_pod_ready)
        .map(|p| (p, svc))
        .ok_or_else(|| {
            PortForwardError::Unusable(format!("no ready {service} pods in {namespace}"))
        })
}

/// Forwards to the target port of Service port `service_port` on a ready
/// pod of `service`.
pub async fn service_port_forward(
    client: &Client,
    namespace: &str,
    service: &str,
    service_port: i32,
) -> Result<Forward, PortForwardError> {
    let (pod, svc) = first_ready_pod(client, namespace, service).await?;
    let port = resolve_target_port(&svc, &pod, service_port)?;
    pod_port_forward(
        client,
        namespace,
        pod.metadata.name.as_deref().unwrap_or_default(),
        port,
    )
    .await
}

/// Forwards to `container_port` on a ready pod of `service`.
pub async fn service_pod_port_forward(
    client: &Client,
    namespace: &str,
    service: &str,
    container_port: i32,
) -> Result<Forward, PortForwardError> {
    let (pod, _) = first_ready_pod(client, namespace, service).await?;
    pod_port_forward(
        client,
        namespace,
        pod.metadata.name.as_deref().unwrap_or_default(),
        container_port,
    )
    .await
}

/// Forwards a local port to `target_port` of pod `pod`.
pub async fn pod_port_forward(
    client: &Client,
    namespace: &str,
    pod: &str,
    target_port: i32,
) -> Result<Forward, PortForwardError> {
    let port = u16::try_from(target_port)
        .map_err(|_| PortForwardError::Unusable(format!("invalid target port {target_port}")))?;
    let lis = TcpListener::bind("127.0.0.1:0").await?;
    let local_port = lis.local_addr()?.port();
    let stop = CancellationToken::new();
    let api = Api::<Pod>::namespaced(client.clone(), namespace);
    let (pod, token) = (pod.to_string(), stop.clone());
    tokio::spawn(async move {
        loop {
            let mut conn = tokio::select! {
                r = lis.accept() => match r {
                    Ok((c, _)) => c,
                    Err(err) => {
                        tracing::warn!(%err, "port forward: accept failed");
                        return;
                    }
                },
                _ = token.cancelled() => return,
            };
            let (api, pod, token) = (api.clone(), pod.clone(), token.clone());
            tokio::spawn(async move {
                let mut pf = match api.portforward(&pod, &[port]).await {
                    Ok(pf) => pf,
                    Err(err) => {
                        tracing::warn!(%err, pod, "port forward: opening stream failed");
                        return;
                    }
                };
                let Some(mut upstream) = pf.take_stream(port) else {
                    return;
                };
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut conn, &mut upstream) => {}
                    _ = token.cancelled() => {}
                }
            });
        }
    });
    Ok(Forward { local_port, stop })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        Container, ContainerPort, PodCondition, PodSpec, PodStatus, ServicePort, ServiceSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};

    fn pod_with_port(port: i32) -> Pod {
        Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "envoy".into(),
                    ports: Some(vec![ContainerPort {
                        name: Some("http".into()),
                        container_port: port,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn svc(port: i32, target: Option<IntOrString>) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some("hivekeep-gate-router".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port,
                    target_port: target,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn resolve_target_port_cases() {
        let pod = pod_with_port(8080);
        let zero = pod_with_port(0);
        let s = |t: &str| Some(IntOrString::String(t.into()));
        let cases: Vec<(&str, Service, &Pod, Option<i32>)> = vec![
            (
                "int target port",
                svc(80, Some(IntOrString::Int(8080))),
                &pod,
                Some(8080),
            ),
            ("named target port", svc(80, s("http")), &pod, Some(8080)),
            ("named target not found", svc(80, s("nope")), &pod, None),
            (
                "named target resolves to zero",
                svc(80, s("http")),
                &zero,
                None,
            ),
            (
                "unset target port defaults to port",
                svc(80, None),
                &pod,
                Some(80),
            ),
            (
                "no matching service port",
                svc(443, Some(IntOrString::Int(8443))),
                &pod,
                None,
            ),
        ];
        for (name, sv, p, want) in cases {
            assert_eq!(resolve_target_port(&sv, p, 80).ok(), want, "{name}");
        }
    }

    #[test]
    fn is_pod_ready_cases() {
        let pod = |phase: &str, ready: &str, deleting: bool| Pod {
            metadata: ObjectMeta {
                deletion_timestamp: deleting.then(|| Time(k8s_openapi::jiff::Timestamp::now())),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some(phase.into()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".into(),
                    status: ready.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let no_cond = Pod {
            status: Some(PodStatus {
                phase: Some("Running".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        for (name, p, want) in [
            ("running and ready", pod("Running", "True", false), true),
            ("running not ready", pod("Running", "False", false), false),
            ("pending", pod("Pending", "True", false), false),
            ("terminating", pod("Running", "True", true), false),
            ("no ready condition", no_cond, false),
        ] {
            assert_eq!(is_pod_ready(&p), want, "{name}");
        }
    }

    #[tokio::test]
    #[ignore = "needs a live cluster (KUBECONFIG) with a ready Service named by HK_TEST_SERVICE"]
    async fn service_port_forward_live() {
        let client = Client::try_default().await.unwrap();
        let svc = std::env::var("HK_TEST_SERVICE").unwrap_or_else(|_| "hivekeep-api".into());
        let fwd = service_port_forward(&client, "hivekeep-system", &svc, 443)
            .await
            .unwrap();
        tokio::net::TcpStream::connect(("127.0.0.1", fwd.local_port))
            .await
            .unwrap();
    }
}
