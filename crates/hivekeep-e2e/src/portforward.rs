//! Port forwarding to in-cluster pods through the Kubernetes API.
//!
//! A [`Forward`] listens on `127.0.0.1` and carries each accepted
//! connection over its own port-forward stream, so one broken connection
//! never takes the others down. Dropping it stops accepting.

use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ListParams};
use kube::Client;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::kube_util::is_pod_ready;

/// A running forward.
#[derive(Debug)]
pub struct Forward {
    /// The local port on `127.0.0.1`.
    pub local_port: u16,
    cancel: CancellationToken,
}

impl Forward {
    /// `127.0.0.1:<local_port>`.
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.local_port)
    }

    /// Stops accepting new connections.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for Forward {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Forwards a local port to `port` on the pod `namespace/pod`.
pub async fn pod_port_forward(
    client: &Client,
    namespace: &str,
    pod: &str,
    port: u16,
) -> Result<Forward, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("port forwarding: {e}"))?;
    let local_port = listener
        .local_addr()
        .map_err(|e| format!("port forwarding: {e}"))?
        .port();
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    // Fail early when the pod cannot be forwarded to at all.
    pods.get(pod)
        .await
        .map_err(|e| format!("getting pod {namespace}/{pod}: {e}"))?;
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let pod = pod.to_string();
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                _ = stop.cancelled() => return,
                a = listener.accept() => a,
            };
            let Ok((mut local, _)) = accepted else {
                continue;
            };
            let pods = pods.clone();
            let pod = pod.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let Ok(mut forwarder) = pods.portforward(&pod, &[port]).await else {
                    return;
                };
                let Some(mut upstream) = forwarder.take_stream(port) else {
                    return;
                };
                tokio::select! {
                    _ = stop.cancelled() => {}
                    _ = tokio::io::copy_bidirectional(&mut local, &mut upstream) => {}
                }
                drop(upstream);
                let _ = forwarder.join().await;
            });
        }
    });
    Ok(Forward { local_port, cancel })
}

/// The first ready pod behind `service`.
pub async fn first_ready_pod_for_service(
    client: &Client,
    namespace: &str,
    service: &str,
) -> Result<(Pod, Service), String> {
    let svc = Api::<Service>::namespaced(client.clone(), namespace)
        .get(service)
        .await
        .map_err(|e| format!("getting {service} service: {e}"))?;
    let selector = svc
        .spec
        .as_ref()
        .and_then(|s| s.selector.clone())
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("service {service} has no selector"));
    }
    let selector = selector
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| format!("listing {service} pods: {e}"))?;
    pods.items
        .into_iter()
        .find(is_pod_ready)
        .map(|p| (p, svc))
        .ok_or_else(|| format!("no ready {service} pods in {namespace}"))
}

/// The container port a Service port maps to on `pod`: an integer
/// targetPort (defaulting to the Service port), or a named container port.
pub fn resolve_target_port(svc: &Service, pod: &Pod, service_port: u16) -> Result<u16, String> {
    let name = svc.metadata.name.clone().unwrap_or_default();
    let ports = svc
        .spec
        .as_ref()
        .and_then(|s| s.ports.clone())
        .unwrap_or_default();
    let sp = ports
        .iter()
        .find(|p| p.port == i32::from(service_port))
        .ok_or_else(|| format!("service {name} has no port {service_port}"))?;
    match &sp.target_port {
        None | Some(IntOrString::Int(0)) => Ok(service_port),
        Some(IntOrString::Int(p)) => {
            u16::try_from(*p).map_err(|_| format!("service {name} has invalid targetPort {p}"))
        }
        Some(IntOrString::String(named)) => {
            let containers = pod
                .spec
                .as_ref()
                .map(|s| s.containers.clone())
                .unwrap_or_default();
            containers
                .iter()
                .flat_map(|c| c.ports.iter().flatten())
                .find(|cp| cp.name.as_deref() == Some(named.as_str()))
                .and_then(|cp| u16::try_from(cp.container_port).ok())
                .ok_or_else(|| {
                    format!(
                        "named targetPort {named:?} not found on pod {}",
                        pod.metadata.name.clone().unwrap_or_default()
                    )
                })
        }
    }
}

/// Forwards a local port to the Service port `service_port` of `service`,
/// on its first ready pod.
pub async fn service_port_forward(
    client: &Client,
    namespace: &str,
    service: &str,
    service_port: u16,
) -> Result<Forward, String> {
    let (pod, svc) = first_ready_pod_for_service(client, namespace, service).await?;
    let port = resolve_target_port(&svc, &pod, service_port)?;
    pod_port_forward(
        client,
        namespace,
        &pod.metadata.name.unwrap_or_default(),
        port,
    )
    .await
}

/// Forwards a local port to `container_port` on the first ready pod behind
/// `service`, for a port the Service does not publish.
pub async fn service_pod_port_forward(
    client: &Client,
    namespace: &str,
    service: &str,
    container_port: u16,
) -> Result<Forward, String> {
    let (pod, _) = first_ready_pod_for_service(client, namespace, service).await?;
    pod_port_forward(
        client,
        namespace,
        &pod.metadata.name.unwrap_or_default(),
        container_port,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Container, ContainerPort, PodSpec, ServicePort, ServiceSpec};

    fn svc(target: Option<IntOrString>) -> Service {
        Service {
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 443,
                    target_port: target,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn target_ports() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    ports: Some(vec![ContainerPort {
                        name: Some("grpc".into()),
                        container_port: 8443,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(resolve_target_port(&svc(None), &pod, 443), Ok(443));
        assert_eq!(
            resolve_target_port(&svc(Some(IntOrString::Int(9443))), &pod, 443),
            Ok(9443)
        );
        assert_eq!(
            resolve_target_port(&svc(Some(IntOrString::String("grpc".into()))), &pod, 443),
            Ok(8443)
        );
        assert!(
            resolve_target_port(&svc(Some(IntOrString::String("nope".into()))), &pod, 443).is_err()
        );
        assert!(resolve_target_port(&svc(None), &pod, 80).is_err());
    }
}
