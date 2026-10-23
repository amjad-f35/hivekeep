//! Plain server pods stood up beside the code under test: the origin an
//! Actor's egress lands on, or a client that dials the gate itself. One
//! shared manifest renders them all; a suite names only what differs.

use std::net::IpAddr;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    Pod, PodCertificateProjection, ProjectedVolumeSource, Service, Volume, VolumeMount,
    VolumeProjection,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams, PostParams};

use crate::harness::T;
use crate::images::Image;
use crate::manifest::{
    kubectl_apply, render_manifest, render_str, split_documents, strict_k8s, yaml_list_block,
    Substitutions,
};
use crate::namespace::create_namespace;
use crate::pod::wait_for_pod_ready;

/// The manifest every server pod is rendered from.
pub const SERVER_POD_TEMPLATE: &str = "fixtures/serverpod.yaml.tmpl";
/// Covers a cold image pull on a small node; the servers listen at once.
pub const SERVER_POD_READY_TIMEOUT: Duration = Duration::from_secs(180);
/// The container port the test server serves HTTPS on when given a
/// certificate.
pub const SERVER_TLS_TARGET_PORT: u16 = 8443;
/// Where [`service_dns_tls`] mounts the server's chain and key, one file.
pub const SERVICE_DNS_BUNDLE: &str = "/run/servicedns.podcert.hivekeep.io/credential-bundle.pem";

/// A server to deploy.
#[derive(Debug, Clone)]
pub struct ServerPod {
    /// Names the Pod, its container and the Service alike.
    pub name: String,
    /// The image; its binary must take `--listen=:<port>`.
    pub image: Image,
    /// Passed ahead of the `--listen` the template appends; with the test
    /// server this names the subcommand, e.g. `["grpc"]`.
    pub args: Vec<String>,
    /// What the Service publishes, and so what an assertion sees.
    pub port: u16,
    /// What the binary listens on, defaulting to `port`. Set it to publish
    /// a port the unprivileged container cannot bind, such as 80.
    pub target_port: u16,
    /// When set, the HTTPS listener (container port 8443) is published here
    /// beside the cleartext one, and readiness is probed over HTTPS.
    pub tls_port: u16,
    /// Deploys into this existing namespace instead of a fresh one.
    pub namespace: String,
    /// Probe with the gRPC health protocol instead of an HTTP GET.
    pub grpc_probe: bool,
    /// The HTTP readiness path, defaulting to `/healthz`.
    pub health_path: String,
    /// Volumes the server mounts.
    pub volumes: Vec<Volume>,
    /// Their mounts.
    pub volume_mounts: Vec<VolumeMount>,
}

impl Default for ServerPod {
    fn default() -> Self {
        ServerPod {
            name: String::new(),
            image: Image::Testserver,
            args: Vec::new(),
            port: 0,
            target_port: 0,
            tls_port: 0,
            namespace: String::new(),
            grpc_probe: false,
            health_path: String::new(),
            volumes: Vec::new(),
            volume_mounts: Vec::new(),
        }
    }
}

/// `spec` serving HTTPS as well as HTTP, with a certificate from the
/// cluster's service-DNS signer covering only [`Server::svc_name`]. Unless
/// set, HTTP is on 80 and HTTPS on 443. The server must be the test
/// server's `http` or `websocket` subcommand.
pub fn service_dns_tls(mut spec: ServerPod) -> ServerPod {
    if spec.port == 0 {
        spec.port = 80;
        spec.target_port = 8080;
    }
    if spec.tls_port == 0 {
        spec.tls_port = 443;
    }
    spec.args.push(format!("--tls-cert={SERVICE_DNS_BUNDLE}"));
    spec.args.push(format!("--tls-key={SERVICE_DNS_BUNDLE}"));
    let (dir, file) = SERVICE_DNS_BUNDLE.rsplit_once('/').expect("absolute path");
    spec.volumes.push(Volume {
        name: "servicedns".into(),
        projected: Some(ProjectedVolumeSource {
            sources: Some(vec![VolumeProjection {
                pod_certificate: Some(PodCertificateProjection {
                    signer_name: crate::install::SERVICE_DNS_SIGNER.into(),
                    key_type: "ECDSAP256".into(),
                    credential_bundle_path: Some(file.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    });
    spec.volume_mounts.push(VolumeMount {
        name: "servicedns".into(),
        mount_path: dir.into(),
        read_only: Some(true),
        ..Default::default()
    });
    spec
}

/// A deployed server, as the address a caller dials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Server {
    /// The Service's (and Pod's) name.
    pub name: String,
    /// Its namespace.
    pub namespace: String,
    /// The Service address. Dialing an IP keeps a sandboxed caller off its
    /// resolver and makes the authority in a gate access log exactly what
    /// the test deployed; a TLS client needs [`Server::svc_name`] instead.
    pub cluster_ip: String,
    /// The published cleartext port.
    pub port: u16,
    /// The published HTTPS port, or 0.
    pub tls_port: u16,
}

fn join_host_port(host: &str, port: u16) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{port}"),
        _ => format!("{host}:{port}"),
    }
}

impl Server {
    /// The host:port to dial.
    pub fn address(&self) -> String {
        join_host_port(&self.cluster_ip, self.port)
    }

    /// The host:port of the HTTPS listener.
    pub fn tls_address(&self) -> String {
        join_host_port(&self.cluster_ip, self.tls_port)
    }

    /// `<name>.<namespace>.svc`, the one name a [`service_dns_tls`]
    /// server's certificate covers.
    pub fn svc_name(&self) -> String {
        format!("{}.{}.svc", self.name, self.namespace)
    }
}

/// The substitutions of `spec` rendered into `namespace`.
pub fn server_pod_substitutions(
    spec: &ServerPod,
    namespace: &str,
) -> Result<Substitutions, String> {
    let target_port = if spec.target_port == 0 {
        spec.port
    } else {
        spec.target_port
    };
    if spec.tls_port != 0 && target_port == SERVER_TLS_TARGET_PORT {
        return Err(format!(
            "server {} listens in cleartext on {SERVER_TLS_TARGET_PORT}, which its HTTPS listener (tls_port) needs; set target_port to another port",
            spec.name
        ));
    }
    let tp = target_port.to_string();
    let mut s = Substitutions::default();
    s.inline("${NAME}", spec.name.clone())
        .inline("${NAMESPACE}", namespace)
        .inline("${IMAGE}", spec.image.reference())
        .inline("${PORT}", spec.port.to_string())
        .inline("${TARGET_PORT}", tp.clone());
    s.block("${ARGS}", server_args(spec, &tp));
    s.block("${READINESS_PROBE}", server_readiness_probe(spec, &tp));
    // Indented to the container's and the Service's `ports:` lists.
    s.block(
        "${TLS_CONTAINER_PORT}",
        if spec.tls_port == 0 {
            String::new()
        } else {
            format!("    - name: serve-tls\n      containerPort: {SERVER_TLS_TARGET_PORT}")
        },
    );
    s.block(
        "${TLS_SERVICE_PORT}",
        if spec.tls_port == 0 {
            String::new()
        } else {
            format!(
                "  - name: serve-tls\n    port: {}\n    targetPort: {SERVER_TLS_TARGET_PORT}",
                spec.tls_port
            )
        },
    );
    // volumeMounts is a container field, volumes a pod one.
    s.block(
        "${VOLUME_MOUNTS}",
        yaml_list_block("volumeMounts", &spec.volume_mounts, 4)?,
    );
    s.block("${VOLUMES}", yaml_list_block("volumes", &spec.volumes, 2)?);
    Ok(s)
}

/// The container's `args:` list: `spec.args` then the `--listen` the
/// template always appends.
fn server_args(spec: &ServerPod, target_port: &str) -> String {
    const PAD: &str = "    ";
    let mut out = vec![format!("{PAD}args:")];
    for arg in &spec.args {
        out.push(format!(
            "{PAD}- {}",
            serde_json::to_string(arg).unwrap_or_default()
        ));
    }
    out.push(format!("{PAD}- \"--listen=:{target_port}\""));
    out.join("\n")
}

/// The probe fragment under the template's `readinessProbe:`. kubelet
/// dials the container, so it names the listen port; a TLS server is probed
/// over HTTPS on the listener that needs its certificate.
fn server_readiness_probe(spec: &ServerPod, target_port: &str) -> String {
    if spec.grpc_probe {
        return format!("      grpc:\n        port: {target_port}");
    }
    let path = if spec.health_path.is_empty() {
        "/healthz"
    } else {
        &spec.health_path
    };
    let (port, scheme) = if spec.tls_port != 0 {
        (SERVER_TLS_TARGET_PORT.to_string(), "HTTPS")
    } else {
        (target_port.to_string(), "HTTP")
    };
    format!("      httpGet:\n        path: {path}\n        port: {port}\n        scheme: {scheme}")
}

/// Renders `spec` into a temp file of `t`.
pub fn render_server_pod(t: &T, spec: &ServerPod, namespace: &str) -> std::path::PathBuf {
    let subs = server_pod_substitutions(spec, namespace).unwrap_or_else(|e| t.fatal(e));
    render_manifest(t, SERVER_POD_TEMPLATE, &subs)
}

/// Renders `spec` to text and decodes its Pod and Service strictly.
pub fn render_server_pod_docs(
    spec: &ServerPod,
    namespace: &str,
) -> Result<(String, Pod, Service), String> {
    let raw = std::fs::read_to_string(crate::manifest::fixture_path(SERVER_POD_TEMPLATE))
        .map_err(|e| e.to_string())?;
    let rendered = render_str(&raw, &server_pod_substitutions(spec, namespace)?);
    let (mut pod, mut service) = (None, None);
    for doc in split_documents(&rendered) {
        if doc.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_yaml_ng::from_str(doc)
            .map_err(|e| format!("rendered server manifest is not valid YAML: {e}\n{doc}"))?;
        match v.get("kind").and_then(|k| k.as_str()) {
            Some("Pod") => {
                pod = Some(strict_k8s::<Pod>(doc).map_err(|e| {
                    format!("rendered server Pod does not match the API type: {e}\n{doc}")
                })?)
            }
            Some("Service") => {
                service = Some(strict_k8s::<Service>(doc).map_err(|e| {
                    format!("rendered server Service does not match the API type: {e}\n{doc}")
                })?)
            }
            _ => {}
        }
    }
    match (pod, service) {
        (Some(p), Some(s)) => Ok((rendered, p, s)),
        _ => Err(format!(
            "rendered server manifest is missing a Pod or a Service:\n{rendered}"
        )),
    }
}

/// Applies `spec`'s manifest, waits for readiness and returns the address
/// to dial. Nothing is cleaned up explicitly: everything is namespaced and
/// goes with the namespace (and is kept with it on failure).
pub async fn deploy_server_pod(t: &T, spec: &ServerPod) -> Server {
    let namespace = if spec.namespace.is_empty() {
        create_namespace(t).await.name
    } else {
        spec.namespace.clone()
    };
    kubectl_apply(t, &render_server_pod(t, spec, &namespace));
    wait_for_pod_ready(t, &namespace, &spec.name, SERVER_POD_READY_TIMEOUT).await;
    let svc = Api::<Service>::namespaced(crate::suite::clients().kube(), &namespace)
        .get(&spec.name)
        .await
        .unwrap_or_else(|e| t.fatal(format!("getting service {namespace}/{}: {e}", spec.name)));
    let ip = svc.spec.and_then(|s| s.cluster_ip).unwrap_or_default();
    if ip.is_empty() || ip == "None" {
        t.fatal(format!(
            "service {namespace}/{} has no ClusterIP to dial: {ip:?}",
            spec.name
        ));
    }
    let server = Server {
        name: spec.name.clone(),
        namespace: namespace.clone(),
        cluster_ip: ip,
        port: spec.port,
        tls_port: spec.tls_port,
    };
    if server.tls_port != 0 {
        t.log(format!(
            "server {} is serving at {} and HTTPS at {} (namespace {namespace})",
            spec.name,
            server.address(),
            server.tls_address()
        ));
    } else {
        t.log(format!(
            "server {} is serving at {} (namespace {namespace})",
            spec.name,
            server.address()
        ));
    }
    server
}

/// Whether `pod` is published as a ready, non-terminating TCP backend on
/// `port` by one of `slices`.
pub fn server_endpoint_ready(slices: &[EndpointSlice], pod: &Pod, port: i32) -> bool {
    let pod_ip = pod
        .status
        .as_ref()
        .and_then(|s| s.pod_ip.clone())
        .unwrap_or_default();
    slices.iter().any(|slice| {
        let has_port = slice
            .ports
            .iter()
            .flatten()
            .any(|p| p.port == Some(port) && p.protocol.as_deref() == Some("TCP"));
        has_port
            && slice.endpoints.iter().flatten().any(|e| {
                let Some(r) = &e.target_ref else { return false };
                if r.kind.as_deref() != Some("Pod")
                    || r.namespace != pod.metadata.namespace
                    || r.name != pod.metadata.name
                    || r.uid != pod.metadata.uid
                {
                    return false;
                }
                let c = e.conditions.clone().unwrap_or_default();
                c.ready == Some(true)
                    && c.terminating != Some(true)
                    && e.addresses.contains(&pod_ip)
            })
    })
}

/// Reserves the server's address before its Pod starts, so a caller can
/// issue a certificate for that IP. The namespace must exist;
/// [`deploy_server_pod`] later applies the same Service without replacing
/// it.
pub async fn create_server_service(t: &T, spec: &ServerPod) -> Server {
    if spec.namespace.is_empty() {
        t.fatal("create_server_service requires a test namespace");
    }
    let (_, _, service) =
        render_server_pod_docs(spec, &spec.namespace).unwrap_or_else(|e| t.fatal(e));
    let created = Api::<Service>::namespaced(crate::suite::clients().kube(), &spec.namespace)
        .create(&PostParams::default(), &service)
        .await
        .unwrap_or_else(|e| t.fatal(format!("creating origin Service: {e}")));
    let ip = created.spec.and_then(|s| s.cluster_ip).unwrap_or_default();
    if ip.parse::<IpAddr>().is_err() {
        t.fatal(format!("origin Service has invalid IP {ip:?}"));
    }
    Server {
        name: spec.name.clone(),
        namespace: spec.namespace.clone(),
        cluster_ip: ip,
        port: spec.port,
        tls_port: spec.tls_port,
    }
}

/// Waits (30 s) for the current origin Pod to be published as a ready
/// Service backend. It does not perform the protocol exchange under test.
pub async fn wait_for_server_endpoint(t: &T, spec: &ServerPod) {
    let kube = crate::suite::clients().kube();
    let pod = Api::<Pod>::namespaced(kube.clone(), &spec.namespace)
        .get(&spec.name)
        .await
        .unwrap_or_else(|e| t.fatal(format!("reading origin Pod for endpoint readiness: {e}")));
    let port = i32::from(if spec.target_port == 0 {
        spec.port
    } else {
        spec.target_port
    });
    let slices: Api<EndpointSlice> = Api::namespaced(kube, &spec.namespace);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let list = slices
            .list(
                &ListParams::default().labels(&format!("kubernetes.io/service-name={}", spec.name)),
            )
            .await
            .unwrap_or_else(|e| t.fatal(format!("reading origin EndpointSlices: {e}")));
        if server_endpoint_ready(&list.items, &pod, port) {
            return;
        }
        if std::time::Instant::now() > deadline {
            t.fatal(format!(
                "origin {}/{} has no ready endpoint for Pod {:?} on port {port}",
                spec.namespace, spec.name, pod.metadata.uid
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ClusterTrustBundleProjection, ObjectReference, SecretVolumeSource,
    };
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    fn docs(spec: &ServerPod) -> (Pod, Service) {
        let (raw, pod, svc) = render_server_pod_docs(spec, "test-namespace").unwrap();
        assert!(
            !raw.contains("${"),
            "rendered server manifest still carries a placeholder:\n{raw}"
        );
        (pod, svc)
    }

    fn int(p: &Option<IntOrString>) -> i32 {
        match p {
            Some(IntOrString::Int(i)) => *i,
            other => panic!("not an int port: {other:?}"),
        }
    }

    /// A bare origin, no credentials, as the networking suite deploys it.
    #[test]
    fn render_server_pod_grpc_probe() {
        let (pod, service) = docs(&ServerPod {
            name: "grpcecho".into(),
            args: vec!["grpc".into()],
            port: 50051,
            grpc_probe: true,
            ..Default::default()
        });
        let spec = pod.spec.clone().unwrap();
        let c = &spec.containers[0];
        assert_eq!(
            c.image.as_deref(),
            Some(Image::Testserver.reference().as_str())
        );
        // The port reaches the binary, the container port and the Service alike.
        assert_eq!(c.args.clone().unwrap(), vec!["grpc", "--listen=:50051"]);
        assert_eq!(c.ports.as_ref().unwrap()[0].container_port, 50051);
        let sspec = service.spec.clone().unwrap();
        assert_eq!(sspec.ports.as_ref().unwrap()[0].port, 50051);
        assert_eq!(
            sspec.selector.as_ref().unwrap().get("app"),
            pod.metadata.labels.as_ref().unwrap().get("app")
        );
        let probe = c.readiness_probe.clone().unwrap();
        assert_eq!(probe.grpc.as_ref().unwrap().port, 50051);
        assert!(probe.http_get.is_none());
        // A distroless base declares no USER, so the uid must be explicit.
        assert_eq!(
            c.security_context.as_ref().and_then(|s| s.run_as_user),
            Some(65532)
        );
        // An empty list takes its whole line, key included.
        assert!(
            spec.volumes.unwrap_or_default().is_empty()
                && c.volume_mounts.clone().unwrap_or_default().is_empty()
        );
    }

    /// The other probe kind, and the default health path.
    #[test]
    fn render_server_pod_http_probe() {
        let (pod, _) = docs(&ServerPod {
            name: "httporigin".into(),
            args: vec!["http".into()],
            port: 8080,
            ..Default::default()
        });
        let probe = pod.spec.unwrap().containers[0]
            .readiness_probe
            .clone()
            .unwrap();
        let get = probe.http_get.unwrap();
        assert_eq!(get.path.as_deref(), Some("/healthz"));
        assert_eq!(get.port, IntOrString::Int(8080));
        assert!(probe.grpc.is_none());
    }

    /// Every field kubelet or the binary reaches follows the listener; only
    /// the Service keeps the published port.
    #[test]
    fn render_server_pod_ports() {
        let cases = [
            (
                "maps a privileged published port to an unprivileged listener",
                ServerPod {
                    name: "egresshttp".into(),
                    args: vec!["http".into()],
                    port: 80,
                    target_port: 8080,
                    ..Default::default()
                },
                80,
                8080,
            ),
            (
                "defaults the listener to port when target_port is unset",
                ServerPod {
                    name: "httporigin".into(),
                    args: vec!["http".into()],
                    port: 8080,
                    ..Default::default()
                },
                8080,
                8080,
            ),
        ];
        for (name, spec, published, listen) in cases {
            let (pod, service) = docs(&spec);
            let c = pod.spec.unwrap().containers[0].clone();
            let sports = service.spec.unwrap().ports.unwrap();
            assert_eq!(
                (c.ports.as_ref().unwrap().len(), sports.len()),
                (1, 1),
                "{name}"
            );
            assert_eq!(
                c.args.unwrap(),
                vec!["http".to_string(), format!("--listen=:{listen}")],
                "{name}"
            );
            assert_eq!(c.ports.unwrap()[0].container_port, listen, "{name}");
            assert_eq!(
                c.readiness_probe.unwrap().http_get.unwrap().port,
                IntOrString::Int(listen),
                "{name}"
            );
            assert_eq!(sports[0].port, published, "{name}");
            assert_eq!(int(&sports[0].target_port), listen, "{name}");
        }
    }

    /// Both volume kinds the credential-carrying servers need; the
    /// projection nests deepest, so it is what an indentation slip shows in.
    #[test]
    fn render_server_pod_volumes() {
        let spec = ServerPod {
            name: "egressprobe".into(),
            args: vec!["egressprobe".into()],
            port: 8080,
            volume_mounts: vec![
                VolumeMount {
                    name: "actor-identity".into(),
                    mount_path: "/run/actor-identity".into(),
                    ..Default::default()
                },
                VolumeMount {
                    name: "podidentity".into(),
                    mount_path: "/run/podidentity.podcert.hivekeep.io".into(),
                    ..Default::default()
                },
            ],
            volumes: vec![
                Volume {
                    name: "actor-identity".into(),
                    secret: Some(SecretVolumeSource {
                        secret_name: Some("probe-actor-identity".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Volume {
                    name: "podidentity".into(),
                    projected: Some(ProjectedVolumeSource {
                        sources: Some(vec![VolumeProjection {
                            cluster_trust_bundle: Some(ClusterTrustBundleProjection {
                                signer_name: Some(crate::install::SERVICE_DNS_SIGNER.into()),
                                label_selector: Some(LabelSelector {
                                    match_labels: Some(
                                        [(
                                            "podcert.hivekeep.io/canarying".to_string(),
                                            "live".to_string(),
                                        )]
                                        .into(),
                                    ),
                                    ..Default::default()
                                }),
                                path: "trust-bundle.pem".into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (pod, _) = docs(&spec);
        let pspec = pod.spec.unwrap();
        let mounts = pspec.containers[0].volume_mounts.clone().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[0].mount_path, "/run/actor-identity");
        let volumes = pspec.volumes.unwrap();
        assert_eq!(volumes.len(), 2);
        assert_eq!(
            volumes[0].secret.as_ref().unwrap().secret_name.as_deref(),
            Some("probe-actor-identity")
        );
        let ctb = volumes[1]
            .projected
            .as_ref()
            .unwrap()
            .sources
            .as_ref()
            .unwrap()[0]
            .cluster_trust_bundle
            .clone()
            .unwrap();
        assert_eq!(
            ctb.signer_name.as_deref(),
            Some(crate::install::SERVICE_DNS_SIGNER)
        );
        assert_eq!(
            ctb.label_selector
                .unwrap()
                .match_labels
                .unwrap()
                .get("podcert.hivekeep.io/canarying")
                .map(String::as_str),
            Some("live")
        );
        // Every mount names a volume that exists.
        for m in &mounts {
            assert!(
                volumes.iter().any(|v| v.name == m.name),
                "volumeMount {} names no volume",
                m.name
            );
        }
    }

    /// Both schemes: cleartext on port, HTTPS on 8443 published on tls_port
    /// and probed over HTTPS.
    #[test]
    fn render_server_pod_tls_port() {
        let (pod, service) = docs(&ServerPod {
            name: "tls".into(),
            port: 80,
            target_port: 8080,
            tls_port: 443,
            ..Default::default()
        });
        let c = pod.spec.unwrap().containers[0].clone();
        let ports = c.ports.clone().unwrap();
        assert_eq!(ports.len(), 2);
        assert_eq!(
            (ports[1].name.as_deref(), ports[1].container_port),
            (Some("serve-tls"), 8443)
        );
        let sports = service.spec.unwrap().ports.unwrap();
        assert_eq!(sports.len(), 2);
        assert_eq!((sports[0].port, int(&sports[0].target_port)), (80, 8080));
        assert_eq!(
            (
                sports[1].name.as_deref(),
                sports[1].port,
                int(&sports[1].target_port)
            ),
            (Some("serve-tls"), 443, 8443)
        );
        let get = c.readiness_probe.unwrap().http_get.unwrap();
        assert_eq!(
            (get.scheme.as_deref(), get.port),
            (Some("HTTPS"), IntOrString::Int(8443))
        );
        assert!(server_pod_substitutions(
            &ServerPod {
                name: "x".into(),
                port: 8443,
                tls_port: 443,
                ..Default::default()
            },
            "n"
        )
        .is_err());
    }

    #[test]
    fn service_dns_tls_shape() {
        let base = ServerPod {
            name: "wss-origin".into(),
            args: vec!["websocket".into(), "--echo".into()],
            health_path: "/readyz".into(),
            volumes: vec![Volume {
                name: "extra".into(),
                empty_dir: Some(Default::default()),
                ..Default::default()
            }],
            volume_mounts: vec![VolumeMount {
                name: "extra".into(),
                mount_path: "/run/extra".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let spec = service_dns_tls(base.clone());
        // Pure: the caller's value is untouched (it is moved in by clone).
        assert_eq!(
            (
                base.args.len(),
                base.volumes.len(),
                base.port,
                base.tls_port
            ),
            (2, 1, 0, 0)
        );
        assert_eq!(
            (
                spec.port,
                spec.target_port,
                spec.tls_port,
                spec.health_path.as_str()
            ),
            (80, 8080, 443, "/readyz")
        );
        let explicit = service_dns_tls(ServerPod {
            port: 8080,
            tls_port: 8443,
            ..Default::default()
        });
        assert_eq!(
            (explicit.port, explicit.target_port, explicit.tls_port),
            (8080, 0, 8443)
        );

        let (pod, _) = docs(&spec);
        let pspec = pod.spec.unwrap();
        let c = &pspec.containers[0];
        let want = vec![
            "websocket".to_string(),
            "--echo".into(),
            format!("--tls-cert={SERVICE_DNS_BUNDLE}"),
            format!("--tls-key={SERVICE_DNS_BUNDLE}"),
            "--listen=:8080".into(),
        ];
        assert_eq!(c.args.clone().unwrap(), want);
        let get = c.readiness_probe.clone().unwrap().http_get.unwrap();
        assert_eq!(
            (get.scheme.as_deref(), get.path.as_deref(), get.port),
            (Some("HTTPS"), Some("/readyz"), IntOrString::Int(8443))
        );
        let volumes = pspec.volumes.unwrap();
        let mounts = c.volume_mounts.clone().unwrap();
        assert_eq!((volumes.len(), mounts.len()), (2, 2));
        let cert = volumes[1]
            .projected
            .as_ref()
            .unwrap()
            .sources
            .as_ref()
            .unwrap()[0]
            .pod_certificate
            .clone()
            .unwrap();
        assert_eq!(
            (
                cert.signer_name.as_str(),
                cert.key_type.as_str(),
                cert.credential_bundle_path.as_deref()
            ),
            (
                crate::install::SERVICE_DNS_SIGNER,
                "ECDSAP256",
                Some("credential-bundle.pem")
            )
        );
        let m = &mounts[1];
        assert_eq!(m.name, volumes[1].name);
        assert_eq!(
            format!("{}/{}", m.mount_path, cert.credential_bundle_path.unwrap()),
            SERVICE_DNS_BUNDLE
        );
        assert_eq!(m.read_only, Some(true));
    }

    #[test]
    fn server_addresses() {
        for (ip, addr, tls) in [
            ("10.96.0.7", "10.96.0.7:80", "10.96.0.7:443"),
            ("fd00:10:96::7", "[fd00:10:96::7]:80", "[fd00:10:96::7]:443"),
        ] {
            let s = Server {
                name: "mitm-origin".into(),
                namespace: "abcd-123".into(),
                cluster_ip: ip.into(),
                port: 80,
                tls_port: 443,
            };
            assert_eq!(s.svc_name(), "mitm-origin.abcd-123.svc");
            assert_eq!(s.address(), addr);
            assert_eq!(s.tls_address(), tls);
        }
    }

    #[test]
    fn server_endpoint_readiness() {
        let mut pod = Pod::default();
        pod.metadata.name = Some("origin".into());
        pod.metadata.namespace = Some("test".into());
        pod.metadata.uid = Some("current".into());
        pod.status = Some(k8s_openapi::api::core::v1::PodStatus {
            pod_ip: Some("10.0.0.2".into()),
            ..Default::default()
        });
        let base = EndpointSlice {
            address_type: "IPv4".into(),
            ports: Some(vec![EndpointPort {
                port: Some(8443),
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            endpoints: Some(vec![Endpoint {
                addresses: vec!["10.0.0.2".into()],
                target_ref: Some(ObjectReference {
                    kind: Some("Pod".into()),
                    namespace: Some("test".into()),
                    name: Some("origin".into()),
                    uid: Some("current".into()),
                    ..Default::default()
                }),
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        };
        type Change = fn(&mut EndpointSlice);
        let cases: Vec<(&str, Change, bool)> = vec![
            ("ready origin", |_| {}, true),
            (
                "not ready",
                |s| {
                    s.endpoints.as_mut().unwrap()[0]
                        .conditions
                        .as_mut()
                        .unwrap()
                        .ready = Some(false)
                },
                false,
            ),
            (
                "unknown readiness",
                |s| {
                    s.endpoints.as_mut().unwrap()[0]
                        .conditions
                        .as_mut()
                        .unwrap()
                        .ready = None
                },
                false,
            ),
            (
                "previous pod",
                |s| {
                    s.endpoints.as_mut().unwrap()[0]
                        .target_ref
                        .as_mut()
                        .unwrap()
                        .uid = Some("previous".into())
                },
                false,
            ),
            (
                "wrong address",
                |s| s.endpoints.as_mut().unwrap()[0].addresses = vec!["10.0.0.3".into()],
                false,
            ),
            (
                "wrong port",
                |s| s.ports.as_mut().unwrap()[0].port = Some(443),
                false,
            ),
            (
                "terminating",
                |s| {
                    s.endpoints.as_mut().unwrap()[0]
                        .conditions
                        .as_mut()
                        .unwrap()
                        .terminating = Some(true)
                },
                false,
            ),
        ];
        for (name, change, want) in cases {
            let mut s = base.clone();
            change(&mut s);
            assert_eq!(server_endpoint_ready(&[s], &pod, 8443), want, "{name}");
        }
    }
}
