//! Installing the hostpath and NFS CSI drivers the external-volume demos
//! use, each fronted by an mTLS proxy hivekeep-api dials and registered with
//! a CSIDriverConfig.

use super::env::BOOTSTRAP_TIMEOUT;
use super::{Env, Result, StepError};
use crate::installdefaults;
use crate::kube::{self, describe, Gvk, ObjectExt, Patch, WorkloadKind};
use crate::log;
use std::path::PathBuf;

/// Host directory the keeper mounts sandbox images under. Both drivers bind
/// it with bidirectional propagation so volumes they mount inside it are
/// visible to the keeper.
pub(crate) const WORKER_HOST_DIR: &str = "/var/lib/hivekeep";

/// Where the third-party driver bundles are expected, under the manifests
/// directory. They are not shipped with the installer.
pub(crate) const CSI_HOSTPATH_DIR: &str = "csi/third_party/csi-driver-host-path";
pub(crate) const CSI_NFS_DIR: &str = "csi/third_party/csi-driver-nfs";

/// Fronts the hostpath driver's proxy sidecar, which terminates mTLS on the
/// TCP endpoint hivekeep-api dials. The CSIDriverConfig names it.
pub(crate) const CSI_HOSTPATH_CONTROLLER_SERVICE: &str = "
apiVersion: v1
kind: Service
metadata:
  name: csi-hostpath-controller
  namespace: default
spec:
  selector:
    app.kubernetes.io/name: csi-hostpath-socat
  ports:
  - port: 50051
    targetPort: 10000
    name: grpc
";

const CSI_HOSTPATH_DRIVER_CONFIG: &str = "
apiVersion: hivekeep.io/v1
kind: CSIDriverConfig
metadata:
  name: hostpath.csi.k8s.io
spec:
  driverName: hostpath.csi.k8s.io
  controllerEndpoint: tcp://csi-hostpath-controller.default.svc.cluster.local:50051
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-hostpath/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    serverName: csi-hostpath-controller.default.svc
";

/// Fronts the Envoy sidecar the controller patch adds to the NFS controller.
pub(crate) const CSI_NFS_CONTROLLER_SERVICE: &str = "
apiVersion: v1
kind: Service
metadata:
  name: csi-nfs-controller
  namespace: kube-system
spec:
  selector:
    app: csi-nfs-controller
  ports:
  - port: 50052
    targetPort: 10000
    name: grpc
";

const CSI_NFS_ENVOY_CONFIG_MAP_TEMPLATE: &str = r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: csi-nfs-envoy-config
  namespace: kube-system
data:
  envoy.yaml: |
    node:
      id: csi-nfs-envoy
      cluster: csi-nfs-envoy
    static_resources:
      listeners:
      - name: csi_tls_listener
        address:
          socket_address: { address: 0.0.0.0, port_value: 10000 }
        filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              require_client_certificate: true
              common_tls_context:
                alpn_protocols:
                - h2
                tls_certificate_sds_secret_configs:
                - name: servicedns_serving_cert
                  sds_config:
                    resource_api_version: V3
                    path_config_source:
                      path: /etc/envoy/sds-servicedns-cert.yaml
                combined_validation_context:
                  default_validation_context:
                    match_typed_subject_alt_names:
                    - san_type: URI
                      matcher:
                        exact: "{{API_SERVER_SPIFFE_ID}}"
                  validation_context_sds_secret_config:
                    name: podidentity_validation_context
                    sds_config:
                      resource_api_version: V3
                      path_config_source:
                        path: /etc/envoy/sds-podidentity-validation.yaml
          filters:
          - name: envoy.filters.network.tcp_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
              stat_prefix: csi_proxy
              cluster: csi_unix_socket
      clusters:
      - name: csi_unix_socket
        connect_timeout: 0.25s
        type: STATIC
        load_assignment:
          cluster_name: csi_unix_socket
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  pipe:
                    path: /csi/csi.sock
  sds-servicedns-cert.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: servicedns_serving_cert
      tls_certificate:
        certificate_chain: { filename: /run/servicedns/credential-bundle.pem }
        private_key: { filename: /run/servicedns/credential-bundle.pem }
        watched_directory: { path: /run/servicedns }
  sds-podidentity-validation.yaml: |
    resources:
    - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
      name: podidentity_validation_context
      validation_context:
        trusted_ca: { filename: /run/podidentity-ca/trust-bundle.pem }
        watched_directory: { path: /run/podidentity-ca }
"#;

/// The NFS controller's Envoy config: an mTLS listener accepting only
/// hivekeep-api's identity, proxied to the driver's unix socket.
pub(crate) fn csi_nfs_envoy_config_map(api_server_spiffe_id: &str) -> String {
    CSI_NFS_ENVOY_CONFIG_MAP_TEMPLATE.replace("{{API_SERVER_SPIFFE_ID}}", api_server_spiffe_id)
}

/// Adds the Envoy TLS proxy to the upstream NFS controller and removes the
/// socat sidecar earlier installs added.
const CSI_NFS_CONTROLLER_PATCH: &str = "
spec:
  template:
    spec:
      containers:
      - $patch: delete
        name: socat
      - name: envoy
        image: envoyproxy/envoy:v1.39.5@sha256:8fc594a5d85eb76dca24155cfb9208882406873932b20cf33649a0a611194c43
        command:
        - /usr/local/bin/envoy
        args:
        - -c
        - /etc/envoy/envoy.yaml
        readinessProbe:
          tcpSocket:
            port: 10000
          periodSeconds: 5
        securityContext:
          # Root because the driver creates csi.sock mode 0755; nothing else is needed.
          runAsUser: 0
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop:
            - ALL
        volumeMounts:
        - mountPath: /etc/envoy
          name: envoy-config
          readOnly: true
        - mountPath: /csi
          name: socket-dir
        - mountPath: /run/servicedns
          name: servicedns-certs
          readOnly: true
        - mountPath: /run/podidentity-ca
          name: podidentity-ca
          readOnly: true
      volumes:
      - name: envoy-config
        configMap:
          name: csi-nfs-envoy-config
      - name: servicedns-certs
        projected:
          sources:
          - podCertificate:
              signerName: servicedns.podcert.hivekeep.io/identity
              keyType: ECDSAP256
              credentialBundlePath: credential-bundle.pem
      - name: podidentity-ca
        projected:
          sources:
          - clusterTrustBundle:
              signerName: podidentity.podcert.hivekeep.io/identity
              labelSelector:
                matchLabels:
                  podcert.hivekeep.io/canarying: live
              path: trust-bundle.pem
";

/// Registers the NFS driver; its serverName is the one DNS name the
/// servicedns signer issues, `<service>.<namespace>.svc`.
pub(crate) const CSI_NFS_DRIVER_CONFIG: &str = "
apiVersion: hivekeep.io/v1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
  tls:
    enabled: true
    usePodIdentity: true
    serverName: csi-nfs-controller.kube-system.svc
";

/// Which drivers a `--setup-csi` value installs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CsiDriverSpec {
    pub hostpath: bool,
    pub nfs: bool,
}

impl CsiDriverSpec {
    pub(crate) fn installs_nothing(self) -> bool {
        !self.hostpath && !self.nfs
    }
}

/// Every accepted `--setup-csi` value. `true`, `false` and empty are the
/// older boolean spellings and still work.
pub(crate) fn csi_driver(driver: &str) -> Option<CsiDriverSpec> {
    let both = CsiDriverSpec {
        hostpath: true,
        nfs: true,
    };
    match driver {
        "nfs" => Some(CsiDriverSpec {
            nfs: true,
            hostpath: false,
        }),
        "hostpath" => Some(CsiDriverSpec {
            hostpath: true,
            nfs: false,
        }),
        "both" | "true" => Some(both),
        "none" | "false" | "" => Some(CsiDriverSpec::default()),
        _ => None,
    }
}

/// Whether `driver` names something `--setup-csi` accepts. Needs neither
/// configuration nor cluster, so commands run it while validating
/// arguments: a typo should not cost a credential fetch.
pub fn validate_csi_driver(driver: &str) -> Result<()> {
    csi_driver(driver).map(|_| ()).ok_or_else(|| {
        StepError::msg(format!(
            "unknown CSI driver {driver:?} (valid options: nfs, hostpath, both, none)"
        ))
    })
}

fn yaml_to_json(src: &str, what: &str) -> Result<serde_json::Value> {
    serde_yaml::from_str(src)
        .map_err(|e| StepError::msg(format!("while converting {what} patch to JSON: {e}")))
}

impl Env {
    /// [`validate_csi_driver`] plus the half needing configuration: the
    /// hostpath driver exists only for Kind.
    pub fn check_csi_driver(&self, driver: &str) -> Result<()> {
        validate_csi_driver(driver)?;
        if csi_driver(driver).is_some_and(|s| s.hostpath) {
            return self.require_kind("csi-hostpath");
        }
        Ok(())
    }

    /// Installs the drivers `driver` selects. Both checks come before any
    /// work, so a typo or an unsupported driver leaves nothing behind.
    pub async fn setup_csi(&self, driver: &str) -> Result<()> {
        log::step("setup_csi");
        self.check_csi_driver(driver)?;
        let spec = csi_driver(driver).unwrap_or_default();
        if spec.installs_nothing() {
            log::info("Skipping CSI driver setup.");
            return Ok(());
        }
        self.ensure_csi_prerequisites().await?;
        if spec.hostpath {
            self.setup_csi_hostpath().await?;
        }
        if spec.nfs {
            return self.setup_csi_nfs().await;
        }
        Ok(())
    }

    /// The CRDs (each driver registers a CSIDriverConfig) and the
    /// podcertificate controller (the proxies are issued serving
    /// certificates). Each step returns at once when already done.
    async fn ensure_csi_prerequisites(&self) -> Result<()> {
        self.ensure_crds().await?;
        self.ensure_system_namespace().await?;
        self.deploy_pod_certificate_controller().await
    }

    /// A third-party driver bundle directory, which must be provided.
    fn third_party_dir(&self, rel: &str) -> Result<PathBuf> {
        let dir = self.cfg.manifests_path(&[rel]);
        if !dir.is_dir() {
            return Err(StepError::msg(format!(
                "the CSI driver bundle is not installed at {}; place the upstream driver's deploy manifests there",
                dir.display()
            )));
        }
        Ok(dir)
    }

    async fn setup_csi_hostpath(&self) -> Result<()> {
        log::step("setup_csi_hostpath");
        let k = self.kube()?;
        let driver_dir = self.third_party_dir(CSI_HOSTPATH_DIR)?;
        let bundle = crate::kustomize::build(&driver_dir.join("deploy"))?;
        let objs = kube::decode_manifest_bytes(&bundle)?;

        // A plugin re-applied over a running one leaves stale mounts under
        // the worker directory, so remove the previous deployment first.
        log::info("Cleaning up existing CSI hostpath resources...");
        k.delete(&objs).await?;
        k.delete_named(
            &Gvk::new("", "v1", "Service"),
            "default",
            "csi-hostpath-controller",
            None,
        )
        .await?;
        k.delete_named(
            &Gvk::new("storage.k8s.io", "v1", "StorageClass"),
            "",
            "csi-hostpath-sc",
            None,
        )
        .await?;
        k.wait_deleted(
            &Gvk::new("apps", "v1", "StatefulSet"),
            "default",
            "csi-hostpathplugin",
            self.cfg.rollout_timeout,
        )
        .await?;

        let node = self.kind_node_name().await?;
        log::info(&format!("Using Kind node: {node}"));
        cleanup_kind_worker_dir(&node).await;

        // Before the driver: its proxy sidecar can only be issued a serving
        // certificate once the Service selecting it exists.
        log::info("Exposing the CSI hostpath controller over a TCP Service...");
        k.apply_bytes(CSI_HOSTPATH_CONTROLLER_SERVICE.as_bytes())
            .await?;

        log::info("Deploying the CSI hostpath driver...");
        k.apply_tolerant(&objs, |obj, _| {
            log::warn(&format!(
                "skipping {}: its CRD is not installed on this cluster",
                describe(obj)
            ))
        })
        .await?;
        if k.get(
            &Gvk::new("apps", "v1", "StatefulSet"),
            "default",
            "csi-hostpathplugin",
        )
        .await?
        .is_none()
        {
            return Err(StepError::msg(
                "csi-hostpathplugin StatefulSet was not created",
            ));
        }

        // Pin the plugin to the node whose worker directory it bind-mounts.
        log::info("Patching the CSI hostpath StatefulSet...");
        let patch = format!(
            "
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: {node}
      containers:
      - name: hostpath
        volumeMounts:
        - name: hivekeep-base
          mountPath: {WORKER_HOST_DIR}
          mountPropagation: Bidirectional
      volumes:
      - name: hivekeep-base
        hostPath:
          path: {WORKER_HOST_DIR}
          type: DirectoryOrCreate
"
        );
        k.patch(
            &Gvk::new("apps", "v1", "StatefulSet"),
            "default",
            "csi-hostpathplugin",
            &Patch::Strategic(yaml_to_json(&patch, "csi-hostpathplugin")?),
        )
        .await
        .map_err(|e| {
            StepError::msg(format!(
                "while patching the csi-hostpathplugin StatefulSet: {e}"
            ))
        })?;

        log::info("Creating the csi-hostpath-sc StorageClass...");
        k.apply_path(&driver_dir.join("examples/csi-storageclass.yaml"))
            .await?;

        log::info("Waiting for the CSI hostpath and proxy pods to be ready...");
        for name in ["csi-hostpathplugin", "csi-hostpath-socat"] {
            k.rollout_status(
                WorkloadKind::StatefulSet,
                "default",
                name,
                self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT),
            )
            .await?;
        }

        log::info("Creating the hostpath.csi.k8s.io CSIDriverConfig...");
        k.apply_bytes(CSI_HOSTPATH_DRIVER_CONFIG.as_bytes()).await?;

        // The image cache directories were just wiped; the keeper recreates
        // them on restart.
        log::info("Restarting the keeper DaemonSet (if present)...");
        Ok(
            k.rollout_restart(self.namespace(), "keeper", chrono::Utc::now())
                .await?,
        )
    }

    async fn setup_csi_nfs(&self) -> Result<()> {
        log::step("setup_csi_nfs");
        check_nfsd_support()?;
        let k = self.kube()?;
        let deploy_dir = self.third_party_dir(CSI_NFS_DIR)?.join("deploy");

        log::info("Deploying the NFS server...");
        k.apply_path(&self.cfg.manifests_path(&["csi", "nfs-server.yaml"]))
            .await?;

        // Before the driver: the sidecar's certificate is only issued once a
        // Service selects its pod.
        log::info("Exposing the CSI NFS controller over a TLS Service...");
        k.apply_bytes(CSI_NFS_CONTROLLER_SERVICE.as_bytes()).await?;
        let envoy =
            csi_nfs_envoy_config_map(&installdefaults::api_server_spiffe_id(self.namespace()));
        k.apply_bytes(envoy.as_bytes()).await?;

        log::info("Deploying the CSI NFS driver...");
        for name in [
            "rbac-csi-nfs.yaml",
            "csi-nfs-driverinfo.yaml",
            "csi-nfs-controller.yaml",
            "csi-nfs-node.yaml",
        ] {
            k.apply_path(&deploy_dir.join(name)).await?;
        }

        log::info("Patching the CSI NFS node DaemonSet...");
        let node_patch = format!(
            "
spec:
  template:
    spec:
      containers:
      - name: nfs
        volumeMounts:
        - name: hivekeep-base
          mountPath: {WORKER_HOST_DIR}
          mountPropagation: Bidirectional
      volumes:
      - name: hivekeep-base
        hostPath:
          path: {WORKER_HOST_DIR}
          type: DirectoryOrCreate
"
        );
        k.patch(
            &Gvk::new("apps", "v1", "DaemonSet"),
            "kube-system",
            "csi-nfs-node",
            &Patch::Strategic(yaml_to_json(&node_patch, "csi-nfs-node")?),
        )
        .await
        .map_err(|e| StepError::msg(format!("while patching the csi-nfs-node DaemonSet: {e}")))?;

        log::info("Patching the CSI NFS controller Deployment with the Envoy TLS proxy...");
        k.patch(
            &Gvk::new("apps", "v1", "Deployment"),
            "kube-system",
            "csi-nfs-controller",
            &Patch::Strategic(yaml_to_json(
                CSI_NFS_CONTROLLER_PATCH,
                "csi-nfs-controller",
            )?),
        )
        .await
        .map_err(|e| {
            StepError::msg(format!(
                "while patching the csi-nfs-controller Deployment: {e}"
            ))
        })?;

        // The driver resolves the server's name at provisioning time, so the
        // StorageClass can precede the server's address.
        log::info("Creating the csi-nfs-sc StorageClass...");
        k.apply_path(&deploy_dir.join("example").join("storageclass-nfs.yaml"))
            .await?;

        log::info("Waiting for the NFS server and CSI driver to be ready...");
        let t = self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT);
        k.rollout_status(WorkloadKind::Deployment, "default", "nfs-server", t)
            .await?;
        k.rollout_status(
            WorkloadKind::Deployment,
            "kube-system",
            "csi-nfs-controller",
            t,
        )
        .await?;
        k.rollout_status(WorkloadKind::DaemonSet, "kube-system", "csi-nfs-node", t)
            .await?;

        log::info("Creating the nfs.csi.k8s.io CSIDriverConfig...");
        k.apply_bytes(CSI_NFS_DRIVER_CONFIG.as_bytes()).await?;

        log::info("Restarting the keeper DaemonSet (if present)...");
        Ok(
            k.rollout_restart(self.namespace(), "keeper", chrono::Utc::now())
                .await?,
        )
    }

    /// The node to pin the hostpath driver to: a worker, else the only node.
    async fn kind_node_name(&self) -> Result<String> {
        let k = self.kube()?;
        let node_gvk = Gvk::new("", "v1", "Node");
        let mut nodes = k
            .list(
                &node_gvk,
                "",
                Some("!node-role.kubernetes.io/control-plane"),
            )
            .await
            .map_err(|e| StepError::msg(format!("while listing worker nodes: {e}")))?;
        if nodes.is_empty() {
            nodes = k
                .list(&node_gvk, "", None)
                .await
                .map_err(|e| StepError::msg(format!("while listing nodes: {e}")))?;
        }
        nodes
            .first()
            .map(|n| n.name().to_string())
            .ok_or_else(|| StepError::msg("the cluster reports no nodes"))
    }
}

/// Verifies the host kernel can serve NFS: the in-cluster server runs on the
/// host kernel, and without nfsd mounts fail much less obviously later.
fn check_nfsd_support() -> Result<()> {
    let Ok(filesystems) = std::fs::read_to_string("/proc/filesystems") else {
        // Not Linux, or no procfs: nothing to check against.
        return Ok(());
    };
    if !filesystems.contains("nfsd") {
        return Err(StepError::msg(
            "NFS server support (nfsd) is not active in the host kernel; run: sudo modprobe nfsd",
        ));
    }
    Ok(())
}

/// Clears the sandbox image directory inside the Kind node container, best
/// effort: a stale mount makes removal fail with EBUSY, and the node may not
/// be running. The directory itself stays so running pods' mounts survive.
async fn cleanup_kind_worker_dir(node: &str) {
    log::info("Cleaning up CSI directories on the Kind node...");
    if !docker_container_running(node).await {
        log::warn(&format!(
            "Kind node {node} is not running. Skipping directory cleanup."
        ));
        return;
    }
    let unmount = format!(
        "for mnt in $(mount | grep {WORKER_HOST_DIR} | awk '{{print $3}}'); do umount -f \"${{mnt}}\" || true; done"
    );
    run_quiet("docker", &["exec", node, "sh", "-c", &unmount]).await;
    run_quiet(
        "docker",
        &[
            "exec",
            node,
            "sh",
            "-c",
            &format!("rm -rf {WORKER_HOST_DIR}/*"),
        ],
    )
    .await;
}

async fn docker_container_running(name: &str) -> bool {
    let out = tokio::process::Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("name={name}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .lines()
            .any(|l| l.trim() == name),
        _ => false,
    }
}

async fn run_quiet(program: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_config, Config};

    #[tokio::test]
    async fn setup_csi_decides_before_touching_the_cluster() {
        for (name, driver, want_err) in [
            ("none", "none", None),
            ("the older false", "false", None),
            ("unset", "", None),
            (
                "hostpath off Kind",
                "hostpath",
                Some("only supported for Kind"),
            ),
            ("both off Kind", "both", Some("only supported for Kind")),
            (
                "the older true off Kind",
                "true",
                Some("only supported for Kind"),
            ),
            (
                "unknown driver",
                "nfsv4",
                Some("unknown CSI driver \"nfsv4\""),
            ),
        ] {
            // No cluster: reaching for one is an error naming that.
            let e = Env::local(Config::default());
            match e.setup_csi(driver).await {
                Ok(()) => assert!(want_err.is_none(), "{name}: want an error"),
                Err(err) => {
                    let want = want_err.unwrap_or_else(|| panic!("{name}: {err}"));
                    assert!(err.to_string().contains(want), "{name}: {err}");
                }
            }
        }
    }

    /// The source of a function in this file, from its signature to the
    /// closing brace at its indentation.
    fn function_body(signature: &str) -> &'static str {
        let src: &'static str = include_str!("csi.rs");
        let start = src
            .find(signature)
            .unwrap_or_else(|| panic!("csi.rs has no {signature:?}"));
        let body = &src[start..];
        let end = body.find("\n    }\n").expect("end of function");
        &body[..end]
    }

    #[test]
    fn the_hostpath_controller_service_is_created_before_the_driver() {
        let body = function_body("async fn setup_csi_hostpath(");
        let service = body
            .find("CSI_HOSTPATH_CONTROLLER_SERVICE")
            .expect("applies the controller Service");
        let driver = body
            .find("apply_tolerant")
            .expect("applies the driver bundle tolerantly");
        assert!(
            service < driver,
            "the driver is deployed before its controller Service"
        );
    }

    #[test]
    fn the_nfs_setup_applies_the_shared_storage_class() {
        let body = function_body("async fn setup_csi_nfs(");
        assert!(body.contains(r#"join("example").join("storageclass-nfs.yaml")"#));
        assert!(
            !body.contains("kind: StorageClass"),
            "defines a StorageClass inline"
        );
    }

    #[test]
    #[ignore = "needs the third-party NFS driver bundle under manifests/csi/third_party/csi-driver-nfs"]
    fn the_shared_storage_class_is_csi_nfs_sc() {
        let path = test_config().manifests_path(&[
            CSI_NFS_DIR,
            "deploy",
            "example",
            "storageclass-nfs.yaml",
        ]);
        let objs = kube::load_path(&path).unwrap();
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0].kind(), "StorageClass");
        assert_eq!(objs[0].name(), "csi-nfs-sc");
        assert_eq!(objs[0]["provisioner"], "nfs.csi.k8s.io");
    }

    #[test]
    fn csi_driver_spellings() {
        let both = CsiDriverSpec {
            hostpath: true,
            nfs: true,
        };
        for (driver, want) in [
            (
                "nfs",
                CsiDriverSpec {
                    nfs: true,
                    hostpath: false,
                },
            ),
            (
                "hostpath",
                CsiDriverSpec {
                    hostpath: true,
                    nfs: false,
                },
            ),
            ("both", both),
            ("true", both),
            ("none", CsiDriverSpec::default()),
            ("false", CsiDriverSpec::default()),
            ("", CsiDriverSpec::default()),
        ] {
            let got = csi_driver(driver).unwrap_or_else(|| panic!("no entry for {driver:?}"));
            assert_eq!(got, want, "{driver:?}");
            assert_eq!(
                got.installs_nothing(),
                got == CsiDriverSpec::default(),
                "{driver:?}"
            );
        }
    }

    #[test]
    fn the_nfs_driver_config_enables_tls_against_the_controller_service() {
        let svc: serde_json::Value = serde_yaml::from_str(CSI_NFS_CONTROLLER_SERVICE).unwrap();
        let cfg: serde_json::Value = serde_yaml::from_str(CSI_NFS_DRIVER_CONFIG).unwrap();
        let tls = &cfg["spec"]["tls"];
        assert_eq!(tls["enabled"], true);
        assert_eq!(tls["usePodIdentity"], true);
        let (name, ns) = (
            svc["metadata"]["name"].as_str().unwrap(),
            svc["metadata"]["namespace"].as_str().unwrap(),
        );
        assert_eq!(tls["serverName"], format!("{name}.{ns}.svc"));
        let port = svc["spec"]["ports"][0]["port"].as_i64().unwrap();
        assert_eq!(
            cfg["spec"]["controllerEndpoint"],
            format!("tcp://{name}.{ns}.svc.cluster.local:{port}")
        );
    }

    #[test]
    fn the_nfs_setup_keeps_its_security_decisions() {
        let envoy =
            csi_nfs_envoy_config_map(&installdefaults::api_server_spiffe_id("hivekeep-system"));
        assert!(envoy.contains("require_client_certificate: true"));
        assert!(
            envoy.contains("exact: \"spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api\"")
        );
        assert!(!CSI_NFS_CONTROLLER_PATCH.contains("privileged: true"));
        assert!(CSI_NFS_DRIVER_CONFIG.contains("serverName: csi-nfs-controller.kube-system.svc\n"));
        assert!(CSI_NFS_DRIVER_CONFIG.contains("usePodIdentity: true"));
        let body = function_body("async fn setup_csi_nfs(");
        let service = body.find("CSI_NFS_CONTROLLER_SERVICE").unwrap();
        let driver = body.find("csi-nfs-controller.yaml").unwrap();
        assert!(
            service < driver,
            "the NFS driver is deployed before its controller Service"
        );
    }

    #[test]
    #[ignore = "needs the third-party hostpath driver bundle under manifests/csi/third_party/csi-driver-host-path"]
    fn the_hostpath_proxy_is_not_privileged() {
        let path = test_config().manifests_path(&[
            CSI_HOSTPATH_DIR,
            "deploy",
            "csi-hostpath-testing.yaml",
        ]);
        let objs = kube::load_path(&path).unwrap();
        let mut found = false;
        for obj in objs.iter().filter(|o| o.kind() == "StatefulSet") {
            for c in obj["spec"]["template"]["spec"]["containers"]
                .as_array()
                .unwrap()
            {
                found = true;
                assert_ne!(
                    c.pointer("/securityContext/privileged"),
                    Some(&serde_json::Value::Bool(true)),
                    "{}",
                    describe(obj)
                );
            }
        }
        assert!(found, "no proxy StatefulSet");
    }

    #[tokio::test]
    async fn a_missing_driver_bundle_names_its_expected_path() {
        let dir = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            manifests: dir.path().to_path_buf(),
            ..Default::default()
        });
        let err = e.third_party_dir(CSI_NFS_DIR).unwrap_err().to_string();
        assert!(err.contains("csi/third_party/csi-driver-nfs"), "{err}");
    }

    #[test]
    fn the_nfs_server_manifest_ships() {
        let objs =
            kube::load_path(&test_config().manifests_path(&["csi", "nfs-server.yaml"])).unwrap();
        assert!(objs
            .iter()
            .any(|o| o.kind() == "Deployment" && o.name() == "nfs-server"));
    }
}
