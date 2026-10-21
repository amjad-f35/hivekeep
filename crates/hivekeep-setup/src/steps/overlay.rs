//! Rendering the control plane manifests: the overlay for the install shape,
//! the kustomize components layered over every apply, and the splices the
//! gate router and egress gateway need.

use super::{Env, Result, StepError};
use crate::config::{Config, CredentialProvider};
use crate::images::ENVOY_DATAPLANE;
use crate::{kustomize, log};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Manifest root of the install, relative to the manifests directory.
pub const INSTALL_DIR: &str = "install";

/// Pins each system infra workload to its own node pool, under
/// `--cordon-system-infra-components`.
pub const CORDON_SYSTEM_INFRA_COMPONENT: &str = "install/components/cordon-system-infra-components";
/// Tunes the control and data plane for end-to-end test runs, under
/// `--e2e-tuning`.
pub const E2E_COMPONENT: &str = "install/components/e2e";
/// Puts fake-keeper in the keeper DaemonSet and fake-workersync in place of
/// hivekeep-controller, under `--fake-data-plane`.
pub const FAKE_DATA_PLANE_COMPONENT: &str = "install/components/fake-data-plane";

/// The gate router and egress gateway Deployments.
pub const GATE_ROUTER: &str = "gate-router";
pub const GATE_EGRESS: &str = "gate-egress";

/// Placeholder the envoy dataplane image is substituted for.
pub const ENVOY_DATAPLANE_PLACEHOLDER: &str = "${ENVOY_DATAPLANE_IMAGE}";

/// Marker in the egress sidecar's args the provider flags replace.
pub const EGRESS_INJECT_FLAGS_MARKER: &str = "#HK_EGRESS_INJECT_FLAGS";
/// Markers the additional ext_proc filter and cluster replace.
pub const MITM_EXTPROC_FILTER_MARKER: &str = "#HK_MITM_EXTPROC_FILTER";
pub const MITM_EXTPROC_CLUSTER_MARKER: &str = "#HK_MITM_EXTPROC_CLUSTER";

/// Name of the cluster the additional ext_proc filter dials.
pub const ADDITIONAL_EGRESS_EXTPROC_CLUSTER: &str = "additional_egress_ext_proc";

/// The ConfigMap every control plane component reads its telemetry settings
/// from, and the collector address in it.
pub const OTEL_CONFIG_MAP: &str = "hivekeep-otel-config";
pub const OTEL_ENDPOINT_KEY: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The Deployments that read the telemetry ConfigMap. hivekeep-controller
/// copies its values onto the worker pods it creates, so one patch reaches
/// the whole system.
pub const OTEL_OVERRIDE_DEPLOYMENTS: [&str; 3] =
    ["hivekeep-api", "hivekeep-controller", GATE_ROUTER];

/// The kustomization for a full control plane install, relative to the
/// manifests directory.
///
/// The plain GKE install renders the base kustomization rather than the raw
/// `install/` directory: the directory would also re-apply the podcertificate
/// controller (reverting its size10 flags and WORKERS_PER_SIGNER), the egress
/// gateway, and the SandboxConfig files, which have their own apply steps.
pub fn system_overlay(cfg: &Config) -> &'static str {
    if cfg.kind {
        "install/kind"
    } else {
        "install/base"
    }
}

/// Replaces the one line starting with `marker` by `block`, indented as the
/// marker was; an empty block removes the line. Exactly one marker must be
/// present.
pub fn replace_manifest_marker(raw: &[u8], marker: &str, block: &str) -> Result<Vec<u8>> {
    let text = String::from_utf8_lossy(raw);
    let mut out: Vec<String> = Vec::new();
    let mut replaced = 0;
    for line in text.split('\n') {
        if line.trim().starts_with(marker) {
            let indent = &line[..line.len() - line.trim_start_matches([' ', '\t']).len()];
            if !block.is_empty() {
                out.extend(block.split('\n').map(|l| format!("{indent}{l}")));
            }
            replaced += 1;
            continue;
        }
        out.push(line.to_string());
    }
    if replaced != 1 {
        return Err(StepError::msg(format!(
            "expected 1 {marker} marker, found {replaced}"
        )));
    }
    Ok(out.join("\n").into_bytes())
}

/// The credential-provider flags of the envoy egress sidecar.
pub fn emit_egress_inject_flags(name: &str, address: &str, server_name: &str) -> String {
    format!(
        "- --credential-provider-name={name}
- --credential-provider-address={address}
- --credential-provider-ca-file=/run/servicedns.podcert.hivekeep.io/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.hivekeep.io/credential-bundle.pem
- --credential-provider-server-name={server_name}"
    )
}

/// The additional ext_proc HTTP filter, spliced below the policy ext_proc.
pub fn emit_additional_egress_extproc_filter() -> String {
    format!(
        r#"- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: {ADDITIONAL_EGRESS_EXTPROC_CLUSTER}
      timeout: 2s
    failure_mode_allow: false
    message_timeout: 2s
    request_attributes:
    - filter_state['io.hivekeep.actor.identity']
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
      request_trailer_mode: SKIP
      response_trailer_mode: SKIP
    mutation_rules:
      disallow_system: true
      disallow_is_error: true"#
    )
}

/// The cluster the additional ext_proc filter dials. Its TLS material comes
/// through SDS, so kubelet's certificate rotation reaches it.
pub fn emit_additional_egress_extproc_cluster(
    address: &str,
    port: &str,
    server_name: &str,
) -> String {
    let c = ADDITIONAL_EGRESS_EXTPROC_CLUSTER;
    format!(
        r#"- name: {c}
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  connect_timeout: 1s
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {{}}
  transport_socket:
    name: envoy.transport_sockets.tls
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
      sni: {server_name}
      common_tls_context:
        tls_params:
          tls_minimum_protocol_version: TLSv1_3
          tls_maximum_protocol_version: TLSv1_3
        tls_certificate_sds_secret_configs:
        - name: podidentity_client_cert
          sds_config:
            resource_api_version: V3
            path_config_source:
              path: /etc/envoy/sds-podidentity-cert.yaml
        combined_validation_context:
          default_validation_context:
            match_typed_subject_alt_names:
            - san_type: DNS
              matcher:
                exact: {server_name}
          validation_context_sds_secret_config:
            name: servicedns_validation_context
            sds_config:
              resource_api_version: V3
              path_config_source:
                path: /etc/envoy/sds-servicedns-validation.yaml
  load_assignment:
    cluster_name: {c}
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: {address}
              port_value: {port}"#
    )
}

/// Appends args to the first container of a Deployment in the install
/// namespace, through a kustomize JSON6902 patch that first tests that the
/// first container is the one named, so a reordered manifest fails the
/// render instead of patching the wrong container.
pub fn append_container_args(
    raw: &[u8],
    deployment: &str,
    container: &str,
    args: &[String],
) -> Result<Vec<u8>> {
    let dir = tempfile::Builder::new()
        .prefix("hivekeep-setup-kustomize-")
        .tempdir()
        .map_err(|e| StepError::msg(format!("while creating a scratch kustomization: {e}")))?;
    std::fs::write(dir.path().join("manifest.yaml"), raw)
        .map_err(|e| StepError::msg(format!("while staging a manifest for kustomize: {e}")))?;
    let mut k = format!(
        "apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - manifest.yaml
patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: {deployment}
      namespace: {ns}
    patch: |-
      - op: test
        path: /spec/template/spec/containers/0/name
        value: {container:?}
",
        ns = crate::installdefaults::SYSTEM_NAMESPACE
    );
    for arg in args {
        k.push_str(&format!(
            "      - op: add\n        path: /spec/template/spec/containers/0/args/-\n        value: {arg:?}\n"
        ));
    }
    std::fs::write(dir.path().join("kustomization.yaml"), k)
        .map_err(|e| StepError::msg(format!("while writing a scratch kustomization: {e}")))?;
    Ok(kustomize::build(dir.path())?)
}

/// Appends `--concurrency <n>` to the envoy container of the egress gateway.
pub fn patch_gate_egress_envoy_concurrency(raw: &[u8], concurrency: i64) -> Result<Vec<u8>> {
    append_container_args(
        raw,
        GATE_EGRESS,
        "envoy",
        &["--concurrency".into(), concurrency.to_string()],
    )
}

/// Substitutes the envoy dataplane image reference.
pub fn patch_envoy_dataplane_image(raw: &[u8], image_ref: &str) -> Vec<u8> {
    String::from_utf8_lossy(raw)
        .replace(ENVOY_DATAPLANE_PLACEHOLDER, image_ref)
        .into_bytes()
}

/// Replaces the egress sidecar's provider marker with the provider flags, or
/// removes it when injection is off.
pub fn patch_gate_egress_inject(raw: &[u8], provider: &CredentialProvider) -> Result<Vec<u8>> {
    let block = if provider.enabled() {
        emit_egress_inject_flags(&provider.name, &provider.address, provider.server_name())
    } else {
        String::new()
    };
    replace_manifest_marker(raw, EGRESS_INJECT_FLAGS_MARKER, &block)
}

impl Env {
    /// The kustomize components every control plane apply is composed with.
    /// Each targets its workloads by name and kustomize leaves a stream with
    /// none of them alone, so composing any manifest with them is harmless.
    pub(crate) fn components(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        // e2e before fake-data-plane: e2e appends to keeper's args and the
        // fake data plane removes them.
        if self.cfg.e2e_tuning {
            out.push(self.cfg.manifests_path(&[E2E_COMPONENT]));
        }
        if self.cfg.fake_data_plane {
            out.push(self.cfg.manifests_path(&[FAKE_DATA_PLANE_COMPONENT]));
        }
        if self.cfg.cordon_system_infra_components {
            out.push(self.cfg.manifests_path(&[CORDON_SYSTEM_INFRA_COMPONENT]));
        }
        out
    }

    /// The system overlay directory for this install.
    pub(crate) fn system_overlay_path(&self) -> PathBuf {
        self.cfg.manifests_path(&[system_overlay(&self.cfg)])
    }

    /// A manifest file or kustomization directory, composed with the selected
    /// components, before image resolution.
    pub(crate) fn render(&self, path: &Path) -> Result<Vec<u8>> {
        let components = self.components();
        if !components.is_empty() {
            return Ok(kustomize::compose(path, &components)?);
        }
        let meta = std::fs::metadata(path)
            .map_err(|e| StepError::msg(format!("while reading {}: {e}", path.display())))?;
        if meta.is_dir() {
            return Ok(kustomize::build(path)?);
        }
        std::fs::read(path)
            .map_err(|e| StepError::msg(format!("while reading {}: {e}", path.display())))
    }

    /// [`Self::render`] for a manifest already in memory.
    pub(crate) fn render_bytes(&self, manifest: &[u8]) -> Result<Vec<u8>> {
        let components = self.components();
        if components.is_empty() {
            return Ok(manifest.to_vec());
        }
        Ok(kustomize::compose_bytes(manifest, &components)?)
    }

    /// Renders and resolves images.
    pub(crate) async fn render_resolve(&self, path: &Path) -> Result<Vec<u8>> {
        let manifest = self.render(path)?;
        self.resolve_manifest_bytes(&manifest).await
    }

    /// Renders, resolves images, and applies.
    pub(crate) async fn render_resolve_apply(&self, path: &Path) -> Result<()> {
        let manifest = self.render_resolve(path).await?;
        Ok(self.kube()?.apply_bytes(&manifest).await?)
    }

    /// The full control plane manifest, images resolved.
    pub(crate) async fn render_system_manifests(&self) -> Result<Vec<u8>> {
        let mut manifest = self.render(&self.system_overlay_path())?;
        manifest = super::idpserver::set_idp_server_issuer(&manifest, &self.cfg.actor_jwt_issuer)?;
        manifest = self.patch_gate_router_allowed_spiffe_ids(&manifest)?;
        let image = self.dockerfile_image(ENVOY_DATAPLANE).await?;
        manifest = patch_envoy_dataplane_image(&manifest, &image);
        self.resolve_manifest_bytes(&manifest).await
    }

    /// The gate router manifest, images resolved.
    pub(crate) async fn render_gate_router_manifest(&self) -> Result<Vec<u8>> {
        let manifest = self.render(&self.cfg.manifest(&["gate-router.yaml"]))?;
        let manifest = self.patch_gate_router_allowed_spiffe_ids(&manifest)?;
        let image = self.dockerfile_image(ENVOY_DATAPLANE).await?;
        let manifest = patch_envoy_dataplane_image(&manifest, &image);
        self.resolve_manifest_bytes(&manifest).await
    }

    /// The envoy egress gateway manifest.
    pub(crate) fn gate_egress_manifest_path(&self) -> PathBuf {
        self.cfg.manifest(&["gate-egress.yaml"])
    }

    /// The egress gateway manifest, images resolved.
    pub(crate) async fn render_gate_egress_manifest(
        &self,
        provider: &CredentialProvider,
    ) -> Result<Vec<u8>> {
        let general = !self.cfg.additional_egress_extproc_service.is_empty();
        let image = self.dockerfile_image(ENVOY_DATAPLANE).await?;
        // The ext_proc filter and credential injection are independent
        // splices with their own markers; the component render comes last,
        // over the spliced stream, so node pinning reaches every variant.
        let mut raw = if general {
            self.patch_gate_egress_manifest()?
        } else {
            std::fs::read(self.gate_egress_manifest_path())
                .map_err(|e| StepError::msg(format!("reading egress manifest: {e}")))?
        };
        raw = patch_gate_egress_inject(&raw, provider)?;
        raw = patch_envoy_dataplane_image(&raw, &image);
        if self.cfg.envoy_concurrency > 0 {
            raw = patch_gate_egress_envoy_concurrency(&raw, self.cfg.envoy_concurrency)?;
        }
        let rendered = self.render_bytes(&raw)?;
        self.resolve_manifest_bytes(&rendered).await
    }

    /// Passes the install's allowlist to the envoy gate router, whose base
    /// manifest carries none (without one the router serves no traffic).
    pub(crate) fn patch_gate_router_allowed_spiffe_ids(&self, raw: &[u8]) -> Result<Vec<u8>> {
        if self.cfg.ingress_allowed_spiffe_ids.is_empty() {
            return Ok(raw.to_vec());
        }
        append_container_args(
            raw,
            GATE_ROUTER,
            GATE_ROUTER,
            &[format!(
                "--ingress-allowed-spiffe-ids={}",
                self.cfg.ingress_allowed_spiffe_ids.join(",")
            )],
        )
    }

    /// Splices the additional ext_proc filter (into both HTTP chains) and its
    /// cluster into the envoy egress manifest.
    pub(crate) fn patch_gate_egress_manifest(&self) -> Result<Vec<u8>> {
        let path = self.gate_egress_manifest_path();
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| StepError::msg(format!("reading egress manifest: {e}")))?;
        let spec = &self.cfg.additional_egress_extproc_service;
        let (namespace, svc_port) = spec.split_once('/').ok_or_else(|| {
            StepError::msg(format!("invalid additional ext_proc service {spec:?}"))
        })?;
        let (service, port) = svc_port.split_once(':').ok_or_else(|| {
            StepError::msg(format!("invalid additional ext_proc service {spec:?}"))
        })?;
        let address = format!("{service}.{namespace}.svc.cluster.local");
        let server_name = format!("{service}.{namespace}.svc");
        let filter = emit_additional_egress_extproc_filter();
        let cluster = emit_additional_egress_extproc_cluster(&address, port, &server_name);

        let mut out: Vec<String> = Vec::new();
        let (mut filters, mut clusters) = (0, 0);
        for line in raw.split('\n') {
            let trimmed = line.trim();
            let block = if trimmed.starts_with(MITM_EXTPROC_FILTER_MARKER) {
                filters += 1;
                &filter
            } else if trimmed.starts_with(MITM_EXTPROC_CLUSTER_MARKER) {
                clusters += 1;
                &cluster
            } else {
                out.push(line.to_string());
                continue;
            };
            let indent = &line[..line.len() - line.trim_start_matches([' ', '\t']).len()];
            for l in block.split('\n') {
                out.push(if l.is_empty() {
                    String::new()
                } else {
                    format!("{indent}{l}")
                });
            }
        }
        if filters != 2 || clusters != 1 {
            return Err(StepError::msg(format!(
                "expected 2 filter markers and 1 cluster marker in {}, found {filters} and {clusters}",
                path.display()
            )));
        }
        Ok(out.join("\n").into_bytes())
    }

    /// Applies the egress gateway, restarting a running one whose config
    /// changed in a way a rollout does not pick up.
    pub(crate) async fn apply_gate_egress(&self, provider: &CredentialProvider) -> Result<()> {
        let manifest = self.render_gate_egress_manifest(provider).await?;
        let k = self.kube()?;
        let running = k.deployment_exists(self.namespace(), GATE_EGRESS).await?;
        k.apply_bytes(&manifest).await?;
        if running && (!self.cfg.additional_egress_extproc_service.is_empty() || provider.enabled())
        {
            k.rollout_restart_deployment(self.namespace(), GATE_EGRESS, chrono::Utc::now())
                .await?;
        }
        Ok(())
    }

    /// The environment's telemetry ConfigMap. Single-component redeploys
    /// apply raw manifests, so they must pick the right copy themselves: the
    /// base file on kind would point every component at the GKE endpoint.
    pub(crate) fn otel_config_path(&self) -> PathBuf {
        if self.cfg.kind {
            self.cfg.manifest(&["kind", "hivekeep-otel-config.yaml"])
        } else {
            self.cfg.manifest(&["hivekeep-otel-config.yaml"])
        }
    }

    pub(crate) async fn apply_otel_config(&self) -> Result<()> {
        Ok(self.kube()?.apply_path(&self.otel_config_path()).await?)
    }

    /// Points all control plane telemetry at another collector. Call after
    /// every apply, since the system bundle carries its own copy of the
    /// ConfigMap. Consumers are restarted only on an actual change (a restart
    /// during the bundle's rollout would race the rollout wait); an absent
    /// workload is not an error.
    pub(crate) async fn apply_otel_endpoint_override(&self) -> Result<()> {
        let endpoint = &self.cfg.otlp_endpoint;
        if endpoint.is_empty() {
            return Ok(());
        }
        let k = self.kube()?;
        let ns = self.namespace();
        if let Some(cm) = k.get_config_map(ns, OTEL_CONFIG_MAP).await? {
            if cm.data.as_ref().and_then(|d| d.get(OTEL_ENDPOINT_KEY)) == Some(endpoint) {
                return Ok(());
            }
        }
        log::info(&format!("Overriding {OTEL_ENDPOINT_KEY} with {endpoint}"));
        let data: BTreeMap<String, String> =
            [(OTEL_ENDPOINT_KEY.to_string(), endpoint.clone())].into();
        k.merge_patch_config_map(ns, OTEL_CONFIG_MAP, &data).await?;
        let now = chrono::Utc::now();
        for name in OTEL_OVERRIDE_DEPLOYMENTS {
            k.rollout_restart_deployment(ns, name, now).await?;
        }
        Ok(k.rollout_restart(ns, "keeper", now).await?)
    }
}
