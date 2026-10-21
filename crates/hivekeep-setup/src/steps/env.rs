//! The shared execution context of the install steps: the configuration,
//! the cluster client, and the lazily chosen image resolver.

use super::{Result, StepError};
use crate::builder::Builder;
use crate::config::Config;
use crate::images::{self, DockerfileImage, ImageResolver, Prebuilt, RemoteDigest};
use crate::kube::{self, Gvk};
use crate::{installdefaults, kustomize, log};
use k8s_openapi::api::core::v1::Namespace;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Wait budgets of the bootstrap steps.
pub const NAMESPACE_TIMEOUT: Duration = Duration::from_secs(60);
pub const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(120);
pub const TRUST_BUNDLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEMO_TIMEOUT: Duration = Duration::from_secs(300);

/// Namespace the checked-in manifests name literally.
pub const NAMESPACE_SYSTEM: &str = installdefaults::SYSTEM_NAMESPACE;
/// Namespace of the podcertificate controller.
pub const NAMESPACE_POD_CERT: &str = "podcertificate-controller-system";

/// Which image resolver an Env uses, chosen on first use.
#[derive(Clone)]
enum Resolver {
    Builder(Arc<Builder>),
    Prebuilt(Arc<Prebuilt>),
    Custom(Arc<dyn ImageResolver>),
}

impl Resolver {
    fn as_resolver(&self) -> Arc<dyn ImageResolver> {
        match self {
            Self::Builder(b) => b.clone(),
            Self::Prebuilt(p) => p.clone(),
            Self::Custom(c) => c.clone(),
        }
    }
}

/// The execution context, built once per invocation.
pub struct Env {
    pub cfg: Config,
    kube: Option<kube::Client>,
    resolver: Mutex<Option<Resolver>>,
    /// Dockerfile images already built or pinned this run, by name.
    dockerfile_images: Mutex<HashMap<String, String>>,
}

impl Env {
    /// An Env connected to the cluster the configuration selects.
    pub async fn connect(cfg: Config) -> Result<Self> {
        let client = kube::Client::connect(&cfg.kubeconfig, &cfg.context).await?;
        Ok(Self::with_client(cfg, client))
    }

    /// An Env for commands that never contact a cluster.
    pub fn local(cfg: Config) -> Self {
        Self {
            cfg,
            kube: None,
            resolver: Mutex::new(None),
            dockerfile_images: Mutex::default(),
        }
    }

    /// An Env over an existing client (a fake one in tests).
    pub fn with_client(cfg: Config, client: kube::Client) -> Self {
        Self {
            cfg,
            kube: Some(client),
            resolver: Mutex::new(None),
            dockerfile_images: Mutex::default(),
        }
    }

    /// Replaces the image resolver, for tests.
    pub fn with_resolver(self, resolver: Arc<dyn ImageResolver>) -> Self {
        *self.resolver.lock() = Some(Resolver::Custom(resolver));
        self
    }

    /// Uses the pre-built resolver with `digest` in place of registry
    /// lookups, for tests of pre-built installs.
    pub fn with_digester(self, digest: Arc<dyn images::Digester>) -> Self {
        let prebuilt = Prebuilt::new(self.cfg.images.clone(), digest);
        *self.resolver.lock() = Some(Resolver::Prebuilt(Arc::new(prebuilt)));
        self
    }

    /// The cluster client. Commands that run without a cluster never call it.
    pub fn kube(&self) -> Result<&kube::Client> {
        self.kube
            .as_ref()
            .ok_or_else(|| StepError::msg("this command runs without a cluster connection"))
    }

    pub fn has_cluster(&self) -> bool {
        self.kube.is_some()
    }

    /// The install namespace.
    pub fn namespace(&self) -> &str {
        if self.cfg.namespace.is_empty() {
            NAMESPACE_SYSTEM
        } else {
            &self.cfg.namespace
        }
    }

    /// Fails for a relocated install: the checked-in manifests name the
    /// default namespace literally.
    pub fn require_canonical_namespace(&self, step: &str) -> Result<()> {
        let ns = self.namespace();
        if ns != NAMESPACE_SYSTEM {
            return Err(StepError::msg(format!(
                "{step} cannot be used with HK_NAMESPACE={ns}: manifests/install/ names {NAMESPACE_SYSTEM} \
                 literally; install into another namespace with a deployment that renders them, and use \
                 hivekeep-setup only for the create steps"
            )));
        }
        Ok(())
    }

    fn resolver(&self) -> Result<Resolver> {
        let mut slot = self.resolver.lock();
        if let Some(r) = slot.as_ref() {
            return Ok(r.clone());
        }
        log::step(&format!("images: {}", self.cfg.images.describe()));
        let r = if self.cfg.images.is_prebuilt() {
            Resolver::Prebuilt(Arc::new(Prebuilt::new(
                self.cfg.images.clone(),
                Arc::new(RemoteDigest),
            )))
        } else {
            Resolver::Builder(Arc::new(
                Builder::new(&self.cfg.root, self.cfg.build_env())?
                    .with_extra_flags(self.cfg.docker_build_flags.clone()),
            ))
        };
        *slot = Some(r.clone());
        Ok(r)
    }

    /// The image resolver this install uses.
    pub fn image_resolver(&self) -> Result<Arc<dyn ImageResolver>> {
        Ok(self.resolver()?.as_resolver())
    }

    /// The source builder, for commands that publish images.
    pub fn builder(&self) -> Result<Arc<Builder>> {
        if self.cfg.images.is_prebuilt() {
            return Err(StepError::msg(
                "publishing images builds them from source, which --image-repo does not do; drop it to build \
                 and push from this checkout",
            ));
        }
        match self.resolver()? {
            Resolver::Builder(b) => Ok(b),
            _ => Err(StepError::msg(
                "the image resolver is not the source builder",
            )),
        }
    }

    /// The pinned reference of a Dockerfile image: built and pushed from
    /// source, or looked up in the pre-built repository. Cached per run.
    pub async fn dockerfile_image(&self, img: DockerfileImage) -> Result<String> {
        if let Some(r) = self.dockerfile_images.lock().get(img.name) {
            return Ok(r.clone());
        }
        let reference = if !self.cfg.images.is_prebuilt() {
            images::build_dockerfile_image(
                &self.cfg.root,
                &self.cfg.build_repo,
                img.name,
                &self.cfg.path(&[img.context]),
                &self.cfg.build_platforms,
                &self.cfg.docker_build_flags,
            )
            .await?
        } else {
            let prebuilt = match self.resolver()? {
                Resolver::Prebuilt(p) => p,
                _ => {
                    return Err(StepError::msg(
                        "the image resolver is not the pre-built resolver",
                    ))
                }
            };
            prebuilt.pin(img.name).await.map_err(|e| {
                StepError::msg(format!(
                    "{} is built from a Dockerfile, and the image repository has no readable copy; build it from \
                     source by dropping --image-repo: {e}",
                    img.name
                ))
            })?
        };
        self.dockerfile_images
            .lock()
            .insert(img.name.to_string(), reference.clone());
        Ok(reference)
    }

    /// Reads a manifest path, resolves its images, and applies it.
    pub async fn resolve_and_apply(&self, path: &Path) -> Result<()> {
        let manifest = self.resolve_manifest(path).await?;
        Ok(self.kube()?.apply_bytes(&manifest).await?)
    }

    /// Reads a manifest path and resolves its images.
    pub async fn resolve_manifest(&self, path: &Path) -> Result<Vec<u8>> {
        let manifest = kube::read_path(path)?;
        self.resolve_manifest_bytes(&manifest)
            .await
            .map_err(|e| e.context(format!("in {}", path.display())))
    }

    /// Adds the preview gates to a manifest stream and resolves its images.
    pub async fn resolve_manifest_bytes(&self, manifest: &[u8]) -> Result<Vec<u8>> {
        let resolver = self.image_resolver()?;
        let manifest = super::preview::inject_preview_arg(manifest, &self.cfg.preview_gates)?;
        Ok(resolver.resolve_bytes(&manifest).await?)
    }

    pub async fn resolve_and_apply_bytes(&self, manifest: &[u8]) -> Result<()> {
        let resolved = self.resolve_manifest_bytes(manifest).await?;
        Ok(self.kube()?.apply_bytes(&resolved).await?)
    }

    /// Renders a kustomization directory given relative to the manifests
    /// directory.
    pub fn kustomize(&self, overlay: &str) -> Result<Vec<u8>> {
        Ok(kustomize::build(&self.cfg.manifests_path(&[overlay]))?)
    }

    /// Creates the install namespace, waiting out a previous incarnation
    /// that is still terminating.
    pub async fn ensure_system_namespace(&self) -> Result<()> {
        let ns = self.namespace().to_string();
        let k = self.kube()?;
        if let Some(existing) = k.get_typed::<Namespace>("", &ns).await? {
            if existing.status.and_then(|s| s.phase).as_deref() == Some("Terminating") {
                k.wait_deleted(
                    &Gvk::namespace(),
                    "",
                    &ns,
                    self.cfg.wait_timeout(BOOTSTRAP_TIMEOUT),
                )
                .await?;
            }
        }
        if ns == NAMESPACE_SYSTEM {
            k.apply_path(&self.cfg.manifest(&["hivekeep-system-namespace.yaml"]))
                .await?;
        } else {
            k.ensure_namespace(&ns).await?;
        }
        Ok(k.wait_namespace_active(&ns, NAMESPACE_TIMEOUT).await?)
    }

    /// Fails unless this is a Kind install.
    pub fn require_kind(&self, what: &str) -> Result<()> {
        if !self.cfg.kind {
            return Err(StepError::msg(format!(
                "{what} is only supported for Kind installations; re-run with --kind"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
// Tests build a default Config and adjust the one field under test.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn env_namespace() {
        let cases = [
            (
                "falls back to the canonical namespace when unset",
                "",
                NAMESPACE_SYSTEM,
            ),
            (
                "uses the configured namespace",
                "hivekeep-dev",
                "hivekeep-dev",
            ),
        ];
        for (name, ns, want) in cases {
            let mut cfg = Config::default();
            cfg.namespace = ns.into();
            let e = Env::local(cfg);
            assert_eq!(e.namespace(), want, "{name}");
        }
    }

    #[test]
    fn require_canonical_namespace() {
        let mut cfg = Config::default();
        cfg.namespace = NAMESPACE_SYSTEM.into();
        let e = Env::local(cfg);
        e.require_canonical_namespace("deploy hivekeep-system")
            .unwrap();

        let mut cfg = Config::default();
        cfg.namespace = "hivekeep-dev".into();
        let e = Env::local(cfg);
        let err = e
            .require_canonical_namespace("deploy hivekeep-system")
            .unwrap_err()
            .to_string();
        for want in ["deploy hivekeep-system", "hivekeep-dev", NAMESPACE_SYSTEM] {
            assert!(err.contains(want), "{err:?} does not mention {want:?}");
        }
    }
}
