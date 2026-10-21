//! Where the installer gets container images from.
//!
//! The manifests name every Hivekeep image as
//! `REGISTRY_PLACEHOLDER/hivekeep-<component>:<version>`. An install either
//! builds those images from this checkout and pushes them (the source
//! builder), or maps the references onto images already published under one
//! repository and tag ([`Source`] with a repo). Either way every reference is
//! rewritten to a digest-pinned one before it is applied.

use async_trait::async_trait;

pub mod digest;
pub mod dockerfile;
pub mod platforms;
pub mod prebuilt;
pub mod reference;
pub mod registry;
pub mod release;
#[cfg(test)]
pub(crate) mod testregistry;

pub use digest::{remote_digest, Digester, RemoteDigest};
pub use dockerfile::{build_dockerfile_image, publish_dockerfile_image};
pub use platforms::build_platforms;
pub use prebuilt::Prebuilt;
pub use reference::{component_of, placeholder_pattern, ImageRef, Platform};
pub use release::Incomplete;

/// Registry host and path every manifest image reference starts with.
pub const REGISTRY_PLACEHOLDER: &str = "REGISTRY_PLACEHOLDER";

/// Tag the checked-in manifests carry: the workspace version.
pub const MANIFEST_TAG: &str = env!("CARGO_PKG_VERSION");

/// Every component the installer can deploy an image for, by image name.
///
/// The list is closed on purpose: a reference that is not in it names
/// something no build publishes, and failing beats guessing.
pub const COMPONENTS: &[&str] = &[
    "hivekeep-api",
    "hivekeep-controller",
    "hivekeep-credential-provider-k8s",
    "hivekeep-demo-counter",
    "hivekeep-demo-egress",
    "hivekeep-demo-fspersist",
    "hivekeep-demo-sandbox",
    "hivekeep-fake-keeper",
    "hivekeep-fake-workersync",
    "hivekeep-gate",
    "hivekeep-idp-server",
    "hivekeep-keeper",
    "hivekeep-objectstore-plugin",
    "hivekeep-podcert-controller",
    "hivekeep-sandbox-gvisor",
    "hivekeep-sandbox-microvm",
];

/// The components a release publishes. The demos and the benchmarking fakes
/// stay out, so a pre-built install that needs one fails naming it.
const RELEASED: &[&str] = &[
    "hivekeep-api",
    "hivekeep-controller",
    "hivekeep-credential-provider-k8s",
    "hivekeep-gate",
    "hivekeep-idp-server",
    "hivekeep-keeper",
    "hivekeep-objectstore-plugin",
    "hivekeep-podcert-controller",
    "hivekeep-sandbox-gvisor",
    "hivekeep-sandbox-microvm",
];

/// The components a release publishes.
pub fn release_components() -> Vec<&'static str> {
    RELEASED.to_vec()
}

/// Whether a release publishes `component`.
pub fn is_released(component: &str) -> bool {
    RELEASED.contains(&component)
}

/// The reference the checked-in manifests name a component by.
pub fn manifest_reference(component: &str) -> String {
    format!("{REGISTRY_PLACEHOLDER}/{component}:{MANIFEST_TAG}")
}

/// An image built from its own Dockerfile rather than the shared component
/// build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DockerfileImage {
    /// Image name under the repository.
    pub name: &'static str,
    /// Build context, relative to the repository root.
    pub context: &'static str,
}

/// The Envoy image the envoy gate router and egress gateway run.
pub const ENVOY_DATAPLANE: DockerfileImage = DockerfileImage {
    name: "hivekeep-envoy-dataplane",
    context: "images/envoy-dataplane",
};

/// The Dockerfile images a release publishes.
pub fn release_dockerfile_images() -> Vec<DockerfileImage> {
    vec![ENVOY_DATAPLANE]
}

/// Where images come from. The default builds them from source; a repo
/// switches to images already published there under `tag`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Source {
    /// Registry path holding the images, without a trailing slash.
    pub repo: String,
    /// Tag every image carries.
    pub tag: String,
}

impl Source {
    pub fn is_prebuilt(&self) -> bool {
        !self.repo.is_empty()
    }

    /// The source for the install log.
    pub fn describe(&self) -> String {
        if self.is_prebuilt() {
            format!("pre-built from {}, tag {}", self.repo, self.tag)
        } else {
            "built from source".into()
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ImageError {
    #[error("{0}")]
    Unknown(String),
    #[error("{0}")]
    Registry(String),
    #[error("{0}")]
    Build(String),
    #[error("{0}")]
    Other(String),
}

/// Rewrites every image reference in a manifest stream to a pinned one.
#[async_trait]
pub trait ImageResolver: Send + Sync {
    async fn resolve_bytes(&self, manifest: &[u8]) -> Result<Vec<u8>, ImageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn released_components_are_components() {
        for c in release_components() {
            assert!(COMPONENTS.contains(&c), "{c}");
        }
    }

    #[test]
    fn source_describe() {
        assert_eq!(Source::default().describe(), "built from source");
        let s = Source {
            repo: "registry.example.com/hivekeep".into(),
            tag: "v1".into(),
        };
        assert!(s.is_prebuilt());
        assert_eq!(
            s.describe(),
            "pre-built from registry.example.com/hivekeep, tag v1"
        );
    }

    /// COMPONENTS is closed, so a component added to a manifest but not to
    /// the list would install from source and fail only on a release install.
    /// The same comparison holds the spelling: a reference not in the
    /// canonical `REGISTRY_PLACEHOLDER/<component>:<tag>` form lands here
    /// whole, as an entry the list cannot contain.
    #[test]
    fn components_cover_the_manifests() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("manifests");
        let mut found = BTreeSet::new();
        for entry in walkdir::WalkDir::new(&root) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy();
            let base = name.strip_suffix(".tmpl").unwrap_or(&name);
            if !(base.ends_with(".yaml") || base.ends_with(".yml") || base.ends_with(".json")) {
                continue;
            }
            let body = std::fs::read_to_string(entry.path()).unwrap();
            for m in placeholder_pattern().find_iter(&body) {
                found.insert(
                    component_of(m.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| m.as_str().to_string()),
                );
            }
        }
        // Every reference found must be a known component. Components whose
        // manifests live outside this crate (demos, sandbox workers named
        // through WorkerPools) need not appear.
        let known: BTreeSet<String> = COMPONENTS.iter().map(|c| c.to_string()).collect();
        let stray: Vec<_> = found.difference(&known).collect();
        assert!(stray.is_empty(), "references not in COMPONENTS: {stray:?}");
        assert!(
            !found.is_empty(),
            "no references found under {}",
            root.display()
        );
    }

    /// An image is addressed by its name alone, so two components sharing
    /// one would install each other's binary.
    #[test]
    fn image_names_are_unique() {
        let set: BTreeSet<_> = COMPONENTS.iter().collect();
        assert_eq!(set.len(), COMPONENTS.len());
    }

    /// Image names are flat: a nested path would not be addressable under
    /// `--image-repo` as `<repo>/<name>`.
    #[test]
    fn image_names_are_flat() {
        for c in COMPONENTS
            .iter()
            .chain(std::iter::once(&ENVOY_DATAPLANE.name))
        {
            assert!(!c.contains('/') && c.starts_with("hivekeep-"), "{c}");
        }
        assert_eq!(
            manifest_reference("hivekeep-demo-fspersist"),
            format!("REGISTRY_PLACEHOLDER/hivekeep-demo-fspersist:{MANIFEST_TAG}")
        );
    }

    #[tokio::test]
    async fn remote_digest_reads_the_registry() {
        let reg = testregistry::TestRegistry::start().await;
        reg.push_index(
            "hivekeep/hivekeep-api",
            "v1",
            &[Platform::parse("linux/amd64").unwrap()],
        );
        let d = remote_digest(&format!("{}/hivekeep/hivekeep-api:v1", reg.host()))
            .await
            .unwrap();
        assert!(d.starts_with("sha256:") && d.len() == 71, "{d}");
        let err = remote_digest(&format!("{}/hivekeep/absent:v1", reg.host()))
            .await
            .unwrap_err();
        // A HEAD answer has no body, so only the status says why.
        assert!(err.to_string().contains("404"), "{err}");
    }
}
