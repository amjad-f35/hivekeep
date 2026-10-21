//! Mapping the manifests' image references onto images already published
//! under one repository and tag, pinned to their digests.
//!
//! No builds, no registry writes, and no checkout beyond the manifests
//! themselves; only read access to the registry, to pin each tag.

use super::digest::Digester;
use super::reference::{component_of, placeholder_pattern};
use super::{is_released, ImageError, ImageResolver, Source, COMPONENTS, REGISTRY_PLACEHOLDER};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// The pre-built image resolver.
pub struct Prebuilt {
    src: Source,
    digest: Arc<dyn Digester>,
    /// Pinned references by their tagged form, so a component several
    /// manifests name costs one registry lookup per install.
    pinned: Mutex<HashMap<String, String>>,
}

impl Prebuilt {
    /// `digest` turns a tag into the image it names; outside tests, pass
    /// [`super::RemoteDigest`].
    pub fn new(src: Source, digest: Arc<dyn Digester>) -> Self {
        Self {
            src,
            digest,
            pinned: Mutex::default(),
        }
    }

    /// The reference to install an image by: its tag and the digest that
    /// tag currently names.
    ///
    /// Also serves images the manifests do not name by placeholder, such as
    /// the Dockerfile-built Envoy dataplane, which a release publishes under
    /// the same repository and tag. The digest is load-bearing: ActorTemplate
    /// images and image volumes are admitted only with one, and pinning keeps
    /// an install from shifting under a tag someone moves later.
    pub async fn pin(&self, image: &str) -> Result<String, ImageError> {
        let tagged = format!("{}/{image}:{}", self.src.repo, self.src.tag);
        // A tag that already carries a digest is pinned as written.
        if self.src.tag.contains('@') {
            return Ok(tagged);
        }
        if let Some(r) = self.pinned.lock().get(&tagged) {
            return Ok(r.clone());
        }
        let digest = self.digest.digest(&tagged).await?;
        let reference = format!("{tagged}@{digest}");
        self.pinned.lock().insert(tagged, reference.clone());
        Ok(reference)
    }

    /// Replaces every placeholder reference with the published image it maps
    /// to, collecting every failure rather than stopping at the first: they
    /// are usually related (an unreachable registry, a tag never pushed).
    ///
    /// Substitution is textual: references appear in CRD fields as well as
    /// pod specs, and rewriting bytes leaves everything else exactly as
    /// committed.
    async fn rewrite(&self, manifest: &[u8]) -> Result<Vec<u8>, ImageError> {
        let text = String::from_utf8_lossy(manifest);
        let mut unknown = BTreeSet::new();
        let mut unpinned: BTreeMap<String, String> = BTreeMap::new();
        let mut replacements: HashMap<String, String> = HashMap::new();

        for m in placeholder_pattern().find_iter(&text) {
            let reference = m.as_str();
            if replacements.contains_key(reference) {
                continue;
            }
            let Some(image) = component_of(reference) else {
                unknown.insert(reference.to_string());
                continue;
            };
            match self.pin(image).await {
                Ok(pinned) => {
                    replacements.insert(reference.to_string(), pinned);
                }
                Err(e) => {
                    let mut msg = e.to_string();
                    // An unreleased image may still be in the registry, so it
                    // is looked up like any other; only a failed lookup says
                    // why it might be missing.
                    if !is_released(image) {
                        msg = format!(
                            "{msg}; releases do not publish {image}, so publish it to {} yourself or build it from \
                             source by dropping --image-repo",
                            self.src.repo
                        );
                    }
                    unpinned.insert(image.to_string(), msg);
                }
            }
        }

        let mut errs: Vec<String> = Vec::new();
        if !unknown.is_empty() {
            for r in &unknown {
                errs.push(format!("{r} has no published image"));
            }
            errs.push(format!(
                "installable images under {REGISTRY_PLACEHOLDER} are {}; a reference that is templated has to be \
                 rendered before it can be resolved",
                COMPONENTS.join(", ")
            ));
        }
        errs.extend(unpinned.into_values());
        if !errs.is_empty() {
            return Err(ImageError::Unknown(errs.join("\n")));
        }
        let out = placeholder_pattern().replace_all(&text, |c: &regex::Captures| {
            replacements
                .get(&c[0])
                .cloned()
                .unwrap_or_else(|| c[0].to_string())
        });
        Ok(out.into_owned().into_bytes())
    }
}

#[async_trait]
impl ImageResolver for Prebuilt {
    async fn resolve_bytes(&self, manifest: &[u8]) -> Result<Vec<u8>, ImageError> {
        self.rewrite(manifest).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube;

    const TEST_DIGEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";

    fn test_source() -> Source {
        Source {
            repo: "example.com/hivekeep".into(),
            tag: "v1.2.3".into(),
        }
    }

    fn suffix() -> String {
        format!("@{TEST_DIGEST}")
    }

    /// Stands in for the registry: answers every lookup with TEST_DIGEST, or
    /// with `err`, and records what was asked for.
    #[derive(Default)]
    struct StubRegistry {
        calls: Mutex<BTreeMap<String, usize>>,
        err: Option<String>,
    }

    #[async_trait]
    impl Digester for StubRegistry {
        async fn digest(&self, reference: &str) -> Result<String, ImageError> {
            *self.calls.lock().entry(reference.to_string()).or_default() += 1;
            match &self.err {
                Some(e) => Err(ImageError::Registry(format!(
                    "resolving {reference} to a digest: {e}"
                ))),
                None => Ok(TEST_DIGEST.into()),
            }
        }
    }

    fn resolver(src: Source) -> Prebuilt {
        Prebuilt::new(src, Arc::new(StubRegistry::default()))
    }

    const P: &str = "REGISTRY_PLACEHOLDER";

    #[tokio::test]
    async fn resolve_bytes_cases() {
        let s = suffix();
        let pinned_src = Source {
            repo: "example.com/hivekeep".into(),
            tag: format!("v1.2.3{s}"),
        };
        // (name, source, input, want or error)
        type Case<'a> = (&'a str, Option<Source>, String, Result<String, &'a str>);
        let cases: Vec<Case> = vec![
            ("bare scalar", None, format!("        image: {P}/hivekeep-api:0.1.0\n"),
                Ok(format!("        image: example.com/hivekeep/hivekeep-api:v1.2.3{s}\n"))),
            ("double quoted", None, format!(r#"image: "{P}/hivekeep-keeper:0.1.0""#),
                Ok(format!(r#"image: "example.com/hivekeep/hivekeep-keeper:v1.2.3{s}""#))),
            ("single quoted", None, format!("image: '{P}/hivekeep-gate:0.1.0'"),
                Ok(format!("image: 'example.com/hivekeep/hivekeep-gate:v1.2.3{s}'"))),
            ("flow sequence", None, format!("images: [{P}/hivekeep-controller:0.1.0, other]"),
                Ok(format!("images: [example.com/hivekeep/hivekeep-controller:v1.2.3{s}, other]"))),
            ("CRD field", None, format!("spec:\n  workerImage: {P}/hivekeep-sandbox-gvisor:0.1.0\n"),
                Ok(format!("spec:\n  workerImage: example.com/hivekeep/hivekeep-sandbox-gvisor:v1.2.3{s}\n"))),
            ("tag already carries a digest", Some(pinned_src), format!("image: {P}/hivekeep-api:0.1.0\n"),
                Ok(format!("image: example.com/hivekeep/hivekeep-api:v1.2.3{s}\n"))),
            ("two references on one line", None,
                format!("a: {P}/hivekeep-api:0.1.0, b: {P}/hivekeep-keeper:0.1.0\n"),
                Ok(format!("a: example.com/hivekeep/hivekeep-api:v1.2.3{s}, b: example.com/hivekeep/hivekeep-keeper:v1.2.3{s}\n"))),
            ("prose mention is left alone", None, format!("        # {P}/ image reference for the workload\n"),
                Ok(format!("        # {P}/ image reference for the workload\n"))),
            ("nothing to rewrite", None, "apiVersion: v1\nkind: ConfigMap\n".into(),
                Ok("apiVersion: v1\nkind: ConfigMap\n".into())),
            ("unexpanded placeholder", None, format!("image: {P}/hivekeep-sandbox-${{SANDBOX_CLASS}}:0.1.0\n"),
                Err("hivekeep-sandbox-${SANDBOX_CLASS has no published image")),
            ("reference extending a known one", None, format!("image: {P}/hivekeep-keeper-sidecar:0.1.0\n"),
                Err("hivekeep-keeper-sidecar:0.1.0 has no published image")),
            ("reference without a tag", None, format!("image: {P}/hivekeep-keeper\n"),
                Err("REGISTRY_PLACEHOLDER/hivekeep-keeper has no published image")),
            ("another repository ending in a known name", None, format!("image: {P}/other/hivekeep-keeper:0.1.0\n"),
                Err("REGISTRY_PLACEHOLDER/other/hivekeep-keeper:0.1.0 has no published image")),
            ("unknown component", None, format!("image: {P}/hivekeep-notacomponent:0.1.0\n"),
                Err("hivekeep-notacomponent:0.1.0 has no published image")),
            ("test fixture is not installable", None, format!("image: {P}/hivekeep-e2e-probe:0.1.0\n"),
                Err("hivekeep-e2e-probe:0.1.0 has no published image")),
            ("all failures are reported", None, format!("a: {P}/hivekeep-one:0\nb: {P}/hivekeep-two:0\n"),
                Err("hivekeep-two:0 has no published image")),
        ];
        for (name, src, input, want) in cases {
            let got = resolver(src.unwrap_or_else(test_source))
                .resolve_bytes(input.as_bytes())
                .await;
            match want {
                Ok(w) => assert_eq!(String::from_utf8(got.unwrap()).unwrap(), w, "{name}"),
                Err(sub) => {
                    let err = got.expect_err(name).to_string();
                    assert!(err.contains(sub), "{name}: {err}");
                }
            }
        }
    }

    #[tokio::test]
    async fn reports_every_failure() {
        let input = format!(
            "a: {P}/hivekeep-sandbox-${{SANDBOX_CLASS}}:0.1.0\nb: {P}/example/other/thing:1\nc: {P}/hivekeep-nope:0.1.0\n"
        );
        let err = resolver(test_source())
            .resolve_bytes(input.as_bytes())
            .await
            .unwrap_err()
            .to_string();
        for want in ["SANDBOX_CLASS", "example/other", "hivekeep-nope"] {
            assert!(err.contains(want), "missing {want}: {err}");
        }
    }

    #[tokio::test]
    async fn unreleased_images() {
        let reference = format!("image: {P}/hivekeep-demo-counter:0.1.0\n");
        let hint = "releases do not publish hivekeep-demo-counter";
        let got = resolver(test_source())
            .resolve_bytes(reference.as_bytes())
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(got).unwrap(),
            format!(
                "image: example.com/hivekeep/hivekeep-demo-counter:v1.2.3{}\n",
                suffix()
            )
        );

        let missing = || {
            Arc::new(StubRegistry {
                err: Some("MANIFEST_UNKNOWN".into()),
                ..Default::default()
            })
        };
        let err = Prebuilt::new(test_source(), missing())
            .resolve_bytes(reference.as_bytes())
            .await
            .unwrap_err()
            .to_string();
        for want in ["MANIFEST_UNKNOWN", hint, "dropping --image-repo"] {
            assert!(err.contains(want), "missing {want}: {err}");
        }

        let err = Prebuilt::new(test_source(), missing())
            .resolve_bytes(format!("image: {P}/hivekeep-api:0.1.0\n").as_bytes())
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("releases do not publish"), "{err}");
    }

    #[tokio::test]
    async fn deduplicates_failures() {
        let r = format!("image: {P}/hivekeep-nope:0.1.0\n");
        let err = resolver(test_source())
            .resolve_bytes(r.repeat(3).as_bytes())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err.matches("hivekeep-nope:0.1.0 has no published image")
                .count(),
            1,
            "{err}"
        );
    }

    #[tokio::test]
    async fn looks_up_each_image_once() {
        let registry = Arc::new(StubRegistry::default());
        let r = Prebuilt::new(test_source(), registry.clone());
        let input = format!(
            "a: {P}/hivekeep-api:0.1.0\nb: {P}/hivekeep-keeper:0.1.0\nc: {P}/hivekeep-api:0.1.0\n"
        );
        for _ in 0..2 {
            r.resolve_bytes(input.as_bytes()).await.unwrap();
        }
        let want: BTreeMap<String, usize> = [
            ("example.com/hivekeep/hivekeep-api:v1.2.3".to_string(), 1),
            ("example.com/hivekeep/hivekeep-keeper:v1.2.3".to_string(), 1),
        ]
        .into();
        assert_eq!(*registry.calls.lock(), want);
    }

    #[tokio::test]
    async fn reports_digest_failures() {
        let registry = Arc::new(StubRegistry {
            err: Some("UNAUTHORIZED".into()),
            ..Default::default()
        });
        let input = format!("a: {P}/hivekeep-api:0.1.0\nb: {P}/hivekeep-keeper:0.1.0\n");
        let err = Prebuilt::new(test_source(), registry)
            .resolve_bytes(input.as_bytes())
            .await
            .unwrap_err()
            .to_string();
        for want in [
            "resolving example.com/hivekeep/hivekeep-api:v1.2.3 to a digest: UNAUTHORIZED",
            "resolving example.com/hivekeep/hivekeep-keeper:v1.2.3 to a digest: UNAUTHORIZED",
        ] {
            assert!(err.contains(want), "missing {want}: {err}");
        }
    }

    #[tokio::test]
    async fn pin_cases() {
        let s = suffix();
        for (name, src) in [
            ("looked up", test_source()),
            (
                "tag already carries a digest",
                Source {
                    repo: "example.com/hivekeep".into(),
                    tag: format!("v1.2.3{s}"),
                },
            ),
        ] {
            let got = resolver(src).pin("hivekeep-envoy-dataplane").await.unwrap();
            assert_eq!(
                got,
                format!("example.com/hivekeep/hivekeep-envoy-dataplane:v1.2.3{s}"),
                "{name}"
            );
        }
    }

    /// Resolving every control plane manifest yields one that still parses,
    /// holds no unresolved reference, and pins every image to a digest.
    #[tokio::test]
    async fn resolve_every_install_manifest() {
        let install = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("manifests/install");
        let r = resolver(test_source());
        let mut paths = vec![install.clone()];
        let mut entries: Vec<_> = std::fs::read_dir(&install)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        entries.sort();
        paths.extend(
            entries
                .into_iter()
                .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "yaml")),
        );
        let resolved = regex::Regex::new(r#"example\.com/hivekeep/[^\s"',\]}]+"#).unwrap();
        for p in paths {
            let manifest = kube::read_path(&p).unwrap();
            let out = String::from_utf8(r.resolve_bytes(&manifest).await.unwrap()).unwrap();
            let left: Vec<_> = placeholder_pattern()
                .find_iter(&out)
                .map(|m| m.as_str().to_string())
                .collect();
            assert!(left.is_empty(), "{}: unresolved {left:?}", p.display());
            for m in resolved.find_iter(&out) {
                assert!(
                    m.as_str().contains("@sha256:"),
                    "{} is not pinned",
                    m.as_str()
                );
            }
            kube::decode_manifest(&out).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
        }
    }
}
