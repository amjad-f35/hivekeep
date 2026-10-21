//! Checks for publishing a release's images: the tag, the digests the tags
//! name after a push, and the platforms each published image covers.

use super::digest::remote_digest;
use super::platforms::build_platforms;
use super::reference::{ImageRef, Platform};
use super::registry::{is_index, RegistryClient};
use super::ImageError;
use regex::Regex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

/// OCI label naming the repository an image was built from.
pub const SOURCE_LABEL: &str = "org.opencontainers.image.source";
/// The source repository URL stamped on released images.
pub const SOURCE_URL: &str = "SOURCE_URL_PLACEHOLDER";
/// OCI label naming the commit an image was built from.
pub const REVISION_LABEL: &str = "org.opencontainers.image.revision";
/// OCI label naming the release an image belongs to. It replaces any value
/// inherited from a base image.
pub const VERSION_LABEL: &str = "org.opencontainers.image.version";

fn release_tag() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$").expect("valid regex")
    })
}

/// Fails unless `version` is a release tag, vMAJOR.MINOR.PATCH[-PRERELEASE].
pub fn check_release_version(version: &str) -> Result<(), ImageError> {
    if !release_tag().is_match(version) {
        return Err(ImageError::Other(format!(
            "{version:?} is not a release tag (vMAJOR.MINOR.PATCH[-PRERELEASE])"
        )));
    }
    Ok(())
}

/// Fails unless `tag` is a release tag that makes a valid image reference
/// under `repo`. Runs before anything is built, so a stray tag publishes
/// nothing.
pub fn check_release_tag(repo: &str, tag: &str) -> Result<(), ImageError> {
    check_release_version(tag)?;
    let reference = format!("{}/image:{tag}", repo.trim_end_matches('/'));
    ImageRef::parse(&reference).map_err(|e| {
        ImageError::Other(format!(
            "{repo} with tag {tag} is not a valid image reference: {e}"
        ))
    })?;
    Ok(())
}

/// A line for each of `tagged` whose tag does not name the digest pushed
/// for its repository. `pushed` are digest references, one per repository
/// (`repo/name:tag@sha256:...` or `repo/name@sha256:...`). Catches a tag
/// still naming an earlier push, which a platform check alone would pass.
pub async fn stale_tags(tagged: &[String], pushed: &[String]) -> Result<Vec<String>, ImageError> {
    let mut want: HashMap<String, String> = HashMap::new();
    for r in pushed {
        let d = ImageRef::parse_digest(r)?;
        let digest = d.digest.clone().unwrap_or_default();
        want.insert(d.context(), digest);
    }
    let mut stale = Vec::new();
    for r in tagged {
        let t = ImageRef::parse(r)?;
        if t.tag.is_none() {
            return Err(ImageError::Other(format!("{r} is not a tagged reference")));
        }
        let Some(expected) = want.get(&t.context()) else {
            stale.push(format!("{r}: nothing was pushed for it"));
            continue;
        };
        let got = remote_digest(r).await?;
        if &got != expected {
            stale.push(format!("{r} names {got}, not the pushed {expected}"));
        }
    }
    Ok(stale)
}

/// The platforms a build produces; Dockerfile images use the same set.
pub fn platforms(root: &Path, explicit: &str) -> Result<Vec<String>, ImageError> {
    Ok(build_platforms(root, explicit)?
        .split(',')
        .map(str::to_string)
        .collect())
}

/// A reference missing from its registry, or present without every wanted
/// platform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incomplete {
    pub reference: String,
    /// Wanted platforms the reference lacks; a missing reference lacks all.
    pub missing: Vec<String>,
}

impl std::fmt::Display for Incomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} lacks {}", self.reference, self.missing.join(", "))
    }
}

/// The references that do not hold an image for each of `platforms`.
///
/// Errs only when a registry cannot answer (network, credentials): that says
/// nothing about whether an image exists, and treating it as missing would
/// get published images rebuilt and replaced.
pub async fn check_published(
    refs: &[String],
    platforms: &[String],
) -> Result<Vec<Incomplete>, ImageError> {
    let want: Vec<Platform> = platforms
        .iter()
        .map(|p| Platform::parse(p))
        .collect::<Result<_, _>>()?;
    let mut incomplete = Vec::new();
    for r in refs {
        let have = index_platforms(r).await?;
        let missing: Vec<String> = want
            .iter()
            .zip(platforms)
            .filter(|(w, _)| !have.iter().any(|h| h.satisfies(w)))
            .map(|(_, p)| p.clone())
            .collect();
        if !missing.is_empty() {
            incomplete.push(Incomplete {
                reference: r.clone(),
                missing,
            });
        }
    }
    Ok(incomplete)
}

fn platform_of(v: &serde_json::Value) -> Platform {
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    Platform {
        os: s("os"),
        architecture: s("architecture"),
        variant: s("variant"),
    }
}

/// The platforms of the images `reference` names: each entry of an index, or
/// the one image of a plain manifest (what a single-platform build pushes).
/// None, without an error, when the reference does not exist.
pub async fn index_platforms(reference: &str) -> Result<Vec<Platform>, ImageError> {
    let parsed = ImageRef::parse(reference)?;
    let mut client = RegistryClient::new(parsed)?;
    let manifest = match client.manifest().await {
        Ok(m) => m,
        Err(e) if e.is_not_found() => return Ok(vec![]),
        Err(e) => {
            return Err(ImageError::Registry(format!(
                "while inspecting {reference}: {e}"
            )))
        }
    };
    let doc: serde_json::Value = serde_json::from_slice(&manifest.body).map_err(|e| {
        ImageError::Registry(format!("while reading the manifest of {reference}: {e}"))
    })?;
    if is_index(&manifest.media_type) {
        let entries = doc
            .get("manifests")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();
        return Ok(entries
            .iter()
            .filter_map(|m| m.get("platform"))
            .map(platform_of)
            .collect());
    }
    let config_digest = doc
        .pointer("/config/digest")
        .and_then(|d| d.as_str())
        .ok_or_else(|| {
            ImageError::Registry(format!("the image {reference} names has no config"))
        })?;
    let config = client.blob(config_digest).await.map_err(|e| {
        ImageError::Registry(format!("while reading the config of {reference}: {e}"))
    })?;
    let config: serde_json::Value = serde_json::from_slice(&config).map_err(|e| {
        ImageError::Registry(format!("while reading the config of {reference}: {e}"))
    })?;
    Ok(vec![platform_of(&config)])
}

#[cfg(test)]
mod tests {
    use super::super::testregistry::TestRegistry;
    use super::*;
    use crate::images::{release_components, COMPONENTS};

    #[test]
    fn released_are_components() {
        let mut seen = std::collections::HashSet::new();
        for c in release_components() {
            assert!(
                COMPONENTS.contains(&c),
                "released names {c}, which is not in COMPONENTS"
            );
            assert!(seen.insert(c), "released lists {c} twice");
        }
    }

    #[test]
    fn release_tags() {
        for tag in [
            "v1.2.3",
            "v0.5.0-rc",
            "v0.5.0-rc.1",
            "v0.5.0-pre",
            "v0.5.0-pre.4",
            "v10.0.0-alpha.2-x",
        ] {
            check_release_tag("ghcr.io/example/hivekeep", tag)
                .unwrap_or_else(|e| panic!("{tag}: {e}"));
        }
        for tag in [
            "v1-test",
            "vnext",
            "v1.2.3,latest",
            "1.2.3",
            "v1.2",
            "v1.2.3-",
            "v1.2.3foo",
            "",
        ] {
            assert!(
                check_release_tag("ghcr.io/example/hivekeep", tag).is_err(),
                "{tag}"
            );
        }
        assert!(check_release_tag("not a repo", "v1.2.3").is_err());
    }

    fn amd64() -> Platform {
        Platform::parse("linux/amd64").unwrap()
    }

    #[tokio::test]
    async fn stale_tag_detection() {
        let reg = TestRegistry::start().await;
        let repo = format!("{}/hivekeep", reg.host());
        reg.push_index("hivekeep/current", "v1", &[amd64()]);
        reg.push_index("hivekeep/stale", "v1", &[amd64()]);
        let current = remote_digest(&format!("{repo}/current:v1")).await.unwrap();
        // What the run pushed for "stale": a different index, never tagged.
        let pushed =
            super::super::testregistry::index_body(&[Platform::parse("linux/arm64").unwrap()]);
        let pushed_digest = super::super::registry::sha256_digest(&pushed);

        let got = stale_tags(
            &[
                format!("{repo}/current:v1"),
                format!("{repo}/stale:v1"),
                format!("{repo}/missing:v1"),
            ],
            &[
                format!("{repo}/current:v1@{current}"),
                format!("{repo}/stale@{pushed_digest}"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got[0].contains("stale:v1 names"), "{got:?}");
        assert!(got[1].contains("missing:v1: nothing was pushed"), "{got:?}");
    }

    #[tokio::test]
    async fn published_platform_check() {
        let reg = TestRegistry::start().await;
        let repo = format!("{}/hivekeep", reg.host());
        let arm64v8 = Platform::parse("linux/arm64/v8").unwrap();
        reg.push_index("hivekeep/complete", "v1", &[amd64(), arm64v8]);
        reg.push_index("hivekeep/amd64-only", "v1", &[amd64()]);
        // An image with no platform in its config.
        reg.push_image("hivekeep/single", "v1", &Platform::default());
        // A single-platform build pushes a plain image, not an index.
        reg.push_image("hivekeep/plain-amd64", "v1", &amd64());

        let refs: Vec<String> = [
            "complete:v1",
            "amd64-only:v1",
            "single:v1",
            "plain-amd64:v1",
            "complete:v2",
            "absent:v1",
        ]
        .iter()
        .map(|r| format!("{repo}/{r}"))
        .collect();
        let both = vec!["linux/amd64".to_string(), "linux/arm64".to_string()];
        let got = check_published(&refs, &both).await.unwrap();
        let inc = |r: &str, m: &[&str]| Incomplete {
            reference: format!("{repo}/{r}"),
            missing: m.iter().map(|s| s.to_string()).collect(),
        };
        assert_eq!(
            got,
            vec![
                inc("amd64-only:v1", &["linux/arm64"]),
                inc("single:v1", &["linux/amd64", "linux/arm64"]),
                inc("plain-amd64:v1", &["linux/arm64"]),
                inc("complete:v2", &["linux/amd64", "linux/arm64"]),
                inc("absent:v1", &["linux/amd64", "linux/arm64"]),
            ]
        );

        let got = check_published(
            &[format!("{repo}/plain-amd64:v1")],
            &["linux/amd64".to_string()],
        )
        .await
        .unwrap();
        assert!(got.is_empty(), "{got:?}");
    }

    #[tokio::test]
    async fn check_published_fails_when_the_registry_cannot_answer() {
        let reg = TestRegistry::start_denying().await;
        let reference = format!("{}/hivekeep/hivekeep-api:v1", reg.host());
        assert!(check_published(&[reference], &["linux/amd64".to_string()])
            .await
            .is_err());
    }
}
