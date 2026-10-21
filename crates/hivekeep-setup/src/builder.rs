//! Building the component images from this checkout and pushing them, then
//! rewriting the manifests' references to the pushed digests.
//!
//! Every component is built from one shared Dockerfile ([`DOCKERFILE`],
//! relative to the repository root, which is also the build context) with
//! `docker buildx build --push`, selecting the component with
//! `--build-arg COMPONENT=<name>` and stamping the build with `VERSION`,
//! `COMMIT` and `BUILD_DATE` build args. The Dockerfile maps each name in
//! [`COMPONENTS`] to a crate and binary and ends in a stage of that name. The digest comes from the
//! push itself (`--metadata-file`), so a tag moved by someone else can never
//! be installed by accident.

use crate::images::dockerfile::{docker_binary, docker_build_args, run_build};
use crate::images::{
    build_platforms, component_of, placeholder_pattern, ImageError, ImageResolver, COMPONENTS,
    REGISTRY_PLACEHOLDER,
};
use crate::log;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// The shared Dockerfile, relative to the repository root.
pub const DOCKERFILE: &str = "images/Dockerfile";
/// Environment entry naming the registry built images are pushed to.
pub const BUILD_REPO_ENV: &str = "HK_BUILD_REPO";
/// Environment entry naming the platforms images are built for.
pub const BUILD_PLATFORMS_ENV: &str = "HK_BUILD_PLATFORMS";
/// Environment variable overriding the version stamped into builds.
pub const VERSION_ENV: &str = "HK_VERSION";

/// Serialises tests that set [`VERSION_ENV`].
#[cfg(test)]
pub(crate) static VERSION_ENV_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The source image builder.
pub struct Builder {
    root: PathBuf,
    repo: String,
    platforms: String,
    docker: String,
    extra_flags: Vec<String>,
    /// Pinned references of components already built this run.
    built: Mutex<HashMap<String, String>>,
}

fn lookup(env: &[String], name: &str) -> String {
    let prefix = format!("{name}=");
    env.iter()
        .rev()
        .find_map(|e| e.strip_prefix(&prefix).map(str::to_string))
        .or_else(|| std::env::var(name).ok())
        .unwrap_or_default()
}

fn on_path(binary: &str) -> bool {
    if binary.contains('/') {
        return Path::new(binary).is_file();
    }
    std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join(binary).is_file()))
        .unwrap_or(false)
}

impl Builder {
    /// A builder for the checkout at `root`, with environment entries
    /// (`HK_BUILD_REPO=...`, `HK_BUILD_PLATFORMS=...`) that override the
    /// process environment. Fails up front when docker is missing, so the
    /// install stops before it changes the cluster.
    pub fn new(root: &Path, env: Vec<String>) -> Result<Self, ImageError> {
        let docker = docker_binary();
        if !on_path(&docker) {
            return Err(ImageError::Build(format!(
                "building images from source needs {docker} with buildx on PATH; install it, or install pre-built \
                 images with --image-repo"
            )));
        }
        Ok(Self {
            root: root.to_path_buf(),
            repo: lookup(&env, BUILD_REPO_ENV)
                .trim_end_matches('/')
                .to_string(),
            platforms: lookup(&env, BUILD_PLATFORMS_ENV),
            docker,
            extra_flags: vec![],
            built: Mutex::default(),
        })
    }

    /// Uses `docker` as the docker binary.
    pub fn with_docker(mut self, docker: impl Into<String>) -> Self {
        self.docker = docker.into();
        self
    }

    /// Adds `docker buildx build` flags (cache settings and the like).
    pub fn with_extra_flags(mut self, flags: Vec<String>) -> Self {
        self.extra_flags = flags;
        self
    }

    /// The registry images are pushed to.
    pub fn repo(&self) -> &str {
        &self.repo
    }

    fn require_repo(&self) -> Result<(), ImageError> {
        if self.repo.is_empty() {
            return Err(ImageError::Build(format!(
                "building images from source needs a registry to push to; set {BUILD_REPO_ENV} or --build-repo"
            )));
        }
        Ok(())
    }

    /// The docker arguments that build `component`, push it as `tag`, and
    /// write the push metadata to `metadata_file`.
    pub fn args(
        &self,
        component: &str,
        tag: &str,
        labels: &[String],
        metadata_file: &str,
    ) -> Result<Vec<String>, ImageError> {
        let platforms = build_platforms(&self.root, &self.platforms)?;
        let mut extra = vec![
            "-f".to_string(),
            self.root.join(DOCKERFILE).to_string_lossy().into_owned(),
        ];
        extra.push(format!("--build-arg=COMPONENT={component}"));
        extra.extend(stamp_args(&self.root, &build_version(&self.root)));
        extra.extend(labels.iter().map(|l| format!("--label={l}")));
        extra.extend(self.extra_flags.iter().cloned());
        Ok(docker_build_args(
            &platforms,
            &extra,
            tag,
            metadata_file,
            &self.root.to_string_lossy(),
        ))
    }

    async fn push(
        &self,
        component: &str,
        tag: &str,
        labels: &[String],
    ) -> Result<String, ImageError> {
        self.require_repo()?;
        if !COMPONENTS.contains(&component) {
            return Err(ImageError::Unknown(format!(
                "{component} is not a Hivekeep component"
            )));
        }
        let _t = log::Timer::new(format!("image build {component}"));
        let metadata = tempfile::Builder::new()
            .prefix("buildx-metadata-")
            .suffix(".json")
            .tempfile()
            .map_err(|e| ImageError::Build(e.to_string()))?;
        let staged = format!("{}/{component}:{tag}", self.repo);
        let args = self.args(
            component,
            &staged,
            labels,
            &metadata.path().to_string_lossy(),
        )?;
        let digest = run_build(&self.docker, &self.root, &args, metadata.path(), component)
            .await
            .map_err(|e| ImageError::Build(format!("{staged}: {e}")))?;
        Ok(digest)
    }

    /// Builds and pushes one component, returning `<repo>/<component>@<digest>`.
    /// Each component is built at most once per builder.
    pub async fn build(&self, component: &str) -> Result<String, ImageError> {
        if let Some(r) = self.built.lock().get(component) {
            return Ok(r.clone());
        }
        let tag = format!("build-{}", chrono::Utc::now().timestamp());
        let digest = self.push(component, &tag, &[]).await?;
        let reference = format!("{}/{component}@{digest}", self.repo);
        self.built
            .lock()
            .insert(component.to_string(), reference.clone());
        Ok(reference)
    }

    /// Builds and pushes components under `tag`, each `key=value` in
    /// `labels` set as an image label, returning
    /// `<repo>/<component>:<tag>@<digest>` in order. A build that reports no
    /// digest fails the whole call, so a release is never short an image.
    pub async fn build_tagged(
        &self,
        tag: &str,
        labels: &[String],
        components: &[&str],
    ) -> Result<Vec<String>, ImageError> {
        let _t = log::Timer::new(format!("image build --tag={tag}"));
        let mut refs = Vec::with_capacity(components.len());
        for c in components {
            let digest = self.push(c, tag, labels).await?;
            refs.push(format!("{}/{c}:{tag}@{digest}", self.repo));
        }
        Ok(refs)
    }
}

#[async_trait]
impl ImageResolver for Builder {
    /// Builds every component the manifest references and pins each
    /// reference to what was pushed.
    async fn resolve_bytes(&self, manifest: &[u8]) -> Result<Vec<u8>, ImageError> {
        let text = String::from_utf8_lossy(manifest);
        let mut unknown = BTreeSet::new();
        let mut wanted = BTreeSet::new();
        for m in placeholder_pattern().find_iter(&text) {
            match component_of(m.as_str()) {
                Some(c) => {
                    wanted.insert(c);
                }
                None => {
                    unknown.insert(m.as_str().to_string());
                }
            }
        }
        if !unknown.is_empty() {
            let mut lines: Vec<String> = unknown
                .iter()
                .map(|r| format!("{r} names no buildable component"))
                .collect();
            lines.push(format!(
                "buildable images under {REGISTRY_PLACEHOLDER} are {}; a reference that is templated has to be \
                 rendered before it can be resolved",
                COMPONENTS.join(", ")
            ));
            return Err(ImageError::Unknown(lines.join("\n")));
        }
        let mut pinned = HashMap::new();
        for c in wanted {
            pinned.insert(c, self.build(c).await?);
        }
        let out = placeholder_pattern().replace_all(&text, |cap: &regex::Captures| {
            component_of(&cap[0])
                .and_then(|c| pinned.get(c).cloned())
                .unwrap_or_else(|| cap[0].to_string())
        });
        Ok(out.into_owned().into_bytes())
    }
}

/// The version stamped into builds of this checkout: `HK_VERSION`, else
/// `git describe`, else `dev`.
pub fn build_version(root: &Path) -> String {
    build_version_with(root, std::env::var(VERSION_ENV).ok().as_deref())
}

/// [`build_version`] with the override given rather than read from the
/// environment.
pub fn build_version_with(root: &Path, explicit: Option<&str>) -> String {
    if let Some(v) = explicit.filter(|v| !v.is_empty()) {
        return v.to_string();
    }
    let out = std::process::Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .current_dir(root)
        .stderr(std::process::Stdio::null())
        .output();
    match out {
        Ok(o) if o.status.success() => {
            let v = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if v.is_empty() {
                "dev".into()
            } else {
                v
            }
        }
        _ => "dev".into(),
    }
}

/// The commit checked out at `root` and its committer date (RFC 3339).
/// `None` when there is no commit, as in a source tarball.
pub fn commit_stamp(root: &Path) -> Option<(String, String)> {
    let out = std::process::Command::new("git")
        .args(["show", "-s", "--format=%H %cI", "HEAD"])
        .current_dir(root)
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let fields: Vec<&str> = text.split_whitespace().collect();
    match fields.as_slice() {
        [commit, date] => Some((commit.to_string(), date.to_string())),
        _ => None,
    }
}

/// The build args that stamp an image built from `root`: the version always,
/// and the commit and its date when there is one. The date is the commit's,
/// so rebuilding one commit stamps the same values.
pub fn stamp_args(root: &Path, version: &str) -> Vec<String> {
    let mut args = vec![format!("--build-arg=VERSION={version}")];
    if let Some((commit, date)) = commit_stamp(root) {
        args.push(format!("--build-arg=COMMIT={commit}"));
        args.push(format!("--build-arg=BUILD_DATE={date}"));
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A docker stand-in that appends each invocation's arguments to a file
    /// (one line per call) and writes push metadata with a digest derived
    /// from the call count. With `fail_after`, calls past it write metadata
    /// without a digest.
    fn fake_docker(dir: &Path, fail_after: Option<usize>) -> (String, PathBuf) {
        let args_file = dir.join("args");
        let binary = dir.join("docker");
        let limit = fail_after
            .map(|n| n.to_string())
            .unwrap_or_else(|| "999999".into());
        let script = format!(
            "#!/bin/sh\necho \"$@\" >> {args}\nwhile [ $# -gt 0 ]; do if [ \"$1\" = \"--metadata-file\" ]; then shift; META=\"$1\"; fi; shift; done\n\
             n=$(wc -l < {args} | tr -d ' ')\nif [ \"$n\" -gt {limit} ]; then printf '{{}}' > \"$META\"; else printf '{{\"containerimage.digest\":\"sha256:%064d\"}}' \"$n\" > \"$META\"; fi\n",
            args = args_file.display()
        );
        std::fs::write(&binary, script).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        (binary.to_string_lossy().into_owned(), args_file)
    }

    fn builder(root: &Path, docker: &str) -> Builder {
        Builder::new(
            root,
            vec![
                "HK_BUILD_REPO=repo".into(),
                "HK_BUILD_PLATFORMS=linux/amd64".into(),
            ],
        )
        .map(|b| b.with_docker(docker))
        .unwrap_or_else(|_| Builder {
            root: root.to_path_buf(),
            repo: "repo".into(),
            platforms: "linux/amd64".into(),
            docker: docker.into(),
            extra_flags: vec![],
            built: Mutex::default(),
        })
    }

    fn digest(n: usize) -> String {
        format!("sha256:{n:064}")
    }

    /// Every build selects its component and carries the version stamp, so
    /// what is pushed is addressable by component name and identifiable.
    #[test]
    fn args_always_select_the_component_and_stamp() {
        let root = tempfile::tempdir().unwrap();
        let b = builder(root.path(), "docker");
        for c in ["hivekeep-api", "hivekeep-sandbox-gvisor"] {
            let args = b.args(c, &format!("repo/{c}:t"), &[], "meta.json").unwrap();
            assert!(
                args.contains(&format!("--build-arg=COMPONENT={c}")),
                "{args:?}"
            );
            assert!(
                args.iter().any(|a| a.starts_with("--build-arg=VERSION=")),
                "{args:?}"
            );
            assert!(args.contains(&"-f".to_string()), "{args:?}");
        }
    }

    /// The subcommand stays first, and the tag and context stay last.
    #[test]
    fn args_keep_the_subcommand_in_front_and_context_last() {
        let root = tempfile::tempdir().unwrap();
        let b = builder(root.path(), "docker");
        let args = b
            .args("hivekeep-api", "repo/hivekeep-api:t", &[], "meta.json")
            .unwrap();
        assert_eq!(&args[..2], ["buildx", "build"]);
        let n = args.len();
        assert_eq!(
            &args[n - 3..],
            ["-t", "repo/hivekeep-api:t", &*root.path().to_string_lossy()]
        );
    }

    #[test]
    fn build_version_prefers_the_override() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(build_version_with(dir.path(), Some("v1.2.3")), "v1.2.3");
    }

    /// A source tarball has no git metadata, so the version falls back to
    /// "dev" rather than stamping an empty one.
    #[test]
    fn build_version_falls_back_to_dev() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(build_version_with(dir.path(), Some("")), "dev");
    }

    #[test]
    fn stamp_args_stamp_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "-q"],
            vec![
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "test",
            ],
        ] {
            let st = std::process::Command::new("git")
                .args(&args)
                .current_dir(dir.path())
                .output();
            match st {
                Ok(o) if o.status.success() => {}
                _ => {
                    eprintln!("skipping: git unavailable");
                    return;
                }
            }
        }
        let (commit, date) = commit_stamp(dir.path()).expect("a commit and its date");
        assert_eq!(commit.len(), 40);
        assert!(!date.is_empty());
        assert_eq!(
            stamp_args(dir.path(), "v1.2.3"),
            [
                "--build-arg=VERSION=v1.2.3".to_string(),
                format!("--build-arg=COMMIT={commit}"),
                format!("--build-arg=BUILD_DATE={date}"),
            ]
        );
        let bare = tempfile::tempdir().unwrap();
        assert_eq!(
            stamp_args(bare.path(), "v1.2.3").len(),
            1,
            "without a commit only the version is stamped"
        );
    }

    /// A release is only installable if every image carries the tag and the
    /// name `--image-repo` looks it up by.
    #[tokio::test]
    async fn build_tagged() {
        let dir = tempfile::tempdir().unwrap();
        let (docker, args_file) = fake_docker(dir.path(), None);
        let b = builder(dir.path(), &docker);
        let refs = b
            .build_tagged(
                "v1.2.3",
                &["org.example/source=https://example.com".into()],
                &["hivekeep-api", "hivekeep-keeper"],
            )
            .await
            .unwrap();
        assert_eq!(
            refs,
            [
                format!("repo/hivekeep-api:v1.2.3@{}", digest(1)),
                format!("repo/hivekeep-keeper:v1.2.3@{}", digest(2))
            ]
        );
        let recorded = std::fs::read_to_string(args_file).unwrap();
        let calls: Vec<&str> = recorded.lines().collect();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].starts_with("buildx build"), "{}", calls[0]);
        for want in [
            "-t repo/hivekeep-api:v1.2.3",
            "--label=org.example/source=https://example.com",
            "--build-arg=COMPONENT=hivekeep-api",
        ] {
            assert!(calls[0].contains(want), "{want}: {}", calls[0]);
        }
    }

    /// A build that reports no digest fails the release rather than leaving
    /// it short an image.
    #[tokio::test]
    async fn build_tagged_rejects_missing_refs() {
        let dir = tempfile::tempdir().unwrap();
        let (docker, _) = fake_docker(dir.path(), Some(1));
        let b = builder(dir.path(), &docker);
        assert!(b
            .build_tagged("v1", &[], &["hivekeep-api", "hivekeep-keeper"])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn resolve_bytes_builds_each_component_once() {
        let dir = tempfile::tempdir().unwrap();
        let (docker, args_file) = fake_docker(dir.path(), None);
        let b = builder(dir.path(), &docker);
        let input = "a: REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0\nb: \"REGISTRY_PLACEHOLDER/hivekeep-api:0.1.0\"\n";
        let out = String::from_utf8(b.resolve_bytes(input.as_bytes()).await.unwrap()).unwrap();
        let want = format!("repo/hivekeep-api@{}", digest(1));
        assert_eq!(out, format!("a: {want}\nb: \"{want}\"\n"));
        assert_eq!(
            std::fs::read_to_string(args_file).unwrap().lines().count(),
            1
        );

        let err = b
            .resolve_bytes(b"x: REGISTRY_PLACEHOLDER/hivekeep-nope:1\n")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("hivekeep-nope:1 names no buildable component"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn building_needs_a_repository() {
        let dir = tempfile::tempdir().unwrap();
        let (docker, _) = fake_docker(dir.path(), None);
        let mut b = builder(dir.path(), &docker);
        b.repo.clear();
        let err = b.build("hivekeep-api").await.unwrap_err();
        assert!(err.to_string().contains(BUILD_REPO_ENV), "{err}");
    }

    /// The shared Dockerfile knows every component: a build arm mapping it
    /// to a binary and a final stage named after it. A component missing
    /// from either would only fail at image build time.
    #[test]
    fn dockerfile_covers_every_component() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(DOCKERFILE);
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for c in COMPONENTS {
            assert!(
                text.contains(&format!("      {c})")),
                "{c}: no build arm in {DOCKERFILE}"
            );
            assert!(
                text.lines()
                    .any(|l| l.starts_with("FROM ") && l.ends_with(&format!(" AS {c}"))),
                "{c}: no stage in {DOCKERFILE}"
            );
        }
        for arg in [
            "ARG COMPONENT",
            "ARG VERSION",
            "ARG COMMIT",
            "ARG BUILD_DATE",
        ] {
            assert!(text.contains(arg), "{DOCKERFILE} does not declare {arg}");
        }
        assert!(
            text.trim_end().ends_with("FROM ${COMPONENT}"),
            "{DOCKERFILE} must end in the COMPONENT stage"
        );
    }

    #[test]
    fn env_entries_override_the_process_environment() {
        assert_eq!(
            lookup(
                &["HK_BUILD_REPO=a".into(), "HK_BUILD_REPO=b".into()],
                "HK_BUILD_REPO"
            ),
            "b"
        );
    }
}
