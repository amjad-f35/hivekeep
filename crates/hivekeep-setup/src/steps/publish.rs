//! Publishing a release: the hivectl binaries with checksums, the worker
//! images, and every released image under the release tag.

use super::{Env, Result, StepError};
use crate::images::{self, release};
use crate::log;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};

/// The binary a release attaches, and the crate that builds it.
pub(crate) const RELEASE_BINARY: &str = "hivectl";
pub(crate) const RELEASE_BINARY_PACKAGE: &str = "hivekeep-ctl";

/// Environment variable the release build stamps the version into.
pub(crate) const VERSION_STAMP_ENV: &str = "HIVEKEEP_VERSION";
/// Environment variable the release build stamps the commit into.
pub(crate) const COMMIT_STAMP_ENV: &str = "HIVEKEEP_COMMIT";

/// The worker images a WorkerPool points at, one per sandbox class. No
/// manifest names them, so an install never publishes them on the way.
pub(crate) const WORKER_IMAGES: [&str; 2] = ["hivekeep-sandbox-gvisor", "hivekeep-sandbox-microvm"];

/// The OS/arch pairs the release binary is built for. Windows is not among
/// them.
pub(crate) fn release_binary_platforms() -> Vec<&'static str> {
    vec!["darwin/amd64", "darwin/arm64", "linux/amd64", "linux/arm64"]
}

/// The release file name for a platform: `hivectl-linux-amd64`.
pub(crate) fn release_binary_name(platform: &str) -> String {
    format!("{RELEASE_BINARY}-{}", platform.replace('/', "-"))
}

/// The Rust target a platform builds with. Linux targets are musl, so the
/// binaries are static.
pub(crate) fn rust_target(platform: &str) -> Result<&'static str> {
    Ok(match platform {
        "darwin/amd64" => "x86_64-apple-darwin",
        "darwin/arm64" => "aarch64-apple-darwin",
        "linux/amd64" => "x86_64-unknown-linux-musl",
        "linux/arm64" => "aarch64-unknown-linux-musl",
        other => {
            return Err(StepError::msg(format!(
                "no release target for platform {other}"
            )))
        }
    })
}

/// Runs the build tools, so tests can stand in for them.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(String, String)],
    ) -> Result<()>;
}

/// Runs commands for real, their output on stderr.
pub struct HostRunner;

#[async_trait]
impl CommandRunner for HostRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(String, String)],
    ) -> Result<()> {
        let status = tokio::process::Command::new(program)
            .args(args)
            .current_dir(dir)
            .envs(env.iter().map(|(k, v)| (k, v)))
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await
            .map_err(|e| StepError::msg(format!("while running {program}: {e}")))?;
        if !status.success() {
            return Err(StepError::msg(format!(
                "{program} {}: {status}",
                args.join(" ")
            )));
        }
        Ok(())
    }
}

/// The cargo invocation building the release binary for a target.
pub(crate) fn release_build_args(target: &str) -> Vec<String> {
    [
        "build",
        "--release",
        "--locked",
        "-p",
        RELEASE_BINARY_PACKAGE,
        "--bin",
        RELEASE_BINARY,
        "--target",
        target,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Writes `path`'s SHA-256 beside it as `<path>.sha256`, in the
/// `<hex>  <name>` form `shasum -a 256 -c` reads, naming the file without its
/// directory. Returns the checksum file's path.
pub(crate) fn write_checksum(path: &Path) -> Result<PathBuf> {
    let data = std::fs::read(path)
        .map_err(|e| StepError::msg(format!("while hashing {}: {e}", path.display())))?;
    let digest = hex::encode(Sha256::digest(&data));
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let mut sum = path.as_os_str().to_owned();
    sum.push(".sha256");
    let sum = PathBuf::from(sum);
    std::fs::write(&sum, format!("{digest}  {name}\n"))
        .map_err(|e| StepError::msg(format!("while writing {}: {e}", sum.display())))?;
    Ok(sum)
}

/// The tagged reference of every image a release publishes to `repo`.
pub(crate) fn release_image_refs(repo: &str, tag: &str) -> Vec<String> {
    let repo = repo.trim_end_matches('/');
    let mut refs: Vec<String> = images::release_components()
        .iter()
        .map(|c| format!("{repo}/{c}:{tag}"))
        .collect();
    refs.extend(
        images::release_dockerfile_images()
            .iter()
            .map(|i| format!("{repo}/{}:{tag}", i.name)),
    );
    refs
}

/// The publish release-images settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishReleaseOptions {
    /// Rebuild and replace images the registry already holds for the tag.
    pub overwrite: bool,
}

fn write_lines(w: &mut dyn Write, header: &str, lines: &[String]) -> Result<()> {
    writeln!(w, "\n{header}").map_err(|e| StepError::msg(e.to_string()))?;
    for l in lines {
        writeln!(w, "{l}").map_err(|e| StepError::msg(e.to_string()))?;
    }
    Ok(())
}

impl Env {
    /// Builds hivectl for every release platform into `out_dir`, stamped with
    /// the release version and commit, with a checksum beside each, and
    /// writes the produced paths to `w`.
    pub async fn publish_release_binaries(&self, w: &mut dyn Write, out_dir: &Path) -> Result<()> {
        self.publish_release_binaries_with(&HostRunner, w, out_dir, &release_binary_platforms())
            .await
    }

    pub(crate) async fn publish_release_binaries_with(
        &self,
        runner: &dyn CommandRunner,
        w: &mut dyn Write,
        out_dir: &Path,
        platforms: &[&str],
    ) -> Result<()> {
        let version = self.hivekeep_version();
        release::check_release_version(&version).map_err(|e| {
            StepError::msg(format!(
                "the build version: {e}; set HK_VERSION to the release tag"
            ))
        })?;
        let Some((commit, _)) = crate::builder::commit_stamp(&self.cfg.root) else {
            return Err(StepError::msg(format!(
                "release binaries are stamped with the commit they were built from, and {} has none checked out",
                self.cfg.root.display()
            )));
        };
        log::step(&format!("publish_release_binaries ({version})"));
        std::fs::create_dir_all(out_dir)
            .map_err(|e| StepError::msg(format!("while creating {}: {e}", out_dir.display())))?;
        let mut written = Vec::new();
        for platform in platforms {
            let target = rust_target(platform)?;
            let env = vec![
                (VERSION_STAMP_ENV.to_string(), version.clone()),
                (COMMIT_STAMP_ENV.to_string(), commit.clone()),
            ];
            runner
                .run("cargo", &release_build_args(target), &self.cfg.root, &env)
                .await
                .map_err(|e| {
                    e.context(format!("while building {RELEASE_BINARY} for {platform}"))
                })?;
            let built = self
                .cfg
                .path(&["target", target, "release", RELEASE_BINARY]);
            let out = out_dir.join(release_binary_name(platform));
            std::fs::copy(&built, &out).map_err(|e| {
                StepError::msg(format!(
                    "while copying {} to {}: {e}",
                    built.display(),
                    out.display()
                ))
            })?;
            let sum = write_checksum(&out)?;
            written.push(out.display().to_string());
            written.push(sum.display().to_string());
        }
        write_lines(w, &format!("Release binaries for {version}:"), &written)
    }

    /// Builds and pushes the worker images and writes `<image>: <ref>`
    /// lines, after every build so the refs sit together.
    pub async fn publish_worker_images(&self, w: &mut dyn Write) -> Result<()> {
        let version = self.hivekeep_version();
        log::step(&format!("publish_worker_images ({version})"));
        let builder = self.builder()?;
        let mut refs = Vec::new();
        for img in WORKER_IMAGES {
            refs.push(format!("{img}: {}", builder.build(img).await?));
        }
        write_lines(w, &format!("Worker images for {version}:"), &refs)
    }

    /// Builds and pushes every released image under the release tag. A
    /// registry already holding the complete set is left alone unless
    /// `overwrite`; after pushing, every tag must name the pushed digest on
    /// every platform.
    pub async fn publish_release_images(
        &self,
        w: &mut dyn Write,
        opts: PublishReleaseOptions,
    ) -> Result<()> {
        let repo = self.cfg.build_repo.clone();
        if repo.is_empty() {
            return Err(StepError::msg(
                "publishing release images needs a registry to push to; set HK_BUILD_REPO or --build-repo",
            ));
        }
        let tag = self.hivekeep_version();
        release::check_release_tag(&repo, &tag).map_err(|e| {
            StepError::msg(format!(
                "the build version: {e}; set HK_VERSION to the release tag"
            ))
        })?;
        log::step(&format!("publish_release_images ({tag})"));
        let platforms = release::platforms(&self.cfg.root, &self.cfg.build_platforms)?;
        let tagged = release_image_refs(&repo, &tag);
        if !opts.overwrite {
            let incomplete = release::check_published(&tagged, &platforms).await?;
            if incomplete.is_empty() {
                log::info(&format!(
                    "{repo} already holds every image for {tag}; nothing built. Pass --overwrite to rebuild and replace them."
                ));
                return Ok(());
            }
            for i in &incomplete {
                log::info(&format!(
                    "to build: {} lacks {}",
                    i.reference,
                    i.missing.join(", ")
                ));
            }
        }
        let builder = self.builder()?;
        let Some((revision, _)) = crate::builder::commit_stamp(&self.cfg.root) else {
            return Err(StepError::msg(format!(
                "release images are labeled with the commit they were built from, and {} has none checked out",
                self.cfg.root.display()
            )));
        };
        let labels = vec![
            format!("{}={}", release::SOURCE_LABEL, release::SOURCE_URL),
            format!("{}={revision}", release::REVISION_LABEL),
            format!("{}={tag}", release::VERSION_LABEL),
        ];
        // The Dockerfile images are slower and likelier to fail, so they go
        // first: a failure there publishes nothing under the tag.
        let docker_flags: Vec<String> = labels.iter().map(|l| format!("--label={l}")).collect();
        let mut refs = Vec::new();
        for img in images::release_dockerfile_images() {
            refs.push(
                images::publish_dockerfile_image(
                    &self.cfg.root,
                    &repo,
                    img.name,
                    &tag,
                    &self.cfg.path(&[img.context]),
                    &self.cfg.build_platforms,
                    &docker_flags,
                )
                .await?,
            );
        }
        let components = images::release_components();
        refs.extend(builder.build_tagged(&tag, &labels, &components).await?);

        let incomplete = release::check_published(&tagged, &platforms).await?;
        if !incomplete.is_empty() {
            let lines: Vec<String> = incomplete
                .iter()
                .map(|i| format!("{} lacks {}", i.reference, i.missing.join(", ")))
                .collect();
            return Err(StepError::msg(format!(
                "after publishing, {} image(s) are incomplete:\n  {}",
                incomplete.len(),
                lines.join("\n  ")
            )));
        }
        let stale = release::stale_tags(&tagged, &refs).await?;
        if !stale.is_empty() {
            return Err(StepError::msg(format!(
                "after publishing, {} tag(s) do not name the pushed image:\n  {}",
                stale.len(),
                stale.join("\n  ")
            )));
        }
        write_lines(w, &format!("Release images for {tag}:"), &refs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use parking_lot::Mutex;

    /// The version variable is process-wide, so the tests that set it share
    /// one lock.
    fn with_version<T>(v: &str, f: impl FnOnce() -> T) -> T {
        let _g = crate::builder::VERSION_ENV_LOCK.lock();
        std::env::set_var(crate::builder::VERSION_ENV, v);
        let out = f();
        std::env::remove_var(crate::builder::VERSION_ENV);
        out
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn builder_refuses_prebuilt_images() {
        let dir = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            root: dir.path().to_path_buf(),
            images: images::Source {
                repo: "example.com/hivekeep".into(),
                tag: "v1.2.3".into(),
            },
            ..Default::default()
        });
        let err = e
            .builder()
            .err()
            .expect("no builder under --image-repo")
            .to_string();
        assert!(err.contains("--image-repo"), "{err}");
    }

    #[test]
    fn publish_release_images_needs_a_registry() {
        let dir = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            root: dir.path().to_path_buf(),
            ..Default::default()
        });
        let err = rt().block_on(
            e.publish_release_images(&mut std::io::sink(), PublishReleaseOptions::default()),
        );
        assert!(err.unwrap_err().to_string().contains("HK_BUILD_REPO"));
    }

    /// Every release image already published for every platform: nothing is
    /// rebuilt, so no builder (or docker) is needed.
    #[test]
    fn publish_release_images_skips_a_complete_release() {
        use crate::images::reference::Platform;
        let rt = rt();
        let reg = rt.block_on(crate::images::testregistry::TestRegistry::start());
        let repo = format!("{}/hivekeep", reg.host());
        let platforms = [
            Platform::parse("linux/amd64").unwrap(),
            Platform::parse("linux/arm64").unwrap(),
        ];
        for r in release_image_refs(&repo, "v1.2.3") {
            let (name, tag) = r.rsplit_once(':').unwrap();
            let path = name.strip_prefix(&format!("{}/", reg.host())).unwrap();
            reg.push_index(path, tag, &platforms);
        }
        let dir = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            root: dir.path().to_path_buf(),
            build_repo: repo,
            build_platforms: "linux/amd64,linux/arm64".into(),
            ..Default::default()
        });
        let res = with_version("v1.2.3", || {
            rt.block_on(
                e.publish_release_images(&mut std::io::sink(), PublishReleaseOptions::default()),
            )
        });
        res.expect("every image is published");
    }

    #[test]
    fn publish_release_images_rejects_a_non_release_tag() {
        for version in ["v1-test", "v1.2.3,latest", "dev"] {
            let dir = tempfile::tempdir().unwrap();
            let e = Env::local(Config {
                root: dir.path().to_path_buf(),
                build_repo: "example.com/hivekeep".into(),
                ..Default::default()
            });
            let res = with_version(version, || {
                rt().block_on(e.publish_release_images(
                    &mut std::io::sink(),
                    PublishReleaseOptions { overwrite: true },
                ))
            });
            let err = res.unwrap_err().to_string();
            assert!(err.contains("not a release tag"), "{version}: {err}");
        }
    }

    #[test]
    fn release_image_refs_cover_the_release() {
        let refs = release_image_refs("example.com/hivekeep/", "v1");
        assert_eq!(
            refs.len(),
            images::release_components().len() + images::release_dockerfile_images().len()
        );
        for want in [
            "example.com/hivekeep/hivekeep-api:v1",
            "example.com/hivekeep/hivekeep-envoy-dataplane:v1",
            "example.com/hivekeep/hivekeep-credential-provider-k8s:v1",
            "example.com/hivekeep/hivekeep-objectstore-plugin:v1",
        ] {
            assert!(refs.iter().any(|r| r == want), "{refs:?} lacks {want}");
        }
        assert!(!refs
            .iter()
            .any(|r| r == "example.com/hivekeep/hivekeep-demo-counter:v1"));
    }

    #[test]
    fn release_binary_names() {
        let got: Vec<String> = release_binary_platforms()
            .iter()
            .map(|p| release_binary_name(p))
            .collect();
        assert_eq!(
            got,
            [
                "hivectl-darwin-amd64",
                "hivectl-darwin-arm64",
                "hivectl-linux-amd64",
                "hivectl-linux-arm64"
            ]
        );
        for p in release_binary_platforms() {
            rust_target(p).unwrap();
        }
    }

    #[test]
    fn write_checksum_names_the_file_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hivectl-linux-amd64");
        std::fs::write(&path, b"binary").unwrap();
        let sum = write_checksum(&path).unwrap();
        assert_eq!(sum, dir.path().join("hivectl-linux-amd64.sha256"));
        let want = format!(
            "{}  hivectl-linux-amd64\n",
            hex::encode(Sha256::digest(b"binary"))
        );
        assert_eq!(std::fs::read_to_string(sum).unwrap(), want);
    }

    #[test]
    fn publish_release_binaries_rejects_a_non_release_tag() {
        let dir = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            root: dir.path().to_path_buf(),
            ..Default::default()
        });
        let res = with_version("dev", || {
            rt().block_on(e.publish_release_binaries(&mut std::io::sink(), out.path()))
        });
        assert!(res.unwrap_err().to_string().contains("not a release tag"));
    }

    /// Records the build and drops a fake binary where cargo would.
    type Call = (Vec<String>, Vec<(String, String)>);
    struct FakeCargo(Mutex<Vec<Call>>);

    #[async_trait]
    impl CommandRunner for FakeCargo {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            dir: &Path,
            env: &[(String, String)],
        ) -> Result<()> {
            assert_eq!(program, "cargo");
            let target = &args[args.iter().position(|a| a == "--target").unwrap() + 1];
            let out = dir.join("target").join(target).join("release");
            std::fs::create_dir_all(&out).unwrap();
            std::fs::write(out.join(RELEASE_BINARY), b"built").unwrap();
            self.0.lock().push((args.to_vec(), env.to_vec()));
            Ok(())
        }
    }

    #[test]
    fn publish_release_binaries_stamps_version_and_commit() {
        // Needs a git checkout to stamp from: a throwaway one with a commit.
        let work = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .args(["-c", "user.name=test", "-c", "user.email=test@example.com"])
                .args(args)
                .current_dir(work.path())
                .output()
                .is_ok_and(|o| o.status.success())
        };
        if !(git(&["init", "-q"]) && git(&["commit", "-q", "--allow-empty", "-m", "stamp"])) {
            eprintln!("skipping: git is unavailable");
            return;
        }
        let commit = crate::builder::commit_stamp(work.path())
            .expect("the throwaway checkout has a commit")
            .0;
        let out = tempfile::tempdir().unwrap();
        let e = Env::local(Config {
            root: work.path().to_path_buf(),
            ..Default::default()
        });
        let runner = FakeCargo(Mutex::new(vec![]));
        let mut buf = Vec::new();
        with_version("v1.2.3", || {
            rt().block_on(e.publish_release_binaries_with(
                &runner,
                &mut buf,
                out.path(),
                &["linux/amd64"],
            ))
        })
        .unwrap();
        let calls = runner.0.lock();
        assert_eq!(calls[0].0, release_build_args("x86_64-unknown-linux-musl"));
        assert!(calls[0]
            .1
            .contains(&(VERSION_STAMP_ENV.to_string(), "v1.2.3".to_string())));
        assert!(calls[0].1.contains(&(COMMIT_STAMP_ENV.to_string(), commit)));
        assert!(out.path().join("hivectl-linux-amd64.sha256").is_file());
    }
}
