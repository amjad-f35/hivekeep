//! Images built with `docker buildx` and pushed, pinned by the digest the
//! push reports.

use super::platforms::build_platforms;
use super::ImageError;
use std::path::Path;
use tokio::process::Command;

/// Environment variable naming the docker binary (default `docker`).
pub const DOCKER_ENV: &str = "HK_DOCKER";

/// The docker binary to run.
pub fn docker_binary() -> String {
    std::env::var(DOCKER_ENV)
        .ok()
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| "docker".into())
}

/// docker arguments that build `context` for `platforms`, push it as `tag`,
/// and write the build metadata to `metadata_file`. `extra_flags` go before
/// the tag and context, which stay last.
pub fn docker_build_args(
    platforms: &str,
    extra_flags: &[String],
    tag: &str,
    metadata_file: &str,
    context: &str,
) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "buildx".into(),
        "build".into(),
        format!("--platform={platforms}"),
        "--push".into(),
        "--metadata-file".into(),
        metadata_file.into(),
    ];
    args.extend(extra_flags.iter().cloned());
    args.extend(["-t".to_string(), tag.to_string(), context.to_string()]);
    args
}

/// The digest buildx pushed, from the file `--metadata-file` wrote. Taking
/// it from the push means a tag that names another push cannot stand in.
pub fn pushed_digest(metadata: &[u8]) -> Result<String, ImageError> {
    let parsed: serde_json::Value = serde_json::from_slice(metadata)
        .map_err(|e| ImageError::Build(format!("while parsing the buildx metadata: {e}")))?;
    match parsed.get("containerimage.digest").and_then(|d| d.as_str()) {
        Some(d) if !d.is_empty() => Ok(d.to_string()),
        _ => Err(ImageError::Build(
            "the buildx metadata has no containerimage.digest".into(),
        )),
    }
}

/// Runs `docker <args>` in `root` (build output to stderr, so stdout stays
/// clean under CI log capture) and returns the pushed digest from the
/// metadata file the args name.
pub async fn run_build(
    docker: &str,
    root: &Path,
    args: &[String],
    metadata_file: &Path,
    what: &str,
) -> Result<String, ImageError> {
    let status = Command::new(docker)
        .args(args)
        .current_dir(root)
        .stdout(std::process::Stdio::from(std::io::stderr()))
        .stderr(std::process::Stdio::inherit())
        .status()
        .await
        .map_err(|e| ImageError::Build(format!("while building the {what} image: {e}")))?;
    if !status.success() {
        return Err(ImageError::Build(format!(
            "while building the {what} image: docker exited with {status}"
        )));
    }
    let raw = std::fs::read(metadata_file).map_err(|e| {
        ImageError::Build(format!("while reading the buildx metadata for {what}: {e}"))
    })?;
    pushed_digest(&raw)
}

/// Builds an image from `context`, pushes it to `repo/<image_name>`, and
/// returns the digest-pinned reference. The build-time tag only gives buildx
/// a name to push to; the returned reference always uses the digest.
pub async fn build_dockerfile_image(
    root: &Path,
    repo: &str,
    image_name: &str,
    context: &Path,
    platforms: &str,
    extra_flags: &[String],
) -> Result<String, ImageError> {
    let tag = format!("build-{}", chrono::Utc::now().timestamp());
    publish_dockerfile_image(
        root,
        repo,
        image_name,
        &tag,
        context,
        platforms,
        extra_flags,
    )
    .await
}

/// [`build_dockerfile_image`] with the tag chosen by the caller, for a
/// release that publishes every image under one tag.
pub async fn publish_dockerfile_image(
    root: &Path,
    repo: &str,
    image_name: &str,
    tag: &str,
    context: &Path,
    platforms: &str,
    extra_flags: &[String],
) -> Result<String, ImageError> {
    if repo.is_empty() {
        return Err(ImageError::Build(format!(
            "building {image_name} needs a registry to push to; set HK_BUILD_REPO or --build-repo"
        )));
    }
    let image_repo = format!("{}/{image_name}", repo.trim_end_matches('/'));
    let stage_tag = format!("{image_repo}:{tag}");
    let platforms = build_platforms(root, platforms)?;
    let metadata = tempfile::Builder::new()
        .prefix("buildx-metadata-")
        .suffix(".json")
        .tempfile()
        .map_err(|e| ImageError::Build(e.to_string()))?;
    let args = docker_build_args(
        &platforms,
        extra_flags,
        &stage_tag,
        &metadata.path().to_string_lossy(),
        &context.to_string_lossy(),
    );
    let digest = run_build(&docker_binary(), root, &args, metadata.path(), image_name)
        .await
        .map_err(|e| ImageError::Build(format!("{stage_tag}: {e}")))?;
    Ok(format!("{image_repo}@{digest}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_args() {
        let flags = vec!["--cache-from".to_string(), "type=gha".to_string()];
        assert_eq!(
            docker_build_args(
                "linux/amd64",
                &flags,
                "repo/img:build-1",
                "meta.json",
                "ctx"
            ),
            [
                "buildx",
                "build",
                "--platform=linux/amd64",
                "--push",
                "--metadata-file",
                "meta.json",
                "--cache-from",
                "type=gha",
                "-t",
                "repo/img:build-1",
                "ctx"
            ]
        );
        assert_eq!(
            docker_build_args("linux/amd64", &[], "repo/img:build-1", "meta.json", "ctx"),
            [
                "buildx",
                "build",
                "--platform=linux/amd64",
                "--push",
                "--metadata-file",
                "meta.json",
                "-t",
                "repo/img:build-1",
                "ctx"
            ]
        );
    }

    #[test]
    fn pushed_digest_cases() {
        const DIGEST: &str =
            "sha256:bab239626dc054856e882a2ef022dfa845834113d213697e1dccd27dfbfac1a5";
        let multi = format!(
            r#"{{"buildx.build.ref":"hk-release/hk-release0/x","containerimage.digest":"{DIGEST}","image.name":"localhost:5000/hivekeep/hivekeep-envoy-dataplane:v1"}}"#
        );
        assert_eq!(
            pushed_digest(multi.as_bytes()).unwrap(),
            DIGEST,
            "multi-platform push"
        );
        assert!(
            pushed_digest(br#"{"buildx.build.ref":"hk-release/hk-release0/x"}"#).is_err(),
            "no digest"
        );
        assert!(pushed_digest(b"").is_err(), "not JSON");
    }
}
