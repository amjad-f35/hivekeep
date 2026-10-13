//! OCI bundles for an actor's containers: the overlay spec the worker
//! mounts the rootfs from, and the process the container runs.
//!
//! Images are pulled and unpacked by an [`ImageCache`], which keeps layers
//! once per node; the keeper only writes each bundle's overlay spec naming
//! the cached layer directories.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use hivekeep_proto::keeper::v1 as pb;
use tonic::Status;

use crate::context::Ctx;
use crate::paths::NodePaths;

pub use hivekeep_node::image::{
    Image, ImageCache, ImageConfig, ImageVolumeOverlay, NoImageCache, OverlaySpec,
    OVERLAY_SPEC_FILE_NAME,
};

/// Dropping this removes every default capability.
const CAPABILITY_ALL: &str = "ALL";
/// Capabilities a container gets unless it drops them.
const DEFAULT_CAPABILITIES: [&str; 3] = ["AUDIT_WRITE", "KILL", "NET_BIND_SERVICE"];
/// The PATH a container gets when neither it nor its image sets one.
const DEFAULT_PATH: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Writes `spec` into the bundle at `bundle` atomically, so the image
/// cache's GC never reads a partial spec.
pub fn write_spec(bundle: &Path, spec: &mut OverlaySpec) -> Result<(), String> {
    spec.version = 1;
    let data =
        serde_json::to_vec_pretty(spec).map_err(|e| format!("while encoding overlay spec: {e}"))?;
    crate::atomicfile::write(&bundle.join(OVERLAY_SPEC_FILE_NAME), &data, 0o644)
        .map_err(|e| format!("while writing overlay spec: {e}"))
}

/// The capabilities a container runs with: the defaults, minus what it
/// drops (all of them for "ALL"), plus what it adds, as sorted `CAP_` names.
pub fn resolve_capabilities(caps: Option<&pb::Capabilities>) -> Vec<String> {
    let mut eff: BTreeSet<String> = DEFAULT_CAPABILITIES.iter().map(|c| c.to_string()).collect();
    if let Some(caps) = caps {
        for d in &caps.drop {
            if d == CAPABILITY_ALL {
                eff.clear();
                break;
            }
            eff.remove(d);
        }
        for a in &caps.add {
            eff.insert(a.clone());
        }
    }
    eff.into_iter().map(|c| format!("CAP_{c}")).collect()
}

/// The container's environment: its own entries first, then the image's,
/// then a default PATH; the first entry for a key wins.
pub fn resolve_actor_env(image: Option<&ImageConfig>, template_env: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let image_env = image.map(|i| i.env.as_slice()).unwrap_or(&[]);
    for e in template_env
        .iter()
        .chain(image_env)
        .map(|s| s.as_str())
        .chain([DEFAULT_PATH])
    {
        let key = e.split('=').next().unwrap_or("");
        if key.is_empty() || !seen.insert(key.to_string()) {
            continue;
        }
        out.push(e.to_string());
    }
    out
}

/// The container's argv: its command replaces the image's entrypoint (and
/// drops the image's command), its args replace the command.
pub fn resolve_process_args(
    image: Option<&ImageConfig>,
    command: &[String],
    args: &[String],
) -> Result<Vec<String>, String> {
    let (mut entrypoint, mut cmd) = match image {
        Some(i) => (i.entrypoint.clone(), i.cmd.clone()),
        None => (Vec::new(), Vec::new()),
    };
    if !command.is_empty() {
        entrypoint = command.to_vec();
        cmd.clear();
    }
    if !args.is_empty() {
        cmd = args.to_vec();
    }
    let argv: Vec<String> = entrypoint.into_iter().chain(cmd).collect();
    if argv.is_empty() {
        return Err("no command specified: image defines neither ENTRYPOINT nor CMD and the container sets neither command nor args".into());
    }
    Ok(argv)
}

/// Prepares one container's bundle: a fresh bundle dir with empty rootfs,
/// upper and work dirs, the image and image volumes pulled, and the overlay
/// spec written. Returns the image config.
pub async fn prepare_oci_directory(
    ctx: &Ctx,
    images: &dyn ImageCache,
    paths: &NodePaths,
    actor_uid: &str,
    container: &pb::Container,
    volumes: &[pb::Volume],
) -> Result<ImageConfig, Status> {
    let bundle = paths.oci_bundle_path(actor_uid, &container.name);
    crate::actordirs::remove_all_writable(&bundle).map_err(|e| {
        crate::status::fs_err(format!("while clearing bundle {}", bundle.display()), &e)
    })?;
    for d in ["rootfs", "upper", "work"] {
        crate::atomicfile::create_dir_all_mode(&bundle.join(d), 0o700)
            .map_err(|e| crate::status::fs_err("while creating container bundle dir", &e))?;
    }
    let mounted: HashSet<&str> = container
        .volume_mounts
        .iter()
        .map(|m| m.name.as_str())
        .collect();
    let wanted: Vec<&pb::Volume> = volumes
        .iter()
        .filter(|v| v.image.is_some() && mounted.contains(v.name.as_str()))
        .collect();
    let image_fut = images.ensure_image(ctx, &container.image);
    let vols_fut = futures::future::try_join_all(wanted.iter().map(|v| async move {
        let r = &v.image.as_ref().expect("filtered").reference;
        let img = images.ensure_image(ctx, r).await.map_err(|e| {
            crate::status::wrap(format!("pulling image {r:?} for volume {:?}", v.name), e)
        })?;
        Ok::<_, Status>(ImageVolumeOverlay {
            name: v.name.clone(),
            image_digest: img.digest,
            layers: img.layer_dirs,
        })
    }));
    let (img, image_volumes) = futures::future::try_join(
        async {
            image_fut
                .await
                .map_err(|e| crate::status::wrap(format!("pulling image {:?}", container.image), e))
        },
        vols_fut,
    )
    .await?;
    let mut spec = OverlaySpec {
        version: 1,
        image_digest: img.digest.clone(),
        layers: img.layer_dirs.clone(),
        extra_dirs: container
            .volume_mounts
            .iter()
            .map(|m| m.mount_path.clone())
            .collect(),
        image_volumes,
    };
    write_spec(&bundle, &mut spec).map_err(Status::internal)?;
    Ok(img.config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn capabilities() {
        assert_eq!(
            resolve_capabilities(None),
            vec!["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"]
        );
        let c = pb::Capabilities {
            add: vec!["SYS_PTRACE".into()],
            drop: vec!["KILL".into()],
        };
        assert_eq!(
            resolve_capabilities(Some(&c)),
            vec!["CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE", "CAP_SYS_PTRACE"]
        );
        let all = pb::Capabilities {
            add: vec!["NET_ADMIN".into()],
            drop: vec!["ALL".into()],
        };
        assert_eq!(resolve_capabilities(Some(&all)), vec!["CAP_NET_ADMIN"]);
    }

    #[test]
    fn env_precedence() {
        let img = ImageConfig {
            env: vec!["A=image".into(), "B=image".into(), "PATH=/img".into()],
            ..Default::default()
        };
        let out = resolve_actor_env(Some(&img), &["A=tmpl".into(), "=bad".into()]);
        assert_eq!(out, vec!["A=tmpl", "B=image", "PATH=/img"]);
        let out = resolve_actor_env(None, &[]);
        assert_eq!(out, vec![DEFAULT_PATH]);
    }

    #[test]
    fn process_args() {
        let img = ImageConfig {
            entrypoint: vec!["/bin/server".into()],
            cmd: vec!["--port=80".into()],
            ..Default::default()
        };
        assert_eq!(
            resolve_process_args(Some(&img), &[], &[]).unwrap(),
            vec!["/bin/server", "--port=80"]
        );
        assert_eq!(
            resolve_process_args(Some(&img), &[], &["--port=90".into()]).unwrap(),
            vec!["/bin/server", "--port=90"]
        );
        assert_eq!(
            resolve_process_args(Some(&img), &["/bin/sh".into()], &[]).unwrap(),
            vec!["/bin/sh"]
        );
        assert!(resolve_process_args(None, &[], &[]).is_err());
    }

    struct Fake;

    #[async_trait]
    impl ImageCache for Fake {
        async fn ensure_image(&self, _: &Ctx, r: &str) -> Result<Image, Status> {
            Ok(Image {
                digest: format!("sha256:{r}"),
                config: ImageConfig {
                    cmd: vec!["run".into()],
                    ..Default::default()
                },
                layer_dirs: vec![format!("/cache/{r}")],
            })
        }
    }

    #[tokio::test]
    async fn prepares_bundle_with_image_volumes() {
        let d = tempfile::tempdir().unwrap();
        let paths = NodePaths::new(d.path());
        let ctr = pb::Container {
            name: "app".into(),
            image: "app-image".into(),
            volume_mounts: vec![
                pb::VolumeMount {
                    name: "models".into(),
                    mount_path: "/models".into(),
                },
                pb::VolumeMount {
                    name: "data".into(),
                    mount_path: "/data".into(),
                },
            ],
            ..Default::default()
        };
        let vols = vec![
            pb::Volume {
                name: "models".into(),
                image: Some(pb::ImageVolumeSource {
                    reference: "model-image".into(),
                }),
                ..Default::default()
            },
            pb::Volume {
                name: "unmounted".into(),
                image: Some(pb::ImageVolumeSource {
                    reference: "other".into(),
                }),
                ..Default::default()
            },
        ];
        let cfg = prepare_oci_directory(&Ctx::background(), &Fake, &paths, "u", &ctr, &vols)
            .await
            .unwrap();
        assert_eq!(cfg.cmd, vec!["run"]);
        let bundle = paths.oci_bundle_path("u", "app");
        assert!(
            bundle.join("rootfs").is_dir()
                && bundle.join("upper").is_dir()
                && bundle.join("work").is_dir()
        );
        let spec: OverlaySpec =
            serde_json::from_slice(&std::fs::read(bundle.join(OVERLAY_SPEC_FILE_NAME)).unwrap())
                .unwrap();
        assert_eq!(spec.version, 1);
        assert_eq!(spec.image_digest, "sha256:app-image");
        assert_eq!(spec.extra_dirs, vec!["/models", "/data"]);
        assert_eq!(spec.image_volumes.len(), 1);
        assert_eq!(spec.image_volumes[0].name, "models");
        assert!(NoImageCache
            .ensure_image(&Ctx::background(), "x")
            .await
            .is_err());
    }
}

#[cfg(test)]
#[path = "oci_tests.rs"]
pub(crate) mod behavior_tests;
