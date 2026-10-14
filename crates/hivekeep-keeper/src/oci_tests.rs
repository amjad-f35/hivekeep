use std::collections::HashMap;
use std::path::PathBuf;

use parking_lot::Mutex;

use super::*;
use async_trait::async_trait;

const DEFAULT_PATH_ENV: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn resolve_actor_env_cases() {
    let img = |env: &[&str]| ImageConfig {
        env: strings(env),
        ..Default::default()
    };
    type Case<'a> = (&'a str, Option<ImageConfig>, Vec<String>, Vec<String>);
    let cases: Vec<Case> = vec![
        (
            "template overrides image by key",
            Some(img(&["FOO=image"])),
            strings(&["FOO=template"]),
            strings(&["FOO=template", DEFAULT_PATH_ENV]),
        ),
        (
            "default PATH applies when neither sets it",
            Some(img(&["FOO=image"])),
            strings(&["BAR=template"]),
            strings(&["BAR=template", "FOO=image", DEFAULT_PATH_ENV]),
        ),
        (
            "image PATH overrides default",
            Some(img(&["PATH=/image/bin"])),
            vec![],
            strings(&["PATH=/image/bin"]),
        ),
        (
            "template PATH overrides default",
            Some(img(&[])),
            strings(&["PATH=/template/bin"]),
            strings(&["PATH=/template/bin"]),
        ),
        (
            "blank and keyless entries are dropped",
            Some(img(&["", "=novalue"])),
            vec![],
            strings(&[DEFAULT_PATH_ENV]),
        ),
        (
            "no image config uses template env and default PATH",
            None,
            strings(&["FOO=template"]),
            strings(&["FOO=template", DEFAULT_PATH_ENV]),
        ),
    ];
    for (name, image, tmpl, want) in cases {
        assert_eq!(resolve_actor_env(image.as_ref(), &tmpl), want, "{name}");
    }
}

#[test]
fn resolve_process_args_cases() {
    let cfg = |entrypoint: &[&str], cmd: &[&str]| ImageConfig {
        entrypoint: strings(entrypoint),
        cmd: strings(cmd),
        ..Default::default()
    };
    type Case<'a> = (
        &'a str,
        Option<ImageConfig>,
        &'a [&'a str],
        &'a [&'a str],
        Option<&'a [&'a str]>,
    );
    let cases: Vec<Case> = vec![
        (
            "image ENTRYPOINT+CMD used when neither is overridden",
            Some(cfg(&["/app"], &["--serve"])),
            &[],
            &[],
            Some(&["/app", "--serve"]),
        ),
        (
            "args override CMD, image ENTRYPOINT kept",
            Some(cfg(&["/init", "/wrapper.sh"], &[])),
            &[],
            &["serve"],
            Some(&["/init", "/wrapper.sh", "serve"]),
        ),
        (
            "command overrides both ENTRYPOINT and CMD",
            Some(cfg(&["/app"], &["--serve"])),
            &["/other"],
            &[],
            Some(&["/other"]),
        ),
        (
            "command and args override both",
            Some(cfg(&["/app"], &["--serve"])),
            &["/other"],
            &["--flag"],
            Some(&["/other", "--flag"]),
        ),
        (
            "image ENTRYPOINT only, no CMD",
            Some(cfg(&["/app/counter"], &[])),
            &[],
            &[],
            Some(&["/app/counter"]),
        ),
        (
            "no image config, command supplies argv",
            None,
            &["/pause"],
            &[],
            Some(&["/pause"]),
        ),
        (
            "empty argv is an error",
            Some(cfg(&[], &[])),
            &[],
            &[],
            None,
        ),
        (
            "no image and no overrides is an error",
            None,
            &[],
            &[],
            None,
        ),
    ];
    for (name, image, command, args, want) in cases {
        let got = resolve_process_args(image.as_ref(), &strings(command), &strings(args));
        match want {
            Some(w) => assert_eq!(got.unwrap(), strings(w), "{name}"),
            None => assert!(got.is_err(), "{name}: {got:?}"),
        }
    }
}

/// The set a container gets when it asks for no adjustment, spelled out so
/// widening or narrowing the default is a deliberate test change.
const WANT_DEFAULT_CAPABILITIES: [&str; 3] =
    ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"];

#[test]
fn resolve_capabilities_cases() {
    let caps = |add: &[&str], drop: &[&str]| pb::Capabilities {
        add: strings(add),
        drop: strings(drop),
    };
    let default = strings(&WANT_DEFAULT_CAPABILITIES);
    let cases: Vec<(&str, Option<pb::Capabilities>, Vec<String>)> = vec![
        ("unset keeps the default set", None, default.clone()),
        (
            "empty keeps the default set",
            Some(caps(&[], &[])),
            default.clone(),
        ),
        (
            "drop removes from the default set",
            Some(caps(&[], &["NET_BIND_SERVICE", "AUDIT_WRITE"])),
            strings(&["CAP_KILL"]),
        ),
        (
            "add grants on top of the default set",
            Some(caps(&["SYS_ADMIN"], &[])),
            strings(&[
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE",
                "CAP_SYS_ADMIN",
            ]),
        ),
        (
            "drop ALL clears the default set",
            Some(caps(&[], &["ALL"])),
            vec![],
        ),
        (
            "drop ALL with add gives an exact set",
            Some(caps(&["NET_ADMIN", "CHOWN"], &["ALL"])),
            strings(&["CAP_CHOWN", "CAP_NET_ADMIN"]),
        ),
        (
            // Drop applies first, so naming a capability in both grants it.
            "add wins over drop",
            Some(caps(&["KILL"], &["KILL"])),
            default.clone(),
        ),
        (
            "adding a default capability does not duplicate it",
            Some(caps(&["KILL"], &[])),
            default.clone(),
        ),
        (
            "dropping a capability outside the default set is a no-op",
            Some(caps(&[], &["SYS_ADMIN"])),
            default.clone(),
        ),
    ];
    for (name, c, want) in cases {
        assert_eq!(resolve_capabilities(c.as_ref()), want, "{name}");
    }
}

/// An image cache over a directory: each known reference has layers, each
/// layer a directory holding one file under `fs/`. Records every pull.
pub(crate) struct DirImageCache {
    root: PathBuf,
    images: HashMap<String, (ImageConfig, Vec<String>)>,
    pub(crate) pulled: Mutex<Vec<String>>,
}

impl DirImageCache {
    pub(crate) fn new(root: &Path) -> Self {
        DirImageCache {
            root: root.to_path_buf(),
            images: HashMap::new(),
            pulled: Mutex::new(Vec::new()),
        }
    }

    /// Adds `reference` with one layer per file name, bottom-most first.
    pub(crate) fn push(&mut self, reference: &str, config: ImageConfig, files: &[&str]) {
        self.images.insert(
            reference.to_string(),
            (config, files.iter().map(|f| f.to_string()).collect()),
        );
    }
}

#[async_trait]
impl ImageCache for DirImageCache {
    async fn ensure_image(&self, _: &Ctx, reference: &str) -> Result<Image, Status> {
        self.pulled.lock().push(reference.to_string());
        let Some((config, files)) = self.images.get(reference) else {
            return Err(Status::not_found(format!("no image {reference}")));
        };
        let digest = format!(
            "sha256:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(reference.as_bytes()))
        );
        let mut layers = Vec::new();
        for (i, f) in files.iter().enumerate() {
            let dir = self.root.join(format!("{}-{i}", &digest[7..19]));
            let file = dir.join("fs").join(f);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(&file, f.as_bytes()).unwrap();
            layers.push(dir.to_string_lossy().into_owned());
        }
        Ok(Image {
            digest,
            config: config.clone(),
            layer_dirs: layers,
        })
    }
}

/// Prepares one container mounting `mounts` of `volumes`, and returns the
/// overlay spec written.
async fn image_volumes_of(
    cache: &DirImageCache,
    volumes: Vec<pb::Volume>,
    mounts: Vec<pb::VolumeMount>,
) -> OverlaySpec {
    let d = tempfile::tempdir().unwrap();
    let paths = NodePaths::new(d.path());
    let ctr = pb::Container {
        name: "app".into(),
        image: "app-image".into(),
        volume_mounts: mounts,
        ..Default::default()
    };
    prepare_oci_directory(&Ctx::background(), cache, &paths, "u", &ctr, &volumes)
        .await
        .unwrap();
    let bundle = paths.oci_bundle_path("u", "app");
    serde_json::from_slice(&std::fs::read(bundle.join(OVERLAY_SPEC_FILE_NAME)).unwrap()).unwrap()
}

fn image_volume(name: &str, reference: &str) -> pb::Volume {
    pb::Volume {
        name: name.into(),
        image: Some(pb::ImageVolumeSource {
            reference: reference.into(),
        }),
        ..Default::default()
    }
}

fn mount(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

/// A mounted image volume records its layers for the worker to compose,
/// and its digest so the cache's GC can protect them.
#[tokio::test]
async fn image_volumes_record_layers_and_digest() {
    let d = tempfile::tempdir().unwrap();
    let mut cache = DirImageCache::new(d.path());
    cache.push("app-image", ImageConfig::default(), &["app"]);
    cache.push(
        "registry.test/agent:v1",
        ImageConfig::default(),
        &["payload-binary"],
    );
    let spec = image_volumes_of(
        &cache,
        vec![image_volume("agent", "registry.test/agent:v1")],
        vec![mount("agent", "/hk")],
    )
    .await;
    assert_eq!(spec.image_volumes.len(), 1);
    let v = &spec.image_volumes[0];
    assert_eq!(v.name, "agent");
    assert_eq!(v.layers.len(), 1);
    assert!(v.image_digest.starts_with("sha256:"), "{}", v.image_digest);
    // The recorded path is a layer directory; the file is under its fs/.
    assert!(Path::new(&v.layers[0]).join("fs/payload-binary").is_file());
}

/// A multi-layer image volume is one entry with its layers bottom-most
/// first.
#[tokio::test]
async fn image_volumes_multi_layer() {
    let d = tempfile::tempdir().unwrap();
    let mut cache = DirImageCache::new(d.path());
    cache.push("app-image", ImageConfig::default(), &["app"]);
    cache.push(
        "registry.test/agent:multi",
        ImageConfig::default(),
        &["base", "payload-binary"],
    );
    let spec = image_volumes_of(
        &cache,
        vec![image_volume("agent", "registry.test/agent:multi")],
        vec![mount("agent", "/hk")],
    )
    .await;
    assert_eq!(spec.image_volumes.len(), 1);
    let layers = &spec.image_volumes[0].layers;
    assert_eq!(layers.len(), 2);
    for (i, want) in ["base", "payload-binary"].iter().enumerate() {
        assert!(
            Path::new(&layers[i]).join("fs").join(want).is_file(),
            "layer {i}"
        );
    }
}

/// An image volume no container mounts is never pulled, so a bad
/// reference on an unused volume cannot fail the actor.
#[tokio::test]
async fn image_volumes_unmounted_volume_not_pulled() {
    let d = tempfile::tempdir().unwrap();
    let mut cache = DirImageCache::new(d.path());
    cache.push("app-image", ImageConfig::default(), &["app"]);
    let spec = image_volumes_of(
        &cache,
        vec![image_volume("agent", "127.0.0.1:1/nope@sha256:abc")],
        vec![],
    )
    .await;
    assert!(spec.image_volumes.is_empty());
    assert_eq!(*cache.pulled.lock(), vec!["app-image".to_string()]);
}
