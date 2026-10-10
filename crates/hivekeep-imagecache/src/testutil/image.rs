//! Test fixtures: hand-built tar archives (including the hostile names a
//! real writer refuses to produce), in-memory layers, and image documents.

use std::io::{Cursor, Write};
use std::time::SystemTime;

use async_trait::async_trait;
use hivekeep_node::context::Ctx;
use tar::{EntryType, Header};

use crate::digest::Digest;
use crate::error::Result;
use crate::layer::{decompress, Layer, LayerReader, VerifyingReader};
use crate::manifest::{
    Compression, ConfigFile, Descriptor, ImageConfig, Manifest, RootFs, MEDIA_DOCKER_LAYER_GZIP,
    MEDIA_DOCKER_MANIFEST, MEDIA_OCI_CONFIG,
};

/// One tar entry to write.
#[derive(Debug, Clone)]
pub struct TarEntry {
    pub name: String,
    pub kind: EntryType,
    pub mode: u32,
    pub body: Vec<u8>,
    pub linkname: String,
    pub mtime: Option<SystemTime>,
    pub devmajor: u32,
    pub devminor: u32,
    pub uid: u64,
    pub gid: u64,
    pub pax: Vec<(String, Vec<u8>)>,
}

impl TarEntry {
    pub fn new(name: &str, kind: EntryType) -> Self {
        TarEntry {
            name: name.to_string(),
            kind,
            mode: 0,
            body: Vec::new(),
            linkname: String::new(),
            mtime: None,
            devmajor: 0,
            devminor: 0,
            uid: 0,
            gid: 0,
            pax: Vec::new(),
        }
    }
    pub fn file(name: &str, body: &str) -> Self {
        TarEntry {
            body: body.as_bytes().to_vec(),
            ..Self::new(name, EntryType::Regular)
        }
    }
    pub fn dir(name: &str) -> Self {
        Self::new(name, EntryType::Directory)
    }
    pub fn symlink(name: &str, target: &str) -> Self {
        TarEntry {
            linkname: target.to_string(),
            ..Self::new(name, EntryType::Symlink)
        }
    }
    pub fn hardlink(name: &str, target: &str) -> Self {
        TarEntry {
            linkname: target.to_string(),
            ..Self::new(name, EntryType::Link)
        }
    }
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    pub fn mtime(mut self, t: SystemTime) -> Self {
        self.mtime = Some(t);
        self
    }
    pub fn dev(mut self, major: u32, minor: u32) -> Self {
        self.devmajor = major;
        self.devminor = minor;
        self
    }
}

fn default_mode(kind: EntryType) -> u32 {
    match kind {
        EntryType::Directory => 0o755,
        EntryType::Symlink => 0o777,
        _ => 0o644,
    }
}

fn put(dst: &mut [u8], s: &[u8]) {
    assert!(
        s.len() <= dst.len(),
        "tar name too long for a test header: {}",
        String::from_utf8_lossy(s)
    );
    dst.fill(0);
    dst[..s.len()].copy_from_slice(s);
}

/// Builds a tar archive from `entries`, writing names verbatim.
pub fn build_tar(entries: &[TarEntry]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for e in entries {
        if !e.pax.is_empty() {
            b.append_pax_extensions(e.pax.iter().map(|(k, v)| (k.as_str(), v.as_slice())))
                .unwrap();
        }
        let mut h = Header::new_ustar();
        h.set_entry_type(e.kind);
        h.set_mode(if e.mode == 0 {
            default_mode(e.kind)
        } else {
            e.mode
        });
        h.set_size(e.body.len() as u64);
        h.set_uid(e.uid);
        h.set_gid(e.gid);
        let mtime = e
            .mtime
            .map(|t| t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs())
            .unwrap_or(0);
        h.set_mtime(mtime);
        h.set_device_major(e.devmajor).unwrap();
        h.set_device_minor(e.devminor).unwrap();
        put(&mut h.as_old_mut().name, e.name.as_bytes());
        put(&mut h.as_old_mut().linkname, e.linkname.as_bytes());
        h.set_cksum();
        b.append(&h, e.body.as_slice()).unwrap();
    }
    b.into_inner().unwrap()
}

/// gzip-compresses `data`.
pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

/// A layer held in memory, gzip-compressed like a pushed layer.
#[derive(Debug, Clone)]
pub struct MemLayer {
    pub compressed: Vec<u8>,
    pub uncompressed_bytes: Vec<u8>,
    pub digest: Digest,
    pub diff_id: Digest,
}

impl MemLayer {
    /// A layer whose uncompressed stream is `raw` (not necessarily a tar).
    pub fn from_raw(raw: Vec<u8>) -> Self {
        let compressed = gzip(&raw);
        MemLayer {
            digest: Digest::of(&compressed),
            diff_id: Digest::of(&raw),
            compressed,
            uncompressed_bytes: raw,
        }
    }

    pub fn descriptor(&self) -> Descriptor {
        Descriptor {
            media_type: MEDIA_DOCKER_LAYER_GZIP.to_string(),
            size: self.compressed.len() as i64,
            digest: self.digest.clone(),
            platform: None,
        }
    }
}

#[async_trait]
impl Layer for MemLayer {
    fn digest(&self) -> Digest {
        self.digest.clone()
    }
    async fn uncompressed(&self, _: &Ctx) -> Result<LayerReader> {
        let raw: LayerReader = Box::new(Cursor::new(self.compressed.clone()));
        decompress(
            Box::new(VerifyingReader::new(raw, self.digest.clone())),
            Compression::Gzip,
        )
    }
}

/// A gzip layer of `entries`.
pub fn mem_layer(entries: &[TarEntry]) -> MemLayer {
    MemLayer::from_raw(build_tar(entries))
}

/// Two one-file layers holding different `lib` contents.
pub fn lib_layers() -> (MemLayer, MemLayer) {
    (
        mem_layer(&[TarEntry::file("lib", "good")]),
        mem_layer(&[TarEntry::file("lib", "EVIL")]),
    )
}

/// The documents of an image built from layers.
#[derive(Debug, Clone)]
pub struct BuiltImage {
    pub manifest: Vec<u8>,
    pub manifest_media_type: String,
    pub digest: Digest,
    pub config: Vec<u8>,
    pub layers: Vec<MemLayer>,
}

/// Builds an image of `layers` with runtime config `cfg`; `diff_ids`
/// overrides the config's diffID list.
pub fn build_image(
    cfg: ImageConfig,
    layers: &[MemLayer],
    diff_ids: Option<Vec<Digest>>,
) -> BuiltImage {
    let config = ConfigFile {
        architecture: crate::manifest::Platform::host_linux().architecture,
        os: "linux".to_string(),
        config: cfg,
        rootfs: RootFs {
            kind: "layers".to_string(),
            diff_ids: diff_ids
                .unwrap_or_else(|| layers.iter().map(|l| l.diff_id.clone()).collect()),
        },
    };
    let config = serde_json::to_vec(&config).unwrap();
    let config_digest = Digest::of(&config);
    let manifest = Manifest {
        schema_version: 2,
        media_type: MEDIA_DOCKER_MANIFEST.to_string(),
        config: Descriptor {
            media_type: MEDIA_OCI_CONFIG.to_string(),
            size: config.len() as i64,
            digest: config_digest.clone(),
            platform: None,
        },
        layers: layers.iter().map(|l| l.descriptor()).collect(),
    };
    let manifest = serde_json::to_vec(&manifest).unwrap();
    BuiltImage {
        digest: Digest::of(&manifest),
        manifest,
        manifest_media_type: MEDIA_DOCKER_MANIFEST.to_string(),
        config,
        layers: layers.to_vec(),
    }
}
