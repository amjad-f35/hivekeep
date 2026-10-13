//! The sandbox assets record, which doubles as the snapshot manifest.
//!
//! An actor's record names the sandbox runtime it runs: its class and its
//! content-addressed binaries for this node's architecture. Run and Restore
//! write it; Checkpoint reads it back (the request no longer carries the
//! runtime) and writes it, extended with the snapshot's files and identity,
//! beside the checkpoint images as the manifest that makes a snapshot
//! self-describing.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufReader, Read};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::atomicfile::{self, chmod, create_dir_all_mode};
use crate::context::Ctx;
use crate::paths::NodePaths;
use crate::status::fs_err;

/// The manifest's object and file name.
pub const MANIFEST_NAME: &str = "manifest.json";

/// The gVisor release asset: a tarball holding `runsc` and its helpers,
/// extracted into a directory rather than kept as one file.
pub const GVISOR_ASSET_NAME: &str = "gvisor";

/// The largest asset accepted, guarding disk against an unbounded URL.
pub const MAX_ASSET_BYTES: i64 = 8 << 30;

/// One content-addressed asset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct AssetEntry {
    /// Where to fetch it.
    pub url: String,
    /// Its hex sha256.
    pub sha256: String,
}

impl AssetEntry {
    /// The URL as scheme://host/path, for logs and errors.
    pub fn display_url(&self) -> String {
        redact_url_str(&self.url)
    }
}

/// An actor's sandbox runtime, and once checkpointed, its snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SandboxAssetsRecord {
    /// The sandbox class.
    #[serde(default)]
    pub sandbox_class: String,
    /// The binaries, by asset name.
    #[serde(default)]
    pub assets: BTreeMap<String, AssetEntry>,
    /// The actor's hive; set in a manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hive: String,
    /// The actor's name; set in a manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_name: String,
    /// The actor's UID; set in a manifest.
    #[serde(default, rename = "actorUid", skip_serializing_if = "String::is_empty")]
    pub actor_uid: String,
    /// The template's hive; set in a manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_template_hive: String,
    /// The template's name; set in a manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_template_name: String,
    /// The checkpoint's files as the worker reported them, so a restore
    /// moves exactly that set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_files: Vec<String>,
    /// The subset of `snapshot_files` that restores the actor at VOLUMES
    /// fidelity on its own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_snapshot_files: Vec<String>,
    /// The fidelity the checkpoint captured, as its label.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fidelity: String,
    /// The node's boot ID when this local copy was written. Not every file
    /// of a local copy is synced, so one from an earlier boot may be torn.
    /// Never set in an uploaded manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub boot_id: String,
}

impl SandboxAssetsRecord {
    /// Parses and validates a record or manifest.
    pub fn parse(data: &[u8]) -> Result<Self, Status> {
        let rec: SandboxAssetsRecord = serde_json::from_slice(data)
            .map_err(|e| Status::internal(format!("while parsing sandbox record/manifest: {e}")))?;
        validate_snapshot_files(&rec.snapshot_files).map_err(|e| {
            Status::internal(format!(
                "sandbox record/manifest has invalid snapshotFiles: {e}"
            ))
        })?;
        validate_data_snapshot_files(&rec.snapshot_files, &rec.data_snapshot_files).map_err(
            |e| {
                Status::internal(format!(
                    "sandbox record/manifest has invalid dataSnapshotFiles: {e}"
                ))
            },
        )?;
        Ok(rec)
    }

    /// The record as JSON.
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("records always serialize")
    }

    /// The hash that names this runtime: every asset's name and digest.
    pub fn bundle_hash(&self) -> String {
        let mut lines: Vec<String> = self
            .assets
            .iter()
            .map(|(name, a)| format!("asset/{name}={}", a.sha256))
            .collect();
        lines.sort();
        format!(
            "sha256:{}",
            hex::encode(Sha256::digest(lines.join("\n").as_bytes()))
        )
    }
}

/// Requires distinct plain file names in the checkpoint directory, none of
/// them the manifest's.
pub fn validate_snapshot_files(files: &[String]) -> Result<(), String> {
    hivekeep_core::snapshot::validate_snapshot_file_names(files).map_err(|e| e.to_string())?;
    for (i, name) in files.iter().enumerate() {
        if name == MANIFEST_NAME {
            return Err(format!(
                "snapshotFiles[{i}] {name:?} is reserved for the snapshot manifest"
            ));
        }
    }
    Ok(())
}

/// Requires every data file to be one of the snapshot files.
pub fn validate_data_snapshot_files(files: &[String], data_files: &[String]) -> Result<(), String> {
    for name in data_files {
        if !files.contains(name) {
            return Err(format!(
                "data snapshot file {name:?} is not one of the snapshot files"
            ));
        }
    }
    Ok(())
}

/// Persists the actor's running sandbox runtime on the node.
pub fn write_sandbox_record(
    paths: &NodePaths,
    actor_uid: &str,
    rec: &SandboxAssetsRecord,
) -> Result<(), Status> {
    let path = paths.actor_sandbox_assets_file(actor_uid);
    if let Some(dir) = path.parent() {
        create_dir_all_mode(dir, 0o700).map_err(|e| fs_err("while creating actor dir", &e))?;
    }
    atomicfile::write(&path, &rec.to_json(), 0o600)
        .map_err(|e| fs_err("while writing sandbox record", &e))
}

/// Loads the actor's on-node sandbox record. A missing record is NotFound.
pub fn read_sandbox_record(
    paths: &NodePaths,
    actor_uid: &str,
) -> Result<SandboxAssetsRecord, Status> {
    let data = fs::read(paths.actor_sandbox_assets_file(actor_uid))
        .map_err(|e| fs_err("while reading sandbox record", &e))?;
    SandboxAssetsRecord::parse(&data)
}

/// Reads the manifest in a snapshot directory without following a symlink
/// at its name.
pub fn read_snapshot_manifest(dir: &Path) -> io::Result<Vec<u8>> {
    let path = dir.join(MANIFEST_NAME);
    let meta = fs::symlink_metadata(&path)?;
    if !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest is not a regular file",
        ));
    }
    fs::read(path)
}

/// Whether `name` is one path element naming a file under the assets dir.
pub fn plain_asset_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\\')
}

/// The local `runsc` among staged asset paths, or "" when the runtime has
/// none.
pub fn runsc_path_for(paths: &BTreeMap<String, String>) -> String {
    paths.get(GVISOR_ASSET_NAME).cloned().unwrap_or_default()
}

/// scheme://host/path of a URL, dropping user info and query, which may
/// carry credentials, for logs and errors.
pub fn redact_url(u: &url::Url) -> String {
    let mut out = format!("{}://", u.scheme());
    if let Some(h) = u.host_str() {
        out.push_str(h);
    }
    if let Some(p) = u.port() {
        out.push_str(&format!(":{p}"));
    }
    out.push_str(u.path());
    if out.ends_with("://") {
        out.push('/');
    }
    out
}

/// [`redact_url`] of an unparsed URL.
pub fn redact_url_str(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(u) => redact_url(&u),
        Err(_) => "<invalid URL>".into(),
    }
}

/// Decompresses and extracts the tarball at `tar_path` into `dest`, picking
/// the compression from the suffix of `url_path`. Only directories and
/// regular files are accepted, nothing may escape `dest`, and the inflated
/// total is capped.
pub fn extract_tar_archive(
    ctx: &Ctx,
    tar_path: &Path,
    url_path: &str,
    dest: &Path,
) -> Result<(), Status> {
    enum Fmt {
        Gz,
        Bz2,
        Zst,
        Tar,
    }
    let fmt = if url_path.ends_with(".tar.gz") || url_path.ends_with(".tgz") {
        Fmt::Gz
    } else if url_path.ends_with(".tar.bz2") || url_path.ends_with(".tbz2") {
        Fmt::Bz2
    } else if url_path.ends_with(".tar.zst") || url_path.ends_with(".tar.zstd") {
        Fmt::Zst
    } else if url_path.ends_with(".tar") {
        Fmt::Tar
    } else {
        return Err(Status::failed_precondition(format!(
            "unsupported archive format for URL {url_path} (must be .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.zst, .tar.zstd, or .tar)"
        )));
    };
    let f = fs::File::open(tar_path).map_err(|e| fs_err("while opening downloaded tarball", &e))?;
    let buf = BufReader::new(f);
    let reader: Box<dyn Read> = match fmt {
        Fmt::Gz => Box::new(flate2::read::GzDecoder::new(buf)),
        Fmt::Bz2 => Box::new(bzip2::read::BzDecoder::new(buf)),
        Fmt::Zst => Box::new(zstd::stream::read::Decoder::with_buffer(buf).map_err(|e| {
            Status::internal(format!("failed to create zstd reader for {url_path}: {e}"))
        })?),
        Fmt::Tar => Box::new(buf),
    };
    let mut archive = tar::Archive::new(reader);
    let mut total: i64 = 0;
    let entries = archive
        .entries()
        .map_err(|e| Status::internal(format!("while reading tarball: {e}")))?;
    for entry in entries {
        if let Some(e) = ctx.err() {
            return Err(Status::from(e));
        }
        let mut entry =
            entry.map_err(|e| Status::internal(format!("while reading tarball: {e}")))?;
        let raw = entry
            .path()
            .map_err(|e| Status::internal(format!("while reading tarball: {e}")))?
            .into_owned();
        let name = crate::paths::lexical_clean(&raw);
        if name.as_os_str().is_empty() {
            continue;
        }
        if raw.is_absolute()
            || raw
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(Status::failed_precondition(format!(
                "tarball entry {:?} escapes the extraction dir",
                raw
            )));
        }
        let out = dest.join(&name);
        let mode = entry.header().mode().unwrap_or(0o644) & 0o777;
        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                create_dir_all_mode(&out, mode | 0o700)
                    .map_err(|e| fs_err("while creating tarball dir", &e))?;
            }
            tar::EntryType::Regular | tar::EntryType::Continuous => {
                total = total.saturating_add(entry.header().size().unwrap_or(0) as i64);
                if total > MAX_ASSET_BYTES {
                    return Err(Status::failed_precondition(format!(
                        "tarball inflates past the {MAX_ASSET_BYTES}-byte cap"
                    )));
                }
                if let Some(parent) = out.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|e| fs_err("while creating tarball parent dir", &e))?;
                }
                let mut file = fs::File::create(&out)
                    .map_err(|e| fs_err("while creating tarball file", &e))?;
                io::copy(&mut entry, &mut file)
                    .map_err(|e| fs_err("while extracting tarball file", &e))?;
                drop(file);
                chmod(&out, mode).map_err(|e| fs_err("while setting tarball file mode", &e))?;
            }
            other => {
                return Err(Status::failed_precondition(format!(
                    "tarball entry {:?} has unsupported type {:?}",
                    raw, other
                )))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> SandboxAssetsRecord {
        let mut assets = BTreeMap::new();
        assets.insert(
            "gvisor".to_string(),
            AssetEntry {
                url: "gs://gvisor/releases/runsc.tar.zst".into(),
                sha256: "ab".repeat(32),
            },
        );
        SandboxAssetsRecord {
            sandbox_class: "gvisor".into(),
            assets,
            ..Default::default()
        }
    }

    #[test]
    fn manifest_round_trips_with_camel_case_names() {
        let mut r = rec();
        r.actor_uid = "u1".into();
        r.snapshot_files = vec!["mem".into(), "fs/img".into()];
        r.data_snapshot_files = vec!["fs/img".into()];
        r.fidelity = "memory".into();
        let json = String::from_utf8(r.to_json()).unwrap();
        assert!(json.contains("\"sandboxClass\":\"gvisor\""));
        assert!(json.contains("\"actorUid\":\"u1\""));
        assert!(json.contains("\"dataSnapshotFiles\""));
        assert!(!json.contains("bootId"));
        assert_eq!(SandboxAssetsRecord::parse(json.as_bytes()).unwrap(), r);
    }

    #[test]
    fn parse_rejects_bad_file_lists() {
        let mut r = rec();
        r.snapshot_files = vec![MANIFEST_NAME.into()];
        assert!(SandboxAssetsRecord::parse(&r.to_json()).is_err());
        let mut r = rec();
        r.snapshot_files = vec!["a".into()];
        r.data_snapshot_files = vec!["b".into()];
        assert!(SandboxAssetsRecord::parse(&r.to_json()).is_err());
        let mut r = rec();
        r.snapshot_files = vec!["../escape".into()];
        assert!(SandboxAssetsRecord::parse(&r.to_json()).is_err());
    }

    #[test]
    fn bundle_hash_covers_names_and_digests_only() {
        let a = rec();
        let mut b = rec();
        b.assets.get_mut("gvisor").unwrap().url = "https://mirror/runsc.tar.zst".into();
        assert_eq!(a.bundle_hash(), b.bundle_hash());
        b.assets.get_mut("gvisor").unwrap().sha256 = "cd".repeat(32);
        assert_ne!(a.bundle_hash(), b.bundle_hash());
        assert!(a.bundle_hash().starts_with("sha256:"));
    }

    #[test]
    fn redaction_drops_query_and_userinfo() {
        assert_eq!(
            redact_url_str("https://user:pw@host:8443/a/b?sig=secret"),
            "https://host:8443/a/b"
        );
        assert_eq!(redact_url_str("not a url"), "<invalid URL>");
    }

    #[test]
    fn plain_names() {
        assert!(plain_asset_name("kernel"));
        for bad in ["", ".", "..", "a/b"] {
            assert!(!plain_asset_name(bad), "{bad}");
        }
    }

    fn tarball(dir: &Path, entries: &[(&str, &[u8])]) -> std::path::PathBuf {
        let p = dir.join("t.tar.gz");
        let f = fs::File::create(&p).unwrap();
        let gz = flate2::write::GzEncoder::new(f, flate2::Compression::fast());
        let mut b = tar::Builder::new(gz);
        for (name, data) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o755);
            h.set_entry_type(tar::EntryType::Regular);
            b.append_data(&mut h, name, *data).unwrap();
        }
        b.into_inner().unwrap().finish().unwrap();
        p
    }

    #[test]
    fn extracts_gzip_tarball() {
        let d = tempfile::tempdir().unwrap();
        let t = tarball(d.path(), &[("runsc", b"bin"), ("gvisor-bin/helper", b"h")]);
        let out = d.path().join("out");
        fs::create_dir(&out).unwrap();
        extract_tar_archive(&Ctx::background(), &t, "https://x/r.tar.gz", &out).unwrap();
        assert_eq!(fs::read(out.join("runsc")).unwrap(), b"bin");
        assert_eq!(fs::read(out.join("gvisor-bin/helper")).unwrap(), b"h");
    }

    #[test]
    fn rejects_unknown_suffix() {
        let d = tempfile::tempdir().unwrap();
        let t = tarball(d.path(), &[("runsc", b"bin")]);
        assert!(extract_tar_archive(&Ctx::background(), &t, "https://x/r.zip", d.path()).is_err());
    }

    #[test]
    fn record_file_round_trip() {
        let d = tempfile::tempdir().unwrap();
        let p = NodePaths::new(d.path());
        assert!(crate::status::is_not_found(
            &read_sandbox_record(&p, "u").unwrap_err()
        ));
        write_sandbox_record(&p, "u", &rec()).unwrap();
        assert_eq!(read_sandbox_record(&p, "u").unwrap(), rec());
    }
}

#[cfg(test)]
#[path = "assets_manifest_tests.rs"]
mod manifest_behavior_tests;
