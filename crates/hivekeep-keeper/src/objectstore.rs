//! Moving snapshot files and sandbox assets between the node and object
//! storage.
//!
//! The keeper never talks to a storage backend itself: an [`ObjectStore`]
//! does, by file path, with the per-actor JWT that authorizes a snapshot
//! transfer. Its errors are gRPC statuses whose codes the keeper reads
//! (NotFound means no such object, Unavailable is retried); their messages
//! are never passed on, since they may quote a signed URL.
//!
//! [`LocalObjectStore`] keeps objects in a local directory, for development
//! and tests. Cloud backends plug in behind the same trait.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::context::Ctx;

pub use hivekeep_node::objectstore::{FetchAsset, FetchSnapshot, ObjectStore, UploadSnapshot};

/// An [`ObjectStore`] over a local directory: `scheme://bucket/path/name`
/// lives at `<root>/<scheme>/<bucket>/<path>/<name>`. The JWT is not
/// checked.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// A store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalObjectStore { root: root.into() }
    }

    /// The local directory holding the objects under `uri`.
    pub fn dir_for(&self, uri: &str) -> Result<PathBuf, Status> {
        let u =
            url::Url::parse(uri).map_err(|_| Status::invalid_argument("not a valid object URI"))?;
        let mut p = self.root.join(u.scheme());
        if let Some(h) = u.host_str() {
            p.push(h);
        }
        for seg in u.path().split('/').filter(|s| !s.is_empty()) {
            if seg == ".." || seg == "." {
                return Err(Status::invalid_argument("object URI escapes its bucket"));
            }
            p.push(seg);
        }
        Ok(p)
    }
}

fn check_name(name: &str) -> Result<(), Status> {
    let ok = !name.is_empty()
        && !name.starts_with('/')
        && name
            .split('/')
            .all(|p| !p.is_empty() && p != "." && p != "..");
    if ok {
        Ok(())
    } else {
        Err(Status::invalid_argument("invalid object file name"))
    }
}

/// Refuses a path under `base` that passes through a symlink, so a planted
/// link cannot point a transfer at a file outside the snapshot directory.
/// Components that do not exist yet are fine.
fn no_symlink_under(base: &Path, rel: &str) -> Result<(), Status> {
    let mut p = base.to_path_buf();
    for part in rel.split('/') {
        p.push(part);
        match fs::symlink_metadata(&p) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(Status::invalid_argument(
                    "snapshot file path passes through a symlink",
                ))
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_status(e)),
        }
    }
    Ok(())
}

fn io_status(e: io::Error) -> Status {
    match e.kind() {
        io::ErrorKind::NotFound => Status::not_found("no such object"),
        io::ErrorKind::PermissionDenied => Status::permission_denied("permission denied"),
        _ => Status::internal("local object store I/O failed"),
    }
}

fn copy_into(src: &Path, dst: &Path) -> Result<(), Status> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(io_status)?;
    }
    let tmp = dst.with_file_name(format!(
        ".{}.part-{}",
        dst.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        crate::rand::suffix()
    ));
    let res = crate::sparsefile::copy_file(src, &tmp)
        .map_err(io_status)
        .and_then(|_| fs::rename(&tmp, dst).map_err(io_status));
    if res.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    res
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn fetch_snapshot(&self, ctx: &Ctx, req: FetchSnapshot) -> Result<(), Status> {
        let dir = self.dir_for(&req.snapshot_uri)?;
        let task = tokio::task::spawn_blocking(move || -> Result<(), Status> {
            for f in &req.files {
                check_name(f)?;
                no_symlink_under(&req.write_path, f)?;
            }
            for f in &req.files {
                let src = dir.join(f);
                if !src.is_file() {
                    return Err(Status::not_found("no such object"));
                }
                let dst = req.write_path.join(f);
                if let Some(parent) = dst.parent() {
                    fs::create_dir_all(parent).map_err(io_status)?;
                }
                crate::sparsefile::copy_file(&src, &dst).map_err(io_status)?;
            }
            Ok(())
        });
        ctx.run(task)
            .await?
            .map_err(|e| Status::internal(format!("fetch task failed: {e}")))?
    }

    async fn upload_snapshot(&self, ctx: &Ctx, req: UploadSnapshot) -> Result<(), Status> {
        let dir = self.dir_for(&req.snapshot_uri)?;
        let task = tokio::task::spawn_blocking(move || -> Result<(), Status> {
            for f in &req.files {
                check_name(f)?;
                no_symlink_under(&req.local_path, f)?;
            }
            for f in &req.files {
                copy_into(&req.local_path.join(f), &dir.join(f))?;
            }
            Ok(())
        });
        ctx.run(task)
            .await?
            .map_err(|e| Status::internal(format!("upload task failed: {e}")))?
    }

    async fn fetch_asset(&self, ctx: &Ctx, req: FetchAsset) -> Result<(), Status> {
        let src = self.dir_for(&req.asset_uri)?;
        let task = tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let mut input = fs::File::open(&src).map_err(io_status)?;
            let len = input.metadata().map_err(io_status)?.len();
            if req.max_bytes > 0 && len > req.max_bytes as u64 {
                return Err(Status::failed_precondition("asset exceeds the size limit"));
            }
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = input.read(&mut buf).map_err(io_status)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            if hex::encode(hasher.finalize()) != req.sha256.to_ascii_lowercase() {
                return Err(Status::failed_precondition("asset digest mismatch"));
            }
            fs::copy(&src, &req.write_path).map_err(io_status)?;
            Ok(())
        });
        ctx.run(task)
            .await?
            .map_err(|e| Status::internal(format!("asset task failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upload_then_fetch_round_trips() {
        let d = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(d.path().join("objects"));
        let src = d.path().join("src");
        fs::create_dir_all(src.join("fs")).unwrap();
        fs::write(src.join("mem"), b"memory").unwrap();
        fs::write(src.join("fs/img"), b"rootfs").unwrap();
        let uri = "gs://bucket/hives/h/actors/a/snapshots/s1";
        let ctx = Ctx::background();
        store
            .upload_snapshot(
                &ctx,
                UploadSnapshot {
                    snapshot_uri: uri.into(),
                    local_path: src.clone(),
                    files: vec!["mem".into(), "fs/img".into()],
                    actor_jwt: String::new(),
                },
            )
            .await
            .unwrap();
        let dst = d.path().join("dst");
        fs::create_dir_all(&dst).unwrap();
        store
            .fetch_snapshot(
                &ctx,
                FetchSnapshot {
                    snapshot_uri: uri.into(),
                    write_path: dst.clone(),
                    files: vec!["fs/img".into()],
                    actor_jwt: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(fs::read(dst.join("fs/img")).unwrap(), b"rootfs");
        let err = store
            .fetch_snapshot(
                &ctx,
                FetchSnapshot {
                    snapshot_uri: uri.into(),
                    write_path: dst,
                    files: vec!["manifest.json".into()],
                    actor_jwt: String::new(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn fetch_asset_checks_digest_and_size() {
        let d = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(d.path().join("objects"));
        let obj = store.dir_for("https://example.com/rel/kernel").unwrap();
        fs::create_dir_all(obj.parent().unwrap()).unwrap();
        fs::write(&obj, b"kernel").unwrap();
        let sum = hex::encode(Sha256::digest(b"kernel"));
        let out = d.path().join("out");
        fs::write(&out, b"").unwrap();
        let req = |sha: &str, max| FetchAsset {
            asset_uri: "https://example.com/rel/kernel".into(),
            sha256: sha.into(),
            write_path: out.clone(),
            max_bytes: max,
        };
        let ctx = Ctx::background();
        store.fetch_asset(&ctx, req(&sum, 100)).await.unwrap();
        assert_eq!(fs::read(&out).unwrap(), b"kernel");
        assert_eq!(
            store
                .fetch_asset(&ctx, req(&"0".repeat(64), 100))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            store
                .fetch_asset(&ctx, req(&sum, 2))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn dir_for_rejects_traversal() {
        let store = LocalObjectStore::new("/objects");
        assert!(
            store
                .dir_for("gs://b/a/../../x")
                .is_ok_and(|p| p.starts_with("/objects"))
                || store.dir_for("gs://b/a/../../x").is_err()
        );
        assert_eq!(
            store.dir_for("gs://bucket/x/y").unwrap(),
            PathBuf::from("/objects/gs/bucket/x/y")
        );
    }
}
