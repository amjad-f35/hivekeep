//! Read-through gets: serve a hit, or fetch once and serve.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::watch;
use walkdir::WalkDir;

use super::{
    is_exdev, read_only_mode, touch, write_entry_meta, CacheError, Fetcher, Flight, Inner, Key,
    Store, DATA_NAME,
};
use crate::atomicfile::{chmod, create_dir_mode, mode_of, tempdir_in};
use crate::context::Ctx;
use crate::sparsefile;

/// Bounds the fetch-then-serve loop. An entry can vanish between a fetch
/// publishing it and this caller's serve only if it sat unused past the
/// min age, so more than a few rounds means something else is deleting
/// under the store root.
const LINK_RETRIES: usize = 3;

/// What an entry's data is, fixed by the method that fetched it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    File,
    Tree,
}

/// How a hit is materialized at the destination.
#[derive(Debug, Clone, Copy)]
enum Serve {
    Link,
    Copy,
    LinkTree,
}

impl Store {
    /// Materializes `key`'s file at `dst` as a hard link to the read-only
    /// cached copy, fetching it first on a miss. `dst` must be absolute,
    /// must not exist, and must be on the cache's mount.
    ///
    /// Concurrent calls for one key share one fetch. A caller whose `ctx`
    /// ends returns early while the fetch runs on for the others. Failed
    /// fetches are not remembered: the next call fetches afresh.
    pub async fn get_file_to(
        &self,
        ctx: &Ctx,
        key: &Key,
        dst: &Path,
        fetch: Fetcher,
    ) -> Result<(), CacheError> {
        self.get_to(ctx, key, dst, fetch, Kind::File, Serve::Link)
            .await
    }

    /// Materializes `key`'s tree at `dst` as a directory the caller owns in
    /// which every file is a hard link to the cached copy, fetching it
    /// first on a miss. `dst` is built beside its name and renamed into
    /// place, so an existing `dst` is a complete tree.
    pub async fn get_dir_to(
        &self,
        ctx: &Ctx,
        key: &Key,
        dst: &Path,
        fetch: Fetcher,
    ) -> Result<(), CacheError> {
        self.get_to(ctx, key, dst, fetch, Kind::Tree, Serve::LinkTree)
            .await
    }

    /// Materializes `key`'s file at `dst` as a private, hole-preserving copy
    /// (mode 0600) the caller may change, on any filesystem.
    pub async fn get_file_copy_to(
        &self,
        ctx: &Ctx,
        key: &Key,
        dst: &Path,
        fetch: Fetcher,
    ) -> Result<(), CacheError> {
        self.get_to(ctx, key, dst, fetch, Kind::File, Serve::Copy)
            .await
    }

    async fn get_to(
        &self,
        ctx: &Ctx,
        key: &Key,
        dst: &Path,
        fetch: Fetcher,
        kind: Kind,
        serve: Serve,
    ) -> Result<(), CacheError> {
        if !dst.is_absolute() {
            return Err(CacheError::Invalid(format!(
                "destination {:?} is not an absolute path",
                dst
            )));
        }
        let mut fetch = Some(fetch);
        let mut attempt = 0;
        loop {
            let inner = self.inner.clone();
            let (k, d) = (key.clone(), dst.to_path_buf());
            let served = tokio::task::spawn_blocking(move || match serve {
                Serve::Link => inner.link_out(&k, &d),
                Serve::Copy => inner.copy_out(&k, &d),
                Serve::LinkTree => inner.link_tree_out(&k, &d),
            })
            .await
            .map_err(|e| CacheError::io("while serving a hit", io::Error::other(e)))??;
            if served {
                tracing::info!(key = %key, dst = %dst.display(), attempt, "filecache: hit");
                return Ok(());
            }
            if attempt >= LINK_RETRIES {
                return Err(CacheError::io(
                    format!("entry for {key}"),
                    io::Error::other(format!(
                        "vanished after {attempt} fetches; is something else deleting under the store root?"
                    )),
                ));
            }
            attempt += 1;
            self.fetch_flight(ctx, key, &mut fetch, kind).await?;
        }
    }

    /// Runs or joins the key's fetch, and waits for it or for `ctx`. The
    /// fetch runs detached from every caller, bounded by the fetch timeout.
    async fn fetch_flight(
        &self,
        ctx: &Ctx,
        key: &Key,
        fetch: &mut Option<Fetcher>,
        kind: Kind,
    ) -> Result<(), CacheError> {
        let flight = {
            let mut flights = self.inner.flights.lock();
            match flights.get(key.dir()) {
                Some(f) => f.clone(),
                None => {
                    let Some(f) = fetch.take() else {
                        return Err(CacheError::Invalid("fetcher already used".into()));
                    };
                    let (tx, rx) = watch::channel(None);
                    flights.insert(key.dir().to_string(), Flight::Fetch(rx.clone()));
                    let inner = self.inner.clone();
                    let key = key.clone();
                    tokio::spawn(async move {
                        let res = fetch_and_publish(&inner, &key, f, kind).await;
                        inner.flights.lock().remove(key.dir());
                        let _ = tx.send(Some(res));
                    });
                    Flight::Fetch(rx)
                }
            }
        };
        match flight {
            Flight::Fetch(mut rx) => {
                let res = ctx
                    .run(async {
                        let v = rx.wait_for(|v| v.is_some()).await;
                        v.map(|v| v.clone())
                    })
                    .await?;
                match res {
                    Ok(Some(Ok(()))) => Ok(()),
                    Ok(Some(Err(status))) => Err(CacheError::Fetch {
                        key: key.to_string(),
                        status,
                    }),
                    // The flight ended without a result: try again.
                    Ok(None) | Err(_) => Ok(()),
                }
            }
            Flight::Retire(mut rx) => {
                // An eviction is retiring the entry; once it is done the
                // caller's serve misses and fetches anew.
                let _ = ctx.run(rx.wait_for(|done| *done)).await?;
                Ok(())
            }
        }
    }
}

/// Runs one fetch into tmp/ and publishes the result under entries/. A
/// failed fetch leaves nothing in the cache.
async fn fetch_and_publish(
    inner: &Arc<Inner>,
    key: &Key,
    fetch: Fetcher,
    kind: Kind,
) -> Result<(), tonic::Status> {
    let internal = |what: &str, e: io::Error| tonic::Status::internal(format!("{what}: {e}"));
    // A flight that completed between this caller's miss and this flight
    // may have published already.
    if inner.data_path(key).exists() {
        return Ok(());
    }
    let tmp = tempdir_in(&inner.tmp_dir(), &format!("{}-", key.dir()))
        .map_err(|e| internal("while creating fetch temp dir", e))?;
    let res = async {
        let data = tmp.join(DATA_NAME);
        if kind == Kind::Tree {
            create_dir_mode(&data, 0o700)
                .map_err(|e| internal("while creating fetch tree dir", e))?;
        }
        let ctx = Ctx::background().with_timeout(inner.fetch_timeout);
        match ctx.run(fetch(ctx.clone(), data.clone())).await {
            Ok(r) => r?,
            Err(c) => return Err(tonic::Status::from(c)),
        }
        let (tmp2, data2, key2, entry) =
            (tmp.clone(), data.clone(), key.clone(), inner.entry_dir(key));
        tokio::task::spawn_blocking(move || -> Result<(), tonic::Status> {
            match kind {
                Kind::File => protect_file(&data2)?,
                Kind::Tree => protect_tree(&data2)?,
            }
            write_entry_meta(&tmp2, &key2).map_err(|e| tonic::Status::internal(e.to_string()))?;
            chmod(&tmp2, 0o755).map_err(|e| internal("while setting entry dir mode", e))?;
            match fs::rename(&tmp2, &entry) {
                Ok(()) => Ok(()),
                // A crashed predecessor's published entry: keep it.
                Err(e)
                    if e.kind() == io::ErrorKind::AlreadyExists
                        || e.raw_os_error() == Some(libc::ENOTEMPTY) =>
                {
                    Ok(())
                }
                Err(e) => Err(internal("while publishing entry", e)),
            }
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("publish task failed: {e}")))?
    }
    .await;
    let _ = crate::actordirs::remove_all(&tmp);
    res
}

/// Checks a file fetcher produced one regular file, and makes it read-only.
fn protect_file(data: &Path) -> Result<(), tonic::Status> {
    let meta = fs::symlink_metadata(data).map_err(|e| {
        tonic::Status::internal(format!("fetcher succeeded but produced no file: {e}"))
    })?;
    if !meta.is_file() {
        return Err(tonic::Status::internal(format!(
            "fetcher produced {:?}, want a regular file",
            meta.file_type()
        )));
    }
    chmod(data, read_only_mode(mode_of(&meta)))
        .map_err(|e| tonic::Status::internal(format!("while making fetched file read-only: {e}")))
}

/// Checks a tree fetcher left only directories and regular files, at least
/// one file, and makes every file read-only and every directory owner-only.
fn protect_tree(data: &Path) -> Result<(), tonic::Status> {
    let mut files = 0;
    for entry in WalkDir::new(data) {
        let entry = entry
            .map_err(|e| tonic::Status::internal(format!("while protecting fetched tree: {e}")))?;
        let ft = entry.file_type();
        if ft.is_dir() {
            chmod(entry.path(), 0o700)
        } else if ft.is_file() {
            files += 1;
            let meta = entry
                .metadata()
                .map_err(|e| tonic::Status::internal(format!("while protecting fetched tree: {e}")))?;
            chmod(entry.path(), read_only_mode(mode_of(&meta)))
        } else {
            let rel = entry.path().strip_prefix(data).unwrap_or(entry.path());
            return Err(tonic::Status::internal(format!(
                "while protecting fetched tree: fetcher produced {:?} of type {:?}, want only directories and regular files",
                rel, ft
            )));
        }
        .map_err(|e| tonic::Status::internal(format!("while protecting fetched tree: {e}")))?;
    }
    if files == 0 {
        return Err(tonic::Status::internal(
            "fetcher succeeded but produced an empty tree",
        ));
    }
    Ok(())
}

impl Inner {
    /// Links the key's data file to `dst` and touches the entry. `false`
    /// means a miss.
    fn link_out(&self, key: &Key, dst: &Path) -> Result<bool, CacheError> {
        let _g = self.hit_mu.read();
        let data = self.data_path(key);
        let meta = match fs::metadata(&data) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(CacheError::io(format!("while checking cache for {key}"), e)),
        };
        if meta.is_dir() {
            return Err(CacheError::Invalid(format!(
                "entry for {key} is a directory tree, not a file (use get_dir_to)"
            )));
        }
        if let Err(e) = fs::hard_link(&data, dst) {
            if e.kind() == io::ErrorKind::AlreadyExists {
                return Err(CacheError::Exists(dst.to_path_buf()));
            }
            if is_exdev(&e) {
                return Err(CacheError::CrossDevice(dst.to_path_buf()));
            }
            return Err(CacheError::io(
                format!("while linking {key} to {}", dst.display()),
                e,
            ));
        }
        touch(&self.entry_dir(key));
        Ok(true)
    }

    /// Copies the key's data file to `dst`. Only the open runs under the
    /// hit lock; the copy reads the held-open handle.
    fn copy_out(&self, key: &Key, dst: &Path) -> Result<bool, CacheError> {
        let Some(mut src) = self.open_data(key)? else {
            return Ok(false);
        };
        let mut out = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dst)
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(CacheError::Exists(dst.to_path_buf()))
            }
            Err(e) => {
                return Err(CacheError::io(
                    format!("while creating {}", dst.display()),
                    e,
                ))
            }
        };
        let _ = crate::atomicfile::set_mode(&out, 0o600);
        if let Err(e) = sparsefile::copy(&mut src, &mut out) {
            drop(out);
            let _ = fs::remove_file(dst);
            return Err(CacheError::io(
                format!("while copying {key} to {}", dst.display()),
                e,
            ));
        }
        Ok(true)
    }

    /// Opens the key's data under the hit lock and touches the entry.
    fn open_data(&self, key: &Key) -> Result<Option<fs::File>, CacheError> {
        let _g = self.hit_mu.read();
        let f = match fs::File::open(self.data_path(key)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CacheError::io(format!("while opening cached {key}"), e)),
        };
        let meta = f
            .metadata()
            .map_err(|e| CacheError::io(format!("while checking cached {key}"), e))?;
        if meta.is_dir() {
            return Err(CacheError::Invalid(format!(
                "entry for {key} is a directory tree, not a file (use get_dir_to)"
            )));
        }
        touch(&self.entry_dir(key));
        Ok(Some(f))
    }

    /// Mirrors the key's tree at `dst` with every file hard-linked, built
    /// beside `dst` and renamed into place.
    fn link_tree_out(&self, key: &Key, dst: &Path) -> Result<bool, CacheError> {
        let _g = self.hit_mu.read();
        let src = self.data_path(key);
        let meta = match fs::metadata(&src) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(CacheError::io(format!("while checking cache for {key}"), e)),
        };
        if !meta.is_dir() {
            return Err(CacheError::Invalid(format!(
                "entry for {key} is a file, not a directory tree (use get_file_to)"
            )));
        }
        match fs::symlink_metadata(dst) {
            Ok(_) => return Err(CacheError::Exists(dst.to_path_buf())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(CacheError::io(
                    format!("while checking destination {}", dst.display()),
                    e,
                ))
            }
        }
        let parent = dst.parent().unwrap_or_else(|| Path::new("/"));
        let base = dst
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let tmp = tempdir_in(parent, &format!(".{base}.linking-"))
            .map_err(|e| CacheError::io(format!("while creating {}", dst.display()), e))?;
        let cleanup = |e: CacheError| {
            let _ = crate::actordirs::remove_all(&tmp);
            e
        };
        chmod(&tmp, mode_of(&meta)).map_err(|e| {
            cleanup(CacheError::io(
                format!("while creating {}", dst.display()),
                e,
            ))
        })?;
        link_tree(&src, &tmp).map_err(cleanup)?;
        if let Err(e) = fs::rename(&tmp, dst) {
            let _ = crate::actordirs::remove_all(&tmp);
            if e.kind() == io::ErrorKind::AlreadyExists || e.raw_os_error() == Some(libc::ENOTEMPTY)
            {
                return Err(CacheError::Exists(dst.to_path_buf()));
            }
            return Err(CacheError::io(
                format!("while placing {}", dst.display()),
                e,
            ));
        }
        touch(&self.entry_dir(key));
        Ok(true)
    }
}

/// Recreates `src`'s directories under the existing `dst` and hard-links
/// its files.
fn link_tree(src: &Path, dst: &Path) -> Result<(), CacheError> {
    for entry in WalkDir::new(src).min_depth(1) {
        let entry =
            entry.map_err(|e| CacheError::io("while walking cached tree", io::Error::other(e)))?;
        let rel = entry.path().strip_prefix(src).unwrap_or(entry.path());
        let target: PathBuf = dst.join(rel);
        let ft = entry.file_type();
        if ft.is_dir() {
            create_dir_mode(&target, 0o700)
                .map_err(|e| CacheError::io("while mirroring cached tree", e))?;
        } else if ft.is_file() {
            if let Err(e) = fs::hard_link(entry.path(), &target) {
                if is_exdev(&e) {
                    return Err(CacheError::CrossDevice(dst.to_path_buf()));
                }
                return Err(CacheError::io("while linking cached tree", e));
            }
        } else {
            return Err(CacheError::Invalid(format!(
                "cached tree holds {:?} of type {:?}, want a directory or regular file",
                rel, ft
            )));
        }
    }
    Ok(())
}
