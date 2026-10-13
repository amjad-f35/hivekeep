//! Sizing, eviction and crash-debris removal.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::sync::watch;
use walkdir::WalkDir;

use super::{CacheError, Flight, Inner, Store, DATA_NAME, RM_PREFIX};
use crate::sparsefile::{allocated_bytes, link_count};

/// What an eviction pass did, or in a dry run would do.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EvictStats {
    /// Entries renamed out of the cache's namespace: from then on lookups
    /// miss, whether or not the physical removal succeeded.
    pub retired: usize,
    /// Allocated bytes returned to the filesystem by completed removals.
    pub freed_bytes: i64,
    /// Entries a consumer still links, which are never evicted.
    pub skipped_linked: usize,
    /// Entries younger than the min age.
    pub skipped_young: usize,
    /// Entries a hit, fetch or new link touched after the pass listed them.
    pub skipped_busy: usize,
}

/// What a debris sweep removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    /// Unfinished fetches removed from tmp/.
    pub tmp_removed: usize,
    /// Retired entries a crashed eviction left behind.
    pub retired_removed: usize,
}

#[derive(Debug, Clone)]
pub(super) struct Candidate {
    pub(super) dir: String,
    pub(super) mtime: SystemTime,
    pub(super) size: i64,
    pub(super) linked: bool,
}

impl Store {
    /// The allocated bytes of every published entry's files, not their
    /// logical lengths: the largest artifacts are sparse memory images, and
    /// budgets are compared against real disk usage.
    pub async fn total_bytes(&self) -> Result<i64, CacheError> {
        let inner = self.inner.clone();
        blocking(move || inner.total_bytes()).await
    }

    /// [`Store::total_bytes`] on the calling thread, for callers that cannot
    /// await, such as a metrics scrape.
    pub fn total_bytes_blocking(&self) -> Result<i64, CacheError> {
        self.inner.total_bytes()
    }

    /// Frees cache space until `target_bytes` are reclaimed or nothing
    /// eligible remains, least recently used first. Only entries no
    /// consumer links and older than the min age are eligible, and a hit or
    /// fetch racing the pass vetoes its entry. A non-positive target is a
    /// no-op. With `dry_run` nothing is touched.
    pub async fn evict_unused(
        &self,
        target_bytes: i64,
        dry_run: bool,
    ) -> Result<EvictStats, (EvictStats, CacheError)> {
        let inner = self.inner.clone();
        match tokio::task::spawn_blocking(move || inner.evict_unused(target_bytes, dry_run)).await {
            Ok(r) => r,
            Err(e) => Err((
                EvictStats::default(),
                CacheError::io("eviction task failed", io::Error::other(e)),
            )),
        }
    }

    /// Removes crash debris: everything under tmp/ and every retired entry
    /// at the root. Run once at startup, before the store serves: a later
    /// sweep would delete the working directories of in-flight fetches.
    pub async fn sweep_debris(&self) -> Result<SweepStats, (SweepStats, CacheError)> {
        let inner = self.inner.clone();
        match tokio::task::spawn_blocking(move || inner.sweep_debris()).await {
            Ok(r) => r,
            Err(e) => Err((
                SweepStats::default(),
                CacheError::io("sweep task failed", io::Error::other(e)),
            )),
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, CacheError> + Send + 'static,
) -> Result<T, CacheError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| CacheError::io("cache task failed", io::Error::other(e)))?
}

fn joined(errs: Vec<String>) -> Option<CacheError> {
    if errs.is_empty() {
        None
    } else {
        Some(CacheError::io(
            "eviction",
            io::Error::other(errs.join("; ")),
        ))
    }
}

fn walk_not_found(e: &walkdir::Error) -> bool {
    e.io_error()
        .is_some_and(|e| e.kind() == io::ErrorKind::NotFound)
}

impl Inner {
    fn total_bytes(&self) -> Result<i64, CacheError> {
        let mut total = 0i64;
        for entry in WalkDir::new(self.entries_dir()) {
            let entry = match entry {
                Ok(e) => e,
                Err(e) if walk_not_found(&e) => continue,
                Err(e) => return Err(CacheError::io("while sizing entries", io::Error::other(e))),
            };
            if !entry.file_type().is_file() {
                continue;
            }
            match entry.metadata() {
                Ok(m) => total += allocated_bytes(&m),
                Err(e) if walk_not_found(&e) => {}
                Err(e) => return Err(CacheError::io("while sizing entries", io::Error::other(e))),
            }
        }
        Ok(total)
    }

    fn sweep_debris(&self) -> Result<SweepStats, (SweepStats, CacheError)> {
        let mut stats = SweepStats::default();
        let mut errs = Vec::new();
        match fs::read_dir(self.tmp_dir()) {
            Ok(rd) => {
                for child in rd.flatten() {
                    match crate::actordirs::remove_all(&child.path()) {
                        Ok(()) => stats.tmp_removed += 1,
                        Err(e) => errs.push(format!(
                            "while removing tmp debris {:?}: {e}",
                            child.file_name()
                        )),
                    }
                }
            }
            Err(e) => errs.push(format!("while listing tmp dir: {e}")),
        }
        match fs::read_dir(&self.root) {
            Ok(rd) => {
                for child in rd.flatten() {
                    if !child.file_name().to_string_lossy().starts_with(RM_PREFIX) {
                        continue;
                    }
                    match crate::actordirs::remove_all(&child.path()) {
                        Ok(()) => stats.retired_removed += 1,
                        Err(e) => errs.push(format!(
                            "while removing retired entry {:?}: {e}",
                            child.file_name()
                        )),
                    }
                }
            }
            Err(e) => errs.push(format!("while listing store root: {e}")),
        }
        match joined(errs) {
            None => Ok(stats),
            Some(e) => Err((stats, e)),
        }
    }

    fn evict_unused(
        &self,
        target: i64,
        dry_run: bool,
    ) -> Result<EvictStats, (EvictStats, CacheError)> {
        let mut stats = EvictStats::default();
        if target <= 0 {
            return Ok(stats);
        }
        let _pass = self.evict_mu.lock();
        let mut errs = Vec::new();
        if !dry_run {
            // Only eviction creates retired dirs while the store serves, and
            // passes are serialized, so retrying these races nothing.
            errs.extend(self.remove_retired_leftovers());
        }
        let (candidates, list_errs) = self.list_candidates();
        errs.extend(list_errs);

        let now = SystemTime::now();
        let mut eligible: Vec<Candidate> = Vec::new();
        for c in candidates {
            if c.linked {
                stats.skipped_linked += 1;
            } else if now.duration_since(c.mtime).unwrap_or_default() < self.min_age {
                stats.skipped_young += 1;
            } else {
                eligible.push(c);
            }
        }
        eligible.sort_by_key(|c| c.mtime);

        let mut retired: Vec<(PathBuf, i64)> = Vec::new();
        let mut selected = 0i64;
        for c in eligible {
            if selected >= target {
                break;
            }
            if dry_run {
                stats.freed_bytes += c.size;
            } else {
                match self.retire_entry(&c) {
                    Ok(Some(p)) => retired.push((p, c.size)),
                    Ok(None) => {
                        stats.skipped_busy += 1;
                        continue;
                    }
                    Err(e) => {
                        errs.push(format!("while retiring entry {}: {e}", c.dir));
                        continue;
                    }
                }
            }
            stats.retired += 1;
            selected += c.size;
        }
        // The slow deletion runs after all retires, outside the hit lock.
        for (path, freed) in retired {
            match fs::remove_dir_all(&path) {
                Ok(()) => stats.freed_bytes += freed,
                Err(e) => errs.push(format!(
                    "while removing retired entry {:?}: {e}",
                    path.file_name()
                )),
            }
        }
        match joined(errs) {
            None => Ok(stats),
            Some(e) => Err((stats, e)),
        }
    }

    fn remove_retired_leftovers(&self) -> Vec<String> {
        let mut errs = Vec::new();
        match fs::read_dir(&self.root) {
            Ok(rd) => {
                for child in rd.flatten() {
                    if child.file_name().to_string_lossy().starts_with(RM_PREFIX) {
                        if let Err(e) = crate::actordirs::remove_all(&child.path()) {
                            errs.push(format!(
                                "while removing retired leftover {:?}: {e}",
                                child.file_name()
                            ));
                        }
                    }
                }
            }
            Err(e) => errs.push(format!("while listing store root: {e}")),
        }
        errs
    }

    /// Lists the published entries, lock-free and therefore stale:
    /// retiring re-checks each victim first.
    fn list_candidates(&self) -> (Vec<Candidate>, Vec<String>) {
        let mut errs = Vec::new();
        let rd = match fs::read_dir(self.entries_dir()) {
            Ok(rd) => rd,
            Err(e) => return (Vec::new(), vec![format!("while listing entries: {e}")]),
        };
        let mut out = Vec::new();
        for child in rd.flatten() {
            let name = child.file_name().to_string_lossy().into_owned();
            let dir = child.path();
            let meta = match fs::metadata(&dir) {
                Ok(m) if m.is_dir() => m,
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => {
                    errs.push(format!("while inspecting entry {name}: {e}"));
                    continue;
                }
            };
            let (size, linked) = match size_entry(&dir) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => {
                    errs.push(format!("while sizing entry {name}: {e}"));
                    continue;
                }
            };
            out.push(Candidate {
                dir: name,
                mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                size,
                linked,
            });
        }
        (out, errs)
    }

    /// Retires `c` if it is still exactly the entry listed, returning the
    /// retired path; `None` is a veto. A fetch or retire in flight on the
    /// key vetoes it too.
    pub(super) fn retire_entry(&self, c: &Candidate) -> io::Result<Option<PathBuf>> {
        let (tx, rx) = watch::channel(false);
        {
            let mut flights = self.flights.lock();
            if flights.contains_key(&c.dir) {
                return Ok(None);
            }
            flights.insert(c.dir.clone(), Flight::Retire(rx));
        }
        let res = (|| {
            let _g = self.hit_mu.write();
            let entry = self.entries_dir().join(&c.dir);
            let meta = match fs::metadata(&entry) {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };
            if meta.modified().ok() != Some(c.mtime) {
                return Ok(None);
            }
            // The touch on a hit is best-effort; ask the files directly.
            if size_entry(&entry)?.1 {
                return Ok(None);
            }
            let rm = self
                .root
                .join(format!("{RM_PREFIX}{}-{}", c.dir, crate::rand::suffix()));
            fs::rename(&entry, &rm)?;
            Ok(Some(rm))
        })();
        self.flights.lock().remove(&c.dir);
        let _ = tx.send(true);
        res
    }
}

/// An entry's allocated bytes, and whether any file of its data carries a
/// consumer hard link.
pub(super) fn size_entry(entry_dir: &Path) -> io::Result<(i64, bool)> {
    let data = entry_dir.join(DATA_NAME);
    let mut size = 0i64;
    let mut linked = false;
    for e in WalkDir::new(entry_dir) {
        let e = match e {
            Ok(e) => e,
            Err(err) if walk_not_found(&err) => continue,
            Err(err) => return Err(io::Error::other(err)),
        };
        if !e.file_type().is_file() {
            continue;
        }
        let meta = match e.metadata() {
            Ok(m) => m,
            Err(err) if walk_not_found(&err) => continue,
            Err(err) => return Err(io::Error::other(err)),
        };
        size += allocated_bytes(&meta);
        if e.path().starts_with(&data) && link_count(&meta) > 1 {
            linked = true;
        }
    }
    Ok((size, linked))
}
