//! Eviction for the layer pool.
//!
//! The pool is a two-level DAG (records reference layers by diffID), so
//! liveness is plain refcounting, recomputed from disk on every pass. An
//! image, and then any layer its removal leaves unreferenced, is evictable
//! unless vetoed by, in order:
//!
//! 1. the root set: bundle overlay specs under the actors dir, written by
//!    the keeper before any mount and removed only after unmount (the
//!    keeper cannot see the workers' mount namespaces);
//! 2. min-age: records and layers younger than it are never touched,
//!    covering the pull, spec-write, mount window;
//! 3. per-victim re-checks against disk state right before acting.
//!
//! Passes reach layers only through records. Pulls write their record
//! before unpacking, so an unexplained layer can only be crash debris,
//! reclaimed once at startup by [`Store::recover_orphans`]. Everything
//! fails toward retention: if the records or specs cannot all be read, a
//! pass does nothing.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use hivekeep_node::context::Ctx;

use crate::digest::Digest;
use crate::error::{Error, ErrorKind, Result};
use crate::removeall::remove_all_writable;
use crate::retire::{is_layer_dir_name, RetireStatus};
use crate::spec::{read_spec, OverlaySpec};
use crate::store::{ImageRecord, Store};

/// The message of a pass skipped for incomplete enumeration.
pub const INCOMPLETE_ENUMERATION: &str = "image cache enumeration incomplete";

/// What eviction must not touch, recomputed at the start of every pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootSet {
    /// Rooted image digests (`sha256:<hex>`).
    pub image_digests: HashSet<String>,
    /// Rooted layer diffID hexes (the layer dir names), covering specs
    /// written without a digest.
    pub layer_hexes: HashSet<String>,
    /// One signature per rooted bundle's exact layer set. A record with a
    /// matching set is rooted too: the multi-platform twin and the record
    /// behind a digestless spec. Exact on purpose: rooting every subset
    /// (a running actor's base image, say) would weaken the byte cap under
    /// heavy layer sharing.
    pub layer_sets: HashSet<String>,
}

/// Canonicalizes a set of layer hexes for comparison.
pub fn layer_set_signature(hexes: &[String]) -> String {
    let mut uniq: Vec<&str> = hexes.iter().map(String::as_str).collect();
    uniq.sort_unstable();
    uniq.dedup();
    uniq.join(",")
}

/// What an eviction pass did (or, dry-run, would do).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvictStats {
    /// Retired layers' recorded sizes: an estimate the caller's next statfs
    /// corrects.
    pub freed_bytes: i64,
    /// Deleted records.
    pub evicted_images: usize,
    /// Retired layer dirs.
    pub evicted_layers: usize,
    /// LRU candidates after listing-stage vetoes.
    pub candidates: usize,
    /// Records excluded because a bundle spec roots them.
    pub rooted_images: usize,
    /// Layers kept during the pass because a bundle spec roots them.
    pub skipped_rooted: usize,
    /// Min-age vetoes, at listing time or by a per-victim re-check.
    pub skipped_fresh: usize,
    /// Layers reclaimed by the startup scan; always zero for passes.
    pub orphan_layers: usize,
}

struct Candidate {
    digest: Digest,
    mtime: SystemTime,
    /// Unique, in record order.
    diff_ids: Vec<String>,
    /// The record's bytes, to restore it if a layer must be kept.
    raw: Vec<u8>,
}

enum Skip {
    Proceed,
    Fresh,
    Gone,
}

fn incomplete(err: Error) -> Error {
    Error::new(
        ErrorKind::IncompleteEnumeration,
        format!("{INCOMPLETE_ENUMERATION}: {err}"),
    )
}

fn mtime_of(p: &Path) -> io::Result<SystemTime> {
    Ok(fs::metadata(p)?.modified().unwrap_or(UNIX_EPOCH))
}

impl Store {
    /// Scans the actors dir for bundle specs: the images and layers actors
    /// placed on this node use. An error means the set may be incomplete
    /// and deleting callers must do nothing. A missing dir or spec is not
    /// an error.
    pub fn in_use(&self) -> Result<RootSet> {
        let mut rs = RootSet::default();
        let Some(actors_dir) = &self.opts.actors_dir else {
            return Ok(rs);
        };
        let actors = match fs::read_dir(actors_dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(rs),
            Err(e) => {
                return Err(Error::io(
                    format!("while listing actors dir {actors_dir:?} for root set"),
                    e,
                ))
            }
        };
        let mut errs = Vec::new();
        for actor in actors {
            let actor = match actor {
                Ok(a) => a,
                Err(e) => {
                    errs.push(Error::io("while listing actors dir for root set", e));
                    continue;
                }
            };
            if !actor.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = actor.file_name().to_string_lossy().into_owned();
            let bundles_dir = actor.path().join("bundles");
            let bundles = match fs::read_dir(&bundles_dir) {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => {
                    errs.push(Error::io(
                        format!("while listing bundles of actor {name:?}"),
                        e,
                    ));
                    continue;
                }
            };
            for bundle in bundles.flatten() {
                if !bundle.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let label = format!("{name}/bundles/{}", bundle.file_name().to_string_lossy());
                match read_spec(&bundle.path()) {
                    Ok(Some(spec)) => add_spec_roots(&mut rs, &spec, &label),
                    Ok(None) => {}
                    Err(e) => {
                        errs.push(e.context(format!("while reading bundle overlay spec {label:?}")))
                    }
                }
            }
        }
        match Error::join(errs) {
            Some(e) => Err(e),
            None => Ok(rs),
        }
    }

    /// Evicts least-recently-used unprotected images (and the layers their
    /// removal leaves unreferenced) until about `target_bytes` is freed.
    /// `i64::MAX` frees everything eligible; `<= 0` evicts nothing. With
    /// `dry_run` nothing is deleted or renamed.
    ///
    /// Per-item failures are aggregated into the error and retried next
    /// pass. An incomplete enumeration skips the pass entirely with an
    /// [`ErrorKind::IncompleteEnumeration`] error.
    pub fn evict_unused(
        &self,
        ctx: &Ctx,
        target_bytes: i64,
        dry_run: bool,
    ) -> (EvictStats, Option<Error>) {
        let mut stats = EvictStats::default();
        let _g = self.evict_mu.lock();

        let roots = match self.in_use() {
            Ok(r) => r,
            Err(e) => return (stats, Some(incomplete(e))),
        };
        let cutoff = SystemTime::now() - self.opts.min_age;
        let (candidates, mut refcount) = match self.list_eviction(&roots, cutoff, &mut stats) {
            Ok(v) => v,
            Err(e) => return (stats, Some(incomplete(e))),
        };
        stats.candidates = candidates.len();
        tracing::debug!(
            target_bytes,
            dry_run,
            rooted_images = stats.rooted_images,
            rooted_layers = roots.layer_hexes.len(),
            candidates = candidates.len(),
            min_age_s = self.opts.min_age.as_secs(),
            "Image cache eviction pass"
        );

        let mut errs = Vec::new();
        let mut retired = Vec::new();
        for cand in &candidates {
            if target_bytes <= 0 || stats.freed_bytes >= target_bytes {
                break;
            }
            if let Some(e) = ctx.err() {
                errs.push(Error::from(e));
                break;
            }
            match self.remove_stale_record(cand, cutoff, dry_run) {
                Err(e) => {
                    errs.push(e);
                    continue;
                }
                Ok(Skip::Fresh) => {
                    tracing::debug!(digest = %cand.digest, "Image cache eviction skipped image: fresh");
                    stats.skipped_fresh += 1;
                    continue;
                }
                Ok(Skip::Gone) => continue,
                Ok(Skip::Proceed) => {}
            }
            tracing::info!(digest = %cand.digest, layers = cand.diff_ids.len(), dry_run, "Image cache evicting image record");
            let (kept, mut cand_retired, retire_errs) = self.retire_candidate_layers(
                cand,
                &mut refcount,
                &roots,
                cutoff,
                dry_run,
                &mut stats,
            );
            errs.extend(retire_errs);
            retired.append(&mut cand_retired);
            if kept {
                // Restore the record so kept layers stay reachable, and put
                // the refcounts back for later victims.
                if !dry_run {
                    match self.write_record_bytes(&cand.digest, &cand.raw) {
                        Err(e) => errs.push(e.context(format!(
                            "while restoring record {} after kept layer",
                            cand.digest
                        ))),
                        Ok(()) => tracing::info!(digest = %cand.digest,
                            "Image cache restored image record: some of its layers must be kept"),
                    }
                }
                for h in &cand.diff_ids {
                    *refcount.entry(h.clone()).or_insert(0) += 1;
                }
                continue;
            }
            stats.evicted_images += 1;
        }
        if !retired.is_empty() {
            let n = retired.len();
            errs.extend(remove_retired_dirs(retired));
            tracing::info!(count = n, "Image cache removed retired layer dirs");
        }
        (stats, Error::join(errs))
    }

    /// Deletes the candidate's record unless a re-check vetoes it, under
    /// the hit lock so a cache hit's touch cannot interleave.
    fn remove_stale_record(
        &self,
        cand: &Candidate,
        cutoff: SystemTime,
        dry_run: bool,
    ) -> Result<Skip> {
        let _g = self.hit_mu.write();
        let path = self.record_path(&cand.digest);
        match mtime_of(&path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Skip::Gone),
            Err(e) => {
                return Err(Error::io(
                    format!("while checking image record {}", cand.digest),
                    e,
                ))
            }
            Ok(m) if m > cutoff => return Ok(Skip::Fresh),
            Ok(_) => {}
        }
        if !dry_run {
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(Error::io(
                        format!("while deleting image record {}", cand.digest),
                        e,
                    ));
                }
            }
        }
        Ok(Skip::Proceed)
    }

    /// Retires the layers the candidate's removal unreferenced. `kept` is
    /// true when any layer must stay; the caller then restores the record,
    /// since a kept layer without one is unreachable until restart.
    fn retire_candidate_layers(
        &self,
        cand: &Candidate,
        refcount: &mut HashMap<String, usize>,
        roots: &RootSet,
        cutoff: SystemTime,
        dry_run: bool,
        stats: &mut EvictStats,
    ) -> (bool, Vec<PathBuf>, Vec<Error>) {
        let (mut kept, mut retired, mut errs) = (false, Vec::new(), Vec::new());
        for hex in &cand.diff_ids {
            let n = refcount.entry(hex.clone()).or_insert(0);
            *n = n.saturating_sub(1);
            if *n > 0 {
                tracing::debug!(diffid = %hex, refcount = *n, "Image cache keeping layer: still referenced");
                continue;
            }
            if roots.layer_hexes.contains(hex) {
                tracing::debug!(diffid = %hex, "Image cache keeping layer: rooted by a bundle spec");
                stats.skipped_rooted += 1;
                kept = true;
                continue;
            }
            let (size, path, st) = if dry_run {
                let (s, st) = self.dry_run_retire(hex, cutoff);
                (s, None, st)
            } else {
                match self.retire_layer(hex, cutoff) {
                    Err(e) => {
                        errs.push(e);
                        kept = true;
                        continue;
                    }
                    Ok((path, st)) => {
                        // Sized from the renamed dir, read-only: eviction never
                        // writes a backfill into the live pool.
                        let size = match (&path, st) {
                            (Some(p), RetireStatus::Retired) => {
                                self.layer_size_read_only(p).unwrap_or(0)
                            }
                            _ => 0,
                        };
                        (size, path, st)
                    }
                }
            };
            match st {
                RetireStatus::Gone => continue,
                RetireStatus::Vetoed => {
                    kept = true;
                    continue;
                }
                RetireStatus::Retired => {}
            }
            tracing::debug!(diffid = %hex, size_bytes = size, dry_run, "Image cache retiring layer");
            retired.extend(path);
            stats.freed_bytes += size;
            stats.evicted_layers += 1;
        }
        (kept, retired, errs)
    }

    /// Reclaims layer dirs no record references. Run once at open, before
    /// the store serves: with no pull in flight a layer without a record is
    /// definitionally garbage. Skipped entirely when the enumeration is
    /// incomplete; spec roots and min-age still veto.
    pub fn recover_orphans(&self, _ctx: &Ctx) -> Result<EvictStats> {
        let mut stats = EvictStats::default();
        let _g = self.evict_mu.lock();
        let roots = self.in_use().map_err(incomplete)?;
        let cutoff = SystemTime::now() - self.opts.min_age;
        let (_, refcount) = self
            .list_eviction(&roots, cutoff, &mut stats)
            .map_err(incomplete)?;
        let (retired, mut errs) = self.sweep_orphan_layers(&roots, &refcount, cutoff, &mut stats);
        errs.extend(remove_retired_dirs(retired));
        if stats.orphan_layers > 0 {
            tracing::info!(
                count = stats.orphan_layers,
                freed_bytes = stats.freed_bytes,
                "Image cache startup scan reclaimed orphan layers"
            );
        }
        match Error::join(errs) {
            Some(e) => Err(e),
            None => Ok(stats),
        }
    }

    fn sweep_orphan_layers(
        &self,
        roots: &RootSet,
        refcount: &HashMap<String, usize>,
        cutoff: SystemTime,
        stats: &mut EvictStats,
    ) -> (Vec<PathBuf>, Vec<Error>) {
        let entries = match fs::read_dir(self.layers_dir()) {
            Ok(e) => e,
            Err(e) => {
                return (
                    Vec::new(),
                    vec![Error::io("while listing layer pool for orphan sweep", e)],
                )
            }
        };
        let (mut retired, mut errs) = (Vec::new(), Vec::new());
        for e in entries.flatten() {
            let hex = e.file_name().to_string_lossy().into_owned();
            // Dot dirs are in flight or already retired; anything not a
            // digest was not put there by the store.
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                || hex.starts_with('.')
                || !is_layer_dir_name(&hex)
            {
                continue;
            }
            if refcount.get(&hex).copied().unwrap_or(0) > 0 || roots.layer_hexes.contains(&hex) {
                continue;
            }
            match self.retire_layer(&hex, cutoff) {
                Err(e) => errs.push(e),
                Ok((Some(path), RetireStatus::Retired)) => {
                    let size = self.layer_size_read_only(&path).unwrap_or(0);
                    tracing::debug!(diffid = %hex, size_bytes = size, "Image cache retiring orphan layer");
                    retired.push(path);
                    stats.freed_bytes += size;
                    stats.orphan_layers += 1;
                }
                Ok(_) => {}
            }
        }
        (retired, errs)
    }

    /// The LRU candidate list and layer refcounts over every record (rooted
    /// and fresh ones included: their references keep shared layers
    /// alive). An error means some record could not be read, decoded, or
    /// its diffIDs parsed, and both callers must do nothing.
    fn list_eviction(
        &self,
        roots: &RootSet,
        cutoff: SystemTime,
        stats: &mut EvictStats,
    ) -> Result<(Vec<Candidate>, HashMap<String, usize>)> {
        let mut refcount: HashMap<String, usize> = HashMap::new();
        let entries = fs::read_dir(self.manifests_dir())
            .map_err(|e| Error::io("while listing manifest records", e))?;
        let mut cands = Vec::new();
        let mut errs = Vec::new();
        for e in entries {
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    errs.push(Error::io("while listing manifest records", err));
                    continue;
                }
            };
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(hex) = name.strip_suffix(".json") else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let digest = Digest::sha256(hex);
            let raw = match fs::read(e.path()) {
                Ok(b) => b,
                Err(err) => {
                    errs.push(Error::io(
                        format!("while reading image record {digest}"),
                        err,
                    ));
                    continue;
                }
            };
            let rec: ImageRecord = match serde_json::from_slice(&raw) {
                Ok(r) => r,
                Err(err) => {
                    // Never a candidate: its unknown layers would strand.
                    errs.push(Error::msg(format!(
                        "while decoding image record {digest}: {err}"
                    )));
                    continue;
                }
            };
            // Deduplicated per record so the decrement stays symmetric.
            let mut unique: Vec<String> = Vec::new();
            for d in &rec.diff_ids {
                match Digest::parse(d) {
                    Ok(id) => {
                        if !unique.contains(&id.hex) {
                            *refcount.entry(id.hex.clone()).or_insert(0) += 1;
                            unique.push(id.hex);
                        }
                    }
                    Err(err) => errs.push(
                        err.context(format!("invalid diffID {d:?} in image record {digest}")),
                    ),
                }
            }
            let Ok(mtime) = mtime_of(&e.path()) else {
                continue;
            };
            if roots.image_digests.contains(&digest.to_string())
                || (!unique.is_empty() && roots.layer_sets.contains(&layer_set_signature(&unique)))
            {
                stats.rooted_images += 1;
                continue;
            }
            if mtime > cutoff {
                stats.skipped_fresh += 1;
                continue;
            }
            cands.push(Candidate {
                digest,
                mtime,
                diff_ids: unique,
                raw,
            });
        }
        if let Some(e) = Error::join(errs) {
            return Err(e);
        }
        // LRU by last use, ties by name.
        cands.sort_by(|a, b| {
            a.mtime
                .cmp(&b.mtime)
                .then_with(|| a.digest.hex.cmp(&b.digest.hex))
        });
        Ok((cands, refcount))
    }
}

fn add_spec_roots(rs: &mut RootSet, spec: &OverlaySpec, bundle: &str) {
    add_image_roots(rs, &spec.image_digest, &spec.layers, bundle);
    for v in &spec.image_volumes {
        add_image_roots(
            rs,
            &v.image_digest,
            &v.layers,
            &format!("{bundle}/volumes/{}", v.name),
        );
    }
}

fn add_image_roots(rs: &mut RootSet, digest: &str, layers: &[String], bundle: &str) {
    if !digest.is_empty() {
        rs.image_digests.insert(digest.to_string());
        tracing::debug!(
            bundle,
            digest,
            layers = layers.len(),
            "Image cache root-set: bundle roots image"
        );
    } else if !layers.is_empty() {
        tracing::debug!(
            bundle,
            layers = layers.len(),
            "Image cache root-set: digestless bundle roots layers only"
        );
    }
    let hexes: Vec<String> = layers
        .iter()
        .map(|l| {
            Path::new(l)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        })
        .collect();
    for h in &hexes {
        rs.layer_hexes.insert(h.clone());
    }
    if !hexes.is_empty() {
        rs.layer_sets.insert(layer_set_signature(&hexes));
    }
}

/// Removes renamed-aside dirs, the slow half of two-phase deletion, with
/// every failure reported (each sits invisible as `.rm-*` until restart).
fn remove_retired_dirs(dirs: Vec<PathBuf>) -> Vec<Error> {
    let errs = parking_lot::Mutex::new(Vec::new());
    let chunks: Vec<Vec<PathBuf>> = {
        let mut c: Vec<Vec<PathBuf>> = vec![Vec::new(); 4];
        for (i, d) in dirs.into_iter().enumerate() {
            c[i % 4].push(d);
        }
        c
    };
    std::thread::scope(|s| {
        for chunk in chunks {
            let errs = &errs;
            s.spawn(move || {
                for d in chunk {
                    if let Err(e) = remove_all_writable(&d) {
                        errs.lock()
                            .push(Error::io(format!("while removing retired layer {d:?}"), e));
                    }
                }
            });
        }
    });
    errs.into_inner()
}

#[cfg(test)]
#[path = "gc_tests.rs"]
mod tests;
