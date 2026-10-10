//! Archiving and restoring a directory tree as a tar file, keeping the
//! metadata a workload's data depends on: modes (setuid, setgid and sticky
//! included), ownership, modification times, symlinks, hardlinks, FIFOs,
//! overlay whiteouts, and `user.*` plus selected overlay xattrs (as PAX
//! `SCHILY.xattr.*` records).
//!
//! The workers snapshot durable-dir volumes and rootfs overlay uppers with
//! it: contents written by the sandboxed workload under arbitrary uids,
//! shipped to object storage, and restored (possibly on another node) where
//! the workload must see them unchanged. Overlay uppers are why device nodes
//! and xattrs matter: a deleted lower file is a 0:0 character device, a
//! replaced directory carries `trusted.overlay.opaque`, and losing either
//! resurrects deleted content after a resume.
//!
//! Extraction is confined to the destination, so a crafted archive cannot
//! write outside it via `..`, an absolute path, or a symlink. It refuses
//! device nodes other than whiteouts and restores only allowed xattrs.
//! Archiving reads through the same confinement, so an entry swapped for a
//! symlink mid-walk cannot pull outside files into the archive.

mod xattr;

use cap_std::fs::{FileTypeExt, MetadataExt};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use cap_std::fs::Dir;
use tar::{EntryType, Header};

use crate::confined;
use crate::error::{Error, Result};

pub use xattr::keep_xattr;

/// Batches tar headers and padding into fewer file operations.
const STREAM_BUF_SIZE: usize = 64 << 10;
/// The PAX record prefix carrying an xattr.
const PAX_XATTR_PREFIX: &str = "SCHILY.xattr.";

/// Reports whether an entry, by slash-separated path relative to the
/// archive root, is left out. Leaving out a directory leaves out its
/// subtree.
pub type SkipFn<'a> = &'a dyn Fn(&str) -> bool;

/// Writes an archive of `src_dir`'s contents (not `src_dir` itself) to
/// `tar_path`. Sockets and devices other than whiteouts are skipped.
pub fn create(tar_path: &Path, src_dir: &Path) -> Result<()> {
    write_archive(tar_path, src_dir, None, false)
}

/// [`create`] leaving out entries `skip` selects.
pub fn create_filtered(tar_path: &Path, src_dir: &Path, skip: SkipFn<'_>) -> Result<()> {
    write_archive(tar_path, src_dir, Some(skip), false)
}

/// [`create`] plus a `./` entry carrying `src_dir`'s own mode, ownership,
/// time and xattrs, which [`extract`] applies to its destination.
pub fn create_with_root(tar_path: &Path, src_dir: &Path) -> Result<()> {
    write_archive(tar_path, src_dir, None, true)
}

/// [`create_with_root`] leaving out entries `skip` selects (never the root
/// entry).
pub fn create_filtered_with_root(tar_path: &Path, src_dir: &Path, skip: SkipFn<'_>) -> Result<()> {
    write_archive(tar_path, src_dir, Some(skip), true)
}

fn write_archive(
    tar_path: &Path,
    src_dir: &Path,
    skip: Option<SkipFn<'_>>,
    include_root: bool,
) -> Result<()> {
    let f =
        File::create(tar_path).map_err(|e| Error::io(format!("creating tar {tar_path:?}"), e))?;
    let mut b = tar::Builder::new(BufWriter::with_capacity(STREAM_BUF_SIZE, f));
    let root =
        confined::open_root(src_dir).map_err(|e| Error::io(format!("opening {src_dir:?}"), e))?;
    let mut w = TreeWriter {
        root: &root,
        src_dir,
        skip,
        linked: HashMap::new(),
        b: &mut b,
    };
    if include_root {
        w.entry(Path::new(""))?;
    }
    w.walk(Path::new(""))?;
    let out = b
        .into_inner()
        .map_err(|e| Error::io(format!("closing tar {tar_path:?}"), e))?;
    // No fsync: a local snapshot written before the current boot is never
    // trusted anyway, and fsync waits behind other actors' journal writes.
    out.into_inner()
        .map_err(|e| Error::io(format!("flushing tar {tar_path:?}"), e.into_error()))?;
    Ok(())
}

struct TreeWriter<'a, W: Write> {
    root: &'a Dir,
    src_dir: &'a Path,
    skip: Option<SkipFn<'a>>,
    /// Multi-link inodes already archived, by (dev, ino), and their name.
    linked: HashMap<(u64, u64), Vec<u8>>,
    b: &'a mut tar::Builder<W>,
}

fn rel_name(rel: &Path) -> String {
    rel.to_string_lossy().into_owned()
}

fn dev_parts(rdev: u64) -> (u32, u32) {
    #[cfg(target_os = "linux")]
    {
        (
            libc::major(rdev as libc::dev_t),
            libc::minor(rdev as libc::dev_t),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        (((rdev >> 24) & 0xff) as u32, (rdev & 0xff_ffff) as u32)
    }
}

impl<W: Write> TreeWriter<'_, W> {
    /// Archives the children of `dir` in lexical order, so identical trees
    /// give byte-identical archives.
    fn walk(&mut self, dir: &Path) -> Result<()> {
        let listing = if dir.as_os_str().is_empty() {
            self.root.entries()
        } else {
            self.root.read_dir(dir)
        }
        .map_err(|e| Error::io(format!("reading directory {:?}", self.src_dir.join(dir)), e))?;
        let mut names: Vec<_> = Vec::new();
        for e in listing {
            let e = e.map_err(|e| {
                Error::io(format!("reading directory {:?}", self.src_dir.join(dir)), e)
            })?;
            names.push(e.file_name());
        }
        names.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        for name in names {
            let rel = dir.join(&name);
            if let Some(skip) = self.skip {
                if skip(&rel_name(&rel)) {
                    continue;
                }
            }
            if self.entry(&rel)? {
                self.walk(&rel)?;
            }
        }
        Ok(())
    }

    /// Archives one path; returns whether it is a directory to descend.
    fn entry(&mut self, rel: &Path) -> Result<bool> {
        let is_root = rel.as_os_str().is_empty();
        let path = self.src_dir.join(rel);
        let md = if is_root {
            self.root.dir_metadata()
        } else {
            self.root.symlink_metadata(rel)
        }
        .map_err(|e| Error::io(format!("lstat {path:?}"), e))?;
        let ft = md.file_type();
        // A socket carries no data and is meaningless without its listener,
        // which is gone by restore time; agents leave them around routinely,
        // and failing on one would make the actor impossible to suspend.
        if ft.is_socket() {
            tracing::warn!(path = %path.display(), root = %self.src_dir.display(), "Skipping socket while archiving directory");
            return Ok(false);
        }
        let mut h = Header::new_gnu();
        h.set_mode(md.mode() & 0o7777);
        h.set_uid(md.uid() as u64);
        h.set_gid(md.gid() as u64);
        h.set_mtime(md.mtime().max(0) as u64);
        h.set_size(0);
        let mut name = if is_root {
            b"./".to_vec()
        } else {
            rel.as_os_str().as_bytes().to_vec()
        };

        // Symlinks are exempt: Linux refuses user.* on them, and overlay
        // metadata never targets them.
        let xattr_rel = if is_root { Path::new(".") } else { rel };
        if !ft.is_symlink() {
            let attrs = xattr::read(self.root, xattr_rel)
                .map_err(|e| Error::io(format!("reading xattrs of {path:?}"), e))?;
            if !attrs.is_empty() {
                let recs: Vec<(String, Vec<u8>)> = attrs
                    .into_iter()
                    .map(|(k, v)| (format!("{PAX_XATTR_PREFIX}{k}"), v))
                    .collect();
                self.b
                    .append_pax_extensions(recs.iter().map(|(k, v)| (k.as_str(), v.as_slice())))
                    .map_err(|e| Error::io(format!("writing tar header for {path:?}"), e))?;
            }
        }

        if ft.is_file() {
            if md.nlink() > 1 {
                let key = (md.dev(), md.ino());
                if let Some(target) = self.linked.get(&key).cloned() {
                    h.set_entry_type(EntryType::Link);
                    return self
                        .put(h, &name, Some(&target), io::empty(), &path)
                        .map(|_| false);
                }
                self.linked.insert(key, name.clone());
            }
            h.set_entry_type(EntryType::Regular);
            h.set_size(md.len());
            let f = open_regular(self.root, rel)
                .map_err(|e| Error::io(format!("opening {path:?}"), e))?;
            let want = md.len();
            let mut counted = Counted {
                inner: f.take(want),
                n: 0,
            };
            self.put(h, &name, None, &mut counted, &path)?;
            if counted.n != want {
                return Err(Error::msg(format!(
                    "archiving contents of {path:?}: file changed size while archiving"
                )));
            }
            return Ok(false);
        }
        if ft.is_char_device() || ft.is_block_device() {
            let (major, minor) = dev_parts(md.rdev());
            // Only whiteouts are archived: extraction refuses any other
            // device, so archiving one would make the snapshot unrestorable.
            if !(ft.is_char_device() && major == 0 && minor == 0) {
                tracing::warn!(path = %path.display(), root = %self.src_dir.display(), "Skipping device node while archiving directory");
                return Ok(false);
            }
            h.set_entry_type(EntryType::Char);
            h.set_device_major(0).ok();
            h.set_device_minor(0).ok();
            self.put(h, &name, None, io::empty(), &path)?;
            return Ok(false);
        }
        if ft.is_dir() {
            h.set_entry_type(EntryType::Directory);
            if !is_root {
                name.push(b'/');
            }
            self.put(h, &name, None, io::empty(), &path)?;
            return Ok(!is_root);
        }
        if ft.is_symlink() {
            let target = self
                .root
                .read_link_contents(rel)
                .map_err(|e| Error::io(format!("reading symlink {path:?}"), e))?;
            h.set_entry_type(EntryType::Symlink);
            self.put(
                h,
                &name,
                Some(target.as_os_str().as_bytes()),
                io::empty(),
                &path,
            )?;
            return Ok(false);
        }
        if ft.is_fifo() {
            h.set_entry_type(EntryType::Fifo);
            self.put(h, &name, None, io::empty(), &path)?;
            return Ok(false);
        }
        Err(Error::msg(format!("unsupported file type at {path:?}")))
    }

    /// Writes `h` with its name and link written verbatim, preceded by GNU
    /// long-name records when they do not fit the header.
    fn put(
        &mut self,
        mut h: Header,
        name: &[u8],
        link: Option<&[u8]>,
        data: impl Read,
        path: &Path,
    ) -> Result<()> {
        let fail = |e: io::Error| Error::io(format!("writing tar header for {path:?}"), e);
        let old = h.as_old_mut();
        if name.len() > old.name.len() {
            self.long_record(EntryType::GNULongName, name)
                .map_err(fail)?;
        }
        put_truncated(&mut h.as_old_mut().name, name);
        if let Some(link) = link {
            if link.len() > h.as_old().linkname.len() {
                self.long_record(EntryType::GNULongLink, link)
                    .map_err(fail)?;
            }
            put_truncated(&mut h.as_old_mut().linkname, link);
        }
        h.set_cksum();
        self.b.append(&h, data).map_err(fail)
    }

    fn long_record(&mut self, kind: EntryType, value: &[u8]) -> io::Result<()> {
        let mut h = Header::new_gnu();
        put_truncated(&mut h.as_old_mut().name, b"././@LongLink");
        h.set_mode(0o644);
        h.set_entry_type(kind);
        h.set_size(value.len() as u64 + 1);
        h.set_cksum();
        self.b.append(&h, value.chain(&[0u8][..]))
    }
}

fn put_truncated(dst: &mut [u8], src: &[u8]) {
    dst.fill(0);
    let n = src.len().min(dst.len());
    dst[..n].copy_from_slice(&src[..n]);
}

struct Counted<R> {
    inner: R,
    n: u64,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.n += n as u64;
        Ok(n)
    }
}

/// Opens `rel` for reading without following a final symlink, failing
/// unless it is still a regular file. Non-blocking, so a FIFO swapped in
/// since the walk cannot block the open.
fn open_regular(root: &Dir, rel: &Path) -> io::Result<File> {
    let f = confined::at_parent(root, rel, |fd, name| {
        // SAFETY: a valid directory fd and NUL-terminated name.
        let r = unsafe {
            libc::openat(
                fd,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned a new fd we own.
        Ok(unsafe { File::from_raw_fd(r) })
    })?;
    if !f.metadata()?.is_file() {
        return Err(io::Error::other("no longer a regular file"));
    }
    Ok(f)
}

/// Unpacks `tar_path` into the existing `dst_dir`. Modes, ownership, times,
/// symlinks and hardlinks are restored. An entry colliding with an existing
/// path replaces it, except that a directory is kept for a directory entry.
pub fn extract(tar_path: &Path, dst_dir: &Path) -> Result<()> {
    let f = File::open(tar_path).map_err(|e| Error::io(format!("opening tar {tar_path:?}"), e))?;
    let root = confined::open_root(dst_dir)
        .map_err(|e| Error::io(format!("opening destination {dst_dir:?}"), e))?;
    let mut archive = tar::Archive::new(BufReader::with_capacity(STREAM_BUF_SIZE, f));
    // Directory metadata is applied once every child exists.
    let mut dirs: BTreeMap<PathBuf, Meta> = BTreeMap::new();
    let entries = archive
        .entries()
        .map_err(|e| Error::io(format!("reading tar {tar_path:?}"), e))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| Error::io(format!("reading tar {tar_path:?}"), e))?;
        let raw = entry.path_bytes().into_owned();
        let meta =
            Meta::of(&mut entry).map_err(|e| Error::io(format!("reading tar {tar_path:?}"), e))?;
        let name = match confined::validate_name(&raw) {
            Err(e) => {
                return Err(Error::msg(format!(
                    "invalid entry in tar {tar_path:?}: {e}"
                )))
            }
            Ok(None) => {
                // A "./" directory entry carries the destination's own
                // metadata; anything else at the root is skipped.
                if !raw.is_empty() && meta.kind == EntryType::Directory {
                    dirs.insert(PathBuf::from("."), meta);
                }
                continue;
            }
            Ok(Some(n)) => n,
        };
        extract_entry(&root, &mut entry, meta, &name, &mut dirs)?;
    }
    restore_dir_meta(&root, dirs)
}

/// The metadata of one archive entry.
#[derive(Debug, Clone)]
struct Meta {
    kind: EntryType,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: Option<SystemTime>,
    size: u64,
    link: Option<Vec<u8>>,
    devmajor: u32,
    devminor: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
}

impl Meta {
    fn of<R: Read>(entry: &mut tar::Entry<'_, R>) -> io::Result<Meta> {
        let mut xattrs = BTreeMap::new();
        if let Some(exts) = entry.pax_extensions()? {
            for ext in exts {
                let ext = ext?;
                if let Ok(k) = ext.key() {
                    if let Some(attr) = k.strip_prefix(PAX_XATTR_PREFIX) {
                        if keep_xattr(attr) {
                            xattrs.insert(attr.to_string(), ext.value_bytes().to_vec());
                        }
                    }
                }
            }
        }
        let h = entry.header();
        let mut kind = h.entry_type();
        if h.as_old().linkflag[0] == 0 && entry.path_bytes().ends_with(b"/") {
            kind = EntryType::Directory;
        }
        Ok(Meta {
            kind,
            mode: h.mode().unwrap_or(0) & 0o7777,
            uid: h.uid().unwrap_or(0) as u32,
            gid: h.gid().unwrap_or(0) as u32,
            mtime: match h.mtime() {
                Ok(0) | Err(_) => None,
                Ok(s) => Some(SystemTime::UNIX_EPOCH + Duration::from_secs(s)),
            },
            size: h.size().unwrap_or(0),
            link: entry.link_name_bytes().map(|b| b.into_owned()),
            devmajor: h.device_major().ok().flatten().unwrap_or(0),
            devminor: h.device_minor().ok().flatten().unwrap_or(0),
            xattrs,
        })
    }

    fn is_whiteout(&self) -> bool {
        self.kind == EntryType::Char && self.devmajor == 0 && self.devminor == 0
    }
}

fn extract_entry<R: Read>(
    root: &Dir,
    entry: &mut tar::Entry<'_, R>,
    meta: Meta,
    name: &Path,
    dirs: &mut BTreeMap<PathBuf, Meta>,
) -> Result<()> {
    let perm = meta.mode & 0o777;
    match meta.kind {
        EntryType::Directory => {
            match confined::mkdir(root, name, perm | 0o700) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(Error::io(format!("creating directory {name:?}"), e)),
            }
            dirs.insert(name.to_path_buf(), meta);
            Ok(())
        }
        EntryType::Regular => {
            replace_existing(root, name)?;
            let mut out = confined::create_file(root, name, perm)
                .map_err(|e| Error::io(format!("creating file {name:?}"), e))?;
            let n = io::copy(entry, &mut out)
                .map_err(|e| Error::io(format!("writing contents of {name:?}"), e))?;
            if n != meta.size {
                return Err(Error::msg(format!(
                    "writing contents of {name:?}: unexpected EOF"
                )));
            }
            drop(out);
            restore_meta(root, name, &meta)
        }
        EntryType::Symlink => {
            replace_existing(root, name)?;
            let target = PathBuf::from(std::ffi::OsStr::from_bytes(
                meta.link.as_deref().unwrap_or_default(),
            ));
            root.symlink_contents(&target, name)
                .map_err(|e| Error::io(format!("creating symlink {name:?} -> {target:?}"), e))?;
            // Only ownership applies to a symlink itself.
            lchown_entry(root, name, &meta)
        }
        EntryType::Fifo => {
            replace_existing(root, name)?;
            confined::mkfifo(root, name, perm)
                .map_err(|e| Error::io(format!("creating fifo {name:?}"), e))?;
            restore_meta(root, name, &meta)
        }
        EntryType::Link => {
            let raw = meta.link.clone().unwrap_or_default();
            let target = match confined::validate_name(&raw) {
                Ok(Some(t)) => t,
                _ => {
                    return Err(Error::msg(format!(
                        "invalid hardlink target {:?} for {name:?}",
                        String::from_utf8_lossy(&raw)
                    )))
                }
            };
            replace_existing(root, name)?;
            root.hard_link(&target, root, name)
                .map_err(|e| Error::io(format!("creating hardlink {name:?} -> {target:?}"), e))
        }
        EntryType::Char | EntryType::Block => {
            // A real device node would hand host hardware to whoever reaches
            // the tree; whiteouts are the only devices a snapshot carries.
            if !meta.is_whiteout() {
                return Err(Error::msg(format!(
                    "refusing device node {name:?} ({}:{}): only 0:0 character devices (overlay whiteouts) are allowed",
                    meta.devmajor, meta.devminor
                )));
            }
            replace_existing(root, name)?;
            confined::mknod(root, name, libc::S_IFCHR, perm, 0, 0)
                .map_err(|e| Error::io(format!("creating device node {name:?}"), e))?;
            restore_meta(root, name, &meta)
        }
        other => Err(Error::msg(format!(
            "unsupported tar entry type {:?} at {name:?}",
            (other.as_byte() as char).to_string()
        ))),
    }
}

/// Removes whatever occupies `name`, so a new entry never writes through a
/// symlink, truncates a shared inode, or collides with a directory.
fn replace_existing(root: &Dir, name: &Path) -> Result<()> {
    match confined::lexists(root, name) {
        Ok(false) => Ok(()),
        Ok(true) => confined::remove_all(root, name)
            .map_err(|e| Error::io(format!("replacing existing path {name:?}"), e)),
        Err(e) => Err(Error::io(format!("checking existing path {name:?}"), e)),
    }
}

/// Applies directory metadata deepest first (a parent's mode cannot then
/// block its children), the root last.
fn restore_dir_meta(root: &Dir, dirs: BTreeMap<PathBuf, Meta>) -> Result<()> {
    let mut names: Vec<(PathBuf, Meta)> = dirs.into_iter().collect();
    names.sort_by(|(a, _), (b, _)| {
        let (ra, rb) = (a.as_os_str() == ".", b.as_os_str() == ".");
        ra.cmp(&rb)
            .then_with(|| b.as_os_str().len().cmp(&a.as_os_str().len()))
    });
    for (name, meta) in names {
        restore_meta(root, &name, &meta)?;
    }
    Ok(())
}

/// Ownership, then mode (chown clears setuid/setgid, which the chmod puts
/// back), then times, then xattrs. The mode keeps setuid, setgid and
/// sticky: a setgid data directory that lost the bit would hand later
/// files the wrong group. These bits govern behavior inside the sandbox
/// only, and the workload could set them itself.
fn restore_meta(root: &Dir, name: &Path, meta: &Meta) -> Result<()> {
    lchown_entry(root, name, meta)?;
    chmod_at(root, name, meta.mode)
        .map_err(|e| Error::io(format!("restoring mode on {name:?}"), e))?;
    if let Some(t) = meta.mtime {
        confined::lutimes(root, name, t)
            .map_err(|e| Error::io(format!("restoring times on {name:?}"), e))?;
    }
    if !meta.xattrs.is_empty() {
        xattr::restore(root, name, &meta.xattrs)
            .map_err(|e| Error::io(format!("restoring xattrs on {name:?}"), e))?;
    }
    Ok(())
}

/// chmod relative to the parent: never opens the entry (which would block
/// on a FIFO), and never follows a final symlink, which entries restored
/// here are not.
fn chmod_at(root: &Dir, name: &Path, mode: u32) -> io::Result<()> {
    confined::at_parent(root, name, |fd, base| {
        // SAFETY: a valid directory fd and NUL-terminated name.
        let r = unsafe { libc::fchmodat(fd, base.as_ptr(), mode as libc::mode_t, 0) };
        if r == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    })
}

/// Restores ownership without following symlinks. Only a privileged
/// process can give files away; an unprivileged one (tests, local tools)
/// keeps the files as its own rather than failing.
fn lchown_entry(root: &Dir, name: &Path, meta: &Meta) -> Result<()> {
    match confined::lchown(root, name, meta.uid, meta.gid) {
        Ok(()) => Ok(()),
        // SAFETY: no preconditions.
        Err(e)
            if e.kind() == io::ErrorKind::PermissionDenied && unsafe { libc::geteuid() } != 0 =>
        {
            Ok(())
        }
        Err(e) => Err(Error::io(
            format!(
                "restoring ownership of {name:?} to {}:{}",
                meta.uid, meta.gid
            ),
            e,
        )),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
