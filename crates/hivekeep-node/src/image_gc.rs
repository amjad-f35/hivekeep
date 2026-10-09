//! What the keeper's image cache eviction loop needs from the cache, and
//! the filesystem usage probe it is driven by.

use async_trait::async_trait;

/// What an image cache eviction pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ImageEvictStats {
    /// Bytes returned to the filesystem.
    pub freed_bytes: i64,
    /// Images evicted.
    pub evicted_images: usize,
    /// Layers evicted.
    pub evicted_layers: usize,
    /// Images considered.
    pub candidates: usize,
    /// Images some bundle still uses.
    pub rooted_images: usize,
    /// Layers no image record names.
    pub orphan_layers: usize,
    /// Images skipped as in use.
    pub skipped_rooted: usize,
    /// Images skipped as younger than the min age.
    pub skipped_fresh: usize,
}

/// Why an eviction pass failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageEvictError {
    /// The cache could not enumerate everything in use, so nothing was
    /// evicted: evicting without the full root set could take a layer a
    /// bundle needs.
    IncompleteEnumeration(String),
    /// Some entries failed; each retries next pass.
    Partial(String),
}

/// What the loop needs from the image cache.
#[async_trait]
pub trait ImageGcStore: Send + Sync + 'static {
    /// The cached layers' total size.
    async fn cache_size(&self) -> Result<i64, String>;
    /// One eviction pass toward `target` bytes.
    async fn evict_unused(
        &self,
        target: i64,
        dry_run: bool,
    ) -> (ImageEvictStats, Option<ImageEvictError>);
}

/// The capacity and available bytes of the filesystem holding `dir`.
pub fn statfs(dir: &std::path::Path) -> Result<(u64, u64), String> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: a valid C string and a zeroed out-parameter.
        let r = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
        if r != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let frsize = st.f_frsize as u64;
        Ok((st.f_blocks as u64 * frsize, st.f_bavail as u64 * frsize))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Err("statfs is unsupported on this platform".into())
    }
}
