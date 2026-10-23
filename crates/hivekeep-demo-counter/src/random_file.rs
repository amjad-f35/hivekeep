//! A blob of random bytes on the root filesystem whose hash is logged
//! periodically, so filesystem checkpoint/restore is observable in logs.

use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;

/// Where the random blob lives in the container.
pub const RANDOM_FILE: &str = "/random-content-file";
/// Size of the random blob.
pub const RANDOM_FILE_SIZE: usize = 1024 * 1024;

/// Writes `RANDOM_FILE_SIZE` random bytes to `path`.
pub fn write(path: &Path) -> io::Result<()> {
    let mut buf = vec![0u8; RANDOM_FILE_SIZE];
    rand::rng().fill_bytes(&mut buf);
    std::fs::write(path, buf)
}

/// Unpadded base64 of the SHA-256 of the file at `path`.
pub fn hash(path: &Path) -> io::Result<String> {
    let bytes = std::fs::read(path)?;
    Ok(base64::engine::general_purpose::STANDARD_NO_PAD.encode(Sha256::digest(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_unpadded() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("r");
        write(&p).unwrap();
        assert_eq!(
            std::fs::metadata(&p).unwrap().len() as usize,
            RANDOM_FILE_SIZE
        );
        let h = hash(&p).unwrap();
        assert_eq!(h, hash(&p).unwrap());
        assert_eq!(h.len(), 43);
        assert!(!h.ends_with('='));
    }
}
