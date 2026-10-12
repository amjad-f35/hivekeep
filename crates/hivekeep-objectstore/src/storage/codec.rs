//! Compressing snapshot content for upload and restoring it on download.
//!
//! A seekable file is written in the sparse-extent format; any other stream
//! as plain zstd. The download side tells them apart by the leading magic,
//! so snapshots written as plain zstd keep restoring.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;

use tokio_util::sync::CancellationToken;

use super::parzstd::ZSTD_LEVEL;
use super::sparse::ExtentRange;
use super::sparsezstd::{
    read_sparse_zstd, write_sparse_extents, write_sparse_zstd, CancelWriter, SPARSE_MAGIC,
};

/// What to compress.
pub enum Content {
    /// A file, scanned for extents when it is compressed.
    File(File),
    /// A file already scanned.
    Sparse {
        /// The file.
        file: File,
        /// Its logical size.
        size: i64,
        /// Its populated extents.
        exts: Vec<ExtentRange>,
        /// Their total.
        populated: i64,
    },
    /// Any other stream.
    Stream(Box<dyn Read + Send>),
}

/// What [`write_content`] compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteContentResult {
    /// The source's logical size, holes included.
    pub logical_bytes: i64,
    /// The bytes read and compressed: the populated set for a file, the
    /// whole stream otherwise.
    pub populated_bytes: i64,
    /// Whether the sparse-extent format was used.
    pub sparse: bool,
}

/// Compresses `content` into `out` with `workers` compression threads.
pub fn write_content(
    cancel: &CancellationToken,
    out: &mut dyn Write,
    content: &mut Content,
    workers: i64,
) -> io::Result<WriteContentResult> {
    match content {
        Content::Sparse {
            file,
            size,
            exts,
            populated,
        } => {
            let (logical, populated) =
                write_sparse_extents(cancel, out, file, *size, exts, *populated, workers)?;
            Ok(WriteContentResult {
                logical_bytes: logical,
                populated_bytes: populated,
                sparse: true,
            })
        }
        Content::File(f) => {
            let (logical, populated) = write_sparse_zstd(cancel, out, f, workers)?;
            Ok(WriteContentResult {
                logical_bytes: logical,
                populated_bytes: populated,
                sparse: true,
            })
        }
        Content::Stream(r) => {
            let n = plain_zstd(cancel, out, r.as_mut())?;
            Ok(WriteContentResult {
                logical_bytes: n,
                populated_bytes: n,
                sparse: false,
            })
        }
    }
}

/// Writes `src` to `w` as one plain zstd stream and returns the bytes read.
pub fn plain_zstd(
    cancel: &CancellationToken,
    w: &mut dyn Write,
    src: &mut dyn Read,
) -> io::Result<i64> {
    let mut zw = zstd::stream::write::Encoder::new(
        CancelWriter {
            dst: w,
            cancel: cancel.clone(),
        },
        ZSTD_LEVEL,
    )?;
    let n = io::copy(src, &mut zw)?;
    zw.finish()?;
    Ok(n as i64)
}

/// What [`decode_content`] restored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecodeResult {
    /// The logical size written.
    pub logical_bytes: i64,
    /// Non-hole bytes written on the plain-stream file path; zero otherwise.
    pub written_bytes: i64,
    /// Whether the input used the sparse-extent format.
    pub sparse: bool,
}

/// Where decoded content goes.
pub enum DecodeTarget<'a> {
    /// A file, written sparse.
    File(&'a mut File),
    /// Any other writer.
    Writer(&'a mut dyn Write),
}

/// Decompresses `src` into `out`, detecting the format from the magic. A
/// plain stream into a file is written sparse too: all-zero blocks become
/// holes, so only the resident set of a memory image is written.
pub fn decode_content(out: DecodeTarget<'_>, src: &mut dyn Read) -> io::Result<DecodeResult> {
    let mut magic = [0u8; 8];
    let mut n = 0;
    while n < magic.len() {
        match src.read(&mut magic[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("while reading object header: {e}"),
                ));
            }
        }
    }
    if n == magic.len() && &magic == SPARSE_MAGIC {
        let DecodeTarget::File(f) = out else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sparse-extent snapshot requires a file destination",
            ));
        };
        let size = read_sparse_zstd(f, src)
            .map_err(|e| io::Error::new(e.kind(), format!("in sparse-extent decode: {e}")))?;
        return Ok(DecodeResult {
            logical_bytes: size,
            written_bytes: 0,
            sparse: true,
        });
    }
    let head = io::Cursor::new(magic[..n].to_vec());
    let mut chained = head.chain(src);
    let mut zr = zstd::stream::read::Decoder::new(&mut chained)?;
    match out {
        DecodeTarget::File(f) => {
            let (size, written) = copy_zstd_sparse(f, &mut zr)
                .map_err(|e| io::Error::new(e.kind(), format!("in sparse decompress: {e}")))?;
            Ok(DecodeResult {
                logical_bytes: size,
                written_bytes: written,
                sparse: false,
            })
        }
        DecodeTarget::Writer(w) => {
            let size = io::copy(&mut zr, w)?;
            Ok(DecodeResult {
                logical_bytes: size as i64,
                written_bytes: 0,
                sparse: false,
            })
        }
    }
}

/// Copies `src` into `dst` skipping all-zero 64 KiB blocks, so they become
/// holes. `dst` is emptied first, so skipped blocks cannot expose stale
/// bytes, and sized to the logical length at the end. Returns the logical
/// size and the bytes actually written.
pub fn copy_zstd_sparse(dst: &mut File, src: &mut dyn Read) -> io::Result<(i64, i64)> {
    dst.set_len(0)
        .map_err(|e| io::Error::new(e.kind(), format!("truncating dst: {e}")))?;
    const BLOCK: usize = 64 << 10;
    let mut buf = vec![0u8; BLOCK];
    let (mut pos, mut written) = (0u64, 0i64);
    loop {
        let mut n = 0;
        while n < BLOCK {
            match src.read(&mut buf[n..]) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if n > 0 {
            let chunk = &buf[..n];
            if !all_zero(chunk) {
                dst.write_all_at(chunk, pos)?;
                written += n as i64;
            }
            pos += n as u64;
        }
        if n < BLOCK {
            break;
        }
    }
    dst.set_len(pos)?;
    Ok((pos as i64, written))
}

/// Whether `b` is all zero bytes, eight at a time.
pub fn all_zero(b: &[u8]) -> bool {
    let (words, rest) = b.as_chunks::<8>();
    words.iter().all(|w| u64::from_le_bytes(*w) == 0) && rest.iter().all(|&x| x == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::testdata::disk_blocks;
    use std::io::Cursor;

    fn token() -> CancellationToken {
        CancellationToken::new()
    }

    #[test]
    fn copy_zstd_sparse_round_trip() {
        let size = 4usize << 20;
        let mut want = vec![0u8; size];
        for (i, b) in want.iter_mut().enumerate().take(4096) {
            *b = (i % 251 + 1) as u8;
        }
        for (i, b) in want.iter_mut().enumerate().skip(1 << 20).take(9000) {
            *b = (i % 253 + 1) as u8;
        }
        for (i, b) in want.iter_mut().enumerate().skip(size - 5000).take(4000) {
            *b = (i % 249 + 1) as u8;
        }
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("memory-ranges");
        let mut f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&out)
            .unwrap();
        let (logical, written) = copy_zstd_sparse(&mut f, &mut Cursor::new(&want)).unwrap();
        assert_eq!(logical, size as i64);
        assert!(written < size as i64, "written {written}: not sparse");
        assert!(std::fs::read(&out).unwrap() == want, "round-trip mismatch");
        let _ = disk_blocks(&out);
    }

    #[test]
    fn copy_zstd_sparse_clears_stale_data() {
        let size = 2usize << 20;
        let mut want = vec![0u8; size];
        for (i, b) in want.iter_mut().enumerate().take(4096) {
            *b = (i % 251 + 1) as u8;
        }
        for (i, b) in want.iter_mut().enumerate().skip(1 << 20).take(4096) {
            *b = (i % 253 + 1) as u8;
        }
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("dst");
        std::fs::write(&out, vec![0xFFu8; size + size / 2]).unwrap();
        let mut f = File::options().read(true).write(true).open(&out).unwrap();
        copy_zstd_sparse(&mut f, &mut Cursor::new(&want)).unwrap();
        assert!(
            std::fs::read(&out).unwrap() == want,
            "stale data not cleared / wrong size"
        );
    }

    #[test]
    fn write_decode_content_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // Sparse file.
        let size = 8i64 << 20;
        let src_path = dir.path().join("src");
        {
            let f = File::create(&src_path).unwrap();
            f.set_len(size as u64).unwrap();
            let data: Vec<u8> = (0..70000).map(|i| (i % 251 + 1) as u8).collect();
            f.write_all_at(&data, 2 << 20).unwrap();
            f.sync_all().unwrap();
        }
        let mut content = Content::File(File::open(&src_path).unwrap());
        let mut buf = Vec::new();
        let w = write_content(&token(), &mut buf, &mut content, 2).unwrap();
        assert!(w.sparse);
        assert_eq!(w.logical_bytes, size);
        let dst_path = dir.path().join("dst");
        let mut dst = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dst_path)
            .unwrap();
        let d = decode_content(DecodeTarget::File(&mut dst), &mut Cursor::new(&buf)).unwrap();
        assert!(d.sparse);
        assert_eq!(d.logical_bytes, size);
        assert!(std::fs::read(&dst_path).unwrap() == std::fs::read(&src_path).unwrap());

        // Plain reader.
        let want = b"hivekeep payload\n".repeat(1000);
        let mut content = Content::Stream(Box::new(Cursor::new(want.clone())));
        let mut buf = Vec::new();
        let w = write_content(&token(), &mut buf, &mut content, 2).unwrap();
        assert!(!w.sparse);
        let mut out = Vec::new();
        decode_content(DecodeTarget::Writer(&mut out), &mut Cursor::new(&buf)).unwrap();
        assert_eq!(out, want);
    }

    #[test]
    fn all_zero_checks_every_byte() {
        assert!(all_zero(&[0; 17]));
        let mut b = [0u8; 17];
        b[16] = 1;
        assert!(!all_zero(&b));
        b[16] = 0;
        b[3] = 1;
        assert!(!all_zero(&b));
    }
}
