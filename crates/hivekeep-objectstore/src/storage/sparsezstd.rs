//! The sparse-extent snapshot format.
//!
//! ```text
//! magic[8] | version:u32 | zstd( totalSize:i64 | (off:i64, len:i64, data[len])* | -1:i64 )
//! ```
//!
//! The magic and version are in the clear so a reader can dispatch on them;
//! what follows is zstd frames over the metadata interleaved with only the
//! populated extents' data (holes are neither read nor compressed), ended by
//! an offset of -1. A guest memory image is mostly free RAM, so this cuts
//! the compression from the whole logical image (say 2 GiB) to its resident
//! set (say 150 MiB). All integers are little-endian.

use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

use tokio_util::sync::CancellationToken;

use super::parzstd::{par_zstd_chunk_size, ParZstd};
use super::sparse::{sparse_extents, ExtentRange};
use crate::error::Error;

/// Marks the sparse-extent format. Eight bytes that are not a zstd frame
/// magic, so a reader tells this format from a plain zstd stream.
pub const SPARSE_MAGIC: &[u8; 8] = b"HKSPARSE";
/// The format version, a little-endian u32 right after the magic. Bump it on
/// any incompatible change so readers refuse what they cannot parse.
pub const SPARSE_VERSION: u32 = 2;
/// The end-of-stream sentinel offset.
pub const SPARSE_END_OFFSET: i64 = -1;

/// A writer that stops between writes once `cancel` fires, so compression
/// halts even while an asynchronous destination drains queued frames.
pub struct CancelWriter<W> {
    /// The destination.
    pub dst: W,
    /// Stops the writes.
    pub cancel: CancellationToken,
}

impl<W: Write> Write for CancelWriter<W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(Error::canceled().into());
        }
        self.dst.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.dst.flush()
    }
}

/// Encodes the sparse file `src` to `dst`, returning the logical size and the
/// populated bytes read.
pub fn write_sparse_zstd(
    cancel: &CancellationToken,
    dst: &mut dyn Write,
    src: &mut File,
    workers: i64,
) -> io::Result<(i64, i64)> {
    let size = src.metadata()?.len() as i64;
    let (exts, populated) = sparse_extents(src, size)?;
    src.seek(SeekFrom::Start(0))?;
    write_sparse_extents(cancel, dst, src, size, &exts, populated, workers)
}

/// Encodes the already scanned extents of `src`. Reads move the file
/// position, so a caller can see how far compression got.
pub fn write_sparse_extents(
    cancel: &CancellationToken,
    dst: &mut dyn Write,
    src: &mut File,
    size: i64,
    exts: &[ExtentRange],
    populated: i64,
    workers: i64,
) -> io::Result<(i64, i64)> {
    {
        let mut header = Vec::with_capacity(12);
        header.extend_from_slice(SPARSE_MAGIC);
        header.extend_from_slice(&SPARSE_VERSION.to_le_bytes());
        dst.write_all(&header)?;
        // Hand the header over now: a destination that fails early is seen
        // before a whole chunk has been read.
        dst.flush()?;
    }
    let mut zw = ParZstd::with_chunk(
        CancelWriter {
            dst: &mut *dst,
            cancel: cancel.clone(),
        },
        workers,
        par_zstd_chunk_size(populated),
    );
    let mut data = 0i64;
    let res = (|| -> io::Result<()> {
        zw.write_all(&size.to_le_bytes())?;
        let mut buf = vec![0u8; 1 << 20];
        for e in exts {
            zw.write_all(&e.off.to_le_bytes())?;
            zw.write_all(&e.length.to_le_bytes())?;
            src.seek(SeekFrom::Start(e.off as u64))?;
            let mut left = e.length;
            while left > 0 {
                if cancel.is_cancelled() {
                    return Err(Error::canceled().into());
                }
                let want = left.min(buf.len() as i64) as usize;
                let n = src.read(&mut buf[..want])?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("reading extent @{}+{}: unexpected EOF", e.off, e.length),
                    ));
                }
                zw.write_all(&buf[..n])?;
                data += n as i64;
                left -= n as i64;
            }
        }
        zw.write_all(&SPARSE_END_OFFSET.to_le_bytes())?;
        Ok(())
    })();
    match res {
        Ok(()) => zw.close()?,
        Err(e) => {
            let _ = zw.close();
            return Err(e);
        }
    }
    Ok((size, data))
}

fn read_i64(r: &mut dyn Read, what: &str) -> io::Result<i64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)
        .map_err(|e| io::Error::new(e.kind(), format!("reading {what}: {e}")))?;
    Ok(i64::from_le_bytes(b))
}

/// Decodes the sparse-extent format into `dst`, which becomes a sparse file:
/// the holes between extents are never written. `src` is positioned just
/// after the magic. `dst` is sized to the logical size first, so trailing
/// holes and the exact size are kept.
pub fn read_sparse_zstd(dst: &mut File, src: &mut dyn Read) -> io::Result<i64> {
    let mut v = [0u8; 4];
    src.read_exact(&mut v)
        .map_err(|e| io::Error::new(e.kind(), format!("reading sparse format version: {e}")))?;
    let ver = u32::from_le_bytes(v);
    if ver != SPARSE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported sparse snapshot format version {ver} (this build supports {SPARSE_VERSION})"),
        ));
    }
    let mut zr = zstd::stream::read::Decoder::new(src)?;
    let size = read_i64(&mut zr, "totalSize")?;
    if size < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("negative totalSize {size}"),
        ));
    }
    dst.set_len(0)?;
    dst.set_len(size as u64)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let off = read_i64(&mut zr, "extent offset")?;
        if off == SPARSE_END_OFFSET {
            break;
        }
        let length = read_i64(&mut zr, "extent length")?;
        // The stream is downloaded data: an extent out of range would write
        // past the file or wrap the arithmetic.
        if off < 0 || length < 0 || off > size || length > size - off {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("sparse extent out of range (off={off} len={length} size={size})"),
            ));
        }
        let mut at = off as u64;
        let mut left = length as usize;
        while left > 0 {
            let n = left.min(buf.len());
            zr.read_exact(&mut buf[..n]).map_err(|e| {
                io::Error::new(e.kind(), format!("writing extent @{off}+{length}: {e}"))
            })?;
            dst.write_all_at(&buf[..n], at)?;
            at += n as u64;
            left -= n;
        }
    }
    Ok(size)
}

/// Writes one upload part's share of the sparse-extent stream: the header and
/// total size when `first`, its own extents, and the sentinel when `last`.
/// Reads use positional I/O so every part can read the file concurrently.
/// Each part is sized frames, so a restore can split it and decode them in
/// parallel.
pub fn write_sparse_part(
    dst: &mut dyn Write,
    src: &File,
    size: i64,
    ranges: &[ExtentRange],
    first: bool,
    last: bool,
) -> io::Result<()> {
    if first {
        let mut header = Vec::with_capacity(12);
        header.extend_from_slice(SPARSE_MAGIC);
        header.extend_from_slice(&SPARSE_VERSION.to_le_bytes());
        dst.write_all(&header)?;
    }
    let mut zw = ParZstd::new(BufWriter::with_capacity(64 << 10, dst), 1);
    let res = (|| -> io::Result<()> {
        if first {
            zw.write_all(&size.to_le_bytes())?;
        }
        let mut buf = vec![0u8; 1 << 20];
        for e in ranges {
            zw.write_all(&e.off.to_le_bytes())?;
            zw.write_all(&e.length.to_le_bytes())?;
            let (mut at, end) = (e.off, e.off + e.length);
            while at < end {
                let n = (end - at).min(buf.len() as i64) as usize;
                let read = src.read_at(&mut buf[..n], at as u64).map_err(|err| {
                    io::Error::new(
                        err.kind(),
                        format!("reading extent @{}+{}: {err}", e.off, e.length),
                    )
                })?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("reading extent @{}+{}: unexpected EOF", e.off, e.length),
                    ));
                }
                zw.write_all(&buf[..read])?;
                at += read as i64;
            }
        }
        if last {
            zw.write_all(&SPARSE_END_OFFSET.to_le_bytes())?;
        }
        Ok(())
    })();
    match res {
        Ok(()) => {
            let mut w = zw.finish()?;
            w.flush()
        }
        Err(e) => {
            let _ = zw.close();
            Err(e)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::parzstd::{tests::FailAfter, PAR_ZSTD_CHUNK};
    use crate::storage::sparse::plan_sparse_parts;
    use crate::storage::testdata::*;
    use std::io::Cursor;

    const M: i64 = 1 << 20;

    fn token() -> CancellationToken {
        CancellationToken::new()
    }

    /// Expands an encoded sparse stream back to the full file contents.
    pub fn decode_sparse(stream: &[u8]) -> Vec<u8> {
        assert!(stream.starts_with(SPARSE_MAGIC), "bad magic");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("decoded");
        let mut out = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        read_sparse_zstd(&mut out, &mut Cursor::new(&stream[SPARSE_MAGIC.len()..])).unwrap();
        std::fs::read(path).unwrap()
    }

    /// Concatenates every part's output, as the composed object holds them.
    pub fn encode_snapshot(src: &File, logical: i64, groups: &[Vec<ExtentRange>]) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, g) in groups.iter().enumerate() {
            write_sparse_part(&mut out, src, logical, g, i == 0, i == groups.len() - 1).unwrap();
        }
        out
    }

    /// The same parts written as one unsized streaming frame each, the
    /// layout earlier writers used; restores must keep decoding it.
    fn encode_snapshot_old_format(
        src: &File,
        logical: i64,
        groups: &[Vec<ExtentRange>],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; 1 << 20];
        for (i, g) in groups.iter().enumerate() {
            if i == 0 {
                out.extend_from_slice(SPARSE_MAGIC);
                out.extend_from_slice(&SPARSE_VERSION.to_le_bytes());
            }
            let mut zw = zstd::stream::write::Encoder::new(&mut out, 1).unwrap();
            if i == 0 {
                zw.write_all(&logical.to_le_bytes()).unwrap();
            }
            for e in g {
                zw.write_all(&e.off.to_le_bytes()).unwrap();
                zw.write_all(&e.length.to_le_bytes()).unwrap();
                let (mut at, end) = (e.off, e.off + e.length);
                while at < end {
                    let n = src
                        .read_at(&mut buf[..(end - at).min(1 << 20) as usize], at as u64)
                        .unwrap();
                    zw.write_all(&buf[..n]).unwrap();
                    at += n as i64;
                }
            }
            if i == groups.len() - 1 {
                zw.write_all(&SPARSE_END_OFFSET.to_le_bytes()).unwrap();
            }
            zw.finish().unwrap();
        }
        out
    }

    #[test]
    fn sparse_zstd_round_trip() {
        let cases: Vec<(&str, i64, Vec<Region>)> = vec![
            ("empty", 0, vec![]),
            ("all-hole", 4 * M, vec![]),
            (
                "all-data",
                256 << 10,
                vec![Region {
                    off: 0,
                    len: 256 << 10,
                    fill: 0xAB,
                }],
            ),
            (
                "leading-hole",
                2 * M,
                vec![Region {
                    off: M,
                    len: 64 << 10,
                    fill: 0x11,
                }],
            ),
            (
                "trailing-hole",
                2 * M,
                vec![Region {
                    off: 0,
                    len: 64 << 10,
                    fill: 0x22,
                }],
            ),
            (
                "single-extent-midfile",
                4 * M,
                vec![Region {
                    off: 2 * M,
                    len: 70000,
                    fill: 0x33,
                }],
            ),
            (
                "multi-extent",
                8 * M,
                vec![
                    Region {
                        off: 0,
                        len: 4096,
                        fill: 0x44,
                    },
                    Region {
                        off: 2 * M,
                        len: 100000,
                        fill: 0x55,
                    },
                    Region {
                        off: 5 * M,
                        len: 8192,
                        fill: 0x66,
                    },
                ],
            ),
        ];
        for (name, size, regions) in cases {
            let dir = tempfile::tempdir().unwrap();
            let src_path = dir.path().join("src");
            write_sparse_source(&src_path, size, &regions);
            let mut src = File::open(&src_path).unwrap();
            let mut buf = Vec::new();
            let (logical, _) = write_sparse_zstd(&token(), &mut buf, &mut src, 2).unwrap();
            assert_eq!(logical, size, "{name}");
            assert!(buf.starts_with(SPARSE_MAGIC), "{name}: missing magic");
            let dst_path = dir.path().join("dst");
            let mut dst = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dst_path)
                .unwrap();
            let got =
                read_sparse_zstd(&mut dst, &mut Cursor::new(&buf[SPARSE_MAGIC.len()..])).unwrap();
            assert_eq!(got, size, "{name}");
            assert_eq!(
                std::fs::read(&dst_path).unwrap(),
                std::fs::read(&src_path).unwrap(),
                "{name}: mismatch"
            );
        }
    }

    /// Builds a stream after the magic with arbitrary header values.
    fn craft(version: u32, size: i64, frames: &[(i64, i64)], sentinel: bool) -> Vec<u8> {
        let mut out = version.to_le_bytes().to_vec();
        let mut zw = zstd::stream::write::Encoder::new(&mut out, 1).unwrap();
        zw.write_all(&size.to_le_bytes()).unwrap();
        for &(off, len) in frames {
            zw.write_all(&off.to_le_bytes()).unwrap();
            zw.write_all(&len.to_le_bytes()).unwrap();
            if len > 0 {
                zw.write_all(&vec![0u8; len as usize]).unwrap();
            }
        }
        if sentinel {
            zw.write_all(&SPARSE_END_OFFSET.to_le_bytes()).unwrap();
        }
        zw.finish().unwrap();
        out
    }

    fn read_err(stream: &[u8]) -> String {
        let dir = tempfile::tempdir().unwrap();
        let mut dst = File::create(dir.path().join("dst")).unwrap();
        read_sparse_zstd(&mut dst, &mut Cursor::new(stream))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn read_sparse_zstd_rejects_malformed() {
        for (name, stream, want) in [
            (
                "wrong version",
                craft(SPARSE_VERSION + 1, 0, &[], true),
                "unsupported sparse snapshot format version",
            ),
            (
                "negative size",
                craft(SPARSE_VERSION, -1, &[], true),
                "negative totalSize",
            ),
            (
                "extent past end",
                craft(SPARSE_VERSION, 100, &[(50, 100)], true),
                "out of range",
            ),
            (
                "negative extent offset",
                craft(SPARSE_VERSION, 100, &[(-5, 1)], true),
                "out of range",
            ),
        ] {
            let e = read_err(&stream);
            assert!(e.contains(want), "{name}: {e}");
        }
    }

    #[test]
    fn read_sparse_zstd_truncated() {
        let stream = craft(SPARSE_VERSION, 1 << 20, &[(0, 4096)], false);
        read_err(&stream);
    }

    #[test]
    fn sparse_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory-ranges");
        let f = File::create(&path).unwrap();
        f.set_len(1 << 20).unwrap();
        f.write_all_at(b"hello sparse", 4096).unwrap();
        f.sync_all().unwrap();
        let mut f = File::open(&path).unwrap();
        let mut blob = Vec::new();
        write_sparse_zstd(&token(), &mut blob, &mut f, 2).unwrap();
        blob[SPARSE_MAGIC.len()] ^= 0xFF;
        read_err(&blob[SPARSE_MAGIC.len()..]);
    }

    #[test]
    fn write_sparse_zstd_skips_holes() {
        let size = 8 * M;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory-ranges");
        let data: Vec<u8> = (0..70000).map(|i| (i % 251 + 1) as u8).collect();
        {
            let f = File::create(&path).unwrap();
            f.set_len(size as u64).unwrap();
            f.write_all_at(&data, 2 << 20).unwrap();
            f.sync_all().unwrap();
        }
        let blk = disk_blocks(&path);
        if blk == 0 || blk * 512 >= size as u64 {
            eprintln!("filesystem did not make the source sparse; skipping");
            return;
        }
        let mut src = File::open(&path).unwrap();
        let mut buf = Vec::new();
        let (logical, data_bytes) = write_sparse_zstd(&token(), &mut buf, &mut src, 2).unwrap();
        assert_eq!(logical, size);
        assert!(
            data_bytes < size / 2,
            "fed {data_bytes} bytes; holes not skipped"
        );
    }

    #[test]
    fn write_sparse_zstd_one_worker_dst_fails() {
        for (name, size) in [("tail_4KiB", 4usize << 10), ("chunk_9MiB", 9 << 20)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("src");
            std::fs::write(&path, noise(size as u64, size)).unwrap();
            let mut src = File::open(&path).unwrap();
            // The header write succeeds; the first frame fails.
            let mut dst = FailAfter { n: 1, msg: "boom" };
            let e = write_sparse_zstd(&token(), &mut dst, &mut src, 1).unwrap_err();
            assert!(e.to_string().contains("boom"), "{name}: {e}");
        }
    }

    #[test]
    fn sparse_parts_round_trip() {
        for (name, size, writes, parts) in [
            ("one part", 8 * M, vec![0, 4 * M], 1),
            ("two parts", 8 * M, vec![0, 4 * M], 2),
            ("more parts than extents", 16 * M, vec![M, 9 * M], 5),
            ("leading and trailing holes", 32 * M, vec![8 * M], 3),
            ("adjacent extents", 16 * M, vec![0, M, 2 * M], 4),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut src = sparse_file_with(dir.path(), size, &writes);
            let (exts, populated) = sparse_extents(&src, size).unwrap();
            assert!(
                populated >= (writes.len() as i64) << 20,
                "{name}: populated {populated}"
            );
            let groups = plan_sparse_parts(&exts, populated, parts);
            let got = encode_snapshot(&src, size, &groups);
            let mut want = Vec::new();
            src.seek(SeekFrom::Start(0)).unwrap();
            write_sparse_zstd(&token(), &mut want, &mut src, 2).unwrap();
            assert!(
                decode_sparse(&got) == decode_sparse(&want),
                "{name}: range-split output differs"
            );
        }
    }

    #[test]
    fn sparse_parts_write_sized_frames() {
        let (logical, populated, extent) = (80 * M, 40 * M, 4 * M);
        let dir = tempfile::tempdir().unwrap();
        let src = write_snapshot_source(dir.path(), logical, populated, extent);
        let (exts, pop) = sparse_extents(&src, logical).unwrap();
        let groups = plan_sparse_parts(&exts, pop, 2);
        let (frames, sized, max_size) = zstd_frame_stats(&encode_snapshot(&src, logical, &groups));
        assert!(
            frames > groups.len(),
            "{frames} frames for {} parts",
            groups.len()
        );
        assert_eq!(sized, frames);
        assert!(max_size <= PAR_ZSTD_CHUNK as u64);
    }

    #[test]
    fn sparse_snapshot_formats_decode() {
        let (logical, populated, extent) = (80 * M, 40 * M, 4 * M);
        let dir = tempfile::tempdir().unwrap();
        let src = write_snapshot_source(dir.path(), logical, populated, extent);
        let (exts, pop) = sparse_extents(&src, logical).unwrap();
        let groups = plan_sparse_parts(&exts, pop, 2);
        let mut want = vec![0u8; logical as usize];
        src.read_exact_at(&mut want, 0).unwrap();
        assert!(
            decode_sparse(&encode_snapshot(&src, logical, &groups)) == want,
            "own format differs"
        );
        assert!(
            decode_sparse(&encode_snapshot_old_format(&src, logical, &groups)) == want,
            "old format differs"
        );
    }

    // The sparse-part benchmarks (BenchmarkWriteSparsePart,
    // BenchmarkDecodeSparse) measure throughput on a 2 GiB image; they run
    // here as an ignored timing test.
    #[test]
    #[ignore = "benchmark: writes a 2 GiB sparse image"]
    fn bench_sparse_parts() {
        let (logical, populated, extent) = (2048 * M, 512 * M, 8 * M);
        let dir = tempfile::tempdir().unwrap();
        let src = write_snapshot_source(dir.path(), logical, populated, extent);
        let (exts, pop) = sparse_extents(&src, logical).unwrap();
        let groups = plan_sparse_parts(&exts, pop, crate::storage::sparse::sparse_part_count(pop));
        let start = std::time::Instant::now();
        let stream = encode_snapshot(&src, logical, &groups);
        eprintln!("encoded {} MiB in {:?}", pop >> 20, start.elapsed());
        let start = std::time::Instant::now();
        assert_eq!(decode_sparse(&stream).len() as i64, logical);
        eprintln!("decoded in {:?}", start.elapsed());
    }
}
