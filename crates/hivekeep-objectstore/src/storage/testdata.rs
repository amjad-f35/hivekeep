//! Test fixtures for the compression and upload tests: snapshot-like data,
//! sparse source files and a zstd frame walker.

use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use super::sparsezstd::SPARSE_MAGIC;

fn page_text() -> Vec<u8> {
    let words = [
        "func",
        "return",
        "error",
        "nil",
        "context",
        "string",
        "struct",
        "0x00007f3a",
        "import",
        "package",
        "self",
        "def",
        "None",
        "true",
        "false",
        "{",
        "}",
        "\n\t",
        "for",
        "range",
        "if",
        "else",
        "len",
        "make",
        "[]byte",
        "int64",
        "README",
    ];
    let mut rng = StdRng::seed_from_u64(3);
    let mut b = Vec::new();
    while b.len() < 64 << 10 {
        b.extend_from_slice(words[rng.random_range(0..words.len())].as_bytes());
        b.push(b' ');
    }
    b.truncate(64 << 10);
    b
}

/// Fills `p` with a fixed-seed mix of 4 KiB pages: a quarter zero, a quarter
/// random and half text-like, so it compresses roughly like guest memory.
pub fn fill_snapshot_data(rng: &mut StdRng, p: &mut [u8]) {
    let text = page_text();
    for pg in p.chunks_mut(4096) {
        match rng.random_range(0..4) {
            0 => pg.fill(0),
            1 => rng.fill_bytes(pg),
            _ => {
                let off = rng.random_range(0..text.len() - pg.len());
                pg.copy_from_slice(&text[off..off + pg.len()]);
            }
        }
    }
}

/// `n` snapshot-like bytes from `seed`.
pub fn random_bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut b = vec![0u8; n];
    let mut rng = StdRng::seed_from_u64(seed);
    fill_snapshot_data(&mut rng, &mut b);
    b
}

/// Incompressible bytes from `seed`.
pub fn noise(seed: u64, n: usize) -> Vec<u8> {
    let mut b = vec![0u8; n];
    StdRng::seed_from_u64(seed).fill_bytes(&mut b);
    b
}

/// Feeds `data` to `w` in uneven writes, so chunk boundaries fall inside
/// them.
pub fn write_in_pieces(w: &mut impl Write, mut data: &[u8]) {
    let mut rng = StdRng::seed_from_u64(data.len() as u64);
    while !data.is_empty() {
        let n = data.len().min(1 + rng.random_range(0..3 << 20));
        w.write_all(&data[..n]).unwrap();
        data = &data[n..];
    }
}

/// Decodes a whole zstd stream.
pub fn zstd_decode_all(stream: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if stream.is_empty() {
        return out;
    }
    zstd::stream::read::Decoder::new(stream)
        .unwrap()
        .read_to_end(&mut out)
        .unwrap();
    out
}

/// Walks the zstd frames of `stream` (after the sparse header when present)
/// and reports how many there are, how many declare their content size, and
/// the largest declared size.
pub fn zstd_frame_stats(mut stream: &[u8]) -> (usize, usize, u64) {
    if stream.starts_with(SPARSE_MAGIC) {
        stream = &stream[SPARSE_MAGIC.len() + 4..];
    }
    let (mut frames, mut sized, mut max_size) = (0, 0, 0u64);
    while !stream.is_empty() {
        let magic = u32::from_le_bytes(stream[..4].try_into().unwrap());
        if magic & 0xFFFF_FFF0 == 0x184D_2A50 {
            let n = u32::from_le_bytes(stream[4..8].try_into().unwrap()) as usize;
            stream = &stream[8 + n..];
            continue;
        }
        assert_eq!(magic, 0xFD2F_B528, "frame {frames}: bad magic");
        frames += 1;
        let fhd = stream[4];
        let fcs_flag = fhd >> 6;
        let single = fhd & 0x20 != 0;
        let checksum = fhd & 0x04 != 0;
        let dict = [0usize, 1, 2, 4][(fhd & 3) as usize];
        let fcs_len = match fcs_flag {
            0 => usize::from(single),
            1 => 2,
            2 => 4,
            _ => 8,
        };
        let mut pos = 5 + usize::from(!single) + dict;
        if fcs_len > 0 {
            let mut v = [0u8; 8];
            v[..fcs_len].copy_from_slice(&stream[pos..pos + fcs_len]);
            let mut size = u64::from_le_bytes(v);
            if fcs_len == 2 {
                size += 256;
            }
            sized += 1;
            max_size = max_size.max(size);
        }
        pos += fcs_len;
        loop {
            let bh = u32::from(stream[pos])
                | u32::from(stream[pos + 1]) << 8
                | u32::from(stream[pos + 2]) << 16;
            pos += 3;
            if (bh >> 1) & 3 == 1 {
                pos += 1;
            } else {
                pos += (bh >> 3) as usize;
            }
            if bh & 1 == 1 {
                break;
            }
        }
        if checksum {
            pos += 4;
        }
        stream = &stream[pos..];
    }
    (frames, sized, max_size)
}

/// A run of identical non-zero bytes at `off` in a test source.
#[derive(Debug, Clone, Copy)]
pub struct Region {
    /// Offset.
    pub off: i64,
    /// Length.
    pub len: i64,
    /// The byte repeated.
    pub fill: u8,
}

/// Creates a file of logical `size` with `regions` written in; the gaps stay
/// zero and become holes on a sparse filesystem.
pub fn write_sparse_source(path: &Path, size: i64, regions: &[Region]) {
    let f = File::create(path).unwrap();
    f.set_len(size as u64).unwrap();
    for r in regions {
        f.write_all_at(&vec![r.fill; r.len as usize], r.off as u64)
            .unwrap();
    }
    f.sync_all().unwrap();
}

/// A sparse file of `size` with 1 MiB of random data at each offset, open
/// for reading and writing.
pub fn sparse_file_with(dir: &Path, size: i64, offsets: &[i64]) -> File {
    let path = dir.join(format!("sparse-{}", rand::random::<u64>()));
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    f.set_len(size as u64).unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let mut buf = vec![0u8; 1 << 20];
    for &off in offsets {
        rng.fill_bytes(&mut buf);
        f.write_all_at(&buf, off as u64).unwrap();
    }
    f.sync_all().unwrap();
    f
}

/// A sparse file of `logical` bytes whose `populated` bytes are spread over
/// evenly spaced extents of `extent` bytes.
pub fn write_snapshot_source(dir: &Path, logical: i64, populated: i64, extent: i64) -> File {
    let path = dir.join("snapshot");
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    f.set_len(logical as u64).unwrap();
    let n = populated / extent;
    let stride = logical / n;
    let mut rng = StdRng::seed_from_u64(1);
    let mut buf = vec![0u8; extent as usize];
    for i in 0..n {
        fill_snapshot_data(&mut rng, &mut buf);
        f.write_all_at(&buf, (i * stride) as u64).unwrap();
    }
    f
}

/// The number of 512-byte blocks a file occupies.
pub fn disk_blocks(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).map(|m| m.blocks()).unwrap_or(0)
}
