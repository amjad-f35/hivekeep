//! Finding a sparse file's populated extents and splitting them into
//! balanced upload parts.
//!
//! A guest memory image is mostly holes, so its populated extents are what
//! actually gets compressed and uploaded. A large one is uploaded as several
//! parts at once; measured on a cloud worker node writing 64 MiB objects, one
//! stream reached 87 MB/s and eight 518 MB/s, so the limit is how early every
//! stream can start.

use std::fs::File;
use std::io;

/// The populated source bytes per part. Smaller parts stop amortizing the
/// quarter second a part upload costs before it moves any data.
pub const SPARSE_PART_TARGET: i64 = 64 << 20;
/// The most parts: past a dozen streams per-stream throughput falls faster
/// than parallelism gains.
pub const SPARSE_MAX_PARTS: usize = 12;

/// A populated byte range of the source file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentRange {
    /// Offset of the first byte.
    pub off: i64,
    /// Length in bytes.
    pub length: i64,
}

#[cfg(unix)]
fn seek(f: &File, off: i64, whence: libc::c_int) -> io::Result<Option<i64>> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: lseek on a valid descriptor has no memory-safety concerns.
    let r = unsafe { libc::lseek(f.as_raw_fd(), off as libc::off_t, whence) };
    if r < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENXIO) {
            return Ok(None);
        }
        return Err(err);
    }
    Ok(Some(r as i64))
}

/// Lists the populated extents of `src` (of logical `size`) with
/// SEEK_DATA/SEEK_HOLE, and their total. Moves the file position; callers
/// that care restore it.
#[cfg(unix)]
pub fn sparse_extents(src: &File, size: i64) -> io::Result<(Vec<ExtentRange>, i64)> {
    let mut exts = Vec::new();
    let mut populated = 0i64;
    let mut off = 0i64;
    while off < size {
        let Some(ds) = seek(src, off, libc::SEEK_DATA)
            .map_err(|e| io::Error::new(e.kind(), format!("SEEK_DATA: {e}")))?
        else {
            // The rest of the file is a hole.
            break;
        };
        if ds >= size {
            break;
        }
        let de = seek(src, ds, libc::SEEK_HOLE)
            .map_err(|e| io::Error::new(e.kind(), format!("SEEK_HOLE: {e}")))?
            .unwrap_or(size)
            .min(size);
        exts.push(ExtentRange {
            off: ds,
            length: de - ds,
        });
        populated += de - ds;
        off = de;
    }
    Ok((exts, populated))
}

/// Without SEEK_DATA the whole file is one extent.
#[cfg(not(unix))]
pub fn sparse_extents(_src: &File, size: i64) -> io::Result<(Vec<ExtentRange>, i64)> {
    if size == 0 {
        return Ok((Vec::new(), 0));
    }
    Ok((
        vec![ExtentRange {
            off: 0,
            length: size,
        }],
        size,
    ))
}

/// Splits extents into `n` groups of roughly equal populated bytes, cutting
/// an extent at a group boundary when that balances better. Groups keep file
/// order, so concatenating the parts reproduces the single-stream order.
pub fn plan_sparse_parts(exts: &[ExtentRange], populated: i64, n: usize) -> Vec<Vec<ExtentRange>> {
    let n = n.max(1);
    let per = (populated / n as i64).max(1);
    let mut groups: Vec<Vec<ExtentRange>> = Vec::with_capacity(n);
    let mut cur: Vec<ExtentRange> = Vec::new();
    let mut cur_bytes = 0i64;
    for e in exts {
        let mut e = *e;
        while e.length > 0 {
            let mut room = per - cur_bytes;
            // The last group takes whatever is left rather than starting one
            // more.
            if groups.len() == n - 1 {
                room = e.length;
            }
            let take = room.min(e.length);
            cur.push(ExtentRange {
                off: e.off,
                length: take,
            });
            cur_bytes += take;
            e.off += take;
            e.length -= take;
            if cur_bytes >= per && groups.len() < n - 1 {
                groups.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
        }
    }
    if !cur.is_empty() {
        groups.push(cur);
    }
    groups
}

/// How many parts to split a snapshot of `populated` bytes into.
pub fn sparse_part_count(populated: i64) -> usize {
    ((populated / SPARSE_PART_TARGET).max(1) as usize).min(SPARSE_MAX_PARTS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_sparse_parts_balances() {
        let exts = [
            ExtentRange { off: 0, length: 10 },
            ExtentRange {
                off: 100,
                length: 30,
            },
            ExtentRange {
                off: 500,
                length: 60,
            },
        ];
        const POPULATED: i64 = 100;
        for n in 1..=8 {
            let groups = plan_sparse_parts(&exts, POPULATED, n);
            let mut total = 0;
            let mut last = -1;
            for g in &groups {
                for r in g {
                    assert!(
                        r.off >= last,
                        "n={n}: ranges out of order at {} after {last}",
                        r.off
                    );
                    last = r.off;
                    total += r.length;
                }
            }
            assert_eq!(total, POPULATED, "n={n}");
            assert!(groups.len() <= n, "n={n}: got {} groups", groups.len());
        }
    }

    #[test]
    fn part_count_is_bounded() {
        assert_eq!(sparse_part_count(0), 1);
        assert_eq!(sparse_part_count(192 << 20), 3);
        assert_eq!(sparse_part_count(10 << 30), SPARSE_MAX_PARTS);
    }
}
