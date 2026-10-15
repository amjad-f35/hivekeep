//! Reading a shared library's `DT_SONAME`.
//!
//! Only as much ELF as that takes: the section headers, the dynamic section
//! and the string table it links to. Both classes and both byte orders are
//! read; anything malformed is an error rather than a guess.

use std::io::Read;

use crate::error::{Error, Result};

const SHT_DYNAMIC: u32 = 6;
const DT_NULL: u64 = 0;
const DT_SONAME: u64 = 14;
/// Larger files are not driver libraries worth reading whole.
const MAX_LEN: u64 = 1 << 30;

struct Reader<'a> {
    b: &'a [u8],
    le: bool,
    wide: bool,
}

impl Reader<'_> {
    fn bytes<const N: usize>(&self, off: u64) -> Result<[u8; N]> {
        let off = usize::try_from(off).map_err(|_| truncated())?;
        self.b
            .get(off..off.checked_add(N).ok_or_else(truncated)?)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(truncated)
    }

    fn u16(&self, off: u64) -> Result<u16> {
        let b = self.bytes::<2>(off)?;
        Ok(if self.le {
            u16::from_le_bytes(b)
        } else {
            u16::from_be_bytes(b)
        })
    }

    fn u32(&self, off: u64) -> Result<u32> {
        let b = self.bytes::<4>(off)?;
        Ok(if self.le {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        })
    }

    fn u64(&self, off: u64) -> Result<u64> {
        let b = self.bytes::<8>(off)?;
        Ok(if self.le {
            u64::from_le_bytes(b)
        } else {
            u64::from_be_bytes(b)
        })
    }

    /// A word: 8 bytes in ELF64, 4 in ELF32.
    fn word(&self, off: u64) -> Result<u64> {
        if self.wide {
            self.u64(off)
        } else {
            self.u32(off).map(u64::from)
        }
    }
}

fn truncated() -> Error {
    Error::msg("truncated or malformed ELF file")
}

struct Section {
    kind: u32,
    offset: u64,
    size: u64,
    link: u32,
}

fn section(r: &Reader<'_>, shoff: u64, shentsize: u64, i: u64) -> Result<Section> {
    let base = shoff
        .checked_add(i.checked_mul(shentsize).ok_or_else(truncated)?)
        .ok_or_else(truncated)?;
    if r.wide {
        Ok(Section {
            kind: r.u32(base + 4)?,
            offset: r.u64(base + 24)?,
            size: r.u64(base + 32)?,
            link: r.u32(base + 40)?,
        })
    } else {
        Ok(Section {
            kind: r.u32(base + 4)?,
            offset: r.u32(base + 16)?.into(),
            size: r.u32(base + 20)?.into(),
            link: r.u32(base + 24)?,
        })
    }
}

/// The `DT_SONAME` of an ELF image, `None` when it carries none.
pub fn soname_of(b: &[u8]) -> Result<Option<String>> {
    if b.len() < 16 || &b[..4] != b"\x7fELF" {
        return Err(Error::msg("not an ELF file"));
    }
    let wide = match b[4] {
        1 => false,
        2 => true,
        _ => return Err(Error::msg("unknown ELF class")),
    };
    let le = match b[5] {
        1 => true,
        2 => false,
        _ => return Err(Error::msg("unknown ELF byte order")),
    };
    let r = Reader { b, le, wide };
    let (shoff, shentsize, shnum) = if wide {
        (r.u64(0x28)?, r.u16(0x3a)?, r.u16(0x3c)?)
    } else {
        (r.u32(0x20)?.into(), r.u16(0x2e)?, r.u16(0x30)?)
    };
    let (shentsize, shnum) = (u64::from(shentsize), u64::from(shnum));
    let Some(dynamic) = (0..shnum)
        .map(|i| section(&r, shoff, shentsize, i))
        .find(|s| s.as_ref().map_or(true, |s| s.kind == SHT_DYNAMIC))
        .transpose()?
    else {
        return Ok(None);
    };
    let strtab = section(&r, shoff, shentsize, dynamic.link.into())?;
    let entsize = if wide { 16 } else { 8 };
    let word = if wide { 8 } else { 4 };
    let mut off = dynamic.offset;
    let end = dynamic
        .offset
        .checked_add(dynamic.size)
        .ok_or_else(truncated)?;
    while off + entsize <= end {
        let tag = r.word(off)?;
        let val = r.word(off + word)?;
        if tag == DT_NULL {
            break;
        }
        if tag == DT_SONAME {
            if val >= strtab.size {
                return Err(truncated());
            }
            let start = usize::try_from(strtab.offset + val).map_err(|_| truncated())?;
            let limit = usize::try_from(strtab.offset + strtab.size).map_err(|_| truncated())?;
            let s = b.get(start..limit.min(b.len())).ok_or_else(truncated)?;
            let n = s.iter().position(|c| *c == 0).ok_or_else(truncated)?;
            return String::from_utf8(s[..n].to_vec())
                .map(Some)
                .map_err(|_| Error::msg("DT_SONAME is not UTF-8"));
        }
        off += entsize;
    }
    Ok(None)
}

/// Reads the `DT_SONAME` of an already opened, regular file.
pub fn soname_of_file(mut f: std::fs::File) -> Result<Option<String>> {
    let mut b = Vec::new();
    (&mut f)
        .take(MAX_LEN)
        .read_to_end(&mut b)
        .map_err(|e| Error::io("reading ELF file", e))?;
    soname_of(&b)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A minimal little-endian ELF64 image with a dynamic section whose
    /// `DT_SONAME` (when given) names `soname`.
    pub(crate) fn elf64(soname: Option<&str>) -> Vec<u8> {
        let mut strtab = vec![0u8];
        let name_off = strtab.len() as u64;
        if let Some(s) = soname {
            strtab.extend_from_slice(s.as_bytes());
            strtab.push(0);
        }
        let mut dynamic = Vec::new();
        if soname.is_some() {
            dynamic.extend_from_slice(&DT_SONAME.to_le_bytes());
            dynamic.extend_from_slice(&name_off.to_le_bytes());
        }
        dynamic.extend_from_slice(&[0u8; 16]);
        let strtab_off = 64u64;
        let dynamic_off = strtab_off + strtab.len() as u64;
        let shoff = dynamic_off + dynamic.len() as u64;
        let mut b = vec![0u8; 64];
        b[..4].copy_from_slice(b"\x7fELF");
        b[4] = 2;
        b[5] = 1;
        b[6] = 1;
        b[0x28..0x30].copy_from_slice(&shoff.to_le_bytes());
        b[0x3a..0x3c].copy_from_slice(&64u16.to_le_bytes());
        b[0x3c..0x3e].copy_from_slice(&3u16.to_le_bytes());
        b.extend_from_slice(&strtab);
        b.extend_from_slice(&dynamic);
        let sh = |kind: u32, off: u64, size: u64, link: u32| {
            let mut s = vec![0u8; 64];
            s[4..8].copy_from_slice(&kind.to_le_bytes());
            s[24..32].copy_from_slice(&off.to_le_bytes());
            s[32..40].copy_from_slice(&size.to_le_bytes());
            s[40..44].copy_from_slice(&link.to_le_bytes());
            s
        };
        b.extend(sh(0, 0, 0, 0));
        b.extend(sh(3, strtab_off, strtab.len() as u64, 0));
        b.extend(sh(SHT_DYNAMIC, dynamic_off, dynamic.len() as u64, 1));
        b
    }

    #[test]
    fn reads_the_soname() {
        assert_eq!(
            soname_of(&elf64(Some("libcuda.so.1"))).unwrap().as_deref(),
            Some("libcuda.so.1")
        );
        assert_eq!(soname_of(&elf64(None)).unwrap(), None);
    }

    #[test]
    fn rejects_what_is_not_elf() {
        assert!(soname_of(b"stub-lib").is_err());
        let mut cut = elf64(Some("libx.so.1"));
        cut.truncate(80);
        assert!(soname_of(&cut).is_err());
    }
}
