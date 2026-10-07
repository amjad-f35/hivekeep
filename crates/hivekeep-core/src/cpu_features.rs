//! The `cpu_features` compatibility attribute: CPU feature names encoded as a
//! bitmask the control plane compares by subset.
//!
//! A snapshot records the features its sandbox could use; a worker can restore
//! it only when the worker's features include all of them. The encoding is
//! shared by everything that produces or compares the attribute, so it lives
//! here once:
//!
//! - [`encode_cpu_features`] turns feature names into the canonical value:
//!   lowercase hex, no prefix, no leading zeros, `"0"` for none. Equal sets
//!   encode to equal strings.
//! - [`decode_cpu_features`] turns a value back into names, refusing anything
//!   non-canonical or any bit the architecture's table does not name.
//! - [`FeatureMask`] is the parsed value, with the subset test matching uses.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::BufRead;
use std::sync::OnceLock;

mod table;

/// Width of a `cpu_features` bitmask in 64-bit words. The amd64 table uses
/// bits up to 261; six words leave room for CPUID blocks through 11 within
/// schema version v1.
pub const MASK_WORDS: usize = 6;

/// Errors from encoding, decoding or probing CPU features.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CpuFeaturesError {
    /// No feature table exists for the architecture.
    #[error("unsupported architecture {0:?} for cpu_features")]
    UnsupportedArch(String),
    /// A feature name the architecture's table does not know.
    #[error("unknown {arch} CPU feature {name:?}")]
    UnknownFeature {
        /// The architecture.
        arch: String,
        /// The unknown name.
        name: String,
    },
    /// A value that is not a canonical mask.
    #[error("invalid cpu_features value {0:?}")]
    InvalidValue(String),
    /// A value setting a bit no feature has.
    #[error("cpu_features value {value:?} sets bit {bit}, which is not a known {arch} feature")]
    UnknownBit {
        /// The value.
        value: String,
        /// The lowest offending bit.
        bit: usize,
        /// The architecture.
        arch: String,
    },
    /// `/proc/cpuinfo` could not be read or lacked a field.
    #[error("{0}")]
    CpuInfo(String),
}

/// A decoded `cpu_features` value: bit `i` set means the feature at bit `i`
/// of the architecture's table is present. Fixed-size, so matching allocates
/// nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FeatureMask(pub [u64; MASK_WORDS]);

impl FeatureMask {
    /// Sets `bit`. Panics when the bit is outside the mask.
    pub fn set(&mut self, bit: usize) {
        self.0[bit / 64] |= 1 << (bit % 64);
    }

    /// Whether `bit` is set.
    pub fn has(&self, bit: usize) -> bool {
        bit < MASK_WORDS * 64 && self.0[bit / 64] & (1 << (bit % 64)) != 0
    }

    /// Whether every bit set here is also set in `w`.
    pub fn subset_of(&self, w: &FeatureMask) -> bool {
        self.0.iter().zip(w.0.iter()).all(|(m, w)| m & !w == 0)
    }

    /// The lowest bit set here but not in `w`, if any.
    pub fn first_not_in(&self, w: &FeatureMask) -> Option<usize> {
        self.0
            .iter()
            .zip(w.0.iter())
            .enumerate()
            .find_map(|(i, (m, w))| {
                let extra = m & !w;
                (extra != 0).then(|| i * 64 + extra.trailing_zeros() as usize)
            })
    }

    /// Parses the canonical encoding, refusing anything else: empty values,
    /// leading zeros, uppercase, a prefix, or more digits than the mask holds.
    pub fn parse(s: &str) -> Option<FeatureMask> {
        let b = s.as_bytes();
        if b.is_empty() || b.len() > MASK_WORDS * 16 || (b.len() > 1 && b[0] == b'0') {
            return None;
        }
        let mut m = FeatureMask::default();
        for (i, c) in b.iter().rev().enumerate() {
            let v = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                _ => return None,
            } as u64;
            m.0[i / 16] |= v << (4 * (i % 16));
        }
        Some(m)
    }
}

/// The canonical encoding: lowercase hex, no prefix, no leading zeros, `"0"`
/// for the empty mask.
impl fmt::Display for FeatureMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut top = MASK_WORDS - 1;
        while top > 0 && self.0[top] == 0 {
            top -= 1;
        }
        write!(f, "{:x}", self.0[top])?;
        for i in (0..top).rev() {
            write!(f, "{:016x}", self.0[i])?;
        }
        Ok(())
    }
}

/// One architecture's feature names by bit, with the reverse index and the
/// set of bits that have a name.
#[derive(Debug, Clone)]
pub struct FeatureTable {
    names: BTreeMap<usize, &'static str>,
    index: HashMap<&'static str, usize>,
    defined: FeatureMask,
}

impl FeatureTable {
    /// Indexes `entries`. A bit outside the mask is an error in the table
    /// itself, so it panics rather than dropping the name.
    pub fn new(entries: &[(usize, &'static str)]) -> Self {
        let mut t = FeatureTable {
            names: BTreeMap::new(),
            index: HashMap::new(),
            defined: FeatureMask::default(),
        };
        for &(bit, name) in entries {
            assert!(
                bit < MASK_WORDS * 64,
                "CPU feature {name:?} has bit {bit} outside a {}-bit mask",
                MASK_WORDS * 64
            );
            t.defined.set(bit);
            t.names.insert(bit, name);
            t.index.insert(name, bit);
        }
        t
    }

    /// The feature at `bit`.
    pub fn name(&self, bit: usize) -> Option<&'static str> {
        self.names.get(&bit).copied()
    }

    /// The bit of feature `name`.
    pub fn bit(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Every named bit, ascending.
    pub fn bits(&self) -> impl Iterator<Item = usize> + '_ {
        self.names.keys().copied()
    }

    /// Every name, in bit order.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.names.values().copied()
    }

    /// The bits that have a name.
    pub fn defined(&self) -> FeatureMask {
        self.defined
    }

    /// How many features the table names.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether the table names no feature.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// The feature table for `arch` (`"amd64"` or `"arm64"`).
pub fn table(arch: &str) -> Option<&'static FeatureTable> {
    static AMD64: OnceLock<FeatureTable> = OnceLock::new();
    static ARM64: OnceLock<FeatureTable> = OnceLock::new();
    match arch {
        "amd64" => Some(AMD64.get_or_init(|| FeatureTable::new(table::AMD64))),
        "arm64" => Some(ARM64.get_or_init(|| FeatureTable::new(table::ARM64))),
        _ => None,
    }
}

/// The architectures with a feature table.
pub const ARCHITECTURES: [&str; 2] = ["amd64", "arm64"];

/// The running host's architecture in the attribute's spelling.
pub fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn table_for(arch: &str) -> Result<&'static FeatureTable, CpuFeaturesError> {
    table(arch).ok_or_else(|| CpuFeaturesError::UnsupportedArch(arch.to_string()))
}

/// Encodes the feature names `runsc cpu-features` printed for `arch`.
/// Blank names are skipped and order and duplicates do not matter. An unknown
/// name is an error rather than a dropped bit: a snapshot encoded without a
/// feature its sandbox used would match workers that lack it.
pub fn encode_cpu_features<S: AsRef<str>>(
    arch: &str,
    features: &[S],
) -> Result<String, CpuFeaturesError> {
    let t = table_for(arch)?;
    let mut m = FeatureMask::default();
    for raw in features {
        let name = raw.as_ref().trim();
        if name.is_empty() {
            continue;
        }
        let bit = t
            .bit(name)
            .ok_or_else(|| CpuFeaturesError::UnknownFeature {
                arch: arch.to_string(),
                name: name.to_string(),
            })?;
        m.set(bit);
    }
    Ok(m.to_string())
}

/// The feature names `value` encodes for `arch`, in bit order. A set bit with
/// no name is an error: no encoder of this schema could have produced it.
pub fn decode_cpu_features(arch: &str, value: &str) -> Result<Vec<String>, CpuFeaturesError> {
    let t = table_for(arch)?;
    let m = FeatureMask::parse(value)
        .ok_or_else(|| CpuFeaturesError::InvalidValue(value.to_string()))?;
    if let Some(bit) = m.first_not_in(&t.defined) {
        return Err(CpuFeaturesError::UnknownBit {
            value: value.to_string(),
            bit,
            arch: arch.to_string(),
        });
    }
    Ok(t.bits()
        .filter(|b| m.has(*b))
        .map(|b| t.names[&b].to_string())
        .collect())
}

/// Reads `/proc/cpuinfo` for the host CPU's vendor and model, the
/// `cpu_vendor` and `cpu_model` attributes of a micro-VM runtime.
pub fn probe_cpu_identity() -> Result<(String, String), CpuFeaturesError> {
    let f = std::fs::File::open("/proc/cpuinfo")
        .map_err(|e| CpuFeaturesError::CpuInfo(format!("while opening /proc/cpuinfo: {e}")))?;
    parse_cpu_info(host_arch(), std::io::BufReader::new(f))
}

/// Extracts the vendor and model for `arch` from `/proc/cpuinfo` content,
/// from the first processor that lists them:
///
/// - amd64: `vendor_id` (`GenuineIntel`) and `<cpu family>:<model>` (`6:143`).
/// - arm64: `CPU implementer` (`0x41`) and
///   `<CPU variant>:<CPU part>:<CPU revision>` (`0x0:0xd40:1`).
pub fn parse_cpu_info(arch: &str, r: impl BufRead) -> Result<(String, String), CpuFeaturesError> {
    let keys: &[&str] = match arch {
        "amd64" => &["vendor_id", "cpu family", "model"],
        "arm64" => &["CPU implementer", "CPU variant", "CPU part", "CPU revision"],
        _ => {
            return Err(CpuFeaturesError::CpuInfo(format!(
                "unsupported architecture {arch:?} for CPU identity"
            )))
        }
    };
    let mut fields: HashMap<&str, String> = HashMap::new();
    for line in r.lines() {
        if fields.len() == keys.len() {
            break;
        }
        let line = line
            .map_err(|e| CpuFeaturesError::CpuInfo(format!("while reading /proc/cpuinfo: {e}")))?;
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let k = k.trim();
        if let Some(key) = keys.iter().find(|key| **key == k) {
            fields.entry(key).or_insert_with(|| v.trim().to_string());
        }
    }
    let mut values = Vec::with_capacity(keys.len());
    for k in keys {
        match fields.get(k) {
            Some(v) if !v.is_empty() => values.push(v.clone()),
            _ => {
                return Err(CpuFeaturesError::CpuInfo(format!(
                    "missing {k:?} in /proc/cpuinfo"
                )))
            }
        }
    }
    let vendor = values.remove(0);
    Ok((vendor, values.join(":")))
}

#[cfg(test)]
mod tests;
