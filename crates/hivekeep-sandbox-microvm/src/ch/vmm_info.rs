//! What `vmm.ping` reports about the VMM, and the version-dependent
//! behaviours the worker keys off it.

use serde::Deserialize;

/// Restore guest RAM by faulting pages in as the guest touches them, so an
/// idle restored guest holds its working set rather than its snapshot.
pub const MEM_RESTORE_ON_DEMAND: &str = "OnDemand";
/// Restore guest RAM by reading every populated extent up front.
pub const MEM_RESTORE_EAGER: &str = "Copy";

/// The first release that advances the guest clock across a restore itself.
pub const CLOCK_ADVANCE_SINCE: [u32; 3] = [53, 0, 0];
/// The first release whose on-demand restore prefaults every page in the
/// background and refuses a snapshot until that finishes.
pub const PREFAULTING_SINCE: [u32; 3] = [53, 0, 0];
/// The release that stops prefaulting unconditionally, exclusive; zero while
/// no fix is known. Setting it brings on-demand restores back on its own.
pub const PREFAULTING_UNTIL: [u32; 3] = [0, 0, 0];

/// The VMM's self-description. `version` is a semver (`53.0.0`),
/// `build_version` the release tag it was built from (`v53.0`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct VmmInfo {
    /// Semver.
    #[serde(default)]
    pub version: String,
    /// Release tag.
    #[serde(default)]
    pub build_version: String,
    /// Compiled-in features.
    #[serde(default)]
    pub features: Vec<String>,
}

/// Parses a dotted version, tolerating a leading `v`, a missing patch and
/// trailing build metadata (`53.0.0-dirty`).
pub fn parse_version(s: &str) -> Option<[u32; 3]> {
    let s = s.trim();
    let s = s.strip_prefix('v').unwrap_or(s);
    if s.is_empty() {
        return None;
    }
    let s = s.split(['-', '+']).next().unwrap_or(s);
    let mut out = [0u32; 3];
    for (i, part) in s.splitn(3, '.').enumerate() {
        out[i] = part.parse().ok()?;
    }
    Some(out)
}

impl VmmInfo {
    /// The version, preferring the semver field and falling back to the tag.
    pub fn semver(&self) -> Option<[u32; 3]> {
        parse_version(&self.version).or_else(|| parse_version(&self.build_version))
    }

    /// Whether the VMM repairs the guest clock across a restore by itself.
    /// An unreadable version says no: wrongly believing it would leave every
    /// resumed guest on a stale clock with no error to show.
    pub fn advances_guest_clock_on_restore(&self) -> bool {
        self.semver().is_some_and(|v| v >= CLOCK_ADVANCE_SINCE)
    }

    /// Whether an on-demand restore prefaults everything. An unreadable
    /// version says yes: choosing on-demand on an affected VMM leaves a guest
    /// that cannot pass its wakeup probe, eager on an unaffected one merely
    /// costs memory.
    pub fn prefaults_unconditionally(&self) -> bool {
        self.prefaults_within(PREFAULTING_SINCE, PREFAULTING_UNTIL)
    }

    fn prefaults_within(&self, since: [u32; 3], until: [u32; 3]) -> bool {
        let Some(v) = self.semver() else { return true };
        if v < since {
            return false;
        }
        until == [0, 0, 0] || v < until
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(version: &str, build: &str) -> VmmInfo {
        VmmInfo {
            version: version.into(),
            build_version: build.into(),
            features: vec![],
        }
    }

    #[test]
    fn advances_the_guest_clock() {
        for (name, i, want) in [
            ("v52 does not", info("52.0.0", "v52.0"), false),
            ("v53 does", info("53.0.0", "v53.0"), true),
            ("later releases keep the fix", info("60.1.2", ""), true),
            ("much older", info("41.0.0", ""), false),
            ("tag only", info("", "v52.0"), false),
            ("tag only, fixed", info("", "v53.0"), true),
            ("suffixed", info("53.0.0-dirty", ""), true),
            ("empty", info("", ""), false),
            ("garbage", info("not-a-version", ""), false),
            ("partial garbage", info("53.x.0", ""), false),
        ] {
            assert_eq!(i.advances_guest_clock_on_restore(), want, "{name}");
        }
    }

    #[test]
    fn prefaults_unconditionally() {
        for (name, i, want) in [
            ("v52 unaffected", info("52.0.0", "v52.0"), false),
            ("v53 affected", info("53.0.0", "v53.0"), true),
            (
                "a later release stays affected until a fix is known",
                info("60.1.2", ""),
                true,
            ),
            ("much older", info("41.0.0", ""), false),
            ("tag only", info("", "v52.0"), false),
            ("tag only, affected", info("", "v53.0"), true),
            ("suffixed", info("53.0.0-dirty", ""), true),
            ("empty", info("", ""), true),
            ("garbage", info("not-a-version", ""), true),
            ("partial garbage", info("53.x.0", ""), true),
        ] {
            assert_eq!(i.prefaults_unconditionally(), want, "{name}");
        }
    }

    #[test]
    fn an_upper_bound_retires_the_workaround() {
        assert!(info("54.0.0", "").prefaults_within(PREFAULTING_SINCE, [55, 0, 0]));
        assert!(!info("55.0.0", "").prefaults_within(PREFAULTING_SINCE, [55, 0, 0]));
    }

    #[test]
    fn parses_versions() {
        for (s, want) in [
            ("53.0.0", Some([53, 0, 0])),
            ("v53.0", Some([53, 0, 0])),
            ("53", Some([53, 0, 0])),
            (" 52.1.3 ", Some([52, 1, 3])),
            ("53.0.0+build7", Some([53, 0, 0])),
            ("", None),
            ("v", None),
            ("abc", None),
            ("53.abc", None),
        ] {
            assert_eq!(parse_version(s), want, "{s:?}");
        }
    }
}
