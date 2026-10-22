//! Capability sets as the kernel reports them for this process.
//!
//! Asking the kernel, rather than reading back the OCI spec the keeper
//! wrote, is the point: it proves the sandbox applied the requested set.

use std::path::Path;

use serde::Serialize;

/// Where the kernel reports this process's capability sets.
pub const PROC_STATUS: &str = "/proc/self/status";

/// Capability names by bit position, unprefixed as an ActorTemplate spells
/// them. The order is fixed by the kernel's capability numbering and must not
/// be sorted. A bit past the end is reported as `CAP_<n>`.
pub const CAPABILITY_NAMES: &[&str] = &[
    "CHOWN",
    "DAC_OVERRIDE",
    "DAC_READ_SEARCH",
    "FOWNER",
    "FSETID",
    "KILL",
    "SETGID",
    "SETUID",
    "SETPCAP",
    "LINUX_IMMUTABLE",
    "NET_BIND_SERVICE",
    "NET_BROADCAST",
    "NET_ADMIN",
    "NET_RAW",
    "IPC_LOCK",
    "IPC_OWNER",
    "SYS_MODULE",
    "SYS_RAWIO",
    "SYS_CHROOT",
    "SYS_PTRACE",
    "SYS_PACCT",
    "SYS_ADMIN",
    "SYS_BOOT",
    "SYS_NICE",
    "SYS_RESOURCE",
    "SYS_TIME",
    "SYS_TTY_CONFIG",
    "MKNOD",
    "LEASE",
    "AUDIT_WRITE",
    "AUDIT_CONTROL",
    "SETFCAP",
    "MAC_OVERRIDE",
    "MAC_ADMIN",
    "SYSLOG",
    "WAKE_ALARM",
    "BLOCK_SUSPEND",
    "AUDIT_READ",
    "PERFMON",
    "BPF",
    "CHECKPOINT_RESTORE",
];

/// Each capability set by name, in bit order, so a suite can compare against
/// an expected list without sorting.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitiesResponse {
    /// The bounding set.
    pub bounding: Vec<String>,
    /// The effective set.
    pub effective: Vec<String>,
    /// The permitted set.
    pub permitted: Vec<String>,
    /// The inheritable set.
    pub inheritable: Vec<String>,
    /// The ambient set.
    pub ambient: Vec<String>,
    /// Whether no_new_privs is set.
    pub no_new_privs: bool,
    /// A read or parse failure, so a failing assertion explains itself.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// Turns a hexadecimal `Cap*` mask into capability names. An empty mask is
/// an empty (not absent) list: "no capabilities" is an assertable result.
pub fn decode_cap_mask(hex: &str) -> Result<Vec<String>, String> {
    let hex = hex.trim();
    let mask = u64::from_str_radix(hex, 16)
        .map_err(|e| format!("parsing capability mask {hex:?}: {e}"))?;
    Ok((0..64)
        .filter(|bit| mask & (1u64 << bit) != 0)
        .map(|bit| match CAPABILITY_NAMES.get(bit) {
            Some(name) => (*name).to_string(),
            None => format!("CAP_{bit}"),
        })
        .collect())
}

/// Parses the `Cap*` and `NoNewPrivs` lines of a `/proc/<pid>/status` file.
pub fn parse_proc_status(status: &str) -> Result<CapabilitiesResponse, String> {
    let mut out = CapabilitiesResponse::default();
    for line in status.lines() {
        let Some((key, value)) = line.split_once('\t') else {
            continue;
        };
        let dst = match key.trim() {
            "NoNewPrivs:" => {
                out.no_new_privs = value.trim() == "1";
                continue;
            }
            "CapInh:" => &mut out.inheritable,
            "CapPrm:" => &mut out.permitted,
            "CapEff:" => &mut out.effective,
            "CapBnd:" => &mut out.bounding,
            "CapAmb:" => &mut out.ambient,
            _ => continue,
        };
        *dst = decode_cap_mask(value)?;
    }
    Ok(out)
}

/// Reads the capability sets from `path`.
pub fn read_proc_capabilities(path: &Path) -> Result<CapabilitiesResponse, String> {
    let status =
        std::fs::read_to_string(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    parse_proc_status(&status)
}

/// The `/capabilities` reply: the sets, or the error that kept them unread.
pub fn capabilities() -> CapabilitiesResponse {
    read_proc_capabilities(Path::new(PROC_STATUS)).unwrap_or_else(|error| CapabilitiesResponse {
        error,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // The capabilities suite needs a cluster, so its assertions are only as
    // good as this decoder and its table; these masks have known meanings.
    #[test]
    fn decode_cap_mask_cases() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("no capabilities", "0000000000000000", &[]),
            (
                // The container-runtime default set: its 14 well-known
                // members validate the table's bit positions.
                "container-runtime default set",
                "00000000a80425fb",
                &[
                    "CHOWN",
                    "DAC_OVERRIDE",
                    "FOWNER",
                    "FSETID",
                    "KILL",
                    "SETGID",
                    "SETUID",
                    "SETPCAP",
                    "NET_BIND_SERVICE",
                    "NET_RAW",
                    "SYS_CHROOT",
                    "MKNOD",
                    "AUDIT_WRITE",
                    "SETFCAP",
                ],
            ),
            // The keeper's default set: KILL (5), NET_BIND_SERVICE (10), AUDIT_WRITE (29).
            (
                "keeper default set",
                "0000000020000420",
                &["KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"],
            ),
            (
                "single capability",
                "0000000000000400",
                &["NET_BIND_SERVICE"],
            ),
            // A bit past the table is still reported, not silently dropped.
            ("unknown capability bit", "0000020000000000", &["CAP_41"]),
            (
                "leading and trailing whitespace is tolerated",
                "  0000000000000020\n",
                &["KILL"],
            ),
        ];
        for (name, mask, want) in cases {
            let got = decode_cap_mask(mask)
                .unwrap_or_else(|e| panic!("{name}: decode_cap_mask({mask:?}) failed: {e}"));
            assert_eq!(got, names(want), "{name}: decode_cap_mask({mask:?})");
        }
    }

    #[test]
    fn decode_cap_mask_invalid() {
        assert!(decode_cap_mask("nothex").is_err());
    }

    // The table is indexed by capability value; a wrong length means it has
    // drifted from the kernel's numbering.
    #[test]
    fn capability_names_table() {
        // CHOWN (0) .. CHECKPOINT_RESTORE (40)
        assert_eq!(CAPABILITY_NAMES.len(), 41);
        for (i, name) in CAPABILITY_NAMES.iter().enumerate() {
            assert!(!name.is_empty(), "CAPABILITY_NAMES[{i}] is empty");
        }
    }

    #[test]
    fn parse_proc_status_reads_every_set() {
        let status = "Name:\tprobe\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000400\nCapEff:\t0000000000000400\nCapBnd:\t0000000020000420\nCapAmb:\t0000000000000000\nNoNewPrivs:\t1\n";
        let got = parse_proc_status(status).unwrap();
        assert_eq!(got.inheritable, Vec::<String>::new());
        assert_eq!(got.permitted, names(&["NET_BIND_SERVICE"]));
        assert_eq!(got.effective, names(&["NET_BIND_SERVICE"]));
        assert_eq!(
            got.bounding,
            names(&["KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"])
        );
        assert!(got.no_new_privs);
        let json = serde_json::to_value(&got).unwrap();
        assert!(json.get("noNewPrivs").is_some() && json.get("error").is_none());
    }
}
