//! Parsing `/proc/self/mountinfo` for the mounts below a directory. The
//! parser takes any reader, so it is tested everywhere; only the caller
//! that opens the real file is Linux-only.

use std::io::{self, BufRead};
use std::path::Path;

/// mountinfo octal-escapes these characters in paths.
const ESCAPES: [(&str, &str); 4] = [
    (" ", r"\040"),
    ("\t", r"\011"),
    ("\n", r"\012"),
    ("\\", r"\134"),
];

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match ESCAPES.iter().find(|(raw, _)| raw.starts_with(c)) {
            Some((_, esc)) => out.push_str(esc),
            None => out.push(c),
        }
    }
    out
}

/// Decodes mountinfo's octal escapes for space, tab, newline and backslash.
pub fn unescape_mount_path(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    'outer: while !rest.is_empty() {
        for (raw, esc) in ESCAPES {
            if let Some(r) = rest.strip_prefix(esc) {
                out.push_str(raw);
                rest = r;
                continue 'outer;
            }
        }
        let c = rest.chars().next().unwrap_or_default();
        out.push(c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

/// The `n`th (0-based) space-separated field of `line`.
fn field(line: &[u8], n: usize) -> Option<&[u8]> {
    let f = line.split(|&b| b == b' ').nth(n)?;
    (!f.is_empty()).then_some(f)
}

/// The mount points at or below `dir` in `r`, which must be in mountinfo
/// format (`ID parentID major:minor root MOUNTPOINT options...`). Lines
/// are compared in escaped form, so only matches are decoded; lines have
/// no length cap, since an overlay's record lists every lowerdir.
pub fn mount_points_in(r: impl io::Read, dir: &Path) -> io::Result<Vec<String>> {
    let clean =
        String::from_utf8_lossy(&crate::confined::clean(dir.as_os_str().as_encoded_bytes()))
            .into_owned();
    let esc = escape(&clean);
    let esc = esc.as_bytes();
    let mut points = Vec::new();
    let mut reader = io::BufReader::new(r);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let l = line.strip_suffix(b"\n").unwrap_or(&line);
        let Some(mp) = field(l, 4) else { continue };
        if let Some(rest) = mp.strip_prefix(esc) {
            if rest.is_empty() || rest[0] == b'/' {
                points.push(unescape_mount_path(&String::from_utf8_lossy(mp)));
            }
        }
    }
    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_points_in_subtree() {
        let mi = [
            "22 26 0:20 / /sys rw,nosuid,nodev shared:7 - sysfs sysfs rw",
            "40 21 8:1 / /var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/task/rootfs rw shared:1 - overlay overlay rw,lowerdir=/x",
            "41 21 8:1 / /var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/pause/rootfs rw - overlay overlay rw",
            "42 21 8:1 / /var/lib/hivekeep-sandbox-gvisor/actors/a2/bundles/task/rootfs rw - overlay overlay rw",
            r"43 21 8:1 / /var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/odd\040name/rootfs rw - overlay overlay rw",
            "44 21 8:1 / /var/lib/hivekeep-sandbox-gvisor/actors/a1-sibling/bundles/x/rootfs rw - overlay overlay rw",
            "malformed line",
            "",
        ]
        .join("\n");
        let got = mount_points_in(
            mi.as_bytes(),
            Path::new("/var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles"),
        )
        .unwrap();
        assert_eq!(
            got,
            vec![
                "/var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/task/rootfs",
                "/var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/pause/rootfs",
                "/var/lib/hivekeep-sandbox-gvisor/actors/a1/bundles/odd name/rootfs",
            ]
        );
    }

    #[test]
    fn mount_points_in_exact_dir_match() {
        let got = mount_points_in(
            "40 21 8:1 / /mnt/target rw - ext4 /dev/sda1 rw".as_bytes(),
            Path::new("/mnt/target"),
        )
        .unwrap();
        assert_eq!(got, vec!["/mnt/target"]);
    }

    // A dir with a space matches its own mounts and not a longer sibling.
    #[test]
    fn mount_points_in_escaped_dir() {
        let mi = [
            r"40 21 8:1 / /mnt/odd\040name rw - ext4 /dev/sda1 rw",
            r"41 21 8:1 / /mnt/odd\040name/rootfs rw - overlay overlay rw",
            r"42 21 8:1 / /mnt/odd\040name2/rootfs rw - overlay overlay rw",
            "",
        ]
        .join("\n");
        let got = mount_points_in(mi.as_bytes(), Path::new("/mnt/odd name")).unwrap();
        assert_eq!(got, vec!["/mnt/odd name", "/mnt/odd name/rootfs"]);
    }

    // One line can exceed any fixed buffer; the lines after it still count.
    #[test]
    fn mount_points_in_long_line() {
        let long = format!(
            "40 21 8:1 / /mnt/target/rootfs rw - overlay overlay rw,lowerdir={}",
            "/l".repeat(40_000)
        );
        assert!(long.len() > 64 * 1024);
        let mi = [
            long.as_str(),
            "41 21 8:1 / /mnt/target/volumes/agent rw - overlay overlay rw",
            "",
        ]
        .join("\n");
        let got = mount_points_in(mi.as_bytes(), Path::new("/mnt/target")).unwrap();
        assert_eq!(got, vec!["/mnt/target/rootfs", "/mnt/target/volumes/agent"]);
    }

    #[test]
    fn unescape_mount_path_cases() {
        for (input, want) in [
            (r"/plain/path", "/plain/path"),
            (r"/with\040space", "/with space"),
            (r"/tab\011and\012newline", "/tab\tand\nnewline"),
            (r"/back\134slash", r"/back\slash"),
        ] {
            assert_eq!(unescape_mount_path(input), want);
        }
    }
}
