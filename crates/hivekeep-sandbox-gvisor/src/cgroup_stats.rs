//! Point-in-time readings of a cgroup v2 directory.
//!
//! gVisor runs a whole sandbox in one host process tree, so the sandbox's
//! cgroup leaf is where its memory and CPU show up. Reads take the directory
//! as an argument, so they work against fixture trees too.

use std::io;
use std::path::Path;

/// One reading. Fields the kernel does not expose read as zero rather than
/// failing the sample: a partial reading beats none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sample {
    /// `memory.current`: bytes charged to the cgroup, page cache included.
    pub memory_current_bytes: u64,
    /// `memory.peak`: the high-water mark. Zero before kernel 5.19.
    pub memory_peak_bytes: u64,
    /// `memory.current` less reclaimable cache (`inactive_file`), floored at
    /// zero; the figure to compare against a limit.
    pub memory_working_set_bytes: u64,
    /// `cpu.stat` `usage_usec`. Zero without the cpu controller.
    pub cpu_usage_usec: u64,
    /// `memory.events` `oom_kill`.
    pub oom_kills: u64,
    /// `cgroup.events` reports `populated 0`: no process is left. False when
    /// the file is absent, so a kernel that does not say never reads as dead.
    pub empty: bool,
}

/// Reads the cgroup at `dir`.
///
/// Fails only when the cgroup itself is unreadable: a missing directory or a
/// missing (`NotFound`) or malformed `memory.current`. Every other file is
/// best effort, since each has a legitimate reason to be absent.
pub fn read(dir: &Path) -> io::Result<Sample> {
    let current_path = dir.join("memory.current");
    let current = read_uint(&current_path)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("reading {}: not found", current_path.display()),
        )
    })?;
    let peak = read_uint(&dir.join("memory.peak"))
        .ok()
        .flatten()
        .unwrap_or(0);
    let inactive_file = read_keyed_uint(&dir.join("memory.stat"), "inactive_file")
        .ok()
        .flatten();
    let cpu = read_keyed_uint(&dir.join("cpu.stat"), "usage_usec")
        .ok()
        .flatten()
        .unwrap_or(0);
    let oom_kills = read_keyed_uint(&dir.join("memory.events"), "oom_kill")
        .ok()
        .flatten()
        .unwrap_or(0);
    let populated = read_keyed_uint(&dir.join("cgroup.events"), "populated")
        .ok()
        .flatten();
    // Without inactive_file there is nothing to subtract; over-reporting is
    // the safe direction. The files are not one snapshot, so saturate.
    let working_set = inactive_file.map_or(current, |i| current.saturating_sub(i));
    Ok(Sample {
        memory_current_bytes: current,
        memory_peak_bytes: peak,
        memory_working_set_bytes: working_set,
        cpu_usage_usec: cpu,
        oom_kills,
        empty: populated == Some(0),
    })
}

/// A file holding one unsigned integer: `None` when absent, an error when
/// present but unparseable.
pub fn read_uint(path: &Path) -> io::Result<Option<u64>> {
    match std::fs::read_to_string(path) {
        Ok(s) => s.trim().parse().map(Some).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parsing {}: {e}", path.display()),
            )
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// One key of a flat-keyed file (`<key> <value>` lines): `None` when the
/// file or key is absent. Malformed lines are skipped.
pub fn read_keyed_uint(path: &Path, key: &str) -> io::Result<Option<u64>> {
    let s = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    for line in s.lines() {
        let mut f = line.split_whitespace();
        if let (Some(k), Some(v), None) = (f.next(), f.next(), f.next()) {
            if k == key {
                return v.parse().map(Some).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("parsing {key:?} of {}: {e}", path.display()),
                    )
                });
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Many keys, with inactive_file neither first nor last and neighbours a
    /// "first number wins" parser would pick up instead.
    const FULL_MEMORY_STAT: &str = "anon 104857600\nfile 52428800\nkernel 8388608\nkernel_stack 262144\nslab 4194304\nsock 0\nshmem 0\nfile_mapped 1048576\nfile_dirty 0\nfile_writeback 0\ninactive_anon 0\nactive_anon 104857600\ninactive_file 20971520\nactive_file 31457280\nunevictable 0\n";
    const FULL_CPU_STAT: &str = "usage_usec 1234567\nuser_usec 1000000\nsystem_usec 234567\nnr_periods 0\nnr_throttled 0\nthrottled_usec 0\n";

    fn cgroup(files: &[(&str, &str)]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for (name, content) in files {
            std::fs::write(d.path().join(name), content).unwrap();
        }
        d
    }

    fn sample(current: u64, peak: u64, ws: u64, cpu: u64) -> Sample {
        Sample {
            memory_current_bytes: current,
            memory_peak_bytes: peak,
            memory_working_set_bytes: ws,
            cpu_usage_usec: cpu,
            ..Default::default()
        }
    }

    #[test]
    fn reads() {
        type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, Sample);
        let cases: Vec<Case> = vec![
            (
                "all files present",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.peak", "209715200\n"),
                    ("memory.stat", FULL_MEMORY_STAT),
                    ("cpu.stat", FULL_CPU_STAT),
                ],
                sample(157286400, 209715200, 136314880, 1234567),
            ),
            (
                "no memory.peak",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.stat", FULL_MEMORY_STAT),
                    ("cpu.stat", FULL_CPU_STAT),
                ],
                sample(157286400, 0, 136314880, 1234567),
            ),
            (
                "no cpu.stat",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.peak", "209715200\n"),
                    ("memory.stat", FULL_MEMORY_STAT),
                ],
                sample(157286400, 209715200, 136314880, 0),
            ),
            (
                "no memory.stat",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.peak", "209715200\n"),
                    ("cpu.stat", FULL_CPU_STAT),
                ],
                sample(157286400, 209715200, 157286400, 1234567),
            ),
            (
                "memory.stat without inactive_file",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.stat", "anon 104857600\nfile 52428800\n"),
                ],
                sample(157286400, 0, 157286400, 0),
            ),
            (
                "inactive_file above memory.current floors at zero",
                vec![
                    ("memory.current", "1000\n"),
                    ("memory.stat", "inactive_file 4000\n"),
                ],
                sample(1000, 0, 0, 0),
            ),
            (
                "zero usage",
                vec![
                    ("memory.current", "0\n"),
                    ("memory.peak", "0\n"),
                    ("memory.stat", "inactive_file 0\n"),
                    ("cpu.stat", "usage_usec 0\n"),
                ],
                Sample::default(),
            ),
            (
                "live cgroup is not empty",
                vec![
                    ("memory.current", "1000\n"),
                    (
                        "memory.events",
                        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
                    ),
                    ("cgroup.events", "populated 1\nfrozen 0\n"),
                ],
                sample(1000, 0, 1000, 0),
            ),
            (
                "OOM-killed sandbox is empty",
                vec![
                    ("memory.current", "16384\n"),
                    (
                        "memory.events",
                        "low 0\nhigh 0\nmax 0\noom 5334\noom_kill 3\noom_group_kill 0\n",
                    ),
                    ("cgroup.events", "populated 0\nfrozen 0\n"),
                ],
                Sample {
                    oom_kills: 3,
                    empty: true,
                    ..sample(16384, 0, 16384, 0)
                },
            ),
            (
                "no cgroup.events is not taken for empty",
                vec![("memory.current", "1000\n")],
                sample(1000, 0, 1000, 0),
            ),
            (
                "malformed lines are skipped",
                vec![
                    ("memory.current", "157286400\n"),
                    (
                        "memory.stat",
                        "\nanon\n\ninactive_file 20971520\nbogus 1 2 3\n   \n",
                    ),
                    ("cpu.stat", "nr_periods\n\nusage_usec 1234567\n"),
                ],
                sample(157286400, 0, 136314880, 1234567),
            ),
            (
                "unparseable optional files degrade to zero",
                vec![
                    ("memory.current", "157286400\n"),
                    ("memory.peak", "not-a-number\n"),
                    ("cpu.stat", "usage_usec eleventy\n"),
                    ("memory.stat", "inactive_file lots\n"),
                    ("memory.events", "oom_kill lots\n"),
                    // Garbage here must not read as a dead sandbox.
                    ("cgroup.events", "populated no\n"),
                ],
                sample(157286400, 0, 157286400, 0),
            ),
        ];
        for (name, files, want) in cases {
            let d = cgroup(&files);
            assert_eq!(read(d.path()).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn missing_cgroup_is_not_found() {
        let d = tempfile::tempdir().unwrap();
        for dir in [d.path().join("nope"), d.path().to_path_buf()] {
            assert_eq!(read(&dir).unwrap_err().kind(), io::ErrorKind::NotFound);
        }
    }

    #[test]
    fn malformed_memory_current_is_an_error() {
        let d = cgroup(&[("memory.current", "max\n")]);
        let err = read(d.path()).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
    }
}
