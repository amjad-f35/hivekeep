//! The `kill` subcommand: SIGKILL every process whose command line contains a
//! string, the way a crash would. Run from an ephemeral container sharing a
//! worker's PID namespace, it takes down an Actor's sandbox processes, each of
//! which names the Actor's UID on its command line.

use std::io;
use std::path::Path;

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

/// Kills the processes under `proc_dir` whose command line contains
/// `needle`, other than `self_pid`, and reports how many it killed. A process
/// already gone by the time it is signalled still counts: a gofer or
/// virtiofsd can exit on its own once the process it serves dies.
pub fn kill_matching(proc_dir: &Path, needle: &str, self_pid: i32) -> io::Result<usize> {
    let mut killed = 0;
    for entry in std::fs::read_dir(proc_dir)? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<i32>().ok())
        else {
            continue;
        };
        if pid == self_pid {
            continue;
        }
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        if !contains(&cmdline, needle.as_bytes()) {
            continue;
        }
        // SAFETY: kill(2) has no memory-safety preconditions.
        if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                return Err(io::Error::new(err.kind(), format!("killing {pid}: {err}")));
            }
        }
        killed += 1;
    }
    Ok(killed)
}

/// Runs the `kill` subcommand.
pub fn run(cmdline_contains: &str) -> anyhow::Result<()> {
    let n = kill_matching(
        Path::new("/proc"),
        cmdline_contains,
        std::process::id() as i32,
    )?;
    println!("killed {n}");
    if n == 0 {
        anyhow::bail!("no process command line contains {cmdline_contains:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn kill_matching_kills_the_process() {
        use std::time::{Duration, Instant};
        let mut child = std::process::Command::new("sleep")
            .arg("31337")
            .spawn()
            .unwrap();
        // The child's argv can read empty until exec has completed.
        let cmdline = format!("/proc/{}/cmdline", child.id());
        let start = Instant::now();
        loop {
            if std::fs::read(&cmdline)
                .map(|b| contains(&b, b"sleep\x0031337"))
                .unwrap_or(false)
            {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "{cmdline} still has no command line"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let n = kill_matching(
            Path::new("/proc"),
            "sleep\x0031337",
            std::process::id() as i32,
        )
        .unwrap();
        assert_eq!(n, 1);
        let status = child.wait().unwrap();
        assert!(!status.success(), "matching process exited cleanly");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn kill_matching_skips_self() {
        // The test binary's own command line contains its name.
        let me = std::env::args().next().unwrap();
        let n = kill_matching(Path::new("/proc"), &me, std::process::id() as i32).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn kill_matching_counts_a_process_already_gone() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        // A process listed but reaped by the time it is killed.
        let proc_dir = tempfile::tempdir().unwrap();
        let dir = proc_dir.path().join(pid.to_string());
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("cmdline"), b"virtiofsd\x00actor-uid").unwrap();
        let n = kill_matching(proc_dir.path(), "actor-uid", std::process::id() as i32).unwrap();
        assert_eq!(n, 1);
    }
}
