//! Sweeping what a sandbox left on the host: mounts and directories under its
//! shared tree and runtime directory, and orphaned VMM and virtiofsd
//! processes. The worker boots VMs itself, so a failed or cancelled start does
//! not clean up after itself, and the sandbox id (the Actor uid) would collide
//! on the next attempt.

use std::path::{Path, PathBuf};

use hivekeep_sandbox_gvisor::node_path;

use crate::kata::paths::{vm_dir, SHARED_SANDBOXES_DIR};

/// The mount points under any of `dirs` in `mountinfo`, each with the dir it
/// is under, deepest first.
pub fn mounts_under(mountinfo: &str, dirs: &[PathBuf]) -> Vec<(PathBuf, usize)> {
    let mut out = Vec::new();
    for line in mountinfo.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 5 {
            continue;
        }
        let mp = Path::new(f[4]);
        if let Some(i) = dirs.iter().position(|d| mp == d || mp.starts_with(d)) {
            out.push((mp.to_path_buf(), i));
        }
    }
    out.sort_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
    out
}

/// Removes leftover host-side state of sandbox `id`, lazily unmounting
/// everything under its directories first, and kills its orphaned processes.
///
/// A directory is removed only once every mount beneath it is known to be
/// detached: the shared tree holds binds whose sources are not the worker's
/// (the keeper's volume directories), and removing through a live bind would
/// delete the Actor's data.
pub fn cleanup_sandbox_state(id: &str) {
    let dirs = vec![Path::new(SHARED_SANDBOXES_DIR).join(id), vm_dir(id)];
    let mut removable = vec![false; dirs.len()];
    match std::fs::read_to_string("/proc/self/mountinfo") {
        Err(e) => {
            tracing::warn!(err = %e, "Cannot read mountinfo; leaving sandbox dirs in place");
        }
        Ok(info) => {
            removable = vec![true; dirs.len()];
            for (mp, i) in mounts_under(&info, &dirs) {
                if let Err(e) = detach(&mp) {
                    tracing::warn!(mount = %mp.display(), err = %e, "Failed to unmount leftover sandbox mount");
                    removable[i] = false;
                }
            }
        }
    }
    for (d, ok) in dirs.iter().zip(removable) {
        if !ok {
            tracing::warn!(dir = %d.display(), "Leaving sandbox dir in place: mounts under it were not all detached");
            continue;
        }
        match std::fs::remove_dir_all(d) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(dir = %d.display(), err = %e, "Failed to remove leftover sandbox dir")
            }
        }
    }
    kill_orphans(id);
}

#[cfg(target_os = "linux")]
fn detach(mp: &Path) -> std::io::Result<()> {
    nix::mount::umount2(mp, nix::mount::MntFlags::MNT_DETACH).map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
fn detach(_mp: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Kills processes left by a previous attempt for `id`: a cancelled start
/// leaves its VMM running (reparented) and holding guest RAM and sockets.
fn kill_orphans(id: &str) {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    let me = std::process::id();
    for e in entries.flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(cmdline) = std::fs::read(e.path().join("cmdline")) else {
            continue;
        };
        let exe = std::fs::read_link(e.path().join("exe")).unwrap_or_default();
        if is_sandbox_process(
            &node_path::actors_dir(),
            id,
            &String::from_utf8_lossy(&cmdline),
            &exe,
        ) {
            // SAFETY: plain kill of a pid we just matched.
            if unsafe { libc::kill(pid as i32, libc::SIGKILL) } != 0 {
                tracing::warn!(pid, exe = %exe.display(), err = %std::io::Error::last_os_error(), "Failed to kill orphaned sandbox process");
            }
        }
    }
}

/// Whether a process is a VMM or virtiofsd serving sandbox `id`. Both carry
/// the id in their socket paths. The keeper stages their binaries under each
/// Actor's own directory, so an executable under an Actor's directory
/// settles it: this sandbox's only if it is `id`'s. A binary from elsewhere
/// is judged by its name.
pub fn is_sandbox_process(actors_dir: &Path, id: &str, cmdline: &str, exe: &Path) -> bool {
    if !cmdline.contains(id) {
        return false;
    }
    if exe.starts_with(actors_dir) && exe != actors_dir {
        return exe.starts_with(node_path::actor_sandbox_assets_dir(actors_dir, id))
            && exe != node_path::actor_sandbox_assets_dir(actors_dir, id);
    }
    let argv0 = cmdline.split('\0').next().unwrap_or_default();
    argv0.contains("cloud-hypervisor") || argv0.contains("virtiofsd")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_sandbox_processes() {
        let actors = Path::new("/var/lib/hivekeep/actors");
        let id = "0e16c0fa-5c06-4b48-8742-4068635c08c1";
        let other = "7d3b1a9c-2e4f-4a6b-9c8d-0f1e2d3c4b5a";
        let staged = node_path::actor_sandbox_assets_dir(actors, id).join("cloud-hypervisor");
        let other_staged =
            node_path::actor_sandbox_assets_dir(actors, other).join("cloud-hypervisor");
        let argv = |a: &[&str]| format!("{}\0", a.join("\0"));
        let s = staged.to_string_lossy().to_string();
        let o = other_staged.to_string_lossy().to_string();
        let api = format!("/run/vc/vm/{id}/clh-api.sock");
        let other_api = format!("/run/vc/vm/{other}/clh-api.sock");
        for (name, cmdline, exe, want) in [
            (
                "staged VMM",
                argv(&[&s, "--api-socket", &api]),
                staged.clone(),
                true,
            ),
            (
                "staged virtiofsd",
                argv(&[&s, &format!("--socket-path=/run/vc/vm/{id}/virtiofsd.sock")]),
                staged.clone(),
                true,
            ),
            (
                "VMM by name",
                argv(&["cloud-hypervisor", "--api-socket", &api]),
                PathBuf::from("/usr/bin/cloud-hypervisor"),
                true,
            ),
            (
                "another sandbox",
                argv(&[&o, "--api-socket", &other_api]),
                other_staged.clone(),
                false,
            ),
            (
                "another sandbox's VMM naming the id",
                argv(&[
                    &o,
                    "--api-socket",
                    &other_api,
                    "--log",
                    &format!("/tmp/{id}"),
                ]),
                other_staged.clone(),
                false,
            ),
            (
                "unrelated process naming the id",
                argv(&["/bin/sh", "-c", &format!("echo {id}")]),
                PathBuf::from("/bin/busybox"),
                false,
            ),
        ] {
            assert_eq!(
                is_sandbox_process(actors, id, &cmdline, &exe),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn finds_mounts_deepest_first() {
        let info = "1 0 0:1 / /run/vc/vm/x rw - tmpfs t rw\n2 1 0:2 / /run/vc/vm/x/deep/er rw - tmpfs t rw\n3 0 0:3 / /run/vc/vm/xy rw - tmpfs t rw\n";
        let got = mounts_under(info, &[PathBuf::from("/run/vc/vm/x")]);
        let paths: Vec<_> = got
            .iter()
            .map(|(p, _)| p.to_string_lossy().to_string())
            .collect();
        assert_eq!(paths, ["/run/vc/vm/x/deep/er", "/run/vc/vm/x"]);
    }
}
