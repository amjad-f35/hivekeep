//! A minimal init for when the worker runs as PID 1 of its container: it
//! starts the worker again as its child, forwards signals to it, reaps every
//! orphan, and exits with the child's status.

use std::io;
use std::os::unix::process::CommandExt;
use std::sync::atomic::{AtomicI32, Ordering};

/// The signals passed through to the child.
pub const FORWARDED: [libc::c_int; 6] = [
    libc::SIGTERM,
    libc::SIGINT,
    libc::SIGHUP,
    libc::SIGQUIT,
    libc::SIGUSR1,
    libc::SIGUSR2,
];

/// Write end of the self-pipe the handlers report signals on.
static PIPE_WR: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_signal(sig: libc::c_int) {
    let fd = PIPE_WR.load(Ordering::Relaxed);
    if fd >= 0 {
        let b = sig as u8;
        // SAFETY: write(2) is async-signal-safe; a full pipe just drops the
        // byte, and the reap after every wakeup covers a lost SIGCHLD.
        unsafe { libc::write(fd, (&b as *const u8).cast(), 1) };
    }
}

/// Runs the init and exits when this process is PID 1; otherwise returns.
/// Call it first thing in `main`.
pub fn run_if_pid1() {
    if std::process::id() != 1 {
        return;
    }
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("pid1: finding the executable: {e}");
            std::process::exit(1);
        }
    };
    let argv: Vec<String> = std::env::args().collect();
    std::process::exit(supervise(&exe.to_string_lossy(), &argv));
}

/// Starts `path` with `argv` as a child, reaps until it exits, and returns its
/// shell-style exit code.
pub fn supervise(path: &str, argv: &[String]) -> i32 {
    match supervise_inner(path, argv) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("pid1: {e}");
            1
        }
    }
}

fn supervise_inner(path: &str, argv: &[String]) -> io::Result<i32> {
    #[cfg(target_os = "linux")]
    {
        // A no-op as PID 1, but it lets tests run this as an ordinary process.
        // SAFETY: plain prctl.
        if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
            return Err(io::Error::other(format!(
                "setting child subreaper: {}",
                io::Error::last_os_error()
            )));
        }
    }
    let mut fds = [0; 2];
    // SAFETY: fds has room for the two descriptors.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    for fd in fds {
        // SAFETY: fcntl on descriptors we own.
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
        }
    }
    PIPE_WR.store(fds[1], Ordering::SeqCst);
    // Subscribe before starting the child so no exit goes unnoticed.
    let mut previous = Vec::new();
    for sig in FORWARDED.iter().copied().chain([libc::SIGCHLD]) {
        // SAFETY: installing a handler that only writes to the self-pipe.
        let old = unsafe { libc::signal(sig, on_signal as *const () as libc::sighandler_t) };
        previous.push((sig, old));
    }
    let restore = || {
        for (sig, old) in &previous {
            // SAFETY: putting back what was installed before.
            unsafe { libc::signal(*sig, *old) };
        }
        PIPE_WR.store(-1, Ordering::SeqCst);
        // SAFETY: closing the descriptors this call created.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    };
    let mut cmd = std::process::Command::new(path);
    if let Some((first, rest)) = argv.split_first() {
        cmd.arg0(first).args(rest);
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            restore();
            return Err(io::Error::other(format!("starting {path}: {e}")));
        }
    };
    let pid = child.id() as libc::pid_t;
    // Reaped below with wait4, never through the std handle.
    std::mem::forget(child);
    let code = loop {
        if let Some(status) = reap(pid) {
            break exit_code(status);
        }
        let mut p = libc::pollfd {
            fd: fds[0],
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd; a timeout bounds a lost wakeup.
        unsafe { libc::poll(&mut p, 1, 1000) };
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: reading into a local buffer.
            let n = unsafe { libc::read(fds[0], buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break;
            }
            for &sig in &buf[..n as usize] {
                let sig = sig as libc::c_int;
                if sig != libc::SIGCHLD {
                    // SAFETY: signalling our own child, which is not yet reaped.
                    unsafe { libc::kill(pid, sig) };
                }
            }
        }
    };
    restore();
    Ok(code)
}

/// Collects every exited child and returns `pid`'s status if it was among
/// them. One SIGCHLD may stand for several exits.
fn reap(pid: libc::pid_t) -> Option<libc::c_int> {
    let mut found = None;
    loop {
        let mut status = 0;
        // SAFETY: status is a valid out pointer.
        let got = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if got < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if got <= 0 {
            return found;
        }
        if got == pid {
            found = Some(status);
        }
    }
}

/// Maps a wait status to a shell-style exit code.
pub fn exit_code(status: libc::c_int) -> i32 {
    if libc::WIFSIGNALED(status) {
        128 + libc::WTERMSIG(status)
    } else {
        libc::WEXITSTATUS(status)
    }
}
