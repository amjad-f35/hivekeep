//! The PID 1 supervisor, in a test binary of its own: it reaps every child of
//! the process and installs process-wide signal handlers.

use std::time::Duration;

use hivekeep_sandbox_microvm::pid1::supervise;

fn argv(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

#[test]
fn supervisor_behaviour() {
    // One test, run in order: the cases share the process's children and
    // signal dispositions.
    assert_eq!(
        supervise("/bin/sh", &argv(&["sh", "-c", "exit 3"])),
        3,
        "exit code"
    );
    assert_eq!(
        supervise("/bin/sh", &argv(&["sh", "-c", "kill -KILL $$"])),
        128 + 9,
        "signal exit code"
    );

    assert_eq!(
        supervise(
            "/bin/sh",
            &argv(&["sh", "-c", "(sh -c 'exit 0' &); sleep 0.5"])
        ),
        0,
        "orphans"
    );
    let mut status = 0;
    // SAFETY: plain waitpid.
    let got = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
    assert_eq!(got, -1, "a child was left unreaped");

    // SIGTERM is forwarded. Ignore it ourselves around the supervisor, so a
    // signal landing after it returns does not end the test.
    // SAFETY: ignoring SIGTERM for this test process.
    unsafe { libc::signal(libc::SIGTERM, libc::SIG_IGN) };
    let t = std::thread::spawn(|| supervise("/bin/sh", &argv(&["sh", "-c", "exec sleep 30"])));
    let start = std::time::Instant::now();
    while !t.is_finished() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "SIGTERM was not forwarded"
        );
        // SAFETY: signalling ourselves.
        unsafe { libc::kill(std::process::id() as i32, libc::SIGTERM) };
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(t.join().unwrap(), 128 + libc::SIGTERM, "forwarded signal");
}
