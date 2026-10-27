//! CPU time and resident memory of this process.

/// User plus system CPU seconds since the process started.
pub fn cpu_seconds() -> f64 {
    match rusage() {
        Some(ru) => tv(ru.ru_utime) + tv(ru.ru_stime),
        None => 0.0,
    }
}

fn tv(t: libc::timeval) -> f64 {
    t.tv_sec as f64 + t.tv_usec as f64 / 1e6
}

fn rusage() -> Option<libc::rusage> {
    // SAFETY: getrusage writes into the zeroed struct we pass.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to an initialized rusage.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    (rc == 0).then_some(ru)
}

/// Resident set size and whether it is only the lifetime peak: current
/// from `/proc` on Linux, the getrusage peak elsewhere.
pub fn rss() -> (u64, bool) {
    if let Some(kb) = proc_status_field("VmRSS:") {
        return (kb * 1024, false);
    }
    match rusage() {
        // Linux reports kilobytes, macOS bytes.
        Some(ru) if cfg!(target_os = "linux") => (ru.ru_maxrss as u64 * 1024, true),
        Some(ru) => (ru.ru_maxrss as u64, true),
        None => (0, true),
    }
}

/// OS threads of this process, from `/proc` on Linux; zero elsewhere.
pub fn threads() -> usize {
    proc_status_field("Threads:").unwrap_or(0) as usize
}

fn proc_status_field(name: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let v = line.strip_prefix(name)?;
        v.trim().trim_end_matches(" kB").trim().parse().ok()
    })
}
