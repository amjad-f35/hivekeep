//! The process's CPU and memory use, as heartbeats report them.

use std::time::{Duration, Instant};

/// CPU time used so far by this process.
fn cpu_time() -> Duration {
    #[cfg(unix)]
    {
        // SAFETY: getrusage fills the zeroed struct it is given.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } == 0 {
            let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32) * 1_000);
            return tv(ru.ru_utime) + tv(ru.ru_stime);
        }
    }
    Duration::ZERO
}

/// Peak resident memory of this process, in bytes.
fn max_rss() -> u64 {
    #[cfg(unix)]
    {
        // SAFETY: as above.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } == 0 {
            let rss = ru.ru_maxrss.max(0) as u64;
            // Linux reports KiB, macOS bytes.
            return if cfg!(target_os = "macos") {
                rss
            } else {
                rss * 1024
            };
        }
    }
    0
}

/// CPU use between successive samples.
pub struct Usage {
    last_wall: Instant,
    last_cpu: Duration,
}

impl Usage {
    /// Starts measuring now.
    pub fn start() -> Self {
        Usage {
            last_wall: Instant::now(),
            last_cpu: cpu_time(),
        }
    }

    /// The CPU percentage (of one core) since the last sample, and the
    /// process's peak resident memory in bytes.
    pub fn sample(&mut self) -> (f64, u64) {
        let now = Instant::now();
        let cpu = cpu_time();
        let wall = now.duration_since(self.last_wall).as_secs_f64();
        let used = cpu.saturating_sub(self.last_cpu).as_secs_f64();
        self.last_wall = now;
        self.last_cpu = cpu;
        let pct = if wall > 0.0 { used / wall * 100.0 } else { 0.0 };
        (pct, max_rss())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_are_sane() {
        let mut u = Usage::start();
        let mut x = 0u64;
        let until = Instant::now() + Duration::from_millis(30);
        while Instant::now() < until {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(x);
        let (cpu, mem) = u.sample();
        assert!(cpu >= 0.0);
        #[cfg(unix)]
        assert!(mem > 0);
        let _ = mem;
    }
}
