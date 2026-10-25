//! UseCPU: a replaceable pool of threads that each burn a target fraction
//! of one core.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;
use tonic::Status;

use super::service::Service;
use crate::pb::glutton::v1 as pb;

/// Cycle length used when a request leaves `cycle_length_ms` at 0.
pub const DEFAULT_CYCLE: Duration = Duration::from_millis(100);

/// Absorbs the work loop's arithmetic so the optimizer cannot drop it.
static BURN_SINK: AtomicU64 = AtomicU64::new(0);

struct Pool {
    /// Dropping a worker's sender wakes and stops it.
    stops: Vec<mpsc::Sender<()>>,
    threads: Vec<JoinHandle<()>>,
}

/// The running CPU load pool. [`CpuLoad::set`] replaces it; [`CpuLoad::stop`]
/// tears it down.
pub struct CpuLoad {
    pool: Mutex<Option<Pool>>,
    /// The cap applied when a request asks to be capped: the machine's
    /// available parallelism.
    max_parallelism: Mutex<usize>,
}

impl Default for CpuLoad {
    fn default() -> Self {
        CpuLoad::new()
    }
}

impl CpuLoad {
    /// An empty pool capped at the machine's available parallelism.
    pub fn new() -> CpuLoad {
        CpuLoad {
            pool: Mutex::new(None),
            max_parallelism: Mutex::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1),
            ),
        }
    }

    /// Overrides the cap, for tests that pin it.
    #[cfg(test)]
    pub(crate) fn set_max_parallelism(&self, n: usize) {
        *self.max_parallelism.lock() = n;
    }

    /// Starts `num_cores` threads, each targeting `duty_cycle` of one core
    /// per cycle, after stopping any running pool. With `capped`, the count
    /// is clamped to the available parallelism: oversubscribing pins every
    /// core and starves individual threads below their target. Returns the
    /// number of threads started.
    pub fn set(&self, num_cores: usize, duty_cycle: f64, cycle: Duration, capped: bool) -> usize {
        let mut n = num_cores;
        if capped {
            n = n.min(*self.max_parallelism.lock());
        }
        let cycle = if cycle.is_zero() {
            DEFAULT_CYCLE
        } else {
            cycle
        };
        let duty = duty_cycle.clamp(0.0, 1.0);

        let mut pool = self.pool.lock();
        stop_pool(pool.take());
        if n == 0 {
            return 0;
        }
        let mut stops = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for i in 0..n {
            let (tx, rx) = mpsc::channel::<()>();
            stops.push(tx);
            let handle = std::thread::Builder::new()
                .name(format!("glutton-cpu-{i}"))
                .spawn(move || worker(rx, duty, cycle))
                .expect("spawn CPU load thread");
            threads.push(handle);
        }
        *pool = Some(Pool { stops, threads });
        n
    }

    /// Stops any running pool and waits for its threads.
    pub fn stop(&self) {
        stop_pool(self.pool.lock().take());
    }

    /// The current pool size.
    pub fn n(&self) -> usize {
        self.pool
            .lock()
            .as_ref()
            .map(|p| p.threads.len())
            .unwrap_or(0)
    }
}

impl Drop for CpuLoad {
    fn drop(&mut self) {
        self.stop();
    }
}

fn stop_pool(pool: Option<Pool>) {
    if let Some(Pool { stops, threads }) = pool {
        drop(stops);
        for t in threads {
            let _ = t.join();
        }
    }
}

/// Alternates between spinning on arithmetic for the cycle's work budget and
/// sleeping for the rest. The budget is measured in this thread's CPU time,
/// not wall time, so throttling or preemption cannot make it look spent
/// while the thread was off-CPU.
fn worker(stop: mpsc::Receiver<()>, duty: f64, cycle: Duration) {
    let budget = cycle.mul_f64(duty);
    let rest = cycle - budget;
    let mut x: u64 = 1;
    loop {
        if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty)) {
            break;
        }
        if !budget.is_zero() {
            let start = thread_cpu_time();
            loop {
                // A batch between clock reads keeps the clock call from
                // dominating the load itself.
                for _ in 0..4096 {
                    x = x.wrapping_mul(1103515245).wrapping_add(12345);
                }
                if thread_cpu_time().saturating_sub(start) >= budget {
                    break;
                }
            }
        }
        if !rest.is_zero()
            && !matches!(
                stop.recv_timeout(rest),
                Err(mpsc::RecvTimeoutError::Timeout)
            )
        {
            break;
        }
    }
    BURN_SINK.store(x, Ordering::Relaxed);
}

/// CPU time consumed by the calling thread.
#[cfg(unix)]
pub fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// CPU time consumed by the calling thread; wall time where no per-thread
/// clock exists.
#[cfg(not(unix))]
pub fn thread_cpu_time() -> Duration {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH.get_or_init(std::time::Instant::now).elapsed()
}

impl Service {
    /// Replaces the running CPU load; `num_cores` 0 stops it.
    pub fn use_cpu(&self, req: &pb::UseCpuRequest) -> Result<pb::UseCpuResponse, Status> {
        if req.num_cores < 0 {
            return Err(Status::invalid_argument("num_cores must be non-negative"));
        }
        if !(0.0..=1.0).contains(&req.duty_cycle) {
            return Err(Status::invalid_argument(format!(
                "duty_cycle {} must be in [0, 1]",
                req.duty_cycle
            )));
        }
        if req.cycle_length_ms < 0 {
            return Err(Status::invalid_argument(
                "cycle_length_ms must be non-negative",
            ));
        }
        let cycle = Duration::from_millis(req.cycle_length_ms as u64);
        let capped = req.cap_at_max_parallelism.unwrap_or(true);
        let n = self
            .cpu
            .set(req.num_cores as usize, req.duty_cycle, cycle, capped);
        self.metrics.cpu_workers.set(n as i64);
        Ok(pb::UseCpuResponse {
            num_cores: n as i32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glutton::service::tests::new_service;

    fn req(n: i32, duty: f64, cycle: i32, cap: Option<bool>) -> pb::UseCpuRequest {
        pb::UseCpuRequest {
            num_cores: n,
            duty_cycle: duty,
            cycle_length_ms: cycle,
            cap_at_max_parallelism: cap,
        }
    }

    #[test]
    fn use_cpu_lifecycle() {
        let (svc, _d) = new_service();
        svc.cpu.set_max_parallelism(8);
        assert_eq!(svc.use_cpu(&req(2, 0.1, 20, None)).unwrap().num_cores, 2);
        assert_eq!(svc.cpu.n(), 2);
        assert_eq!(svc.use_cpu(&req(1, 0.1, 20, None)).unwrap().num_cores, 1);
        assert_eq!(svc.cpu.n(), 1);
        assert_eq!(
            svc.use_cpu(&pb::UseCpuRequest::default())
                .unwrap()
                .num_cores,
            0
        );
        assert_eq!(svc.cpu.n(), 0);
    }

    #[test]
    fn cpu_load_stop() {
        let c = CpuLoad::new();
        assert_eq!(c.set(2, 0.1, Duration::from_millis(20), false), 2);
        c.stop();
        assert_eq!(c.n(), 0);
        c.stop();
        assert_eq!(c.n(), 0);
        assert_eq!(c.set(1, 0.1, Duration::from_millis(20), false), 1);
        c.stop();
    }

    #[test]
    fn use_cpu_cap_at_max_parallelism() {
        let (svc, _d) = new_service();
        svc.cpu.set_max_parallelism(2);
        for (name, cap, want) in [
            ("unset defaults to capped", None, 2),
            ("explicit true is capped", Some(true), 2),
            ("explicit false is uncapped", Some(false), 8),
        ] {
            let resp = svc.use_cpu(&req(8, 0.01, 50, cap)).unwrap();
            assert_eq!(resp.num_cores, want, "{name}");
            svc.use_cpu(&pb::UseCpuRequest::default()).unwrap();
        }
    }

    #[test]
    fn use_cpu_rejects_invalid_args() {
        let (svc, _d) = new_service();
        for (name, r) in [
            ("negative num_cores", req(-1, 0.0, 0, None)),
            ("duty_cycle below range", req(1, -0.1, 0, None)),
            ("duty_cycle above range", req(1, 1.5, 0, None)),
            ("negative cycle_length_ms", req(1, 0.0, -5, None)),
        ] {
            assert_eq!(
                svc.use_cpu(&r).unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "{name}"
            );
        }
    }

    #[cfg(unix)]
    fn process_user_cpu_time() -> Duration {
        // SAFETY: `ru` is a valid, writable rusage.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        assert_eq!(rc, 0);
        Duration::new(ru.ru_utime.tv_sec as u64, ru.ru_utime.tv_usec as u32 * 1000)
    }

    /// At 100% duty the process's user CPU time must advance by a good
    /// share of the wall time: the worker burns CPU, not just time.
    #[cfg(unix)]
    #[test]
    fn use_cpu_burns_cpu_time() {
        let (svc, _d) = new_service();
        let start = process_user_cpu_time();
        svc.use_cpu(&req(1, 1.0, 10, None)).unwrap();
        let window = Duration::from_millis(300);
        std::thread::sleep(window);
        svc.use_cpu(&pb::UseCpuRequest::default()).unwrap();
        let delta = process_user_cpu_time() - start;
        assert!(
            delta >= window / 2,
            "user CPU delta {delta:?} over {window:?}"
        );
    }
}
