//! The usage state of one activation of an Actor (one Run or Restore): its
//! epoch, the CPU it used, its latest sample, and which of its initial,
//! periodic and final records were written.
//!
//! Stats reads, the sampler and the lifecycle records all touch it without
//! the Actor's lifecycle lock, so it serializes itself.

use std::collections::HashMap;
use std::future::Future;
use std::time::SystemTime;

use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::error::{Error, Result};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

/// The message of a reading that returned neither a sample nor an error.
pub const NO_SAMPLE: &str = "usage reading returned no sample";

/// What one reading returns: the sample and, for a source summing several
/// counters, each counter's raw CPU by name (`None`: the sample's own CPU is
/// the one counter).
pub type Reading = (
    Option<pb::WorkloadStatsSample>,
    Option<HashMap<String, u64>>,
);

#[derive(Default)]
struct State {
    /// CPU used in the activation; only ever grows.
    cpu: u64,
    /// The last raw value of each counter.
    last: HashMap<String, u64>,
    /// What the discovery read serves, pending samples included.
    latest: Option<pb::WorkloadStatsSample>,
    /// The newest sample with numbers, for the final record.
    measured: Option<pb::WorkloadStatsSample>,
    initial_done: bool,
    ended: bool,
}

/// One activation's usage.
pub struct Activation {
    epoch: i64,
    /// The counters start at zero with the activation, so a first reading
    /// counts in full.
    zero_base: bool,
    /// Serializes readings, so a later reading never holds an earlier value.
    read_lock: tokio::sync::Mutex<()>,
    state: Mutex<State>,
}

impl std::fmt::Debug for Activation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Activation")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// `t` in Unix nanoseconds.
pub fn unix_nanos(t: SystemTime) -> i64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

impl Activation {
    /// An activation starting at `now`. `resumes_counters` is set when the
    /// source's CPU counters survive into it, as a restored guest's do; CPU
    /// then counts from the first reading.
    pub fn new(now: SystemTime, resumes_counters: bool) -> Self {
        Activation {
            epoch: unix_nanos(now),
            zero_base: !resumes_counters,
            read_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(State::default()),
        }
    }

    /// When the activation began, Unix nanoseconds.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Stamps `s`, a sample with no CPU reading, with the epoch.
    pub fn with_epoch(&self, mut s: pb::WorkloadStatsSample) -> pb::WorkloadStatsSample {
        s.epoch_unix_nano = self.epoch;
        s
    }

    /// Takes one reading and returns its sample with the epoch set and the raw
    /// CPU replaced by the CPU used in this activation.
    ///
    /// A counter missing from a reading keeps its last value; one that
    /// decreased restarted, so its new value counts; one never seen before
    /// counts in full only when the counters start at zero. Waiting for
    /// another reading ends with `cancel`.
    pub async fn measure<F, Fut>(
        &self,
        cancel: &CancellationToken,
        read: F,
    ) -> Result<pb::WorkloadStatsSample>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Reading>>,
    {
        let _g = tokio::select! {
            g = self.read_lock.lock() => g,
            _ = cancel.cancelled() => return Err(Error::Cancelled),
        };
        let (s, parts) = read().await?;
        let mut s = s.ok_or_else(|| Error::msg(NO_SAMPLE))?;
        s.epoch_unix_nano = self.epoch;
        let parts = parts.unwrap_or_else(|| HashMap::from([(String::new(), s.cpu_usage_usec)]));
        s.cpu_usage_usec = self.fold(parts);
        Ok(s)
    }

    fn fold(&self, parts: HashMap<String, u64>) -> u64 {
        let mut st = self.state.lock();
        for (name, raw) in parts {
            st.cpu = match st.last.get(&name).copied() {
                None if self.zero_base => st.cpu.saturating_add(raw),
                None => st.cpu,
                Some(prev) if raw >= prev => st.cpu.saturating_add(raw - prev),
                Some(_) => st.cpu.saturating_add(raw),
            };
            st.last.insert(name, raw);
        }
        st.cpu
    }

    /// Records `s` as the latest sample, and as the newest measured one when
    /// it has numbers, each unless a newer one is already there.
    pub fn store(&self, s: pb::WorkloadStatsSample) {
        Self::store_locked(&mut self.state.lock(), s);
    }

    fn store_locked(st: &mut State, s: pb::WorkloadStatsSample) {
        // Compared on its own: a pending sample stored first must not discard
        // a measured one read earlier.
        if s.source != pb::StatsSource::Unspecified as i32
            && st
                .measured
                .as_ref()
                .is_none_or(|m| s.observed_at_unix_nano >= m.observed_at_unix_nano)
        {
            st.measured = Some(s.clone());
        }
        if st
            .latest
            .as_ref()
            .is_none_or(|l| s.observed_at_unix_nano >= l.observed_at_unix_nano)
        {
            st.latest = Some(s);
        }
    }

    /// Ends the wait for the initial reading, once. With a reading it stores
    /// it and runs `write`, unless the final record came first; `None` means
    /// the reading failed, and periodic records start without an initial one.
    /// `write` runs under the activation's lock.
    pub fn initial(
        &self,
        s: Option<pb::WorkloadStatsSample>,
        write: impl FnOnce(&pb::WorkloadStatsSample),
    ) {
        let mut st = self.state.lock();
        if st.initial_done {
            return;
        }
        st.initial_done = true;
        let Some(s) = s else { return };
        if st.ended {
            return;
        }
        write(&s);
        Self::store_locked(&mut st, s);
    }

    /// Whether a sweep should take a periodic reading: after the initial
    /// reading and before the final record.
    pub fn sampling(&self) -> bool {
        let st = self.state.lock();
        st.initial_done && !st.ended
    }

    /// Stores `s` and runs `write` while the activation is between its initial
    /// reading and its final record.
    pub fn periodic(
        &self,
        s: pb::WorkloadStatsSample,
        write: impl FnOnce(&pb::WorkloadStatsSample),
    ) {
        let mut st = self.state.lock();
        if !st.initial_done || st.ended {
            return;
        }
        write(&s);
        Self::store_locked(&mut st, s);
    }

    /// Ends the activation and runs `write` with the newest measured sample,
    /// once.
    pub fn finish(&self, write: impl FnOnce(Option<&pb::WorkloadStatsSample>)) {
        let mut st = self.state.lock();
        if st.ended {
            return;
        }
        st.ended = true;
        write(st.measured.as_ref());
    }

    /// The last stored sample.
    pub fn latest(&self) -> Option<pb::WorkloadStatsSample> {
        self.state.lock().latest.clone()
    }
}

#[cfg(test)]
mod tests;
