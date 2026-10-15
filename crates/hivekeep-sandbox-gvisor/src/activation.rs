//! The usage bookkeeping of one activation: its epoch, cumulative CPU, the
//! latest sample, and which of its initial, periodic and final records have
//! been written.

use std::collections::HashMap;
use std::time::SystemTime;

use hivekeep_proto::worker::v1 as pb;
use parking_lot::Mutex;

use crate::error::{Error, Result};

#[derive(Default)]
struct State {
    /// CPU accumulated over the activation.
    cpu: u64,
    /// Last raw counter per part.
    last: HashMap<String, u64>,
    latest: Option<pb::WorkloadStatsSample>,
    /// The newest sample with real numbers.
    measured: Option<pb::WorkloadStatsSample>,
    initial_done: bool,
    ended: bool,
}

/// One activation's usage. Readings are serialized; records are written at
/// most once each for initial and final, and periodic ones only between.
pub struct Activation {
    epoch: i64,
    /// Counters seen for the first time count from zero (a fresh cgroup).
    zero_base: bool,
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

fn unix_nanos(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Now, in Unix nanoseconds.
pub fn now_unix_nanos() -> i64 {
    unix_nanos(SystemTime::now())
}

impl Activation {
    /// An activation starting `now`. `resumes_counters` is set when the
    /// counters carry over from before the activation, so their first values
    /// are a baseline rather than usage.
    pub fn new(now: SystemTime, resumes_counters: bool) -> Self {
        Activation {
            epoch: unix_nanos(now),
            zero_base: !resumes_counters,
            read_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(State::default()),
        }
    }

    /// The activation's start, Unix nanoseconds.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Stamps `s` with the epoch.
    pub fn with_epoch(&self, mut s: pb::WorkloadStatsSample) -> pb::WorkloadStatsSample {
        s.epoch_unix_nano = self.epoch;
        s
    }

    /// Takes one reading through `read`, one at a time, folding its raw CPU
    /// counters (by part; `None` means the sample's own counter) into the
    /// activation's cumulative CPU.
    pub async fn measure<F>(&self, read: F) -> Result<pb::WorkloadStatsSample>
    where
        F: FnOnce() -> Result<(
            Option<pb::WorkloadStatsSample>,
            Option<HashMap<String, u64>>,
        )>,
    {
        let _g = self.read_lock.lock().await;
        let (s, parts) = read()?;
        let mut s = s.ok_or_else(|| Error::msg("usage reading returned no sample"))?;
        s.epoch_unix_nano = self.epoch;
        let parts = parts.unwrap_or_else(|| HashMap::from([(String::new(), s.cpu_usage_usec)]));
        s.cpu_usage_usec = self.fold(parts);
        Ok(s)
    }

    fn fold(&self, parts: HashMap<String, u64>) -> u64 {
        let mut st = self.state.lock();
        for (name, raw) in parts {
            match st.last.get(&name).copied() {
                None if self.zero_base => st.cpu = st.cpu.saturating_add(raw),
                None => {}
                Some(prev) if raw >= prev => st.cpu = st.cpu.saturating_add(raw - prev),
                // The counter restarted.
                Some(_) => st.cpu = st.cpu.saturating_add(raw),
            }
            st.last.insert(name, raw);
        }
        st.cpu
    }

    /// Caches `s` as the latest sample.
    pub fn store(&self, s: pb::WorkloadStatsSample) {
        Self::store_locked(&mut self.state.lock(), s);
    }

    fn store_locked(st: &mut State, s: pb::WorkloadStatsSample) {
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

    /// Records the initial sample, once; `None` (no reading) only marks the
    /// initial record done. Nothing is written after the final record.
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

    /// Whether periodic sampling applies: after the initial record and
    /// before the final one.
    pub fn sampling(&self) -> bool {
        let st = self.state.lock();
        st.initial_done && !st.ended
    }

    /// Records a periodic sample while sampling.
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

    /// Writes the final record, once, from the newest measured sample.
    pub fn finish(&self, write: impl FnOnce(Option<&pb::WorkloadStatsSample>)) {
        let mut st = self.state.lock();
        if st.ended {
            return;
        }
        st.ended = true;
        write(st.measured.as_ref());
    }

    /// The latest cached sample.
    pub fn latest(&self) -> Option<pb::WorkloadStatsSample> {
        self.state.lock().latest.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(cpu: u64, at: i64) -> pb::WorkloadStatsSample {
        pb::WorkloadStatsSample {
            source: pb::StatsSource::Cgroup as i32,
            cpu_usage_usec: cpu,
            observed_at_unix_nano: at,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn folds_cpu_across_readings() {
        let a = Activation::new(SystemTime::now(), false);
        let s = a
            .measure(|| Ok((Some(measured(100, 1)), None)))
            .await
            .unwrap();
        assert_eq!((s.cpu_usage_usec, s.epoch_unix_nano), (100, a.epoch()));
        assert_eq!(
            a.measure(|| Ok((Some(measured(150, 2)), None)))
                .await
                .unwrap()
                .cpu_usage_usec,
            150
        );
        // A restarted counter adds its whole value.
        assert_eq!(
            a.measure(|| Ok((Some(measured(30, 3)), None)))
                .await
                .unwrap()
                .cpu_usage_usec,
            180
        );
        assert!(a.measure(|| Ok((None, None))).await.is_err());

        let resumed = Activation::new(SystemTime::now(), true);
        assert_eq!(
            resumed
                .measure(|| Ok((Some(measured(1000, 1)), None)))
                .await
                .unwrap()
                .cpu_usage_usec,
            0
        );
        assert_eq!(
            resumed
                .measure(|| Ok((Some(measured(1010, 2)), None)))
                .await
                .unwrap()
                .cpu_usage_usec,
            10
        );
    }

    #[test]
    fn records_initial_periodic_final_in_order() {
        let a = Activation::new(SystemTime::now(), false);
        let mut writes = Vec::new();
        a.periodic(measured(1, 1), |_| writes.push("periodic-early"));
        assert!(!a.sampling());
        a.initial(Some(measured(1, 1)), |_| writes.push("initial"));
        a.initial(Some(measured(1, 1)), |_| writes.push("initial-again"));
        assert!(a.sampling());
        a.periodic(measured(2, 2), |_| writes.push("periodic"));
        // A pending sample is latest but not measured.
        a.store(pb::WorkloadStatsSample {
            observed_at_unix_nano: 3,
            ..Default::default()
        });
        assert_eq!(a.latest().unwrap().observed_at_unix_nano, 3);
        a.finish(|m| {
            assert_eq!(m.unwrap().cpu_usage_usec, 2);
            writes.push("final");
        });
        a.finish(|_| writes.push("final-again"));
        a.periodic(measured(3, 4), |_| writes.push("periodic-late"));
        assert!(!a.sampling());
        assert_eq!(writes, vec!["initial", "periodic", "final"]);
    }

    #[test]
    fn initial_after_final_writes_nothing() {
        let a = Activation::new(SystemTime::now(), false);
        a.finish(|m| assert!(m.is_none()));
        a.initial(Some(measured(1, 1)), |_| {
            panic!("initial written after final")
        });
    }
}
