//! Metrics of the tracker's background uploads and detaches, and of the
//! actors on this node holding a snapshot object storage does not have.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use tonic::Status;

use super::actor::ActorSnapshots;
use super::snapshot_ref::SnapshotRef;
use crate::clock::SharedClock;
use crate::context::Ctx;
use crate::labels::fidelity_label;

const UPLOAD_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];
const DETACH_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0,
];
const OVERDUE_BUCKETS: &[f64] = &[
    0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0,
];

/// How long uploads have been overdue, in bands.
const BANDS: [&str; 4] = ["under_1m", "1m_to_5m", "5m_to_15m", "over_15m"];

fn band(overdue: Duration) -> usize {
    let m = Duration::from_secs(60);
    if overdue <= m {
        0
    } else if overdue <= 5 * m {
        1
    } else if overdue <= 15 * m {
        2
    } else {
        3
    }
}

/// The tracker's instruments.
#[derive(Clone)]
pub struct Instruments {
    upload_duration: HistogramVec,
    detach_duration: HistogramVec,
    upload_overdue: prometheus::Histogram,
    detach_overdue: prometheus::Histogram,
    tally_slot: Arc<Mutex<Option<Arc<UndurableTally>>>>,
}

impl Instruments {
    /// Registers the instruments on `registry`.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let upload_duration = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_keeper_snapshot_upload_duration_seconds",
                "Duration of one background upload of a local snapshot to object storage.",
            )
            .buckets(UPLOAD_BUCKETS.to_vec()),
            &["hk_snapshot_fidelity", "error_type"],
        )?;
        let detach_duration = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_keeper_snapshot_detach_duration_seconds",
                "Duration of deleting an actor's local snapshots and node files in a background detach, excluding any upload.",
            )
            .buckets(DETACH_BUCKETS.to_vec()),
            &["hk_snapshot_fidelity", "error_type"],
        )?;
        let upload_overdue = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "hivekeep_keeper_snapshot_upload_overdue_duration_seconds",
                "Time from when a background upload is due to when a worker starts it.",
            )
            .buckets(OVERDUE_BUCKETS.to_vec()),
        )?;
        let detach_overdue = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "hivekeep_keeper_snapshot_detach_overdue_duration_seconds",
                "Time from when a background detach is due to when a worker starts it.",
            )
            .buckets(OVERDUE_BUCKETS.to_vec()),
        )?;
        let tally_slot = Arc::new(Mutex::new(None));
        registry.register(Box::new(upload_duration.clone()))?;
        registry.register(Box::new(detach_duration.clone()))?;
        registry.register(Box::new(upload_overdue.clone()))?;
        registry.register(Box::new(detach_overdue.clone()))?;
        registry.register(Box::new(TallyCollector::new(tally_slot.clone())?))?;
        Ok(Instruments {
            upload_duration,
            detach_duration,
            upload_overdue,
            detach_overdue,
            tally_slot,
        })
    }

    pub(crate) fn watch(&self, tally: Arc<UndurableTally>) {
        *self.tally_slot.lock() = Some(tally);
    }

    /// Records one background upload attempt. A success that found the
    /// snapshot already committed uploaded nothing, so it is not recorded.
    pub(crate) fn record_upload(
        &self,
        ctx: &Ctx,
        target: &SnapshotRef,
        elapsed: Duration,
        uploaded: bool,
        err: Option<&Status>,
    ) {
        if err.is_none() && !uploaded {
            return;
        }
        self.upload_duration
            .with_label_values(&[fidelity_label(target.fidelity()), &error_type(ctx, err)])
            .observe(elapsed.as_secs_f64());
    }

    /// Records one background detach's cleanup.
    pub(crate) fn record_detach(
        &self,
        ctx: &Ctx,
        target: &SnapshotRef,
        elapsed: Duration,
        err: Option<&Status>,
    ) {
        self.detach_duration
            .with_label_values(&[fidelity_label(target.fidelity()), &error_type(ctx, err)])
            .observe(elapsed.as_secs_f64());
    }

    pub(crate) fn record_upload_overdue(&self, overdue: Duration) {
        self.upload_overdue.observe(overdue.as_secs_f64());
    }

    pub(crate) fn record_detach_overdue(&self, overdue: Duration) {
        self.detach_overdue.observe(overdue.as_secs_f64());
    }
}

/// The gRPC code naming a failure, or "" for success. A context that ended
/// names its own reason.
pub(super) fn error_type(ctx: &Ctx, err: Option<&Status>) -> String {
    let Some(err) = err else { return String::new() };
    if let Some(c) = ctx.err() {
        return crate::status::code_name(Status::from(c).code()).to_string();
    }
    crate::status::code_name(err.code()).to_string()
}

#[derive(Debug, Clone, Copy)]
struct Summary {
    undurable_since: DateTime<Utc>,
    uploadable: bool,
}

/// The actors holding an undurable local snapshot on this node.
pub struct UndurableTally {
    clock: SharedClock,
    upload_delay: Duration,
    has_store: bool,
    actors: Mutex<HashMap<String, Summary>>,
}

/// What a tally reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TallyCounts {
    /// Actors with an overdue upload, by band.
    pub overdue_bands: [i64; 4],
    /// Age of the oldest undurable local snapshot.
    pub oldest_undurable: Duration,
}

impl UndurableTally {
    pub(crate) fn new(clock: SharedClock, upload_delay: Duration, has_store: bool) -> Self {
        UndurableTally {
            clock,
            upload_delay,
            has_store,
            actors: Mutex::new(HashMap::new()),
        }
    }

    /// Updates the actor's summary from its entry.
    pub(crate) fn observe(&self, a: &ActorSnapshots) {
        let Some(local) = &a.rec.latest_local else {
            self.forget(&a.actor_uid);
            return;
        };
        if !a.upload_needed(local) {
            self.forget(&a.actor_uid);
            return;
        }
        let keep = a.keep_uploading();
        let since = a
            .rec
            .undurable_since
            .or_else(|| {
                if keep {
                    a.rec.uploading.as_ref().and_then(|u| u.commit_time)
                } else {
                    None
                }
            })
            .or(local.commit_time);
        let Some(since) = since else {
            self.forget(&a.actor_uid);
            return;
        };
        let s = Summary {
            undurable_since: since,
            uploadable: self.has_store && (a.can_upload(Some(local)) || keep),
        };
        self.actors.lock().insert(a.actor_uid.clone(), s);
    }

    /// Drops the actor.
    pub(crate) fn forget(&self, actor_uid: &str) {
        self.actors.lock().remove(actor_uid);
    }

    /// The current counts.
    pub fn counts(&self) -> TallyCounts {
        let now = self.clock.now();
        let mut c = TallyCounts::default();
        for s in self.actors.lock().values() {
            let age = (now - s.undurable_since).to_std().unwrap_or_default();
            c.oldest_undurable = c.oldest_undurable.max(age);
            if s.uploadable && age > self.upload_delay {
                c.overdue_bands[band(age - self.upload_delay)] += 1;
            }
        }
        c
    }
}

/// Reports the tally's counts at each scrape.
struct TallyCollector {
    slot: Arc<Mutex<Option<Arc<UndurableTally>>>>,
    overdue: GaugeVec,
    age: prometheus::Gauge,
    descs: Vec<Desc>,
}

impl TallyCollector {
    fn new(slot: Arc<Mutex<Option<Arc<UndurableTally>>>>) -> prometheus::Result<Self> {
        let overdue = GaugeVec::new(
            Opts::new(
                "hivekeep_keeper_snapshot_upload_overdue_actors",
                "Actors on this node whose background snapshot upload is overdue, by how long.",
            ),
            &["hk_snapshot_upload_overdue_band"],
        )?;
        let age = prometheus::Gauge::with_opts(Opts::new(
            "hivekeep_keeper_snapshot_undurable_age_max_seconds",
            "Age of the oldest local snapshot on this node that object storage does not hold yet, or zero.",
        ))?;
        let mut descs: Vec<Desc> = overdue.desc().into_iter().cloned().collect();
        descs.extend(age.desc().into_iter().cloned());
        Ok(TallyCollector {
            slot,
            overdue,
            age,
            descs,
        })
    }
}

impl Collector for TallyCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Some(tally) = self.slot.lock().clone() else {
            return Vec::new();
        };
        let c = tally.counts();
        for (i, b) in BANDS.iter().enumerate() {
            self.overdue
                .with_label_values(&[b])
                .set(c.overdue_bands[i] as f64);
        }
        self.age.set(c.oldest_undurable.as_secs_f64());
        let mut out = self.overdue.collect();
        out.extend(self.age.collect());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::snapshot_tracker::actor::ActorRecord;
    use hivekeep_core::resource::ActorRef;
    use hivekeep_proto::keeper::v1 as pb;

    #[test]
    fn bands() {
        assert_eq!(band(Duration::from_secs(30)), 0);
        assert_eq!(band(Duration::from_secs(120)), 1);
        assert_eq!(band(Duration::from_secs(600)), 2);
        assert_eq!(band(Duration::from_secs(3600)), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn tally_counts_overdue_uploadable_actors() {
        let start = DateTime::from_timestamp(10_000, 0).unwrap();
        let clock = Arc::new(MockClock::new(start));
        let tally = UndurableTally::new(clock.clone(), Duration::from_secs(300), true);
        let mut a = ActorSnapshots::new(1, "u", ActorRecord::default());
        a.rec.latest_local = Some(SnapshotRef {
            snapshot: pb::Snapshot {
                snapshot_uuid: "s".into(),
                generation: 1,
                fidelity: 3,
                object: Some(pb::ObjectStorage {
                    snapshot_uri: "gs://b/x".into(),
                    sharing: 0,
                }),
            },
            commit_time: Some(start),
            upload_time: None,
            actor: ActorRef::new("h", "a"),
        });
        tally.observe(&a);
        assert_eq!(tally.counts().overdue_bands, [0; 4]);
        tokio::time::advance(Duration::from_secs(400)).await;
        let c = tally.counts();
        assert_eq!(c.overdue_bands, [0, 1, 0, 0]);
        assert_eq!(c.oldest_undurable, Duration::from_secs(400));
        tally.forget("u");
        assert_eq!(tally.counts(), TallyCounts::default());
    }

    #[test]
    fn collector_registers() {
        let reg = Registry::new();
        Instruments::new(&reg).unwrap();
        assert!(
            Instruments::new(&reg).is_err(),
            "double registration is refused"
        );
    }
}
