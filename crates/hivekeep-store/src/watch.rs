//! Subscriptions to Worker changes.

use hivekeep_proto::api::v1 as pb;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Capacity of a watch's event buffer.
pub const WATCH_BUFFER: usize = 128;

/// What happened to a Worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WorkerEventType {
    /// The Worker was registered.
    Created = 0,
    /// The Worker (or its allocation) changed.
    Updated = 1,
    /// The Worker was removed.
    Deleted = 2,
}

impl WorkerEventType {
    /// The type encoded as `b`, if any.
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Created),
            1 => Some(Self::Updated),
            2 => Some(Self::Deleted),
            _ => None,
        }
    }
}

/// One Worker change.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerEvent {
    /// What happened.
    pub kind: WorkerEventType,
    /// The Worker as written (or as it was, for a delete).
    pub worker: pb::Worker,
}

/// An active subscription to Worker changes.
///
/// [`recv`](Self::recv) yields events until the watch ends, then `None`. A
/// watch ends when it is closed or dropped, or when the backend can no longer
/// guarantee it has delivered every change (it fell behind, or lost its
/// connection). Callers treat the end as a signal to re-list and
/// re-subscribe.
#[derive(Debug)]
pub struct WorkerWatch {
    events: mpsc::Receiver<WorkerEvent>,
    stop: CancellationToken,
}

impl WorkerWatch {
    /// A watch fed by `events`, whose producer stops when `stop` is
    /// cancelled.
    pub fn new(events: mpsc::Receiver<WorkerEvent>, stop: CancellationToken) -> Self {
        WorkerWatch { events, stop }
    }

    /// The next event, or `None` once the watch has ended.
    pub async fn recv(&mut self) -> Option<WorkerEvent> {
        self.events.recv().await
    }

    /// Ends the watch. Events already buffered are still returned by
    /// [`recv`](Self::recv); nothing new is. Safe to call repeatedly.
    pub fn close(&mut self) {
        self.stop.cancel();
        self.events.close();
    }
}

impl Drop for WorkerWatch {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
