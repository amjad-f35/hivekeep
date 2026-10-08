//! A stdout handler that writes from its own thread.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::SystemTime;

use super::{Attr, Level, RecordHandler, StdoutRecord, Value};

/// The message of a drop report.
pub const DROP_REPORT: &str = "Dropped actor event records from stdout";

/// Writes records through another handler from its own thread, so a slow
/// stdout cannot block the caller. A full queue drops the record and counts
/// it; the writer reports the count through the same handler once it
/// catches up, so a level that silences the default logger cannot hide the
/// loss.
pub struct AsyncHandler {
    inner: Arc<dyn RecordHandler>,
    tx: Mutex<Option<SyncSender<StdoutRecord>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    dropped: Arc<AtomicU64>,
}

impl AsyncHandler {
    /// Starts the writer; `capacity` records may wait.
    pub fn new(inner: Arc<dyn RecordHandler>, capacity: usize) -> Self {
        let (tx, rx) = sync_channel::<StdoutRecord>(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let (base, d) = (inner.clone(), dropped.clone());
        let writer = std::thread::spawn(move || {
            let mut reported = 0;
            let report = |reported: &mut u64| {
                let n = d.load(Ordering::SeqCst);
                if n > *reported {
                    base.handle(StdoutRecord {
                        time: SystemTime::now(),
                        level: Level::Warn,
                        message: DROP_REPORT.into(),
                        attrs: vec![Attr {
                            key: "dropped".into(),
                            value: Value::U64(n - *reported),
                        }],
                    });
                    *reported = n;
                }
            };
            for rec in rx {
                base.handle(rec);
                report(&mut reported);
            }
            report(&mut reported);
        });
        AsyncHandler {
            inner,
            tx: Mutex::new(Some(tx)),
            writer: Mutex::new(Some(writer)),
            dropped,
        }
    }

    /// Writes what is queued and stops the writer. A record handled after
    /// this is written on the caller's thread.
    pub fn close(&self) {
        self.tx.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(w) = self.writer.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = w.join();
        }
    }

    /// Records dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }
}

impl RecordHandler for AsyncHandler {
    fn enabled(&self, level: Level) -> bool {
        self.inner.enabled(level)
    }

    fn handle(&self, rec: StdoutRecord) {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        match tx.as_ref() {
            // The writer is gone; a late record, as at shutdown, is written
            // here.
            None => {
                drop(tx);
                self.inner.handle(rec)
            }
            Some(tx) => match tx.try_send(rec) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    self.dropped.fetch_add(1, Ordering::SeqCst);
                }
                Err(TrySendError::Disconnected(rec)) => self.inner.handle(rec),
            },
        }
    }
}

impl Drop for AsyncHandler {
    fn drop(&mut self) {
        self.close();
    }
}
