//! Helpers shared by the node service tests.

use std::sync::Arc;

use parking_lot::Mutex;

/// Collects log lines emitted on this thread while the guard lives.
pub(crate) struct LogCapture {
    buf: Arc<Mutex<Vec<u8>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

impl LogCapture {
    pub(crate) fn start() -> Self {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let w = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || CaptureWriter(w.clone()))
            .finish();
        // With no process-wide subscriber, a callsite first hit on a thread
        // without one while at most one scoped subscriber is alive caches
        // as uninteresting, hiding it from scoped subscribers made later.
        // Any process-wide subscriber rules that out; if another test
        // installed one first, that one serves.
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
        let guard = tracing::subscriber::set_default(sub);
        tracing::callsite::rebuild_interest_cache();
        LogCapture { buf, _guard: guard }
    }
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }
    /// Forgets what was captured so far.
    pub(crate) fn reset(&self) {
        self.buf.lock().clear();
    }
    /// How many times `msg` was logged.
    pub(crate) fn count(&self, msg: &str) -> usize {
        self.text().matches(msg).count()
    }
}

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
