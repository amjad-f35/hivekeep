//! An in-memory object store and helpers for tests that drive the keeper,
//! the control plane or the plugins without cloud storage. The in-memory
//! [`crate::store::Store`]s live in [`crate::store::fake`].

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::storage::{BytesReader, ObjectReader, ObjectStorage, UploadBody};

/// An in-memory object store implementing both [`ObjectStorage`] and
/// [`Store`], keyed by `bucket/object`.
#[derive(Default)]
pub struct MemObjects {
    m: Mutex<BTreeMap<String, Bytes>>,
    streaming: bool,
    report_size: bool,
}

impl MemObjects {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Advertises streamed puts, so uploads pipe compression into them.
    pub fn streaming(mut self, on: bool) -> Self {
        self.streaming = on;
        self
    }

    /// Makes readers report the object's size, as the cloud clients do.
    pub fn report_size(mut self, on: bool) -> Self {
        self.report_size = on;
        self
    }

    /// Stores an object.
    pub fn insert(&self, bucket: &str, object: &str, data: impl Into<Bytes>) {
        self.m
            .lock()
            .insert(format!("{bucket}/{object}"), data.into());
    }

    /// An object's bytes.
    pub fn get(&self, bucket: &str, object: &str) -> Option<Bytes> {
        self.m.lock().get(&format!("{bucket}/{object}")).cloned()
    }

    /// Removes an object.
    pub fn remove(&self, bucket: &str, object: &str) {
        self.m.lock().remove(&format!("{bucket}/{object}"));
    }

    /// Every key, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.m.lock().keys().cloned().collect()
    }
}

#[async_trait]
impl ObjectStorage for MemObjects {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        let data = self.get(bucket, object).ok_or_else(|| {
            Error::not_found(format!(
                "object not found in storage backend: {bucket}/{object}"
            ))
        })?;
        Ok(if self.report_size {
            Box::new(BytesReader::sized(data))
        } else {
            Box::new(BytesReader::new(data))
        })
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        let data = body.read_all().await?;
        self.insert(bucket, object, data);
        Ok(())
    }

    fn streams_puts(&self) -> bool {
        self.streaming
    }
}

/// Captures JSON log lines on the current thread.
#[derive(Clone, Default)]
pub struct LogCapture {
    buf: Arc<Mutex<Vec<u8>>>,
}

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    /// Routes this thread's logs here until the guard drops.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let buf = self.buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || CaptureWriter(buf.clone()))
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// Everything logged so far.
    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }
}

/// Serializes tests that change process environment variables.
pub static ENV_LOCK: Mutex<()> = parking_lot::const_mutex(());
