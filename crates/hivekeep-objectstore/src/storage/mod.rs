//! Reading and writing snapshot content in object storage.
//!
//! [`ObjectStorage`] is the byte-moving surface a backend implements (GCS and
//! S3 live in [`crate::gcs`] and [`crate::s3`]). On top of it this module
//! compresses snapshot files into the sparse-extent zstd format, uploads them
//! within a shared memory budget, and restores them as sparse files.

pub mod codec;
pub mod env;
pub mod parzstd;
pub mod pipe;
pub mod ranged;
pub mod sparse;
pub mod sparsezstd;
pub mod transfer;
pub mod uploader;

#[cfg(test)]
pub(crate) mod testdata;

use std::fs::File;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, ReadBuf};

use crate::error::{Error, Result};

pub use codec::{DecodeResult, WriteContentResult};
pub use transfer::{
    fetch_file_with_zstd, fetch_object, fetch_to_path_with_zstd, open_object, send_path_with_zstd,
};
pub use uploader::{Uploader, DEFAULT_UPLOAD_MEMORY};

/// Which backend a client talks to; budgets are sized per backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// Google Cloud Storage.
    Gcs,
    /// S3 or an S3-compatible store.
    S3,
    /// Anything else, such as an in-memory store, which bounds its own
    /// buffers.
    Other,
}

/// An object being read.
pub trait ObjectRead: AsyncRead + Send + Unpin {
    /// The whole object's size as the backend reported it on opening.
    fn object_size(&self) -> Option<i64> {
        None
    }

    /// Stops any background reads and reports a failure to release the
    /// object. Dropping the reader also stops them.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A boxed [`ObjectRead`].
pub type ObjectReader = Box<dyn ObjectRead>;

/// The size an [`ObjectReader`]'s backend reported, if any.
pub fn object_size(r: &ObjectReader) -> Option<i64> {
    r.object_size().filter(|n| *n >= 0)
}

/// An in-memory object.
pub struct BytesReader {
    data: bytes::Bytes,
    pos: usize,
    size: Option<i64>,
}

impl BytesReader {
    /// A reader over `data` that reports no size.
    pub fn new(data: impl Into<bytes::Bytes>) -> Self {
        BytesReader {
            data: data.into(),
            pos: 0,
            size: None,
        }
    }

    /// A reader over `data` that reports its size.
    pub fn sized(data: impl Into<bytes::Bytes>) -> Self {
        let data: bytes::Bytes = data.into();
        let size = Some(data.len() as i64);
        BytesReader { data, pos: 0, size }
    }
}

impl AsyncRead for BytesReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = buf.remaining().min(self.data.len() - self.pos);
        let pos = self.pos;
        buf.put_slice(&self.data[pos..pos + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl ObjectRead for BytesReader {
    fn object_size(&self) -> Option<i64> {
        self.size
    }
}

/// An object reader over any [`AsyncRead`], with an optional size.
pub struct StreamObject<R> {
    inner: R,
    size: Option<i64>,
    on_close: Option<Box<dyn FnMut() + Send>>,
}

impl<R> StreamObject<R> {
    /// Wraps `inner`.
    pub fn new(inner: R, size: Option<i64>) -> Self {
        StreamObject {
            inner,
            size,
            on_close: None,
        }
    }

    /// Runs `f` when the reader is closed or dropped.
    pub fn on_close(mut self, f: impl FnMut() + Send + 'static) -> Self {
        self.on_close = Some(Box::new(f));
        self
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for StreamObject<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: AsyncRead + Send + Unpin> ObjectRead for StreamObject<R> {
    fn object_size(&self) -> Option<i64> {
        self.size
    }

    fn close(&mut self) -> io::Result<()> {
        if let Some(mut f) = self.on_close.take() {
            f();
        }
        Ok(())
    }
}

impl<R> Drop for StreamObject<R> {
    fn drop(&mut self) {
        if let Some(mut f) = self.on_close.take() {
            f();
        }
    }
}

/// An upload's body: a stream of at most `size_bound` bytes, when known.
pub struct UploadBody {
    /// The bytes.
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
    /// An upper bound on the remaining length, which lets a backend size its
    /// lookahead to the object instead of its largest buffer.
    pub size_bound: Option<i64>,
}

impl UploadBody {
    /// A body of unknown length.
    pub fn new(reader: impl AsyncRead + Send + 'static) -> Self {
        UploadBody {
            reader: Box::pin(reader),
            size_bound: None,
        }
    }

    /// A body of at most `max` bytes.
    pub fn bounded(reader: impl AsyncRead + Send + 'static, max: i64) -> Self {
        UploadBody {
            reader: Box::pin(reader),
            size_bound: Some(max),
        }
    }

    /// An in-memory body.
    pub fn from_bytes(data: impl Into<bytes::Bytes>) -> Self {
        let data: bytes::Bytes = data.into();
        let n = data.len() as i64;
        Self::bounded(BytesReader::new(data), n)
    }

    /// Reads the whole body.
    pub async fn read_all(mut self) -> io::Result<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let mut out = Vec::new();
        self.reader.read_to_end(&mut out).await?;
        Ok(out)
    }
}

/// The result of a sparse-file upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SparsePut {
    /// The file's logical size.
    pub logical_bytes: i64,
    /// The populated bytes compressed.
    pub populated_bytes: i64,
}

/// A backend that uploads a sparse file as concurrent parts by file range.
#[async_trait]
pub trait SparseFilePutter: Send + Sync {
    /// Compresses and uploads `file` in up to `max_parts` parts. `Ok(None)`
    /// means the file is too small to split, and the caller streams it
    /// instead.
    async fn put_sparse_file(
        &self,
        bucket: &str,
        object: &str,
        file: Arc<File>,
        max_parts: usize,
    ) -> Result<Option<SparsePut>>;
}

/// The byte-moving surface of an object store.
#[async_trait]
pub trait ObjectStorage: Send + Sync + 'static {
    /// Opens an object for reading. A missing object or bucket fails with
    /// [`crate::error::ErrorKind::NotFound`].
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader>;

    /// Writes `body` to an object, replacing it.
    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()>;

    /// The backend kind, which sizes upload budgets.
    fn backend(&self) -> BackendKind {
        BackendKind::Other
    }

    /// Whether `put_object` takes a streamed body without buffering it, so
    /// compression can be piped straight into the upload.
    fn streams_puts(&self) -> bool {
        false
    }

    /// The backend's range-parallel sparse upload, if it has one.
    fn sparse_putter(&self) -> Option<&dyn SparseFilePutter> {
        None
    }
}

#[async_trait]
impl<T: ObjectStorage + ?Sized> ObjectStorage for Arc<T> {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        (**self).get_object(bucket, object).await
    }
    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        (**self).put_object(bucket, object, body).await
    }
    fn backend(&self) -> BackendKind {
        (**self).backend()
    }
    fn streams_puts(&self) -> bool {
        (**self).streams_puts()
    }
    fn sparse_putter(&self) -> Option<&dyn SparseFilePutter> {
        (**self).sparse_putter()
    }
}

/// Splits `scheme://bucket/object` into the bucket and object name.
pub fn parse_object_url(url: &str) -> Result<(String, String)> {
    let u =
        url::Url::parse(url).map_err(|e| Error::other(format!("while parsing {url:?}: {e}")))?;
    let bucket = u.host_str().unwrap_or_default().to_string();
    let path = percent_encoding::percent_decode_str(u.path())
        .decode_utf8_lossy()
        .into_owned();
    Ok((bucket, path.trim_start_matches('/').to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn object_size_unknown() {
        let r: ObjectReader = Box::new(BytesReader::new(&b"x"[..]));
        assert_eq!(object_size(&r), None);
        let r: ObjectReader = Box::new(StreamObject::new(BytesReader::new(&b"x"[..]), Some(-1)));
        assert_eq!(object_size(&r), None);
        let mut r: ObjectReader = Box::new(BytesReader::sized(&b"xy"[..]));
        assert_eq!(object_size(&r), Some(2));
        let mut s = String::new();
        r.read_to_string(&mut s).await.unwrap();
        assert_eq!(s, "xy");
    }

    #[test]
    fn parses_object_urls() {
        assert_eq!(
            parse_object_url("gs://bucket/snap/memory.zstd").unwrap(),
            ("bucket".to_string(), "snap/memory.zstd".to_string())
        );
    }
}
