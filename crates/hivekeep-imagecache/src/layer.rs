//! Image layers as a pull consumes them: a blob digest known from the
//! manifest, and an uncompressed tar stream opened on demand.
//!
//! The remote implementation streams the blob (download, digest check,
//! decompression) without buffering it, so memory use is independent of
//! layer size. Its stream is a blocking [`Read`], consumed on a blocking
//! thread by the unpacker.

use std::io::{self, Read};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_node::context::Ctx;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_util::sync::CancellationToken;

use crate::digest::{Digest, Hasher};
use crate::error::Result;
use crate::manifest::{layer_compression, Compression, Descriptor};
use crate::registry::Session;

/// A blocking byte stream.
pub type LayerReader = Box<dyn Read + Send>;

/// One layer of an image.
#[async_trait]
pub trait Layer: Send + Sync {
    /// The compressed blob's digest, from the manifest.
    fn digest(&self) -> Digest;
    /// Opens the uncompressed tar stream. The compressed bytes are checked
    /// against [`Layer::digest`] once the stream is read to its end.
    async fn uncompressed(&self, ctx: &Ctx) -> Result<LayerReader>;
}

/// A layer fetched from a registry.
pub struct RemoteLayer {
    session: Arc<Session>,
    descriptor: Descriptor,
    cancel: CancellationToken,
}

impl RemoteLayer {
    /// The layer `descriptor` names in `session`'s repository. `cancel`
    /// aborts an open stream mid-read.
    pub fn new(session: Arc<Session>, descriptor: Descriptor, cancel: CancellationToken) -> Self {
        RemoteLayer {
            session,
            descriptor,
            cancel,
        }
    }
}

#[async_trait]
impl Layer for RemoteLayer {
    fn digest(&self) -> Digest {
        self.descriptor.digest.clone()
    }

    async fn uncompressed(&self, ctx: &Ctx) -> Result<LayerReader> {
        let compression = layer_compression(&self.descriptor.media_type)?;
        let stream = self
            .session
            .blob_stream(ctx, &self.descriptor.digest, self.cancel.clone())
            .await?;
        let raw: LayerReader = Box::new(SyncIoBridge::new(StreamReader::new(stream)));
        let verified = VerifyingReader::new(raw, self.descriptor.digest.clone());
        decompress(Box::new(verified), compression)
    }
}

/// Wraps `r` in the decompressor for `compression`. The decompressor is
/// built on first read: building one reads the stream header, and a remote
/// stream may only be read off the async runtime.
pub fn decompress(r: LayerReader, compression: Compression) -> Result<LayerReader> {
    Ok(match compression {
        Compression::None => r,
        c => Box::new(LazyDecoder {
            pending: Some((r, c)),
            inner: None,
        }),
    })
}

struct LazyDecoder {
    pending: Option<(LayerReader, Compression)>,
    inner: Option<LayerReader>,
}

impl Read for LazyDecoder {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some((r, c)) = self.pending.take() {
            self.inner = Some(match c {
                Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(r)),
                Compression::Zstd => Box::new(zstd::stream::read::Decoder::new(r)?),
                Compression::None => r,
            });
        }
        match self.inner.as_mut() {
            Some(r) => r.read(buf),
            None => Ok(0),
        }
    }
}

/// Checks that everything read through it hashes to a digest, failing the
/// read that reaches end of stream otherwise.
pub struct VerifyingReader<R> {
    inner: R,
    want: Digest,
    hasher: Option<Hasher>,
    read: u64,
}

impl<R: Read> VerifyingReader<R> {
    /// Verifies `inner` against `want`.
    pub fn new(inner: R, want: Digest) -> Self {
        VerifyingReader {
            inner,
            want,
            hasher: Some(Hasher::new()),
            read: 0,
        }
    }
}

impl<R: Read> Read for VerifyingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.read += n as u64;
            if let Some(h) = self.hasher.as_mut() {
                h.update(&buf[..n]);
            }
            return Ok(n);
        }
        if !buf.is_empty() {
            if let Some(h) = self.hasher.take() {
                let got = h.finish();
                if got != self.want {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "error verifying sha256 checksum after reading {} bytes; got {:?}, want {:?}",
                            self.read, got.to_string(), self.want.to_string()
                        ),
                    ));
                }
            }
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifying_reader_checks_at_eof() {
        let data = b"hello world".to_vec();
        let mut ok = VerifyingReader::new(&data[..], Digest::of(&data));
        let mut out = Vec::new();
        ok.read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
        let mut bad = VerifyingReader::new(&data[..], Digest::of(b"other"));
        let err = bad.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("error verifying sha256 checksum"));
    }
}
