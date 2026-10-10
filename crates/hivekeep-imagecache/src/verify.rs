//! Checking layer content against its claimed diffID before it enters the
//! pool.
//!
//! The pool shares layers across images by diffID, but each image's author
//! chooses the diffIDs in its config. Hashing the uncompressed stream as it
//! unpacks stops one image from planting content under another image's
//! layer. Reading the stream to its end also completes the compressed blob's
//! own digest check.

use std::io::{self, Read};

use hivekeep_node::context::Ctx;

use crate::digest::{Digest, Hasher};
use crate::error::{Error, ErrorKind, Result};
use crate::layer::{Layer, LayerReader};

/// The message of a diffID mismatch.
pub const DIFF_ID_MISMATCH: &str = "layer content does not match its claimed diffID";

/// Counts and hashes everything read through it.
struct Tee<R> {
    inner: R,
    hasher: Hasher,
    consumed: i64,
}

impl<R: Read> Read for Tee<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        self.consumed += n as i64;
        Ok(n)
    }
}

/// Passes `stream` to `consume`, drains whatever it left unread, and checks
/// the whole stream hashes to `diff_id`. Returns only the bytes `consume`
/// read: the rest never lands on disk, so counting it would let an image
/// inflate the size eviction accounts for.
pub fn verify_stream(
    stream: LayerReader,
    diff_id: &Digest,
    consume: impl FnOnce(&mut dyn Read) -> Result<()>,
) -> Result<i64> {
    let mut tee = Tee {
        inner: stream,
        hasher: Hasher::new(),
        consumed: 0,
    };
    consume(&mut tee)?;
    let consumed = tee.consumed;
    io::copy(&mut tee, &mut io::sink())
        .map_err(|e| Error::msg(format!("while draining layer stream: {e}")))?;
    let got = tee.hasher.finish();
    if &got != diff_id {
        return Err(Error::new(
            ErrorKind::DiffIdMismatch,
            format!("{DIFF_ID_MISMATCH}: content hashes to {got}, config claims {diff_id}"),
        ));
    }
    Ok(consumed)
}

/// Opens `layer` and runs [`verify_stream`] over it on a blocking thread.
pub async fn read_verified_layer<F>(
    ctx: &Ctx,
    layer: &dyn Layer,
    diff_id: &Digest,
    consume: F,
) -> Result<i64>
where
    F: FnOnce(&mut dyn Read) -> Result<()> + Send + 'static,
{
    let stream = layer
        .uncompressed(ctx)
        .await
        .map_err(|e| e.context("while opening layer stream"))?;
    let diff_id = diff_id.clone();
    crate::spawn::blocking(move || verify_stream(stream, &diff_id, consume)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::image::{lib_layers, mem_layer, TarEntry};

    // The hash covers the whole uncompressed stream, while the returned
    // length covers only what the consumer read.
    #[tokio::test]
    async fn read_verified_layer_drains_stream() {
        let (good, evil) = lib_layers();
        let diff_id = good.diff_id.clone();
        let read_ten = |r: &mut dyn Read| -> Result<()> {
            io::copy(&mut r.take(10), &mut io::sink()).map_err(|e| Error::msg(e.to_string()))?;
            Ok(())
        };
        let n = read_verified_layer(&Ctx::background(), &good, &diff_id, read_ten)
            .await
            .unwrap();
        assert_eq!(n, 10);
        let err = read_verified_layer(&Ctx::background(), &evil, &diff_id, read_ten)
            .await
            .unwrap_err();
        assert!(err.is(ErrorKind::DiffIdMismatch), "{err}");
    }

    #[test]
    fn verify_stream_counts_consumed_only() {
        let l = mem_layer(&[TarEntry::file("a", "x")]);
        let raw = l.uncompressed_bytes.clone();
        let n = verify_stream(Box::new(io::Cursor::new(raw)), &l.diff_id, |_| Ok(())).unwrap();
        assert_eq!(n, 0);
    }
}
