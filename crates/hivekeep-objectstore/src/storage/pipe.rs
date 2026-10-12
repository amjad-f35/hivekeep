//! A bounded pipe from a blocking writer (a compressor on a worker thread)
//! to an async reader (an upload).

use std::io::{self, Write};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

/// How much the writer buffers before it hands a chunk over.
const PIPE_CHUNK: usize = 256 << 10;
/// Chunks in flight.
const PIPE_DEPTH: usize = 4;

/// The write end. Dropping it, or [`close`](Self::close), ends the stream.
pub struct PipeWriter {
    tx: Option<mpsc::Sender<io::Result<Bytes>>>,
    buf: BytesMut,
}

/// The read end.
pub type PipeReader = StreamReader<ReceiverStream<io::Result<Bytes>>, Bytes>;

/// A new pipe.
pub fn pipe() -> (PipeWriter, PipeReader) {
    let (tx, rx) = mpsc::channel(PIPE_DEPTH);
    (
        PipeWriter {
            tx: Some(tx),
            buf: BytesMut::with_capacity(PIPE_CHUNK),
        },
        StreamReader::new(ReceiverStream::new(rx)),
    )
}

impl PipeWriter {
    fn send(&mut self, item: io::Result<Bytes>) -> io::Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"))?;
        tx.blocking_send(item)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the upload stopped reading"))
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = self.buf.split().freeze();
        self.send(Ok(chunk))
    }

    /// Ends the stream, delivering `err` to the reader instead of EOF when
    /// set.
    pub fn close(mut self, err: Option<io::Error>) {
        match err {
            None => {
                let _ = self.flush_buf();
            }
            Some(e) => {
                self.buf.clear();
                let _ = self.send(Err(e));
            }
        }
        self.tx.take();
    }
}

impl Write for PipeWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.tx.is_none() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));
        }
        self.buf.extend_from_slice(b);
        if self.buf.len() >= PIPE_CHUNK {
            self.flush_buf()?;
        }
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn carries_bytes_and_errors() {
        let (mut w, mut r) = pipe();
        let h = tokio::task::spawn_blocking(move || {
            w.write_all(&vec![7u8; 1 << 20]).unwrap();
            w.close(None);
        });
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        h.await.unwrap();
        assert_eq!(out.len(), 1 << 20);

        let (mut w, mut r) = pipe();
        tokio::task::spawn_blocking(move || {
            w.write_all(b"x").unwrap();
            w.close(Some(io::Error::other("compress failed")));
        });
        let mut out = Vec::new();
        assert!(r.read_to_end(&mut out).await.is_err());

        let (mut w, r) = pipe();
        drop(r);
        let e = tokio::task::spawn_blocking(move || w.write_all(&vec![0u8; 1 << 20]))
            .await
            .unwrap();
        assert_eq!(e.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    }
}
