//! One direction of a tunneled byte stream.
//!
//! Most tunneled connections carry small request/response exchanges, so
//! copying starts with a small buffer and only moves to a large one once a
//! read fills it; every chunk is written as soon as it is read.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Size of the first read buffer.
pub const SMALL_BUF_SIZE: usize = 512;
/// Size of the buffer used once a read fills the small one.
pub const BUF_SIZE: usize = 32 << 10;

/// Counters of one copy, kept for error context.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CopyStats {
    /// Bytes read from the source.
    pub read: u64,
    /// Bytes written to the destination.
    pub written: u64,
}

/// A failed copy, with how far it got.
#[derive(Debug, thiserror::Error)]
pub enum CopyError {
    /// Reading the source failed.
    #[error("egress: reading stream after {} bytes read, {} written: {source}", stats.read, stats.written)]
    Read {
        /// Progress at the failure.
        stats: CopyStats,
        /// The read error.
        source: io::Error,
    },
    /// Writing the destination failed.
    #[error("egress: writing stream after {} bytes read, {} written: {source}", stats.read, stats.written)]
    Write {
        /// Progress at the failure.
        stats: CopyStats,
        /// The write error.
        source: io::Error,
    },
}

impl CopyError {
    /// The underlying I/O error.
    pub fn io(&self) -> &io::Error {
        match self {
            CopyError::Read { source, .. } | CopyError::Write { source, .. } => source,
        }
    }

    /// Progress at the failure.
    pub fn stats(&self) -> CopyStats {
        match self {
            CopyError::Read { stats, .. } | CopyError::Write { stats, .. } => *stats,
        }
    }
}

async fn write_chunk<W: AsyncWrite + Unpin + ?Sized>(
    dst: &mut W,
    buf: &[u8],
    stats: &mut CopyStats,
) -> Result<(), CopyError> {
    let mut rest = buf;
    while !rest.is_empty() {
        match dst.write(rest).await {
            Ok(0) => {
                return Err(CopyError::Write {
                    stats: *stats,
                    source: io::ErrorKind::WriteZero.into(),
                })
            }
            Ok(n) => {
                stats.written += n as u64;
                rest = &rest[n..];
            }
            Err(source) => {
                return Err(CopyError::Write {
                    stats: *stats,
                    source,
                })
            }
        }
    }
    Ok(())
}

/// Copies `src` to `dst` until end of stream, returning bytes written.
pub async fn stream_copy<R, W>(src: &mut R, dst: &mut W) -> Result<CopyStats, CopyError>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut stats = CopyStats::default();
    let mut small = [0u8; SMALL_BUF_SIZE];
    let mut big: Option<Vec<u8>> = None;
    loop {
        let buf: &mut [u8] = match big.as_mut() {
            Some(b) => b,
            None => &mut small,
        };
        let n = match src.read(buf).await {
            Ok(0) => return Ok(stats),
            Ok(n) => n,
            Err(source) => return Err(CopyError::Read { stats, source }),
        };
        stats.read += n as u64;
        let full = n == buf.len();
        write_chunk(dst, &buf[..n], &mut stats).await?;
        match (&big, full) {
            (None, true) => big = Some(vec![0; BUF_SIZE]),
            (Some(_), false) => big = None,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    struct ChunkReader {
        data: Vec<u8>,
        sizes: Vec<usize>,
        reads: usize,
    }

    impl AsyncRead for ChunkReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.data.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let size = self.sizes[self.reads % self.sizes.len()];
            let n = size.min(buf.remaining()).min(self.data.len());
            self.reads += 1;
            let chunk: Vec<u8> = self.data.drain(..n).collect();
            buf.put_slice(&chunk);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn copies_everything() {
        let data: Vec<u8> = (0..1 << 20).map(|i: usize| (i * 7) as u8).collect();
        for sizes in [
            vec![1],
            vec![SMALL_BUF_SIZE - 1],
            vec![SMALL_BUF_SIZE],
            vec![SMALL_BUF_SIZE + 1],
            vec![SMALL_BUF_SIZE, 1, BUF_SIZE, 3],
            vec![BUF_SIZE],
            vec![1 << 20],
        ] {
            let mut src = ChunkReader {
                data: data.clone(),
                sizes: sizes.clone(),
                reads: 0,
            };
            let mut got = Vec::new();
            let stats = stream_copy(&mut src, &mut got).await.unwrap();
            assert_eq!(got, data, "{sizes:?}");
            assert_eq!(
                stats,
                CopyStats {
                    read: data.len() as u64,
                    written: data.len() as u64
                }
            );
        }
    }

    struct ErrReader;
    impl AsyncRead for ErrReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("read failed")))
        }
    }

    /// Accepts half of the first write, then fails (or reports zero bytes).
    struct HalfWriter {
        calls: usize,
        fail: bool,
    }
    impl AsyncWrite for HalfWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.calls += 1;
            if self.calls == 1 {
                return Poll::Ready(Ok(buf.len() / 2));
            }
            if self.fail {
                Poll::Ready(Err(io::Error::other("write failed")))
            } else {
                Poll::Ready(Ok(0))
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn returns_errors() {
        let mut src = tokio::io::AsyncReadExt::chain(&b"abc"[..], ErrReader);
        let err = stream_copy(&mut src, &mut tokio::io::sink())
            .await
            .unwrap_err();
        assert_eq!(err.io().to_string(), "read failed");
        assert!(
            err.to_string()
                .contains("reading stream after 3 bytes read, 3 written"),
            "{err}"
        );

        let err = stream_copy(
            &mut &b"abcd"[..],
            &mut HalfWriter {
                calls: 0,
                fail: true,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.stats().written, 2);
        assert!(
            err.to_string()
                .contains("writing stream after 4 bytes read, 2 written"),
            "{err}"
        );

        let err = stream_copy(
            &mut &b"abcd"[..],
            &mut HalfWriter {
                calls: 0,
                fail: false,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.io().kind(), io::ErrorKind::WriteZero, "short write");
    }

    struct NotifyWriter(tokio::sync::mpsc::UnboundedSender<usize>);
    impl AsyncWrite for NotifyWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let _ = self.0.send(buf.len());
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn delivers_without_waiting_for_more_data() {
        for size in [SMALL_BUF_SIZE, BUF_SIZE] {
            let (mut pw, mut pr) = tokio::io::duplex(BUF_SIZE * 2);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let copy = tokio::spawn(async move {
                stream_copy(&mut pr, &mut NotifyWriter(tx))
                    .await
                    .map(|_| ())
            });
            let mut messages = vec![SMALL_BUF_SIZE];
            if size == BUF_SIZE {
                messages.push(BUF_SIZE);
            }
            for n in messages {
                pw.write_all(&vec![0; n]).await.unwrap();
                let mut got = 0;
                while got < n {
                    got += tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                        .await
                        .expect("message to be copied")
                        .unwrap();
                }
                assert_eq!(got, n);
            }
            drop(pw);
            copy.await.unwrap().unwrap();
        }
    }
}
