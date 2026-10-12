//! Fetching one object as parallel byte ranges, handed out in order.
//!
//! A restore spends most of a download blocked on the socket (450 MB on a
//! cloud worker node took about 2.2 s, 1.3-1.6 s of it waiting for bytes),
//! and one stream is the limit, so an object is fetched as consecutive
//! ranges in parallel. Nothing reads the object's contents here: the ranges
//! are the original byte stream again before anything parses it, so this
//! works on objects written by any earlier version.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{ObjectRead, ObjectReader};
use crate::error::{Error, Result};

/// How much one ranged request fetches. Peak buffering is this times
/// [`DOWNLOAD_CONCURRENCY`].
pub const DOWNLOAD_CHUNK_SIZE: i64 = 16 << 20;
/// How many ranges are in flight.
pub const DOWNLOAD_CONCURRENCY: usize = 8;

/// Reads the object's `[off, off+n)`; the `usize` is the range's index,
/// which a backend uses to spread ranges over connections.
pub type FetchRange =
    Arc<dyn Fn(usize, i64, i64) -> BoxFuture<'static, Result<Bytes>> + Send + Sync>;

type Chunk = std::result::Result<(Bytes, OwnedSemaphorePermit), Error>;

/// Reassembles parallel ranged reads into one ordered stream.
pub struct RangedReader {
    size: i64,
    cancel: CancellationToken,
    ordered: mpsc::Receiver<oneshot::Receiver<Chunk>>,
    pending: Option<oneshot::Receiver<Chunk>>,
    cur: Bytes,
    cur_permit: Option<OwnedSemaphorePermit>,
    err: Option<Error>,
    done: bool,
}

/// Starts fetching `size` bytes in chunks, at most
/// [`DOWNLOAD_CONCURRENCY`] in flight. `head`, the already open body of the
/// first range, supplies the first chunk. Cancelling `parent` stops the
/// fetches and fails the reads; so does closing or dropping the reader.
pub fn new_ranged_reader(
    parent: &CancellationToken,
    size: i64,
    head: ObjectReader,
    fetch: FetchRange,
) -> RangedReader {
    let cancel = parent.child_token();
    let (tx, rx) = mpsc::channel(DOWNLOAD_CONCURRENCY);
    let slots = Arc::new(Semaphore::new(DOWNLOAD_CONCURRENCY));
    tokio::spawn(schedule(cancel.clone(), size, head, fetch, slots, tx));
    RangedReader {
        size,
        cancel,
        ordered: rx,
        pending: None,
        cur: Bytes::new(),
        cur_permit: None,
        err: None,
        done: false,
    }
}

fn stopped(off: i64, size: i64) -> Error {
    Error::canceled().context(format!("ranged read stopped at offset {off} of {size}"))
}

async fn schedule(
    cancel: CancellationToken,
    size: i64,
    head: ObjectReader,
    fetch: FetchRange,
    slots: Arc<Semaphore>,
    ordered: mpsc::Sender<oneshot::Receiver<Chunk>>,
) {
    let mut head = Some(head);
    let mut off = 0i64;
    let mut i = 0usize;
    while off < size {
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                stop(&ordered, off, size).await;
                return;
            }
            p = slots.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => return,
            },
        };
        let (tx, rx) = oneshot::channel();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                stop(&ordered, off, size).await;
                return;
            }
            sent = ordered.send(rx) => if sent.is_err() { return },
        }
        let n = DOWNLOAD_CHUNK_SIZE.min(size - off);
        let fut: BoxFuture<'static, Result<Bytes>> = match head.take() {
            Some(mut h) => Box::pin(async move {
                let mut buf = vec![0u8; n as usize];
                h.read_exact(&mut buf).await.map_err(|e| {
                    Error::from(e).context(format!("while reading range {off}+{n}"))
                })?;
                Ok(Bytes::from(buf))
            }),
            None => fetch(i, off, n),
        };
        let c = cancel.clone();
        tokio::spawn(async move {
            let res = tokio::select! {
                biased;
                _ = c.cancelled() => Err(stopped(off, size)),
                r = fut => r,
            };
            let _ = tx.send(res.map(|b| (b, permit)));
        });
        off += n;
        i += 1;
    }
}

async fn stop(ordered: &mpsc::Sender<oneshot::Receiver<Chunk>>, off: i64, size: i64) {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(Err(stopped(off, size)));
    let _ = ordered.send(rx).await;
}

impl RangedReader {
    /// The whole object's size.
    pub fn size(&self) -> i64 {
        self.size
    }
}

impl AsyncRead for RangedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        loop {
            if let Some(e) = &this.err {
                return Poll::Ready(Err(e.clone().into()));
            }
            if !this.cur.is_empty() {
                let n = buf.remaining().min(this.cur.len());
                buf.put_slice(&this.cur.split_to(n));
                if this.cur.is_empty() {
                    // Hand the slot back for the next range.
                    this.cur_permit = None;
                }
                return Poll::Ready(Ok(()));
            }
            if this.done {
                return Poll::Ready(Ok(()));
            }
            if this.pending.is_none() {
                match this.ordered.poll_recv(cx) {
                    Poll::Ready(Some(rx)) => this.pending = Some(rx),
                    Poll::Ready(None) => {
                        this.done = true;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            let rx = this.pending.as_mut().expect("pending is set");
            match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(Ok((bytes, permit)))) => {
                    this.pending = None;
                    this.cur = bytes;
                    this.cur_permit = Some(permit);
                }
                Poll::Ready(Ok(Err(e))) => {
                    this.pending = None;
                    this.err = Some(e);
                }
                Poll::Ready(Err(_)) => {
                    this.pending = None;
                    this.err = Some(stopped(0, this.size));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl ObjectRead for RangedReader {
    fn object_size(&self) -> Option<i64> {
        Some(self.size)
    }

    fn close(&mut self) -> io::Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}

impl Drop for RangedReader {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{BytesReader, StreamObject};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn close_stops_requests() {
        let (head, mut body) = tokio::io::duplex(64);
        let (ctx_tx, mut ctx_rx) = mpsc::channel::<CancellationToken>(1);
        let parent = CancellationToken::new();
        let fetch_token = parent.clone();
        let fetch: FetchRange = Arc::new(move |_, _, _| {
            let tx = ctx_tx.clone();
            let t = fetch_token.child_token();
            Box::pin(async move {
                let _ = tx.send(t.clone()).await;
                std::future::pending::<()>().await;
                Ok(Bytes::new())
            })
        });
        let mut r = new_ranged_reader(
            &parent,
            DOWNLOAD_CHUNK_SIZE + 1,
            Box::new(StreamObject::new(head, None)),
            fetch,
        );
        // Leave the first range blocked waiting for the rest of its body.
        body.write_all(b"partial").await.unwrap();
        let _later = ctx_rx.recv().await.unwrap();
        r.close().unwrap();
        assert!(
            r.cancel.is_cancelled(),
            "close did not cancel the later range request"
        );
        // The first range's body is dropped, so writing to it fails.
        let res = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Err(e) = body.write_all(b"more").await {
                    return e;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("close left the first range reader open");
        assert_eq!(res.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn cancel_is_not_eof() {
        let parent = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<()>(DOWNLOAD_CONCURRENCY);
        let head = Box::new(BytesReader::new(vec![0u8; DOWNLOAD_CHUNK_SIZE as usize]));
        let fetch: FetchRange = Arc::new(move |_, _, n| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(()).await;
                Ok(Bytes::from(vec![0u8; n as usize]))
            })
        });
        let size = (DOWNLOAD_CONCURRENCY as i64 + 1) * DOWNLOAD_CHUNK_SIZE;
        let mut r = new_ranged_reader(&parent, size, head, fetch);
        // Every slot holds a fetched range, so the last range waits for one.
        for _ in 0..DOWNLOAD_CONCURRENCY - 1 {
            rx.recv().await.unwrap();
        }
        parent.cancel();
        let mut sink = Vec::new();
        let err = r.read_to_end(&mut sink).await.unwrap_err();
        let err: Error = err.into();
        assert!(
            matches!(err.kind(), crate::error::ErrorKind::Canceled),
            "read {} of {size} bytes, err {err}, want canceled",
            sink.len()
        );
    }

    #[tokio::test]
    async fn reassembles_in_order() {
        let data: Vec<u8> = (0..(2 * DOWNLOAD_CHUNK_SIZE + 5))
            .map(|i| (i % 251) as u8)
            .collect();
        let data = Bytes::from(data);
        let head = Box::new(BytesReader::new(data.slice(..DOWNLOAD_CHUNK_SIZE as usize)));
        let d = data.clone();
        let fetch: FetchRange = Arc::new(move |_, off, n| {
            let d = d.clone();
            Box::pin(async move { Ok(d.slice(off as usize..(off + n) as usize)) })
        });
        let mut r = new_ranged_reader(&CancellationToken::new(), data.len() as i64, head, fetch);
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert!(out == data, "reassembled bytes differ");
    }
}
