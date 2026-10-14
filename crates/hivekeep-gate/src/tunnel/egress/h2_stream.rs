//! A CONNECT stream over HTTP/2 as a byte stream.

use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use h2::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Converts an HTTP/2 error to an I/O error, keeping an underlying I/O
/// error when there is one.
pub fn h2_to_io(e: h2::Error) -> io::Error {
    if e.is_io() {
        return e
            .into_io()
            .unwrap_or_else(|| io::Error::other("http2 i/o error"));
    }
    io::Error::other(e)
}

/// One tunneled stream: request body upstream, response body downstream.
/// Shutting down the write side ends the request body, which the gateway
/// sees as the Actor's half-close.
pub struct H2Stream {
    send: SendStream<Bytes>,
    recv: RecvStream,
    pending: Bytes,
    write_closed: bool,
    read_done: bool,
    /// Keeps the connection alive in the pool's accounting while open.
    _guard: Option<Box<dyn Send + Sync>>,
}

impl H2Stream {
    /// Wraps an accepted CONNECT exchange.
    pub fn new(
        send: SendStream<Bytes>,
        recv: RecvStream,
        guard: Option<Box<dyn Send + Sync>>,
    ) -> Self {
        H2Stream {
            send,
            recv,
            pending: Bytes::new(),
            write_closed: false,
            read_done: false,
            _guard: guard,
        }
    }
}

impl AsyncRead for H2Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.pending.is_empty() {
                let n = self.pending.len().min(buf.remaining());
                let chunk = self.pending.split_to(n);
                buf.put_slice(&chunk);
                return Poll::Ready(Ok(()));
            }
            if self.read_done {
                return Poll::Ready(Ok(()));
            }
            match ready!(self.recv.poll_data(cx)) {
                Some(Ok(data)) => {
                    let _ = self.recv.flow_control().release_capacity(data.len());
                    self.pending = data;
                }
                Some(Err(e)) => {
                    if e.reason() == Some(h2::Reason::NO_ERROR) {
                        self.read_done = true;
                        continue;
                    }
                    return Poll::Ready(Err(h2_to_io(e)));
                }
                None => {
                    self.read_done = true;
                }
            }
        }
    }
}

impl AsyncWrite for H2Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream write side closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.send.reserve_capacity(buf.len());
        let n = match ready!(self.send.poll_capacity(cx)) {
            Some(Ok(n)) => n.min(buf.len()),
            Some(Err(e)) => return Poll::Ready(Err(h2_to_io(e))),
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream closed",
                )))
            }
        };
        if n == 0 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.send
            .send_data(Bytes::copy_from_slice(&buf[..n]), false)
            .map_err(h2_to_io)?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.write_closed {
            self.write_closed = true;
            self.send.send_data(Bytes::new(), true).map_err(h2_to_io)?;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for H2Stream {
    fn drop(&mut self) {
        if !self.write_closed {
            // Ends the request body; a stream the gateway has not finished
            // is then reset when the response side is dropped.
            let _ = self.send.send_data(Bytes::new(), true);
        }
        if !self.read_done {
            self.send.send_reset(h2::Reason::CANCEL);
        }
    }
}
