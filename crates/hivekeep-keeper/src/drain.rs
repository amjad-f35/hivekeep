//! Draining a gRPC server on shutdown: readiness off, a delay so load
//! balancers notice, then a graceful stop bounded by a deadline, after
//! which the server's connections are closed by force.
//!
//! A tonic server drives each connection in a task of its own, so stopping
//! the server task leaves accepted connections, and the RPCs on them,
//! running. Serving from [`force_closable`] connections lets the drain cut
//! them once the deadline passes.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::transport::server::Connected;

use crate::context::Ctx;
use crate::http_endpoints::Readiness;

/// How a drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every in-flight RPC finished before the deadline.
    Graceful,
    /// The deadline passed; connections were closed with RPCs running.
    Forced,
}

/// A connection that fails every read and write once its token is
/// cancelled, which makes the server drop it.
pub struct ForceClosable<T> {
    inner: T,
    closed: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<T> ForceClosable<T> {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        match self.closed.as_mut().poll(cx) {
            Poll::Ready(()) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "connection closed by a forced drain",
            )),
            Poll::Pending => Ok(()),
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ForceClosable<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ForceClosable<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<T: Connected> Connected for ForceClosable<T> {
    type ConnectInfo = T::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

/// Wraps each connection `incoming` accepts so cancelling `close` closes
/// it.
pub fn force_closable<S, T, E>(
    incoming: S,
    close: CancellationToken,
) -> impl Stream<Item = Result<ForceClosable<T>, E>>
where
    S: Stream<Item = Result<T, E>>,
{
    incoming.map(move |c| {
        c.map(|inner| ForceClosable {
            inner,
            closed: Box::pin(close.clone().cancelled_owned()),
        })
    })
}

/// Drains `server`, which stops accepting calls and finishes the ones in
/// flight once `shutdown` is cancelled. Readiness turns off first, then
/// after `delay` the graceful stop starts; if it has not finished within
/// `timeout`, `force_close` is cancelled, closing the connections the
/// server accepted through [`force_closable`], and the server is awaited.
pub async fn drain_on_shutdown<T>(
    readiness: &Readiness,
    delay: Duration,
    timeout: Duration,
    shutdown: &Ctx,
    force_close: &CancellationToken,
    mut server: JoinHandle<T>,
) -> DrainOutcome {
    readiness.mark_not_ready();
    tokio::time::sleep(delay).await;
    shutdown.cancel();
    if tokio::time::timeout(timeout, &mut server).await.is_ok() {
        return DrainOutcome::Graceful;
    }
    tracing::warn!("Drain deadline exceeded; forcing stop");
    force_close.cancel();
    let _ = server.await;
    DrainOutcome::Forced
}

#[cfg(test)]
#[path = "drain_tests.rs"]
mod tests;
