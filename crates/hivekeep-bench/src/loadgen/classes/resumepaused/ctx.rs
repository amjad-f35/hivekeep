//! A call's lifetime: a cancellation token and an optional deadline, the
//! two ways a load-class call can be cut short.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Status;

/// Why a call was cut short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// The token fired.
    Canceled,
    /// The deadline passed.
    DeadlineExceeded,
}

impl Cut {
    /// The gRPC status a cut call reports.
    pub fn status(self) -> Status {
        match self {
            Cut::Canceled => Status::cancelled("context canceled"),
            Cut::DeadlineExceeded => Status::deadline_exceeded("context deadline exceeded"),
        }
    }
}

/// A cancellation token plus an optional deadline.
#[derive(Debug, Clone, Default)]
pub struct Ctx {
    cancel: CancellationToken,
    deadline: Option<Instant>,
}

impl Ctx {
    /// A context ended only by `cancel`.
    pub fn new(cancel: CancellationToken) -> Self {
        Ctx {
            cancel,
            deadline: None,
        }
    }

    /// A context that never ends.
    pub fn background() -> Self {
        Self::default()
    }

    /// This context, also ending `d` from now.
    pub fn with_timeout(&self, d: Duration) -> Self {
        let at = Instant::now() + d;
        Ctx {
            cancel: self.cancel.clone(),
            deadline: Some(self.deadline.map_or(at, |old| old.min(at))),
        }
    }

    /// Resolves when the context ends.
    pub async fn done(&self) -> Cut {
        match self.deadline {
            Some(d) => tokio::select! {
                _ = self.cancel.cancelled() => Cut::Canceled,
                _ = tokio::time::sleep_until(d) => Cut::DeadlineExceeded,
            },
            None => {
                self.cancel.cancelled().await;
                Cut::Canceled
            }
        }
    }

    /// Runs `fut` unless the context ends first.
    pub async fn run<T>(&self, fut: impl Future<Output = T>) -> Result<T, Cut> {
        tokio::select! {
            biased;
            c = self.done() => Err(c),
            v = fut => Ok(v),
        }
    }

    /// Sleeps `d` unless the context ends first.
    pub async fn sleep(&self, d: Duration) -> Result<(), Cut> {
        self.run(tokio::time::sleep(d)).await
    }
}
