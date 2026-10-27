//! The benchmark's error type.

use std::io;

/// What can go wrong running the benchmark.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A failure described in words.
    #[error("{0}")]
    Msg(String),
    /// An I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// A JSON encoding or decoding failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A TLS configuration failure.
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    /// A certificate or key failure.
    #[error(transparent)]
    Identity(#[from] hivekeep_identity::error::IdentityError),
}

impl Error {
    /// An error from a message.
    pub fn msg(m: impl Into<String>) -> Self {
        Error::Msg(m.into())
    }
}

/// Result with [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
