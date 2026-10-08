//! Errors of the identity library.

/// Why an identity operation failed.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// Key material could not be generated, parsed or used.
    #[error("key: {0}")]
    Key(String),

    /// A certificate signing request is malformed or its signature is bad.
    #[error("certificate signing request: {0}")]
    Csr(String),

    /// A certificate could not be built, signed or parsed.
    #[error("certificate: {0}")]
    Certificate(String),

    /// A CA or JWT authority pool is unusable (empty, unknown active entry,
    /// malformed state file, ...).
    #[error("pool: {0}")]
    Pool(String),

    /// A request asked for an identity that may not be issued.
    #[error("refused: {0}")]
    Refused(String),

    /// A JWT could not be minted.
    #[error("signing JWT: {0}")]
    Sign(String),

    /// A JWT failed verification.
    #[error("invalid JWT: {0}")]
    InvalidToken(String),

    /// The verification keys of an issuer could not be obtained.
    #[error("verification keys: {0}")]
    Keys(String),

    /// PEM input is malformed.
    #[error("PEM: {0}")]
    Pem(String),

    /// A trust bundle could not be assembled.
    #[error("trust bundle: {0}")]
    TrustBundle(String),

    /// Reading a state file failed.
    #[error("reading {path}: {source}")]
    Io {
        /// The file.
        path: String,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
}

/// Result alias of this crate.
pub type Result<T, E = IdentityError> = std::result::Result<T, E>;
