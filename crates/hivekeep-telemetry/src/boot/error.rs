//! Errors from process boot helpers.

/// A boot step that could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// A required option was left empty.
    #[error("{0} is required")]
    Missing(&'static str),
    /// A log level flag value that is not a level.
    #[error("invalid log level {0:?} (want debug, info, warn, or error)")]
    InvalidLogLevel(String),
    /// A log format value that is not a format.
    #[error("invalid log format {0:?} (want json or text)")]
    InvalidLogFormat(String),
    /// An exporter could not be built.
    #[error("create {what}: {source}")]
    Exporter {
        /// Which exporter.
        what: &'static str,
        /// Why.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The global subscriber was already set.
    #[error("install the global log subscriber: {0}")]
    Subscriber(String),
}
