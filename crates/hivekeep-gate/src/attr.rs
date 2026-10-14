//! Label names and values shared by the gate's metrics and logs.

/// Metric label: the Actor template's hive.
pub const TEMPLATE_HIVE: &str = "template_hive";
/// Metric label: the Actor template's name.
pub const TEMPLATE_NAME: &str = "template_name";
/// Metric label: how routing a request ended.
pub const ROUTER_OUTCOME: &str = "outcome";
/// Metric label: whether routing resumed the Actor.
pub const ROUTER_RESUME: &str = "resume";

/// Template dimension used when the template is not known.
pub const TEMPLATE_UNKNOWN: &str = "unknown";

/// Normalizes an empty template dimension to [`TEMPLATE_UNKNOWN`].
pub fn normalize_template_dimension(dim: &str) -> &str {
    if dim.is_empty() {
        TEMPLATE_UNKNOWN
    } else {
        dim
    }
}

/// Values of [`ROUTER_RESUME`].
pub mod resume {
    /// The Actor was already running.
    pub const NONE: &str = "none";
    /// This request started a resume.
    pub const TRIGGERED: &str = "triggered";
    /// This request joined a resume already in flight.
    pub const JOINED: &str = "joined";
    /// Unknown.
    pub const UNKNOWN: &str = "unknown";
}

/// Values of [`ROUTER_OUTCOME`].
pub mod outcome {
    /// Routed.
    pub const OK: &str = "ok";
    /// The client went away.
    pub const CANCELLED: &str = "cancelled";
    /// A deadline passed.
    pub const TIMEOUT: &str = "timeout";
    /// No worker had room.
    pub const NO_CAPACITY: &str = "no_capacity";
    /// The Actor was in a state that cannot serve yet.
    pub const FAILED_PRECONDITION: &str = "failed_precondition";
    /// A concurrent operation held the Actor.
    pub const LOCK_CONFLICT: &str = "lock_conflict";
    /// The Actor does not exist.
    pub const NOT_FOUND: &str = "not_found";
    /// The control plane (or the router) was unavailable.
    pub const UNAVAILABLE: &str = "unavailable";
    /// Rate limited.
    pub const RATE_LIMITED: &str = "rate_limited";
    /// Refused by policy or authorization.
    pub const DENIED: &str = "denied";
    /// Resuming failed otherwise.
    pub const RESUME_ERROR: &str = "resume_error";
}
