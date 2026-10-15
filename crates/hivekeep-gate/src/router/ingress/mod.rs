//! Ingress routing: requests name an Actor; the router resumes it (parking
//! the request through transient unavailability) and forwards to the
//! worker it runs on.

pub mod errors;
pub mod handler;
pub mod parking;
pub mod resumer;

pub use handler::{
    Handler, ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_METADATA_KEY, ORIGINAL_DST_PORT_KEY,
    WORKER_SPIFFE_ID_KEY,
};
pub use parking::{
    ParkedRequestConfig, ParkingMetrics, ParkingStatus, DEFAULT_PARKED_REQUEST_BUDGET,
    DEFAULT_PARKED_REQUEST_MAX, DEFAULT_PARKED_REQUEST_RETRY_FACTOR,
    DEFAULT_PARKED_REQUEST_RETRY_INTERVAL, DEFAULT_PARKED_REQUEST_RETRY_JITTER,
};
pub use resumer::{ActorApi, ActorResumer, RequestCtx, ResumeError, ResumeOutcome};
