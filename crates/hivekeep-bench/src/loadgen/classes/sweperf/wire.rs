//! The JSON the in-sandbox trajectory server speaks.

use serde::{Deserialize, Serialize};

/// The `/status` liveness reply; `status` is "up" once the server serves.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StatusResponse {
    /// "up" when ready.
    #[serde(default)]
    pub status: String,
}

/// The `/execute` body: the one-based, inclusive range of steps to replay.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ExecuteRequest {
    /// First step, one-based.
    pub start_step: usize,
    /// Last step, inclusive.
    pub end_step: usize,
}

/// The `/execute` reply. A job ID means the work was accepted and runs in
/// the background; otherwise the run is already over and the exit code,
/// stderr and error describe it.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecuteResponse {
    /// The background job, if any.
    pub job_id: String,
    /// The job's status word.
    pub status: String,
    /// A synchronous run's exit code.
    pub exit_code: i64,
    /// A synchronous run's stderr.
    pub stderr: String,
    /// A synchronous run's error.
    pub error: String,
}

/// The `/status?job_id=` reply. A job still running reports no exit code,
/// which is not the same as 0.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct JobStatusResponse {
    /// The job.
    pub job_id: String,
    /// RUNNING, COMPLETED or FAILED.
    pub status: String,
    /// The exit code once finished.
    pub exit_code: Option<i64>,
    /// This chunk's in-container command time.
    pub execution_duration_ms: f64,
    /// The last step run.
    pub completed_step: i64,
    /// Why the job failed.
    pub error: String,
}
