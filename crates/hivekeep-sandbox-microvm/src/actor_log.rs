//! Forwarding a container's stdout and stderr from the guest to the worker's
//! stdout, one JSON record per line, through the shared Actor log format of
//! [`hivekeep_core::actor_log`]: an Actor's own JSON fields are kept and the
//! Actor's identity labels and container name are added.
//!
//! The reads are repeated unary agent calls over the Actor's agent
//! connection; forwarding ends when teardown closes that connection.

use std::io::Stdout;
use std::sync::LazyLock;
use std::time::Duration;

use hivekeep_core::actor_log::{gce_labels_from_env, ActorLogger};
use hivekeep_core::resource::ActorAttribution;

use crate::kata::agent::AgentClient;

/// The process's Actor record writer. Each record is one write to stdout,
/// so records and the worker's own log lines never interleave mid-line.
static LOGGER: LazyLock<ActorLogger<Stdout>> =
    LazyLock::new(|| ActorLogger::new(std::io::stdout(), gce_labels_from_env()));

/// How much one read asks for.
const READ_LEN: u32 = 32 * 1024;
/// The longest line kept whole; longer ones are split.
const MAX_LINE: usize = 64 * 1024;

fn emit(a: &ActorAttribution, container: &str, line: &[u8]) {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    LOGGER.emit_container_line(line, a, container);
}

/// Forwards one stream of container `stream_id` until the agent stops
/// answering.
pub async fn forward(
    ac: AgentClient,
    a: ActorAttribution,
    stream_id: String,
    container: String,
    stderr: bool,
) {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let data = match ac
            .read_stdio(&stream_id, &stream_id, stderr, READ_LEN)
            .await
        {
            Ok(d) => d,
            Err(_) => break,
        };
        if data.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        buf.extend_from_slice(&data);
        while let Some(i) = buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buf.drain(..=i).collect();
            emit(&a, &container, &line[..line.len() - 1]);
        }
        if buf.len() > MAX_LINE {
            emit(&a, &container, &buf);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        emit(&a, &container, &buf);
    }
}

/// Starts forwarding both streams of a container.
pub fn start(ac: &AgentClient, a: &ActorAttribution, stream_id: &str, container: &str) {
    for stderr in [false, true] {
        tokio::spawn(forward(
            ac.clone(),
            a.clone(),
            stream_id.into(),
            container.into(),
            stderr,
        ));
    }
}
