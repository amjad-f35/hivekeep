//! `/connect`: sends one HTTP CONNECT — the request a client makes of a
//! forward proxy — and reports the answer.

use std::sync::Arc;

use rustls::ClientConfig;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::address::{split_host_port, validate_target};
use crate::dial::{BoxedIo, DEADLINE_EXCEEDED};
use crate::http1::{read_body, read_head};
use crate::limits::{MAX_CONNECT_ANSWER_BODY, REQUEST_TIMEOUT};
use crate::tls;
use crate::wire::{ConnectRequest, ConnectResponse};

/// Runs the instruction and returns the HTTP status and report. A proxy's
/// refusal is still an answer (200); failing to get an answer is 502.
pub async fn run(input: ConnectRequest) -> (u16, ConnectResponse) {
    if let Err(e) = validate_target(&input.proxy, "proxy") {
        return (400, ConnectResponse::error(e));
    }
    if let Err(e) = validate_target(&input.target, "target") {
        return (400, ConnectResponse::error(e));
    }
    // Trust is settled before dialing, so a bad rootCA opens no connection.
    let tls_config = if input.tls {
        match tls::roots(&input.root_ca) {
            Ok(roots) => Some(tls::client_config(roots, &[])),
            Err(e) => return (400, ConnectResponse::error(e)),
        }
    } else {
        None
    };
    let mut response = ConnectResponse::default();
    let outcome =
        tokio::time::timeout(REQUEST_TIMEOUT, perform(&input, tls_config, &mut response)).await;
    match outcome {
        Ok(Ok(())) => (200, response),
        Ok(Err(e)) => {
            response.error = e;
            (502, response)
        }
        Err(_) => {
            response.error = DEADLINE_EXCEEDED.into();
            (502, response)
        }
    }
}

async fn perform(
    input: &ConnectRequest,
    tls_config: Option<Arc<ClientConfig>>,
    response: &mut ConnectResponse,
) -> Result<(), String> {
    let proxy = &input.proxy;
    let target = &input.target;
    let tcp = TcpStream::connect(proxy)
        .await
        .map_err(|e| format!("dialing proxy {proxy}: {e}"))?;
    let mut conn: BoxedIo = match tls_config {
        None => Box::new(tcp),
        Some(config) => {
            let (host, _) = split_host_port(proxy).unwrap_or_default();
            let stream = tls::handshake(tcp, &host, config)
                .await
                .map_err(|e| format!("TLS handshake with proxy {proxy}: {e}"))?;
            response.tls = true;
            Box::new(stream)
        }
    };

    let request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
    conn.write_all(request.as_bytes())
        .await
        .map_err(|e| format!("sending CONNECT {target}: {e}"))?;
    conn.flush()
        .await
        .map_err(|e| format!("sending CONNECT {target}: {e}"))?;
    let mut leftover = Vec::new();
    let head = read_head(&mut conn, &mut leftover)
        .await
        .map_err(|e| format!("reading the answer to CONNECT {target}: {e}"))?;
    response.status_code = head.status;
    response.protocol = head.protocol.clone();
    // A 2xx makes the connection the tunnel: there is no body, and waiting
    // for one would hang on bytes the proxy never sends.
    let body = read_body(
        &mut conn,
        leftover,
        &head,
        "CONNECT",
        MAX_CONNECT_ANSWER_BODY,
    )
    .await
    .map_err(|e| format!("reading the body of the answer to CONNECT {target}: {e}"))?;
    response.body = String::from_utf8_lossy(&body).into_owned();
    Ok(())
}
