//! `/post`: a POST whose body is held back after the headers.
//!
//! The headers go out at once and the body follows only after a delay longer
//! than the gate's cached policy decisions live, so a test can check that a
//! connection admitted earlier is still judged correctly when its body
//! finally arrives.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::address::validate_url;
use crate::dial::{self, DEADLINE_EXCEEDED};
use crate::http1::read_response;
use crate::limits::{MAX_RESPONSE_BODY, MUX_BODY_SIZE, REQUEST_TIMEOUT};
use crate::tls;
use crate::wire::{FetchRequest, FetchResponse};

/// The request head for a [`MUX_BODY_SIZE`]-byte upload.
pub fn upload_head(request_uri: &str, host: &str) -> String {
    format!(
        "POST {request_uri} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {MUX_BODY_SIZE}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
    )
}

/// The upload body.
pub fn upload_body() -> Vec<u8> {
    vec![b'a'; MUX_BODY_SIZE]
}

/// Runs the instruction and returns the HTTP status and report.
pub async fn run(input: FetchRequest, body_delay: Duration) -> (u16, FetchResponse) {
    let parts = match validate_url(&input.url) {
        Ok(p) => p,
        Err(e) => return (400, FetchResponse::error(e)),
    };
    // Trust is settled before anything is dialed, so a bad rootCA is the
    // caller's mistake and costs no connection.
    let https = parts.scheme == "https";
    let tls_config = if https {
        match tls::roots(&input.root_ca) {
            Ok(roots) => Some(tls::client_config(roots, &[])),
            Err(e) => return (400, FetchResponse::error(e)),
        }
    } else {
        None
    };
    let address = parts.dial_address(if https { "443" } else { "80" });

    let exchange = async {
        let mut conn = dial::open(&address, tls_config.map(|c| (parts.hostname.as_str(), c)))
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        conn.write_all(upload_head(&parts.request_uri, &parts.host).as_bytes())
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        tokio::time::sleep(body_delay).await;
        conn.write_all(&upload_body())
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        read_response(&mut conn, "POST", MAX_RESPONSE_BODY)
            .await
            .map_err(|e| format!("reading response: {e}"))
    };
    match tokio::time::timeout(REQUEST_TIMEOUT + body_delay, exchange).await {
        Err(_) => (
            502,
            FetchResponse::error(format!("request failed: {DEADLINE_EXCEEDED}")),
        ),
        Ok(Err(e)) => (502, FetchResponse::error(e)),
        Ok(Ok(resp)) => (
            resp.status,
            FetchResponse {
                status_code: resp.status,
                body: String::from_utf8_lossy(&resp.body).into_owned(),
                protocol: resp.protocol,
                tls: https,
                ..Default::default()
            },
        ),
    }
}
