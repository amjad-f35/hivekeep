//! `/slow-headers`: a plain-HTTP GET whose header block is finished only
//! after a pause, to exercise how the egress path treats a request that
//! trickles in.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::address::validate_url;
use crate::dial::{self, DEADLINE_EXCEEDED};
use crate::http1::read_response;
use crate::limits::{MAX_RESPONSE_BODY, REQUEST_TIMEOUT};
use crate::wire::{FetchRequest, FetchResponse};

/// Runs the instruction and returns the HTTP status and report.
pub async fn run(input: FetchRequest, header_delay: Duration) -> (u16, FetchResponse) {
    let parts = match validate_url(&input.url) {
        Ok(p) => p,
        Err(e) => return (400, FetchResponse::error(e)),
    };
    if parts.scheme != "http" {
        return (400, FetchResponse::error("URL scheme must be http"));
    }
    let address = parts.dial_address("80");

    let exchange = async {
        let mut conn = dial::open(&address, None)
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        // Everything but the blank line that ends the header block.
        let head = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            parts.request_uri, parts.host
        );
        conn.write_all(head.as_bytes())
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        tokio::time::sleep(header_delay).await;
        conn.write_all(b"\r\n")
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        read_response(&mut conn, "GET", MAX_RESPONSE_BODY)
            .await
            .map_err(|e| format!("reading response: {e}"))
    };
    match tokio::time::timeout(REQUEST_TIMEOUT + header_delay, exchange).await {
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
                ..Default::default()
            },
        ),
    }
}
