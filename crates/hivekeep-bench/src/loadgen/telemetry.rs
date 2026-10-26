//! Latency bookkeeping shared by the user classes: where a latency came
//! from, server-elapsed extraction, and the sampled-trace log line.

use std::time::Duration;

use super::trace::SpanContext;

/// The latency is the client's own measurement.
pub const SOURCE_CLIENT: &str = "client";
/// The latency is the server's reported handling time.
pub const SOURCE_SERVER: &str = "server";

/// A duration in fractional milliseconds.
pub fn ms_float(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1e6
}

/// Writes one structured line per sampled span; operators rebuild the
/// trace stream from these lines.
pub fn log_sampled_trace(
    span: &SpanContext,
    name: &str,
    latency: Duration,
    source: &str,
    err: Option<&str>,
) {
    if !span.sampled {
        return;
    }
    let trace_id = span.trace_id_hex();
    let duration_ms = ms_float(latency);
    match err {
        Some(e) => {
            tracing::info!(name, %trace_id, duration_ms, source, err = e, "traced span (failed)")
        }
        None => tracing::info!(name, %trace_id, duration_ms, source, "traced span"),
    }
}

fn parse_micros(v: Option<&str>, fallback: Duration) -> (Duration, &'static str) {
    match v.and_then(|s| s.parse::<i64>().ok()) {
        Some(us) if us >= 0 => (Duration::from_micros(us as u64), SOURCE_SERVER),
        _ => (fallback, SOURCE_CLIENT),
    }
}

/// The server elapsed time under `key` in gRPC response metadata (trailers
/// are merged into it for unary calls), else `fallback` from the client.
pub fn elapsed_from_metadata(
    md: &tonic::metadata::MetadataMap,
    key: &str,
    fallback: Duration,
) -> (Duration, &'static str) {
    parse_micros(md.get(key).and_then(|v| v.to_str().ok()), fallback)
}

/// The server elapsed time under `key` in HTTP response headers, else
/// `fallback` from the client.
pub fn elapsed_from_headers(
    h: &http::HeaderMap,
    key: &str,
    fallback: Duration,
) -> (Duration, &'static str) {
    parse_micros(h.get(key).and_then(|v| v.to_str().ok()), fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::SERVER_ELAPSED_HEADER;

    #[test]
    fn elapsed_prefers_the_server_value() {
        let fallback = Duration::from_millis(9);
        let mut h = http::HeaderMap::new();
        assert_eq!(
            elapsed_from_headers(&h, SERVER_ELAPSED_HEADER, fallback),
            (fallback, SOURCE_CLIENT)
        );
        h.insert(SERVER_ELAPSED_HEADER, "1500".parse().unwrap());
        assert_eq!(
            elapsed_from_headers(&h, SERVER_ELAPSED_HEADER, fallback),
            (Duration::from_micros(1500), SOURCE_SERVER)
        );
        h.insert(SERVER_ELAPSED_HEADER, "soon".parse().unwrap());
        assert_eq!(
            elapsed_from_headers(&h, SERVER_ELAPSED_HEADER, fallback),
            (fallback, SOURCE_CLIENT)
        );

        let mut md = tonic::metadata::MetadataMap::new();
        assert_eq!(
            elapsed_from_metadata(&md, SERVER_ELAPSED_HEADER, fallback).1,
            SOURCE_CLIENT
        );
        md.insert(SERVER_ELAPSED_HEADER, "42".parse().unwrap());
        assert_eq!(
            elapsed_from_metadata(&md, SERVER_ELAPSED_HEADER, fallback),
            (Duration::from_micros(42), SOURCE_SERVER)
        );
        assert_eq!(ms_float(Duration::from_micros(1500)), 1.5);
    }
}
