//! Request tracing for the load generator: trace and span IDs, a sampler
//! whose probability can change at runtime, and W3C `traceparent`
//! propagation onto outgoing gRPC and HTTP requests.
//!
//! Spans are not exported anywhere; a sampled span's trace ID is written to
//! the log (see [`super::telemetry::log_sampled_trace`]) and carried to the
//! services it calls, whose own traces join on it.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use rand::RngCore;

/// The W3C trace-context header.
pub const TRACEPARENT: &str = "traceparent";

/// A trace ID / span ID pair and whether it is sampled.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanContext {
    /// 16-byte trace ID.
    pub trace_id: [u8; 16],
    /// 8-byte span ID.
    pub span_id: [u8; 8],
    /// Whether the trace is recorded.
    pub sampled: bool,
}

impl fmt::Debug for SpanContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.traceparent())
    }
}

impl SpanContext {
    /// The trace ID as 32 lowercase hex digits.
    pub fn trace_id_hex(&self) -> String {
        hex::encode(self.trace_id)
    }

    /// The `traceparent` header value.
    pub fn traceparent(&self) -> String {
        format!(
            "00-{}-{}-{}",
            hex::encode(self.trace_id),
            hex::encode(self.span_id),
            if self.sampled { "01" } else { "00" }
        )
    }

    /// Parses a `traceparent` value; `None` when malformed or all-zero.
    pub fn parse(value: &str) -> Option<SpanContext> {
        let parts: Vec<&str> = value.trim().split('-').collect();
        if parts.len() < 4 || parts[0].len() != 2 || parts[0] == "ff" {
            return None;
        }
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        hex::decode_to_slice(parts[1], &mut trace_id).ok()?;
        hex::decode_to_slice(parts[2], &mut span_id).ok()?;
        let flags = u8::from_str_radix(parts[3], 16).ok()?;
        if trace_id == [0; 16] || span_id == [0; 8] {
            return None;
        }
        Some(SpanContext {
            trace_id,
            span_id,
            sampled: flags & 1 == 1,
        })
    }
}

/// A parent-based, trace-ID-ratio sampler whose probability can be swapped
/// at runtime: a span with a parent follows the parent's decision; a root
/// span is sampled when the low 63 bits of its trace ID fall below
/// `probability * 2^63`.
#[derive(Debug)]
pub struct Sampler {
    probability: AtomicU64,
}

impl Sampler {
    /// A sampler at `probability` (clamped to `[0, 1]`).
    pub fn new(probability: f64) -> Self {
        let s = Sampler {
            probability: AtomicU64::new(0),
        };
        s.update_probability(probability);
        s
    }

    /// Changes the probability for spans started from now on.
    pub fn update_probability(&self, p: f64) {
        let p = if p.is_nan() { 0.0 } else { p.clamp(0.0, 1.0) };
        self.probability.store(p.to_bits(), Ordering::SeqCst);
    }

    /// The current probability.
    pub fn probability(&self) -> f64 {
        f64::from_bits(self.probability.load(Ordering::SeqCst))
    }

    /// Whether a span in `trace_id` with `parent` should be recorded.
    pub fn should_sample(&self, parent: Option<&SpanContext>, trace_id: &[u8; 16]) -> bool {
        if let Some(p) = parent {
            return p.sampled;
        }
        let p = self.probability();
        if p >= 1.0 {
            return true;
        }
        if p <= 0.0 {
            return false;
        }
        let mut low = [0u8; 8];
        low.copy_from_slice(&trace_id[8..]);
        let x = u64::from_be_bytes(low) >> 1;
        let bound = (p * (1u64 << 63) as f64) as u64;
        x < bound
    }
}

/// Starts spans under one sampler.
#[derive(Debug, Clone)]
pub struct Tracer {
    name: Arc<str>,
    sampler: Arc<Sampler>,
}

impl Tracer {
    /// A tracer named `name` over `sampler`.
    pub fn new(name: &str, sampler: Arc<Sampler>) -> Self {
        Tracer {
            name: name.into(),
            sampler,
        }
    }

    /// A tracer that never samples.
    pub fn never(name: &str) -> Self {
        Self::new(name, Arc::new(Sampler::new(0.0)))
    }

    /// The tracer's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The tracer's sampler.
    pub fn sampler(&self) -> &Arc<Sampler> {
        &self.sampler
    }

    /// Starts a root span.
    pub fn start(&self, name: &str) -> Span {
        self.start_with_parent(name, None)
    }

    /// Starts a span under `parent`, or a root span.
    pub fn start_with_parent(&self, name: &str, parent: Option<&SpanContext>) -> Span {
        let mut rng = rand::rng();
        let trace_id = match parent {
            Some(p) => p.trace_id,
            None => loop {
                let mut id = [0u8; 16];
                rng.fill_bytes(&mut id);
                if id != [0; 16] {
                    break id;
                }
            },
        };
        let span_id = loop {
            let mut id = [0u8; 8];
            rng.fill_bytes(&mut id);
            if id != [0; 8] {
                break id;
            }
        };
        let sampled = self.sampler.should_sample(parent, &trace_id);
        Span {
            name: name.to_string(),
            context: SpanContext {
                trace_id,
                span_id,
                sampled,
            },
            start: Instant::now(),
            attributes: Vec::new(),
            error: None,
        }
    }
}

/// One timed operation.
#[derive(Debug, Clone)]
pub struct Span {
    /// The operation's name.
    pub name: String,
    /// IDs and sampling decision.
    pub context: SpanContext,
    /// When the span started.
    pub start: Instant,
    /// Key/value attributes recorded on the span.
    pub attributes: Vec<(String, String)>,
    /// The error the operation ended with, if any.
    pub error: Option<String>,
}

impl Span {
    /// Records an attribute.
    pub fn set_attribute(&mut self, key: &str, value: impl fmt::Display) {
        self.attributes.push((key.to_string(), value.to_string()));
    }

    /// Records that the operation failed.
    pub fn record_error(&mut self, err: impl fmt::Display) {
        self.error = Some(err.to_string());
    }

    /// Whether the span is sampled.
    pub fn is_sampled(&self) -> bool {
        self.context.sampled
    }

    /// The span's context, for propagation.
    pub fn context(&self) -> &SpanContext {
        &self.context
    }

    /// Ends the span; a sampled span is logged at debug level.
    pub fn end(self) {
        if self.context.sampled {
            tracing::debug!(
                span = %self.name,
                trace_id = %self.context.trace_id_hex(),
                duration_ms = self.start.elapsed().as_secs_f64() * 1e3,
                attributes = ?self.attributes,
                error = self.error.as_deref().unwrap_or(""),
                "span ended"
            );
        }
    }
}

/// Puts `ctx` on a gRPC request's metadata.
pub fn inject_metadata(ctx: &SpanContext, md: &mut tonic::metadata::MetadataMap) {
    if let Ok(v) = ctx.traceparent().parse() {
        md.insert(TRACEPARENT, v);
    }
}

/// Puts `ctx` on an HTTP request's headers.
pub fn inject_headers(ctx: &SpanContext, headers: &mut http::HeaderMap) {
    if let Ok(v) = http::HeaderValue::from_str(&ctx.traceparent()) {
        headers.insert(TRACEPARENT, v);
    }
}

/// Reads a span context from HTTP headers.
pub fn extract_headers(headers: &http::HeaderMap) -> Option<SpanContext> {
    headers
        .get(TRACEPARENT)?
        .to_str()
        .ok()
        .and_then(SpanContext::parse)
}

static GLOBAL: OnceLock<Tracer> = OnceLock::new();

/// Installs the process-wide tracer (first call wins) and returns it.
pub fn init(service_name: &str, sampler: Arc<Sampler>) -> Result<Tracer, String> {
    if service_name.is_empty() {
        return Err("service name is required".into());
    }
    Ok(GLOBAL
        .get_or_init(|| Tracer::new(service_name, sampler))
        .clone())
}

/// The process-wide tracer, or a never-sampling one before [`init`].
pub fn global() -> Tracer {
    GLOBAL
        .get()
        .cloned()
        .unwrap_or_else(|| Tracer::never("hivekeep-loadgen"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_requires_a_service_name() {
        assert!(init("", Arc::new(Sampler::new(1.0))).is_err());
    }

    #[test]
    fn traceparent_round_trips() {
        let t = Tracer::new("t", Arc::new(Sampler::new(1.0)));
        let span = t.start("op");
        let tp = span.context.traceparent();
        assert_eq!(tp.len(), 55);
        assert!(tp.ends_with("-01"));
        assert_eq!(SpanContext::parse(&tp), Some(span.context));
        assert_eq!(SpanContext::parse("garbage"), None);
        assert_eq!(
            SpanContext::parse("00-00000000000000000000000000000000-0000000000000001-01"),
            None
        );
    }

    #[test]
    fn sampler_follows_probability_and_parent() {
        let s = Sampler::new(0.0);
        let t = Tracer::new("t", Arc::new(Sampler::new(0.0)));
        assert!(!t.start("x").is_sampled());
        assert!(!s.should_sample(None, &[0xff; 16]));
        s.update_probability(1.0);
        assert!(s.should_sample(None, &[0xff; 16]));
        s.update_probability(0.5);
        let mut low = [0u8; 16];
        low[8] = 0x10;
        assert!(s.should_sample(None, &low));
        assert!(!s.should_sample(None, &[0xff; 16]));
        let parent = SpanContext {
            trace_id: [1; 16],
            span_id: [1; 8],
            sampled: true,
        };
        s.update_probability(0.0);
        assert!(
            s.should_sample(Some(&parent), &parent.trace_id),
            "parent decision wins"
        );
        let child =
            Tracer::new("t", Arc::new(Sampler::new(0.0))).start_with_parent("c", Some(&parent));
        assert_eq!(child.context.trace_id, parent.trace_id);
        assert!(child.is_sampled());
        s.update_probability(7.0);
        assert_eq!(s.probability(), 1.0, "clamped");
    }

    #[test]
    fn ratio_is_roughly_honoured() {
        let t = Tracer::new("t", Arc::new(Sampler::new(0.25)));
        let n = (0..4000).filter(|_| t.start("x").is_sampled()).count();
        assert!((700..1300).contains(&n), "sampled {n} of 4000 at 0.25");
    }

    #[test]
    fn injects_into_metadata_and_headers() {
        let t = Tracer::new("t", Arc::new(Sampler::new(1.0)));
        let span = t.start("x");
        let mut md = tonic::metadata::MetadataMap::new();
        inject_metadata(span.context(), &mut md);
        assert_eq!(
            md.get(TRACEPARENT).unwrap().to_str().unwrap(),
            span.context.traceparent()
        );
        let mut h = http::HeaderMap::new();
        inject_headers(span.context(), &mut h);
        assert_eq!(extract_headers(&h), Some(span.context));
    }
}
