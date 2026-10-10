//! Which sources the relay carries.

use std::num::NonZeroUsize;

use lru::LruCache;
use opentelemetry_proto::tonic::resource::v1::Resource;
use parking_lot::Mutex;
use tonic::Status;

use crate::attrs::SERVICE_NAME_KEY;

/// The only sources the relay carries, keyed by the `service.name` their
/// resource declares: the sandbox worker binaries.
///
/// This is a protocol contract, not a security boundary: `service.name` is
/// client-provided, so a compromised process could claim a worker name. It
/// prevents accidental misuse (an Actor SDK pointed at the socket) and keeps
/// the verbatim pass-through explicit, so telemetry whose identity must be
/// rewritten gets its own path rather than this one.
pub const WORKER_SERVICES: &[&str] = &["hivekeep-sandbox-gvisor", "hivekeep-sandbox-microvm"];

/// Bounds the set of rejected names remembered for logging.
const LOGGED_CAPACITY: usize = 256;

/// Applies [`WORKER_SERVICES`] and logs the first rejection of each name.
///
/// The log matters because of how the failure presents: `OTEL_SERVICE_NAME`
/// or `OTEL_RESOURCE_ATTRIBUTES` on a worker pod overrides the worker's own
/// name, the relay then refuses every export with `PermissionDenied`, which
/// SDKs do not retry, and the worker's telemetry silently stops. One line
/// per distinct name makes it findable; the bounded cache keeps arbitrary
/// client-provided names from growing memory while still suppressing an
/// exporter that retries for the pod's whole life.
pub struct SourceGate {
    logged: Mutex<LruCache<String, ()>>,
}

impl Default for SourceGate {
    fn default() -> Self {
        SourceGate {
            logged: Mutex::new(LruCache::new(
                NonZeroUsize::new(LOGGED_CAPACITY).expect("nonzero"),
            )),
        }
    }
}

/// The `service.name` a resource declares, or empty.
pub fn resource_service_name(r: Option<&Resource>) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    r.into_iter()
        .flat_map(|r| &r.attributes)
        .find(|kv| kv.key == SERVICE_NAME_KEY)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

impl SourceGate {
    /// Accepts a worker resource; rejects anything else, a missing name
    /// included: an unidentified source is the one the relay cannot vouch
    /// for.
    pub fn check(&self, r: Option<&Resource>) -> Result<(), Status> {
        let name = resource_service_name(r);
        if WORKER_SERVICES.contains(&name.as_str()) {
            return Ok(());
        }
        let first = self.logged.lock().put(name.clone(), ()).is_none();
        if first {
            tracing::warn!(
                service.name = %name,
                allowed = ?WORKER_SERVICES,
                note = "logged once per distinct service.name",
                "OTLP relay rejected telemetry from an unrecognized source; it is being dropped, not retried. If this is a worker, check whether OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES on the worker pod is overriding its service.name"
            );
        }
        Err(Status::permission_denied(format!(
            "the OTLP relay carries worker telemetry only, got service.name {name:?}; a source whose identity has to be rewritten must not be forwarded verbatim. If this is a worker, an OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES override on the worker pod would produce exactly this"
        )))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};

    pub(crate) fn service_resource(name: &str) -> Option<Resource> {
        if name.is_empty() {
            return Some(Resource::default());
        }
        Some(Resource {
            attributes: vec![KeyValue {
                key: SERVICE_NAME_KEY.into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(name.into())),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    #[test]
    fn source_gate_logs_each_rejection_once() {
        let gate = SourceGate::default();
        let out = capture(|| {
            for _ in 0..3 {
                assert_eq!(
                    gate.check(service_resource("actor").as_ref())
                        .unwrap_err()
                        .code(),
                    tonic::Code::PermissionDenied
                );
            }
            assert!(gate.check(service_resource("keeper").as_ref()).is_err());
            assert!(gate
                .check(service_resource("hivekeep-sandbox-gvisor").as_ref())
                .is_ok());
        });
        assert_eq!(out.matches("rejected telemetry").count(), 2, "{out}");
        assert!(out.contains("OTEL_SERVICE_NAME"), "{out}");
    }

    #[test]
    fn worker_services_match_the_worker_binaries() {
        // Each sandbox worker crate's binary name is the service.name it
        // reports.
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut found = Vec::new();
        for dir in ["hivekeep-sandbox-gvisor", "hivekeep-sandbox-microvm"] {
            let manifest = std::fs::read_to_string(crates.join(dir).join("Cargo.toml")).unwrap();
            let bin = manifest
                .split("[[bin]]")
                .nth(1)
                .and_then(|s| s.lines().find(|l| l.trim_start().starts_with("name")))
                .and_then(|l| l.split('"').nth(1))
                .unwrap_or_else(|| panic!("no [[bin]] name in {dir}/Cargo.toml"))
                .to_string();
            assert!(
                WORKER_SERVICES.contains(&bin.as_str()),
                "{dir} reports {bin:?}, which the relay would refuse"
            );
            found.push(bin);
        }
        for name in WORKER_SERVICES {
            assert!(
                found.iter().any(|f| f == name),
                "{name} is allowed but no worker binary declares it"
            );
        }
    }
}
