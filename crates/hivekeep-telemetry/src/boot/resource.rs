//! The resource shared by the tracer, meter and logger providers.

use std::sync::LazyLock;

use opentelemetry::KeyValue;
use opentelemetry_sdk::resource::{Resource, TelemetryResourceDetector};

use crate::attrs::{OTLP_RELAY_KEY, SERVICE_INSTANCE_ID_KEY, SERVICE_NAME_KEY};
use crate::env::Env;

/// Overrides `service.name`.
pub const SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
/// Extra resource attributes, `key=value` pairs separated by commas.
pub const RESOURCE_ATTRIBUTES_ENV: &str = "OTEL_RESOURCE_ATTRIBUTES";

/// Generated once so every provider's resource carries the same ID.
static SERVICE_INSTANCE_ID: LazyLock<String> = LazyLock::new(|| uuid::Uuid::new_v4().to_string());

/// This process's `service.instance.id`.
pub fn service_instance_id() -> &'static str {
    &SERVICE_INSTANCE_ID
}

/// Builds the resource for `service_name`. The `OTEL_*` variables are
/// applied last, so they override the defaults: an operator can rename a
/// component or pin its instance ID without a code change.
pub fn new_resource(env: &dyn Env, service_name: &str, extra: Vec<KeyValue>) -> Resource {
    let mut attrs = vec![
        KeyValue::new(SERVICE_NAME_KEY, service_name.to_string()),
        KeyValue::new(SERVICE_INSTANCE_ID_KEY, service_instance_id().to_string()),
    ];
    attrs.extend(extra);
    let mut overrides = parse_resource_attributes(&env.get(RESOURCE_ATTRIBUTES_ENV));
    let name = env.trimmed(SERVICE_NAME_ENV);
    if !name.is_empty() {
        overrides.push(KeyValue::new(SERVICE_NAME_KEY, name));
    }
    Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_schema_url(attrs, opentelemetry_semantic_conventions::SCHEMA_URL)
        .with_attributes(overrides)
        .build()
}

/// Parses `OTEL_RESOURCE_ATTRIBUTES`: comma-separated `key=value` pairs with
/// percent-encoded values. Malformed entries are skipped.
pub fn parse_resource_attributes(raw: &str) -> Vec<KeyValue> {
    raw.split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let k = k.trim();
            if k.is_empty() {
                return None;
            }
            let v = percent_encoding::percent_decode_str(v.trim())
                .decode_utf8()
                .ok()?;
            Some(KeyValue::new(k.to_string(), v.into_owned()))
        })
        .collect()
}

/// The export-path attribute of a component that could have used the
/// relay. `relay_capable` false means the component never had one and gets
/// no attribute at all; true means it did, and `has_relay` says whether it
/// got it. A relay-capable component without its relay is the degraded case
/// worth alerting on, so it is labeled `direct` rather than left unlabeled.
pub fn relay_attrs(relay_capable: bool, has_relay: bool) -> Vec<KeyValue> {
    if !relay_capable {
        return Vec::new();
    }
    vec![KeyValue::new(
        OTLP_RELAY_KEY,
        if has_relay { "relay" } else { "direct" },
    )]
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::env::MapEnv;
    use std::collections::HashMap;

    pub(crate) fn resource_attrs(res: &Resource) -> HashMap<String, String> {
        res.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn new_resource_defaults() {
        let res = new_resource(&MapEnv::default(), "hivekeep-api", vec![]);
        let attrs = resource_attrs(&res);
        assert_eq!(attrs[SERVICE_NAME_KEY], "hivekeep-api");
        assert!(!attrs[SERVICE_INSTANCE_ID_KEY].is_empty());
        assert!(attrs.contains_key("telemetry.sdk.name"));
        assert_eq!(
            res.schema_url(),
            Some(opentelemetry_semantic_conventions::SCHEMA_URL)
        );
    }

    #[test]
    fn new_resource_env_wins() {
        let env = MapEnv::new([
            (SERVICE_NAME_ENV, "from-env"),
            (
                RESOURCE_ATTRIBUTES_ENV,
                "service.instance.id=fixed-id,team=a%20b",
            ),
        ]);
        let attrs = resource_attrs(&new_resource(&env, "hivekeep-api", vec![]));
        assert_eq!(attrs[SERVICE_NAME_KEY], "from-env");
        assert_eq!(attrs[SERVICE_INSTANCE_ID_KEY], "fixed-id");
        assert_eq!(attrs["team"], "a b");
    }

    #[test]
    fn relay_attrs_cases() {
        for (name, capable, conn, want) in [
            ("relay capable with conn", true, true, Some("relay")),
            ("relay capable fell back", true, false, Some("direct")),
            ("not relay capable", false, false, None),
            // Capability, not the connection, is the gate.
            ("not relay capable with conn", false, true, None),
        ] {
            let res = new_resource(
                &MapEnv::default(),
                "hivekeep-sandbox-gvisor",
                relay_attrs(capable, conn),
            );
            let got = resource_attrs(&res).get(OTLP_RELAY_KEY).cloned();
            assert_eq!(got.as_deref(), want, "{name}");
        }
    }
}
