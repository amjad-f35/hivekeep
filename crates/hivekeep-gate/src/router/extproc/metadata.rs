//! The request as the external processor sees it: headers (lowercased,
//! repeated values comma-joined) and the attributes Envoy attached.

use std::collections::HashMap;

use envoy_types::pb::envoy::config::core::v3::HeaderValue;
use envoy_types::pb::google::protobuf::{value::Kind, Struct};

use super::attributes::AUTHORITY_HEADER;

/// Headers and attributes of one request.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestMetadata {
    /// Header values by lowercase name.
    pub headers: HashMap<String, String>,
    /// `:path`.
    pub path: String,
    /// The last of `:authority` and `host`.
    pub host: String,
    /// `:method`.
    pub method: String,
    /// Attributes by filter name.
    pub attributes: HashMap<String, Struct>,
}

impl RequestMetadata {
    /// Collects `headers` (preferring `value`, falling back to `raw_value`).
    pub fn new(headers: &[HeaderValue], attributes: HashMap<String, Struct>) -> Self {
        let mut md = RequestMetadata {
            attributes,
            ..Default::default()
        };
        for h in headers {
            let k = h.key.to_ascii_lowercase();
            let val = if h.value.is_empty() && !h.raw_value.is_empty() {
                String::from_utf8_lossy(&h.raw_value).into_owned()
            } else {
                h.value.clone()
            };
            match md.headers.get_mut(&k) {
                Some(existing) => {
                    existing.push(',');
                    existing.push_str(&val);
                }
                None => {
                    md.headers.insert(k.clone(), val.clone());
                }
            }
            match k.as_str() {
                ":path" => md.path = val,
                ":method" => md.method = val,
                k if k == AUTHORITY_HEADER || k == "host" => md.host = val,
                _ => {}
            }
        }
        md
    }

    /// A header's value (case-insensitive), or "".
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }

    /// Sets a header.
    pub fn set(&mut self, name: &str, value: impl Into<String>) {
        self.headers.insert(name.to_ascii_lowercase(), value.into());
    }

    /// An attribute's value from any filter: strings as is, numbers in
    /// shortest decimal form, "" when absent.
    pub fn attribute(&self, name: &str) -> String {
        for attrs in self.attributes.values() {
            if let Some(v) = attrs.fields.get(name) {
                return match &v.kind {
                    Some(Kind::NumberValue(n)) => format_number(*n),
                    Some(Kind::StringValue(s)) => s.clone(),
                    _ => String::new(),
                };
            }
        }
        String::new()
    }
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// A string attribute value.
pub fn string_value(s: impl Into<String>) -> envoy_types::pb::google::protobuf::Value {
    envoy_types::pb::google::protobuf::Value {
        kind: Some(Kind::StringValue(s.into())),
    }
}

/// A number attribute value.
pub fn number_value(n: f64) -> envoy_types::pb::google::protobuf::Value {
    envoy_types::pb::google::protobuf::Value {
        kind: Some(Kind::NumberValue(n)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::headers::TARGET_ACTOR_HEADER;
    use crate::router::extproc::attributes::TARGET_ACTOR_FILTER_STATE_ATTRIBUTE;

    fn hv(k: &str, v: &str) -> HeaderValue {
        HeaderValue {
            key: k.into(),
            value: v.into(),
            ..Default::default()
        }
    }

    fn map(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn extract_metadata() {
        let cases = vec![
            (
                "basic path and authority",
                vec![
                    hv(":path", "/api/v1/test"),
                    hv(":authority", "example.com"),
                    hv("X-Request-ID", "req-123"),
                ],
                map(&[
                    (":path", "/api/v1/test"),
                    (":authority", "example.com"),
                    ("x-request-id", "req-123"),
                ]),
                "/api/v1/test",
                "example.com",
            ),
            (
                "host header overrides empty or authority",
                vec![
                    hv(":path", "/api/v1/test"),
                    hv(":authority", "authority.com"),
                    hv("Host", "host.com"),
                ],
                map(&[
                    (":path", "/api/v1/test"),
                    (":authority", "authority.com"),
                    ("host", "host.com"),
                ]),
                "/api/v1/test",
                "host.com",
            ),
            (
                "authority header overrides host when it comes after",
                vec![
                    hv(":path", "/api/v1/test"),
                    hv("Host", "host.com"),
                    hv(":authority", "authority.com"),
                ],
                map(&[
                    (":path", "/api/v1/test"),
                    ("host", "host.com"),
                    (":authority", "authority.com"),
                ]),
                "/api/v1/test",
                "authority.com",
            ),
            (
                "no authority or host headers",
                vec![
                    hv(":path", "/api/v1/test"),
                    hv("x-something-else", "custom-value"),
                ],
                map(&[
                    (":path", "/api/v1/test"),
                    ("x-something-else", "custom-value"),
                ]),
                "/api/v1/test",
                "",
            ),
            (
                "headers are lowercased",
                vec![
                    hv("UPPER-KEY", "UPPER-VALUE"),
                    hv("camelCaseKey", "camelValue"),
                ],
                map(&[("upper-key", "UPPER-VALUE"), ("camelcasekey", "camelValue")]),
                "",
                "",
            ),
            (
                "repeated headers are joined with commas",
                vec![hv("X-Repeated", "first"), hv("x-repeated", "second")],
                map(&[("x-repeated", "first,second")]),
                "",
                "",
            ),
        ];
        for (name, headers, want, path, host) in cases {
            let md = RequestMetadata::new(&headers, HashMap::new());
            assert_eq!(md.headers, want, "{name}");
            assert_eq!(md.path, path, "{name}");
            assert_eq!(md.host, host, "{name}");
        }
    }

    #[test]
    fn header_comma_joins_duplicates() {
        let md = RequestMetadata::new(
            &[
                hv("X-Single", "single-val"),
                hv("X-Multiple", "val-1"),
                hv("X-Multiple", "val-2"),
            ],
            HashMap::new(),
        );
        assert_eq!(md.header("X-Single"), "single-val");
        assert_eq!(md.header("X-Multiple"), "val-1,val-2");
        assert_eq!(md.header("X-Missing"), "");
        assert_eq!(RequestMetadata::default().header("X-Single"), "");
    }

    fn attrs(k: &str, v: envoy_types::pb::google::protobuf::Value) -> HashMap<String, Struct> {
        [(
            "envoy.filters.http.ext_proc".to_string(),
            Struct {
                fields: [(k.to_string(), v)].into(),
            },
        )]
        .into()
    }

    #[test]
    fn attribute() {
        let md = RequestMetadata::new(
            &[],
            attrs(
                TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
                string_value("team-a/actor-1"),
            ),
        );
        assert_eq!(
            md.attribute(TARGET_ACTOR_FILTER_STATE_ATTRIBUTE),
            "team-a/actor-1"
        );
        assert_eq!(md.attribute("filter_state['does.not.exist']"), "");
    }

    #[test]
    fn header_is_case_insensitive() {
        let md = RequestMetadata::new(
            &[hv(&TARGET_ACTOR_HEADER.to_uppercase(), "team-a/actor-1")],
            HashMap::new(),
        );
        assert_eq!(md.header(TARGET_ACTOR_HEADER), "team-a/actor-1");
    }

    #[test]
    fn attribute_number() {
        let port = "filter_state['some.object'].port";
        let md = RequestMetadata::new(&[], attrs(port, number_value(8443.0)));
        assert_eq!(md.attribute(port), "8443");
    }
}
