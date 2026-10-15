//! Which handler a request goes to: ingress (routing to Actors) or egress
//! (the gateway's policy enforcement), decided from attributes Envoy
//! attaches, never from client-controlled headers.

use std::fmt;

use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest;

use super::attributes::{DIRECTION_ATTRIBUTE, FILTER_CHAIN_NAME_ATTRIBUTE};

/// The traffic a handler serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Requests to Actors.
    Ingress,
    /// Requests from Actors.
    Egress,
}

impl Direction {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Ingress => "ingress",
            Direction::Egress => "egress",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The egress gateway's outer CONNECT filter chain.
pub const EGRESS_FILTER_CHAIN_NAME: &str = "egress";
/// The chain for TLS the gateway terminates.
pub const EGRESS_TLS_MITM_FILTER_CHAIN_NAME: &str = "egress_tls_mitm";
/// The chain for cleartext HTTP inside a tunnel.
pub const EGRESS_CLEARTEXT_FILTER_CHAIN_NAME: &str = "egress_cleartext";

/// Whether `name` is one of the egress gateway's filter chains.
pub fn is_egress_filter_chain(name: &str) -> bool {
    matches!(
        name,
        EGRESS_FILTER_CHAIN_NAME
            | EGRESS_TLS_MITM_FILTER_CHAIN_NAME
            | EGRESS_CLEARTEXT_FILTER_CHAIN_NAME
    )
}

/// A string attribute of the request from any filter, or "".
pub fn request_attribute(req: &ProcessingRequest, name: &str) -> String {
    use envoy_types::pb::google::protobuf::value::Kind;
    for attrs in req.attributes.values() {
        if let Some(v) = attrs.fields.get(name) {
            return match &v.kind {
                Some(Kind::StringValue(s)) => s.clone(),
                _ => String::new(),
            };
        }
    }
    String::new()
}

/// The direction of `req`.
pub fn direction_of(req: &ProcessingRequest) -> Direction {
    if request_attribute(req, DIRECTION_ATTRIBUTE) == Direction::Egress.as_str()
        || is_egress_filter_chain(&request_attribute(req, FILTER_CHAIN_NAME_ATTRIBUTE))
    {
        Direction::Egress
    } else {
        Direction::Ingress
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use envoy_types::pb::envoy::config::core::v3::{HeaderMap, HeaderValue};
    use envoy_types::pb::envoy::service::ext_proc::v3::{processing_request, HttpHeaders};
    use envoy_types::pb::google::protobuf::Struct;

    use crate::router::extproc::metadata::string_value;

    pub(crate) const INGRESS_HTTP_LISTENER: &str = "ingress_http_listener";
    const INGRESS_HTTPS_LISTENER: &str = "ingress_https_listener";

    pub(crate) fn connect_request(filter_key: &str, chain: &str) -> ProcessingRequest {
        let raw = |k: &str, v: &str| HeaderValue {
            key: k.into(),
            raw_value: v.as_bytes().to_vec(),
            ..Default::default()
        };
        let mut req = ProcessingRequest {
            request: Some(processing_request::Request::RequestHeaders(HttpHeaders {
                headers: Some(HeaderMap {
                    headers: vec![raw(":method", "CONNECT"), raw(":authority", "10.0.0.9:443")],
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        if !chain.is_empty() {
            req.attributes.insert(
                filter_key.to_string(),
                Struct {
                    fields: [(FILTER_CHAIN_NAME_ATTRIBUTE.to_string(), string_value(chain))].into(),
                },
            );
        }
        req
    }

    #[test]
    fn direction_of_cases() {
        let ext = "envoy.filters.http.ext_proc";
        for (name, key, chain, want) in [
            (
                "egress filter chain",
                ext,
                EGRESS_FILTER_CHAIN_NAME,
                Direction::Egress,
            ),
            (
                "egress filter chain under a renamed filter",
                "some.custom.ext_proc.name",
                EGRESS_FILTER_CHAIN_NAME,
                Direction::Egress,
            ),
            (
                "CONNECT on the ingress HTTP listener",
                ext,
                INGRESS_HTTP_LISTENER,
                Direction::Ingress,
            ),
            (
                "CONNECT on the ingress HTTPS listener",
                ext,
                INGRESS_HTTPS_LISTENER,
                Direction::Ingress,
            ),
            ("no attributes at all", "", "", Direction::Ingress),
            (
                "unrecognised filter chain",
                ext,
                "some-other-chain",
                Direction::Ingress,
            ),
        ] {
            assert_eq!(direction_of(&connect_request(key, chain)), want, "{name}");
        }
    }

    #[test]
    fn direction_attribute_marks_egress() {
        let mut req = connect_request("", "");
        req.attributes.insert(
            "envoy.filters.http.ext_proc".into(),
            Struct {
                fields: [(DIRECTION_ATTRIBUTE.to_string(), string_value("egress"))].into(),
            },
        );
        assert_eq!(direction_of(&req), Direction::Egress);
    }

    #[test]
    fn ignores_client_supplied_attribute_header() {
        let mut req = connect_request("envoy.filters.http.ext_proc", INGRESS_HTTP_LISTENER);
        if let Some(processing_request::Request::RequestHeaders(h)) = &mut req.request {
            let hm = h.headers.as_mut().unwrap();
            for k in [FILTER_CHAIN_NAME_ATTRIBUTE, "x-envoy-filter-chain-name"] {
                hm.headers.push(HeaderValue {
                    key: k.into(),
                    raw_value: EGRESS_FILTER_CHAIN_NAME.as_bytes().to_vec(),
                    ..Default::default()
                });
            }
        }
        assert_eq!(direction_of(&req), Direction::Ingress);
    }

    #[test]
    fn inner_egress_chains() {
        for chain in [
            EGRESS_TLS_MITM_FILTER_CHAIN_NAME,
            EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
        ] {
            assert_eq!(
                direction_of(&connect_request("envoy.filters.http.ext_proc", chain)),
                Direction::Egress,
                "{chain}"
            );
        }
        assert!(!is_egress_filter_chain("egress_something_else"));
    }
}
