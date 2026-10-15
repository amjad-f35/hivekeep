//! set_filter_state filters stamping the routing keys Envoy and ext_proc
//! share, and the stdout access log.

use envoy_types::pb::envoy::config::accesslog::v3::{access_log, AccessLog};
use envoy_types::pb::envoy::config::core::v3::{
    substitution_format_string, SubstitutionFormatString,
};
use envoy_types::pb::envoy::extensions::access_loggers::stream::v3::{
    stdout_access_log, StdoutAccessLog,
};
use envoy_types::pb::envoy::extensions::filters::common::set_filter_state::v3::{
    filter_state_value, FilterStateValue,
};
use envoy_types::pb::envoy::extensions::filters::http::set_filter_state::v3::Config as HttpSetFilterState;
use envoy_types::pb::envoy::extensions::filters::network::http_connection_manager::v3::{
    http_filter, HttpFilter,
};

pub(crate) use envoy_types::pb::envoy::extensions::filters::common::set_filter_state::v3::filter_state_value::SharedWithUpstream;

use super::consts::*;
use super::pb;
use crate::headers::TARGET_ACTOR_HEADER;
use crate::router::extproc::{
    AUTHORITY_HEADER, CONNECT_AUTHORITY_FILTER_STATE_KEY, TARGET_ACTOR_FILTER_STATE_KEY,
};
use crate::router::ingress::{
    ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_METADATA_KEY, ORIGINAL_DST_PORT_KEY,
    WORKER_SPIFFE_ID_KEY,
};

/// A text format string `format`.
pub(crate) fn text_format(format: &str) -> SubstitutionFormatString {
    SubstitutionFormatString {
        format: Some(substitution_format_string::Format::TextFormatSource(
            pb::inline_string(format),
        )),
        ..Default::default()
    }
}

/// A filter state object `key` built by `factory` from `format`.
pub(crate) fn filter_state_value(
    key: &str,
    factory: &str,
    format: &str,
    shared: SharedWithUpstream,
) -> FilterStateValue {
    FilterStateValue {
        key: Some(filter_state_value::Key::ObjectKey(key.to_string())),
        factory_key: factory.to_string(),
        value: Some(filter_state_value::Value::FormatString(text_format(format))),
        shared_with_upstream: shared as i32,
        ..Default::default()
    }
}

/// An HTTP filter setting `values` on request headers.
fn set_filter_state_http_filter(values: Vec<FilterStateValue>) -> HttpFilter {
    HttpFilter {
        name: HTTP_SET_FILTER_STATE_NAME.to_string(),
        config_type: Some(http_filter::ConfigType::TypedConfig(pb::any(
            HttpSetFilterState {
                on_request_headers: values,
                ..Default::default()
            },
        ))),
        ..Default::default()
    }
}

/// Stamps the target actor (and, when terminating CONNECT, the tunnel
/// authority and the pool key derived from both) before any filter can
/// rewrite the request.
pub(crate) fn actor_routing_filter_state_filter(capture_authority: bool) -> HttpFilter {
    let once = SharedWithUpstream::Once;
    let actor = format!("%CEL(request.headers['{TARGET_ACTOR_HEADER}'])%");
    let mut values = vec![filter_state_value(
        TARGET_ACTOR_FILTER_STATE_KEY,
        "envoy.string",
        &actor,
        once,
    )];
    if capture_authority {
        let authority = format!("%REQ({AUTHORITY_HEADER})%");
        values.push(filter_state_value(
            CONNECT_AUTHORITY_FILTER_STATE_KEY,
            "envoy.string",
            &authority,
            once,
        ));
        values.push(filter_state_value(
            CONNECT_POOL_FILTER_STATE_KEY,
            "envoy.hashable_string",
            &format!("{actor}|{authority}"),
            once,
        ));
    }
    set_filter_state_http_filter(values)
}

fn original_dst_metadata(key: &str) -> String {
    format!("%DYNAMIC_METADATA({ORIGINAL_DST_METADATA_KEY}:{key})%")
}

/// Stamps the worker, actor and port of a CONNECT tunnel, plus a hashable
/// key over all three so each tunnel gets its own upstream connection pool.
pub(crate) fn actor_tunnel_filter_state_filter() -> HttpFilter {
    let worker = original_dst_metadata(ORIGINAL_DST_ADDRESS_KEY);
    let actor = format!("%REQ({TARGET_ACTOR_HEADER})%");
    let port = original_dst_metadata(ORIGINAL_DST_PORT_KEY);
    let once = SharedWithUpstream::Once;
    set_filter_state_http_filter(vec![
        filter_state_value(
            TUNNEL_KEY_FILTER_STATE_KEY,
            "envoy.hashable_string",
            &format!("{worker}|{actor}|{port}"),
            once,
        ),
        filter_state_value(
            TUNNEL_WORKER_FILTER_STATE_KEY,
            "envoy.string",
            &worker,
            once,
        ),
        filter_state_value(TUNNEL_ACTOR_FILTER_STATE_KEY, "envoy.string", &actor, once),
        filter_state_value(TUNNEL_PORT_FILTER_STATE_KEY, "envoy.string", &port, once),
    ])
}

/// Pins the SAN the upstream TLS handshake verifies to the worker identity
/// ext_proc resolved.
pub(crate) fn worker_identity_filter_state_filter() -> HttpFilter {
    let worker_id = original_dst_metadata(WORKER_SPIFFE_ID_KEY);
    set_filter_state_http_filter(vec![filter_state_value(
        UPSTREAM_SANS_FILTER_STATE_KEY,
        "",
        &worker_id,
        SharedWithUpstream::Once,
    )])
}

/// The router filter, always last.
pub(crate) fn router_filter() -> HttpFilter {
    use envoy_types::pb::envoy::extensions::filters::http::router::v3::Router;
    HttpFilter {
        name: HTTP_ROUTER_FILTER_NAME.to_string(),
        config_type: Some(http_filter::ConfigType::TypedConfig(pb::any(
            Router::default(),
        ))),
        ..Default::default()
    }
}

/// An access log writing `format` lines to stdout.
pub(crate) fn stdout_access_log(format: &str) -> Vec<AccessLog> {
    vec![AccessLog {
        name: "envoy.access_loggers.stdout".to_string(),
        config_type: Some(access_log::ConfigType::TypedConfig(pb::any(
            StdoutAccessLog {
                access_log_format: Some(stdout_access_log::AccessLogFormat::LogFormat(
                    text_format(format),
                )),
            },
        ))),
        ..Default::default()
    }]
}
