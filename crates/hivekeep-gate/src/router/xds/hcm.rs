//! HTTP connection managers of the ingress listeners and Envoy's tracing.

use std::time::Duration;

use envoy_types::pb::envoy::config::common::mutation_rules::v3::HeaderMutationRules;
use envoy_types::pb::envoy::config::core::v3::{grpc_service, GrpcService, Http2ProtocolOptions};
use envoy_types::pb::envoy::config::trace::v3::{tracing::http, OpenTelemetryConfig};
use envoy_types::pb::envoy::extensions::filters::http::ext_proc::v3::{
    metadata_options::MetadataNamespaces, processing_mode, ExternalProcessor, MetadataOptions,
    ProcessingMode,
};
use envoy_types::pb::envoy::extensions::filters::network::http_connection_manager::v3::{
    http_connection_manager::{self, CodecType, Tracing, UpgradeConfig},
    http_filter, HttpConnectionManager, HttpFilter, Rds,
};
use envoy_types::pb::envoy::r#type::v3::Percent;
use envoy_types::pb::google::protobuf::Any;

use super::consts::*;
use super::filters::{
    actor_routing_filter_state_filter, actor_tunnel_filter_state_filter, router_filter,
    stdout_access_log, worker_identity_filter_state_filter,
};
use super::pb;
use super::routes::build_connect_routes;
use super::XdsSettings;
use crate::router::extproc::{
    CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE, TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
};
use crate::router::ingress::ORIGINAL_DST_METADATA_KEY;

fn envoy_grpc(cluster_name: &str, timeout: Option<Duration>) -> GrpcService {
    GrpcService {
        target_specifier: Some(grpc_service::TargetSpecifier::EnvoyGrpc(
            grpc_service::EnvoyGrpc {
                cluster_name: cluster_name.to_string(),
                ..Default::default()
            },
        )),
        timeout: timeout.map(pb::duration),
        ..Default::default()
    }
}

fn upgrade(kind: &str) -> UpgradeConfig {
    UpgradeConfig {
        upgrade_type: kind.to_string(),
        ..Default::default()
    }
}

impl XdsSettings {
    /// The ext_proc filter: request headers only, with the routing filter
    /// state and original-destination metadata passed both ways.
    fn ext_proc_filter(&self) -> HttpFilter {
        let namespaces = || MetadataNamespaces {
            untyped: vec![ORIGINAL_DST_METADATA_KEY.to_string()],
            ..Default::default()
        };
        let config = ExternalProcessor {
            grpc_service: Some(envoy_grpc(
                CLUSTER_NAME,
                Some(self.ext_proc_message_timeout),
            )),
            mutation_rules: Some(HeaderMutationRules {
                allow_all_routing: Some(pb::bool_value(true)),
                ..Default::default()
            }),
            message_timeout: Some(pb::duration(self.ext_proc_message_timeout)),
            processing_mode: Some(ProcessingMode {
                request_header_mode: processing_mode::HeaderSendMode::Send as i32,
                response_header_mode: processing_mode::HeaderSendMode::Skip as i32,
                request_body_mode: processing_mode::BodySendMode::None as i32,
                response_body_mode: processing_mode::BodySendMode::None as i32,
                request_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
                response_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
            }),
            request_attributes: vec![
                TARGET_ACTOR_FILTER_STATE_ATTRIBUTE.to_string(),
                CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE.to_string(),
            ],
            metadata_options: Some(MetadataOptions {
                forwarding_namespaces: Some(namespaces()),
                receiving_namespaces: Some(namespaces()),
                ..Default::default()
            }),
            ..Default::default()
        };
        HttpFilter {
            name: HTTP_EXT_PROC_FILTER_NAME.to_string(),
            config_type: Some(http_filter::ConfigType::TypedConfig(pb::any(config))),
            ..Default::default()
        }
    }

    /// The connection manager of an ingress listener, routing over RDS
    /// after ext_proc resolves the actor. `capture_actor_routing` stamps the
    /// target actor first, for listeners clients reach directly.
    pub(crate) fn build_hcm(&self, stat_prefix: &str, capture_actor_routing: bool) -> Any {
        let mut filters = Vec::new();
        if capture_actor_routing {
            filters.push(actor_routing_filter_state_filter(false));
        }
        filters.push(self.ext_proc_filter());
        if self.upstream_connect {
            filters.push(actor_tunnel_filter_state_filter());
        }
        if self.upstream_validated() {
            filters.push(worker_identity_filter_state_filter());
        }
        filters.push(router_filter());
        pb::any(HttpConnectionManager {
            stat_prefix: stat_prefix.to_string(),
            generate_request_id: Some(pb::bool_value(true)),
            upgrade_configs: vec![upgrade("websocket")],
            tracing: self.build_tracing(),
            access_log: stdout_access_log(ACCESS_LOG_FORMAT),
            http_filters: filters,
            route_specifier: Some(http_connection_manager::RouteSpecifier::Rds(Rds {
                route_config_name: ROUTE_NAME.to_string(),
                config_source: Some(pb::ads_config_source()),
            })),
            ..Default::default()
        })
    }

    /// The connection manager terminating CONNECT tunnels and handing their
    /// payload to `main_internal`.
    pub(crate) fn build_connect_terminate_hcm(&self, stat_prefix: &str) -> Any {
        pb::any(HttpConnectionManager {
            stat_prefix: stat_prefix.to_string(),
            generate_request_id: Some(pb::bool_value(true)),
            tracing: self.build_tracing(),
            access_log: stdout_access_log(ACCESS_LOG_FORMAT),
            route_specifier: Some(http_connection_manager::RouteSpecifier::RouteConfig(
                build_connect_routes(),
            )),
            upgrade_configs: vec![upgrade(CONNECT_UPGRADE_TYPE), upgrade("websocket")],
            codec_type: CodecType::Auto as i32,
            http_filters: vec![actor_routing_filter_state_filter(true), router_filter()],
            http2_protocol_options: Some(Http2ProtocolOptions {
                allow_connect: true,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// Envoy's OpenTelemetry tracing, or none without a collector.
    pub(crate) fn build_tracing(&self) -> Option<Tracing> {
        if self.otlp_host.is_empty() {
            return None;
        }
        let otel = OpenTelemetryConfig {
            grpc_service: Some(envoy_grpc(OTLP_CLUSTER_NAME, None)),
            service_name: TRACING_SERVICE_NAME.to_string(),
            ..Default::default()
        };
        Some(Tracing {
            random_sampling: Some(Percent {
                value: self.trace_root_sampling_percent,
            }),
            provider: Some(envoy_types::pb::envoy::config::trace::v3::tracing::Http {
                name: "envoy.tracers.opentelemetry".to_string(),
                config_type: Some(http::ConfigType::TypedConfig(pb::any(otel))),
            }),
            ..Default::default()
        })
    }
}
