//! Cluster builders: ext_proc, OTLP collector, the worker-facing clusters
//! and the internal clusters chaining listeners together.

use std::collections::HashMap;
use std::time::Duration;

use envoy_types::pb::envoy::config::cluster::v3::{
    circuit_breakers::Thresholds,
    cluster::{ClusterDiscoveryType, DiscoveryType, LbConfig, LbPolicy, OriginalDstLbConfig},
    CircuitBreakers, Cluster,
};
use envoy_types::pb::envoy::config::core::v3::{
    address, envoy_internal_address, Address, EnvoyInternalAddress, Http1ProtocolOptions,
    Http2ProtocolOptions, HttpProtocolOptions as CommonHttpProtocolOptions, RoutingPriority,
    TransportSocket,
};
use envoy_types::pb::envoy::config::endpoint::v3::{
    lb_endpoint, ClusterLoadAssignment, Endpoint, LbEndpoint, LocalityLbEndpoints,
};
use envoy_types::pb::envoy::extensions::transport_sockets::internal_upstream::v3::{
    internal_upstream_transport::MetadataValueSource, InternalUpstreamTransport,
};
use envoy_types::pb::envoy::extensions::transport_sockets::raw_buffer::v3::RawBuffer;
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::{
    common_tls_context, subject_alt_name_matcher::SanType, CertificateValidationContext,
    CommonTlsContext, SdsSecretConfig, SubjectAltNameMatcher, UpstreamTlsContext,
};
use envoy_types::pb::envoy::extensions::upstreams::http::v3::{
    http_protocol_options::{
        explicit_http_config::ProtocolConfig, ExplicitHttpConfig, UpstreamProtocolOptions,
        UseDownstreamHttpConfig,
    },
    HttpProtocolOptions,
};
use envoy_types::pb::envoy::r#type::matcher::v3::{string_matcher::MatchPattern, StringMatcher};
use envoy_types::pb::envoy::r#type::metadata::v3::{
    metadata_key::path_segment::Segment, metadata_key::PathSegment, metadata_kind, MetadataKey,
    MetadataKind,
};
use envoy_types::pb::google::protobuf::Any;

use super::consts::*;
use super::pb;
use super::XdsSettings;
use crate::router::ingress::{ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_METADATA_KEY};
use crate::tunnel::ingress::DEFAULT_CONNECT_PORT;

fn static_type(t: DiscoveryType) -> Option<ClusterDiscoveryType> {
    Some(ClusterDiscoveryType::Type(t as i32))
}

/// A load assignment with one endpoint at `addr`.
fn single_endpoint(cluster_name: &str, addr: Address) -> ClusterLoadAssignment {
    ClusterLoadAssignment {
        cluster_name: cluster_name.to_string(),
        endpoints: vec![LocalityLbEndpoints {
            lb_endpoints: vec![LbEndpoint {
                host_identifier: Some(lb_endpoint::HostIdentifier::Endpoint(Endpoint {
                    address: Some(addr),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A load assignment pointing at the internal listener `listener_name`.
pub(crate) fn internal_listener_assignment(
    cluster_name: &str,
    listener_name: &str,
) -> ClusterLoadAssignment {
    single_endpoint(
        cluster_name,
        Address {
            address: Some(address::Address::EnvoyInternalAddress(
                EnvoyInternalAddress {
                    address_name_specifier: Some(
                        envoy_internal_address::AddressNameSpecifier::ServerListenerName(
                            listener_name.to_string(),
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
}

fn thresholds(
    max_connections: Option<u32>,
    max_pending: Option<u32>,
    max_requests: u32,
) -> CircuitBreakers {
    CircuitBreakers {
        thresholds: vec![Thresholds {
            priority: RoutingPriority::Default as i32,
            max_connections: max_connections.map(pb::u32_value),
            max_pending_requests: max_pending.map(pb::u32_value),
            max_requests: Some(pb::u32_value(max_requests)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn uniform_breakers(limit: u32) -> CircuitBreakers {
    thresholds(Some(limit), Some(limit), limit)
}

fn protocol_options(opts: HttpProtocolOptions) -> HashMap<String, Any> {
    HashMap::from([(HTTP_PROTOCOL_OPTIONS_NAME.to_string(), pb::any(opts))])
}

fn explicit(config: ProtocolConfig) -> Option<UpstreamProtocolOptions> {
    Some(UpstreamProtocolOptions::ExplicitHttpConfig(
        ExplicitHttpConfig {
            protocol_config: Some(config),
        },
    ))
}

fn explicit_http2() -> HashMap<String, Any> {
    protocol_options(HttpProtocolOptions {
        upstream_protocol_options: explicit(ProtocolConfig::Http2ProtocolOptions(
            Http2ProtocolOptions::default(),
        )),
        ..Default::default()
    })
}

/// The plaintext transport socket an internal upstream wraps.
pub(crate) fn raw_buffer() -> TransportSocket {
    pb::transport_socket("raw_buffer", pb::any(RawBuffer {}))
}

/// A cluster dialing the actor at the tunnel's worker on its CONNECT port.
pub(crate) fn build_actor_tunnel_cluster(name: &str, http2: bool) -> Cluster {
    let config = if http2 {
        ProtocolConfig::Http2ProtocolOptions(Http2ProtocolOptions::default())
    } else {
        ProtocolConfig::HttpProtocolOptions(Http1ProtocolOptions::default())
    };
    Cluster {
        name: name.to_string(),
        cluster_discovery_type: static_type(DiscoveryType::Static),
        load_assignment: Some(internal_listener_assignment(name, ACTOR_CONNECT_NAME)),
        transport_socket: Some(pb::transport_socket(
            "internal_upstream",
            pb::any(InternalUpstreamTransport {
                transport_socket: Some(raw_buffer()),
                ..Default::default()
            }),
        )),
        circuit_breakers: Some(uniform_breakers(TUNNEL_CIRCUIT_BREAKER_LIMIT)),
        typed_extension_protocol_options: protocol_options(HttpProtocolOptions {
            common_http_protocol_options: Some(CommonHttpProtocolOptions {
                idle_timeout: Some(pb::duration(TUNNEL_IDLE_TIMEOUT)),
                ..Default::default()
            }),
            upstream_protocol_options: explicit(config),
            ..Default::default()
        }),
        ..Default::default()
    }
}

impl XdsSettings {
    /// The ext_proc cluster.
    pub(crate) fn build_cluster(&self) -> Cluster {
        Cluster {
            name: CLUSTER_NAME.to_string(),
            connect_timeout: Some(pb::duration(Duration::from_millis(250))),
            cluster_discovery_type: static_type(DiscoveryType::Static),
            lb_policy: LbPolicy::RoundRobin as i32,
            circuit_breakers: Some(thresholds(None, None, self.ext_proc_max_requests)),
            load_assignment: Some(single_endpoint(
                CLUSTER_NAME,
                pb::socket_address(&self.extproc_addr, u32::from(self.extproc_port)),
            )),
            typed_extension_protocol_options: explicit_http2(),
            ..Default::default()
        }
    }

    /// The OTLP collector cluster Envoy's tracer reports to.
    pub(crate) fn build_otlp_collector_cluster(&self) -> Cluster {
        Cluster {
            name: OTLP_CLUSTER_NAME.to_string(),
            connect_timeout: Some(pb::duration(Duration::from_secs(1))),
            cluster_discovery_type: static_type(DiscoveryType::StrictDns),
            lb_policy: LbPolicy::RoundRobin as i32,
            load_assignment: Some(single_endpoint(
                OTLP_CLUSTER_NAME,
                pb::socket_address(&self.otlp_host, self.otlp_port),
            )),
            typed_extension_protocol_options: explicit_http2(),
            ..Default::default()
        }
    }

    /// The mTLS socket dialing workers, or none without a client certificate.
    pub(crate) fn build_upstream_transport_socket(&self) -> Option<TransportSocket> {
        if self.upstream_credential_bundle_path.is_empty() {
            return None;
        }
        let mut common = CommonTlsContext {
            tls_certificate_sds_secret_configs: vec![SdsSecretConfig {
                name: UPSTREAM_CERT_SECRET_NAME.to_string(),
                sds_config: Some(pb::ads_config_source()),
            }],
            ..Default::default()
        };
        if !self.upstream_trust_bundle_path.is_empty() {
            let default_ctx = CertificateValidationContext {
                match_typed_subject_alt_names: vec![SubjectAltNameMatcher {
                    san_type: SanType::Uri as i32,
                    matcher: Some(StringMatcher {
                        match_pattern: Some(MatchPattern::Prefix(
                            TRUST_DOMAIN_SPIFFE_PREFIX.to_string(),
                        )),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            };
            common.validation_context_type = Some(
                common_tls_context::ValidationContextType::CombinedValidationContext(
                    common_tls_context::CombinedCertificateValidationContext {
                        default_validation_context: Some(default_ctx),
                        validation_context_sds_secret_config: Some(SdsSecretConfig {
                            name: UPSTREAM_TRUST_SECRET_NAME.to_string(),
                            sds_config: Some(pb::ads_config_source()),
                        }),
                        ..Default::default()
                    },
                ),
            );
        }
        let tls = UpstreamTlsContext {
            common_tls_context: Some(common),
            ..Default::default()
        };
        Some(pb::transport_socket(
            TLS_TRANSPORT_SOCKET_NAME,
            pb::any(tls),
        ))
    }

    /// The internal cluster feeding CONNECT payloads into `main_internal`,
    /// carrying the original-destination metadata across.
    pub(crate) fn build_main_internal_cluster(&self) -> Cluster {
        Cluster {
            name: MAIN_INTERNAL_NAME.to_string(),
            cluster_discovery_type: static_type(DiscoveryType::Static),
            load_assignment: Some(internal_listener_assignment(
                MAIN_INTERNAL_NAME,
                MAIN_INTERNAL_NAME,
            )),
            transport_socket: Some(pb::transport_socket(
                "internal_upstream",
                pb::any(InternalUpstreamTransport {
                    transport_socket: Some(raw_buffer()),
                    passthrough_metadata: vec![MetadataValueSource {
                        kind: Some(MetadataKind {
                            kind: Some(metadata_kind::Kind::Host(metadata_kind::Host {})),
                        }),
                        name: ORIGINAL_DST_METADATA_KEY.to_string(),
                    }],
                }),
            )),
            ..Default::default()
        }
    }

    /// The cluster dialing the worker address ext_proc picked, used when
    /// workers are reached without CONNECT tunnels.
    pub(crate) fn build_original_dst_cluster(&self) -> Cluster {
        let mut cluster = Cluster {
            name: ORIGINAL_DST_CLUSTER_NAME.to_string(),
            connect_timeout: Some(pb::duration(Duration::from_secs(5))),
            cluster_discovery_type: static_type(DiscoveryType::OriginalDst),
            lb_policy: LbPolicy::ClusterProvided as i32,
            lb_config: Some(LbConfig::OriginalDstLbConfig(OriginalDstLbConfig {
                metadata_key: Some(MetadataKey {
                    key: ORIGINAL_DST_METADATA_KEY.to_string(),
                    path: vec![PathSegment {
                        segment: Some(Segment::Key(ORIGINAL_DST_ADDRESS_KEY.to_string())),
                    }],
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        if let Some(ts) = self.build_upstream_transport_socket() {
            cluster.transport_socket = Some(ts);
            cluster.typed_extension_protocol_options = protocol_options(HttpProtocolOptions {
                upstream_protocol_options: Some(
                    UpstreamProtocolOptions::UseDownstreamProtocolConfig(UseDownstreamHttpConfig {
                        http_protocol_options: Some(Http1ProtocolOptions::default()),
                        http2_protocol_options: Some(Http2ProtocolOptions::default()),
                        ..Default::default()
                    }),
                ),
                ..Default::default()
            });
        }
        cluster
    }

    /// The cluster opening mTLS HTTP/2 connections to workers' CONNECT port,
    /// at the address the tunnel's filter state names.
    pub(crate) fn build_actor_connect_cluster(&self) -> Cluster {
        Cluster {
            name: ACTOR_CONNECT_NAME.to_string(),
            connect_timeout: Some(pb::duration(Duration::from_secs(5))),
            cluster_discovery_type: static_type(DiscoveryType::OriginalDst),
            lb_policy: LbPolicy::ClusterProvided as i32,
            lb_config: Some(LbConfig::OriginalDstLbConfig(OriginalDstLbConfig {
                upstream_port_override: Some(pb::u32_value(u32::from(DEFAULT_CONNECT_PORT))),
                ..Default::default()
            })),
            transport_socket: self.build_upstream_transport_socket(),
            circuit_breakers: Some(uniform_breakers(ACTOR_CONNECT_CIRCUIT_BREAKER_LIMIT)),
            typed_extension_protocol_options: explicit_http2(),
            ..Default::default()
        }
    }
}
