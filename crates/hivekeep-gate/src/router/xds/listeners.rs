//! Listener builders: the TLS ingress listeners and the internal listeners
//! behind them.

use std::time::Duration;

use envoy_types::pb::envoy::config::core::v3::header_value_option::HeaderAppendAction;
use envoy_types::pb::envoy::config::core::v3::{
    address, socket_address, Address, HeaderValue, HeaderValueOption, SocketAddress,
    TransportSocket,
};
use envoy_types::pb::envoy::config::listener::v3::{
    filter, listener, listener_filter, AdditionalAddress, Filter, FilterChain, Listener,
    ListenerFilter,
};
use envoy_types::pb::envoy::extensions::filters::listener::original_dst::v3::OriginalDst;
use envoy_types::pb::envoy::extensions::filters::network::set_filter_state::v3::Config as NetworkSetFilterState;
use envoy_types::pb::envoy::extensions::filters::network::tcp_proxy::v3::{tcp_proxy, TcpProxy};
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::{
    common_tls_context, subject_alt_name_matcher::SanType, CertificateValidationContext,
    CommonTlsContext, DownstreamTlsContext, SdsSecretConfig, SubjectAltNameMatcher,
};
use envoy_types::pb::envoy::r#type::matcher::v3::{string_matcher::MatchPattern, StringMatcher};
use envoy_types::pb::google::protobuf::Any;

use super::consts::*;
use super::filters::{filter_state_value, stdout_access_log, SharedWithUpstream};
use super::pb;
use super::XdsSettings;
use crate::headers::TARGET_ACTOR_HEADER;
use crate::router::extproc::ORIGINAL_DST_FILTER_STATE_KEY;

fn network_filter(name: &str, config: Any) -> Filter {
    Filter {
        name: name.to_string(),
        config_type: Some(filter::ConfigType::TypedConfig(config)),
    }
}

fn hcm_chain(hcm: Any, transport_socket: Option<TransportSocket>) -> FilterChain {
    FilterChain {
        filters: vec![network_filter(HCM_FILTER_NAME, hcm)],
        transport_socket,
        ..Default::default()
    }
}

fn internal_listener() -> Option<listener::ListenerSpecifier> {
    Some(listener::ListenerSpecifier::InternalListener(
        listener::InternalListenerConfig::default(),
    ))
}

/// An IPv6 wildcard twin of a listener on `port`, so ingress is dual-stack.
pub(crate) fn dual_stack_additional_addresses(port: u16) -> Vec<AdditionalAddress> {
    vec![AdditionalAddress {
        address: Some(Address {
            address: Some(address::Address::SocketAddress(SocketAddress {
                address: "::".to_string(),
                ipv4_compat: false,
                port_specifier: Some(socket_address::PortSpecifier::PortValue(u32::from(port))),
                ..Default::default()
            })),
        }),
        ..Default::default()
    }]
}

/// The internal listener that opens a CONNECT tunnel to the worker named by
/// the request's tunnel filter state.
pub(crate) fn build_actor_connect_listener() -> Listener {
    let set_worker = NetworkSetFilterState {
        on_new_connection: vec![filter_state_value(
            ORIGINAL_DST_FILTER_STATE_KEY,
            "",
            &format!("%FILTER_STATE({TUNNEL_WORKER_FILTER_STATE_KEY}:PLAIN)%"),
            SharedWithUpstream::None,
        )],
        ..Default::default()
    };
    let proxy = TcpProxy {
        stat_prefix: ACTOR_CONNECT_NAME.to_string(),
        cluster_specifier: Some(tcp_proxy::ClusterSpecifier::Cluster(
            ACTOR_CONNECT_NAME.to_string(),
        )),
        tunneling_config: Some(tcp_proxy::TunnelingConfig {
            hostname: format!("actor:%FILTER_STATE({TUNNEL_PORT_FILTER_STATE_KEY}:PLAIN)%"),
            headers_to_add: vec![HeaderValueOption {
                header: Some(HeaderValue {
                    key: TARGET_ACTOR_HEADER.to_string(),
                    value: format!("%FILTER_STATE({TUNNEL_ACTOR_FILTER_STATE_KEY}:PLAIN)%"),
                    ..Default::default()
                }),
                append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                ..Default::default()
            }],
            propagate_response_headers: true,
            ..Default::default()
        }),
        idle_timeout: Some(pb::duration(Duration::ZERO)),
        access_log: stdout_access_log(&tunnel_access_log_format()),
        ..Default::default()
    };
    Listener {
        name: ACTOR_CONNECT_NAME.to_string(),
        listener_specifier: internal_listener(),
        filter_chains: vec![FilterChain {
            filters: vec![
                network_filter(
                    "envoy.filters.network.set_filter_state",
                    pb::any(set_worker),
                ),
                network_filter("envoy.filters.network.tcp_proxy", pb::any(proxy)),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

impl XdsSettings {
    /// The internal listener the CONNECT terminator feeds; it restores the
    /// original destination and routes like the HTTPS listener.
    pub(crate) fn build_main_internal_listener(&self) -> Listener {
        Listener {
            name: MAIN_INTERNAL_NAME.to_string(),
            listener_specifier: internal_listener(),
            filter_chains: vec![hcm_chain(self.build_hcm(MAIN_INTERNAL_NAME, false), None)],
            listener_filters: vec![ListenerFilter {
                name: "envoy.filters.listener.original_dst".to_string(),
                config_type: Some(listener_filter::ConfigType::TypedConfig(pb::any(
                    OriginalDst {},
                ))),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Downstream mTLS: the serving cert and client CAs arrive over SDS, and
    /// only the allowed SPIFFE IDs may connect.
    pub(crate) fn build_downstream_tls_transport_socket(
        &self,
        alpn_protocols: &[&str],
    ) -> TransportSocket {
        let san_matchers = self
            .downstream_allowed_spiffe_ids
            .iter()
            .map(|id| SubjectAltNameMatcher {
                san_type: SanType::Uri as i32,
                matcher: Some(StringMatcher {
                    match_pattern: Some(MatchPattern::Exact(id.clone())),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
        let tls = DownstreamTlsContext {
            require_client_certificate: Some(pb::bool_value(true)),
            common_tls_context: Some(CommonTlsContext {
                alpn_protocols: alpn_protocols.iter().map(|p| p.to_string()).collect(),
                tls_certificate_sds_secret_configs: vec![SdsSecretConfig {
                    name: HTTPS_CERT_SECRET_NAME.to_string(),
                    sds_config: Some(pb::ads_config_source()),
                }],
                validation_context_type: Some(
                    common_tls_context::ValidationContextType::CombinedValidationContext(
                        common_tls_context::CombinedCertificateValidationContext {
                            default_validation_context: Some(CertificateValidationContext {
                                match_typed_subject_alt_names: san_matchers,
                                ..Default::default()
                            }),
                            validation_context_sds_secret_config: Some(SdsSecretConfig {
                                name: INGRESS_CLIENT_TRUST_SECRET_NAME.to_string(),
                                sds_config: Some(pb::ads_config_source()),
                            }),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        pb::transport_socket(TLS_TRANSPORT_SOCKET_NAME, pb::any(tls))
    }

    fn tls_listener(&self, name: &str, port: u16, hcm: Any, alpn: &[&str]) -> Listener {
        Listener {
            name: name.to_string(),
            address: Some(pb::socket_address(WILDCARD_IP, u32::from(port))),
            additional_addresses: dual_stack_additional_addresses(port),
            filter_chains: vec![hcm_chain(
                hcm,
                Some(self.build_downstream_tls_transport_socket(alpn)),
            )],
            ..Default::default()
        }
    }

    /// The HTTPS ingress listener.
    pub(crate) fn build_https_listener(&self) -> Listener {
        self.tls_listener(
            INGRESS_HTTPS_LISTENER,
            self.https_port,
            self.build_hcm("ingress_https", true),
            &["h2", "http/1.1"],
        )
    }

    /// The TLS listener terminating CONNECT tunnels.
    pub(crate) fn build_connect_terminate_tls_listener(&self) -> Listener {
        let hcm = self.build_connect_terminate_hcm(CONNECT_TERMINATE_TLS_LISTENER);
        self.tls_listener(
            CONNECT_TERMINATE_TLS_LISTENER,
            self.connect_tls_port,
            hcm,
            &[],
        )
    }
}
