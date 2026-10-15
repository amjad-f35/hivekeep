//! Route configurations: the RDS-served workload routes and the inline
//! routes of the CONNECT terminator.

use std::time::Duration;

use envoy_types::pb::envoy::config::core::v3::{
    header_value_option::HeaderAppendAction, HeaderValue, HeaderValueOption,
};
use envoy_types::pb::envoy::config::route::v3::{
    header_matcher, route, route_action, route_match, HeaderMatcher, Route, RouteAction,
    RouteConfiguration, RouteMatch, VirtualHost,
};
use envoy_types::pb::envoy::r#type::matcher::v3::{
    string_matcher::MatchPattern, RegexMatcher, StringMatcher,
};

use super::consts::*;
use super::pb;
use super::XdsSettings;
use crate::headers::TARGET_ACTOR_HEADER;
use crate::router::ingress::{ORIGINAL_DST_METADATA_KEY, ORIGINAL_DST_PORT_KEY};
use crate::tunnel::ingress::TARGET_PORT_HEADER;

/// Content types gRPC requests carry, case-insensitively.
pub(crate) const GRPC_CONTENT_TYPE_REGEX: &str = r"(?i)^application/grpc([+;].*)?$";

fn prefix_match(headers: Vec<HeaderMatcher>) -> RouteMatch {
    RouteMatch {
        path_specifier: Some(route_match::PathSpecifier::Prefix("/".to_string())),
        headers,
        ..Default::default()
    }
}

fn string_header(name: &str, pattern: MatchPattern) -> HeaderMatcher {
    HeaderMatcher {
        name: name.to_string(),
        header_match_specifier: Some(header_matcher::HeaderMatchSpecifier::StringMatch(
            StringMatcher {
                match_pattern: Some(pattern),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

impl XdsSettings {
    /// The idle timeout of the workload route: Envoy's default, unless the
    /// route timeout is longer, so a quiet stream never dies first.
    pub(crate) fn route_idle_timeout(&self) -> Duration {
        let backstop = self.route_timeout + ROUTE_IDLE_TIMEOUT_MARGIN;
        backstop.max(ENVOY_DEFAULT_STREAM_IDLE_TIMEOUT)
    }

    fn workload_route_action(&self, cluster: &str) -> RouteAction {
        RouteAction {
            cluster_specifier: Some(route_action::ClusterSpecifier::Cluster(cluster.to_string())),
            timeout: Some(pb::duration(self.route_timeout)),
            idle_timeout: Some(pb::duration(self.route_idle_timeout())),
            ..Default::default()
        }
    }

    /// The workload routes the ingress listeners fetch over RDS.
    pub(crate) fn build_routes(&self) -> RouteConfiguration {
        let routes = if self.upstream_connect {
            self.build_actor_tunnel_routes()
        } else {
            vec![Route {
                r#match: Some(prefix_match(Vec::new())),
                action: Some(route::Action::Route(self.workload_route_action(ORIGINAL_DST_CLUSTER_NAME))),
                request_headers_to_add: vec![HeaderValueOption {
                    header: Some(HeaderValue {
                        key: TARGET_PORT_HEADER.to_string(),
                        value: format!("%DYNAMIC_METADATA({ORIGINAL_DST_METADATA_KEY}:{ORIGINAL_DST_PORT_KEY})%"),
                        ..Default::default()
                    }),
                    append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                    ..Default::default()
                }],
                ..Default::default()
            }]
        };
        RouteConfiguration {
            name: ROUTE_NAME.to_string(),
            virtual_hosts: vec![VirtualHost {
                name: "local_service".to_string(),
                domains: vec!["*".to_string()],
                routes,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// gRPC requests ride HTTP/2 tunnels; everything else HTTP/1. Both strip
    /// the routing header before it reaches the actor.
    fn build_actor_tunnel_routes(&self) -> Vec<Route> {
        let route = |cluster: &str, headers: Vec<HeaderMatcher>| Route {
            r#match: Some(prefix_match(headers)),
            action: Some(route::Action::Route(self.workload_route_action(cluster))),
            request_headers_to_remove: vec![TARGET_ACTOR_HEADER.to_string()],
            ..Default::default()
        };
        let grpc = vec![
            string_header(":method", MatchPattern::Exact("POST".to_string())),
            string_header(
                "content-type",
                MatchPattern::SafeRegex(RegexMatcher {
                    regex: GRPC_CONTENT_TYPE_REGEX.to_string(),
                    ..Default::default()
                }),
            ),
        ];
        vec![
            route(ACTOR_TUNNEL_HTTP2_CLUSTER_NAME, grpc),
            route(ACTOR_TUNNEL_HTTP1_CLUSTER_NAME, Vec::new()),
        ]
    }
}

/// The CONNECT terminator's inline routes: every CONNECT is upgraded into
/// `main_internal`, without a timeout since tunnels are long-lived.
pub(crate) fn build_connect_routes() -> RouteConfiguration {
    RouteConfiguration {
        name: "default".to_string(),
        virtual_hosts: vec![VirtualHost {
            name: "default".to_string(),
            domains: vec!["*".to_string()],
            routes: vec![Route {
                r#match: Some(RouteMatch {
                    path_specifier: Some(route_match::PathSpecifier::ConnectMatcher(
                        route_match::ConnectMatcher {},
                    )),
                    ..Default::default()
                }),
                action: Some(route::Action::Route(RouteAction {
                    upgrade_configs: vec![route_action::UpgradeConfig {
                        upgrade_type: CONNECT_UPGRADE_TYPE.to_string(),
                        connect_config: Some(route_action::upgrade_config::ConnectConfig::default()),
                        ..Default::default()
                    }],
                    cluster_specifier: Some(route_action::ClusterSpecifier::Cluster(
                        MAIN_INTERNAL_NAME.to_string(),
                    )),
                    timeout: Some(pb::duration(Duration::ZERO)),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}
