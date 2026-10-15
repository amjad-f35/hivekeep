//! The ingress handler: resolves the target Actor of a request, resumes
//! it, and tells Envoy which worker to forward to.

use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use envoy_types::pb::envoy::config::core::v3::{
    header_value_option::HeaderAppendAction, HeaderValue, HeaderValueOption,
};
use envoy_types::pb::envoy::service::ext_proc::v3::{
    CommonResponse, HeaderMutation, HeadersResponse,
};
use envoy_types::pb::google::protobuf::{value::Kind, Struct, Value};

use super::errors::map_resume_error;
use super::parking::{ParkedRequestConfig, ParkingLot, ParkingMetrics, ParkingStatus};
use super::resumer::{ActorApi, ActorResumer, RequestCtx};
use crate::headers::{parse_target_actor, TARGET_ACTOR_HEADER};
use crate::router::extproc::{
    Direction, Handler as ExtprocHandler, HandlerResult, RequestMetadata, RouteError,
    CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE, TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
};
use crate::tunnel::ingress::parse_port;

/// The Actor port requests go to when the CONNECT names none.
const DEFAULT_ACTOR_PORT: u16 = 80;
/// The worker port the router reaches the tunnel on.
const WORKER_TUNNEL_PORT: u16 = 443;

/// Dynamic metadata namespace read by Envoy's original-destination cluster.
pub const ORIGINAL_DST_METADATA_KEY: &str = "envoy.filters.listener.original_dst";
/// Key of the worker address in [`ORIGINAL_DST_METADATA_KEY`].
pub const ORIGINAL_DST_ADDRESS_KEY: &str = "local";
/// Key of the Actor port in [`ORIGINAL_DST_METADATA_KEY`].
pub const ORIGINAL_DST_PORT_KEY: &str = "port";
/// Key of the worker's SPIFFE ID in [`ORIGINAL_DST_METADATA_KEY`].
pub const WORKER_SPIFFE_ID_KEY: &str = "worker_spiffe_id";

/// Routes ingress requests to Actors.
pub struct Handler {
    resumer: Arc<ActorResumer>,
    parking: Arc<ParkingLot>,
}

fn string(s: impl Into<String>) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.into())),
    }
}

/// The routing value: the CONNECT's filter-state attribute when set (and
/// not `-`); none when the request is a CONNECT whose outer request named
/// no Actor; the header otherwise.
fn routing_value(md: &RequestMetadata, header: &str, attribute: &str) -> String {
    let v = md.attribute(attribute);
    if !v.is_empty() && v != "-" {
        return v;
    }
    if !md
        .attribute(CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE)
        .is_empty()
    {
        return String::new();
    }
    md.header(header).to_string()
}

fn authority_port(authority: &str) -> Option<u16> {
    let port = if let Some(rest) = authority.strip_prefix('[') {
        rest.split_once("]:")?.1
    } else {
        let (h, p) = authority.rsplit_once(':')?;
        if h.contains(':') {
            return None;
        }
        p
    };
    parse_port(port)
}

impl Handler {
    /// A handler resuming Actors through `api`.
    pub fn new(
        api: Arc<dyn ActorApi>,
        park: ParkedRequestConfig,
        metrics: Option<ParkingMetrics>,
    ) -> Self {
        let lot = Arc::new(ParkingLot::new(park, metrics));
        let resumer = Arc::new(
            ActorResumer::new(api)
                .with_parking(park)
                .with_lot(lot.clone()),
        );
        Handler {
            resumer,
            parking: lot,
        }
    }

    /// The parking lot's state.
    pub fn parking_status(&self) -> ParkingStatus {
        self.parking.status()
    }

    /// The parking lot.
    pub fn parking(&self) -> &Arc<ParkingLot> {
        &self.parking
    }

    /// Routes one request, under the caller's cancellation and deadline.
    pub async fn route(
        &self,
        md: &RequestMetadata,
        ctx: &RequestCtx,
    ) -> Result<HandlerResult, (RouteError, String)> {
        tracing::info!(host = %md.host, "request");
        let target = routing_value(md, TARGET_ACTOR_HEADER, TARGET_ACTOR_FILTER_STATE_ATTRIBUTE);
        let actor = parse_target_actor(&target).map_err(|_| {
            (
                RouteError::req(404, "invalid actor reference"),
                String::new(),
            )
        })?;
        let mut authority = md.attribute(CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE);
        if authority.is_empty() && md.method == "CONNECT" {
            authority = md.host.clone();
        }
        let target_port = authority_port(&authority).unwrap_or(DEFAULT_ACTOR_PORT);
        tracing::info!(actor = %actor, "ResumeActor");
        let (resumed, outcome) = match self.resumer.resume_actor(&actor, ctx).await {
            Ok(r) => r,
            Err(e) => {
                return Err((
                    map_resume_error(&actor, &e),
                    crate::attr::resume::UNKNOWN.to_string(),
                ))
            }
        };
        let mut res = HandlerResult {
            template_hive: resumed
                .actor_template
                .as_ref()
                .map(|t| t.hive.clone())
                .unwrap_or_default(),
            template_name: resumed
                .actor_template
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default(),
            resume: outcome.as_str().to_string(),
            ..Default::default()
        };
        let assignment = resumed
            .status
            .as_ref()
            .and_then(|s| s.worker_assignment.as_ref());
        let worker_ip = assignment
            .and_then(|a| a.worker_pod_ips.first())
            .cloned()
            .unwrap_or_default();
        let spiffe = assignment
            .map(|a| a.worker_spiffe_id.clone())
            .unwrap_or_default();
        tracing::info!(actor = %actor, worker_ip = %worker_ip, worker_spiffe_id = %spiffe, "ResumeActor result");
        let routing_failed = || {
            (
                RouteError::req(500, format!("actor {actor} routing failed")),
                res.resume.clone(),
            )
        };
        let Ok(ip) = worker_ip.parse::<IpAddr>() else {
            return Err(routing_failed());
        };
        if spiffe.is_empty() {
            return Err(routing_failed());
        }
        let target_addr = std::net::SocketAddr::new(ip, WORKER_TUNNEL_PORT).to_string();
        tracing::info!(actor = %actor, target_addr = %target_addr, "route ok");
        let inner = Struct {
            fields: [
                (
                    ORIGINAL_DST_ADDRESS_KEY.to_string(),
                    string(target_addr.clone()),
                ),
                (
                    ORIGINAL_DST_PORT_KEY.to_string(),
                    string(target_port.to_string()),
                ),
                (WORKER_SPIFFE_ID_KEY.to_string(), string(spiffe)),
            ]
            .into(),
        };
        res.dynamic_metadata = Some(Struct {
            fields: [(
                ORIGINAL_DST_METADATA_KEY.to_string(),
                Value {
                    kind: Some(Kind::StructValue(inner)),
                },
            )]
            .into(),
        });
        res.target = target_addr;
        res.response = HeadersResponse {
            response: Some(CommonResponse {
                header_mutation: Some(HeaderMutation {
                    set_headers: vec![HeaderValueOption {
                        header: Some(HeaderValue {
                            key: TARGET_ACTOR_HEADER.into(),
                            raw_value: actor.to_string().into_bytes(),
                            ..Default::default()
                        }),
                        append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        Ok(res)
    }
}

#[async_trait]
impl ExtprocHandler for Handler {
    fn direction(&self) -> Direction {
        Direction::Ingress
    }

    async fn handle_request_headers(
        &self,
        md: &mut RequestMetadata,
    ) -> Result<HandlerResult, RouteError> {
        self.route(md, &RequestCtx::default())
            .await
            .map_err(|(e, _)| e)
    }
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
