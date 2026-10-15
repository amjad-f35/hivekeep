//! The egress handler. The CONNECT leg authenticates the Actor and opens
//! the tunnel (handing the dataplane the SNI rules for TLS inside it); the
//! request legs decide each decrypted or cleartext request against the
//! Actor's policy and inject credentials.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use envoy_types::pb::envoy::config::core::v3::{
    header_value_option::HeaderAppendAction, HeaderValue, HeaderValueOption,
};
use envoy_types::pb::envoy::service::ext_proc::v3::{
    CommonResponse, HeaderMutation, HeadersResponse,
};
use envoy_types::pb::google::protobuf::{value::Kind, ListValue, Struct, Value};
use hivekeep_core::resource::ActorRef;
use hivekeep_core::spiffe::{actor_ref_from_worker_for_actor_spiffe_id, actor_spiffe_id};
use hivekeep_proto::api::v1 as pb;
use tokio_util::sync::CancellationToken;

use super::actor_jwt::Minter;
use super::api::{CredentialProvider, EgressApi};
use super::identity::{authenticate, RootsLoader};
use super::policy_cache::{PolicyCache, PolicyError};
use super::provider::{provider_name_from_uri, sanitize_secret, validate_inject_header};
use crate::credprovider::v1 as cp;
use crate::egress_policy::{normalize_authority, Destination, Policy, SniRule};
use crate::router::extproc::{
    Direction, Handler as ExtprocHandler, HandlerResult, RequestMetadata, RouteError,
    ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE, AUTHORITY_HEADER,
    CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE, EGRESS_CLEARTEXT_FILTER_CHAIN_NAME,
    EGRESS_DIALED_PORT_KEY, EGRESS_DIAL_HOST_KEY, EGRESS_DIAL_KEY, EGRESS_DIAL_NAME,
    EGRESS_FILTER_CHAIN_NAME, EGRESS_METADATA_NAMESPACE, EGRESS_POLICY_JSON_KEY,
    EGRESS_POLICY_METADATA_NAMESPACE, EGRESS_SNI_RULES_KEY, EGRESS_SNI_RULE_MODE_KEY,
    EGRESS_SNI_RULE_PATTERN_KEY, EGRESS_TLS_MITM_FILTER_CHAIN_NAME, FILTER_CHAIN_NAME_ATTRIBUTE,
    REQUESTED_SERVER_NAME_ATTRIBUTE,
};

/// The body of every policy denial, so denials reveal nothing.
pub const DENIED_BODY: &str = "egress denied";
/// The body refusing WebSocket upgrades.
pub const WEBSOCKET_DENIED_BODY: &str = "WebSocket egress is not supported";
/// The body refusing CONNECT inside the tunnel.
pub const CONNECT_DENIED_BODY: &str = "CONNECT egress is not supported";
/// Budget for all credential lookups of one request.
const INJECTION_TIMEOUT: Duration = Duration::from_secs(4);

/// The egress gateway's request handler.
pub struct Handler {
    api: Arc<dyn EgressApi>,
    roots: Option<RootsLoader>,
    policies: PolicyCache,
    jwts: Minter,
    provider: Option<Arc<dyn CredentialProvider>>,
    provider_name: String,
}

fn s(v: impl Into<String>) -> Value {
    Value {
        kind: Some(Kind::StringValue(v.into())),
    }
}

fn st(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value {
        kind: Some(Kind::StructValue(Struct {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        })),
    }
}

fn allow() -> HandlerResult {
    HandlerResult {
        response: HeadersResponse {
            response: Some(CommonResponse::default()),
        },
        ..Default::default()
    }
}

/// The CONNECT leg's dynamic metadata: the dialed port, and the SNI rules
/// (as JSON) the dataplane's TLS dispatch consults.
fn connect_metadata(dest: &Destination, rules: &[SniRule]) -> Struct {
    let json = serde_json::json!({
        EGRESS_SNI_RULES_KEY: rules.iter().map(|r| serde_json::json!({
            EGRESS_SNI_RULE_PATTERN_KEY: r.pattern,
            EGRESS_SNI_RULE_MODE_KEY: r.mode.as_str(),
        })).collect::<Vec<_>>(),
    });
    Struct {
        fields: [
            (
                EGRESS_METADATA_NAMESPACE.to_string(),
                st([(EGRESS_DIALED_PORT_KEY, s(dest.port.to_string()))]),
            ),
            (
                EGRESS_POLICY_METADATA_NAMESPACE.to_string(),
                st([(EGRESS_POLICY_JSON_KEY, s(json.to_string()))]),
            ),
        ]
        .into(),
    }
}

fn metadata_answer(fields: &[(&'static str, String)]) -> Struct {
    Struct {
        fields: [(
            EGRESS_METADATA_NAMESPACE.to_string(),
            st(fields.iter().map(|(k, v)| (*k, s(v.clone())))),
        )]
        .into(),
    }
}

/// Values of a list, for tests and callers.
pub fn list(values: Vec<Value>) -> Value {
    Value {
        kind: Some(Kind::ListValue(ListValue { values })),
    }
}

fn dial_host(dest: &Destination) -> String {
    match dest.ip {
        _ if !dest.hostname.is_empty() => dest.hostname.clone(),
        Some(IpAddr::V6(v6)) => format!("[{v6}]"),
        Some(ip) => ip.to_string(),
        None => String::new(),
    }
}

fn has_token(value: &str, want: &str) -> bool {
    value
        .split(',')
        .any(|t| t.trim().eq_ignore_ascii_case(want))
}

fn is_websocket_upgrade(md: &RequestMetadata) -> bool {
    md.method == "GET"
        && has_token(md.header("connection"), "upgrade")
        && has_token(md.header("upgrade"), "websocket")
}

/// The connection's destination as named by SNI, on the dialed port.
fn connection_destination(sni: &str, port: u16) -> Destination {
    match normalize_authority(sni) {
        Ok(d) if !d.hostname.is_empty() => Destination::host(d.hostname).with_port(port),
        _ => Destination::default().with_port(port),
    }
}

/// The request's destination: its authority (which must agree with Host)
/// on the port the CONNECT dialed.
fn request_destination(md: &RequestMetadata) -> Result<Destination, String> {
    let host = md.header("host");
    let authority = match md.header(AUTHORITY_HEADER) {
        "" => host,
        a => a,
    };
    let named = normalize_authority(authority).map_err(|e| e.to_string())?;
    if !host.is_empty() && host != authority {
        let from_host = normalize_authority(host).ok();
        if from_host.is_none_or(|h| h != named) {
            return Err(format!(
                "request :authority {authority:?} and Host {host:?} name different destinations"
            ));
        }
    }
    let mut dest = Destination {
        hostname: named.hostname,
        ip: named.ip,
        port: 0,
    };
    let raw = md.attribute(CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE);
    if !raw.is_empty() {
        match normalize_authority(&raw) {
            Ok(d) if d.ip.is_some() && d.port != 0 => dest.port = d.port,
            _ => return Err(format!("CONNECT authority {raw:?} is not an IP:port")),
        }
    }
    Ok(dest)
}

fn actor_from_filter_state(md: &RequestMetadata) -> Result<ActorRef, String> {
    let id = md.attribute(ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE);
    if id.is_empty() {
        return Err("no actor identity in filter state".into());
    }
    actor_ref_from_worker_for_actor_spiffe_id(&id).map_err(|e| e.to_string())
}

fn denied() -> RouteError {
    RouteError::req(403, DENIED_BODY)
}

fn wrap(status: u16, cause: impl std::fmt::Display, msg: &str) -> RouteError {
    RouteError::wrap(status, &RouteError::other(cause.to_string()), msg)
}

fn map_status(status: u16, s: &tonic::Status, msg: &str) -> RouteError {
    RouteError::wrap(status, &RouteError::from(s.clone()), msg)
}

fn map_credential_provider_error(s: &tonic::Status) -> RouteError {
    match s.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => map_status(503, s, DENIED_BODY),
        _ => map_status(403, s, DENIED_BODY),
    }
}

fn map_actor_jwt_error(s: &tonic::Status) -> RouteError {
    match s.code() {
        tonic::Code::NotFound => map_status(403, s, DENIED_BODY),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => map_status(503, s, DENIED_BODY),
        _ => map_status(500, s, DENIED_BODY),
    }
}

fn map_egress_identity_error(actor: &ActorRef, s: &tonic::Status) -> RouteError {
    let (hive, name) = (&actor.hive, &actor.name);
    match s.code() {
        tonic::Code::NotFound => map_status(
            403,
            s,
            &format!("egress denied: unknown actor {hive:?}/{name:?}"),
        ),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => map_status(
            503,
            s,
            &format!(
                "egress identity check unavailable for {hive:?}/{name:?}: {}",
                s.message()
            ),
        ),
        _ => map_status(
            403,
            s,
            &format!("egress denied for {hive:?}/{name:?}: {}", s.message()),
        ),
    }
}

impl Handler {
    /// A handler. `roots` loads the Actor identity CAs (CONNECT legs
    /// fail with 503 without it); `provider` serves credential URIs naming
    /// `provider_name` (any name when empty).
    pub fn new(
        api: Arc<dyn EgressApi>,
        roots: Option<RootsLoader>,
        policy_cache_ttl: Duration,
        provider: Option<Arc<dyn CredentialProvider>>,
        provider_name: impl Into<String>,
    ) -> Self {
        Handler {
            policies: PolicyCache::new(api.clone(), policy_cache_ttl),
            jwts: Minter::new(api.clone()),
            api,
            roots,
            provider,
            provider_name: provider_name.into(),
        }
    }

    /// Decides one request, abandoning lookups when `cancel` fires.
    pub async fn handle(
        &self,
        md: &RequestMetadata,
        cancel: &CancellationToken,
    ) -> Result<HandlerResult, RouteError> {
        match md.attribute(FILTER_CHAIN_NAME_ATTRIBUTE).as_str() {
            leg @ (EGRESS_FILTER_CHAIN_NAME | "") => self.handle_connect(md, leg, cancel).await,
            leg @ (EGRESS_TLS_MITM_FILTER_CHAIN_NAME | EGRESS_CLEARTEXT_FILTER_CHAIN_NAME) => {
                self.handle_request(md, leg, cancel).await
            }
            leg => Err(RouteError::req(
                404,
                format!("egress denied: this gateway does not serve filter chain {leg:?}"),
            )),
        }
    }

    async fn handle_connect(
        &self,
        md: &RequestMetadata,
        leg: &str,
        cancel: &CancellationToken,
    ) -> Result<HandlerResult, RouteError> {
        if !md.method.eq_ignore_ascii_case("CONNECT") {
            return Err(RouteError::req(
                405,
                format!("egress denied: expected CONNECT, got {:?}", md.method),
            ));
        }
        let roots = match &self.roots {
            Some(load) => load(),
            None => Err("no actor-identity CA configured".into()),
        }
        .map_err(|e| {
            wrap(
                503,
                e,
                "egress unavailable: actor-identity CA failed to load",
            )
        })?;
        let actor = authenticate(md, roots).map_err(|e| {
            tracing::warn!(err = %e, "egress denied: actor certificate rejected");
            wrap(403, e, "egress denied: invalid actor certificate")
        })?;
        self.validate_actor(&actor, &md.host).await?;
        let dest = match normalize_authority(&md.host) {
            Ok(d) if d.ip.is_some() && d.port != 0 => d,
            _ => {
                tracing::warn!(actor = %actor, leg, authority = %md.host, "egress denied: CONNECT authority is not an IP:port");
                return Err(denied());
            }
        };
        let policy = self.lookup_policy(leg, &actor, cancel).await?;
        let rules = policy.sni_rules(dest.port);
        tracing::info!(actor = %actor, leg, destination = %md.host, sni_rules = rules.len(), "egress tunnel opened: requests inside it are decided one by one");
        let mut res = allow();
        res.dynamic_metadata = Some(connect_metadata(&dest, &rules));
        Ok(res)
    }

    async fn validate_actor(&self, actor: &ActorRef, destination: &str) -> Result<(), RouteError> {
        let a = self
            .api
            .get_actor(pb::GetActorRequest {
                actor: Some(actor.to_object_ref()),
            })
            .await
            .map_err(|s| map_egress_identity_error(actor, &s))?;
        let state = a.status.map(|s| s.state).unwrap_or_default();
        match pb::ActorState::try_from(state).unwrap_or(pb::ActorState::Unspecified) {
            pb::ActorState::Running | pb::ActorState::Resuming => Ok(()),
            other => {
                tracing::warn!(actor = %actor, state = ?other, destination, "egress denied: actor is not placed on a worker");
                Err(RouteError::req(
                    403,
                    format!(
                        "egress denied: actor {:?}/{:?} is {}, not placed on a worker",
                        actor.hive,
                        actor.name,
                        other.as_str_name()
                    ),
                ))
            }
        }
    }

    async fn lookup_policy(
        &self,
        leg: &str,
        actor: &ActorRef,
        cancel: &CancellationToken,
    ) -> Result<Arc<Policy>, RouteError> {
        let got = tokio::select! {
            r = self.policies.get(actor) => r,
            _ = cancel.cancelled() => {
                tracing::debug!(actor = %actor, leg, "egress policy lookup abandoned by the caller");
                return Err(RouteError { kind: crate::router::extproc::ErrorKind::Canceled, ..wrap(408, "context canceled", "egress request canceled") });
            }
        };
        match got {
            Err(PolicyError::NoPolicy) => {
                tracing::warn!(actor = %actor, leg, "egress denied: actor has no egress policy");
                Err(wrap(403, "actor has no egress policy", DENIED_BODY))
            }
            Err(PolicyError::Fetch(s)) => {
                tracing::error!(actor = %actor, leg, err = %s.message(), "egress policy lookup failed");
                Err(map_status(
                    503,
                    &s,
                    "egress unavailable: policy lookup failed",
                ))
            }
            Ok(p) if p.rule_count() == 0 => {
                tracing::warn!(actor = %actor, leg, "egress denied: actor's egress policy has no rules");
                Err(denied())
            }
            Ok(p) => Ok(p),
        }
    }

    async fn handle_request(
        &self,
        md: &RequestMetadata,
        leg: &str,
        cancel: &CancellationToken,
    ) -> Result<HandlerResult, RouteError> {
        if is_websocket_upgrade(md) {
            tracing::warn!(leg, host = %md.host, "egress denied: WebSocket upgrades are not supported");
            return Err(RouteError::req(403, WEBSOCKET_DENIED_BODY));
        }
        if md.method.eq_ignore_ascii_case("CONNECT") {
            tracing::warn!(leg, authority = %md.host, "egress denied: CONNECT inside the egress tunnel is not supported");
            return Err(RouteError::req(403, CONNECT_DENIED_BODY));
        }
        let actor = actor_from_filter_state(md).map_err(|e| {
            tracing::warn!(leg, err = %e, "egress denied: request carries no actor identity");
            wrap(403, e, DENIED_BODY)
        })?;
        let dest = request_destination(md).map_err(|e| {
            tracing::warn!(actor = %actor, leg, err = %e, "egress denied: request names no destination a rule could allow");
            wrap(403, e, DENIED_BODY)
        })?;
        let policy = self.lookup_policy(leg, &actor, cancel).await?;
        let decrypted = leg == EGRESS_TLS_MITM_FILTER_CHAIN_NAME;
        let sni = md.attribute(REQUESTED_SERVER_NAME_ATTRIBUTE);
        if decrypted
            && !policy
                .evaluate_request(&connection_destination(&sni, dest.port), true)
                .allowed
        {
            tracing::warn!(actor = %actor, leg, host = %md.host, sni = %sni, "egress denied: no https rule covers the connection's SNI");
            return Err(denied());
        }
        let decision = policy.evaluate_request(&dest, decrypted);
        if !decision.allowed {
            tracing::warn!(actor = %actor, leg, host = %md.host, dialed_port = dest.port, sni = %sni, "egress denied: no rule allows the destination");
            return Err(denied());
        }
        let injected = match tokio::time::timeout(
            INJECTION_TIMEOUT,
            self.apply_effects(&actor, &dest, md, decision.effects.as_ref()),
        )
        .await
        {
            Ok(r) => r?,
            Err(_) => return Err(wrap(503, "credential lookup timed out", DENIED_BODY)),
        };
        tracing::debug!(actor = %actor, leg, rule = ?decision.rule_index, injected_headers = injected.len(), "egress allowed");
        let mut res = allow();
        let common = res
            .response
            .response
            .as_mut()
            .expect("allow sets a common response");
        common.clear_route_cache = true;
        common.header_mutation = Some(HeaderMutation {
            set_headers: injected,
            ..Default::default()
        });
        res.dynamic_metadata = Some(metadata_answer(&[
            (EGRESS_DIAL_KEY, EGRESS_DIAL_NAME.to_string()),
            (EGRESS_DIAL_HOST_KEY, dial_host(&dest)),
        ]));
        Ok(res)
    }

    async fn apply_effects(
        &self,
        actor: &ActorRef,
        dest: &Destination,
        md: &RequestMetadata,
        effects: Option<&pb::HttpRuleEffects>,
    ) -> Result<Vec<HeaderValueOption>, RouteError> {
        let Some(effects) = effects else {
            return Ok(Vec::new());
        };
        let injections: Vec<&pb::CredentialHeader> = effects
            .replace_headers
            .iter()
            .filter(|i| md.headers.contains_key(&i.header.to_ascii_lowercase()))
            .collect();
        let mut out = Vec::with_capacity(injections.len());
        for inj in injections {
            if let Err(e) = validate_inject_header(&inj.header) {
                tracing::error!(actor = %actor, host = %dest.hostname, header = %inj.header, err = %e, "egress denied: policy names an unusable injection header");
                return Err(wrap(500, e, DENIED_BODY));
            }
            let secret = match &inj.actor_jwt {
                Some(src) => self.jwts.token(actor, src).await.map(String::into_bytes).map_err(|s| {
                    tracing::error!(actor = %actor, host = %dest.hostname, audiences = ?src.audiences, err = %s.message(), "egress denied: Actor JWT mint failed");
                    map_actor_jwt_error(&s)
                })?,
                None => self.provider_credential(actor, dest, &inj.credential_uri).await?,
            };
            let mut value = inj.prefix.clone().into_bytes();
            value.extend_from_slice(&secret);
            out.push(HeaderValueOption {
                header: Some(HeaderValue {
                    key: inj.header.clone(),
                    raw_value: value,
                    ..Default::default()
                }),
                append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                ..Default::default()
            });
        }
        Ok(out)
    }

    async fn provider_credential(
        &self,
        actor: &ActorRef,
        dest: &Destination,
        uri: &str,
    ) -> Result<Vec<u8>, RouteError> {
        let Some(provider) = &self.provider else {
            tracing::error!(actor = %actor, host = %dest.hostname, "egress denied: policy requires credential injection but no credential provider is configured");
            return Err(RouteError::req(500, DENIED_BODY));
        };
        if !self.provider_name.is_empty() {
            let name = provider_name_from_uri(uri).map_err(|e| {
                tracing::error!(actor = %actor, uri, err = %e, "egress denied: policy names an unparseable credential URI");
                wrap(500, e, DENIED_BODY)
            })?;
            if name != self.provider_name {
                tracing::error!(actor = %actor, uri, provider = %name, serves = %self.provider_name, "egress denied: credential URI names a provider this gateway does not serve");
                return Err(RouteError::req(500, DENIED_BODY));
            }
        }
        let resp = provider
            .fetch_secret(cp::FetchSecretRequest {
                uri: uri.to_string(),
                actor_spiffe_id: actor_spiffe_id(actor),
            })
            .await
            .map_err(|s| {
                tracing::error!(actor = %actor, uri, err = %s.message(), "egress denied: credential fetch failed");
                map_credential_provider_error(&s)
            })?;
        sanitize_secret(&resp.opaque_bytes).map_err(|e| {
            tracing::error!(actor = %actor, uri, err = %e, "egress denied: unusable credential");
            wrap(503, e, DENIED_BODY)
        })
    }
}

#[async_trait]
impl ExtprocHandler for Handler {
    fn direction(&self) -> Direction {
        Direction::Egress
    }

    async fn handle_request_headers(
        &self,
        md: &mut RequestMetadata,
    ) -> Result<HandlerResult, RouteError> {
        self.handle(md, &CancellationToken::new()).await
    }
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
