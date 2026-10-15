//! A fake control plane, Actor certificates and request metadata for the
//! egress handler's tests.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use envoy_types::pb::envoy::config::core::v3::HeaderValue;
use envoy_types::pb::google::protobuf::Struct;
use futures::future::BoxFuture;
use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::api::EgressApi;
use super::identity::{RootsLoader, FORWARDED_CLIENT_CERT_HEADER};
use crate::router::extproc::metadata::string_value;
use crate::router::extproc::{
    RequestMetadata, EGRESS_FILTER_CHAIN_NAME, FILTER_CHAIN_NAME_ATTRIBUTE,
};
use crate::test_pki::{LeafOpts, TestCa};

pub const HIVE: &str = "default";
pub const ACTOR: &str = "my-actor";
pub const ACTOR_UID: &str = "1b4e28ba-2fa1-11d2-883f-0016d3cca427";
pub const WORKER_FOR_ACTOR_ID: &str =
    "spiffe://hivekeep-actor.local/worker-for-actor/default/my-actor";

#[derive(Default)]
pub struct MockApi {
    pub actor: Mutex<Option<pb::Actor>>,
    pub err: Mutex<Option<tonic::Status>>,
    pub policy: Mutex<Option<pb::EgressPolicy>>,
    pub policy_err: Mutex<Option<tonic::Status>>,
    pub policy_calls: AtomicU32,
    pub policy_gate: Option<Arc<Notify>>,
    pub mint_calls: AtomicU32,
    pub last_mint: Mutex<Option<pb::MintActorJwtRequest>>,
    pub mint_err: Mutex<Option<tonic::Status>>,
}

impl MockApi {
    pub fn with(actor: Option<pb::Actor>, policy: Option<pb::EgressPolicy>) -> Arc<MockApi> {
        Arc::new(MockApi {
            actor: Mutex::new(actor),
            policy: Mutex::new(policy),
            ..Default::default()
        })
    }
}

pub struct Api(pub Arc<MockApi>);

impl EgressApi for Api {
    fn get_actor(
        &self,
        _: pb::GetActorRequest,
    ) -> BoxFuture<'static, Result<pb::Actor, tonic::Status>> {
        let m = self.0.clone();
        Box::pin(async move {
            if let Some(e) = m.err.lock().clone() {
                return Err(e);
            }
            Ok(m.actor.lock().clone().unwrap_or_default())
        })
    }

    fn get_actor_egress_policy(
        &self,
        req: pb::GetActorEgressPolicyRequest,
    ) -> BoxFuture<'static, Result<pb::EgressPolicy, tonic::Status>> {
        let m = self.0.clone();
        Box::pin(async move {
            m.policy_calls.fetch_add(1, Ordering::SeqCst);
            if req.name != SINGLETON_NAME {
                return Err(tonic::Status::invalid_argument(format!(
                    "name must be {SINGLETON_NAME:?}"
                )));
            }
            if let Some(g) = &m.policy_gate {
                g.notified().await;
            }
            if let Some(e) = m.policy_err.lock().clone() {
                return Err(e);
            }
            m.policy
                .lock()
                .clone()
                .ok_or_else(|| tonic::Status::not_found("EgressPolicy not found"))
        })
    }

    fn mint_actor_jwt(
        &self,
        req: pb::MintActorJwtRequest,
    ) -> BoxFuture<'static, Result<pb::MintActorJwtResponse, tonic::Status>> {
        let m = self.0.clone();
        Box::pin(async move {
            let n = m.mint_calls.fetch_add(1, Ordering::SeqCst) + 1;
            *m.last_mint.lock() = Some(req.clone());
            if let Some(e) = m.mint_err.lock().clone() {
                return Err(e);
            }
            let exp = SystemTime::now() + Duration::from_secs(req.expiration_seconds.max(0) as u64);
            let d = exp.duration_since(SystemTime::UNIX_EPOCH).unwrap();
            Ok(pb::MintActorJwtResponse {
                actor_jwt: format!("jwt-{n}"),
                expire_time: Some(prost_types::Timestamp {
                    seconds: d.as_secs() as i64,
                    nanos: d.subsec_nanos() as i32,
                }),
            })
        })
    }
}

pub fn running_actor() -> pb::Actor {
    pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: HIVE.into(),
            name: ACTOR.into(),
            uid: ACTOR_UID.into(),
            ..Default::default()
        }),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Running as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn hosts(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| s.to_string()).collect()
}

pub fn ports(n: &[i32]) -> Option<pb::Ports> {
    Some(pb::Ports {
        all: None,
        numbers: n.to_vec(),
    })
}

pub fn all_ports() -> Option<pb::Ports> {
    Some(pb::Ports {
        all: Some(pb::AllPorts {}),
        numbers: vec![],
    })
}

pub fn http_policy_on(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            http: Some(pb::HttpRule {
                hostnames: hosts(p),
                ports,
                effects: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn http_policy(p: &[&str]) -> pb::EgressPolicy {
    http_policy_on(None, p)
}

pub fn https_policy_on(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: hosts(p),
                ports,
                effects: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn https_policy(p: &[&str]) -> pb::EgressPolicy {
    https_policy_on(None, p)
}

pub fn passthrough_policy(ports: Option<pb::Ports>, p: &[&str]) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: vec![pb::EgressRule {
            tls_passthrough: Some(pb::TlsPassthroughRule {
                hostnames: hosts(p),
                ports,
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn combined(ps: Vec<pb::EgressPolicy>) -> pb::EgressPolicy {
    pb::EgressPolicy {
        rules: ps.into_iter().flat_map(|p| p.rules).collect(),
        ..Default::default()
    }
}

pub fn allow_all_policy() -> pb::EgressPolicy {
    combined(vec![
        http_policy_on(all_ports(), &["*"]),
        https_policy(&["*"]),
    ])
}

pub fn loader(ca: &Arc<Mutex<TestCa>>) -> RootsLoader {
    let ca = ca.clone();
    Arc::new(move || Ok(ca.lock().roots()))
}

pub fn actor_leaf(ca: &TestCa, mutate: impl FnOnce(&mut LeafOpts)) -> Vec<u8> {
    let mut o = LeafOpts {
        uri: Some(WORKER_FOR_ACTOR_ID.into()),
        client: true,
        backdate: Some(Duration::from_secs(300)),
        ..Default::default()
    };
    mutate(&mut o);
    ca.issue(&o).der
}

pub fn xfcc_header(chain: &[&[u8]]) -> String {
    let mut pem = String::new();
    for der in chain {
        pem.push_str(&hivekeep_identity::pem::encode(
            hivekeep_identity::pem::CERTIFICATE,
            der,
        ));
    }
    format!("By=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-egress;Hash=abc123;Chain=\"{}\"", path_escape(&pem))
}

/// Escapes like a URL path segment (spaces and reserved bytes encoded).
pub fn path_escape(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub fn raw(k: &str, v: &str) -> HeaderValue {
    HeaderValue {
        key: k.into(),
        raw_value: v.as_bytes().to_vec(),
        ..Default::default()
    }
}

pub fn attrs(kv: &[(&str, &str)]) -> HashMap<String, Struct> {
    [(
        "envoy.filters.http.ext_proc".to_string(),
        Struct {
            fields: kv
                .iter()
                .map(|(k, v)| (k.to_string(), string_value(*v)))
                .collect(),
        },
    )]
    .into()
}

pub fn egress_metadata(xfcc: &str) -> RequestMetadata {
    let mut h = vec![
        raw(":method", "CONNECT"),
        raw(":authority", "93.184.216.34:80"),
    ];
    if !xfcc.is_empty() {
        h.push(raw(FORWARDED_CLIENT_CERT_HEADER, xfcc));
    }
    RequestMetadata::new(
        &h,
        attrs(&[(FILTER_CHAIN_NAME_ATTRIBUTE, EGRESS_FILTER_CHAIN_NAME)]),
    )
}
