//! Refusing requests that carry fields this binary has no descriptor for.
//!
//! Generated messages drop unknown fields when they decode, so the check
//! runs on the wire bytes, before the service decodes them:
//! [`RejectUnknownFieldsLayer`] reads each gRPC request whose method is in
//! the descriptor pool, decodes it as a dynamic message and answers
//! `InvalidArgument` naming every message that carried an unknown field, at
//! any level of nesting. Compressed request frames are passed through
//! unchecked.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use hivekeep_core::field::{ErrorList, FieldError, Path};
use http_body_util::BodyExt;
use prost_reflect::{DynamicMessage, MapKey, MessageDescriptor, ReflectMessage, Value};
use tonic::Status;
use tower::{Layer, Service};

use crate::protoredact;

/// Reports an error for every message in `msg`, at any depth, that carries
/// unknown fields. An unknown field on `msg` itself is reported at
/// `request`; several on one message collapse to one error naming the
/// first tag.
pub fn find_unknown_fields(msg: &DynamicMessage) -> ErrorList {
    let mut errs = ErrorList::new();
    walk(msg, &Path::root(), &mut errs);
    errs
}

fn walk(msg: &DynamicMessage, path: &Path, errs: &mut ErrorList) {
    if let Some(first) = msg.unknown_fields().next() {
        let at = if path.is_root() {
            Path::new(["request"])
        } else {
            path.clone()
        };
        errs.push(FieldError::invalid_opaque(
            &at,
            format!("unknown field with protobuf tag {}", first.number()),
        ));
    }
    for (fd, value) in msg.fields() {
        let p = path.child(fd.name());
        match value {
            Value::Message(m) => walk(m, &p, errs),
            Value::List(items) => {
                for (i, v) in items.iter().enumerate() {
                    if let Value::Message(m) = v {
                        walk(m, &p.index(i), errs);
                    }
                }
            }
            Value::Map(entries) => {
                let mut keys: Vec<&MapKey> = entries.keys().collect();
                keys.sort();
                for k in keys {
                    if let Value::Message(m) = &entries[k] {
                        walk(m, &p.key(&map_key_text(k)), errs);
                    }
                }
            }
            _ => {}
        }
    }
}

fn map_key_text(k: &MapKey) -> String {
    match k {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(v) => v.to_string(),
        MapKey::I64(v) => v.to_string(),
        MapKey::U32(v) => v.to_string(),
        MapKey::U64(v) => v.to_string(),
        MapKey::String(s) => s.clone(),
    }
}

/// [`find_unknown_fields`] on an encoded message of type `desc`. A message
/// that does not decode is reported at `request`.
pub fn find_unknown_fields_encoded(desc: &MessageDescriptor, encoded: &[u8]) -> ErrorList {
    match DynamicMessage::decode(desc.clone(), encoded) {
        Ok(m) => find_unknown_fields(&m),
        Err(e) => FieldError::internal(&Path::new(["request"]), e.to_string()).into(),
    }
}

/// The status a rejected request gets.
pub fn rejection_status(errs: &ErrorList) -> Status {
    Status::invalid_argument(errs.message())
}

/// Checks a buffered gRPC request body (length-prefixed frames) against
/// `desc`. Compressed frames are skipped.
pub fn check_grpc_body(desc: &MessageDescriptor, mut body: &[u8]) -> Result<(), Status> {
    while body.len() >= 5 {
        let compressed = body[0] == 1;
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        body.advance(5);
        if body.len() < len {
            return Ok(());
        }
        if !compressed {
            let errs = find_unknown_fields_encoded(desc, &body[..len]);
            if !errs.is_empty() {
                return Err(rejection_status(&errs));
            }
        }
        body.advance(len);
    }
    Ok(())
}

/// The tower layer applying the check to a gRPC server.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectUnknownFieldsLayer;

impl<S> Layer<S> for RejectUnknownFieldsLayer {
    type Service = RejectUnknownFields<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RejectUnknownFields { inner }
    }
}

/// The service [`RejectUnknownFieldsLayer`] builds.
#[derive(Debug, Clone)]
pub struct RejectUnknownFields<S> {
    inner: S,
}

impl<S, ResBody> Service<http::Request<tonic::body::Body>> for RejectUnknownFields<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        // Only unary requests are read whole: a client stream ends when the
        // caller says so, and buffering it would hold the call open forever.
        let Some(method) =
            protoredact::method_descriptor(req.uri().path()).filter(|m| !m.is_client_streaming())
        else {
            return Box::pin(inner.call(req));
        };
        let desc = method.input();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes: Bytes = match body.collect().await {
                Ok(c) => c.to_bytes(),
                Err(e) => return Ok(Status::internal(format!("read request: {e}")).into_http()),
            };
            if let Err(status) = check_grpc_body(&desc, &bytes) {
                return Ok(status.into_http());
            }
            let req = http::Request::from_parts(
                parts,
                tonic::body::Body::new(http_body_util::Full::new(bytes)),
            );
            inner.call(req).await
        })
    }
}

/// Checks an already decoded dynamic request; for services that receive
/// dynamic messages directly.
pub fn reject_unknown_fields(msg: &DynamicMessage) -> Result<(), Status> {
    let errs = find_unknown_fields(msg);
    if errs.is_empty() {
        Ok(())
    } else {
        tracing::debug!(
            message = msg.descriptor().full_name(),
            "rejected unknown fields"
        );
        Err(rejection_status(&errs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_proto::api::v1 as api;
    use prost::Message;

    /// A varint field no descriptor declares, standing in for a field added
    /// by a newer client.
    fn unknown_field(num: u32) -> Vec<u8> {
        let mut b = Vec::new();
        prost::encoding::encode_key(num, prost::encoding::WireType::Varint, &mut b);
        prost::encoding::encode_varint(42, &mut b);
        b
    }

    fn desc(name: &str) -> MessageDescriptor {
        protoredact::message_descriptor(name).unwrap()
    }

    /// `msg` encoded, with `extra` appended to the encoding of the
    /// sub-message at `field_path` (field numbers, outermost first).
    fn encode_with_unknown(msg: &DynamicMessage, path: &[&str], extra: &[u8]) -> DynamicMessage {
        let mut m = msg.clone();
        inject(&mut m, path, extra);
        m
    }

    fn inject(m: &mut DynamicMessage, path: &[&str], extra: &[u8]) {
        if path.is_empty() {
            let mut bytes = m.encode_to_vec();
            bytes.extend_from_slice(extra);
            *m = DynamicMessage::decode(m.descriptor(), bytes.as_slice()).unwrap();
            return;
        }
        let (head, rest) = (path[0], &path[1..]);
        let (name, idx) = match head.split_once('#') {
            Some((n, i)) => (n, Some(i.parse::<usize>().unwrap())),
            None => (head, None),
        };
        let v = m.get_field_by_name_mut(name).unwrap();
        match (v, idx) {
            (Value::Message(sub), None) => inject(sub, rest, extra),
            (Value::List(items), Some(i)) => {
                if let Value::Message(sub) = &mut items[i] {
                    inject(sub, rest, extra)
                }
            }
            _ => panic!("bad path {head}"),
        }
    }

    fn dynamic<M: Message>(name: &str, m: &M) -> DynamicMessage {
        protoredact::to_dynamic(&desc(name), m)
    }

    fn paths(errs: &ErrorList) -> Vec<String> {
        errs.iter().map(|e| e.path.as_str().to_string()).collect()
    }

    fn create_actor() -> api::CreateActorRequest {
        api::CreateActorRequest {
            actor: Some(api::Actor {
                metadata: Some(api::ResourceMetadata {
                    hive: "team-a".into(),
                    name: "actor-1".into(),
                    ..Default::default()
                }),
                actor_template: Some(api::ObjectRef {
                    hive: "team-a".into(),
                    name: "tmpl1".into(),
                }),
                worker_selector: Some(api::Selector {
                    match_labels: [("tier".to_string(), "paid".to_string())].into(),
                }),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn find_unknown_fields_table() {
        let base = dynamic("hivekeep.api.v1.CreateActorRequest", &create_actor());
        let u = |n| unknown_field(n);
        let cases: Vec<(&str, DynamicMessage, Vec<&str>)> = vec![
            ("no unknown fields", base.clone(), vec![]),
            (
                "empty message",
                dynamic(
                    "hivekeep.api.v1.CreateActorRequest",
                    &api::CreateActorRequest::default(),
                ),
                vec![],
            ),
            (
                "unknown field on the request itself",
                encode_with_unknown(&base, &[], &u(9999)),
                vec!["request"],
            ),
            (
                "unknown field on a nested message",
                encode_with_unknown(&base, &["actor", "metadata"], &u(9999)),
                vec!["actor.metadata"],
            ),
            (
                "unknown fields on several messages",
                {
                    let m = encode_with_unknown(&base, &["actor", "metadata"], &u(9998));
                    let m = encode_with_unknown(&m, &["actor", "worker_selector"], &u(9997));
                    encode_with_unknown(&m, &[], &u(9999))
                },
                vec!["request", "actor.metadata", "actor.worker_selector"],
            ),
            (
                "several unknown fields on one message collapse to a single error",
                {
                    let mut extra = u(9998);
                    extra.extend(u(9999));
                    encode_with_unknown(
                        &dynamic("hivekeep.api.v1.Actor", &api::Actor::default()),
                        &[],
                        &extra,
                    )
                },
                vec!["request"],
            ),
            (
                "unknown field inside a repeated message element",
                encode_with_unknown(
                    &dynamic(
                        "hivekeep.api.v1.ListActorsResponse",
                        &api::ListActorsResponse {
                            actors: vec![
                                api::Actor {
                                    actor_template: Some(api::ObjectRef {
                                        name: "tmpl1".into(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                api::Actor {
                                    actor_template: Some(api::ObjectRef {
                                        name: "tmpl2".into(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        },
                    ),
                    &["actors#1"],
                    &u(9999),
                ),
                vec!["actors[1]"],
            ),
            (
                "unknown field inside a map value message",
                {
                    let st = prost_reflect::DescriptorPool::global()
                        .get_message_by_name("google.protobuf.Struct")
                        .unwrap();
                    let val = prost_reflect::DescriptorPool::global()
                        .get_message_by_name("google.protobuf.Value")
                        .unwrap();
                    let mut v = DynamicMessage::new(val);
                    v.set_field_by_name("string_value", Value::String("x".into()));
                    let v = encode_with_unknown(&v, &[], &u(9999));
                    let mut s = DynamicMessage::new(st);
                    s.set_field_by_name(
                        "fields",
                        Value::Map([(MapKey::String("amd64".into()), Value::Message(v))].into()),
                    );
                    s
                },
                vec!["fields[amd64]"],
            ),
            (
                "scalar map values cannot carry unknown fields",
                dynamic(
                    "hivekeep.api.v1.Selector",
                    &api::Selector {
                        match_labels: [
                            ("tier".to_string(), "paid".to_string()),
                            ("region".to_string(), "us".to_string()),
                        ]
                        .into(),
                    },
                ),
                vec![],
            ),
        ];
        for (name, msg, want) in cases {
            let got = find_unknown_fields(&msg);
            assert_eq!(paths(&got), want, "{name}");
            assert!(
                got.iter()
                    .all(|e| e.kind == hivekeep_core::field::ErrorKind::Invalid),
                "{name}"
            );
        }
    }

    fn frame(msg: &[u8]) -> Vec<u8> {
        let mut out = vec![0];
        out.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        out.extend_from_slice(msg);
        out
    }

    #[test]
    fn reject_unknown_fields_unary() {
        let d = desc("hivekeep.api.v1.CreateActorRequest");
        // A clean request passes.
        let clean = create_actor().encode_to_vec();
        assert!(check_grpc_body(&d, &frame(&clean)).is_ok());
        // An unknown field is rejected with the field path and tag.
        let mut bad = api::CreateActorRequest::default().encode_to_vec();
        bad.extend(unknown_field(9999));
        let st = check_grpc_body(&d, &frame(&bad)).unwrap_err();
        assert_eq!(st.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            st.message(),
            "request: Invalid value: unknown field with protobuf tag 9999"
        );
        // A compressed frame is passed through.
        let mut compressed = frame(&bad);
        compressed[0] = 1;
        assert!(check_grpc_body(&d, &compressed).is_ok());
        // Dynamic requests are checked directly.
        let m = DynamicMessage::decode(d.clone(), bad.as_slice()).unwrap();
        assert!(reject_unknown_fields(&m).is_err());
    }

    #[tokio::test]
    async fn layer_rejects_before_the_service_and_passes_unknown_methods() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let inner = tower::service_fn(move |_req: http::Request<tonic::body::Body>| {
            c.fetch_add(1, Ordering::SeqCst);
            async {
                Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::default()))
            }
        });
        let mut svc = RejectUnknownFieldsLayer.layer(inner);
        let mut bad = api::CreateActorRequest::default().encode_to_vec();
        bad.extend(unknown_field(9999));
        let req = |path: &str, body: Vec<u8>| {
            http::Request::builder()
                .uri(format!("http://x{path}"))
                .body(tonic::body::Body::new(http_body_util::Full::new(
                    Bytes::from(body),
                )))
                .unwrap()
        };
        let resp = svc
            .call(req("/hivekeep.api.v1.Control/CreateActor", frame(&bad)))
            .await
            .unwrap();
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "3");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "the service ran despite the rejection"
        );
        svc.call(req(
            "/hivekeep.api.v1.Control/CreateActor",
            frame(&create_actor().encode_to_vec()),
        ))
        .await
        .unwrap();
        svc.call(req("/not.a.Known/Method", frame(&bad)))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
