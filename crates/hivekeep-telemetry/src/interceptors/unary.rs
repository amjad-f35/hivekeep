//! Logging and status mapping around unary handlers.
//!
//! [`server_unary`] is for the public API: a handler's [`super::ApiError`]
//! reaches the caller with its code, and any other error, including a
//! status from an upstream service, becomes `Internal` with a generic
//! prefix. [`internal_server_unary`] is for internal services: the same
//! mapping, with the full error text. Both log one line per call with the
//! method, the request and response (with `debug_redact` fields masked; see
//! [`crate::protoredact`]), the error, and the elapsed time.

use std::future::Future;
use std::time::{Duration, Instant};

use prost_reflect::MessageDescriptor;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use super::error::{chain_text, from_error, BoxError};
use crate::contextlogging::ProtoValue;
use crate::protoredact;

/// Carries the server's handler duration in microseconds, so clients can
/// report a latency unaffected by their own scheduling overhead. Sent as
/// response metadata on success and with the status on failure.
pub const SERVER_ELAPSED_TRAILER: &str = "x-server-elapsed-us";

/// The caller's identity as the authentication layer wants it logged.
/// Insert it into the request extensions; the wrappers log it with the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedPrincipal(pub String);

fn describe<M: prost::Message>(desc: Option<MessageDescriptor>, msg: &M) -> ProtoValue {
    match desc {
        Some(d) => ProtoValue::of(&d, msg),
        // Without a schema the fields cannot be checked for secrets, so
        // only the size is logged.
        None => ProtoValue::text(format!("<{} bytes, schema unknown>", msg.encoded_len())),
    }
}

/// Keeps list responses out of the log. A page of a thousand Actors is tens
/// of kilobytes per call, and at scale those pages rotate the lifecycle
/// lines out of the container log. A failed call logs its absent response
/// as `null`.
pub fn resp_for_log<M: prost::Message>(
    method: &str,
    desc: Option<MessageDescriptor>,
    resp: Option<&M>,
) -> ProtoValue {
    let Some(resp) = resp else {
        return ProtoValue::none();
    };
    let name = method.rsplit('/').next().unwrap_or(method);
    if name.starts_with("List") {
        return ProtoValue::text(format!(
            "<{name} response elided, {} bytes>",
            resp.encoded_len()
        ));
    }
    describe(desc, resp)
}

fn elapsed_value(elapsed: Duration) -> MetadataValue<tonic::metadata::Ascii> {
    MetadataValue::from(elapsed.as_micros() as u64)
}

async fn run<Req, Resp, F, Fut>(
    method: &str,
    req: Request<Req>,
    handler: F,
) -> (
    Option<String>,
    ProtoValue,
    Result<Response<Resp>, BoxError>,
    Duration,
    ProtoValue,
)
where
    Req: prost::Message,
    Resp: prost::Message,
    F: FnOnce(Request<Req>) -> Fut,
    Fut: Future<Output = Result<Response<Resp>, BoxError>>,
{
    let method_desc = protoredact::method_descriptor(method);
    let principal = req
        .extensions()
        .get::<LoggedPrincipal>()
        .map(|p| p.0.clone());
    let req_log = describe(method_desc.as_ref().map(|m| m.input()), req.get_ref());
    let start = Instant::now();
    let result = handler(req).await;
    let elapsed = start.elapsed();
    let resp_log = resp_for_log(
        method,
        method_desc.as_ref().map(|m| m.output()),
        result.as_ref().ok().map(|r| r.get_ref()),
    );
    (principal, req_log, result, elapsed, resp_log)
}

/// Wraps a public API handler; see the module docs.
pub async fn server_unary<Req, Resp, F, Fut>(
    method: &str,
    req: Request<Req>,
    handler: F,
) -> Result<Response<Resp>, Status>
where
    Req: prost::Message,
    Resp: prost::Message,
    F: FnOnce(Request<Req>) -> Fut,
    Fut: Future<Output = Result<Response<Resp>, BoxError>>,
{
    let (principal, req_log, result, elapsed, resp_log) = run(method, req, handler).await;
    let err_text = result.as_ref().err().map(|e| chain_text(&**e));
    tracing::info!(
        method,
        req = %req_log,
        resp = %resp_log,
        err = err_text.as_deref(),
        elapsed_time = ?elapsed,
        principal = principal.as_deref(),
        "Handle RPC"
    );
    match result {
        Ok(mut resp) => {
            resp.metadata_mut()
                .insert(SERVER_ELAPSED_TRAILER, elapsed_value(elapsed));
            Ok(resp)
        }
        Err(err) => {
            let (status, api) = from_error(&*err);
            let mut status = if api {
                status
            } else {
                Status::internal(format!("internal server error: {}", chain_text(&*err)))
            };
            status
                .metadata_mut()
                .insert(SERVER_ELAPSED_TRAILER, elapsed_value(elapsed));
            Err(status)
        }
    }
}

/// Wraps an internal service handler; see the module docs.
pub async fn internal_server_unary<Req, Resp, F, Fut>(
    method: &str,
    req: Request<Req>,
    handler: F,
) -> Result<Response<Resp>, Status>
where
    Req: prost::Message,
    Resp: prost::Message,
    F: FnOnce(Request<Req>) -> Fut,
    Fut: Future<Output = Result<Response<Resp>, BoxError>>,
{
    let (_, req_log, result, elapsed, resp_log) = run(method, req, handler).await;
    let err_text = result.as_ref().err().map(|e| chain_text(&**e));
    tracing::info!(
        method,
        req = %req_log,
        resp = %resp_log,
        err = err_text.as_deref(),
        elapsed_time = ?elapsed,
        "Handle RPC"
    );
    result.map_err(|err| from_error(&*err).0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use crate::interceptors::error::{wrap, ApiError, ContextError};
    use crate::protoredact::PLACEHOLDER;
    use hivekeep_proto::{api::v1 as api, keeper::v1 as keeper};
    use tonic::Code;

    const METHOD: &str = "/test.Service/Method";

    fn ok_ref() -> Result<Response<api::ObjectRef>, BoxError> {
        Ok(Response::new(api::ObjectRef {
            hive: "h".into(),
            name: "response".into(),
        }))
    }

    /// Runs `f` on a current-thread runtime with log capture.
    fn block<T>(f: impl Future<Output = T>) -> (T, String) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut out = None;
        let log = capture(|| out = Some(rt.block_on(f)));
        (out.unwrap(), log)
    }

    #[test]
    fn status_error_interceptor() {
        type Case = (
            &'static str,
            Option<fn() -> BoxError>,
            Code,
            &'static str,
            bool,
        );
        let cases: Vec<Case> = vec![
            ("Success", None, Code::Ok, "", true),
            (
                "WrappedStatusBecomesInternal",
                Some(|| wrap("outer error", Status::not_found("actor not found"))),
                Code::Internal,
                "internal server error: outer error: ",
                false,
            ),
            (
                "RawErrorFallback",
                Some(|| "database connection failed".into()),
                Code::Internal,
                "internal server error: database connection failed",
                false,
            ),
            (
                "APIErrorInChain",
                Some(|| {
                    wrap(
                        "workflow failed at step Load",
                        ApiError::not_found("actor not found"),
                    )
                }),
                Code::NotFound,
                "actor not found",
                false,
            ),
            (
                "APIErrorWinsOverStatusItWraps",
                Some(|| {
                    Box::new(
                        ApiError::internal("while calling keeper: keeper down")
                            .with_source(Status::unavailable("keeper down")),
                    )
                }),
                Code::Internal,
                "while calling keeper: keeper down",
                false,
            ),
            (
                "ContextErrorKeepsItsCode",
                Some(|| wrap("while loading actor", ContextError::DeadlineExceeded)),
                Code::DeadlineExceeded,
                "while loading actor: context deadline exceeded",
                false,
            ),
        ];
        for (name, err, want_code, want_msg, expect_response) in cases {
            let (res, _) = block(server_unary(
                METHOD,
                Request::new(api::ObjectRef::default()),
                |_| async move {
                    match err {
                        None => ok_ref(),
                        Some(e) => Err(e()),
                    }
                },
            ));
            if expect_response {
                assert_eq!(res.unwrap().get_ref().name, "response", "{name}");
                continue;
            }
            let st = res.unwrap_err();
            assert_eq!(st.code(), want_code, "{name}");
            if name == "WrappedStatusBecomesInternal" {
                assert!(
                    st.message().starts_with(want_msg) && st.message().contains("actor not found"),
                    "{name}: {}",
                    st.message()
                );
            } else {
                assert_eq!(st.message(), want_msg, "{name}");
            }
        }
    }

    #[test]
    fn internal_server_unary_interceptor_codes() {
        type Case = (&'static str, fn() -> BoxError, Code, &'static str);
        let cases: Vec<Case> = vec![
            (
                "apierror keeps its code",
                || Box::new(ApiError::failed_precondition("snapshot is corrupt")),
                Code::FailedPrecondition,
                "snapshot is corrupt",
            ),
            (
                "wrapped apierror keeps its code",
                || wrap("while restoring", ApiError::not_found("actor not found")),
                Code::NotFound,
                "actor not found",
            ),
            (
                "upstream status becomes Internal without its details",
                || {
                    wrap(
                        "while calling worker",
                        Status::with_details(
                            Code::DataLoss,
                            "boom",
                            bytes::Bytes::from_static(b"detail"),
                        ),
                    )
                },
                Code::Internal,
                "while calling worker: ",
            ),
            (
                "wrapped upstream status becomes Internal",
                || {
                    wrap(
                        "while calling downstream",
                        Status::unavailable("backend down"),
                    )
                },
                Code::Internal,
                "while calling downstream: ",
            ),
            (
                "wrapped context error keeps the context's code",
                || {
                    wrap(
                        "gave up waiting for the actor's lock",
                        ContextError::Canceled,
                    )
                },
                Code::Cancelled,
                "gave up waiting for the actor's lock: context canceled",
            ),
            (
                "wrapped plain error becomes Internal",
                || wrap("while parsing manifest", "bad json"),
                Code::Internal,
                "while parsing manifest: bad json",
            ),
        ];
        for (name, err, want_code, want_msg) in cases {
            let (res, _) = block(internal_server_unary(
                METHOD,
                Request::new(api::ObjectRef::default()),
                |_| async move { Err::<Response<api::ObjectRef>, _>(err()) },
            ));
            let st = res.unwrap_err();
            assert_eq!(st.code(), want_code, "{name}");
            assert!(
                st.message().starts_with(want_msg),
                "{name}: {}",
                st.message()
            );
            assert!(st.details().is_empty(), "{name}: details leaked");
        }
    }

    #[test]
    fn server_unary_interceptor_emits_elapsed_trailer() {
        const MIN: Duration = Duration::from_millis(5);
        let (res, _) = block(server_unary(
            METHOD,
            Request::new(api::ObjectRef::default()),
            |_| async {
                tokio::time::sleep(MIN).await;
                ok_ref()
            },
        ));
        let resp = res.unwrap();
        let v: u64 = resp
            .metadata()
            .get(SERVER_ELAPSED_TRAILER)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(Duration::from_micros(v) >= MIN, "reported {v}us");
    }

    #[test]
    fn server_unary_interceptor_request_log_masks_env_values() {
        let req = keeper::RunRequest {
            spec: Some(keeper::WorkloadSpec {
                containers: vec![keeper::Container {
                    name: "main".into(),
                    env: vec![
                        keeper::EnvVar {
                            name: "API_KEY".into(),
                            value: "sk-secret".into(),
                        },
                        keeper::EnvVar {
                            name: "PLAIN".into(),
                            value: "not-a-secret".into(),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let (res, log) = block(server_unary(
            "/hivekeep.keeper.v1.Keeper/Run",
            Request::new(req.clone()),
            |_| async { Ok::<_, BoxError>(Response::new(keeper::RunResponse::default())) },
        ));
        res.unwrap();
        for secret in ["sk-secret", "not-a-secret"] {
            assert!(!log.contains(secret), "log contains {secret}: {log}");
        }
        for want in [
            r#""name":"API_KEY""#,
            r#""name":"PLAIN""#,
            &format!(r#""value":"{PLACEHOLDER}""#),
        ] {
            assert!(log.contains(want), "log missing {want}: {log}");
        }
        assert_eq!(req.spec.unwrap().containers[0].env[0].value, "sk-secret");
    }

    #[test]
    fn server_unary_interceptor_response_log_masks_actor_jwt() {
        const TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl";
        let (res, log) = block(server_unary(
            "/hivekeep.api.v1.Control/MintActorJwt",
            Request::new(api::MintActorJwtRequest::default()),
            |_| async {
                Ok::<_, BoxError>(Response::new(api::MintActorJwtResponse {
                    actor_jwt: TOKEN.into(),
                    ..Default::default()
                }))
            },
        ));
        assert!(!log.contains(TOKEN), "{log}");
        assert!(
            log.contains(&format!(r#""actor_jwt":"{PLACEHOLDER}""#)),
            "{log}"
        );
        assert_eq!(
            res.unwrap().get_ref().actor_jwt,
            TOKEN,
            "the response returned to the client was mutated"
        );
    }

    #[test]
    fn internal_server_unary_interceptor_log_masks_secret_fields() {
        // The worker's container env is a labeled repeated string.
        let spec = hivekeep_proto::worker::v1::ContainerSpec {
            env: vec!["TOKEN=hunter2-hunter2".into()],
            ..Default::default()
        };
        let desc = protoredact::message_descriptor("hivekeep.worker.v1.ContainerSpec").unwrap();
        let v = ProtoValue::of(&desc, &spec);
        assert!(!v.to_string().contains("hunter2"), "{v}");
        let (res, log) = block(internal_server_unary(
            "/hivekeep.api.v1.Control/GetActor",
            Request::new(api::GetActorRequest::default()),
            |_| async { Ok::<_, BoxError>(Response::new(api::Actor::default())) },
        ));
        res.unwrap();
        assert!(log.contains("Handle RPC"), "{log}");
    }

    #[test]
    fn server_unary_interceptor_logs_nil_response_on_handler_error() {
        for method in [
            "/hivekeep.api.v1.Control/MintActorJwt",
            "/hivekeep.api.v1.Control/ListActors",
        ] {
            let (res, log) = block(server_unary(
                method,
                Request::new(api::ObjectRef::default()),
                |_| async {
                    Err::<Response<api::ListActorsResponse>, BoxError>(Box::new(
                        ApiError::permission_denied("no"),
                    ))
                },
            ));
            assert_eq!(res.unwrap_err().code(), Code::PermissionDenied);
            assert!(log.contains(r#""resp":null"#), "{method}: {log}");
        }
    }

    #[test]
    fn server_unary_interceptor_elides_list_responses() {
        let resp = api::ListActorsResponse {
            actors: vec![api::Actor {
                metadata: Some(api::ResourceMetadata {
                    name: "gym-0123456789ab".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let (res, log) = block(server_unary(
            "/hivekeep.api.v1.Control/ListActors",
            Request::new(api::ListActorsRequest {
                hive: "nemo-gym".into(),
                ..Default::default()
            }),
            |_| async move { Ok::<_, BoxError>(Response::new(resp)) },
        ));
        assert!(
            !log.contains("gym-0123456789ab") && log.contains("ListActors response elided"),
            "{log}"
        );
        assert_eq!(res.unwrap().get_ref().actors.len(), 1);
    }

    #[test]
    fn principal_is_logged() {
        let mut req = Request::new(api::ObjectRef::default());
        req.extensions_mut()
            .insert(LoggedPrincipal("spiffe://cluster.local/ns/a/sa/b".into()));
        let (_, log) = block(server_unary(METHOD, req, |_| async { ok_ref() }));
        assert!(
            log.contains(r#""principal":"spiffe://cluster.local/ns/a/sa/b""#),
            "{log}"
        );
        assert!(log.contains("schema unknown"), "{log}");
    }
}
