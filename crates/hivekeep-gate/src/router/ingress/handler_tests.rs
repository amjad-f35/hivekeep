use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use envoy_types::pb::envoy::config::core::v3::HeaderValue;
use envoy_types::pb::google::protobuf::Struct;
use futures::future::BoxFuture;
use hivekeep_proto::api::v1 as pb;
use parking_lot::Mutex;
use tonic::{Code, Status};

use super::*;
use crate::attr::outcome;
use crate::router::extproc::metadata::string_value;
use crate::router::extproc::{
    QueryRecorder, CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE, TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
};
use crate::router::ingress::parking::tests::rejected_by_outcome;
use crate::router::ingress::resumer::tests::FnApi;
use crate::tunnel::ingress::TARGET_PORT_HEADER;

const UUID: &str = "123e4567-e89b-12d3-a456-426614174000";
const WORKER_ID: &str = "spiffe://cluster.local/ns/workers/sa/hivekeep-worker";

type Resume =
    Arc<dyn Fn(pb::ResumeActorRequest) -> Result<pb::ResumeActorResponse, Status> + Send + Sync>;

fn api(
    f: Resume,
) -> (
    Arc<dyn crate::router::ingress::resumer::ActorApi>,
    Arc<AtomicU32>,
) {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    (
        Arc::new(FnApi(move |req| {
            c.fetch_add(1, Ordering::SeqCst);
            let r = f(req);
            Box::pin(async move { r }) as BoxFuture<'static, _>
        })),
        calls,
    )
}

fn assigned(ips: &[&str], spiffe: &str) -> pb::ResumeActorResponse {
    pb::ResumeActorResponse {
        actor: Some(pb::Actor {
            status: Some(pb::ActorStatus {
                worker_assignment: Some(pb::WorkerAssignment {
                    worker_pod_ips: ips.iter().map(|s| s.to_string()).collect(),
                    worker_spiffe_id: spiffe.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        resumed: false,
    }
}

fn ok_api() -> (
    Arc<dyn crate::router::ingress::resumer::ActorApi>,
    Arc<AtomicU32>,
) {
    api(Arc::new(|_| Ok(assigned(&["10.0.0.52"], WORKER_ID))))
}

fn hv(k: &str, v: &str) -> HeaderValue {
    HeaderValue {
        key: k.into(),
        value: v.into(),
        ..Default::default()
    }
}

fn attrs(kv: &[(&str, &str)]) -> HashMap<String, Struct> {
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

fn md_for(name: &str, hive: &str, extra: &[HeaderValue]) -> RequestMetadata {
    let mut h = extra.to_vec();
    h.push(hv(TARGET_ACTOR_HEADER, &format!("{hive}/{name}")));
    RequestMetadata::new(&h, HashMap::new())
}

fn handler(a: Arc<dyn crate::router::ingress::resumer::ActorApi>) -> Handler {
    Handler::new(a, ParkedRequestConfig::disabled(), None)
}

fn meta(res: &HandlerResult, key: &str) -> String {
    use envoy_types::pb::google::protobuf::value::Kind;
    let outer = res.dynamic_metadata.as_ref().unwrap();
    let Some(Kind::StructValue(inner)) = &outer.fields[ORIGINAL_DST_METADATA_KEY].kind else {
        panic!("no original dst")
    };
    match &inner.fields[key].kind {
        Some(Kind::StringValue(s)) => s.clone(),
        _ => panic!("not a string"),
    }
}

fn mutations(res: &HandlerResult) -> HashMap<String, String> {
    res.response
        .response
        .as_ref()
        .and_then(|r| r.header_mutation.as_ref())
        .map(|m| {
            m.set_headers
                .iter()
                .map(|o| {
                    let h = o.header.as_ref().unwrap();
                    (
                        h.key.to_ascii_lowercase(),
                        String::from_utf8_lossy(&h.raw_value).into_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn accepts_mixed_case_routing_headers() {
    let seen = Arc::new(Mutex::new(None));
    let s = seen.clone();
    let (a, _) = api(Arc::new(move |req| {
        *s.lock() = req.actor;
        Ok(assigned(&["10.0.0.52"], WORKER_ID))
    }));
    let mut md = RequestMetadata::new(&[hv("Hk-Target-Actor", "team-a/actor-1")], HashMap::new());
    handler(a).handle_request_headers(&mut md).await.unwrap();
    let r = seen.lock().clone().unwrap();
    assert_eq!((r.hive.as_str(), r.name.as_str()), ("team-a", "actor-1"));
}

#[tokio::test]
async fn rejects_duplicate_or_comma_headers() {
    let cases: Vec<(&str, Vec<HeaderValue>, HashMap<String, Struct>)> = vec![
        (
            "duplicate headers with different values",
            vec![
                hv(TARGET_ACTOR_HEADER, "demo/a"),
                hv(TARGET_ACTOR_HEADER, "secret/x"),
            ],
            HashMap::new(),
        ),
        (
            "duplicate headers with identical values",
            vec![
                hv(TARGET_ACTOR_HEADER, "demo/a"),
                hv(TARGET_ACTOR_HEADER, "demo/a"),
            ],
            HashMap::new(),
        ),
        (
            "single header with comma-joined value",
            vec![hv(TARGET_ACTOR_HEADER, "demo/a,secret/x")],
            HashMap::new(),
        ),
        (
            "filter state attribute with comma-joined value",
            vec![],
            attrs(&[(TARGET_ACTOR_FILTER_STATE_ATTRIBUTE, "demo/a,secret/x")]),
        ),
    ];
    for (name, headers, at) in cases {
        let (a, calls) = api(Arc::new(|_| Err(Status::unknown("should not be called"))));
        let mut md = RequestMetadata::new(&headers, at);
        let err = handler(a)
            .handle_request_headers(&mut md)
            .await
            .unwrap_err();
        assert_eq!(err.http_status(), 404, "{name}");
        assert!(
            err.to_string().contains("invalid actor reference"),
            "{name}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{name}");
    }
}

#[tokio::test]
async fn connect_attribute_overrides_inner_header() {
    let seen = Arc::new(Mutex::new(String::new()));
    let s = seen.clone();
    let (a, _) = api(Arc::new(move |req| {
        let r = req.actor.unwrap();
        *s.lock() = format!("{}/{}", r.hive, r.name);
        Ok(assigned(&["10.0.0.52"], WORKER_ID))
    }));
    let mut md = RequestMetadata::new(
        &[hv(TARGET_ACTOR_HEADER, "secret/x")],
        attrs(&[(
            TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
            &format!("team-a/{UUID}"),
        )]),
    );
    let res = handler(a).handle_request_headers(&mut md).await.unwrap();
    assert_eq!(*seen.lock(), format!("team-a/{UUID}"));
    assert_eq!(
        mutations(&res)[TARGET_ACTOR_HEADER],
        format!("team-a/{UUID}")
    );
}

#[test]
fn does_not_log_sensitive_data() {
    let secret = "do-not-log-me";
    let authority = format!("{UUID}.team-a.actors.resources.hivekeep.io");
    let md = md_for(
        UUID,
        "team-a",
        &[
            hv(":path", &format!("/api/v1/reset?token={secret}")),
            hv(":authority", &authority),
            hv(":method", "POST"),
            hv("authorization", &format!("Bearer {secret}")),
            hv("cookie", &format!("session={secret}")),
        ],
    );
    // Callsite interest is global: a handler test on another thread can
    // register these callsites while no subscriber exists and leave them
    // disabled, so an empty capture is retried with a fresh subscriber.
    let mut captured = None;
    for _ in 0..5 {
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let b = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || BufWriter(b.clone()))
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let res = tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (a, _) = ok_api();
                    let mut md = md.clone();
                    handler(a).handle_request_headers(&mut md).await.unwrap()
                })
        });
        let out = String::from_utf8(buf.lock().clone()).unwrap();
        assert!(
            !out.contains(secret),
            "router log leaked a sensitive value: {out}"
        );
        if !out.is_empty() {
            captured = Some((res, out));
            break;
        }
    }
    let (res, out) = captured.expect("router logged nothing");
    assert!(
        out.contains(UUID),
        "router log lacks routing context: {out}"
    );
    let rec = QueryRecorder::new(10);
    rec.add_router_request(
        chrono::Utc::now(),
        Duration::from_millis(1),
        "Route ok",
        &res.target,
        &md,
    );
    for q in rec.get() {
        assert!(
            !serde_json::to_string(&q).unwrap().contains(secret),
            "recorder retained a sensitive value"
        );
    }
}

#[derive(Clone)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for BufWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn handle_request_headers_cases() {
    let host = format!("{UUID}.team-a.actors.resources.hivekeep.io");
    let id = format!("team-a/{UUID}");
    type Case = (
        &'static str,
        &'static str,
        String,
        Result<pb::ResumeActorResponse, Status>,
        Result<(String, String), (u16, String)>,
    );
    let st = |c, m: &str| Err(Status::new(c, m));
    let cases: Vec<Case> = vec![
        (
            "invalid actor header returns 404",
            "INVALID",
            "invalid-host.com".into(),
            Ok(Default::default()),
            Err((404, "invalid actor reference".into())),
        ),
        (
            "non-gRPC resume error collapses to 500 without leaking detail",
            UUID,
            host.clone(),
            st(Code::Unknown, "resume failed with sensitive detail"),
            Err((500, format!("error resuming actor {id}"))),
        ),
        (
            "FailedPrecondition maps to 503 with preserved desc",
            UUID,
            host.clone(),
            st(Code::FailedPrecondition, "no worker has room for the actor"),
            Err((
                503,
                format!("actor {id} unavailable: no worker has room for the actor"),
            )),
        ),
        (
            "NotFound maps to 404",
            UUID,
            host.clone(),
            st(Code::NotFound, "actor missing"),
            Err((404, format!("actor {id} not found"))),
        ),
        (
            "Unavailable maps to 503",
            UUID,
            host.clone(),
            st(Code::Unavailable, "control-plane down"),
            Err((503, format!("actor {id} unavailable"))),
        ),
        (
            "DeadlineExceeded maps to 504",
            UUID,
            host.clone(),
            st(Code::DeadlineExceeded, "deadline"),
            Err((504, format!("actor {id} request timed out"))),
        ),
        (
            "bad Actor IP from resume returns 500 without leaking IP",
            UUID,
            host.clone(),
            Ok(assigned(&["invalid-ip"], WORKER_ID)),
            Err((500, format!("actor {id} routing failed"))),
        ),
        (
            "no Actor IPs from resume returns 500",
            UUID,
            host.clone(),
            Ok(assigned(&[], WORKER_ID)),
            Err((500, format!("actor {id} routing failed"))),
        ),
        (
            "no worker SPIFFE ID from resume returns 500",
            UUID,
            host.clone(),
            Ok(assigned(&["10.0.0.52"], "")),
            Err((500, format!("actor {id} routing failed"))),
        ),
        (
            "successful resume ignores host and port for actor routing",
            UUID,
            "127.0.0.1:44681".into(),
            Ok(assigned(&["10.0.0.52"], WORKER_ID)),
            Ok(("10.0.0.52:443".into(), "80".into())),
        ),
        (
            "dual-stack resume routes to the first IP",
            UUID,
            "127.0.0.1:44681".into(),
            Ok(assigned(&["fd00::52", "10.0.0.52"], WORKER_ID)),
            Ok(("[fd00::52]:443".into(), "80".into())),
        ),
    ];
    for (name, actor_name, authority, resp, want) in cases {
        let (a, _) = api(Arc::new(move |req| {
            assert_eq!(
                req.actor.unwrap().name,
                UUID,
                "unexpected identifier parsed"
            );
            resp.clone()
        }));
        let mut md = md_for(
            actor_name,
            "team-a",
            &[
                hv(":path", "/v1/actors/invoke"),
                hv(":authority", &authority),
                hv(":method", "POST"),
            ],
        );
        let got = handler(a).handle_request_headers(&mut md).await;
        match want {
            Err((status, body)) => {
                let e = got.expect_err(name);
                assert_eq!(e.to_string(), body, "{name}");
                assert_eq!(e.http_status(), status, "{name}");
            }
            Ok((target, port)) => {
                let r = got.unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(r.target, target, "{name}");
                let m = mutations(&r);
                assert_eq!(m.len(), 1, "{name}");
                assert!(
                    !m.contains_key(TARGET_PORT_HEADER),
                    "{name}: target port is only dynamic metadata"
                );
                assert_eq!(m[TARGET_ACTOR_HEADER], id, "{name}");
                assert_eq!(meta(&r, ORIGINAL_DST_ADDRESS_KEY), target, "{name}");
                assert_eq!(meta(&r, ORIGINAL_DST_PORT_KEY), port, "{name}");
                assert_eq!(meta(&r, WORKER_SPIFFE_ID_KEY), WORKER_ID, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn handles_connect_method() {
    let (a, _) = ok_api();
    let mut md = md_for(
        UUID,
        "team-a",
        &[
            hv(
                ":authority",
                &format!("{UUID}.team-a.actors.resources.hivekeep.io:9090"),
            ),
            hv(":method", "CONNECT"),
        ],
    );
    let r = handler(a).handle_request_headers(&mut md).await.unwrap();
    assert_eq!(r.target, "10.0.0.52:443");
    assert_eq!(meta(&r, ORIGINAL_DST_ADDRESS_KEY), "10.0.0.52:443");
    assert_eq!(meta(&r, ORIGINAL_DST_PORT_KEY), "9090");
}

#[tokio::test]
async fn uses_retained_connect_authority_for_port() {
    let (a, _) = ok_api();
    let mut md = RequestMetadata::new(
        &[hv(":authority", "inner.example")],
        attrs(&[
            (
                CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE,
                "unrelated.example:9090",
            ),
            (
                TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
                &format!("team-a/{UUID}"),
            ),
        ]),
    );
    let r = handler(a).handle_request_headers(&mut md).await.unwrap();
    assert_eq!(meta(&r, ORIGINAL_DST_PORT_KEY), "9090");
    assert_eq!(mutations(&r)[TARGET_ACTOR_HEADER], format!("team-a/{UUID}"));
}

#[tokio::test]
async fn connect_requires_outer_target_actor() {
    let id = format!("team-a/{UUID}");
    let ca = (
        CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE,
        "unrelated.example:9090",
    );
    let cases: Vec<(
        &str,
        Vec<HeaderValue>,
        HashMap<String, Struct>,
        Option<String>,
    )> = vec![
        (
            "no outer target and inner header is rejected",
            vec![
                hv(TARGET_ACTOR_HEADER, &id),
                hv(":authority", "inner.example"),
            ],
            attrs(&[ca]),
            None,
        ),
        (
            "empty outer target and inner header is rejected",
            vec![
                hv(TARGET_ACTOR_HEADER, &id),
                hv(":authority", "inner.example"),
            ],
            attrs(&[ca, (TARGET_ACTOR_FILTER_STATE_ATTRIBUTE, "")]),
            None,
        ),
        (
            "no outer target and no inner header is rejected",
            vec![hv(":authority", "inner.example")],
            attrs(&[ca]),
            None,
        ),
        (
            "comma-separated outer target is rejected",
            vec![hv(":authority", "inner.example")],
            attrs(&[
                ca,
                (TARGET_ACTOR_FILTER_STATE_ATTRIBUTE, "team-a/a,team-b/b"),
            ]),
            None,
        ),
        (
            "outer target resolves even if inner header is omitted",
            vec![hv(":authority", "inner.example")],
            attrs(&[ca, (TARGET_ACTOR_FILTER_STATE_ATTRIBUTE, &id)]),
            Some(id.clone()),
        ),
        (
            "outer target X overrides inner header Y",
            vec![
                hv(TARGET_ACTOR_HEADER, "team-b/untrusted-actor"),
                hv(":authority", "inner.example"),
            ],
            attrs(&[ca, (TARGET_ACTOR_FILTER_STATE_ATTRIBUTE, &id)]),
            Some(id.clone()),
        ),
    ];
    for (name, headers, at, want) in cases {
        let seen = Arc::new(Mutex::new(String::new()));
        let s = seen.clone();
        let (a, calls) = api(Arc::new(move |req| {
            let r = req.actor.unwrap();
            *s.lock() = format!("{}/{}", r.hive, r.name);
            Ok(assigned(&["10.0.0.52"], WORKER_ID))
        }));
        let mut md = RequestMetadata::new(&headers, at);
        let got = handler(a).handle_request_headers(&mut md).await;
        match want {
            None => {
                assert_eq!(got.unwrap_err().http_status(), 404, "{name}");
                assert_eq!(calls.load(Ordering::SeqCst), 0, "{name}");
            }
            Some(want) => {
                let r = got.unwrap();
                assert_eq!(*seen.lock(), want, "{name}");
                assert_eq!(mutations(&r)[TARGET_ACTOR_HEADER], want, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn full_lot_serves_running_actor() {
    let (a, calls) = api(Arc::new(|_| Ok(assigned(&["10.0.0.1"], WORKER_ID))));
    let h = Handler::new(
        a,
        ParkedRequestConfig::with(1, Duration::from_secs(1)),
        None,
    );
    let _priming = h.parking().enter(outcome::UNAVAILABLE).unwrap();
    let mut md = md_for(UUID, "team-a", &[hv(":authority", "x")]);
    let r = h
        .handle_request_headers(&mut md)
        .await
        .expect("a running Actor is served despite a full lot");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(r.target, "10.0.0.1:443");
    assert_eq!(h.parking().active_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn full_lot_sheds_parked_request() {
    for (name, code, want_code, want_outcome) in [
        (
            "ResourceExhausted is kept as the cause",
            Code::ResourceExhausted,
            Some(Code::ResourceExhausted),
            outcome::NO_CAPACITY,
        ),
        (
            "Aborted is dropped",
            Code::Aborted,
            None,
            outcome::UNAVAILABLE,
        ),
        (
            "FailedPrecondition is dropped",
            Code::FailedPrecondition,
            None,
            outcome::UNAVAILABLE,
        ),
        (
            "Unavailable is dropped",
            Code::Unavailable,
            None,
            outcome::UNAVAILABLE,
        ),
    ] {
        let (a, calls) = api(Arc::new(move |_| Err(Status::new(code, "transient"))));
        let reg = prometheus::Registry::new();
        let h = Handler::new(
            a,
            ParkedRequestConfig::with(1, Duration::from_millis(500)),
            Some(ParkingMetrics::new(&reg).unwrap()),
        );
        let priming = h.parking().enter(outcome::UNAVAILABLE).unwrap();
        let mut md = md_for(UUID, "team-a", &[hv(":authority", "x")]);
        let e = h.handle_request_headers(&mut md).await.unwrap_err();
        assert_eq!(e.http_status(), 503, "{name}");
        assert!(e.to_string().contains("router at capacity"), "{name}");
        assert_eq!(e.code, want_code, "{name}");
        assert_eq!(
            rejected_by_outcome(&reg),
            HashMap::from([(want_outcome.to_string(), 1)]),
            "{name}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{name}");
        tokio::time::sleep(Duration::from_millis(600)).await;
        priming.release(crate::router::ingress::parking::ParkOutcome::Served);
    }
}
