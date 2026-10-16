//! A minimal ttrpc client (and test server): the lightweight RPC protocol the
//! kata guest agent serves, protobuf messages over a single stream.
//!
//! Each frame is a 10-byte header (payload length and stream id as big-endian
//! u32, a message type byte, a flags byte) followed by the payload. A call is
//! a `Request` frame on a fresh odd stream id answered by a `Response` frame
//! on the same id, so calls multiplex freely over one connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Frame header length.
pub const HEADER_LEN: usize = 10;
/// Largest payload either side accepts.
pub const MAX_MESSAGE_LEN: usize = 4 << 20;
/// Message type of a request frame.
pub const TYPE_REQUEST: u8 = 1;
/// Message type of a response frame.
pub const TYPE_RESPONSE: u8 = 2;

/// A call's envelope.
#[derive(Clone, PartialEq, Message)]
pub struct Request {
    /// Fully qualified service, e.g. `grpc.AgentService`.
    #[prost(string, tag = "1")]
    pub service: String,
    /// Method name.
    #[prost(string, tag = "2")]
    pub method: String,
    /// The encoded request message.
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
    /// The caller's remaining time, nanoseconds; 0 for none.
    #[prost(int64, tag = "4")]
    pub timeout_nano: i64,
    /// Call metadata.
    #[prost(message, repeated, tag = "5")]
    pub metadata: Vec<KeyValue>,
}

/// One metadata pair.
#[derive(Clone, PartialEq, Message)]
pub struct KeyValue {
    /// Key.
    #[prost(string, tag = "1")]
    pub key: String,
    /// Value.
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A call's outcome envelope.
#[derive(Clone, PartialEq, Message)]
pub struct Response {
    /// The call's status; absent or code 0 is success.
    #[prost(message, optional, tag = "1")]
    pub status: Option<RpcStatus>,
    /// The encoded response message.
    #[prost(bytes = "vec", tag = "2")]
    pub payload: Vec<u8>,
}

/// A gRPC-style status.
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    /// gRPC code.
    #[prost(int32, tag = "1")]
    pub code: i32,
    /// Message.
    #[prost(string, tag = "2")]
    pub message: String,
    /// Details.
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

/// A failed call.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TtrpcError {
    /// The connection is closed.
    #[error("ttrpc: closed")]
    Closed,
    /// The server answered with an error status.
    #[error("ttrpc: {message} (code {code})")]
    Status {
        /// gRPC code.
        code: i32,
        /// Message.
        message: String,
    },
    /// A message could not be encoded or decoded.
    #[error("ttrpc: {0}")]
    Codec(String),
}

type Pending = Arc<Mutex<HashMap<u32, oneshot::Sender<Result<Vec<u8>, TtrpcError>>>>>;

/// A ttrpc client over one connection.
#[derive(Debug, Clone)]
pub struct Client {
    tx: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    next_id: Arc<AtomicU32>,
    closed: CancellationToken,
}

fn frame(stream: u32, kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(HEADER_LEN + payload.len());
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(&stream.to_be_bytes());
    f.push(kind);
    f.push(0);
    f.extend_from_slice(payload);
    f
}

/// Reads one frame: its stream id, type and payload.
async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<(u32, u8, Vec<u8>)> {
    let mut h = [0u8; HEADER_LEN];
    r.read_exact(&mut h).await?;
    let len = u32::from_be_bytes([h[0], h[1], h[2], h[3]]) as usize;
    let stream = u32::from_be_bytes([h[4], h[5], h[6], h[7]]);
    if len > MAX_MESSAGE_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ttrpc: message of {len} bytes exceeds the {MAX_MESSAGE_LEN}-byte limit"),
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok((stream, h[8], payload))
}

/// Drops a call's pending slot when its future goes away.
struct Slot<'a> {
    pending: &'a Pending,
    id: u32,
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.pending.lock().remove(&self.id);
    }
}

impl Client {
    /// A client over `stream`. Needs a tokio runtime; the connection is
    /// served by background tasks until [`Client::close`] or EOF.
    pub fn new<S>(stream: S) -> Client
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
        let pending: Pending = Arc::default();
        let closed = CancellationToken::new();
        {
            let closed = closed.clone();
            tokio::spawn(async move {
                loop {
                    let f = tokio::select! {
                        f = rx.recv() => f,
                        _ = closed.cancelled() => None,
                    };
                    let Some(f) = f else { break };
                    if wr.write_all(&f).await.is_err() {
                        break;
                    }
                }
                closed.cancel();
                let _ = wr.shutdown().await;
            });
        }
        {
            let (closed, pending) = (closed.clone(), pending.clone());
            tokio::spawn(async move {
                loop {
                    let r = tokio::select! {
                        r = read_frame(&mut rd) => r,
                        _ = closed.cancelled() => break,
                    };
                    let Ok((id, kind, payload)) = r else { break };
                    if kind != TYPE_RESPONSE {
                        continue;
                    }
                    let Some(waiter) = pending.lock().remove(&id) else {
                        continue;
                    };
                    let result = match Response::decode(payload.as_slice()) {
                        Err(e) => Err(TtrpcError::Codec(e.to_string())),
                        Ok(resp) => match resp.status {
                            Some(s) if s.code != 0 => Err(TtrpcError::Status {
                                code: s.code,
                                message: s.message,
                            }),
                            _ => Ok(resp.payload),
                        },
                    };
                    let _ = waiter.send(result);
                }
                closed.cancel();
                // Fail every call still waiting.
                pending.lock().clear();
            });
        }
        Client {
            tx,
            pending,
            next_id: Arc::new(AtomicU32::new(1)),
            closed,
        }
    }

    /// Calls `service/method`. `timeout` is passed to the server as the
    /// call's deadline and bounds the wait here.
    pub async fn call<Req: Message, Resp: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: &Req,
        timeout: Option<Duration>,
    ) -> Result<Resp, TtrpcError> {
        if self.closed.is_cancelled() {
            return Err(TtrpcError::Closed);
        }
        let id = self.next_id.fetch_add(2, Ordering::SeqCst);
        let envelope = Request {
            service: service.into(),
            method: method.into(),
            payload: req.encode_to_vec(),
            timeout_nano: timeout.map_or(0, |t| t.as_nanos().min(i64::MAX as u128) as i64),
            metadata: vec![],
        };
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id, tx);
        let _slot = Slot {
            pending: &self.pending,
            id,
        };
        if self
            .tx
            .send(frame(id, TYPE_REQUEST, &envelope.encode_to_vec()))
            .await
            .is_err()
        {
            return Err(TtrpcError::Closed);
        }
        let wait = async {
            tokio::select! {
                r = rx => r.unwrap_or(Err(TtrpcError::Closed)),
                _ = self.closed.cancelled() => Err(TtrpcError::Closed),
            }
        };
        let payload = match timeout {
            Some(t) => tokio::time::timeout(t, wait)
                .await
                .map_err(|_| TtrpcError::Status {
                    code: tonic::Code::DeadlineExceeded as i32,
                    message: "context deadline exceeded".into(),
                })??,
            None => wait.await?,
        };
        Resp::decode(payload.as_slice()).map_err(|e| TtrpcError::Codec(e.to_string()))
    }

    /// Closes the connection; calls in flight and later fail with
    /// [`TtrpcError::Closed`].
    pub fn close(&self) {
        self.closed.cancel();
    }

    /// Whether the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }
}

/// Handles one request: `(service, method, payload)` to a response payload
/// or an error status.
pub type Handler = Arc<
    dyn Fn(String, String, Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, RpcStatus>> + Send + Sync,
>;

/// Serves ttrpc calls on `stream` with `handler` until EOF. Calls are handled
/// concurrently. For tests and fakes of the guest agent.
pub async fn serve<S>(stream: S, handler: Handler)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let wr = Arc::new(tokio::sync::Mutex::new(wr));
    while let Ok((id, kind, payload)) = read_frame(&mut rd).await {
        if kind != TYPE_REQUEST {
            continue;
        }
        let (handler, wr) = (handler.clone(), wr.clone());
        tokio::spawn(async move {
            let resp = match Request::decode(payload.as_slice()) {
                Err(e) => Response {
                    status: Some(RpcStatus {
                        code: tonic::Code::InvalidArgument as i32,
                        message: e.to_string(),
                        details: vec![],
                    }),
                    payload: vec![],
                },
                Ok(req) => match handler(req.service, req.method, req.payload).await {
                    Ok(p) => Response {
                        status: None,
                        payload: p,
                    },
                    Err(s) => Response {
                        status: Some(s),
                        payload: vec![],
                    },
                },
            };
            let f = frame(id, TYPE_RESPONSE, &resp.encode_to_vec());
            let _ = wr.lock().await.write_all(&f).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn echo() -> Handler {
        Arc::new(|service, method, payload| {
            Box::pin(async move {
                if method == "Fail" {
                    return Err(RpcStatus {
                        code: 5,
                        message: format!("{service}/{method} refused"),
                        details: vec![],
                    });
                }
                if method == "Hang" {
                    futures::future::pending::<()>().await;
                }
                Ok(payload)
            })
        })
    }

    #[tokio::test]
    async fn calls_round_trip_and_multiplex() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(b, echo()));
        let c = Client::new(a);
        let calls = (0..20).map(|i| {
            let c = c.clone();
            async move {
                let req = KeyValue {
                    key: format!("k{i}"),
                    value: "v".into(),
                };
                let got: KeyValue = c.call("svc", "Echo", &req, None).await.unwrap();
                assert_eq!(got, req);
            }
        });
        futures::future::join_all(calls).await;
    }

    #[tokio::test]
    async fn errors_carry_the_status() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(b, echo()));
        let c = Client::new(a);
        let err = c
            .call::<_, KeyValue>("svc", "Fail", &KeyValue::default(), None)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            TtrpcError::Status {
                code: 5,
                message: "svc/Fail refused".into()
            }
        );
    }

    #[tokio::test]
    async fn close_fails_calls_in_flight() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(b, echo()));
        let c = Client::new(a);
        let c2 = c.clone();
        let call = tokio::spawn(async move {
            c2.call::<_, KeyValue>("svc", "Hang", &KeyValue::default(), None)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        c.close();
        assert_eq!(call.await.unwrap().unwrap_err(), TtrpcError::Closed);
        assert_eq!(
            c.call::<_, KeyValue>("svc", "Echo", &KeyValue::default(), None)
                .await
                .unwrap_err(),
            TtrpcError::Closed
        );
    }

    #[tokio::test]
    async fn a_timeout_bounds_the_wait() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(serve(b, echo()));
        let c = Client::new(a);
        let err = c
            .call::<_, KeyValue>(
                "svc",
                "Hang",
                &KeyValue::default(),
                Some(Duration::from_millis(20)),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, TtrpcError::Status { code: 4, .. }), "{err}");
    }
}
