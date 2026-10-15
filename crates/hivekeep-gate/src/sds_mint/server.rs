//! The SDS gRPC service (delta only).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};
use envoy_types::pb::envoy::service::secret::v3::secret_discovery_service_server::{
    SecretDiscoveryService, SecretDiscoveryServiceServer,
};
use hivekeep_identity::host_cert::HostCertMinter;
use tokio_stream::wrappers::ReceiverStream;

use super::delta::serve_stream;

/// Depth of each stream's send queue.
const SEND_DEPTH: usize = 8;

/// The minting SDS server.
#[derive(Clone)]
pub struct SdsServer {
    minter: Arc<HostCertMinter>,
    nonce: Arc<AtomicU64>,
}

impl SdsServer {
    /// A server minting with `minter`.
    pub fn new(minter: Arc<HostCertMinter>) -> Self {
        SdsServer {
            minter,
            nonce: Arc::default(),
        }
    }

    /// The gRPC service.
    pub fn into_service(self) -> SecretDiscoveryServiceServer<Self> {
        SecretDiscoveryServiceServer::new(self)
    }
}

type Out<T> = ReceiverStream<Result<T, tonic::Status>>;

#[async_trait]
impl SecretDiscoveryService for SdsServer {
    type DeltaSecretsStream = Out<DeltaDiscoveryResponse>;
    type StreamSecretsStream = Out<DiscoveryResponse>;

    async fn delta_secrets(
        &self,
        request: tonic::Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::DeltaSecretsStream>, tonic::Status> {
        let incoming = request.into_inner();
        let (tx, mut rx) = tokio::sync::mpsc::channel(SEND_DEPTH);
        let (out_tx, out_rx) = tokio::sync::mpsc::channel(SEND_DEPTH);
        let (minter, nonce) = (self.minter.clone(), self.nonce.clone());
        tokio::spawn(async move {
            let forward = async {
                while let Some(r) = rx.recv().await {
                    if out_tx.send(Ok(r)).await.is_err() {
                        return;
                    }
                }
            };
            let serve = serve_stream(minter, nonce, incoming, tx);
            let (res, _) = tokio::join!(serve, forward);
            if let Err(e) = res {
                let _ = out_tx
                    .send(Err(tonic::Status::invalid_argument(e.to_string())))
                    .await;
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(out_rx)))
    }

    async fn stream_secrets(
        &self,
        _: tonic::Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::StreamSecretsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("only delta SDS is served"))
    }

    async fn fetch_secrets(
        &self,
        _: tonic::Request<DiscoveryRequest>,
    ) -> Result<tonic::Response<DiscoveryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("only delta SDS is served"))
    }
}
