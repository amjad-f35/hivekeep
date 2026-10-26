//! How the worker exchanges messages with the master: a ZeroMQ DEALER
//! socket identified by the worker's node ID, or an in-memory pair for
//! tests.

use async_trait::async_trait;
use tokio::sync::mpsc;
use zeromq::{Socket, SocketOptions, SocketRecv, SocketSend, ZmqMessage};

use super::message::{DecodeError, Message};

/// Why a message could not be sent or received.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The socket failed.
    #[error("zmq: {0}")]
    Zmq(#[from] zeromq::ZmqError),
    /// The bytes were not a message.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// The other end is gone.
    #[error("connection closed")]
    Closed,
}

/// A message channel to the master.
#[async_trait]
pub trait Transport: Send {
    /// Sends one message.
    async fn send(&mut self, msg: Message) -> Result<(), TransportError>;
    /// Waits for the next message.
    async fn recv(&mut self) -> Result<Message, TransportError>;
}

/// The master over ZeroMQ.
pub struct ZmqTransport {
    socket: zeromq::DealerSocket,
}

impl ZmqTransport {
    /// Connects a DEALER socket identified by `node_id` to
    /// `tcp://host:port`.
    pub async fn connect(host: &str, port: u16, node_id: &str) -> Result<Self, TransportError> {
        let mut opts = SocketOptions::default();
        opts.peer_identity(
            zeromq::util::PeerIdentity::try_from(node_id.as_bytes().to_vec())
                .map_err(TransportError::Zmq)?,
        );
        let mut socket = zeromq::DealerSocket::with_options(opts);
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        socket.connect(&format!("tcp://{host}:{port}")).await?;
        Ok(ZmqTransport { socket })
    }
}

#[async_trait]
impl Transport for ZmqTransport {
    async fn send(&mut self, msg: Message) -> Result<(), TransportError> {
        self.socket.send(ZmqMessage::from(msg.encode())).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Message, TransportError> {
        let frames = self.socket.recv().await?;
        // A DEALER sees exactly what the master's ROUTER sent after the
        // routing frame: one frame, the message.
        let last = frames.into_vec().pop().ok_or(TransportError::Closed)?;
        Ok(Message::decode(&last)?)
    }
}

/// An in-memory transport: the worker's end of a channel pair.
pub struct ChannelTransport {
    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>,
}

/// The master's end of a [`ChannelTransport`], for tests.
pub struct MasterEnd {
    /// Messages from the worker.
    pub from_worker: mpsc::UnboundedReceiver<Message>,
    /// Messages to the worker.
    pub to_worker: mpsc::UnboundedSender<Message>,
}

impl ChannelTransport {
    /// A connected worker/master pair.
    pub fn pair() -> (ChannelTransport, MasterEnd) {
        let (wtx, mrx) = mpsc::unbounded_channel();
        let (mtx, wrx) = mpsc::unbounded_channel();
        (
            ChannelTransport { tx: wtx, rx: wrx },
            MasterEnd {
                from_worker: mrx,
                to_worker: mtx,
            },
        )
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    async fn send(&mut self, msg: Message) -> Result<(), TransportError> {
        self.tx.send(msg).map_err(|_| TransportError::Closed)
    }

    async fn recv(&mut self) -> Result<Message, TransportError> {
        self.rx.recv().await.ok_or(TransportError::Closed)
    }
}
