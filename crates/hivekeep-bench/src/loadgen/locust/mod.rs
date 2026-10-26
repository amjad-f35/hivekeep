//! The worker side of the locust distributed protocol.
//!
//! The worker connects a ZeroMQ DEALER socket to the master, announces
//! itself with `client_ready`, and from then on follows the master's
//! `spawn` / `stop` / `quit` messages while sending `heartbeat` every
//! second and `stats` every few seconds. Messages are msgpack triples of
//! `(type, data, node_id)`.

pub mod message;
pub mod runner;
pub mod stats;
pub mod transport;
pub mod usage;

pub use runner::{Exit, Runner, Timing};
pub use transport::{ChannelTransport, Transport, ZmqTransport};
