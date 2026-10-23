//! The networking suite: ingress through the gate router (HTTP, CONNECT to
//! arbitrary ports, HTTP/2 and gRPC, WebSocket) and egress through the
//! egress gate (policy, TLS interception and passthrough, DNS, gRPC,
//! refusal of an Actor's own CONNECT and WebSocket upgrades).
//!
//! Every cluster test is `#[ignore]`d and needs `HK_E2E=1` and `HK_E2E_KUBECONFIG`; the
//! pure helper tests run with plain `cargo test`. The suite-wide setup
//! (clients, preflight) happens on the first cluster test.

mod accesslog;
mod arbitraryport;
mod connectegress;
mod egresspolicy;
mod grpcegress;
mod grpcingress;
mod httpwire;
mod microvmclock;
mod networking;
mod websocketegress;
mod websocketingress;
