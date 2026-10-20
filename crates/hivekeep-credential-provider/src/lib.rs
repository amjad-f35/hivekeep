//! The Kubernetes Secrets credential provider: a gRPC service resolving
//! `hk-secret://k8s.io/...` URIs to Secret values on behalf of the egress
//! gateway's credential injector. It is the only component on the
//! credential-injection path with Kubernetes access; the gateway itself
//! never reads Secrets.
//!
//! * [`pb`]: the `hivekeep.credprovider.v1` gRPC API.
//! * [`secret_uri`]: the URI grammar this provider serves.
//! * [`nsauthz`]: the hive → namespace authorization policy.
//! * [`provider`]: the `CredentialProvider` service over the Kubernetes API.
//! * [`tls`]: mutual TLS that reloads the caller CA per connection and pins
//!   the injector's SPIFFE ID.
//! * [`metrics`]: Prometheus metrics of secret fetches.
//! * [`http`], [`health`], [`statusz`]: the plain-HTTP side endpoints.

pub mod health;
pub mod http;
pub mod nsauthz;
pub mod secret_uri;
pub mod tls;
