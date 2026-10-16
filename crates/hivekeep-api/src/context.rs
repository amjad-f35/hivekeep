//! What the server knows about the caller of an RPC.
//!
//! The authentication layer stores a [`Principal`] and the peer's verified
//! certificates in the request's extensions; handlers read them back with
//! [`CallContext::of`]. Extensions are server-side only, so a client cannot
//! forge them. In-process calls (reconcilers, the WorkerService delegating to
//! Control) mark themselves with [`SystemCall`], which authorization lets
//! through.

use std::sync::Arc;

use hivekeep_core::preview::{header_opts_in, PREVIEW_HEADER};
use tonic::Request;

/// How a principal authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    /// A client certificate verified by the TLS layer; the id is its first
    /// URI SAN.
    Mtls,
    /// A bearer JWT; the id is its subject.
    Jwt,
}

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    /// The identity: a SPIFFE ID for mTLS, the token subject for JWT.
    pub id: String,
    /// How it authenticated.
    pub kind: PrincipalKind,
    /// The token issuer, for JWT principals.
    pub issuer: String,
}

impl Principal {
    /// A principal authenticated by client certificate.
    pub fn mtls(id: impl Into<String>) -> Self {
        Principal {
            id: id.into(),
            kind: PrincipalKind::Mtls,
            issuer: String::new(),
        }
    }

    /// A principal authenticated by bearer token.
    pub fn jwt(id: impl Into<String>, issuer: impl Into<String>) -> Self {
        Principal {
            id: id.into(),
            kind: PrincipalKind::Jwt,
            issuer: issuer.into(),
        }
    }
}

/// The DER certificate chain the peer presented, leaf first, as verified by
/// the TLS layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerCertificates(pub Arc<Vec<Vec<u8>>>);

impl PeerCertificates {
    /// The leaf certificate.
    pub fn leaf(&self) -> Option<&[u8]> {
        self.0.first().map(Vec::as_slice)
    }
}

/// Marks a request issued by the server itself on the system's behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemCall;

/// Caller information of one RPC.
#[derive(Debug, Clone, Default)]
pub struct CallContext {
    /// The authenticated caller, if any.
    pub principal: Option<Principal>,
    /// The peer's certificates, if it presented any.
    pub peer_certs: Option<PeerCertificates>,
    /// Whether the request opted in to preview features.
    pub preview_opt_in: bool,
    /// Whether the server issued the request itself.
    pub system: bool,
}

impl CallContext {
    /// Reads the caller information of `req`.
    pub fn of<T>(req: &Request<T>) -> Self {
        let ext = req.extensions();
        let system = ext.get::<SystemCall>().is_some();
        let opted = header_opts_in(
            req.metadata()
                .get_all(PREVIEW_HEADER)
                .iter()
                .filter_map(|v| v.to_str().ok()),
        );
        CallContext {
            principal: ext.get::<Principal>().cloned(),
            peer_certs: ext.get::<PeerCertificates>().cloned(),
            // The system always acts with every server-enabled gate.
            preview_opt_in: opted || system,
            system,
        }
    }

    /// The context of an in-process system call.
    pub fn system() -> Self {
        CallContext {
            principal: None,
            peer_certs: None,
            preview_opt_in: true,
            system: true,
        }
    }
}

/// Wraps `msg` in a request marked as a system call.
pub fn system_request<T>(msg: T) -> Request<T> {
    let mut req = Request::new(msg);
    req.extensions_mut().insert(SystemCall);
    req
}

/// Wraps `msg` in a request carrying `principal` and, optionally, a peer
/// certificate chain, as the authentication layer would. For tests and
/// in-process callers.
pub fn request_as<T>(
    msg: T,
    principal: Option<Principal>,
    certs: Option<Vec<Vec<u8>>>,
) -> Request<T> {
    let mut req = Request::new(msg);
    if let Some(p) = principal {
        req.extensions_mut().insert(p);
    }
    if let Some(c) = certs {
        req.extensions_mut().insert(PeerCertificates(Arc::new(c)));
    }
    req
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_extensions_and_preview_header() {
        let mut req = request_as((), Some(Principal::jwt("alice", "https://idp")), None);
        assert!(!CallContext::of(&req).preview_opt_in);
        req.metadata_mut()
            .insert(PREVIEW_HEADER, "true".parse().unwrap());
        let cc = CallContext::of(&req);
        assert!(cc.preview_opt_in);
        assert_eq!(cc.principal.unwrap().id, "alice");
        assert!(!cc.system);
        let sys = CallContext::of(&system_request(()));
        assert!(sys.system && sys.preview_opt_in);
    }
}
