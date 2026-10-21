//! The identity provider's routes:
//!
//! * `GET /.well-known/openid-configuration`: the discovery document;
//! * `GET /openid/v1/jwks`: the key set;
//! * `GET /healthz`: liveness.
//!
//! Both documents are rebuilt from the key source on every request, so a
//! rotated pool is published at once, and a source that cannot be read
//! answers 500 rather than a stale or empty set. `HEAD` is answered like
//! `GET` without a body; other methods get 405.

use bytes::Bytes;
use hivekeep_identity::jwt::authority::JwtSigner;
use hivekeep_identity::oidc::{IdentityProvider, DISCOVERY_PATH, JWKS_PATH};
use http::{header, Method, StatusCode};
use http_body_util::Full;

/// Path of the liveness endpoint.
pub const HEALTHZ_PATH: &str = "/healthz";

/// Response type of the server.
pub type HttpResponse = http::Response<Full<Bytes>>;

/// Serves one issuer's documents.
pub struct IdpServer<S> {
    idp: IdentityProvider<S>,
}

#[derive(Clone, Copy)]
enum Route {
    Discovery,
    Jwks,
    Healthz,
}

fn text(status: StatusCode, body: &'static str) -> HttpResponse {
    let mut r = http::Response::new(Full::new(Bytes::from_static(body.as_bytes())));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    r
}

impl<S: JwtSigner> IdpServer<S> {
    /// A server publishing `keys` for `issuer`, which must be
    /// `https://<host>`. Fails if the keys cannot be published now.
    pub fn new(issuer: &str, keys: S) -> hivekeep_identity::Result<Self> {
        Ok(IdpServer {
            idp: IdentityProvider::new(issuer, keys)?,
        })
    }

    /// Answers a request for `path` with `method`.
    pub fn handle(&self, method: &Method, path: &str) -> HttpResponse {
        let route = match path {
            DISCOVERY_PATH => Route::Discovery,
            JWKS_PATH => Route::Jwks,
            HEALTHZ_PATH => Route::Healthz,
            _ => return text(StatusCode::NOT_FOUND, "404 page not found\n"),
        };
        if method != Method::GET && method != Method::HEAD {
            let mut r = text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed\n");
            r.headers_mut()
                .insert(header::ALLOW, header::HeaderValue::from_static("GET, HEAD"));
            return r;
        }
        let doc = match route {
            Route::Healthz => return http::Response::new(Full::new(Bytes::new())),
            Route::Discovery => self.idp.discovery(),
            Route::Jwks => self.idp.jwks(),
        };
        let doc = match doc {
            Ok(doc) => doc,
            Err(e) => {
                tracing::error!(err = %e, "Cannot publish the actor JWT keys");
                return text(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "cannot publish the key set\n",
                );
            }
        };
        let body = if method == Method::HEAD {
            Bytes::new()
        } else {
            Bytes::from(doc)
        };
        let mut r = http::Response::new(Full::new(body));
        r.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json; charset=utf-8"),
        );
        r
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
