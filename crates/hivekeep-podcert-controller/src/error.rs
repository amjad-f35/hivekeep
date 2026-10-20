//! Why handling a PodCertificateRequest failed. API errors are boxed to
//! keep the error small.

use hivekeep_identity::IdentityError;

/// A failure to answer a request or to publish a signer's roots.
#[derive(Debug, thiserror::Error)]
pub enum SignerError {
    /// Another replica owns the request. Not a failure: it is looked at
    /// again once assignments may have changed.
    #[error("item not assigned to this replica")]
    NotAssigned,

    /// The request carries neither a stub CSR nor a PKIX public key.
    #[error("pod certificate request does not contain a public key")]
    NoPublicKey,

    /// The stub PKCS#10 request is malformed.
    #[error("while parsing stub PKCS#10 request: {0}")]
    StubRequest(#[source] IdentityError),

    /// The legacy PKIX public key is malformed.
    #[error("while parsing PKIX public key: {0}")]
    PkixPublicKey(#[source] IdentityError),

    /// The CA pool could not sign.
    #[error("while signing certificate: {0}")]
    Sign(#[source] IdentityError),

    /// The CA pool's roots could not be read.
    #[error("while retrieving CA pool trust anchors: {0}")]
    TrustAnchors(#[source] IdentityError),

    /// Writing the request's status failed.
    #[error("while updating PodCertificateRequest: {0}")]
    UpdateStatus(#[source] Box<kube::Error>),

    /// Listing the namespace's Services failed.
    #[error("while listing services: {0}")]
    ListServices(#[source] Box<kube::Error>),

    /// Listing the Pods a Service selects failed.
    #[error("while selecting pods for service {service:?}: {source}")]
    SelectPods {
        /// `namespace/name` of the Service.
        service: String,
        /// The API error.
        #[source]
        source: Box<kube::Error>,
    },

    /// No Service selects the Pod yet. Retried, so that a serving
    /// certificate is not issued (and cached for its lifetime) without DNS
    /// names while the Service is still being created.
    #[error("pod {namespace}/{pod} is not (yet) selected by any Service; refusing to issue a serving cert with no DNS SANs")]
    NotSelected {
        /// Namespace of the Pod.
        namespace: String,
        /// Name of the Pod.
        pod: String,
    },

    /// The request is addressed to another signer.
    #[error("unexpected signer {0:?}")]
    UnexpectedSigner(String),

    /// The request allows a lifetime below the signer's minimum.
    #[error("requested certificate lifetime must be at least one hour")]
    LifetimeTooShort,
}
