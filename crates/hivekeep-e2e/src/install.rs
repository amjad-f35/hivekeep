//! Names of the installed components the harness reaches: namespaces,
//! Services, Deployments, ports and signers. They follow the install
//! manifests; [`crate::env::resource_name`] applies a packaging prefix.

/// The canonical install namespace.
pub const SYSTEM_NAMESPACE: &str = hivekeep_k8s::install_defaults::SYSTEM_NAMESPACE;

/// The hivekeep-api Service.
pub const API_SERVICE: &str = hivekeep_k8s::install_defaults::API_SERVICE_NAME;
/// The hivekeep-api Deployment.
pub const API_DEPLOYMENT: &str = "hivekeep-api";
/// The controller Deployment.
pub const CONTROLLER_DEPLOYMENT: &str = "hivekeep-controller";
/// The ServiceAccount the harness mints its API bearer token from.
pub const CLIENT_SERVICE_ACCOUNT: &str = hivekeep_k8s::install_defaults::CLIENT_SERVICE_ACCOUNT;
/// The keeper's ServiceAccount.
pub const KEEPER_SERVICE_ACCOUNT: &str = hivekeep_k8s::install_defaults::KEEPER_SERVICE_ACCOUNT;
/// The ingress gate router Service (and Deployment).
pub const ROUTER_SERVICE: &str = hivekeep_k8s::install_defaults::ROUTER_SERVICE_NAME;
/// The `app` label of the egress gate's pods.
pub const EGRESS_APP_LABEL: &str = "app=gate-egress";
/// The bundled credential provider Deployment.
pub const CREDENTIAL_PROVIDER_DEPLOYMENT: &str = "k8s-credential-provider";

/// hivekeep-api's gRPC Service port.
pub const API_SERVICE_PORT: u16 = 443;
/// The router's HTTPS Service port.
pub const ROUTER_HTTPS_SERVICE_PORT: u16 = 443;
/// The router's CONNECT-over-TLS Service port, a listener separate from
/// HTTPS: only it accepts CONNECT.
pub const ROUTER_CONNECT_TLS_SERVICE_PORT: u16 = 8444;
/// The router's status page port; loopback-only in the pod and not
/// published by the Service, so it is forwarded to the pod directly.
pub const ROUTER_STATUS_PORT: u16 = 4040;
/// The egress Envoy's admin port.
pub const EGRESS_ENVOY_ADMIN_PORT: u16 = 15000;

/// The request header naming the Actor a gate request is for.
pub const TARGET_ACTOR_HEADER: &str = "hk-target-actor";

/// Signer of the serving certificates of in-cluster Services.
pub const SERVICE_DNS_SIGNER: &str = "servicedns.podcert.hivekeep.io/identity";
/// The ClusterTrustBundle the pod-certificate controller publishes for
/// [`SERVICE_DNS_SIGNER`].
pub const SERVICE_DNS_TRUST_BUNDLE: &str = "servicedns.podcert.hivekeep.io:identity:primary-bundle";
/// Selects the live (not canarying) trust bundles.
pub const LIVE_BUNDLE_SELECTOR: &str = "podcert.hivekeep.io/canarying=live";
/// Signer of pod-identity (SPIFFE) client certificates.
pub const POD_IDENTITY_SIGNER: &str = hivekeep_identity::pod_cert::POD_IDENTITY_SIGNER;
/// Trust domain of pod SPIFFE IDs.
pub const TRUST_DOMAIN: &str = hivekeep_k8s::install_defaults::POD_TRUST_DOMAIN;

/// The API's DNS name: what its serving certificate covers and the audience
/// of the bearer token minted for it.
pub fn api_server_name(namespace: &str) -> String {
    format!("{}.{namespace}.svc", crate::env::resource_name(API_SERVICE))
}

/// The SPIFFE ID the pod-identity signer issues for `service_account` in
/// `namespace`.
pub fn spiffe_id(namespace: &str, service_account: &str) -> String {
    format!("spiffe://{TRUST_DOMAIN}/ns/{namespace}/sa/{service_account}")
}
