//! The canonical install layout: namespace, Service and ServiceAccount
//! names, and the SPIFFE identities derived from them.
//!
//! Components take these names from here (or from a flag defaulting to
//! them) so a relocated install changes them in one place.

/// Namespace the control plane is installed into by default.
pub const SYSTEM_NAMESPACE: &str = "hivekeep-system";
/// Service fronting hivekeep-api.
pub const API_SERVICE_NAME: &str = "hivekeep-api";
/// Service fronting the gate ingress router.
pub const ROUTER_SERVICE_NAME: &str = "gate-router";
/// Service fronting the actor identity provider.
pub const IDP_SERVICE_NAME: &str = "idp";
/// ServiceAccount out-of-cluster clients mint tokens from.
pub const CLIENT_SERVICE_ACCOUNT: &str = "hivekeep-client";

/// Trust domain of the pod-identity signer.
pub const POD_TRUST_DOMAIN: &str = "cluster.local";
/// ServiceAccount of the keeper DaemonSet.
pub const KEEPER_SERVICE_ACCOUNT: &str = "hivekeep-keeper";
/// ServiceAccount of the gate ingress router.
pub const ROUTER_SERVICE_ACCOUNT: &str = "hivekeep-gate-router";
/// ServiceAccount of the gate egress proxy.
pub const EGRESS_SERVICE_ACCOUNT: &str = "hivekeep-gate-egress";
/// ServiceAccount of the controller.
pub const CONTROLLER_SERVICE_ACCOUNT: &str = "hivekeep-controller";
/// ServiceAccount of hivekeep-api.
pub const API_SERVER_SERVICE_ACCOUNT: &str = "hivekeep-api";
/// ServiceAccount worker pods run as when their pool names none; the
/// controller creates it in the pool's namespace.
pub const WORKER_SERVICE_ACCOUNT: &str = "hivekeep-worker";

/// Downward-API variable a pod learns its namespace from.
pub const POD_NAMESPACE_ENV: &str = "POD_NAMESPACE";

/// The namespace this pod runs in, falling back to the install default.
pub fn namespace_from_pod_env() -> String {
    match std::env::var(POD_NAMESPACE_ENV) {
        Ok(ns) if !ns.is_empty() => ns,
        _ => SYSTEM_NAMESPACE.to_string(),
    }
}

/// The default `iss` of actor JWTs: the in-cluster URL of the identity
/// provider Service.
pub fn actor_jwt_issuer(namespace: &str) -> String {
    format!("https://{IDP_SERVICE_NAME}.{namespace}.svc")
}

/// In-cluster host name of hivekeep-api.
pub fn api_service_host(namespace: &str) -> String {
    format!("{API_SERVICE_NAME}.{namespace}.svc")
}

/// SPIFFE ID the pod-identity signer issues to a ServiceAccount.
pub fn spiffe_id(namespace: &str, service_account: &str) -> String {
    format!("spiffe://{POD_TRUST_DOMAIN}/ns/{namespace}/sa/{service_account}")
}

/// SPIFFE ID of the keeper.
pub fn keeper_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, KEEPER_SERVICE_ACCOUNT)
}

/// SPIFFE ID of the gate ingress router.
pub fn router_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, ROUTER_SERVICE_ACCOUNT)
}

/// SPIFFE ID of the gate egress proxy.
pub fn egress_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, EGRESS_SERVICE_ACCOUNT)
}

/// SPIFFE ID of the controller.
pub fn controller_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, CONTROLLER_SERVICE_ACCOUNT)
}

/// SPIFFE ID of hivekeep-api.
pub fn api_server_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, API_SERVER_SERVICE_ACCOUNT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities() {
        assert_eq!(
            keeper_spiffe_id("hivekeep-system"),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-keeper"
        );
        assert_eq!(api_service_host("x"), "hivekeep-api.x.svc");
        assert_eq!(actor_jwt_issuer("x"), "https://idp.x.svc");
    }
}
