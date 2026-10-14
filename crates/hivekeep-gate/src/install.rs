//! Names and paths of a default Hivekeep install that the gate and the
//! sandbox tunnel rely on. Each can be overridden by a flag.

use hivekeep_identity::pod_cert::service_account_spiffe_id;

/// The namespace Hivekeep's system components run in.
pub const SYSTEM_NAMESPACE: &str = "hivekeep-system";
/// The gate router's Service (and Deployment) name.
pub const ROUTER_SERVICE_NAME: &str = "gate-router";
/// Service account of the gate router.
pub const ROUTER_SERVICE_ACCOUNT: &str = "hivekeep-gate-router";
/// Service account of the egress gateway.
pub const EGRESS_SERVICE_ACCOUNT: &str = "hivekeep-gate-egress";
/// Service account of the node agent.
pub const KEEPER_SERVICE_ACCOUNT: &str = "hivekeep-keeper";
/// The control plane's in-cluster service name.
pub const API_SERVICE_NAME: &str = "hivekeep-api";
/// The keeper's node-local socket serving workers.
pub const WORKER_SUPPORT_SOCKET: &str = "/var/lib/hivekeep/worker-support.sock";
/// Environment variable carrying the Pod's namespace.
pub const POD_NAMESPACE_ENV: &str = "POD_NAMESPACE";

/// The Pod's namespace from the environment, or [`SYSTEM_NAMESPACE`].
pub fn namespace_from_env() -> String {
    std::env::var(POD_NAMESPACE_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| SYSTEM_NAMESPACE.to_string())
}

/// SPIFFE ID of the gate router in `namespace`.
pub fn router_spiffe_id(namespace: &str) -> String {
    service_account_spiffe_id(namespace, ROUTER_SERVICE_ACCOUNT)
}

/// SPIFFE ID of the egress gateway in `namespace`.
pub fn egress_spiffe_id(namespace: &str) -> String {
    service_account_spiffe_id(namespace, EGRESS_SERVICE_ACCOUNT)
}

/// SPIFFE ID of the keeper in `namespace`.
pub fn keeper_spiffe_id(namespace: &str) -> String {
    service_account_spiffe_id(namespace, KEEPER_SERVICE_ACCOUNT)
}

/// The control plane's in-cluster host name in `namespace`.
pub fn api_service_host(namespace: &str) -> String {
    format!("{API_SERVICE_NAME}.{namespace}.svc")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spiffe_ids() {
        assert_eq!(
            router_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-router"
        );
        assert_eq!(
            keeper_spiffe_id("ns"),
            "spiffe://cluster.local/ns/ns/sa/hivekeep-keeper"
        );
        assert_eq!(api_service_host("ns"), "hivekeep-api.ns.svc");
    }
}
