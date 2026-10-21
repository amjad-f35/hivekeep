//! The canonical install layout: namespace, Service and ServiceAccount names,
//! and the identities derived from them.
//!
//! Hardcoded layout assumptions fail closed in a relocated install, in ways
//! that do not point at the naming, so every component that needs one of these
//! names takes it from here (or from a flag defaulting to it).

/// Namespace the control plane is installed into by default.
pub const SYSTEM_NAMESPACE: &str = "hivekeep-system";
/// Service fronting hivekeep-api.
pub const API_SERVICE_NAME: &str = "hivekeep-api";
/// Service fronting the gate ingress router.
pub const ROUTER_SERVICE_NAME: &str = "gate-router";
/// Service fronting the actor identity provider.
pub const IDP_SERVICE_NAME: &str = "idp";
/// ServiceAccount hivectl and the installer's demo steps mint tokens from.
pub const CLIENT_SERVICE_ACCOUNT: &str = "hivekeep-client";

/// Trust domain of the pod-identity signer.
pub const KEEPER_TRUST_DOMAIN: &str = "cluster.local";
pub const KEEPER_SERVICE_ACCOUNT: &str = "hivekeep-keeper";
pub const ROUTER_SERVICE_ACCOUNT: &str = "hivekeep-gate-router";
pub const EGRESS_SERVICE_ACCOUNT: &str = "hivekeep-gate-egress";
pub const CONTROLLER_SERVICE_ACCOUNT: &str = "hivekeep-controller";
pub const API_SERVER_SERVICE_ACCOUNT: &str = "hivekeep-api";

/// Google service account snapshot transfers run as on GCS.
pub const TRANSFER_SERVICE_ACCOUNT_NAME: &str = "hivekeep-snapshot-transfer";

/// ServiceAccount worker pods run as.
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
/// provider Service. Relying parties compare it byte for byte.
pub fn actor_jwt_issuer(namespace: &str) -> String {
    format!("https://{IDP_SERVICE_NAME}.{namespace}.svc")
}

/// In-cluster host name of hivekeep-api.
pub fn api_service_host(namespace: &str) -> String {
    format!("{API_SERVICE_NAME}.{namespace}.svc")
}

/// Kubernetes username of the client ServiceAccount's tokens.
pub fn client_principal(namespace: &str) -> String {
    format!("system:serviceaccount:{namespace}:{CLIENT_SERVICE_ACCOUNT}")
}

/// SPIFFE ID the pod-identity signer issues to a ServiceAccount.
pub fn spiffe_id(namespace: &str, service_account: &str) -> String {
    format!("spiffe://{KEEPER_TRUST_DOMAIN}/ns/{namespace}/sa/{service_account}")
}

pub fn keeper_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, KEEPER_SERVICE_ACCOUNT)
}

pub fn router_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, ROUTER_SERVICE_ACCOUNT)
}

pub fn egress_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, EGRESS_SERVICE_ACCOUNT)
}

pub fn controller_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, CONTROLLER_SERVICE_ACCOUNT)
}

pub fn api_server_spiffe_id(namespace: &str) -> String {
    spiffe_id(namespace, API_SERVER_SERVICE_ACCOUNT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_principal_is_the_token_subject() {
        assert_eq!(
            client_principal("hivekeep-system"),
            "system:serviceaccount:hivekeep-system:hivekeep-client"
        );
    }

    #[test]
    fn keeper_spiffe_id_cases() {
        // The canonical value is effectively wire format: peers reject any
        // other string.
        assert_eq!(
            keeper_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-keeper"
        );
        assert_eq!(
            keeper_spiffe_id("team-a-hivekeep"),
            "spiffe://cluster.local/ns/team-a-hivekeep/sa/hivekeep-keeper"
        );
    }

    #[test]
    fn router_spiffe_id_default() {
        assert_eq!(
            router_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-router"
        );
    }

    #[test]
    fn egress_spiffe_id_default() {
        assert_eq!(
            egress_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-egress"
        );
    }

    #[test]
    fn controller_spiffe_id_default() {
        assert_eq!(
            controller_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-controller"
        );
    }

    #[test]
    fn api_server_spiffe_id_default() {
        assert_eq!(
            api_server_spiffe_id(SYSTEM_NAMESPACE),
            "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api"
        );
    }

    #[test]
    fn api_service_host_cases() {
        assert_eq!(
            api_service_host("hivekeep-system"),
            "hivekeep-api.hivekeep-system.svc"
        );
        assert_eq!(
            api_service_host("hivekeep-prod"),
            "hivekeep-api.hivekeep-prod.svc"
        );
    }

    #[test]
    fn actor_jwt_issuer_cases() {
        assert_eq!(
            actor_jwt_issuer(SYSTEM_NAMESPACE),
            "https://idp.hivekeep-system.svc"
        );
        assert_eq!(
            actor_jwt_issuer("team-a-hivekeep"),
            "https://idp.team-a-hivekeep.svc"
        );
    }

    #[test]
    fn namespace_from_pod_env_cases() {
        // One test owns the variable so parallel tests cannot race on it.
        std::env::set_var(POD_NAMESPACE_ENV, "");
        assert_eq!(namespace_from_pod_env(), SYSTEM_NAMESPACE);
        std::env::set_var(POD_NAMESPACE_ENV, "team-a-hivekeep");
        assert_eq!(namespace_from_pod_env(), "team-a-hivekeep");
        std::env::remove_var(POD_NAMESPACE_ENV);
    }
}
