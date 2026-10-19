//! Names of the installed control-plane components, overridable through the
//! environment for non-default installs.

use std::env;

/// Environment variable naming the Hivekeep system namespace.
pub const NAMESPACE_ENV: &str = "HK_NAMESPACE";
/// Environment variable naming the `hivekeep-api` Service.
pub const API_SERVICE_ENV: &str = "HK_API_SERVICE_NAME";
/// Environment variable naming the gate Service.
pub const GATE_SERVICE_ENV: &str = "HK_GATE_SERVICE_NAME";

/// Default system namespace.
pub const DEFAULT_NAMESPACE: &str = "hivekeep-system";
/// Default `hivekeep-api` Service name.
pub const DEFAULT_API_SERVICE: &str = "hivekeep-api";
/// Default gate Service name.
pub const DEFAULT_GATE_SERVICE: &str = "gate-router";

/// The gate's CONNECT-over-TLS port.
pub const GATE_CONNECT_TLS_PORT: u16 = 8444;

/// Request header naming the Actor a CONNECT tunnel targets, as `<hive>/<name>`.
pub const TARGET_ACTOR_HEADER: &str = "hk-target-actor";

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

/// The system namespace.
pub fn system_namespace() -> String {
    env_or(NAMESPACE_ENV, DEFAULT_NAMESPACE)
}

/// The `hivekeep-api` Service name.
pub fn api_service_name() -> String {
    env_or(API_SERVICE_ENV, DEFAULT_API_SERVICE)
}

/// The gate Service name.
pub fn gate_service_name() -> String {
    env_or(GATE_SERVICE_ENV, DEFAULT_GATE_SERVICE)
}

/// The DNS name `hivekeep-api`'s serving certificate is issued for.
pub fn api_server_name() -> String {
    format!("{}.{}.svc", api_service_name(), system_namespace())
}

/// The DNS name the gate's serving certificate is issued for.
pub fn gate_server_name() -> String {
    format!("{}.{}.svc", gate_service_name(), system_namespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_names_are_service_dns_names() {
        // Only meaningful when the overrides are unset, as in CI.
        if env::var_os(NAMESPACE_ENV).is_none() && env::var_os(API_SERVICE_ENV).is_none() {
            assert_eq!(api_server_name(), "hivekeep-api.hivekeep-system.svc");
        }
        if env::var_os(NAMESPACE_ENV).is_none() && env::var_os(GATE_SERVICE_ENV).is_none() {
            assert_eq!(gate_server_name(), "gate-router.hivekeep-system.svc");
        }
    }
}
