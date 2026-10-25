//! The endpoint names the actor's loop calls; the driver's egress policy
//! allows exactly these.

/// Namespace holding the endpoint Services the benchmark deploys.
pub const TARGET_NAMESPACE: &str = "egress-tests-targets";

/// The most endpoints a run can use.
pub const MAX_ENDPOINTS: i64 = 1024;

/// DNS domain every endpoint name sits directly under.
pub const ENDPOINT_DOMAIN: &str = "egress-tests-targets.svc.cluster.local";

/// Matches every endpoint name and nothing below them: the wildcard stands
/// for exactly one leftmost label. The target's certificate is issued for it.
pub const ENDPOINT_HOST_PATTERN: &str = "*.egress-tests-targets.svc.cluster.local";

/// The name of endpoint `i`'s Service.
pub fn service_name(i: usize) -> String {
    format!("egress-target-{i}")
}

/// The DNS name of endpoint `i`.
pub fn endpoint_host(i: usize) -> String {
    format!("{}.{ENDPOINT_DOMAIN}", service_name(i))
}

/// The URL the loop requests for endpoint `i` over `scheme` (`http` or
/// `https`), each on its default port.
pub fn endpoint_url(i: usize, scheme: &str) -> String {
    format!("{scheme}://{}/", endpoint_host(i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::actor::api::{SCHEME_HTTP, SCHEME_HTTPS};

    #[test]
    fn endpoint_names() {
        assert_eq!(service_name(7), "egress-target-7");
        assert_eq!(
            endpoint_host(7),
            "egress-target-7.egress-tests-targets.svc.cluster.local"
        );
        assert_eq!(
            endpoint_url(7, SCHEME_HTTP),
            "http://egress-target-7.egress-tests-targets.svc.cluster.local/"
        );
        assert_eq!(
            endpoint_url(7, SCHEME_HTTPS),
            "https://egress-target-7.egress-tests-targets.svc.cluster.local/"
        );
        assert_eq!(
            ENDPOINT_HOST_PATTERN,
            "*.egress-tests-targets.svc.cluster.local"
        );
        assert_eq!(
            ENDPOINT_DOMAIN,
            format!("{TARGET_NAMESPACE}.svc.cluster.local")
        );
        assert_eq!(ENDPOINT_HOST_PATTERN, format!("*.{ENDPOINT_DOMAIN}"));
    }
}
