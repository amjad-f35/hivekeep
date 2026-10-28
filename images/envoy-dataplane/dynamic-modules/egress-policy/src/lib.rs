//! The egress-policy Envoy dynamic module of the gate's egress Envoy.
//!
//! Two filters share a per-worker-thread policy store
//! (`egress_policy_core::store`):
//!
//! - `envoy_hivekeep_egress_policy_store`, an HTTP filter on the outer
//!   CONNECT leg after ext_proc, interns the SNI rules the gate answered
//!   with and writes their hash to filter state ([`store_filter`]);
//! - `envoy_hivekeep_egress_policy`, a listener filter on the inner MITM
//!   listener after tls_inspector, looks the rules up by that hash and
//!   writes the filter chain to use ([`listener_filter`]).
//!
//! All decisions live in `egress_policy_core`; this crate only adapts them
//! to the SDK's callbacks.

pub mod listener_filter;
pub mod store_filter;

use envoy_proxy_dynamic_modules_rust_sdk::declare_all_init_functions;

declare_all_init_functions!(
    init,
    http: store_filter::new_http_filter_config_fn,
    listener: listener_filter::new_listener_filter_config_fn,
);

/// Module load hook; there is no global state to set up.
fn init() -> bool {
    true
}

#[cfg(test)]
mod tests {
    #[test]
    fn module_loads() {
        assert!(super::init());
    }
}
