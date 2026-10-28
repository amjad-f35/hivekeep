//! Listener filter on the inner MITM listener: picks the filter chain for a
//! tunnelled connection.

use egress_policy_core::keys::{FILTER_CHAIN_KEY, POLICY_HASH_SHARED};
use egress_policy_core::{stored_policy_verdict, transport_verdict};
use envoy_proxy_dynamic_modules_rust_sdk::abi::envoy_dynamic_module_type_on_listener_filter_status;
use envoy_proxy_dynamic_modules_rust_sdk::{
    envoy_log_trace, EnvoyListenerFilter, EnvoyListenerFilterConfig, ListenerFilter, ListenerFilterConfig,
};

type ListenerStatus = envoy_dynamic_module_type_on_listener_filter_status;

/// The filter takes no configuration.
pub struct ChainFilterConfig;

impl<ELF: EnvoyListenerFilter> ListenerFilterConfig<ELF> for ChainFilterConfig {
    fn new_listener_filter(&self, _envoy: &mut ELF) -> Box<dyn ListenerFilter<ELF>> {
        Box::new(ChainFilter)
    }
}

/// Runs once per connection, after tls_inspector and http_inspector have
/// set the transport and SNI.
pub struct ChainFilter;

impl<ELF: EnvoyListenerFilter> ListenerFilter<ELF> for ChainFilter {
    fn on_accept(&mut self, envoy: &mut ELF) -> ListenerStatus {
        let lossy = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        let transport = envoy.get_detected_transport_protocol().map(|b| lossy(b.as_slice()));
        let mut sni = None;
        let chain = match transport_verdict(transport.as_deref()) {
            Some(chain) => chain,
            None => {
                sni = envoy.get_requested_server_name().map(|b| lossy(b.as_slice()));
                let hash = envoy.get_filter_state_bytes(POLICY_HASH_SHARED).map(|b| b.as_slice().to_vec());
                stored_policy_verdict(sni.as_deref(), hash.as_deref())
            }
        };
        envoy_log_trace!("egress policy: transport={:?} sni={:?} chain={}", transport, sni, chain.as_str());
        envoy.set_filter_state_bytes(FILTER_CHAIN_KEY, chain.as_str().as_bytes());
        ListenerStatus::Continue
    }
}

/// SDK entry point for the listener filter config.
pub fn new_listener_filter_config_fn<EC: EnvoyListenerFilterConfig, ELF: EnvoyListenerFilter>(
    _config: &mut EC,
    _name: &str,
    _config_bytes: &[u8],
) -> Option<Box<dyn ListenerFilterConfig<ELF>>> {
    Some(Box::new(ChainFilterConfig))
}

#[cfg(test)]
mod tests {
    use super::*;
    use egress_policy_core::{Admission, EgressPolicy, PolicyStore, SniMode, SniRule};
    use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyListenerFilter, MockEnvoyListenerFilterConfig};
    use std::rc::Rc;

    fn filter() -> Box<dyn ListenerFilter<MockEnvoyListenerFilter>> {
        let mut cfg = MockEnvoyListenerFilterConfig::new();
        let config = new_listener_filter_config_fn::<MockEnvoyListenerFilterConfig, MockEnvoyListenerFilter>(
            &mut cfg,
            "envoy_hivekeep_egress_policy",
            b"",
        )
        .expect("a filter config");
        let mut m = MockEnvoyListenerFilter::new();
        config.new_listener_filter(&mut m)
    }

    fn expect_chain(m: &mut MockEnvoyListenerFilter, want: &'static str) {
        m.expect_set_filter_state_bytes()
            .withf(move |key, value| key == FILTER_CHAIN_KEY && value == want.as_bytes())
            .times(1)
            .returning(|_, _| true);
    }

    fn assert_tls(sni: Option<&'static [u8]>, hash_state: Option<Vec<u8>>, want: &'static str) {
        let mut m = MockEnvoyListenerFilter::new();
        m.expect_get_detected_transport_protocol().returning(|| Some(EnvoyBuffer::new(b"tls")));
        m.expect_get_requested_server_name().returning(move || sni.map(EnvoyBuffer::new));
        m.expect_get_filter_state_bytes()
            .withf(|key| key == POLICY_HASH_SHARED)
            .returning(move |_| hash_state.as_deref().map(EnvoyBuffer::new));
        expect_chain(&mut m, want);
        assert_eq!(filter().on_accept(&mut m), ListenerStatus::Continue);
    }

    fn interned(rules: &[(&str, SniMode)]) -> (Rc<EgressPolicy>, Vec<u8>) {
        let p = EgressPolicy::new(rules.iter().map(|(p, m)| SniRule { pattern: (*p).into(), mode: *m }).collect());
        let rc = match PolicyStore::with_local(|s| s.admit(Rc::new(p))) {
            Admission::Stored(rc) | Admission::Shared(rc) => rc,
            Admission::Collision => panic!("collision"),
        };
        let state = rc.hash.to_string().into_bytes();
        (rc, state)
    }

    fn mail_rules() -> (Rc<EgressPolicy>, Vec<u8>) {
        interned(&[("api.mail.test", SniMode::Mitm), ("*.mail.test", SniMode::Mitm)])
    }

    fn assert_transport(transport: Option<&'static [u8]>, want: &'static str) {
        let mut m = MockEnvoyListenerFilter::new();
        m.expect_get_detected_transport_protocol().returning(move || transport.map(EnvoyBuffer::new));
        expect_chain(&mut m, want);
        let mut f = filter();
        assert_eq!(f.on_accept(&mut m), ListenerStatus::Continue);
        assert_eq!(f.on_data(&mut m, 0), ListenerStatus::Continue);
    }

    #[test]
    fn non_tls_transports() {
        assert_transport(None, "cleartext");
        assert_transport(Some(b"raw_buffer"), "cleartext");
        assert_transport(Some(b"quic"), "denied");
    }

    #[test]
    fn tls_sni_against_the_interned_policy() {
        let (_held, state) = mail_rules();
        assert_tls(Some(b"api.mail.test"), Some(state.clone()), "mitm");
        assert_tls(Some(b"inbox.mail.test"), Some(state.clone()), "mitm");
        assert_tls(Some(b"API.Mail.TEST"), Some(state.clone()), "mitm");
        assert_tls(Some(b"mail.test"), Some(state.clone()), "denied");
        assert_tls(Some(b"www.example.com"), Some(state.clone()), "denied");
        assert_tls(None, Some(state), "denied");
    }

    #[test]
    fn tls_without_a_usable_policy_is_denied() {
        let (_empty, state) = interned(&[]);
        assert_tls(Some(b"api.mail.test"), Some(state), "denied");
        assert_tls(Some(b"api.mail.test"), Some(b"not-a-hash".to_vec()), "denied");
        assert_tls(Some(b"api.mail.test"), Some(b"9999999999999999999".to_vec()), "denied");
        assert_tls(Some(b"api.mail.test"), None, "denied");
    }

    #[test]
    fn passthrough_rules_pick_the_passthrough_chain() {
        let (_held, state) = interned(&[("pinned.mail.test", SniMode::Passthrough)]);
        assert_tls(Some(b"pinned.mail.test"), Some(state), "passthrough");
    }
}
