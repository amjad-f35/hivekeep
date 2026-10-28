//! HTTP filter on the CONNECT leg: interns the stream's policy and
//! publishes its hash.

use std::rc::Rc;

use egress_policy_core::keys::{
    POLICY_HASH_LOCAL, POLICY_JSON_KEY, POLICY_METADATA_NAMESPACE, STAT_PARSE_ERRORS, STAT_POLICY_SHARED,
    STAT_STORE_SIZE,
};
use egress_policy_core::{Admission, EgressPolicy, PolicyStore};
use envoy_proxy_dynamic_modules_rust_sdk::abi::{
    envoy_dynamic_module_type_metadata_source, envoy_dynamic_module_type_on_http_filter_request_headers_status,
};
use envoy_proxy_dynamic_modules_rust_sdk::{
    envoy_log_trace, EnvoyCounterId, EnvoyGaugeId, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter,
    HttpFilterConfig,
};

type HeadersStatus = envoy_dynamic_module_type_on_http_filter_request_headers_status;

/// Stat handles, defined once per filter config.
#[derive(Clone, Copy)]
pub struct Stats {
    pub store_size: EnvoyGaugeId,
    pub parse_errors: EnvoyCounterId,
    pub policy_shared: EnvoyCounterId,
}

/// The filter's configuration: it takes none beyond its stats.
pub struct StoreFilterConfig {
    pub stats: Stats,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for StoreFilterConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(StoreFilter::new(self.stats))
    }
}

/// One CONNECT stream. Holds its policy for as long as the stream lives, so
/// the tunnelled connection inside can always find it.
pub struct StoreFilter {
    pub stats: Stats,
    pub policy: Option<Rc<EgressPolicy>>,
}

impl StoreFilter {
    pub fn new(stats: Stats) -> Self {
        Self { stats, policy: None }
    }

    /// Reads and parses the policy the gate put in dynamic metadata, then
    /// blanks the metadata value so the JSON is not kept per stream.
    fn take_policy<EHF: EnvoyHttpFilter>(envoy: &mut EHF) -> Option<Rc<EgressPolicy>> {
        let json = envoy.get_metadata_string(
            envoy_dynamic_module_type_metadata_source::Dynamic,
            POLICY_METADATA_NAMESPACE,
            POLICY_JSON_KEY,
        )?;
        let policy = EgressPolicy::parse(json.as_slice())?;
        envoy.set_dynamic_metadata_string(POLICY_METADATA_NAMESPACE, POLICY_JSON_KEY, "");
        Some(policy)
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for StoreFilter {
    fn on_request_headers(&mut self, envoy: &mut EHF, _end_of_stream: bool) -> HeadersStatus {
        let Some(policy) = Self::take_policy(envoy) else {
            // No hash is published, so the inner listener denies any TLS
            // in this tunnel.
            let _ = envoy.increment_counter(self.stats.parse_errors, 1);
            return HeadersStatus::Continue;
        };
        envoy_log_trace!("egress policy store: {:?}", policy);
        let policy = match PolicyStore::with_local(|s| s.admit(policy)) {
            Admission::Stored(p) => {
                let _ = envoy.increase_gauge(self.stats.store_size, 1);
                p
            }
            Admission::Shared(p) => {
                let _ = envoy.increment_counter(self.stats.policy_shared, 1);
                p
            }
            Admission::Collision => {
                // Fail closed rather than hand out a hash that names
                // someone else's rules.
                envoy.send_response(400, &[], Some(b"hash collision"), Some("hash_collision"));
                return HeadersStatus::StopIteration;
            }
        };
        // set_filter_state later in the chain copies this to the
        // upstream-shared key the inner listener reads.
        envoy.set_filter_state_bytes(POLICY_HASH_LOCAL, policy.hash.to_string().as_bytes());
        self.policy = Some(policy);
        HeadersStatus::Continue
    }

    // The last callback before the filter is dropped.
    fn on_stream_complete(&mut self, envoy: &mut EHF) {
        if let Some(policy) = self.policy.take() {
            if PolicyStore::with_local(|s| s.release(&policy)) {
                let _ = envoy.decrease_gauge(self.stats.store_size, 1);
            }
        }
    }
}

/// SDK entry point for the HTTP filter config.
pub fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    config: &mut EC,
    _name: &str,
    _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    let stats = Stats {
        store_size: config.define_gauge(STAT_STORE_SIZE).ok()?,
        parse_errors: config.define_counter(STAT_PARSE_ERRORS).ok()?,
        policy_shared: config.define_counter(STAT_POLICY_SHARED).ok()?,
    };
    Some(Box::new(StoreFilterConfig { stats }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use egress_policy_core::{SniMode, SniRule};
    use envoy_proxy_dynamic_modules_rust_sdk::{EnvoyBuffer, MockEnvoyHttpFilter};

    const STATS: Stats =
        Stats { store_size: EnvoyGaugeId(1), parse_errors: EnvoyCounterId(2), policy_shared: EnvoyCounterId(3) };
    const RULES: &[u8] =
        br#"{"rules":[{"pattern":"api.example.com","mode":"mitm"},{"pattern":"*.example.com","mode":"passthrough"}]}"#;

    fn expect_policy_read(m: &mut MockEnvoyHttpFilter, json: &'static [u8]) {
        m.expect_get_metadata_string()
            .withf(|src, ns, key| {
                *src == envoy_dynamic_module_type_metadata_source::Dynamic
                    && ns == POLICY_METADATA_NAMESPACE
                    && key == POLICY_JSON_KEY
            })
            .returning(move |_, _, _| Some(EnvoyBuffer::new(json)))
            .once();
        m.expect_set_dynamic_metadata_string()
            .withf(|ns, key, value| ns == POLICY_METADATA_NAMESPACE && key == POLICY_JSON_KEY && value.is_empty())
            .return_const(())
            .once();
    }

    fn expect_hash_written(m: &mut MockEnvoyHttpFilter, want: Option<u64>) {
        m.expect_set_filter_state_bytes()
            .withf(move |key, value| {
                let got = std::str::from_utf8(value).ok().and_then(|s| s.parse::<u64>().ok());
                key == POLICY_HASH_LOCAL && got.is_some() && (want.is_none() || got == want)
            })
            .returning(|_, _| true)
            .once();
    }

    #[test]
    fn interns_shares_and_releases_the_policy() {
        let mut m = MockEnvoyHttpFilter::default();
        let mut first = StoreFilter::new(STATS);
        expect_policy_read(&mut m, RULES);
        m.expect_increase_gauge().withf(|id, v| *id == EnvoyGaugeId(1) && *v == 1).returning(|_, _| Ok(())).once();
        expect_hash_written(&mut m, None);
        assert_eq!(first.on_request_headers(&mut m, false), HeadersStatus::Continue);
        let held = first.policy.clone().unwrap();
        assert_eq!(
            held.rules,
            vec![
                SniRule { pattern: "api.example.com".into(), mode: SniMode::Mitm },
                SniRule { pattern: "*.example.com".into(), mode: SniMode::Passthrough },
            ]
        );
        let hash = held.hash;
        drop(held);

        // A second stream with the same rules reuses the entry.
        let mut second = StoreFilter::new(STATS);
        expect_policy_read(&mut m, RULES);
        m.expect_increment_counter().withf(|id, v| *id == EnvoyCounterId(3) && *v == 1).returning(|_, _| Ok(())).once();
        expect_hash_written(&mut m, Some(hash));
        assert_eq!(second.on_request_headers(&mut m, false), HeadersStatus::Continue);
        assert!(Rc::ptr_eq(first.policy.as_ref().unwrap(), second.policy.as_ref().unwrap()));

        // The first stream ending keeps the entry for the second.
        first.on_stream_complete(&mut m);
        assert!(PolicyStore::with_local(|s| s.lookup(hash)).is_some());

        // The last one removes it.
        m.expect_decrease_gauge().withf(|id, v| *id == EnvoyGaugeId(1) && *v == 1).returning(|_, _| Ok(())).once();
        second.on_stream_complete(&mut m);
        assert!(PolicyStore::with_local(|s| s.lookup(hash)).is_none());
    }

    #[test]
    fn missing_or_invalid_metadata_counts_a_parse_error() {
        let mut f = StoreFilter::new(STATS);
        let mut m = MockEnvoyHttpFilter::default();
        m.expect_get_metadata_string().returning(|_, _, _| None).once();
        m.expect_increment_counter().withf(|id, v| *id == EnvoyCounterId(2) && *v == 1).returning(|_, _| Ok(())).once();
        assert_eq!(f.on_request_headers(&mut m, false), HeadersStatus::Continue);
        assert!(f.policy.is_none());

        m.expect_get_metadata_string().returning(|_, _, _| Some(EnvoyBuffer::new(b"not-json"))).once();
        m.expect_increment_counter().withf(|id, v| *id == EnvoyCounterId(2) && *v == 1).returning(|_, _| Ok(())).once();
        assert_eq!(f.on_request_headers(&mut m, false), HeadersStatus::Continue);
        assert!(f.policy.is_none());
        // Nothing to release.
        f.on_stream_complete(&mut m);
    }

    #[test]
    fn a_hash_collision_fails_the_stream() {
        let json: &'static [u8] = br#"{"rules":[{"pattern":"collide.example.com","mode":"mitm"}]}"#;
        let incoming = EgressPolicy::parse(json).unwrap();
        let mut squatter = EgressPolicy::new(vec![SniRule { pattern: "other.test".into(), mode: SniMode::Passthrough }]);
        squatter.hash = incoming.hash;
        let squatter = Rc::new(squatter);
        assert!(matches!(PolicyStore::with_local(|s| s.admit(Rc::clone(&squatter))), Admission::Stored(_)));

        let mut f = StoreFilter::new(STATS);
        let mut m = MockEnvoyHttpFilter::default();
        expect_policy_read(&mut m, json);
        m.expect_send_response()
            .withf(|code, headers, body, details| {
                *code == 400 && headers.is_empty() && body == &Some(&b"hash collision"[..]) && details == &Some("hash_collision")
            })
            .return_const(())
            .once();
        assert_eq!(f.on_request_headers(&mut m, false), HeadersStatus::StopIteration);
        assert!(f.policy.is_none());
        assert!(PolicyStore::with_local(|s| s.release(&squatter)));
    }
}
