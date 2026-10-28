//! Names shared with the gate and with gate-egress.yaml. Changing any of
//! them is a wire change: the gate writes the metadata, and the Envoy
//! configuration copies and matches the filter state.

/// Dynamic metadata namespace the gate's ext_proc answer puts the policy in.
pub const POLICY_METADATA_NAMESPACE: &str = "io.hivekeep.policy.egress";

/// Key under [`POLICY_METADATA_NAMESPACE`] holding the policy JSON.
pub const POLICY_JSON_KEY: &str = "policy_json";

/// Filter state the CONNECT leg writes the policy hash to. It is local to
/// that stream; a set_filter_state filter later in the chain copies it to
/// [`POLICY_HASH_SHARED`] with upstream sharing, which the module API cannot
/// do itself yet.
pub const POLICY_HASH_LOCAL: &[u8] = b"io.hivekeep.policy.egress.hash.not_shared";

/// Filter state the inner listener reads the policy hash from.
pub const POLICY_HASH_SHARED: &[u8] = b"io.hivekeep.policy.egress.hash";

/// Filter state the inner listener writes the chosen chain name to; the
/// listener's filter_chain_matcher switches on it.
pub const FILTER_CHAIN_KEY: &[u8] = b"io.hivekeep.egress.filter_chain";

/// Gauge: distinct policies interned on this worker thread (Envoy sums the
/// threads).
pub const STAT_STORE_SIZE: &str = "hk_egress.policy_store_size";

/// Counter: CONNECT requests whose policy metadata was missing or invalid.
pub const STAT_PARSE_ERRORS: &str = "hk_egress.parse_errors";

/// Counter: CONNECT requests that reused an already interned policy.
pub const STAT_POLICY_SHARED: &str = "hk_egress.policy_shared";
