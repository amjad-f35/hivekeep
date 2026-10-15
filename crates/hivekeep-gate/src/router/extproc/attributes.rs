//! Names shared between the router's external processor and the Envoy
//! configuration it serves: filter-state keys, metadata namespaces,
//! attributes and substitution formats.

/// Filter state holding the Actor a CONNECT names (`<hive>/<actor>`).
pub const TARGET_ACTOR_FILTER_STATE_KEY: &str = "io.hivekeep.target.actor";
/// Filter state holding the CONNECT authority.
pub const CONNECT_AUTHORITY_FILTER_STATE_KEY: &str = "io.hivekeep.connect.authority";
/// The attribute reading [`TARGET_ACTOR_FILTER_STATE_KEY`].
pub const TARGET_ACTOR_FILTER_STATE_ATTRIBUTE: &str = "filter_state['io.hivekeep.target.actor']";
/// The attribute reading [`CONNECT_AUTHORITY_FILTER_STATE_KEY`].
pub const CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE: &str =
    "filter_state['io.hivekeep.connect.authority']";
/// Filter state holding the verified Actor identity of an egress tunnel.
pub const ACTOR_IDENTITY_FILTER_STATE_KEY: &str = "io.hivekeep.actor.identity";
/// The attribute reading [`ACTOR_IDENTITY_FILTER_STATE_KEY`].
pub const ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE: &str =
    "filter_state['io.hivekeep.actor.identity']";
/// Dynamic metadata namespace of egress decisions.
pub const EGRESS_METADATA_NAMESPACE: &str = "io.hivekeep.egress";
/// Key of the dialed port in [`EGRESS_METADATA_NAMESPACE`].
pub const EGRESS_DIALED_PORT_KEY: &str = "dialed_port";
/// Dynamic metadata namespace carrying the compiled egress policy.
pub const EGRESS_POLICY_METADATA_NAMESPACE: &str = "io.hivekeep.policy.egress";
/// Filter state holding the egress policy's hash.
pub const EGRESS_POLICY_HASH_FILTER_STATE_KEY: &str = "io.hivekeep.policy.egress.hash";
/// Key of the policy JSON in [`EGRESS_POLICY_METADATA_NAMESPACE`].
pub const EGRESS_POLICY_JSON_KEY: &str = "policy_json";
/// Key of the SNI rules list.
pub const EGRESS_SNI_RULES_KEY: &str = "rules";
/// Key of an SNI rule's pattern.
pub const EGRESS_SNI_RULE_PATTERN_KEY: &str = "pattern";
/// Key of an SNI rule's mode.
pub const EGRESS_SNI_RULE_MODE_KEY: &str = "mode";
/// Filter state naming the inner egress filter chain to use.
pub const EGRESS_FILTER_CHAIN_FILTER_STATE_KEY: &str = "io.hivekeep.egress.filter_chain";
/// Inner chain: terminate TLS.
pub const EGRESS_FILTER_CHAIN_MITM: &str = "mitm";
/// Inner chain: pass TLS through.
pub const EGRESS_FILTER_CHAIN_PASSTHROUGH: &str = "passthrough";
/// Inner chain: cleartext HTTP.
pub const EGRESS_FILTER_CHAIN_CLEARTEXT: &str = "cleartext";
/// Inner chain: refuse the connection.
pub const EGRESS_FILTER_CHAIN_DENIED: &str = "denied";
/// Key of the dial decision in [`EGRESS_METADATA_NAMESPACE`].
pub const EGRESS_DIAL_KEY: &str = "dial";
/// Dial by name.
pub const EGRESS_DIAL_NAME: &str = "name";
/// Key of the host to dial in [`EGRESS_METADATA_NAMESPACE`].
pub const EGRESS_DIAL_HOST_KEY: &str = "host";
/// Attribute an alternative dataplane sets to mark the direction.
pub const DIRECTION_ATTRIBUTE: &str = "io.hivekeep.extproc.direction";
/// Attribute naming the filter chain a request arrived on.
pub const FILTER_CHAIN_NAME_ATTRIBUTE: &str = "xds.filter_chain_name";
/// Substitution format reading the dialed port.
pub const EGRESS_DIALED_PORT_FORMAT: &str = "%DYNAMIC_METADATA(io.hivekeep.egress:dialed_port)%";
/// Filter state Envoy reads the upstream port from.
pub const UPSTREAM_DYNAMIC_PORT_FILTER_STATE_KEY: &str = "envoy.upstream.dynamic_port";
/// Substitution format reading the host to dial.
pub const EGRESS_DIAL_HOST_FORMAT: &str = "%DYNAMIC_METADATA(io.hivekeep.egress:host)%";
/// Filter state Envoy reads the upstream host from.
pub const UPSTREAM_DYNAMIC_HOST_FILTER_STATE_KEY: &str = "envoy.upstream.dynamic_host";
/// Substitution format reading the policy hash.
pub const EGRESS_POLICY_HASH_FORMAT: &str =
    "%FILTER_STATE(io.hivekeep.policy.egress.hash.not_shared:PLAIN)%";
/// Filter state carrying the original destination of a connection.
pub const ORIGINAL_DST_FILTER_STATE_KEY: &str =
    "envoy.network.transport_socket.original_dst_address";
/// Attribute holding the TLS SNI.
pub const REQUESTED_SERVER_NAME_ATTRIBUTE: &str = "connection.requested_server_name";
/// The `:authority` pseudo-header.
pub const AUTHORITY_HEADER: &str = ":authority";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_names_agree() {
        assert_eq!(
            TARGET_ACTOR_FILTER_STATE_ATTRIBUTE,
            format!("filter_state['{TARGET_ACTOR_FILTER_STATE_KEY}']")
        );
        assert_eq!(
            CONNECT_AUTHORITY_FILTER_STATE_ATTRIBUTE,
            format!("filter_state['{CONNECT_AUTHORITY_FILTER_STATE_KEY}']")
        );
        assert_eq!(
            ACTOR_IDENTITY_FILTER_STATE_ATTRIBUTE,
            format!("filter_state['{ACTOR_IDENTITY_FILTER_STATE_KEY}']")
        );
        assert_eq!(
            EGRESS_DIALED_PORT_FORMAT,
            format!("%DYNAMIC_METADATA({EGRESS_METADATA_NAMESPACE}:{EGRESS_DIALED_PORT_KEY})%")
        );
        assert_eq!(
            EGRESS_DIAL_HOST_FORMAT,
            format!("%DYNAMIC_METADATA({EGRESS_METADATA_NAMESPACE}:{EGRESS_DIAL_HOST_KEY})%")
        );
        assert_eq!(
            EGRESS_POLICY_HASH_FORMAT,
            format!("%FILTER_STATE({EGRESS_POLICY_HASH_FILTER_STATE_KEY}.not_shared:PLAIN)%")
        );
    }
}
