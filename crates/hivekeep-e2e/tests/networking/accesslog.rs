//! Parsing the egress gate's access-log lines.

use hivekeep_core::resource::ActorRef;

use crate::networking::{
    access_log_field, protocol_gateway_access_log_matches, GatewayAccessLogLine,
};

#[test]
fn access_log_fields() {
    let line = r#"authority=10.96.0.7:80 code=200 peer_san="spiffe://hivekeep-actor.local/worker-for-actor/networking-e2e/ws-1""#;
    for (key, want) in [
        ("authority", "10.96.0.7:80"),
        ("code", "200"),
        (
            "peer_san",
            "spiffe://hivekeep-actor.local/worker-for-actor/networking-e2e/ws-1",
        ),
    ] {
        assert_eq!(access_log_field(line, key).as_deref(), Some(want), "{key}");
    }
    // Keys match whole.
    assert_eq!(
        access_log_field("response.code=503 code=200", "code").as_deref(),
        Some("200")
    );
}

#[test]
fn protocol_gateway_access_log_matching() {
    let aref = ActorRef::new("networking-e2e", "ws-1");
    let cases = [
        (
            "envoy success",
            "envoy",
            r#"authority=10.96.0.7:80 code=200 peer_san="spiffe://hivekeep-actor.local/worker-for-actor/networking-e2e/ws-1""#,
            true,
        ),
        (
            "envoy wrong actor identity",
            "envoy",
            r#"authority=10.96.0.7:80 code=200 peer_san="spiffe://hivekeep-actor.local/worker-for-actor/networking-e2e/ws-2""#,
            false,
        ),
        (
            "envoy wrong hive identity",
            "envoy",
            r#"authority=10.96.0.7:80 code=200 peer_san="spiffe://hivekeep-actor.local/worker-for-actor/other/ws-1""#,
            false,
        ),
        (
            "envoy actor certificate identity is not the transport peer",
            "envoy",
            r#"authority=10.96.0.7:80 code=200 peer_san="spiffe://hivekeep-actor.local/actor/networking-e2e/ws-1""#,
            false,
        ),
    ];
    for (name, container, line, want) in cases {
        let entry = GatewayAccessLogLine {
            container: container.into(),
            text: line.into(),
        };
        assert_eq!(
            protocol_gateway_access_log_matches(&entry, &aref, "10.96.0.7:80"),
            want,
            "{name}"
        );
    }
}
