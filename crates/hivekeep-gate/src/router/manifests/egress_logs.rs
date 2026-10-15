//! The egress gateway's CONNECT routes and access logs.

use std::collections::BTreeMap;

use serde_json::Value;

use super::support::*;

/// Every route in the bootstrap matched by connect_matcher.
fn connect_routes(tree: &Value) -> Vec<&Value> {
    let mut out = Vec::new();
    for (_, chain) in all_chains(tree) {
        for f in list(chain, "filters") {
            for vh in list(
                child(child(f, "typed_config"), "route_config"),
                "virtual_hosts",
            ) {
                out.extend(
                    list(vh, "routes")
                        .into_iter()
                        .filter(|r| is_set(child(r, "match"), "connect_matcher")),
                );
            }
        }
    }
    out
}

/// Envoy applies a route timeout to a CONNECT tunnel's whole lifetime, so a
/// route left at the 15s default would cut every long-lived outbound
/// connection after 15s. The tunnel routes must disable it.
#[test]
fn egress_manifests_disable_the_connect_timeout() {
    let tree = egress_bootstrap();
    let routes = connect_routes(&tree);
    assert!(!routes.is_empty(), "found no connect_matcher route; the manifest changed shape and this test is checking nothing");
    for r in routes {
        let action = child(r, "route");
        let cluster = s(action, "cluster");
        let Some(timeout) = action.get("timeout").and_then(Value::as_str) else {
            panic!("connect_matcher route to cluster {cluster:?} sets no timeout, so it falls back to Envoy's 15s default");
        };
        let d = humantime::parse_duration(timeout)
            .unwrap_or_else(|e| panic!("connect_matcher route to cluster {cluster:?} has timeout {timeout:?}, which is not a duration: {e}"));
        assert!(d.is_zero(), "connect_matcher route to cluster {cluster:?} has timeout {timeout}; it must be 0 to disable it");
    }
}

/// The filter on the access logs of the per-request legs.
const WANT_SAMPLED_LOG_FILTER: &str = r#"
or_filter:
  filters:
  - status_code_filter:
      comparison:
        op: GE
        value: {default_value: 400, runtime_key: access_log.min_status}
  - response_flag_filter: {}
  - and_filter:
      filters:
      - header_filter:
          header:
            name: content-type
            string_match: {prefix: application/grpc}
      - grpc_status_filter: {exclude: true, statuses: [OK]}
  - runtime_filter:
      runtime_key: access_log.sample
      percent_sampled: {numerator: 1, denominator: HUNDRED}
      use_independent_randomness: true
"#;

/// The access logs of every network filter in every chain, grouped by their
/// json_format leg; the CONNECT log has a text format and is grouped as
/// "connect".
fn chain_access_logs_by_leg(tree: &Value) -> BTreeMap<String, Vec<&Value>> {
    let mut out: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for (_, chain) in all_chains(tree) {
        for f in list(chain, "filters") {
            for al in list(child(f, "typed_config"), "access_log") {
                let leg = s(
                    child(
                        child(child(al, "typed_config"), "log_format"),
                        "json_format",
                    ),
                    "leg",
                );
                let leg = if leg.is_empty() { "connect" } else { leg };
                out.entry(leg.to_string()).or_default().push(al);
            }
        }
    }
    out
}

/// Every actor request lands in the mitm or cleartext access log, so both keep
/// failures and a sample of the rest behind runtime keys. No other chain's log
/// may filter, since each is the only record of its events.
#[test]
fn egress_manifests_sample_the_per_request_access_logs() {
    let want = parse_yaml(WANT_SAMPLED_LOG_FILTER);
    let tree = egress_bootstrap();
    let logs = chain_access_logs_by_leg(&tree);
    let sampled = ["mitm", "cleartext"];
    for leg in sampled {
        let als = logs.get(leg).cloned().unwrap_or_default();
        assert!(!als.is_empty(), "no access log with leg {leg:?}; the manifest changed shape and this test is checking nothing");
        for al in als {
            assert_eq!(
                child(al, "filter"),
                &want,
                "access log {leg:?} filter mismatch"
            );
        }
    }
    for (leg, als) in &logs {
        if sampled.contains(&leg.as_str()) {
            continue;
        }
        for al in als {
            assert!(
                !is_set(al, "filter"),
                "access log {leg:?} has a filter; only {sampled:?} may"
            );
        }
    }
}

/// An access-log command that prints the path with its query string.
fn query_path_command(s: &str) -> Option<String> {
    let mut rest = s;
    while let Some(i) = rest.find("%REQ(") {
        let after = &rest[i + 5..];
        if let Some(end) = after.find(')') {
            let inner = &after[..end];
            if after[end + 1..].starts_with('%') && inner.contains("PATH") {
                return Some(format!("%REQ({inner})%"));
            }
        }
        rest = after;
    }
    s.contains("%PATH(WQ").then(|| "%PATH(WQ".to_string())
}

#[test]
fn query_path_command_cases() {
    for (format, hit) in [
        ("%REQ(:PATH)%", true),
        ("%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%", true),
        ("%PATH(WQ:ORIG_OR_PATH)%", true),
        ("%PATH(NQ:ORIG_OR_PATH)%", false),
        ("%REQ(:AUTHORITY)%", false),
    ] {
        assert_eq!(query_path_command(format).is_some(), hit, "{format}");
    }
}

/// Decrypted actor traffic can carry credentials in the query, so every access
/// log prints the path without it.
#[test]
fn egress_manifests_log_paths_without_the_query() {
    fn walk(v: &Value, in_format: bool, formats: &mut usize) {
        match v {
            Value::Object(m) => {
                for (k, c) in m {
                    walk(c, in_format || k == "log_format", formats);
                }
            }
            Value::Array(a) => a.iter().for_each(|c| walk(c, in_format, formats)),
            Value::String(s) if in_format => {
                *formats += 1;
                if let Some(m) = query_path_command(s) {
                    panic!("access log format {s:?} logs the query string with {m}; use %PATH(NQ:ORIG_OR_PATH)%");
                }
            }
            _ => {}
        }
    }
    let mut formats = 0;
    walk(&egress_bootstrap(), false, &mut formats);
    assert!(
        formats > 0,
        "no access log formats found; the manifest changed shape and this test is checking nothing"
    );
}
