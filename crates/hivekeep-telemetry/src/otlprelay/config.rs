//! The relay's upstream configuration, resolved from the standard OTLP
//! exporter variables of the keeper's own environment.

use std::path::Path;

use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};

use crate::env::Env;

/// The collector every signal exports to unless overridden.
pub const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The traces collector.
pub const TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
/// The metrics collector.
pub const METRICS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
/// The logs collector.
pub const LOGS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// Upstream compression, `gzip` or `none`.
pub const COMPRESSION_ENV: &str = "OTEL_EXPORTER_OTLP_COMPRESSION";
/// Traces compression.
pub const TRACES_COMPRESSION_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION";
/// Metrics compression.
pub const METRICS_COMPRESSION_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION";
/// Logs compression.
pub const LOGS_COMPRESSION_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION";

/// Headers the collector expects (an API key, a tenant). Unlike endpoint
/// and compression these are per call, so signals may differ and are
/// resolved separately.
pub const HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_HEADERS";
/// Traces headers; replaces [`HEADERS_ENV`] rather than merging with it.
pub const TRACES_HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_HEADERS";
/// Metrics headers.
pub const METRICS_HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_HEADERS";
/// Logs headers.
pub const LOGS_HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_HEADERS";

/// The port assumed when an endpoint names none.
pub const OTLP_DEFAULT_PORT: &str = "4317";

/// Upstream compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Uncompressed.
    None,
    /// gzip.
    Gzip,
}

impl Compression {
    /// `none` or `gzip`.
    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
        }
    }
}

/// A configuration the relay refuses to start with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// Rejects a relative socket path: the keeper and the worker would resolve
/// it against different working directories.
pub fn validate_socket_path(sock: &Path) -> Result<(), ConfigError> {
    if !sock.is_absolute() {
        return Err(ConfigError(format!(
            "the OTLP relay socket path {:?} is relative; it must be absolute, since the keeper and the worker would otherwise resolve it against different working directories",
            sock.display().to_string()
        )));
    }
    Ok(())
}

/// Parses the list the OTLP headers variables carry
/// (`key1=value1,key2=value2`, values percent-encoded). Keys are
/// lower-cased, as gRPC metadata keys are case-insensitive. Errors name the
/// position, never the text: any entry may be a credential.
pub fn parse_headers(raw: &str) -> Result<MetadataMap, ConfigError> {
    let mut md = MetadataMap::new();
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(md);
    }
    for (i, pair) in raw.split(',').enumerate() {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            return Err(ConfigError(format!(
                "OTLP header {} is not in key=value form",
                i + 1
            )));
        };
        let key = key.trim().to_lowercase();
        if key.is_empty() {
            return Err(ConfigError(format!(
                "OTLP header {} has an empty name",
                i + 1
            )));
        }
        // Percent-decoding makes a value with "," or "=" (a base64 token)
        // expressible at all; '+' stays a literal plus.
        let decoded = percent_decode_strict(value.trim()).ok_or_else(|| {
            ConfigError(format!(
                "OTLP header {key:?} has a value that is not valid percent-encoding"
            ))
        })?;
        let k = AsciiMetadataKey::from_bytes(key.as_bytes())
            .map_err(|_| ConfigError(format!("OTLP header {} has an invalid name", i + 1)))?;
        let v = AsciiMetadataValue::try_from(decoded.as_str()).map_err(|_| {
            ConfigError(format!(
                "OTLP header {key:?} has a value that cannot be sent as metadata"
            ))
        })?;
        md.append(k, v);
    }
    Ok(md)
}

/// Percent-decodes `s`, failing on a `%` not followed by two hex digits.
fn percent_decode_strict(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if !(b.get(i + 1)?.is_ascii_hexdigit() && b.get(i + 2)?.is_ascii_hexdigit()) {
                return None;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .ok()
        .map(|c| c.into_owned())
}

/// The headers for one signal: the signal-specific variable replaces the
/// generic one, per the OTLP spec.
pub fn upstream_headers(env: &dyn Env, signal_env: &str) -> Result<MetadataMap, ConfigError> {
    let (name, raw) = match env.trimmed(signal_env) {
        s if !s.is_empty() => (signal_env, s),
        _ => (HEADERS_ENV, env.get(HEADERS_ENV)),
    };
    parse_headers(&raw).map_err(|e| ConfigError(format!("while reading {name}: {e}")))
}

/// Resolves each signal to its own variable, falling back to the generic
/// one, and requires every resolved value to agree: the relay carries every
/// signal over one connection. The error names the variables both values
/// came from.
pub fn resolve_per_signal(
    env: &dyn Env,
    what: &str,
    generic_env: &str,
    signal_envs: &[&str],
) -> Result<String, ConfigError> {
    let generic = env.trimmed(generic_env);
    let mut resolved = String::new();
    let mut resolved_env = "";
    for &signal in signal_envs {
        let (v, src) = match env.trimmed(signal) {
            s if !s.is_empty() => (s, signal),
            _ => (generic.clone(), generic_env),
        };
        if v.is_empty() {
            continue;
        }
        if !resolved.is_empty() && v != resolved {
            return Err(ConfigError(format!(
                "signal-specific {what} conflict: {resolved_env}={resolved:?} vs {src}={v:?}; the relay carries every signal over one connection"
            )));
        }
        resolved = v;
        resolved_env = src;
    }
    Ok(resolved)
}

/// The upstream compression.
pub fn upstream_compression(env: &dyn Env) -> Result<Compression, ConfigError> {
    let v = resolve_per_signal(
        env,
        "compression settings",
        COMPRESSION_ENV,
        &[
            TRACES_COMPRESSION_ENV,
            METRICS_COMPRESSION_ENV,
            LOGS_COMPRESSION_ENV,
        ],
    )?;
    match v.as_str() {
        "" | "none" => Ok(Compression::None),
        "gzip" => Ok(Compression::Gzip),
        other => Err(ConfigError(format!(
            "unsupported OTLP compression {other:?}, want gzip or none"
        ))),
    }
}

/// The collector the relay forwards to, as a bare `host:port`, or empty
/// when none is configured.
pub fn upstream_target(env: &dyn Env) -> Result<String, ConfigError> {
    let v = resolve_per_signal(
        env,
        "endpoints",
        ENDPOINT_ENV,
        &[TRACES_ENDPOINT_ENV, METRICS_ENDPOINT_ENV, LOGS_ENDPOINT_ENV],
    )?;
    if v.is_empty() {
        return Ok(v);
    }
    normalize_endpoint(&v)
}

/// Splits `host:port`, accepting bracketed IPv6 literals. `None` when no
/// port is given.
fn split_host_port(s: &str) -> Option<(String, String)> {
    if let Some(rest) = s.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after.strip_prefix(':')?;
        return Some((host.to_string(), port.to_string()));
    }
    let (host, port) = s.rsplit_once(':')?;
    if host.contains(':') {
        return None;
    }
    Some((host.to_string(), port.to_string()))
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Accepts a bare `host:port` and the URL form the OTLP variables carry,
/// and returns `host:port`. `https` is rejected rather than downgraded: the
/// relay forwards in plaintext, so honoring it would ship telemetry in the
/// clear to an endpoint that asked for TLS.
pub fn normalize_endpoint(addr: &str) -> Result<String, ConfigError> {
    let mut hostport = addr;
    if let Some((scheme, rest)) = addr.split_once("://") {
        match scheme {
            "http" => {}
            "https" => {
                return Err(ConfigError(format!(
                    "OTLP collector endpoint {addr:?} uses https, which the relay does not support: it forwards over a plaintext gRPC connection. Point it at an http:// endpoint"
                )))
            }
            other => {
                return Err(ConfigError(format!(
                    "OTLP collector endpoint {addr:?} has unsupported scheme {other:?}, want http"
                )))
            }
        }
        hostport = rest.split(['/', '?', '#']).next().unwrap_or("");
    }
    let (host, port) = match split_host_port(hostport) {
        Some((h, p)) => (h, p),
        None => (
            hostport.trim_matches(['[', ']']).to_string(),
            OTLP_DEFAULT_PORT.to_string(),
        ),
    };
    if host.is_empty() {
        return Err(ConfigError(format!(
            "OTLP collector endpoint {addr:?} names no host"
        )));
    }
    Ok(join_host_port(&host, &port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::MapEnv;

    #[test]
    fn normalize_endpoint_table() {
        let dp = OTLP_DEFAULT_PORT;
        for (name, input, want, want_err) in [
            (
                "host and port",
                "otel-collector.hivekeep-system.svc:4317",
                "otel-collector.hivekeep-system.svc:4317".to_string(),
                "",
            ),
            (
                "bare host defaults the port",
                "otel-collector",
                format!("otel-collector:{dp}"),
                "",
            ),
            (
                "http url",
                "http://otel-collector:4317",
                "otel-collector:4317".into(),
                "",
            ),
            (
                "http url without port",
                "http://otel-collector",
                format!("otel-collector:{dp}"),
                "",
            ),
            ("ipv6 literal", "[::1]:4317", "[::1]:4317".into(), ""),
            (
                "ipv6 literal without port",
                "[::1]",
                format!("[::1]:{dp}"),
                "",
            ),
            (
                "https rejected",
                "https://otel-collector:4317",
                String::new(),
                "https",
            ),
            (
                "unknown scheme rejected",
                "grpc://otel-collector:4317",
                String::new(),
                "unsupported scheme",
            ),
            (
                "empty host rejected",
                "http://:4317",
                String::new(),
                "names no host",
            ),
        ] {
            let got = normalize_endpoint(input);
            if want_err.is_empty() {
                assert_eq!(got.unwrap(), want, "{name}");
            } else {
                let e = got.unwrap_err();
                assert!(e.0.contains(want_err), "{name}: {e}");
            }
        }
    }

    fn env4(
        generic: &str,
        traces: &str,
        metrics: &str,
        logs: &str,
        names: [&'static str; 4],
    ) -> MapEnv {
        MapEnv::new([
            (names[0], generic),
            (names[1], traces),
            (names[2], metrics),
            (names[3], logs),
        ])
    }

    const EP: [&str; 4] = [
        ENDPOINT_ENV,
        TRACES_ENDPOINT_ENV,
        METRICS_ENDPOINT_ENV,
        LOGS_ENDPOINT_ENV,
    ];
    const CP: [&str; 4] = [
        COMPRESSION_ENV,
        TRACES_COMPRESSION_ENV,
        METRICS_COMPRESSION_ENV,
        LOGS_COMPRESSION_ENV,
    ];

    #[test]
    fn upstream_target_table() {
        #[allow(clippy::type_complexity)]
        let cases: &[(&str, &str, &str, &str, &str, &str, bool, &str, &str)] = &[
            ("unset", "", "", "", "", "", false, "", ""),
            (
                "generic only",
                "collector:4317",
                "",
                "",
                "",
                "collector:4317",
                false,
                "",
                "",
            ),
            (
                "logs specific agrees",
                "collector:4317",
                "",
                "",
                "collector:4317",
                "collector:4317",
                false,
                "",
                "",
            ),
            (
                "logs specific conflicts with generic",
                "generic:4317",
                "",
                "",
                "logs-only:4317",
                "",
                true,
                "",
                "",
            ),
            (
                "logs specific conflicts with traces",
                "",
                "a:4317",
                "",
                "b:4317",
                "",
                true,
                "",
                "",
            ),
            (
                "signal specific overrides generic",
                "generic:4317",
                "specific:4317",
                "specific:4317",
                "specific:4317",
                "specific:4317",
                false,
                "",
                "",
            ),
            (
                "two overrides and an unset signal conflict with generic",
                "generic:4317",
                "specific:4317",
                "specific:4317",
                "",
                "",
                true,
                ENDPOINT_ENV,
                LOGS_ENDPOINT_ENV,
            ),
            (
                "generic conflicts with different traces specific",
                "generic:4317",
                "traces-only:4317",
                "",
                "",
                "",
                true,
                "",
                "",
            ),
            (
                "generic conflicts with different metrics specific",
                "generic:4317",
                "",
                "metrics-only:4317",
                "",
                "",
                true,
                "",
                "",
            ),
            (
                "matching generic and signal specific",
                "collector:4317",
                "collector:4317",
                "collector:4317",
                "",
                "collector:4317",
                false,
                "",
                "",
            ),
            (
                "traces specific alone",
                "",
                "specific:4317",
                "",
                "",
                "specific:4317",
                false,
                "",
                "",
            ),
            (
                "metrics specific alone",
                "",
                "",
                "specific:4317",
                "",
                "specific:4317",
                false,
                "",
                "",
            ),
            (
                "conflicting signals rejected",
                "",
                "a:4317",
                "b:4317",
                "",
                "",
                true,
                "",
                "",
            ),
            (
                "whitespace trimmed",
                "  collector:4317  ",
                "",
                "",
                "",
                "collector:4317",
                false,
                "",
                "",
            ),
        ];
        for (name, g, t, m, l, want, want_err, err_has, err_lacks) in cases {
            let got = upstream_target(&env4(g, t, m, l, EP));
            if *want_err {
                let e = got.expect_err(name).0;
                if !err_has.is_empty() {
                    assert!(e.contains(err_has), "{name}: {e}");
                }
                if !err_lacks.is_empty() {
                    assert!(!e.contains(err_lacks), "{name}: {e}");
                }
            } else {
                assert_eq!(&got.unwrap(), want, "{name}");
            }
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn upstream_compression_table() {
        use Compression::*;
        let cases: &[(&str, &str, &str, &str, &str, Option<Compression>)] = &[
            ("unset", "", "", "", "", Some(None)),
            ("logs specific gzip", "", "", "", "gzip", Some(Gzip)),
            (
                "logs conflicts with metrics",
                "",
                "",
                "gzip",
                "none",
                Option::None,
            ),
            ("generic gzip", "gzip", "", "", "", Some(Gzip)),
            ("generic none", "none", "", "", "", Some(None)),
            ("traces specific gzip", "", "gzip", "", "", Some(Gzip)),
            ("metrics specific gzip", "", "", "gzip", "", Some(Gzip)),
            ("both specific gzip", "", "gzip", "gzip", "", Some(Gzip)),
            (
                "conflicting compression rejected",
                "",
                "gzip",
                "none",
                "",
                Option::None,
            ),
            (
                "generic conflicts with traces",
                "none",
                "gzip",
                "",
                "",
                Option::None,
            ),
            (
                "invalid compression rejected",
                "zstd",
                "",
                "",
                "",
                Option::None,
            ),
        ];
        for (name, g, t, m, l, want) in cases {
            let got = upstream_compression(&env4(g, t, m, l, CP)).ok();
            assert_eq!(&got, want, "{name}");
        }
    }

    fn get(md: &MetadataMap, k: &str) -> Vec<String> {
        md.get_all(k)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect()
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn parse_headers_table() {
        let cases: &[(&str, &str, &[(&str, &str)], bool)] = &[
            ("empty", "", &[], false),
            ("single", "api-key=secret", &[("api-key", "secret")], false),
            (
                "multiple with spaces",
                "api-key=secret, x-tenant = sub ",
                &[("api-key", "secret"), ("x-tenant", "sub")],
                false,
            ),
            (
                "key lower-cased",
                "X-Tenant=sub",
                &[("x-tenant", "sub")],
                false,
            ),
            (
                "percent-encoded value",
                "authorization=Bearer%20abc%3D%3D",
                &[("authorization", "Bearer abc==")],
                false,
            ),
            (
                "plus preserved",
                "authorization=Bearer+token%20val",
                &[("authorization", "Bearer+token val")],
                false,
            ),
            ("empty value kept", "x-tenant=", &[("x-tenant", "")], false),
            (
                "trailing comma tolerated",
                "api-key=secret,",
                &[("api-key", "secret")],
                false,
            ),
            ("missing equals rejected", "api-key", &[], true),
            ("empty name rejected", "=secret", &[], true),
            ("bad percent-encoding rejected", "api-key=%zz", &[], true),
        ];
        for (name, raw, want, want_err) in cases {
            match parse_headers(raw) {
                Err(e) => {
                    assert!(*want_err, "{name}: {e}");
                    assert!(
                        !e.0.contains("secret"),
                        "{name}: error leaks the value: {e}"
                    );
                }
                Ok(md) => {
                    assert!(!*want_err, "{name}: no error");
                    assert_eq!(md.len(), want.len(), "{name}");
                    for (k, v) in *want {
                        assert_eq!(get(&md, k), vec![v.to_string()], "{name}[{k}]");
                    }
                }
            }
        }
    }

    #[test]
    fn upstream_headers_signal_override() {
        let env = MapEnv::new([
            (HEADERS_ENV, "api-key=generic"),
            (TRACES_HEADERS_ENV, "x-tenant=traces"),
        ]);
        let traces = upstream_headers(&env, TRACES_HEADERS_ENV).unwrap();
        assert!(
            get(&traces, "api-key").is_empty(),
            "the signal variable replaces the generic one"
        );
        assert_eq!(get(&traces, "x-tenant"), vec!["traces"]);
        let metrics = upstream_headers(&env, METRICS_HEADERS_ENV).unwrap();
        assert_eq!(get(&metrics, "api-key"), vec!["generic"]);
    }

    #[test]
    fn socket_path_must_be_absolute() {
        let e = validate_socket_path(Path::new("relative/r.sock")).unwrap_err();
        assert!(e.0.contains("absolute"));
        assert!(validate_socket_path(Path::new("/run/r.sock")).is_ok());
    }
}
