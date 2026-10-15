//! Connecting to the Hivekeep control plane.

use std::time::Duration;

use hivekeep_proto::api::v1::control_client::ControlClient;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::k8s_channel::{balanced_channel, is_k8s_target};
use crate::k8s_resolver::KubeSliceSource;

/// Turns a gRPC dial target (`k8s:///host:port`, `dns:///host:port`,
/// `https://host:port` or `host:port`) into an https endpoint URI and the
/// host to verify.
pub fn endpoint_uri(target: &str) -> Result<(String, String), String> {
    let rest = ["k8s:///", "dns:///", "https://"]
        .iter()
        .find_map(|p| target.strip_prefix(p))
        .unwrap_or(target);
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err(format!("invalid control-plane address {target:?}"));
    }
    let host = match rest.strip_prefix('[') {
        Some(r) => r.split_once(']').map(|(h, _)| h).unwrap_or(r),
        None => rest.rsplit_once(':').map_or(rest, |(h, _)| h),
    };
    let with_port = if rest
        .rsplit_once(':')
        .is_some_and(|(_, p)| p.parse::<u16>().is_ok())
    {
        rest.to_string()
    } else {
        format!("{rest}:443")
    };
    Ok((format!("https://{with_port}"), host.to_string()))
}

/// The host to verify on a `k8s:///` target's certificate: the name as
/// written for DNS forms (`hivekeep-api.hivekeep-system.svc`), `<svc>.<ns>.svc` for
/// the `<ns>/<svc>` form.
pub fn k8s_tls_host(target: &str) -> Result<String, String> {
    let (_, host) = endpoint_uri(target)?;
    if !host.contains('/') {
        return Ok(host);
    }
    let t = crate::k8s_resolver::parse_target(target).map_err(|e| e.to_string())?;
    Ok(format!("{}.{}.svc", t.service, t.namespace))
}

/// A lazily connecting, mutually authenticated client for the control plane.
///
/// With a Kubernetes client, a `k8s:///` target is resolved from the
/// Service's EndpointSlices and calls are balanced across its ready pods;
/// every other target (or no client) dials the name directly.
pub fn dial_control_plane(
    target: &str,
    ca_file: &str,
    client_cert: &str,
    server_name: &str,
    kube: Option<&kube::Client>,
) -> Result<ControlClient<Channel>, String> {
    if ca_file.is_empty() || client_cert.is_empty() {
        return Err("--hivekeep-api-ca-file and --hivekeep-api-client-cert are required".into());
    }
    let balanced = kube.filter(|_| is_k8s_target(target));
    let (uri, host) = endpoint_uri(target)?;
    let host = if balanced.is_some() {
        k8s_tls_host(target)?
    } else {
        host
    };
    let ca = std::fs::read(ca_file).map_err(|e| format!("reading {ca_file}: {e}"))?;
    let bundle = std::fs::read(client_cert).map_err(|e| format!("reading {client_cert}: {e}"))?;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(bundle.clone(), bundle))
        .domain_name(if server_name.is_empty() {
            host
        } else {
            server_name.to_string()
        });
    let endpoint = move |uri: String| -> Result<Endpoint, String> {
        Ok(Endpoint::from_shared(uri)
            .map_err(|e| e.to_string())?
            .tls_config(tls.clone())
            .map_err(|e| crate::tunnel::keeper_dial::error_chain(&e))?
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(30)))
    };
    let channel = match balanced {
        Some(client) => balanced_channel(
            &KubeSliceSource(client.clone()),
            target,
            move |addr: &str| endpoint(format!("https://{addr}")),
        )?,
        None => endpoint(uri)?.connect_lazy(),
    };
    Ok(ControlClient::new(channel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_uri_cases() {
        for (t, uri, host) in [
            (
                "k8s:///hivekeep-api.hivekeep-system.svc:443",
                "https://hivekeep-api.hivekeep-system.svc:443",
                "hivekeep-api.hivekeep-system.svc",
            ),
            (
                "dns:///hivekeep-api.hivekeep-system.svc:8443",
                "https://hivekeep-api.hivekeep-system.svc:8443",
                "hivekeep-api.hivekeep-system.svc",
            ),
            ("api:443", "https://api:443", "api"),
            ("api", "https://api:443", "api"),
            ("[::1]:443", "https://[::1]:443", "::1"),
        ] {
            assert_eq!(
                endpoint_uri(t).unwrap(),
                (uri.to_string(), host.to_string()),
                "{t}"
            );
        }
        assert!(endpoint_uri("k8s:///").is_err());
        assert!(dial_control_plane("api:443", "", "", "", None).is_err());
    }

    #[test]
    fn k8s_tls_host_cases() {
        for (t, host) in [
            (
                "k8s:///api.hivekeep-system.svc:443",
                "api.hivekeep-system.svc",
            ),
            ("k8s:///hivekeep-system/api:443", "api.hivekeep-system.svc"),
            ("k8s:///hivekeep-system/api", "api.hivekeep-system.svc"),
        ] {
            assert_eq!(k8s_tls_host(t).unwrap(), host, "{t}");
        }
    }
}
