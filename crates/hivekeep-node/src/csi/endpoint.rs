//! CSI driver endpoints: `unix://` sockets, `tcp://host:port` and
//! `dns:///host:port` addresses.

use std::fmt;
use std::path::PathBuf;

/// Where a CSI driver listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// A unix socket; the original `unix://...` URI is kept for messages.
    Unix {
        /// The `unix://` URI as configured.
        uri: String,
        /// The socket path.
        path: PathBuf,
    },
    /// A TCP `host:port` (either part may be empty, to be filled in by the
    /// dialer's defaults).
    Tcp(String),
    /// A name resolved through DNS; the original `dns:` URI.
    Dns(String),
}

impl Endpoint {
    /// The scheme: `unix`, `tcp` or `dns`.
    pub fn scheme(&self) -> &'static str {
        match self {
            Endpoint::Unix { .. } => "unix",
            Endpoint::Tcp(_) => "tcp",
            Endpoint::Dns(_) => "dns",
        }
    }

    /// The dial target: the unix URI, the TCP `host:port`, or the DNS URI.
    pub fn target(&self) -> &str {
        match self {
            Endpoint::Unix { uri, .. } => uri,
            Endpoint::Tcp(addr) => addr,
            Endpoint::Dns(uri) => uri,
        }
    }

    /// The `host:port` a DNS endpoint names (the part after the optional
    /// `//authority/`), or the TCP address.
    pub fn host_port(&self) -> Option<&str> {
        match self {
            Endpoint::Unix { .. } => None,
            Endpoint::Tcp(addr) => Some(addr),
            Endpoint::Dns(uri) => {
                let rest = uri.strip_prefix("dns:").unwrap_or(uri);
                Some(match rest.strip_prefix("//") {
                    Some(after) => after.split_once('/').map(|(_, hp)| hp).unwrap_or(after),
                    None => rest,
                })
            }
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.target())
    }
}

/// Parses a driver endpoint. A unix endpoint needs a path, a TCP one a
/// host or port; any other scheme, or none, is refused.
pub fn parse_endpoint(endpoint: &str) -> Result<Endpoint, String> {
    let Some((scheme, rest)) = endpoint.split_once(':') else {
        return Err(format!(
            "unsupported scheme \"\", must be unix, tcp or dns: {endpoint}"
        ));
    };
    if scheme.is_empty()
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    {
        return Err(format!(
            "failed to parse endpoint: missing protocol scheme in {endpoint:?}"
        ));
    }
    // The authority runs from "//" to the next "/"; the path is the rest.
    let (authority, path) = match rest.strip_prefix("//") {
        Some(after) => match after.find('/') {
            Some(i) => (&after[..i], &after[i..]),
            None => (after, ""),
        },
        None => ("", rest),
    };
    match scheme.to_ascii_lowercase().as_str() {
        "unix" => {
            if path.is_empty() {
                return Err(format!("unix endpoint missing path: {endpoint}"));
            }
            Ok(Endpoint::Unix {
                uri: endpoint.to_string(),
                path: PathBuf::from(path),
            })
        }
        "tcp" => {
            if authority.is_empty() {
                return Err(format!("tcp endpoint missing host:port: {endpoint}"));
            }
            Ok(Endpoint::Tcp(authority.to_string()))
        }
        "dns" => Ok(Endpoint::Dns(endpoint.to_string())),
        other => Err(format!(
            "unsupported scheme {other:?}, must be unix, tcp or dns"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoint_cases() {
        struct Case {
            name: &'static str,
            endpoint: &'static str,
            want: Option<(&'static str, &'static str)>,
        }
        let cases = [
            Case {
                name: "valid unix absolute",
                endpoint: "unix:///run/csi.sock",
                want: Some(("unix", "unix:///run/csi.sock")),
            },
            Case {
                name: "valid tcp loopback",
                endpoint: "tcp://127.0.0.1:50051",
                want: Some(("tcp", "127.0.0.1:50051")),
            },
            Case {
                name: "valid tcp hostname",
                endpoint: "tcp://localhost:50051",
                want: Some(("tcp", "localhost:50051")),
            },
            Case {
                name: "valid dns",
                endpoint: "dns:///csi-service:9000",
                want: Some(("dns", "dns:///csi-service:9000")),
            },
            Case {
                name: "invalid scheme",
                endpoint: "http://localhost:50051",
                want: None,
            },
            Case {
                name: "no scheme",
                endpoint: "/run/csi.sock",
                want: None,
            },
            Case {
                name: "tcp missing port",
                endpoint: "tcp://127.0.0.1",
                want: Some(("tcp", "127.0.0.1")),
            },
            Case {
                name: "tcp missing host",
                endpoint: "tcp://:50051",
                want: Some(("tcp", ":50051")),
            },
            Case {
                name: "unix missing path",
                endpoint: "unix://",
                want: None,
            },
        ];
        for c in cases {
            match (parse_endpoint(c.endpoint), c.want) {
                (Ok(ep), Some((scheme, target))) => {
                    assert_eq!(ep.scheme(), scheme, "{}", c.name);
                    assert_eq!(ep.target(), target, "{}", c.name);
                }
                (Err(_), None) => {}
                (got, want) => panic!(
                    "{}: parse_endpoint({:?}) = {got:?}, want {want:?}",
                    c.name, c.endpoint
                ),
            }
        }
    }

    #[test]
    fn unix_path_and_dns_host_port() {
        let ep = parse_endpoint("unix:///var/lib/kubelet/plugins/d/csi.sock").unwrap();
        assert_eq!(
            ep,
            Endpoint::Unix {
                uri: "unix:///var/lib/kubelet/plugins/d/csi.sock".into(),
                path: "/var/lib/kubelet/plugins/d/csi.sock".into()
            }
        );
        assert_eq!(
            parse_endpoint("dns:///csi-service:9000")
                .unwrap()
                .host_port(),
            Some("csi-service:9000")
        );
        assert_eq!(
            parse_endpoint("dns://8.8.8.8/csi-service:9000")
                .unwrap()
                .host_port(),
            Some("csi-service:9000")
        );
        assert_eq!(
            parse_endpoint("dns:csi-service:9000").unwrap().host_port(),
            Some("csi-service:9000")
        );
        // A relative unix "path" is an authority, not a path.
        assert!(parse_endpoint("unix://csi.sock").is_err());
    }
}
