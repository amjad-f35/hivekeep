//! Which dataplane fronts the router, and how its readiness is probed.

use std::fmt;

/// The proxy the router configures (or sits behind).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dataplane {
    /// Envoy, configured over xDS by this router.
    #[default]
    Envoy,
}

impl Dataplane {
    /// Parses a `--gate-dataplane` value; empty means Envoy.
    pub fn parse(s: &str) -> Result<Dataplane, String> {
        match s {
            "" | "envoy" => Ok(Dataplane::Envoy),
            other => Err(format!("--gate-dataplane must be \"envoy\", got {other:?}")),
        }
    }

    /// The flag spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Dataplane::Envoy => "envoy",
        }
    }

    /// The readiness endpoint (`host:port`, path) and its expected body.
    pub fn health_check(self) -> DataplaneHealthCheck {
        match self {
            Dataplane::Envoy => DataplaneHealthCheck {
                addr: "localhost:9901",
                path: "/ready",
                expected_body: "LIVE",
            },
        }
    }
}

impl fmt::Display for Dataplane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A dataplane readiness probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataplaneHealthCheck {
    /// `host:port` of the admin endpoint.
    pub addr: &'static str,
    /// Path probed.
    pub path: &'static str,
    /// Body a ready dataplane answers.
    pub expected_body: &'static str,
}

impl DataplaneHealthCheck {
    /// The probe as a URL.
    pub fn url(&self) -> String {
        format!("http://{}{}", self.addr, self.path)
    }
}

/// Where the xDS server listens: loopback only, for the co-located Envoy.
pub fn xds_listen_address(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_checks_and_parsing() {
        assert_eq!(
            Dataplane::Envoy.health_check().url(),
            "http://localhost:9901/ready"
        );
        assert_eq!(Dataplane::parse("").unwrap(), Dataplane::Envoy);
        assert!(Dataplane::parse("nginx").is_err());
        assert_eq!(xds_listen_address(18000), "127.0.0.1:18000");
    }
}
