//! The scenarios: a traffic mode and how to drive it.

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A kind of traffic the generator sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// HTTPS requests over HTTP/1.1 to the tunnel's ingress listener, as
    /// the router sends them.
    IngressH1,
    /// [`Mode::IngressH1`] over HTTP/2.
    IngressH2,
    /// Per unit: a TLS connection and a CONNECT tunnel to the tunnel's
    /// CONNECT listener, with one request through it.
    Connect,
    /// Plain HTTP to the egress listener, as an Actor's redirected
    /// connections arrive there.
    EgressHttp,
    /// Connections to the egress listener, one request each, then held.
    EgressIdle,
    /// Bytes as fast as possible over a few connections to the discard port.
    EgressStream,
}

impl Mode {
    /// Whether the mode's traffic starts inside a sandbox.
    pub fn is_egress(self) -> bool {
        matches!(
            self,
            Mode::EgressHttp | Mode::EgressIdle | Mode::EgressStream
        )
    }

    /// The mode's wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::IngressH1 => "ingress-h1",
            Mode::IngressH2 => "ingress-h2",
            Mode::Connect => "connect",
            Mode::EgressHttp => "egress-http",
            Mode::EgressIdle => "egress-idle",
            Mode::EgressStream => "egress-stream",
        }
    }
}

/// A mode and how to drive it.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    pub name: &'static str,
    pub description: &'static str,
    /// Names one unit of work, for reading the report.
    pub unit: &'static str,
    pub mode: Mode,
    /// Turns keep-alive off.
    pub new_connection_per_request: bool,
    /// Makes each rung a count of connections to open, at
    /// [`IDLE_OPEN_RATE`] per second, and hold.
    pub holds_connections: bool,
    /// Makes the scenario a bandwidth measurement over this many
    /// connections, run once at the first rung.
    pub streams: usize,
}

/// Held connections per second EgressIdleConns opens. Each costs the
/// tunnel a TCP accept and a CONNECT stream to the gateway.
pub const IDLE_OPEN_RATE: f64 = 200.0;

const fn scenario(
    name: &'static str,
    description: &'static str,
    unit: &'static str,
    mode: Mode,
) -> Scenario {
    Scenario {
        name,
        description,
        unit,
        mode,
        new_connection_per_request: false,
        holds_connections: false,
        streams: 0,
    }
}

/// Every scenario, in the order they run.
pub const SCENARIOS: &[Scenario] = &[
    scenario("IngressH1", "router-style HTTPS requests over HTTP/1.1 keep-alive into ingress, proxied to the Actor", "request", Mode::IngressH1),
    scenario("IngressH2", "IngressH1 over HTTP/2, many requests multiplexed on few connections", "request", Mode::IngressH2),
    scenario("Connect", "per request: new TLS connection, CONNECT to the Actor's port, one request inside", "tunnel", Mode::Connect),
    scenario("EgressHTTP", "Actor-style plain HTTP keep-alive out through egress; one CONNECT stream to the gateway per connection", "request", Mode::EgressHttp),
    Scenario {
        new_connection_per_request: true,
        ..scenario("EgressNewConnection", "EgressHTTP with a new connection per request, so a new CONNECT stream per request", "request", Mode::EgressHttp)
    },
    Scenario {
        holds_connections: true,
        ..scenario("EgressIdleConns", "rungs are connection counts: open at 200/s through egress, one request each, then hold", "held connection", Mode::EgressIdle)
    },
    Scenario {
        streams: 4,
        ..scenario("EgressTCP", "4 connections write 64 KiB chunks as fast as they can through egress to a discarding gateway; one rung", "write", Mode::EgressStream)
    },
];

/// The scenarios named in a comma-separated list (any case), or all of
/// them for `all` or an empty list.
pub fn select(list: &str) -> Result<Vec<Scenario>> {
    if list.is_empty() || list == "all" {
        return Ok(SCENARIOS.to_vec());
    }
    list.split(',')
        .map(str::trim)
        .map(|name| {
            SCENARIOS
                .iter()
                .find(|s| s.name.eq_ignore_ascii_case(name))
                .cloned()
                .ok_or_else(|| Error::msg(format!("unknown scenario {name:?}; known: {}", names())))
        })
        .collect()
}

/// The scenario names, comma-separated.
pub fn names() -> String {
    SCENARIOS
        .iter()
        .map(|s| s.name)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_scenarios() {
        assert_eq!(select("all").unwrap().len(), 7);
        let some = select("ingressh1, EgressTCP").unwrap();
        assert_eq!(
            some.iter().map(|s| s.name).collect::<Vec<_>>(),
            ["IngressH1", "EgressTCP"]
        );
        assert!(select("Nope").is_err(), "accepted an unknown scenario");
    }

    #[test]
    fn idle_description_names_the_open_rate() {
        let idle = select("EgressIdleConns").unwrap().remove(0);
        assert!(idle.description.contains(&format!("{IDLE_OPEN_RATE:.0}/s")));
    }

    #[test]
    fn mode_wire_names() {
        for m in [
            Mode::IngressH1,
            Mode::IngressH2,
            Mode::Connect,
            Mode::EgressHttp,
            Mode::EgressIdle,
            Mode::EgressStream,
        ] {
            assert_eq!(serde_json::to_value(m).unwrap(), m.as_str());
        }
    }
}
