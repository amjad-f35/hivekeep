//! Preview feature gates.
//!
//! A server enables gates with its `--preview` flag; all are off by
//! default. Independently, each API request may opt in to preview features
//! with the [`PREVIEW_HEADER`] metadata key; a request that does not is
//! treated as if every gate were off. The header is a guard against using
//! preview features by accident, not an access control.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// gRPC metadata key with which a client opts in to preview features. The
/// value is a boolean such as `"true"`.
pub const PREVIEW_HEADER: &str = "hk-preview-allowed";

/// The value that enables every known gate.
pub const ALL_GATES: &str = "ALL";

/// A gate-controlled preview feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Gate {
    /// External (CSI) volumes.
    ExternalVolumes,
    /// Configuring a golden Actor's egress policy on its template.
    GoldenEgressPolicy,
    /// Honouring an ActorTemplate's own snapshot storage location.
    PerTemplateSnapshotLocation,
}

impl Gate {
    /// Every known gate, sorted by name.
    pub const ALL: [Gate; 3] = [
        Gate::ExternalVolumes,
        Gate::GoldenEgressPolicy,
        Gate::PerTemplateSnapshotLocation,
    ];

    /// The gate's flag name.
    pub fn name(self) -> &'static str {
        match self {
            Gate::ExternalVolumes => "ExternalVolumes",
            Gate::GoldenEgressPolicy => "GoldenEgressPolicy",
            Gate::PerTemplateSnapshotLocation => "PerTemplateSnapshotLocation",
        }
    }
}

impl fmt::Display for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A `--preview` value that names no known gate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown preview gate {value:?}: supported gates: ALL, {}", Gate::ALL.map(|g| g.name()).join(", "))]
pub struct UnknownGate {
    /// The rejected value.
    pub value: String,
}

impl FromStr for Gate {
    type Err = UnknownGate;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Gate::ALL
            .into_iter()
            .find(|g| g.name() == s)
            .ok_or_else(|| UnknownGate {
                value: s.to_string(),
            })
    }
}

/// The set of enabled gates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreviewGates(BTreeSet<Gate>);

impl PreviewGates {
    /// No gates enabled.
    pub fn none() -> Self {
        Self::default()
    }

    /// Every gate enabled.
    pub fn all() -> Self {
        PreviewGates(Gate::ALL.into_iter().collect())
    }

    /// Builds the set from `--preview` values. [`ALL_GATES`] enables every
    /// gate and empty values are skipped (the installer passes `--preview=`
    /// when no gate is on); any other unknown value is an error.
    pub fn from_flags<I, S>(values: I) -> Result<Self, UnknownGate>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut set = BTreeSet::new();
        for v in values {
            let v = v.as_ref();
            if v.is_empty() {
                continue;
            }
            if v == ALL_GATES {
                set.extend(Gate::ALL);
            } else {
                set.insert(v.parse()?);
            }
        }
        Ok(PreviewGates(set))
    }

    /// Whether `gate` is enabled.
    pub fn is_enabled(&self, gate: Gate) -> bool {
        self.0.contains(&gate)
    }

    /// Enables `gate`.
    pub fn with(mut self, gate: Gate) -> Self {
        self.0.insert(gate);
        self
    }

    /// The enabled gates.
    pub fn iter(&self) -> impl Iterator<Item = Gate> + '_ {
        self.0.iter().copied()
    }
}

impl fmt::Display for PreviewGates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = Gate::ALL
            .iter()
            .map(|g| format!("{}={}", g, if self.is_enabled(*g) { "on" } else { "off" }))
            .collect();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// Whether a request's [`PREVIEW_HEADER`] values opt in: any value that is
/// exactly a true boolean spelling (`1`, `t`, `T`, `true`, `TRUE`, `True`)
/// does. Values are not trimmed.
pub fn header_opts_in<'a, I: IntoIterator<Item = &'a str>>(values: I) -> bool {
    values
        .into_iter()
        .any(|v| matches!(v, "1" | "t" | "T" | "true" | "TRUE" | "True"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags() {
        assert_eq!(
            PreviewGates::from_flags::<_, &str>([]).unwrap(),
            PreviewGates::none()
        );
        assert_eq!(
            PreviewGates::from_flags(["ALL"]).unwrap(),
            PreviewGates::all()
        );
        let g = PreviewGates::from_flags(["ExternalVolumes"]).unwrap();
        assert!(g.is_enabled(Gate::ExternalVolumes));
        assert!(!g.is_enabled(Gate::GoldenEgressPolicy));
        // `--preview=` with no gates parses as one empty value.
        assert_eq!(
            PreviewGates::from_flags([""]).unwrap(),
            PreviewGates::none()
        );
        assert_eq!(
            PreviewGates::from_flags(["", "ExternalVolumes"]).unwrap(),
            PreviewGates::none().with(Gate::ExternalVolumes)
        );
        let err = PreviewGates::from_flags(["Nope"]).unwrap_err();
        assert!(err.to_string().contains("unknown preview gate \"Nope\""));
        assert_eq!(
            g.to_string(),
            "[ExternalVolumes=on, GoldenEgressPolicy=off, PerTemplateSnapshotLocation=off]"
        );
    }

    #[test]
    fn header() {
        assert!(header_opts_in(["true"]));
        assert!(header_opts_in(["false", "1"]));
        assert!(!header_opts_in(["false", "yes"]));
        assert!(!header_opts_in([]));
    }
}
