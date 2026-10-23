//! Preview gates of the install under test.
//!
//! `HK_E2E_PREVIEW` lists the gates the cluster was installed with, in the
//! installer's syntax: comma- or space-separated gate names, or `ALL`. Tests
//! of a preview feature start with [`require_preview`], which skips them
//! unless the gate is listed. In CI (`CI=true`) the variable must be set,
//! even if only to the empty string, so a lane that forgets it cannot
//! silently skip every preview test.

use std::sync::OnceLock;

use hivekeep_core::preview::{Gate, PreviewGates};

use crate::harness::T;
use crate::settings::PREVIEW_ENV;

static GATES: OnceLock<PreviewGates> = OnceLock::new();

/// Whether the preview setting must be present.
fn preview_required(ci: Option<&str>) -> bool {
    ci == Some("true")
}

/// The gate names in `value`; `set` says whether the setting was given at
/// all.
pub fn preview_gates(value: &str, set: bool, ci: Option<&str>) -> Result<Vec<String>, String> {
    if !set && preview_required(ci) {
        return Err(format!(
            "{PREVIEW_ENV} must be set in CI (empty for no preview gates)"
        ));
    }
    Ok(value
        .split([',', ' ', '\t', '\n'])
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// Reads the gates from the environment and fixes them for the process.
pub fn init_preview() -> Result<(), String> {
    let raw = std::env::var(PREVIEW_ENV).ok();
    let ci = std::env::var("CI").ok();
    let names = preview_gates(raw.as_deref().unwrap_or(""), raw.is_some(), ci.as_deref())?;
    let gates = PreviewGates::from_flags(&names).map_err(|e| format!("{PREVIEW_ENV}: {e}"))?;
    let _ = GATES.set(gates);
    Ok(())
}

/// The gates the install under test enabled (none before [`init_preview`]).
pub fn gates() -> PreviewGates {
    GATES.get().cloned().unwrap_or_default()
}

/// Whether `gate` is enabled on the install under test.
pub fn is_enabled(gate: Gate) -> bool {
    gates().is_enabled(gate)
}

/// Whether the API client must opt in to preview features.
pub fn any_enabled() -> bool {
    gates().iter().next().is_some()
}

/// Skips `t` unless the install under test enabled `gate`.
pub fn require_preview(t: &T, gate: Gate) {
    if !is_enabled(gate) {
        t.skip(format!(
            "preview gate {gate:?} is not enabled (set {PREVIEW_ENV})",
            gate = gate.name()
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_gates_cases() {
        struct Case {
            name: &'static str,
            ci: Option<&'static str>,
            value: Option<&'static str>,
            want: &'static [&'static str],
            want_err: bool,
        }
        let cases = [
            Case {
                name: "unset locally",
                ci: None,
                value: None,
                want: &[],
                want_err: false,
            },
            Case {
                name: "unset in CI",
                ci: Some("true"),
                value: None,
                want: &[],
                want_err: true,
            },
            Case {
                name: "empty in CI",
                ci: Some("true"),
                value: Some(""),
                want: &[],
                want_err: false,
            },
            Case {
                name: "ALL",
                ci: Some("true"),
                value: Some("ALL"),
                want: &["ALL"],
                want_err: false,
            },
            Case {
                name: "list",
                ci: None,
                value: Some("A, B C"),
                want: &["A", "B", "C"],
                want_err: false,
            },
        ];
        for c in cases {
            let got = preview_gates(c.value.unwrap_or(""), c.value.is_some(), c.ci);
            assert_eq!(got.is_err(), c.want_err, "{}: {got:?}", c.name);
            if let Ok(got) = got {
                assert_eq!(got, c.want, "{}", c.name);
            }
        }
    }
}
