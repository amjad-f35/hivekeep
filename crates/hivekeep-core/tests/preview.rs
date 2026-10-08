//! Preview gates and the per-request opt-in.

use std::collections::BTreeMap;

use hivekeep_core::preview::{header_opts_in, Gate, PreviewGates, ALL_GATES};
use hivekeep_core::validation::Operation;
use regex::Regex;

fn as_map(g: &PreviewGates) -> BTreeMap<&'static str, bool> {
    Gate::ALL
        .iter()
        .map(|x| (x.name(), g.is_enabled(*x)))
        .collect()
}

fn map(on: &[Gate]) -> BTreeMap<&'static str, bool> {
    Gate::ALL
        .iter()
        .map(|x| (x.name(), on.contains(x)))
        .collect()
}

#[test]
fn init_from_flags() {
    use Gate::*;
    type Case = (&'static str, Vec<&'static str>, Option<Vec<Gate>>);
    let cases: Vec<Case> = vec![
        ("no values", vec![], Some(vec![])),
        ("ALL", vec!["ALL"], Some(Gate::ALL.to_vec())),
        (
            "one gate",
            vec!["ExternalVolumes"],
            Some(vec![ExternalVolumes]),
        ),
        (
            "another gate",
            vec!["GoldenEgressPolicy"],
            Some(vec![GoldenEgressPolicy]),
        ),
        (
            "two gates",
            vec!["GoldenEgressPolicy", "ExternalVolumes"],
            Some(vec![ExternalVolumes, GoldenEgressPolicy]),
        ),
        ("repeated ALL", vec!["ALL", "ALL"], Some(Gate::ALL.to_vec())),
        ("unknown", vec!["Bogus"], None),
        // `--preview=` reaches us as one empty value and means no gates.
        ("empty", vec![""], Some(vec![])),
        ("star is not a wildcard", vec!["*"], None),
        ("lowercase all", vec!["all"], None),
    ];
    for (name, values, want) in cases {
        match (PreviewGates::from_flags(&values), want) {
            (Ok(g), Some(on)) => assert_eq!(as_map(&g), map(&on), "{name}"),
            (Err(_), None) => {}
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

#[test]
fn real_gates_are_pascal_case() {
    let re = Regex::new(r"^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]*)*$").unwrap();
    for s in ["ExternalVolumes", "Foo", "FooV2", "FooBar2Baz"] {
        assert!(re.is_match(s), "pattern rejected {s:?}");
    }
    for s in [
        "",
        "externalVolumes",
        "External_Volumes",
        "External-Volumes",
        "EXTERNAL",
        ALL_GATES,
        "F",
        "Foo Bar",
    ] {
        assert!(!re.is_match(s), "pattern accepted {s:?}");
    }
    assert!(!Gate::ALL.is_empty());
    for g in Gate::ALL {
        assert!(re.is_match(g.name()), "gate {g} is not PascalCase");
        assert_eq!(g.name().parse::<Gate>().unwrap(), g);
    }
}

#[test]
fn is_enabled_known_and_unknown_gate() {
    let g = PreviewGates::from_flags(["ALL"]).unwrap();
    assert!(g.is_enabled(Gate::ExternalVolumes));
    // A gate name the server does not know cannot name a gate at all.
    assert!("Bogus".parse::<Gate>().is_err());
}

#[test]
fn verify_flags() {
    for (values, want_err) in [
        (vec![], false),
        (vec!["ALL"], false),
        (vec!["ExternalVolumes"], false),
        (vec!["ExternalVolumes", "GoldenEgressPolicy"], false),
        (vec!["Bogus"], true),
        (vec!["ALL", "Bogus"], true),
    ] {
        assert_eq!(
            PreviewGates::from_flags(&values).is_err(),
            want_err,
            "{values:?}"
        );
    }
}

/// Header values that opt a request in, matching strict boolean parsing.
#[test]
fn header_opt_in_values() {
    for (name, values, want) in [
        ("no metadata", vec![], false),
        ("true", vec!["true"], true),
        ("TRUE", vec!["TRUE"], true),
        ("false", vec!["false"], false),
        ("garbage", vec!["yes please"], false),
        ("empty", vec![""], false),
        ("any true", vec!["false", "true"], true),
        ("padded", vec![" true"], false),
    ] {
        assert_eq!(header_opts_in(values), want, "{name}");
    }
}

#[test]
fn opt_in_and_out() {
    let op = Operation::create();
    assert!(!op.opted_out());
    let op = op.with_opt_out(true);
    assert!(op.opted_out());
    let op = op.with_opt_out(false);
    assert!(!op.opted_out());
    let op = op.with_opt_out(true);
    assert!(op.opted_out());
}

#[test]
fn is_enabled_for_request() {
    let gates = PreviewGates::none().with(Gate::ExternalVolumes);
    let base = Operation::create().with_gates(gates);
    for (name, op, gate, want) in [
        (
            "enabled, unmarked",
            base.clone(),
            Gate::ExternalVolumes,
            true,
        ),
        (
            "enabled, opted out",
            base.clone().with_opt_out(true),
            Gate::ExternalVolumes,
            false,
        ),
        (
            "enabled, opted in",
            base.clone().with_opt_out(true).with_opt_out(false),
            Gate::ExternalVolumes,
            true,
        ),
        (
            "disabled, opted in",
            base.clone(),
            Gate::GoldenEgressPolicy,
            false,
        ),
    ] {
        assert_eq!(op.gate_enabled(gate), want, "{name}");
    }
}
