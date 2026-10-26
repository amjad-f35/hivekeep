//! The benchmark automation's `isolate:` keys must set exactly the fake
//! keeper's tuning variables. The fake keeper ignores a variable it does not
//! know, so a key left behind by a rename would tune nothing, silently.

use std::collections::BTreeSet;
use std::path::Path;

use hivekeep_bench::isolate::fake_keeper::config::{tuning_env, TUNING_FLAGS};

/// The `FAKE_KEEPER_*` names in the `ISOLATE_TUNING = { ... }` table of the
/// automation's util.py.
fn automation_tuning_vars(src: &str) -> Option<BTreeSet<String>> {
    let start = src.find("ISOLATE_TUNING = {")?;
    let body = &src[start..];
    let body = &body[..body.find('}')?];
    let mut vars = BTreeSet::new();
    let mut rest = body;
    while let Some(i) = rest.find("\"FAKE_KEEPER_") {
        rest = &rest[i + 1..];
        let end = rest.find('"')?;
        vars.insert(rest[..end].to_string());
        rest = &rest[end + 1..];
    }
    Some(vars)
}

#[test]
fn isolate_tuning_matches_automation() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("automation/util.py");
    let src = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let got = automation_tuning_vars(&src)
        .unwrap_or_else(|| panic!("{} has no ISOLATE_TUNING table", path.display()));
    let want: BTreeSet<String> = TUNING_FLAGS.iter().map(|f| tuning_env(f)).collect();
    assert_eq!(
        got,
        want,
        "{} ISOLATE_TUNING sets {got:?}, want the fake keeper's tuning variables {want:?}",
        path.display()
    );
}

#[test]
fn table_parser_reads_only_the_table() {
    let src = r#"
OTHER = {"x": "FAKE_KEEPER_NOT_THIS"}
ISOLATE_TUNING = {
    "delay": "FAKE_KEEPER_DELAY",
    "delayRun": "FAKE_KEEPER_DELAY_RUN",
}
AFTER = "FAKE_KEEPER_NOR_THIS"
"#;
    let got = automation_tuning_vars(src).unwrap();
    assert_eq!(
        got,
        BTreeSet::from([
            "FAKE_KEEPER_DELAY".to_string(),
            "FAKE_KEEPER_DELAY_RUN".to_string()
        ])
    );
    assert!(automation_tuning_vars("nothing here").is_none());
}
