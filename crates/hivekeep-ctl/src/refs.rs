//! Parsing of the small values flags carry: resource references, worker
//! selectors, tag scopes and suspend modes.

use std::collections::HashMap;

use hivekeep_core::names::{check_label_key, check_label_value};
use hivekeep_core::resource::{parse_hived_name, ParseRefError};
use hivekeep_proto::api::v1 as pb;

/// Most labels a worker selector may hold.
pub const MAX_WORKER_SELECTOR_LABELS: usize = 10;

/// Parses a reference to a hive-scoped resource, written `<hive>/<name>` or
/// as a bare `<name>` in `default_hive`.
pub fn parse_ref(value: &str, default_hive: &str) -> Result<pb::ObjectRef, ParseRefError> {
    parse_hived_name(value, default_hive)
}

/// Parses a comma-separated `key=value` worker selector. An empty string
/// yields `None`, which clears an Actor's selector on update.
pub fn parse_worker_selector(raw: &str) -> Result<Option<pb::Selector>, String> {
    if raw.is_empty() {
        return Ok(None);
    }
    let mut match_labels = HashMap::new();
    for entry in raw.split(',') {
        let (key, val) = entry.split_once('=').ok_or_else(|| {
            format!("invalid worker selector {raw:?}: entry {entry:?} must be in key=value format")
        })?;
        let errs = check_label_key(key);
        if !errs.is_empty() {
            return Err(format!(
                "invalid worker selector {raw:?}: invalid label key {key:?}: {}",
                errs.join("; ")
            ));
        }
        let errs = check_label_value(val);
        if !errs.is_empty() {
            return Err(format!(
                "invalid worker selector {raw:?}: invalid label value {val:?} for key {key:?}: {}",
                errs.join("; ")
            ));
        }
        if match_labels
            .insert(key.to_string(), val.to_string())
            .is_some()
        {
            return Err(format!(
                "invalid worker selector {raw:?}: duplicate label key {key:?}"
            ));
        }
    }
    if match_labels.len() > MAX_WORKER_SELECTOR_LABELS {
        return Err(format!(
            "invalid worker selector {raw:?}: at most {MAX_WORKER_SELECTOR_LABELS} labels are allowed, got {}",
            match_labels.len()
        ));
    }
    Ok(Some(pb::Selector { match_labels }))
}

/// Parses a `--scope` value: `hive` or `published`, case-insensitively.
pub fn parse_tag_scope(value: &str) -> Result<pb::TagScope, String> {
    match value.to_ascii_lowercase().as_str() {
        "hive" => Ok(pb::TagScope::Hive),
        "published" => Ok(pb::TagScope::Published),
        _ => Err(format!(
            "invalid scope {value:?}; must be hive or published"
        )),
    }
}

/// `suspend actor --mode async`: return once the snapshot is on the node.
pub const SUSPEND_MODE_ASYNC: &str = "async";
/// `suspend actor --mode sync`: return once the snapshot is in object storage.
pub const SUSPEND_MODE_SYNC: &str = "sync";

/// Parses a `--mode` value into a durability mode.
pub fn parse_suspend_mode(mode: &str) -> Result<pb::DurabilityMode, String> {
    match mode {
        SUSPEND_MODE_ASYNC => Ok(pb::DurabilityMode::Asynchronous),
        SUSPEND_MODE_SYNC => Ok(pb::DurabilityMode::Synchronous),
        _ => Err(format!(
            "invalid --mode {mode:?}: must be one of {SUSPEND_MODE_ASYNC}, {SUSPEND_MODE_SYNC}"
        )),
    }
}

/// A reference to a global resource (a Hive or a Worker).
pub fn global_ref(name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: String::new(),
        name: name.to_string(),
    }
}

/// A reference to a hive-scoped resource.
pub fn hived_ref(hive: &str, name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: hive.to_string(),
        name: name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refs() {
        for (value, default, want) in [
            ("tpl", "team-a", Some(("team-a", "tpl"))),
            ("other/tpl", "team-a", Some(("other", "tpl"))),
            ("other/tpl", "", Some(("other", "tpl"))),
            ("tpl", "", None),
            ("a/b/c", "x", None),
            ("/tpl", "x", None),
            ("Bad", "x", None),
            ("a/", "x", None),
        ] {
            let got = parse_ref(value, default);
            match want {
                Some((h, n)) => assert_eq!(got.unwrap(), hived_ref(h, n), "{value}"),
                None => {
                    let err = got.unwrap_err().to_string();
                    assert!(err.contains("malformed reference"), "{value}: {err}");
                }
            }
        }
    }

    #[test]
    fn worker_selectors() {
        assert_eq!(parse_worker_selector("").unwrap(), None);
        let s = parse_worker_selector("pool=fast,example.com/tier=gold")
            .unwrap()
            .unwrap();
        assert_eq!(s.match_labels.len(), 2);
        assert_eq!(s.match_labels["example.com/tier"], "gold");
        let s = parse_worker_selector("a=").unwrap().unwrap();
        assert_eq!(s.match_labels["a"], "");

        for (raw, want) in [
            ("novalue", "must be in key=value format"),
            ("a=b,", "must be in key=value format"),
            ("-bad=x", "invalid label key"),
            ("a=-bad", "invalid label value"),
            ("a=1,a=2", "duplicate label key"),
        ] {
            let err = parse_worker_selector(raw).unwrap_err();
            assert!(err.contains(want), "{raw}: {err}");
        }
        let many: Vec<String> = (0..11).map(|i| format!("k{i}=v")).collect();
        let err = parse_worker_selector(&many.join(",")).unwrap_err();
        assert!(err.contains("at most 10 labels"), "{err}");
    }

    #[test]
    fn tag_scopes() {
        assert_eq!(parse_tag_scope("hive").unwrap(), pb::TagScope::Hive);
        assert_eq!(
            parse_tag_scope("PUBLISHED").unwrap(),
            pb::TagScope::Published
        );
        assert!(parse_tag_scope("global")
            .unwrap_err()
            .contains("must be hive or published"));
    }

    #[test]
    fn suspend_modes() {
        assert_eq!(
            parse_suspend_mode("async").unwrap(),
            pb::DurabilityMode::Asynchronous
        );
        assert_eq!(
            parse_suspend_mode("sync").unwrap(),
            pb::DurabilityMode::Synchronous
        );
        for bad in ["", "SYNC", "eventually"] {
            assert!(parse_suspend_mode(bad)
                .unwrap_err()
                .contains("invalid --mode"));
        }
    }
}
