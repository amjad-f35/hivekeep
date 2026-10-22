//! Lint findings that are allowed to fail `validate` for now.
//!
//! An exemption's identity is the exact (rule, subject, message) triple. That
//! exactness lets `validate` tell "this finding is exempted" apart from "the API
//! changed and this is a new finding", even when both share a subject.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One specific finding that is allowed to fail validation.
///
/// Field order defines the canonical sort order: rule, then subject, then message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Exemption {
    pub rule: String,
    pub subject: String,
    pub message: String,
}

impl Exemption {
    pub fn new(rule: &str, subject: &str, message: &str) -> Self {
        Exemption {
            rule: rule.into(),
            subject: subject.into(),
            message: message.into(),
        }
    }
}

/// Errors reading or writing an exemptions file.
#[derive(Debug, thiserror::Error)]
pub enum ExemptionError {
    #[error("while reading {path}: {source}")]
    Read { path: String, source: io::Error },
    #[error("while parsing {path}: {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
    #[error("while encoding exemptions: {0}")]
    Encode(serde_json::Error),
    #[error("while writing {path}: {source}")]
    Write { path: String, source: io::Error },
    #[error("exemptions are out of order")]
    Unsorted,
}

/// Reads exemptions from `path`. A missing file means zero exemptions.
pub fn load(path: &Path) -> Result<Vec<Exemption>, ExemptionError> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ExemptionError::Read {
                path: path.display().to_string(),
                source,
            })
        }
    };
    serde_json::from_slice(&data).map_err(|source| ExemptionError::Parse {
        path: path.display().to_string(),
        source,
    })
}

/// Writes exemptions to `path` as sorted, indented JSON.
pub fn save(path: &Path, exemptions: &[Exemption]) -> Result<(), ExemptionError> {
    let mut sorted = exemptions.to_vec();
    sorted.sort();
    let mut data = serde_json::to_vec_pretty(&sorted).map_err(ExemptionError::Encode)?;
    data.push(b'\n');
    std::fs::write(path, data).map_err(|source| ExemptionError::Write {
        path: path.display().to_string(),
        source,
    })
}

/// Fails unless `exemptions` are in canonical order.
pub fn check_sorted(exemptions: &[Exemption]) -> Result<(), ExemptionError> {
    if exemptions.windows(2).all(|w| w[0] <= w[1]) {
        Ok(())
    } else {
        Err(ExemptionError::Unsorted)
    }
}

/// Exemptions counted by value, so each one can be claimed by one finding.
#[derive(Debug, Default)]
pub struct Set {
    remaining: HashMap<Exemption, usize>,
}

impl Set {
    pub fn new(exemptions: &[Exemption]) -> Self {
        let mut remaining = HashMap::new();
        for e in exemptions {
            *remaining.entry(e.clone()).or_insert(0) += 1;
        }
        Set { remaining }
    }

    /// Claims an unclaimed exemption matching (rule, subject, message), if any.
    pub fn consume(&mut self, rule: &str, subject: &str, message: &str) -> bool {
        match self
            .remaining
            .get_mut(&Exemption::new(rule, subject, message))
        {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    /// Every exemption never claimed by a finding, sorted.
    pub fn unused(&self) -> Vec<Exemption> {
        let mut out: Vec<Exemption> = self
            .remaining
            .iter()
            .flat_map(|(e, &n)| std::iter::repeat_n(e.clone(), n))
            .collect();
        out.sort();
        out
    }
}

/// Compares current findings to recorded exemptions: findings with no exemption
/// (`missing`) and exemptions with no finding (`stale`), both sorted.
pub fn diff(current: &[Exemption], exemptions: &[Exemption]) -> (Vec<Exemption>, Vec<Exemption>) {
    let mut set = Set::new(exemptions);
    let mut missing: Vec<Exemption> = current
        .iter()
        .filter(|c| !set.consume(&c.rule, &c.subject, &c.message))
        .cloned()
        .collect();
    missing.sort();
    (missing, set.unused())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(rule: &str, subject: &str, message: &str) -> Exemption {
        Exemption::new(rule, subject, message)
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let got = load(&dir.path().join("does-not-exist.json")).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exemptions.json");
        let input = vec![
            ex(
                "documented",
                "hivekeep.api.v1.Foo",
                "message has no doc comment",
            ),
            ex(
                "delete-request-shape",
                "hivekeep.api.v1.Control.DeleteActor",
                r#"field "any_state" is not a message"#,
            ),
        ];
        save(&path, &input).unwrap();
        let got = load(&path).unwrap();
        assert_eq!(got, vec![input[1].clone(), input[0].clone()]);
    }

    #[test]
    fn save_sorts_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exemptions.json");
        save(
            &path,
            &[
                ex("documented", "hivekeep.api.v1.B", "m"),
                ex("documented", "hivekeep.api.v1.A", "m"),
            ],
        )
        .unwrap();
        assert_eq!(
            load(&path).unwrap(),
            vec![
                ex("documented", "hivekeep.api.v1.A", "m"),
                ex("documented", "hivekeep.api.v1.B", "m"),
            ]
        );
        // An empty list is written as `[]`, not omitted.
        save(&path, &[]).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "[]\n");
    }

    #[test]
    fn check_sorted_cases() {
        let cases: Vec<(&str, Vec<Exemption>, bool)> = vec![
            ("nil", vec![], false),
            (
                "singleton",
                vec![ex("documented", "hivekeep.api.v1.A", "m")],
                false,
            ),
            (
                "sorted",
                vec![
                    ex("documented", "hivekeep.api.v1.A", "m"),
                    ex("documented", "hivekeep.api.v1.B", "m"),
                    ex("no-oneofs", "hivekeep.api.v1.A", "m"),
                ],
                false,
            ),
            (
                "out of order subject",
                vec![
                    ex("documented", "hivekeep.api.v1.TagScope", "m"),
                    ex("documented", "hivekeep.api.v1.ActorState", "m"),
                ],
                true,
            ),
            (
                "out of order rule",
                vec![
                    ex("no-oneofs", "hivekeep.api.v1.A", "m"),
                    ex("documented", "hivekeep.api.v1.A", "m"),
                ],
                true,
            ),
        ];
        for (name, exemptions, want_err) in cases {
            assert_eq!(check_sorted(&exemptions).is_err(), want_err, "{name}");
        }
    }

    #[test]
    fn set_consume() {
        let mut set = Set::new(&[ex(
            "documented",
            "hivekeep.api.v1.Foo",
            "message has no doc comment",
        )]);
        assert!(set.consume(
            "documented",
            "hivekeep.api.v1.Foo",
            "message has no doc comment"
        ));
        assert!(!set.consume(
            "documented",
            "hivekeep.api.v1.Foo",
            "message has no doc comment"
        ));
        assert!(!set.consume(
            "documented",
            "hivekeep.api.v1.Bar",
            "message has no doc comment"
        ));
    }

    #[test]
    fn diff_reports_missing_and_stale() {
        let current = [
            ex(
                "documented",
                "hivekeep.api.v1.Foo",
                "message has no doc comment",
            ),
            ex(
                "documented",
                "hivekeep.api.v1.Bar",
                "message has no doc comment",
            ),
        ];
        let exemptions = [
            ex(
                "documented",
                "hivekeep.api.v1.Foo",
                "message has no doc comment",
            ),
            ex(
                "documented",
                "hivekeep.api.v1.Baz",
                "message has no doc comment",
            ),
        ];
        let (missing, stale) = diff(&current, &exemptions);
        assert_eq!(
            missing,
            vec![ex(
                "documented",
                "hivekeep.api.v1.Bar",
                "message has no doc comment"
            )]
        );
        assert_eq!(
            stale,
            vec![ex(
                "documented",
                "hivekeep.api.v1.Baz",
                "message has no doc comment"
            )]
        );
    }

    #[test]
    fn set_unused_reports_unclaimed_exemptions() {
        let mut set = Set::new(&[
            ex(
                "documented",
                "hivekeep.api.v1.Foo",
                "message has no doc comment",
            ),
            ex(
                "documented",
                "hivekeep.api.v1.Bar",
                "message has no doc comment",
            ),
        ]);
        set.consume(
            "documented",
            "hivekeep.api.v1.Foo",
            "message has no doc comment",
        );
        assert_eq!(
            set.unused(),
            vec![ex(
                "documented",
                "hivekeep.api.v1.Bar",
                "message has no doc comment"
            )]
        );
    }
}
