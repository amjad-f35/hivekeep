//! Label selectors for `-l/--selector`: `a=b`, `a!=b`, `a in (x,y)`,
//! `a notin (x)`, `a` (exists), `!a` (does not exist), `a>3`, `a<3`, joined
//! by commas, all of which must match.

use std::collections::{BTreeSet, HashMap};

use hivekeep_core::names::{check_label_key, check_label_value};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Equals(String),
    NotEquals(String),
    In(BTreeSet<String>),
    NotIn(BTreeSet<String>),
    Exists,
    DoesNotExist,
    GreaterThan(i64),
    LessThan(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Requirement {
    key: String,
    op: Op,
}

/// A parsed selector. The empty selector matches everything.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelSelector {
    requirements: Vec<Requirement>,
}

/// Splits on commas that are not inside parentheses.
fn split_top_level(s: &str) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced parenthesis".into());
                }
            }
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("unbalanced parenthesis".into());
    }
    parts.push(&s[start..]);
    Ok(parts)
}

fn check_key(key: &str) -> Result<String, String> {
    let errs = check_label_key(key);
    if errs.is_empty() {
        Ok(key.to_string())
    } else {
        Err(format!("invalid label key {key:?}: {}", errs.join("; ")))
    }
}

fn check_value(value: &str) -> Result<String, String> {
    let errs = check_label_value(value);
    if errs.is_empty() {
        Ok(value.to_string())
    } else {
        Err(format!(
            "invalid label value {value:?}: {}",
            errs.join("; ")
        ))
    }
}

fn parse_set(rest: &str) -> Result<BTreeSet<String>, String> {
    let inner = rest
        .trim()
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| format!("expected a parenthesized value list, got {:?}", rest.trim()))?;
    let mut set = BTreeSet::new();
    for v in inner.split(',') {
        set.insert(check_value(v.trim())?);
    }
    Ok(set)
}

fn parse_requirement(raw: &str) -> Result<Requirement, String> {
    let r = raw.trim();
    if r.is_empty() {
        return Err("empty requirement".into());
    }
    if let Some(key) = r.strip_prefix('!') {
        return Ok(Requirement {
            key: check_key(key.trim())?,
            op: Op::DoesNotExist,
        });
    }
    for (token, make) in [
        (
            "!=",
            (|v: &str| check_value(v).map(Op::NotEquals)) as fn(&str) -> Result<Op, String>,
        ),
        ("==", |v| check_value(v).map(Op::Equals)),
        ("=", |v| check_value(v).map(Op::Equals)),
        (">", |v| {
            v.parse()
                .map(Op::GreaterThan)
                .map_err(|_| format!("{v:?} is not an integer"))
        }),
        ("<", |v| {
            v.parse()
                .map(Op::LessThan)
                .map_err(|_| format!("{v:?} is not an integer"))
        }),
    ] {
        if let Some((key, value)) = r.split_once(token) {
            return Ok(Requirement {
                key: check_key(key.trim())?,
                op: make(value.trim())?,
            });
        }
    }
    let mut words = r.splitn(2, char::is_whitespace);
    let key = words.next().unwrap_or_default();
    let rest = words.next().unwrap_or("").trim_start();
    if rest.is_empty() {
        return Ok(Requirement {
            key: check_key(key)?,
            op: Op::Exists,
        });
    }
    if let Some(set) = rest.strip_prefix("notin") {
        return Ok(Requirement {
            key: check_key(key)?,
            op: Op::NotIn(parse_set(set)?),
        });
    }
    if let Some(set) = rest.strip_prefix("in") {
        return Ok(Requirement {
            key: check_key(key)?,
            op: Op::In(parse_set(set)?),
        });
    }
    Err(format!("unexpected {rest:?} after key {key:?}"))
}

impl LabelSelector {
    /// Parses a selector.
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.trim().is_empty() {
            return Ok(LabelSelector::default());
        }
        let requirements = split_top_level(s)?
            .into_iter()
            .map(parse_requirement)
            .collect::<Result<_, _>>()?;
        Ok(LabelSelector { requirements })
    }

    /// Whether `labels` satisfy every requirement.
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        self.requirements.iter().all(|r| {
            let v = labels.get(&r.key);
            match &r.op {
                Op::Equals(want) => v == Some(want),
                Op::NotEquals(want) => v != Some(want),
                Op::In(set) => v.is_some_and(|v| set.contains(v)),
                Op::NotIn(set) => !v.is_some_and(|v| set.contains(v)),
                Op::Exists => v.is_some(),
                Op::DoesNotExist => v.is_none(),
                Op::GreaterThan(n) => v
                    .and_then(|v| v.parse::<i64>().ok())
                    .is_some_and(|v| v > *n),
                Op::LessThan(n) => v
                    .and_then(|v| v.parse::<i64>().ok())
                    .is_some_and(|v| v < *n),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn matching() {
        let l = labels(&[("pool", "fast"), ("tier", "gold"), ("gen", "3")]);
        for (sel, want) in [
            ("", true),
            ("pool=fast", true),
            ("pool==fast", true),
            ("pool=slow", false),
            ("pool!=slow", true),
            ("missing!=x", true),
            ("pool in (fast, slow)", true),
            ("pool notin (fast)", false),
            ("missing notin (fast)", true),
            ("tier", true),
            ("!tier", false),
            ("!missing", true),
            ("gen>2", true),
            ("gen<2", false),
            ("pool=fast,tier in (gold,silver),!missing", true),
            ("pool=fast,tier=silver", false),
        ] {
            let s = LabelSelector::parse(sel).unwrap_or_else(|e| panic!("{sel}: {e}"));
            assert_eq!(s.matches(&l), want, "{sel}");
        }
    }

    #[test]
    fn invalid() {
        for sel in [
            "pool in (a",
            "pool in a",
            "-bad=x",
            "a=b,",
            "a>x",
            "pool frob (a)",
        ] {
            assert!(LabelSelector::parse(sel).is_err(), "{sel}");
        }
    }
}
