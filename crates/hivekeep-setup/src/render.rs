//! `${NAME}` placeholder expansion for manifest templates.
//!
//! Templates are line-oriented: a line that names a dropped placeholder is
//! removed entirely (so optional YAML blocks disappear cleanly), and every
//! other line has its known placeholders substituted. Unknown placeholders are
//! left as written.

use std::collections::BTreeMap;
use std::path::Path;

/// Reads the template at `path` and expands it.
pub fn template(
    path: &Path,
    values: &BTreeMap<String, String>,
    drop: &[&str],
) -> Result<Vec<u8>, std::io::Error> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        std::io::Error::new(e.kind(), format!("while reading {}: {e}", path.display()))
    })?;
    Ok(expand(&raw, values, drop))
}

/// Expands `body`: lines naming a placeholder in `drop` are removed, and every
/// `${NAME}` in `values` is replaced on the lines that remain.
pub fn expand(body: &str, values: &BTreeMap<String, String>, drop: &[&str]) -> Vec<u8> {
    let drop_tokens: Vec<String> = drop.iter().map(|n| token(n)).collect();
    let trailing_newline = body.ends_with('\n');
    let trimmed = body.strip_suffix('\n').unwrap_or(body);

    let kept: Vec<String> = trimmed
        .split('\n')
        .filter(|line| !drop_tokens.iter().any(|t| line.contains(t.as_str())))
        .map(|line| replace_all(line, values))
        .collect();

    let mut out = kept.join("\n");
    if trailing_newline {
        out.push('\n');
    }
    out.into_bytes()
}

/// Replaces placeholders in a single left-to-right pass, so a substituted
/// value is never itself expanded.
fn replace_all(line: &str, values: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let name = &after[..end];
                match values.get(name) {
                    Some(v) => out.push_str(v),
                    None => {
                        out.push_str("${");
                        out.push_str(name);
                        out.push('}');
                    }
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

fn token(name: &str) -> String {
    format!("${{{name}}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vals(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn expand_cases() {
        struct Case {
            name: &'static str,
            body: &'static str,
            values: BTreeMap<String, String>,
            drop: Vec<&'static str>,
            want: &'static str,
        }
        let cases = [
            Case {
                name: "substitutes every occurrence",
                body: "bucket: ${BUCKET_NAME}\nother: ${BUCKET_NAME}\n",
                values: vals(&[("BUCKET_NAME", "hivekeep-snapshots")]),
                drop: vec![],
                want: "bucket: hivekeep-snapshots\nother: hivekeep-snapshots\n",
            },
            Case {
                name: "drops lines naming a dropped placeholder",
                body: "spec:\n${EXTERNAL_VOLUMES}\n  replicas: 1\n",
                values: vals(&[]),
                drop: vec!["EXTERNAL_VOLUMES"],
                want: "spec:\n  replicas: 1\n",
            },
            Case {
                name: "drop wins over an indented placeholder line",
                body: "args:\n    ${VALIDATE_EXISTING_FILE_PATH_ARG}\n",
                values: vals(&[]),
                drop: vec!["VALIDATE_EXISTING_FILE_PATH_ARG"],
                want: "args:\n",
            },
            Case {
                name: "multiline substitution keeps YAML structure",
                body: "volumes:\n${EXTERNAL_VOLUMES}\n",
                values: vals(&[(
                    "EXTERNAL_VOLUMES",
                    "  - name: external-data\n    capacity: 1Gi",
                )]),
                drop: vec![],
                want: "volumes:\n  - name: external-data\n    capacity: 1Gi\n",
            },
            Case {
                name: "leaves unknown placeholders alone",
                body: "image: ${WORKLOAD_IMAGE}\n",
                values: vals(&[("BUCKET_NAME", "b")]),
                drop: vec![],
                want: "image: ${WORKLOAD_IMAGE}\n",
            },
            Case {
                name: "preserves absence of a trailing newline",
                body: "a: ${V}",
                values: vals(&[("V", "1")]),
                drop: vec![],
                want: "a: 1",
            },
        ];
        for c in cases {
            let got = String::from_utf8(expand(c.body, &c.values, &c.drop)).unwrap();
            assert_eq!(got, c.want, "{}", c.name);
        }
    }
}
