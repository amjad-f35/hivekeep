//! YAML for `-o yaml` and manifests.
//!
//! Output is rendered from a JSON value in the block style operators know
//! from cluster tooling: keys sorted, two-space indentation, sequence items
//! at their parent key's indentation, and strings quoted only when they would
//! otherwise read back as something else. Input is any YAML (and so any
//! JSON) holding exactly one document.

use serde_json::Value;

/// Renders `value` as a YAML document, ending in a newline.
pub fn to_string(value: &Value) -> String {
    let mut out = String::new();
    match value {
        Value::Object(m) if !m.is_empty() => emit_map(value, 0, &mut out),
        Value::Array(a) if !a.is_empty() => emit_seq(value, 0, &mut out),
        scalar => {
            out.push_str(&scalar_text(scalar, 0));
            out.push('\n');
        }
    }
    out
}

fn sorted_entries(value: &Value) -> Vec<(&String, &Value)> {
    let mut entries: Vec<_> = value.as_object().into_iter().flatten().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

fn is_nested(v: &Value) -> bool {
    match v {
        Value::Object(m) => !m.is_empty(),
        Value::Array(a) => !a.is_empty(),
        _ => false,
    }
}

fn emit_map(value: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    for (k, v) in sorted_entries(value) {
        out.push_str(&pad);
        out.push_str(&string_text(k, indent));
        out.push(':');
        match v {
            Value::Object(m) if !m.is_empty() => {
                out.push('\n');
                emit_map(v, indent + 2, out);
            }
            Value::Array(a) if !a.is_empty() => {
                out.push('\n');
                emit_seq(v, indent, out);
            }
            scalar => {
                out.push(' ');
                out.push_str(&scalar_text(scalar, indent));
                out.push('\n');
            }
        }
    }
}

fn emit_seq(value: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    for item in value.as_array().into_iter().flatten() {
        if is_nested(item) {
            // Render the item one level deeper, then put the dash in place of
            // its first line's indentation.
            let mut inner = String::new();
            if item.is_object() {
                emit_map(item, indent + 2, &mut inner);
            } else {
                emit_seq(item, indent + 2, &mut inner);
            }
            out.push_str(&pad);
            out.push_str("- ");
            out.push_str(&inner[indent + 2..]);
        } else {
            out.push_str(&pad);
            out.push_str("- ");
            out.push_str(&scalar_text(item, indent));
            out.push('\n');
        }
    }
}

fn scalar_text(v: &Value, indent: usize) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => string_text(s, indent),
        Value::Object(_) => "{}".into(),
        Value::Array(_) => "[]".into(),
    }
}

/// Whether a plain scalar `s` would be read back as something other than a
/// string: null, a boolean, a number or a timestamp.
fn resolves_to_non_string(s: &str) -> bool {
    const SPECIAL: &[&str] = &[
        "", "~", "null", "Null", "NULL", "true", "True", "TRUE", "false", "False", "FALSE", "y",
        "Y", "yes", "Yes", "YES", "n", "N", "no", "No", "NO", "on", "On", "ON", "off", "Off",
        "OFF", ".inf", ".Inf", ".INF", "+.inf", "+.Inf", "+.INF", "-.inf", "-.Inf", "-.INF",
        ".nan", ".NaN", ".NAN", "<<",
    ];
    if SPECIAL.contains(&s) {
        return true;
    }
    looks_numeric(s) || looks_like_timestamp(s)
}

fn looks_numeric(s: &str) -> bool {
    let t = s.strip_prefix(['+', '-']).unwrap_or(s);
    if t.is_empty() {
        return false;
    }
    if let Some(hex) = t.strip_prefix("0x") {
        return !hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit() || c == '_');
    }
    if let Some(oct) = t.strip_prefix("0o") {
        return !oct.is_empty() && oct.chars().all(|c| c.is_digit(8) || c == '_');
    }
    if let Some(bin) = t.strip_prefix("0b") {
        return !bin.is_empty() && bin.chars().all(|c| c == '0' || c == '1' || c == '_');
    }
    // Decimal integers and floats, with optional exponent and the
    // digit-group underscores YAML 1.1 allows.
    let cleaned: String = t.chars().filter(|&c| c != '_').collect();
    if cleaned.is_empty()
        || !cleaned
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit() || c == '.')
    {
        return false;
    }
    if cleaned == "." {
        return false;
    }
    cleaned.parse::<f64>().is_ok()
        || (cleaned.contains(':')
            && cleaned
                .split(':')
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit() || c == '.')))
}

fn looks_like_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 8
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5].is_ascii_digit()
        && (b[6] == b'-' || (b[6].is_ascii_digit() && b.get(7) == Some(&b'-')))
}

fn plain_allowed(s: &str) -> bool {
    let first = match s.chars().next() {
        Some(c) => c,
        None => return false,
    };
    if s.starts_with(' ') || s.ends_with(' ') || s.starts_with("---") || s.starts_with("...") {
        return false;
    }
    if "#,[]{}&*!|>'\"%@`".contains(first) {
        return false;
    }
    let second_is_space = s.chars().nth(1).is_none_or(|c| c == ' ');
    if "-?:".contains(first) && second_is_space {
        return false;
    }
    if s.contains(": ") || s.contains(" #") || s.ends_with(':') {
        return false;
    }
    !s.chars().any(|c| c.is_control())
}

fn single_quote_allowed(s: &str) -> bool {
    !s.chars().any(|c| c.is_control())
}

fn literal_allowed(s: &str) -> bool {
    !s.starts_with(' ') && !s.contains(" \n") && !s.chars().any(|c| c.is_control() && c != '\n')
}

fn string_text(s: &str, indent: usize) -> String {
    if resolves_to_non_string(s) {
        return double_quoted(s);
    }
    if s.contains('\n') {
        return if literal_allowed(s) {
            literal(s, indent)
        } else {
            double_quoted(s)
        };
    }
    if plain_allowed(s) {
        s.to_string()
    } else if single_quote_allowed(s) {
        format!("'{}'", s.replace('\'', "''"))
    } else {
        double_quoted(s)
    }
}

fn literal(s: &str, indent: usize) -> String {
    let trailing = s.len() - s.trim_end_matches('\n').len();
    let chomp = match trailing {
        0 => "-",
        1 => "",
        _ => "+",
    };
    let pad = " ".repeat(indent + 2);
    let body = s.strip_suffix('\n').unwrap_or(s);
    let mut out = format!("|{chomp}");
    for line in body.split('\n') {
        out.push('\n');
        if !line.is_empty() {
            out.push_str(&pad);
            out.push_str(line);
        }
    }
    out
}

fn double_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// An unusable manifest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    /// The input holds no document, or only a null one.
    #[error("manifest is empty")]
    Empty,
    /// The input holds more than one document.
    #[error("manifest holds more than one document, expected one")]
    MultipleDocuments,
    /// The input is not YAML.
    #[error("invalid YAML: {0}")]
    Invalid(String),
}

/// Parses a YAML or JSON manifest holding exactly one document into JSON.
/// Documents are counted the way YAML counts them: the empty document a
/// trailing `---` opens is one too.
pub fn manifest_to_json(data: &str) -> Result<Value, ManifestError> {
    let mut docs = serde_yaml_ng::Deserializer::from_str(data);
    let first = match docs.next() {
        Some(d) => d,
        None => return Err(ManifestError::Empty),
    };
    let value = <Value as serde::Deserialize>::deserialize(first)
        .map_err(|e| ManifestError::Invalid(e.to_string()))?;
    if docs.next().is_some() {
        return Err(ManifestError::MultipleDocuments);
    }
    if value.is_null() {
        return Err(ManifestError::Empty);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn block_style() {
        let v = json!({
            "actors": [{"metadata": {"name": "id-1", "version": "2"}}],
            "empty": {},
            "list": [],
            "nested": [["a", "b"], "c"],
            "b": true,
            "n": 3
        });
        assert_eq!(
            to_string(&v),
            "actors:\n- metadata:\n    name: id-1\n    version: \"2\"\nb: true\nempty: {}\nlist: []\n\"n\": 3\nnested:\n- - a\n  - b\n- c\n"
        );
    }

    #[test]
    fn quoting() {
        for (s, want) in [
            ("plain", "plain"),
            ("2", "\"2\""),
            ("1.5", "\"1.5\""),
            ("true", "\"true\""),
            ("yes", "\"yes\""),
            ("", "\"\""),
            ("2026-01-01T12:00:00Z", "\"2026-01-01T12:00:00Z\""),
            ("a: b", "'a: b'"),
            ("it's: x", "'it''s: x'"),
            ("*", "'*'"),
            ("- x", "'- x'"),
            ("-c", "-c"),
            ("gs://bucket/path", "gs://bucket/path"),
            (
                "user:system:serviceaccount:a:b",
                "user:system:serviceaccount:a:b",
            ),
            ("tab\there", "\"tab\\there\""),
        ] {
            assert_eq!(string_text(s, 0), want, "{s:?}");
        }
    }

    #[test]
    fn multi_line_strings_use_literal_blocks() {
        let v = json!({"script": "echo a\necho b\n"});
        assert_eq!(to_string(&v), "script: |\n  echo a\n  echo b\n");
        let v = json!({"script": "a\nb"});
        assert_eq!(to_string(&v), "script: |-\n  a\n  b\n");
    }

    #[test]
    fn output_reads_back() {
        let v = json!({"a": ["x: y", "2", "", {"k": "v\nw"}], "b": {"c": "'q'"}});
        assert_eq!(manifest_to_json(&to_string(&v)).unwrap(), v);
    }

    #[test]
    fn manifests() {
        assert_eq!(manifest_to_json("a: 1\n").unwrap(), json!({"a": 1}));
        assert_eq!(manifest_to_json("{\"a\": 1}").unwrap(), json!({"a": 1}));
        assert_eq!(manifest_to_json("").unwrap_err(), ManifestError::Empty);
        assert_eq!(
            manifest_to_json("# only a comment\n").unwrap_err(),
            ManifestError::Empty
        );
        assert_eq!(manifest_to_json("~\n").unwrap_err(), ManifestError::Empty);
        assert_eq!(
            manifest_to_json("a: 1\n---\nb: 2\n").unwrap_err(),
            ManifestError::MultipleDocuments
        );
        assert_eq!(
            manifest_to_json("a: 1\n---\n").unwrap_err(),
            ManifestError::MultipleDocuments
        );
        assert!(matches!(
            manifest_to_json("a: [1\n").unwrap_err(),
            ManifestError::Invalid(_)
        ));
    }
}
