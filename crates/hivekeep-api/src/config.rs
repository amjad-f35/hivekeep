//! The `hivekeep-api` configuration file (`hivekeep.config.v1.ApiConfig`).
//!
//! Accepted encodings, chosen by extension: binary protobuf (`.pb`) and
//! protobuf text format (`.textproto`, `.textpb`). The text parser covers
//! what the schema uses: nested messages and scalar fields, `#` comments.

use std::collections::BTreeMap;
use std::path::Path;

use hivekeep_proto::config::v1 as cfg;
use prost::Message;

/// Why a configuration file could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("while reading config file: {0}")]
    Io(#[from] std::io::Error),
    /// The file did not parse.
    #[error("while parsing config file: {0}")]
    Parse(String),
    /// The file parsed but holds an invalid value.
    #[error("{0}")]
    Invalid(String),
}

/// Log levels the config accepts.
pub const LOG_LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];

/// Loads and validates the configuration at `path`.
pub fn load(path: impl AsRef<Path>) -> Result<cfg::ApiConfig, ConfigError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)?;
    let config = match path.extension().and_then(|e| e.to_str()) {
        Some("pb") => cfg::ApiConfig::decode(bytes.as_slice())
            .map_err(|e| ConfigError::Parse(format!("unmarshaling protobuf: {e}")))?,
        Some("textproto") | Some("textpb") => {
            let text = String::from_utf8(bytes)
                .map_err(|e| ConfigError::Parse(format!("not UTF-8: {e}")))?;
            parse_text(&text)?
        }
        other => {
            return Err(ConfigError::Parse(format!(
                "unrecognized config extension {:?}",
                other.unwrap_or("")
            )))
        }
    };
    validate(&config)?;
    Ok(config)
}

/// Checks the values of a parsed config.
pub fn validate(config: &cfg::ApiConfig) -> Result<(), ConfigError> {
    let level = config
        .logging
        .as_ref()
        .map(|l| l.level.as_str())
        .unwrap_or("");
    if !LOG_LEVELS.contains(&level) {
        return Err(ConfigError::Invalid(format!(
            "logging.level: invalid value {level:?}"
        )));
    }
    Ok(())
}

#[derive(Debug)]
enum Value {
    Scalar(String),
    Message(BTreeMap<String, Value>),
}

/// Parses the text format of `ApiConfig`.
pub fn parse_text(text: &str) -> Result<cfg::ApiConfig, ConfigError> {
    let tokens = tokenize(text)?;
    let mut pos = 0;
    let root = parse_fields(&tokens, &mut pos, false)?;
    let mut out = cfg::ApiConfig::default();
    for (key, value) in root {
        match (key.as_str(), value) {
            ("logging", Value::Message(fields)) => {
                let mut logging = cfg::ApiLogging::default();
                for (k, v) in fields {
                    match (k.as_str(), v) {
                        ("level", Value::Scalar(s)) => logging.level = s,
                        (k, _) => {
                            return Err(ConfigError::Parse(format!("unknown field logging.{k}")))
                        }
                    }
                }
                out.logging = Some(logging);
            }
            (k, _) => return Err(ConfigError::Parse(format!("unknown field {k}"))),
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Colon,
    Open,
    Close,
}

fn tokenize(text: &str) -> Result<Vec<Token>, ConfigError> {
    let mut out = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() || c == ',' || c == ';' => {
                chars.next();
            }
            '#' => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            ':' => {
                chars.next();
                out.push(Token::Colon);
            }
            '{' | '<' => {
                chars.next();
                out.push(Token::Open);
            }
            '}' | '>' => {
                chars.next();
                out.push(Token::Close);
            }
            '"' | '\'' => {
                let quote = c;
                chars.next();
                let mut s = String::new();
                loop {
                    match chars.next() {
                        None => return Err(ConfigError::Parse("unterminated string".into())),
                        Some('\\') => match chars.next() {
                            Some('n') => s.push('\n'),
                            Some('t') => s.push('\t'),
                            Some(c) => s.push(c),
                            None => return Err(ConfigError::Parse("unterminated string".into())),
                        },
                        Some(c) if c == quote => break,
                        Some(c) => s.push(c),
                    }
                }
                out.push(Token::Str(s));
            }
            c if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' => {
                let mut s = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
                        s.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                out.push(Token::Ident(s));
            }
            other => {
                return Err(ConfigError::Parse(format!(
                    "unexpected character {other:?}"
                )))
            }
        }
    }
    Ok(out)
}

fn parse_fields(
    tokens: &[Token],
    pos: &mut usize,
    nested: bool,
) -> Result<BTreeMap<String, Value>, ConfigError> {
    let mut fields = BTreeMap::new();
    loop {
        match tokens.get(*pos) {
            None if !nested => return Ok(fields),
            None => return Err(ConfigError::Parse("missing '}'".into())),
            Some(Token::Close) if nested => {
                *pos += 1;
                return Ok(fields);
            }
            Some(Token::Ident(name)) => {
                *pos += 1;
                let name = name.clone();
                if tokens.get(*pos) == Some(&Token::Colon) {
                    *pos += 1;
                }
                let value = match tokens.get(*pos) {
                    Some(Token::Open) => {
                        *pos += 1;
                        Value::Message(parse_fields(tokens, pos, true)?)
                    }
                    Some(Token::Str(s)) | Some(Token::Ident(s)) => {
                        *pos += 1;
                        Value::Scalar(s.clone())
                    }
                    _ => return Err(ConfigError::Parse(format!("missing value for {name}"))),
                };
                if fields.insert(name.clone(), value).is_some() {
                    return Err(ConfigError::Parse(format!("field {name} repeated")));
                }
            }
            Some(t) => return Err(ConfigError::Parse(format!("unexpected token {t:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_format() {
        let c = parse_text("# comment\nlogging {\n  level: \"debug\"\n}\n").unwrap();
        assert_eq!(c.logging.unwrap().level, "debug");
        let c = parse_text("logging: < level: 'warn' >").unwrap();
        assert_eq!(c.logging.unwrap().level, "warn");
        assert!(parse_text("logging { nope: 1 }").is_err());
        assert!(parse_text("logging { level: \"x\"").is_err());
    }

    #[test]
    fn loads_files_by_extension() {
        let dir = tempfile::tempdir().unwrap();
        let text = dir.path().join("c.textproto");
        std::fs::write(&text, "logging { level: \"info\" }").unwrap();
        assert_eq!(load(&text).unwrap().logging.unwrap().level, "info");

        let bin = dir.path().join("c.pb");
        let msg = cfg::ApiConfig {
            logging: Some(cfg::ApiLogging {
                level: "error".into(),
            }),
        };
        std::fs::write(&bin, msg.encode_to_vec()).unwrap();
        assert_eq!(load(&bin).unwrap(), msg);

        let bad = dir.path().join("c.textpb");
        std::fs::write(&bad, "logging { level: \"loud\" }").unwrap();
        assert!(matches!(load(&bad), Err(ConfigError::Invalid(_))));
        let unknown = dir.path().join("c.yaml");
        std::fs::write(&unknown, "").unwrap();
        assert!(matches!(load(&unknown), Err(ConfigError::Parse(_))));
    }
}
