//! `-o` output formats and the shared rendering of messages.

use std::fmt;
use std::io::Write;
use std::str::FromStr;
use std::time::SystemTime;

use serde_json::Value;

use crate::error::{CliError, Result};
use crate::protojson::{self, ProtoMessage};
use crate::table::Table;
use crate::yaml;

/// How results are printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Aligned columns with the most useful fields.
    #[default]
    Table,
    /// The full resource as indented protobuf JSON.
    Json,
    /// The full resource as YAML.
    Yaml,
}

impl FromStr for OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "yaml" => Ok(OutputFormat::Yaml),
            other => Err(format!(
                "invalid output format {other:?}. Must be one of: table, json, yaml"
            )),
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OutputFormat::Table => "table",
            OutputFormat::Json => "json",
            OutputFormat::Yaml => "yaml",
        })
    }
}

/// Renders resources in one format, with ages measured against one clock.
#[derive(Debug, Clone, Copy)]
pub struct Printer {
    /// The output format.
    pub format: OutputFormat,
    /// The time ages are measured at.
    pub now: SystemTime,
}

impl Printer {
    /// A printer measuring ages against the current time.
    pub fn new(format: OutputFormat) -> Self {
        Printer {
            format,
            now: SystemTime::now(),
        }
    }

    /// A printer with a fixed clock.
    pub fn at(format: OutputFormat, now: SystemTime) -> Self {
        Printer { format, now }
    }

    /// Writes `value` as indented JSON or YAML, per the format.
    pub fn write_value(&self, out: &mut dyn Write, value: &Value) -> Result<()> {
        let text = match self.format {
            OutputFormat::Yaml => yaml::to_string(value),
            _ => {
                let mut s = serde_json::to_string_pretty(value)
                    .map_err(|e| CliError::failed(format!("failed to marshal json: {e}")))?;
                s.push('\n');
                s
            }
        };
        out.write_all(text.as_bytes())?;
        Ok(())
    }

    /// Writes `msg` as protobuf JSON or YAML, per the format.
    pub fn write_message<M: ProtoMessage>(&self, out: &mut dyn Write, msg: &M) -> Result<()> {
        let value = protojson::to_value(msg).map_err(CliError::failed)?;
        self.write_value(out, &value)
    }

    /// Writes a single resource: the message itself as JSON or YAML, or the
    /// table rendered by `table`.
    pub fn write_one<M: ProtoMessage>(
        &self,
        out: &mut dyn Write,
        msg: &M,
        table: impl FnOnce() -> Table,
    ) -> Result<()> {
        match self.format {
            OutputFormat::Table => Ok(table().write_to(out)?),
            _ => self.write_message(out, msg),
        }
    }

    /// The age of a resource created at `created`.
    pub fn age(&self, created: Option<&prost_types::Timestamp>) -> String {
        crate::age::format_age(created, self.now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::SharedBuf;
    use serde_json::json;

    #[test]
    fn parse_formats() {
        assert_eq!("yaml".parse::<OutputFormat>(), Ok(OutputFormat::Yaml));
        assert_eq!(
            "xml".parse::<OutputFormat>().unwrap_err(),
            "invalid output format \"xml\". Must be one of: table, json, yaml"
        );
    }

    #[test]
    fn json_is_indented_with_trailing_newline() {
        let mut buf = SharedBuf::default();
        Printer::new(OutputFormat::Json)
            .write_value(&mut buf, &json!({"b": 1, "a": {"c": "x"}}))
            .unwrap();
        assert_eq!(
            buf.contents(),
            "{\n  \"a\": {\n    \"c\": \"x\"\n  },\n  \"b\": 1\n}\n"
        );
    }
}
