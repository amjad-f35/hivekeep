//! `top worker` rows: live CPU and memory usage of worker pods.

use std::io::Write;

use serde::Serialize;

use crate::error::{CliError, Result};
use crate::output::{OutputFormat, Printer};
use crate::table::Table;

/// Usage of one worker pod.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerTopItem {
    /// Pod name.
    pub pod: String,
    /// Worker pool.
    pub pool: String,
    /// Primary sandbox class.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub class: String,
    /// Occupancy, as [`crate::print::worker::occupancy`] renders it.
    pub status: String,
    /// CPU usage, e.g. `250m`.
    pub cpu: String,
    /// Memory usage, e.g. `512Mi`.
    pub memory: String,
    /// Pod namespace.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub namespace: String,
}

#[derive(Serialize)]
struct WorkerTopList<'a> {
    workers: &'a [WorkerTopItem],
}

/// Prints usage rows, sorted by namespace, pool and pod.
pub fn print(p: &Printer, out: &mut dyn Write, mut items: Vec<WorkerTopItem>) -> Result<()> {
    items.sort_by(|a, b| (&a.namespace, &a.pool, &a.pod).cmp(&(&b.namespace, &b.pool, &b.pod)));
    let list = WorkerTopList { workers: &items };
    match p.format {
        OutputFormat::Table => {
            let mut t = Table::new([
                "NAME",
                "POOL",
                "CLASS",
                "STATUS",
                "CPU(CORES)",
                "MEMORY(bytes)",
            ]);
            for i in &items {
                t.row([&i.pod, &i.pool, &i.class, &i.status, &i.cpu, &i.memory].map(String::clone));
            }
            Ok(t.write_to(out)?)
        }
        OutputFormat::Json => {
            // Fields in declaration order, as the struct defines them.
            let mut s = serde_json::to_string_pretty(&list)
                .map_err(|e| CliError::failed(format!("failed to marshal json: {e}")))?;
            s.push('\n');
            out.write_all(s.as_bytes())?;
            Ok(())
        }
        OutputFormat::Yaml => {
            let value = serde_json::to_value(&list)
                .map_err(|e| CliError::failed(format!("failed to marshal json for yaml: {e}")))?;
            p.write_value(out, &value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::SharedBuf;

    fn items() -> Vec<WorkerTopItem> {
        vec![
            WorkerTopItem {
                pod: "worker-pool-b-1".into(),
                pool: "pool-b".into(),
                class: "gvisor".into(),
                status: "FREE".into(),
                cpu: "10m".into(),
                memory: "64Mi".into(),
                namespace: "ns".into(),
            },
            WorkerTopItem {
                pod: "worker-pool-a-1".into(),
                pool: "pool-a".into(),
                class: String::new(),
                status: "ASSIGNED(1/4)".into(),
                cpu: "250m".into(),
                memory: "512Mi".into(),
                namespace: "ns".into(),
            },
        ]
    }

    fn render(format: OutputFormat) -> String {
        let mut buf = SharedBuf::default();
        print(&Printer::new(format), &mut buf, items()).unwrap();
        buf.contents()
    }

    #[test]
    fn table() {
        assert_eq!(
            render(OutputFormat::Table),
            "NAME              POOL     CLASS    STATUS          CPU(CORES)   MEMORY(bytes)\n\
             worker-pool-a-1   pool-a            ASSIGNED(1/4)   250m         512Mi\n\
             worker-pool-b-1   pool-b   gvisor   FREE            10m          64Mi\n"
        );
    }

    #[test]
    fn json_keeps_field_order_and_omits_empty() {
        let got = render(OutputFormat::Json);
        assert!(got.starts_with("{\n  \"workers\": [\n    {\n      \"pod\": \"worker-pool-a-1\",\n      \"pool\": \"pool-a\",\n      \"status\""), "{got}");
    }

    #[test]
    fn yaml_is_sorted() {
        let got = render(OutputFormat::Yaml);
        assert!(
            got.starts_with("workers:\n- cpu: 250m\n  memory: 512Mi\n  namespace: ns\n"),
            "{got}"
        );
    }
}
