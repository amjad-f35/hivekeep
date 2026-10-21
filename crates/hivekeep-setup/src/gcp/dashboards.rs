//! Cloud Monitoring dashboards, created or updated in place by display name
//! (creating always makes a new dashboard, so repeating it would duplicate).

use super::config::{default_dashboard_dir, GcpConfig};
use super::runner::{Gcloud, Runner};
use super::GcpError;
use crate::info;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// The dashboard definitions applied, in order.
pub const DASHBOARD_FILES: &[&str] = &[
    "hivekeep-grpc-dashboard.json",
    "hivekeep-e2e-latency-dashboard.json",
    "hivekeep-snapshot-dashboard.json",
];

fn display_name(d: &Value) -> String {
    d.get("displayName")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

pub fn create_monitoring_dashboards(runner: &dyn Runner, cfg: &GcpConfig) -> Result<(), GcpError> {
    let g = Gcloud { runner };
    let project = format!("--project={}", cfg.project_id);
    let listed = g.run(
        "list dashboards",
        &[
            "monitoring".into(),
            "dashboards".into(),
            "list".into(),
            project.clone(),
            "--format=json".into(),
        ],
    )?;
    let existing: Vec<Value> = if listed.trim().is_empty() {
        vec![]
    } else {
        serde_json::from_str(&listed)
            .map_err(|e| GcpError::Other(format!("list dashboards: {e}")))?
    };
    let by_name: BTreeMap<String, Value> = existing
        .into_iter()
        .map(|d| (display_name(&d), d))
        .collect();

    let dir = if cfg.dashboard_dir.is_empty() {
        default_dashboard_dir()
    } else {
        cfg.dashboard_dir.clone()
    };
    for file in DASHBOARD_FILES {
        let path = Path::new(&dir).join(file);
        let data = std::fs::read_to_string(&path)
            .map_err(|e| GcpError::Other(format!("read {}: {e}", path.display())))?;
        let mut d: Value = serde_json::from_str(&data)
            .map_err(|e| GcpError::Other(format!("parse {}: {e}", path.display())))?;
        let name = display_name(&d);
        let staged = tempfile::Builder::new()
            .prefix("hivekeep-dashboard-")
            .suffix(".json")
            .tempfile()
            .map_err(|e| GcpError::Other(format!("stage dashboard: {e}")))?;
        if let Some(cur) = by_name.get(&name) {
            // Update in place, reusing the resource name and etag.
            let resource = cur
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            d["name"] = Value::String(resource.clone());
            if let Some(etag) = cur.get("etag") {
                d["etag"] = etag.clone();
            }
            std::fs::write(staged.path(), d.to_string())
                .map_err(|e| GcpError::Other(e.to_string()))?;
            info!("Updating dashboard {name:?} ({resource}) from {file}");
            g.run(
                &format!("update dashboard {name:?}"),
                &[
                    "monitoring".into(),
                    "dashboards".into(),
                    "update".into(),
                    resource,
                    format!("--config-from-file={}", staged.path().display()),
                    project.clone(),
                ],
            )?;
        } else {
            std::fs::write(staged.path(), d.to_string())
                .map_err(|e| GcpError::Other(e.to_string()))?;
            info!("Creating dashboard {name:?} from {file}");
            g.run(
                &format!("create dashboard {name:?}"),
                &[
                    "monitoring".into(),
                    "dashboards".into(),
                    "create".into(),
                    format!("--config-from-file={}", staged.path().display()),
                    project.clone(),
                ],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::runner::{FakeRunner, Output};

    #[test]
    fn shipped_dashboards_parse_and_have_names() {
        for f in DASHBOARD_FILES {
            let path = Path::new(&default_dashboard_dir()).join(f);
            let v: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            assert!(
                display_name(&v).starts_with("Hivekeep"),
                "{f}: {}",
                display_name(&v)
            );
        }
    }

    #[test]
    fn existing_dashboards_are_updated_not_duplicated() {
        let runner = FakeRunner::new();
        runner.respond(
            &["monitoring", "dashboards", "list"],
            Output::ok(r#"[{"name":"projects/1/dashboards/abc","displayName":"Hivekeep Snapshot Size & QPS","etag":"e1"}]"#),
        );
        let cfg = GcpConfig {
            project_id: "p".into(),
            ..Default::default()
        };
        create_monitoring_dashboards(&runner, &cfg).unwrap();
        let lines = runner.command_lines();
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.contains("dashboards create"))
                .count(),
            2,
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("dashboards update projects/1/dashboards/abc")),
            "{lines:?}"
        );
    }
}
