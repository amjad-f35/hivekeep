//! Actor templates: `HIVE NAME GOLDEN TAG ERROR AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::table::Table;

/// Sorts by hive, then name.
pub fn sort(templates: &mut [pb::ActorTemplate]) {
    templates.sort_by_cached_key(|t| {
        let m = t.metadata.clone().unwrap_or_default();
        (m.hive, m.name)
    });
}

fn golden_disabled(t: &pb::ActorTemplate) -> bool {
    t.snapshot_config
        .as_ref()
        .and_then(|c| c.golden_snapshot_config.as_ref())
        .is_some_and(|g| g.mode == pb::GoldenSnapshotMode::Disabled as i32)
}

fn table(p: &Printer, templates: &[pb::ActorTemplate]) -> Table {
    let mut tbl = Table::new(["HIVE", "NAME", "GOLDEN TAG", "ERROR", "AGE"]);
    for t in templates {
        let m = t.metadata.clone().unwrap_or_default();
        let golden = t
            .status
            .as_ref()
            .and_then(|s| s.golden_snapshot_status.clone())
            .unwrap_or_default();
        let golden_tag = if golden_disabled(t) {
            "DISABLED".to_string()
        } else {
            golden.golden_tag.map(|r| r.name).unwrap_or_default()
        };
        // An error message is too long for a table cell; flag it instead.
        let err = if golden.error_message.is_empty() {
            ""
        } else {
            "ERROR"
        };
        tbl.row([
            m.hive.clone(),
            m.name.clone(),
            golden_tag,
            err.to_string(),
            p.age(m.create_time.as_ref()),
        ]);
    }
    tbl
}

/// Prints a list of templates.
pub fn print_list(
    p: &Printer,
    out: &mut dyn Write,
    mut templates: Vec<pb::ActorTemplate>,
) -> Result<()> {
    sort(&mut templates);
    match p.format {
        OutputFormat::Table => Ok(table(p, &templates).write_to(out)?),
        _ => p.write_message(
            out,
            &pb::ListActorTemplatesResponse {
                actor_templates: templates,
                next_page_token: String::new(),
            },
        ),
    }
}

/// Prints one template.
pub fn print_one(p: &Printer, out: &mut dyn Write, template: &pb::ActorTemplate) -> Result<()> {
    p.write_one(out, template, || table(p, std::slice::from_ref(template)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::age::timestamp_before;
    use crate::io::SharedBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_767_268_800)
    }

    fn tpl(hive: &str, name: &str, ago: u64) -> pb::ActorTemplate {
        pb::ActorTemplate {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.into(),
                name: name.into(),
                version: 1,
                create_time: Some(timestamp_before(now(), Duration::from_secs(ago))),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn table() {
        let mut golden = tpl("demo-counter", "counter", 300);
        golden.status = Some(pb::ActorTemplateStatus {
            golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
                golden_tag: Some(pb::ObjectRef {
                    hive: "demo-counter".into(),
                    name: "golden-tag".into(),
                }),
                ..Default::default()
            }),
        });
        let mut failed = tpl("demo-counter-microvm", "counter-microvm", 5 * 3600);
        failed.status = Some(pb::ActorTemplateStatus {
            golden_snapshot_status: Some(pb::GoldenSnapshotStatus {
                error_message: "golden actor failed to start".into(),
                ..Default::default()
            }),
        });
        let mut disabled = tpl("demo-counter", "counter-3", 15 * 60);
        disabled.snapshot_config = Some(pb::SnapshotConfig {
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                mode: pb::GoldenSnapshotMode::Disabled as i32,
            }),
            ..Default::default()
        });
        let plain = tpl("demo-counter", "counter-2", 72 * 3600);

        let mut buf = SharedBuf::default();
        print_list(
            &Printer::at(OutputFormat::Table, now()),
            &mut buf,
            vec![golden, failed, plain, disabled],
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "HIVE                   NAME              GOLDEN TAG   ERROR   AGE\n\
             demo-counter           counter           golden-tag           5m\n\
             demo-counter           counter-2                              3d\n\
             demo-counter           counter-3         DISABLED             15m\n\
             demo-counter-microvm   counter-microvm                ERROR   5h\n"
        );
    }

    #[test]
    fn list_yaml() {
        let mut t = tpl("h", "t", 0);
        t.metadata.as_mut().unwrap().create_time = None;
        let mut buf = SharedBuf::default();
        print_list(&Printer::at(OutputFormat::Yaml, now()), &mut buf, vec![t]).unwrap();
        assert_eq!(
            buf.contents(),
            "actorTemplates:\n- metadata:\n    hive: h\n    name: t\n    version: \"1\"\n"
        );
    }
}
