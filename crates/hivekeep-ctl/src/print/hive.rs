//! Hives: `NAME AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::table::Table;

fn name(h: &pb::Hive) -> String {
    h.metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default()
}

fn table(p: &Printer, hives: &[pb::Hive]) -> Table {
    let mut t = Table::new(["NAME", "AGE"]);
    for h in hives {
        let created = h.metadata.as_ref().and_then(|m| m.create_time.as_ref());
        t.row([name(h), p.age(created)]);
    }
    t
}

/// Prints a list of Hives, sorted by name.
pub fn print_list(p: &Printer, out: &mut dyn Write, mut hives: Vec<pb::Hive>) -> Result<()> {
    hives.sort_by_cached_key(name);
    match p.format {
        OutputFormat::Table => Ok(table(p, &hives).write_to(out)?),
        _ => p.write_message(
            out,
            &pb::ListHivesResponse {
                hives,
                next_page_token: String::new(),
            },
        ),
    }
}

/// Prints one Hive.
pub fn print_one(p: &Printer, out: &mut dyn Write, hive: &pb::Hive) -> Result<()> {
    p.write_one(out, hive, || table(p, std::slice::from_ref(hive)))
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

    fn hive(name: &str, ago: u64) -> pb::Hive {
        pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                create_time: Some(timestamp_before(now(), Duration::from_secs(ago))),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn table_is_sorted() {
        let mut buf = SharedBuf::default();
        print_list(
            &Printer::at(OutputFormat::Table, now()),
            &mut buf,
            vec![
                hive("team-c", 72 * 3600),
                hive("team-a", 300),
                hive("team-b", 5 * 3600),
            ],
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "NAME     AGE\nteam-a   5m\nteam-b   5h\nteam-c   3d\n"
        );
    }

    #[test]
    fn list_yaml_and_one_json() {
        let h = pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: "team-a".into(),
                ..Default::default()
            }),
        };
        let mut buf = SharedBuf::default();
        print_list(
            &Printer::at(OutputFormat::Yaml, now()),
            &mut buf,
            vec![h.clone()],
        )
        .unwrap();
        assert_eq!(buf.contents(), "hives:\n- metadata:\n    name: team-a\n");
        let mut buf = SharedBuf::default();
        print_one(&Printer::at(OutputFormat::Json, now()), &mut buf, &h).unwrap();
        assert_eq!(
            buf.contents(),
            "{\n  \"metadata\": {\n    \"name\": \"team-a\"\n  }\n}\n"
        );
    }
}
