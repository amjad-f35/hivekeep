//! Tags: `HIVE NAME SCOPE STATE SNAPSHOT FIDELITY AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use super::enum_name;
use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::table::Table;

/// The completed durable copy of `snapshot`, if it has one.
fn durable(snapshot: Option<&pb::Snapshot>) -> Option<&pb::SnapshotStorage> {
    snapshot
        .and_then(|s| s.durable_snapshot.as_ref())
        .filter(|st| {
            st.state == pb::SnapshotStorageState::Completed as i32
                && st
                    .object
                    .as_ref()
                    .is_some_and(|o| !o.snapshot_uri.is_empty())
        })
}

fn snapshot(tag: &pb::Tag) -> Option<&pb::Snapshot> {
    tag.status.as_ref().and_then(|s| s.snapshot.as_ref())
}

/// Whether a Tag is usable. A Tag is `Pending` until the copy of its own
/// snapshot lands; until then nothing can be created from it.
pub fn state(tag: &pb::Tag) -> &'static str {
    if durable(snapshot(tag)).is_some() {
        "Ready"
    } else {
        "Pending"
    }
}

/// Sorts by hive, then name.
pub fn sort(tags: &mut [pb::Tag]) {
    tags.sort_by_cached_key(|t| {
        let m = t.metadata.clone().unwrap_or_default();
        (m.hive, m.name)
    });
}

fn table(p: &Printer, tags: &[pb::Tag]) -> Table {
    let mut t = Table::new([
        "HIVE", "NAME", "SCOPE", "STATE", "SNAPSHOT", "FIDELITY", "AGE",
    ]);
    for tag in tags {
        let m = tag.metadata.clone().unwrap_or_default();
        // A pending tag has no snapshot yet, so neither its URI nor its
        // fidelity says anything.
        let (uri, fidelity) = match (durable(snapshot(tag)), snapshot(tag)) {
            (Some(st), Some(snap)) => (
                st.object.clone().unwrap_or_default().snapshot_uri,
                enum_name::<pb::SnapshotFidelity>(snap.fidelity),
            ),
            _ => ("<none>".to_string(), "<none>".to_string()),
        };
        t.row([
            m.hive.clone(),
            m.name.clone(),
            enum_name::<pb::TagScope>(tag.scope),
            state(tag).to_string(),
            uri,
            fidelity,
            p.age(m.create_time.as_ref()),
        ]);
    }
    t
}

/// Prints a list of Tags.
pub fn print_list(p: &Printer, out: &mut dyn Write, mut tags: Vec<pb::Tag>) -> Result<()> {
    sort(&mut tags);
    match p.format {
        OutputFormat::Table => Ok(table(p, &tags).write_to(out)?),
        _ => p.write_message(
            out,
            &pb::ListTagsResponse {
                tags,
                next_page_token: String::new(),
            },
        ),
    }
}

/// Prints one Tag.
pub fn print_one(p: &Printer, out: &mut dyn Write, tag: &pb::Tag) -> Result<()> {
    p.write_one(out, tag, || table(p, std::slice::from_ref(tag)))
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

    fn tag(name: &str, scope: pb::TagScope, state: pb::SnapshotStorageState, ago: u64) -> pb::Tag {
        pb::Tag {
            metadata: Some(pb::ResourceMetadata {
                hive: "team-a".into(),
                name: name.into(),
                create_time: Some(timestamp_before(now(), Duration::from_secs(ago))),
                ..Default::default()
            }),
            scope: scope as i32,
            status: Some(pb::TagStatus {
                snapshot: Some(pb::Snapshot {
                    fidelity: pb::SnapshotFidelity::Memory as i32,
                    durable_snapshot: Some(pb::SnapshotStorage {
                        state: state as i32,
                        object: Some(pb::ObjectSnapshot {
                            snapshot_uri: format!("gs://private/hives/team-a/tags/{name}"),
                        }),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn table() {
        use pb::SnapshotStorageState::{Completed, Pending};
        let mut buf = SharedBuf::default();
        print_list(
            &Printer::at(OutputFormat::Table, now()),
            &mut buf,
            vec![
                tag("v2", pb::TagScope::Published, Completed, 300),
                tag("v1", pb::TagScope::Hive, Completed, 5 * 3600),
                tag("v3", pb::TagScope::Hive, Pending, 30),
            ],
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "HIVE     NAME   SCOPE                 STATE     SNAPSHOT                            FIDELITY                   AGE\n\
             team-a   v1     TAG_SCOPE_HIVE        Ready     gs://private/hives/team-a/tags/v1   SNAPSHOT_FIDELITY_MEMORY   5h\n\
             team-a   v2     TAG_SCOPE_PUBLISHED   Ready     gs://private/hives/team-a/tags/v2   SNAPSHOT_FIDELITY_MEMORY   5m\n\
             team-a   v3     TAG_SCOPE_HIVE        Pending   <none>                              <none>                     30s\n"
        );
    }

    #[test]
    fn missing_status_is_pending() {
        assert_eq!(state(&pb::Tag::default()), "Pending");
    }
}
