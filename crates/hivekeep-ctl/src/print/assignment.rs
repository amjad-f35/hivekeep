//! A Worker's Actor assignments: `ACTOR UID CPU(MILLICORES) MEMORY AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::quantity::format_bytes;
use crate::table::Table;

fn actor_display(a: &pb::ActorAssignment) -> String {
    let r = a.actor.clone().unwrap_or_default();
    format!("{}/{}", r.hive, r.name)
}

/// Prints the assignments of one Worker, sorted by Actor.
pub fn print_list(
    p: &Printer,
    out: &mut dyn Write,
    mut assignments: Vec<pb::ActorAssignment>,
) -> Result<()> {
    assignments.sort_by_cached_key(actor_display);
    match p.format {
        OutputFormat::Table => {
            let mut t = Table::new(["ACTOR", "UID", "CPU(MILLICORES)", "MEMORY", "AGE"]);
            for a in &assignments {
                let res = a.resources.unwrap_or_default();
                let created = a.metadata.as_ref().and_then(|m| m.create_time.as_ref());
                t.row([
                    actor_display(a),
                    a.actor_uid.clone(),
                    res.cpu_millicores.to_string(),
                    format_bytes(res.memory_bytes),
                    p.age(created),
                ]);
            }
            Ok(t.write_to(out)?)
        }
        _ => p.write_message(
            out,
            &pb::ListWorkerActorAssignmentsResponse {
                actor_assignments: assignments,
                next_page_token: String::new(),
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::age::timestamp_before;
    use crate::io::SharedBuf;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn table_is_sorted_by_actor() {
        let now = UNIX_EPOCH + Duration::from_secs(1_767_268_800);
        let mk = |hive: &str, name: &str| pb::ActorAssignment {
            metadata: Some(pb::ResourceMetadata {
                name: format!("{hive}-{name}"),
                create_time: Some(timestamp_before(now, Duration::from_secs(600))),
                ..Default::default()
            }),
            actor: Some(pb::ObjectRef {
                hive: hive.into(),
                name: name.into(),
            }),
            actor_uid: format!("uid-{name}"),
            resources: Some(pb::Resources {
                cpu_millicores: 500,
                memory_bytes: 512 << 20,
            }),
        };
        let mut buf = SharedBuf::default();
        print_list(
            &Printer::at(OutputFormat::Table, now),
            &mut buf,
            vec![mk("team-b", "x"), mk("team-a", "y")],
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "ACTOR      UID     CPU(MILLICORES)   MEMORY   AGE\n\
             team-a/y   uid-y   500               512Mi    10m\n\
             team-b/x   uid-x   500               512Mi    10m\n"
        );
    }
}
