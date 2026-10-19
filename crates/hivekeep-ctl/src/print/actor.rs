//! Actors: `HIVE NAME TEMPLATE STATE WORKER POD WORKER IPS VERSION AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use super::enum_name;
use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::table::Table;

/// The template an Actor references, as `<hive>/<name>`.
pub fn template_display(a: &pb::Actor) -> String {
    let r = a.actor_template.clone().unwrap_or_default();
    format!("{}/{}", r.hive, r.name)
}

fn meta(a: &pb::Actor) -> pb::ResourceMetadata {
    a.metadata.clone().unwrap_or_default()
}

/// Sorts by hive, then template, then name.
pub fn sort(actors: &mut [pb::Actor]) {
    actors.sort_by_cached_key(|a| {
        let m = meta(a);
        (m.hive, template_display(a), m.name)
    });
}

fn table(p: &Printer, actors: &[pb::Actor]) -> Table {
    let mut t = Table::new([
        "HIVE",
        "NAME",
        "TEMPLATE",
        "STATE",
        "WORKER POD",
        "WORKER IPS",
        "VERSION",
        "AGE",
    ]);
    for a in actors {
        let m = meta(a);
        let status = a.status.clone().unwrap_or_default();
        let assignment = status.worker_assignment.as_ref();
        let worker = assignment
            .map(|w| format!("{}/{}", w.worker_namespace, w.worker_pod))
            .unwrap_or_else(|| "<none>".into());
        let ips = assignment
            .map(|w| w.worker_pod_ips.join(","))
            .unwrap_or_default();
        t.row([
            m.hive.clone(),
            m.name.clone(),
            template_display(a),
            enum_name::<pb::ActorState>(status.state),
            worker,
            ips,
            m.version.to_string(),
            p.age(m.create_time.as_ref()),
        ]);
    }
    t
}

/// Prints a list of Actors.
pub fn print_list(p: &Printer, out: &mut dyn Write, mut actors: Vec<pb::Actor>) -> Result<()> {
    sort(&mut actors);
    match p.format {
        OutputFormat::Table => Ok(table(p, &actors).write_to(out)?),
        _ => p.write_message(
            out,
            &pb::ListActorsResponse {
                actors,
                next_page_token: String::new(),
            },
        ),
    }
}

/// Prints one Actor.
pub fn print_one(p: &Printer, out: &mut dyn Write, actor: &pb::Actor) -> Result<()> {
    p.write_one(out, actor, || table(p, std::slice::from_ref(actor)))
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

    fn actor(
        hive: &str,
        name: &str,
        tpl: (&str, &str),
        state: pb::ActorState,
        ago: u64,
    ) -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.into(),
                name: name.into(),
                create_time: Some(timestamp_before(now(), Duration::from_secs(ago))),
                ..Default::default()
            }),
            actor_template: Some(pb::ObjectRef {
                hive: tpl.0.into(),
                name: tpl.1.into(),
            }),
            status: Some(pb::ActorStatus {
                state: state as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn render(format: OutputFormat, actors: Vec<pb::Actor>) -> String {
        let mut buf = SharedBuf::default();
        print_list(&Printer::at(format, now()), &mut buf, actors).unwrap();
        buf.contents()
    }

    #[test]
    fn table_with_assignment() {
        let mut a = actor(
            "team-a",
            "id-1",
            ("default", "template-1"),
            pb::ActorState::Running,
            300,
        );
        a.metadata.as_mut().unwrap().version = 2;
        a.status.as_mut().unwrap().worker_assignment = Some(pb::WorkerAssignment {
            worker_namespace: "worker-ns".into(),
            worker_pod: "pod-1".into(),
            worker_pod_ips: vec!["1.2.3.4".into(), "fd00::1".into()],
            ..Default::default()
        });
        assert_eq!(
            render(OutputFormat::Table, vec![a]),
            "HIVE     NAME   TEMPLATE             STATE                 WORKER POD        WORKER IPS        VERSION   AGE\n\
             team-a   id-1   default/template-1   ACTOR_STATE_RUNNING   worker-ns/pod-1   1.2.3.4,fd00::1   2         5m\n"
        );
    }

    #[test]
    fn table_is_sorted() {
        let got = render(
            OutputFormat::Table,
            vec![
                actor(
                    "team-b",
                    "zebra",
                    ("default", "template-1"),
                    pb::ActorState::Suspended,
                    72 * 3600,
                ),
                actor(
                    "team-a",
                    "alpha",
                    ("default", "template-1"),
                    pb::ActorState::Running,
                    300,
                ),
                actor(
                    "team-a",
                    "beta",
                    ("other", "template-2"),
                    pb::ActorState::Suspended,
                    5 * 3600,
                ),
            ],
        );
        assert_eq!(
            got,
            "HIVE     NAME    TEMPLATE             STATE                   WORKER POD   WORKER IPS   VERSION   AGE\n\
             team-a   alpha   default/template-1   ACTOR_STATE_RUNNING     <none>                    0         5m\n\
             team-a   beta    other/template-2     ACTOR_STATE_SUSPENDED   <none>                    0         5h\n\
             team-b   zebra   default/template-1   ACTOR_STATE_SUSPENDED   <none>                    0         3d\n"
        );
    }

    fn minimal() -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                name: "id-1".into(),
                version: 2,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn list_json_and_yaml() {
        assert_eq!(
            render(OutputFormat::Json, vec![minimal()]),
            "{\n  \"actors\": [\n    {\n      \"metadata\": {\n        \"name\": \"id-1\",\n        \"version\": \"2\"\n      }\n    }\n  ]\n}\n"
        );
        assert_eq!(
            render(OutputFormat::Yaml, vec![minimal()]),
            "actors:\n- metadata:\n    name: id-1\n    version: \"2\"\n"
        );
    }

    #[test]
    fn one_json_and_yaml() {
        let mut buf = SharedBuf::default();
        print_one(
            &Printer::at(OutputFormat::Json, now()),
            &mut buf,
            &minimal(),
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "{\n  \"metadata\": {\n    \"name\": \"id-1\",\n    \"version\": \"2\"\n  }\n}\n"
        );
        let mut buf = SharedBuf::default();
        print_one(
            &Printer::at(OutputFormat::Yaml, now()),
            &mut buf,
            &minimal(),
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "metadata:\n  name: id-1\n  version: \"2\"\n"
        );
    }
}
