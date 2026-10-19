//! Workers: `NAME POOL STATE ACTORS CPU(MILLICORES) MEMORY POD AGE`.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use super::enum_name;
use crate::error::Result;
use crate::output::{OutputFormat, Printer};
use crate::quantity::format_bytes;
use crate::table::Table;

fn status(w: &pb::Worker) -> pb::WorkerStatus {
    w.status.clone().unwrap_or_default()
}

/// How full a Worker is: `FREE`, or `ASSIGNED(<hosted>/<capacity>)`. A count
/// rather than the Actors themselves, which a listing does not carry.
pub fn occupancy(w: &pb::Worker) -> String {
    let s = status(w);
    let hosted = s.allocated.unwrap_or_default().actors;
    if hosted == 0 {
        return "FREE".into();
    }
    format!(
        "ASSIGNED({hosted}/{})",
        s.capacity.unwrap_or_default().actors
    )
}

/// The Worker's primary sandbox class, or empty when it reports none.
pub fn sandbox_class(w: &pb::Worker) -> String {
    status(w)
        .sandbox_classes
        .first()
        .map(|c| c.sandbox_class.clone())
        .unwrap_or_default()
}

/// Sorts by namespace, then pool, then pod.
pub fn sort(workers: &mut [pb::Worker]) {
    workers.sort_by(|a, b| {
        (&a.worker_namespace, &a.worker_pool, &a.worker_pod).cmp(&(
            &b.worker_namespace,
            &b.worker_pool,
            &b.worker_pod,
        ))
    });
}

/// `allocated/capacity`, or `-` when the Worker has no capacity to compare
/// against.
fn resource_column(allocated: i64, capacity: i64, format: fn(i64) -> String) -> String {
    if capacity == 0 {
        return "-".into();
    }
    format!("{}/{}", format(allocated), format(capacity))
}

fn table(p: &Printer, workers: &[pb::Worker]) -> Table {
    let mut t = Table::new([
        "NAME",
        "POOL",
        "STATE",
        "ACTORS",
        "CPU(MILLICORES)",
        "MEMORY",
        "POD",
        "AGE",
    ]);
    for w in workers {
        let m = w.metadata.clone().unwrap_or_default();
        let s = status(w);
        let cap = s.capacity.unwrap_or_default();
        let alloc = s.allocated.unwrap_or_default();
        t.row([
            m.name.clone(),
            w.worker_pool.clone(),
            enum_name::<pb::WorkerState>(s.state),
            format!("{}/{}", alloc.actors, cap.actors),
            resource_column(alloc.cpu_millicores, cap.cpu_millicores, |n| n.to_string()),
            resource_column(alloc.memory_bytes, cap.memory_bytes, format_bytes),
            format!("{}/{}", w.worker_namespace, w.worker_pod),
            p.age(m.create_time.as_ref()),
        ]);
    }
    t
}

/// Prints a list of Workers.
pub fn print_list(p: &Printer, out: &mut dyn Write, mut workers: Vec<pb::Worker>) -> Result<()> {
    sort(&mut workers);
    match p.format {
        OutputFormat::Table => Ok(table(p, &workers).write_to(out)?),
        _ => p.write_message(
            out,
            &pb::ListWorkersResponse {
                workers,
                next_page_token: String::new(),
            },
        ),
    }
}

/// Prints one Worker.
pub fn print_one(p: &Printer, out: &mut dyn Write, worker: &pb::Worker) -> Result<()> {
    p.write_one(out, worker, || table(p, std::slice::from_ref(worker)))
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

    fn render(format: OutputFormat, workers: Vec<pb::Worker>) -> String {
        let mut buf = SharedBuf::default();
        print_list(&Printer::at(format, now()), &mut buf, workers).unwrap();
        buf.contents()
    }

    #[test]
    fn table_with_capacity() {
        let w = pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: "worker-1".into(),
                create_time: Some(timestamp_before(now(), Duration::from_secs(72 * 3600))),
                ..Default::default()
            }),
            worker_namespace: "default".into(),
            worker_pool: "pool-1".into(),
            worker_pod: "pod-1".into(),
            status: Some(pb::WorkerStatus {
                state: pb::WorkerState::Active as i32,
                capacity: Some(pb::WorkerResources {
                    actors: 4,
                    cpu_millicores: 2000,
                    memory_bytes: 4 << 30,
                }),
                allocated: Some(pb::WorkerResources {
                    actors: 1,
                    cpu_millicores: 500,
                    memory_bytes: 1 << 30,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            render(OutputFormat::Table, vec![w]),
            "NAME       POOL     STATE                 ACTORS   CPU(MILLICORES)   MEMORY    POD             AGE\n\
             worker-1   pool-1   WORKER_STATE_ACTIVE   1/4      500/2000          1Gi/4Gi   default/pod-1   3d\n"
        );
    }

    #[test]
    fn table_without_status_or_metadata() {
        let w = pb::Worker {
            worker_namespace: "default".into(),
            worker_pool: "pool-1".into(),
            worker_pod: "pod-1".into(),
            ..Default::default()
        };
        assert_eq!(
            render(OutputFormat::Table, vec![w]),
            "NAME   POOL     STATE                      ACTORS   CPU(MILLICORES)   MEMORY   POD             AGE\n\
             \x20      pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-1   56y\n"
        );
    }

    #[test]
    fn table_is_sorted() {
        let mk = |name: &str, ns: &str, pool: &str, pod: &str| pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                create_time: Some(timestamp_before(now(), Duration::from_secs(300))),
                ..Default::default()
            }),
            worker_namespace: ns.into(),
            worker_pool: pool.into(),
            worker_pod: pod.into(),
            ..Default::default()
        };
        let got = render(
            OutputFormat::Table,
            vec![
                mk("worker-z", "default", "pool-1", "pod-z"),
                mk("worker-a", "default", "pool-1", "pod-a"),
                mk("worker-o", "other", "pool-2", "pod-1"),
            ],
        );
        assert_eq!(
            got,
            "NAME       POOL     STATE                      ACTORS   CPU(MILLICORES)   MEMORY   POD             AGE\n\
             worker-a   pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-a   5m\n\
             worker-z   pool-1   WORKER_STATE_UNSPECIFIED   0/0      -                 -        default/pod-z   5m\n\
             worker-o   pool-2   WORKER_STATE_UNSPECIFIED   0/0      -                 -        other/pod-1     5m\n"
        );
    }

    #[test]
    fn occupancy_counts() {
        let mut w = pb::Worker::default();
        assert_eq!(occupancy(&w), "FREE");
        w.status = Some(pb::WorkerStatus {
            capacity: Some(pb::WorkerResources {
                actors: 10,
                ..Default::default()
            }),
            allocated: Some(pb::WorkerResources {
                actors: 3,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(occupancy(&w), "ASSIGNED(3/10)");
    }

    #[test]
    fn one_json() {
        let w = pb::Worker {
            metadata: Some(pb::ResourceMetadata {
                name: "worker-1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = SharedBuf::default();
        print_one(&Printer::at(OutputFormat::Json, now()), &mut buf, &w).unwrap();
        assert_eq!(
            buf.contents(),
            "{\n  \"metadata\": {\n    \"name\": \"worker-1\"\n  }\n}\n"
        );
    }
}
