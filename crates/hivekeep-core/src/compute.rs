//! Compute-resource arithmetic for placement.
//!
//! The same [`pb::Resources`] shape is what an Actor's limits ask for, what a
//! Worker supplies and what a Worker's Actors hold, so the three subtract. A
//! zero dimension means none of that resource; a Worker reports every
//! dimension it has, so a zero in its capacity is one it cannot supply.
//! Empty totals are represented as `None` so an emptied total compares equal
//! to one that never existed.

use hivekeep_proto::api::v1 as pb;

/// Resources for `cpu_millicores` and `memory_bytes`, or `None` when both are
/// zero.
pub fn cpu_memory(cpu_millicores: i64, memory_bytes: i64) -> Option<pb::Resources> {
    if cpu_millicores == 0 && memory_bytes == 0 {
        None
    } else {
        Some(pb::Resources {
            cpu_millicores,
            memory_bytes,
        })
    }
}

/// Whether `have` leaves room for `want` in every dimension. A dimension
/// `want` leaves at zero asks for nothing.
pub fn covers(have: Option<&pb::Resources>, want: Option<&pb::Resources>) -> bool {
    let have = have.cloned().unwrap_or_default();
    let want = want.cloned().unwrap_or_default();
    have.cpu_millicores >= want.cpu_millicores && have.memory_bytes >= want.memory_bytes
}

/// Adjusts a Worker's allocation by one assignment; `sign` is `1` to add and
/// `-1` to release. Returns `None` once the allocation is back to nothing.
pub fn add_to_allocated(
    total: Option<&pb::WorkerResources>,
    assignment: &pb::ActorAssignment,
    sign: i64,
) -> Option<pb::WorkerResources> {
    let total = total.cloned().unwrap_or_default();
    let booked = assignment.resources.unwrap_or_default();
    let out = pb::WorkerResources {
        cpu_millicores: total.cpu_millicores + sign * booked.cpu_millicores,
        memory_bytes: total.memory_bytes + sign * booked.memory_bytes,
        actors: total.actors + sign as i32,
    };
    if out == pb::WorkerResources::default() {
        None
    } else {
        Some(out)
    }
}

/// What a set of assignments takes from a Worker, or `None` for none. Builds
/// the total from scratch, as a cross-check for [`add_to_allocated`].
pub fn sum_allocated(assignments: &[pb::ActorAssignment]) -> Option<pb::WorkerResources> {
    if assignments.is_empty() {
        return None;
    }
    let mut total = pb::WorkerResources {
        actors: assignments.len() as i32,
        ..Default::default()
    };
    for a in assignments {
        let r = a.resources.unwrap_or_default();
        total.cpu_millicores += r.cpu_millicores;
        total.memory_bytes += r.memory_bytes;
    }
    Some(total)
}

/// What a Worker has left after what its Actors hold. Negative once the
/// Worker is overcommitted.
pub fn free(
    capacity: Option<&pb::WorkerResources>,
    allocated: Option<&pb::WorkerResources>,
) -> pb::Resources {
    let c = capacity.cloned().unwrap_or_default();
    let a = allocated.cloned().unwrap_or_default();
    pb::Resources {
        cpu_millicores: c.cpu_millicores - a.cpu_millicores,
        memory_bytes: c.memory_bytes - a.memory_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GI: i64 = 1 << 30;

    fn assignment(uid: &str, cpu: i64, mem: i64) -> pb::ActorAssignment {
        pb::ActorAssignment {
            actor: Some(pb::ObjectRef {
                hive: "demo".into(),
                name: uid.into(),
            }),
            actor_uid: uid.into(),
            resources: cpu_memory(cpu, mem),
            ..Default::default()
        }
    }

    #[test]
    fn covers_cases() {
        let c = |cpu, mem| cpu_memory(cpu, mem);
        assert!(covers(c(4000, 8 * GI).as_ref(), c(2000, 4 * GI).as_ref()));
        assert!(covers(c(2000, 0).as_ref(), c(2000, 0).as_ref()));
        assert!(!covers(c(4000, GI).as_ref(), c(2000, 4 * GI).as_ref()));
        assert!(!covers(c(4000, 0).as_ref(), c(2000, 1 << 20).as_ref()));
        assert!(covers(None, None));
        assert!(!covers(c(-500, 0).as_ref(), c(1000, 0).as_ref()));
    }

    #[test]
    fn allocation_moves_by_one_assignment() {
        let mut total = add_to_allocated(None, &assignment("a", 1000, GI), 1);
        total = add_to_allocated(total.as_ref(), &assignment("b", 500, 2 * GI), 1);
        assert_eq!(
            total,
            Some(pb::WorkerResources {
                actors: 2,
                cpu_millicores: 1500,
                memory_bytes: 3 * GI
            })
        );
        total = add_to_allocated(total.as_ref(), &assignment("b", 500, 2 * GI), -1);
        assert_eq!(
            total,
            Some(pb::WorkerResources {
                actors: 1,
                cpu_millicores: 1000,
                memory_bytes: GI
            })
        );
        assert_eq!(
            add_to_allocated(total.as_ref(), &assignment("a", 1000, GI), -1),
            None
        );
    }

    #[test]
    fn actor_without_resources_still_takes_a_slot() {
        let a = pb::ActorAssignment {
            actor_uid: "a".into(),
            ..Default::default()
        };
        assert_eq!(
            add_to_allocated(None, &a, 1),
            Some(pb::WorkerResources {
                actors: 1,
                ..Default::default()
            })
        );
    }

    #[test]
    fn sum_matches() {
        assert_eq!(sum_allocated(&[]), None);
        assert_eq!(
            sum_allocated(&[assignment("a", 1000, GI), assignment("b", 500, 0)]),
            Some(pb::WorkerResources {
                actors: 2,
                cpu_millicores: 1500,
                memory_bytes: GI
            })
        );
    }

    #[test]
    fn free_goes_negative() {
        let cap = pb::WorkerResources {
            cpu_millicores: 1000,
            memory_bytes: GI,
            actors: 4,
        };
        let alloc = pb::WorkerResources {
            cpu_millicores: 1500,
            memory_bytes: 0,
            actors: 1,
        };
        assert_eq!(
            free(Some(&cap), Some(&alloc)),
            pb::Resources {
                cpu_millicores: -500,
                memory_bytes: GI
            }
        );
        assert_eq!(free(None, None), pb::Resources::default());
    }
}
