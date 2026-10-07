//! The Actor lifecycle as a state machine.
//!
//! ```text
//!               BeginResume            FinishResume
//!   SUSPENDED ─────────────▶ RESUMING ─────────────▶ RUNNING
//!     ▲  ▲                                            │  │
//!     │  │ FinishSuspend                 BeginSuspend │  │ BeginRevert
//!     │  └──────────────── SUSPENDING ◀───────────────┘  ▼
//!     │                                              REVERTING
//!     └──────────────────── FinishRevert ────────────────┘
//!
//!   Crash:        any live state ─▶ CRASHED   (CRASHED ─BeginRevert─▶ REVERTING)
//!   MarkDeleting: any state      ─▶ DELETING  (terminal)
//! ```
//!
//! New Actors start [`ActorState::Suspended`]. Re-applying the event that led
//! to the current state is accepted as a no-op so that retried workflows can
//! fast-forward.

use std::fmt;

use hivekeep_proto::api::v1 as pb;

/// Lifecycle state of an Actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorState {
    /// A worker is assigned and the workload is starting.
    Resuming,
    /// The workload is running.
    Running,
    /// A checkpoint is being taken.
    Suspending,
    /// Not running; the state lives in a snapshot.
    Suspended,
    /// The workload failed.
    Crashed,
    /// Deletion is in progress. Terminal.
    Deleting,
    /// Being returned to the newest durable snapshot.
    Reverting,
}

impl ActorState {
    /// Every state, in wire order.
    pub const ALL: [ActorState; 7] = [
        ActorState::Resuming,
        ActorState::Running,
        ActorState::Suspending,
        ActorState::Suspended,
        ActorState::Crashed,
        ActorState::Deleting,
        ActorState::Reverting,
    ];

    /// The state of a newly created Actor.
    pub const INITIAL: ActorState = ActorState::Suspended;

    /// States in which a node may move the Actor's snapshot to or from object
    /// storage, and so may hold credentials for it: a resume fetches it, a
    /// running Actor keeps what its resume minted, a suspend uploads it and a
    /// suspended Actor may have a background upload running. In every other
    /// state no node acts for the Actor.
    pub const TRANSFER_STATES: [ActorState; 4] = [
        ActorState::Resuming,
        ActorState::Running,
        ActorState::Suspending,
        ActorState::Suspended,
    ];

    /// Converts from the wire enum. `None` for UNSPECIFIED or unknown values.
    pub fn from_proto(value: i32) -> Option<ActorState> {
        match pb::ActorState::try_from(value).ok()? {
            pb::ActorState::Unspecified => None,
            pb::ActorState::Resuming => Some(ActorState::Resuming),
            pb::ActorState::Running => Some(ActorState::Running),
            pb::ActorState::Suspending => Some(ActorState::Suspending),
            pb::ActorState::Suspended => Some(ActorState::Suspended),
            pb::ActorState::Crashed => Some(ActorState::Crashed),
            pb::ActorState::Deleting => Some(ActorState::Deleting),
            pb::ActorState::Reverting => Some(ActorState::Reverting),
        }
    }

    /// The state recorded on an Actor, if any.
    pub fn of(actor: &pb::Actor) -> Option<ActorState> {
        actor
            .status
            .as_ref()
            .and_then(|s| ActorState::from_proto(s.state))
    }

    /// The wire enum.
    pub fn to_proto(self) -> pb::ActorState {
        match self {
            ActorState::Resuming => pb::ActorState::Resuming,
            ActorState::Running => pb::ActorState::Running,
            ActorState::Suspending => pb::ActorState::Suspending,
            ActorState::Suspended => pb::ActorState::Suspended,
            ActorState::Crashed => pb::ActorState::Crashed,
            ActorState::Deleting => pb::ActorState::Deleting,
            ActorState::Reverting => pb::ActorState::Reverting,
        }
    }

    /// Whether a node may hold snapshot credentials for the Actor.
    pub fn is_transfer_state(self) -> bool {
        Self::TRANSFER_STATES.contains(&self)
    }

    /// Whether no further transition is possible.
    pub fn is_terminal(self) -> bool {
        self == ActorState::Deleting
    }

    /// Whether the Actor's template may be changed. Only a SUSPENDED Actor
    /// can switch templates.
    pub fn allows_template_change(self) -> bool {
        self == ActorState::Suspended
    }

    /// Applies `event`, returning the outcome or why it is refused.
    pub fn apply(self, event: ActorEvent) -> Result<Transition, TransitionError> {
        use ActorEvent as E;
        use ActorState as S;
        let to = match (event, self) {
            (E::MarkDeleting, S::Deleting) => return Ok(Transition::Unchanged(self)),
            (E::MarkDeleting, _) => S::Deleting,

            // Deletion is final; a crash report for a deleting Actor is ignored.
            (E::Crash, S::Deleting | S::Crashed) => return Ok(Transition::Unchanged(self)),
            (E::Crash, _) => S::Crashed,

            (E::BeginResume, S::Suspended) => S::Resuming,
            (E::BeginResume, S::Resuming | S::Running) => return Ok(Transition::Unchanged(self)),
            (E::FinishResume, S::Resuming) => S::Running,
            (E::FinishResume, S::Running) => return Ok(Transition::Unchanged(self)),

            (E::BeginSuspend, S::Running) => S::Suspending,
            (E::BeginSuspend, S::Suspending | S::Suspended) => {
                return Ok(Transition::Unchanged(self))
            }
            (E::FinishSuspend, S::Suspending) => S::Suspended,

            (E::BeginRevert { .. }, S::Running | S::Crashed) => S::Reverting,
            (
                E::BeginRevert {
                    has_local_snapshot: true,
                },
                S::Suspended,
            ) => S::Reverting,
            (E::BeginRevert { .. }, S::Reverting) => return Ok(Transition::Unchanged(self)),
            (E::FinishRevert, S::Reverting) => S::Suspended,

            _ => return Err(TransitionError { from: self, event }),
        };
        Ok(Transition::Moved { from: self, to })
    }
}

impl fmt::Display for ActorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_proto().as_str_name())
    }
}

impl From<ActorState> for pb::ActorState {
    fn from(s: ActorState) -> Self {
        s.to_proto()
    }
}

impl From<ActorState> for i32 {
    fn from(s: ActorState) -> Self {
        s.to_proto() as i32
    }
}

/// Something that happens to an Actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorEvent {
    /// A worker was assigned for a resume.
    BeginResume,
    /// The workload started on its worker.
    FinishResume,
    /// A checkpoint was requested.
    BeginSuspend,
    /// The checkpoint completed and the worker was released.
    FinishSuspend,
    /// A revert was requested. A SUSPENDED Actor is only revertable when its
    /// newest snapshot exists only on a node.
    BeginRevert {
        /// Whether the newest snapshot is node-local.
        has_local_snapshot: bool,
    },
    /// The revert completed.
    FinishRevert,
    /// The workload or its worker failed.
    Crash,
    /// Deletion started.
    MarkDeleting,
}

impl fmt::Display for ActorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ActorEvent::BeginResume => "begin resume",
            ActorEvent::FinishResume => "finish resume",
            ActorEvent::BeginSuspend => "begin suspend",
            ActorEvent::FinishSuspend => "finish suspend",
            ActorEvent::BeginRevert { .. } => "begin revert",
            ActorEvent::FinishRevert => "finish revert",
            ActorEvent::Crash => "crash",
            ActorEvent::MarkDeleting => "mark deleting",
        };
        f.write_str(s)
    }
}

/// Result of applying an [`ActorEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// The Actor moves to a new state.
    Moved {
        /// The state before.
        from: ActorState,
        /// The state after.
        to: ActorState,
    },
    /// The event was already applied; nothing changes.
    Unchanged(ActorState),
}

impl Transition {
    /// The state after the transition.
    pub fn state(self) -> ActorState {
        match self {
            Transition::Moved { to, .. } => to,
            Transition::Unchanged(s) => s,
        }
    }

    /// Whether the state changes.
    pub fn changed(self) -> bool {
        matches!(self, Transition::Moved { .. })
    }
}

/// An event that is not allowed in the Actor's current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cannot {event} an Actor in state {from}")]
pub struct TransitionError {
    /// The state the Actor is in.
    pub from: ActorState,
    /// The refused event.
    pub event: ActorEvent,
}

impl From<TransitionError> for tonic::Status {
    fn from(e: TransitionError) -> Self {
        tonic::Status::failed_precondition(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ActorEvent as E;
    use ActorState as S;

    fn moved(from: S, e: E) -> S {
        match from.apply(e) {
            Ok(Transition::Moved { to, .. }) => to,
            other => panic!("{from} --{e}--> {other:?}"),
        }
    }

    #[test]
    fn wire_round_trip() {
        for s in S::ALL {
            assert_eq!(S::from_proto(s.to_proto() as i32), Some(s));
        }
        assert_eq!(S::from_proto(0), None);
        assert_eq!(S::from_proto(99), None);
        assert_eq!(S::Running.to_string(), "ACTOR_STATE_RUNNING");
    }

    #[test]
    fn transfer_states_classify_every_state() {
        let transfer: Vec<_> = S::ALL
            .iter()
            .copied()
            .filter(|s| s.is_transfer_state())
            .collect();
        assert_eq!(
            transfer,
            vec![S::Resuming, S::Running, S::Suspending, S::Suspended]
        );
        for s in [S::Crashed, S::Deleting, S::Reverting] {
            assert!(!s.is_transfer_state());
        }
    }

    #[test]
    fn happy_path() {
        let mut s = S::INITIAL;
        for e in [
            E::BeginResume,
            E::FinishResume,
            E::BeginSuspend,
            E::FinishSuspend,
        ] {
            s = moved(s, e);
        }
        assert_eq!(s, S::Suspended);
    }

    #[test]
    fn revert_paths() {
        assert_eq!(
            moved(
                S::Running,
                E::BeginRevert {
                    has_local_snapshot: false
                }
            ),
            S::Reverting
        );
        assert_eq!(
            moved(
                S::Crashed,
                E::BeginRevert {
                    has_local_snapshot: false
                }
            ),
            S::Reverting
        );
        assert_eq!(
            moved(
                S::Suspended,
                E::BeginRevert {
                    has_local_snapshot: true
                }
            ),
            S::Reverting
        );
        assert!(S::Suspended
            .apply(E::BeginRevert {
                has_local_snapshot: false
            })
            .is_err());
        assert_eq!(moved(S::Reverting, E::FinishRevert), S::Suspended);
        for s in [S::Resuming, S::Suspending, S::Deleting] {
            assert!(
                s.apply(E::BeginRevert {
                    has_local_snapshot: true
                })
                .is_err(),
                "{s}"
            );
        }
    }

    #[test]
    fn crash_and_delete() {
        for s in [
            S::Resuming,
            S::Running,
            S::Suspending,
            S::Suspended,
            S::Reverting,
        ] {
            assert_eq!(moved(s, E::Crash), S::Crashed);
            assert_eq!(moved(s, E::MarkDeleting), S::Deleting);
        }
        assert_eq!(
            S::Crashed.apply(E::Crash),
            Ok(Transition::Unchanged(S::Crashed))
        );
        assert_eq!(
            S::Deleting.apply(E::Crash),
            Ok(Transition::Unchanged(S::Deleting))
        );
        assert_eq!(
            S::Deleting.apply(E::MarkDeleting),
            Ok(Transition::Unchanged(S::Deleting))
        );
        for e in [
            E::BeginResume,
            E::FinishResume,
            E::BeginSuspend,
            E::FinishSuspend,
            E::FinishRevert,
        ] {
            assert!(S::Deleting.apply(e).is_err(), "{e}");
        }
        assert!(S::Deleting.is_terminal());
    }

    #[test]
    fn idempotent_reentry() {
        assert_eq!(
            S::Resuming.apply(E::BeginResume).unwrap().state(),
            S::Resuming
        );
        assert!(!S::Running.apply(E::BeginResume).unwrap().changed());
        assert!(!S::Suspended.apply(E::BeginSuspend).unwrap().changed());
        assert!(!S::Suspending.apply(E::BeginSuspend).unwrap().changed());
    }

    #[test]
    fn refused_transitions_explain_themselves() {
        let err = S::Suspended.apply(E::FinishSuspend).unwrap_err();
        assert_eq!(
            err.to_string(),
            "cannot finish suspend an Actor in state ACTOR_STATE_SUSPENDED"
        );
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(S::Crashed.apply(E::BeginResume).is_err());
        assert!(S::Suspended.allows_template_change() && !S::Running.allows_template_change());
    }

    #[test]
    fn of_actor() {
        let a = pb::Actor {
            status: Some(pb::ActorStatus {
                state: pb::ActorState::Running as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(S::of(&a), Some(S::Running));
        assert_eq!(S::of(&pb::Actor::default()), None);
    }
}
