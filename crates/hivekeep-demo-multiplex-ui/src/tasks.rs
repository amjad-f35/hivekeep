//! Simulated task assignments.
//!
//! The dashboard's "Give a task" button hands a canned prompt to a random
//! agent. The control plane has no notion of these tasks: their lifecycle is
//! driven purely by wall-clock time since the assignment was made, so the
//! page can show work flowing through queued, running and completed while
//! the real agents rotate across workers.

use serde::Serialize;
use std::sync::Mutex;

/// How many assignments the board remembers (newest first).
pub const MAX_ASSIGNMENTS: usize = 50;

/// Canned prompts handed out by "Give a task".
pub const PREDEFINED_TASKS: [&str; 10] = [
    "Read main.py and propose two concrete improvements",
    "Contrast a Kubernetes ReplicaSet with a Deployment",
    "Write a Python function that finds duplicates in a list",
    "Explain when to use a Kubernetes Job instead of a bare Pod",
    "Name three habits that make Python code easier to test",
    "Summarise how Python reclaims memory, in one paragraph",
    "Suggest two ways to make a Kubernetes Service more robust",
    "Write a unit test for a max-of-two-integers function",
    "Describe what an admission controller does in Kubernetes",
    "Sketch a retry-with-backoff policy for an unreliable HTTP client",
];

/// Lifecycle of one assignment as the dashboard renders it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Waiting for its agent to get a worker.
    Queued,
    /// Its agent is (notionally) working on it.
    Running,
    /// Done.
    Completed,
}

/// One task handed to one agent. Times are Unix seconds as floats, which is
/// what the page's relative-time rendering expects.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assignment {
    pub id: String,
    pub agent: String,
    pub task: String,
    pub state: TaskState,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub completed_at: Option<f64>,
    pub queue_for: f64,
    pub run_for: f64,
}

/// The state an assignment should be in at `now`.
pub fn compute_state(a: &Assignment, now: f64) -> TaskState {
    let elapsed = now - a.created_at;
    if elapsed < a.queue_for {
        TaskState::Queued
    } else if elapsed < a.queue_for + a.run_for {
        TaskState::Running
    } else {
        TaskState::Completed
    }
}

/// Moves an assignment to the state it should be in at `now`, stamping the
/// start and completion times the first time each transition is observed.
/// The stamps are the scheduled transition times, not `now`, so a page that
/// polls rarely still sees accurate durations.
pub fn advance(a: &mut Assignment, now: f64) {
    let next = compute_state(a, now);
    if next == a.state {
        return;
    }
    a.state = next;
    match next {
        TaskState::Running if a.started_at.is_none() => {
            a.started_at = Some(a.created_at + a.queue_for)
        }
        TaskState::Completed if a.completed_at.is_none() => {
            a.completed_at = Some(a.created_at + a.queue_for + a.run_for)
        }
        _ => {}
    }
}

/// Rounds half up to one decimal place.
pub fn round_one(v: f64) -> f64 {
    ((v * 10.0 + 0.5) as i64) as f64 / 10.0
}

/// Source of the random choices a new assignment needs; abstracted so tests
/// can pin them.
pub trait Chooser {
    /// A uniformly chosen index in `0..n` (`n > 0`).
    fn index(&mut self, n: usize) -> usize;
    /// A uniformly chosen value in `[0, 1)`.
    fn unit(&mut self) -> f64;
}

/// [`Chooser`] over the thread-local RNG.
pub struct RandomChooser;

impl Chooser for RandomChooser {
    fn index(&mut self, n: usize) -> usize {
        use rand::Rng;
        rand::rng().random_range(0..n)
    }
    fn unit(&mut self) -> f64 {
        rand::random::<f64>()
    }
}

/// Why no assignment could be made.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GiveError {
    #[error("no agents available in the demo hive")]
    NoAgents,
}

/// Builds a fresh assignment: a random agent, a random task, a queue phase
/// of 2–5 s and a run phase of 9–16 s.
pub fn new_assignment(
    agents: &[String],
    chooser: &mut dyn Chooser,
    now: f64,
    now_millis: i64,
) -> Result<Assignment, GiveError> {
    if agents.is_empty() {
        return Err(GiveError::NoAgents);
    }
    let agent = agents[chooser.index(agents.len())].clone();
    let task = PREDEFINED_TASKS[chooser.index(PREDEFINED_TASKS.len())].to_string();
    let queue_for = round_one(2.0 + chooser.unit() * 3.0);
    let run_for = round_one(9.0 + chooser.unit() * 7.0);
    Ok(Assignment {
        id: format!("asg-{now_millis}"),
        agent,
        task,
        state: TaskState::Queued,
        created_at: now,
        started_at: None,
        completed_at: None,
        queue_for,
        run_for,
    })
}

/// The in-memory list of assignments, newest first and capped at
/// [`MAX_ASSIGNMENTS`].
#[derive(Default)]
pub struct TaskBoard {
    items: Mutex<Vec<Assignment>>,
}

impl TaskBoard {
    /// Records a new assignment at the front, dropping the oldest past the cap.
    pub fn push(&self, a: Assignment) {
        let mut items = self.items.lock().expect("task board poisoned");
        items.insert(0, a);
        items.truncate(MAX_ASSIGNMENTS);
    }

    /// Advances every assignment to `now` and returns a copy of the list.
    pub fn snapshot(&self, now: f64) -> Vec<Assignment> {
        let mut items = self.items.lock().expect("task board poisoned");
        for a in items.iter_mut() {
            advance(a, now);
        }
        items.clone()
    }
}

/// The current time as Unix seconds and milliseconds.
pub fn now() -> (f64, i64) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs_f64(), d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed {
        indices: Vec<usize>,
        units: Vec<f64>,
    }
    impl Chooser for Fixed {
        fn index(&mut self, n: usize) -> usize {
            self.indices.remove(0) % n
        }
        fn unit(&mut self) -> f64 {
            self.units.remove(0)
        }
    }

    fn asg(created: f64, queue: f64, run: f64) -> Assignment {
        Assignment {
            id: "asg-1".into(),
            agent: "agent-luna".into(),
            task: "t".into(),
            state: TaskState::Queued,
            created_at: created,
            started_at: None,
            completed_at: None,
            queue_for: queue,
            run_for: run,
        }
    }

    #[test]
    fn compute_state_boundaries() {
        let a = asg(100.0, 2.0, 10.0);
        for (now, want) in [
            (100.0, TaskState::Queued),
            (101.9, TaskState::Queued),
            (102.0, TaskState::Running),
            (111.9, TaskState::Running),
            (112.0, TaskState::Completed),
            (500.0, TaskState::Completed),
        ] {
            assert_eq!(compute_state(&a, now), want, "now={now}");
        }
    }

    #[test]
    fn advance_stamps_scheduled_times() {
        let mut a = asg(100.0, 2.0, 10.0);
        advance(&mut a, 101.0);
        assert_eq!(
            (a.state, a.started_at, a.completed_at),
            (TaskState::Queued, None, None)
        );
        advance(&mut a, 105.0);
        assert_eq!(
            (a.state, a.started_at, a.completed_at),
            (TaskState::Running, Some(102.0), None)
        );
        advance(&mut a, 200.0);
        assert_eq!(
            (a.state, a.started_at, a.completed_at),
            (TaskState::Completed, Some(102.0), Some(112.0))
        );
    }

    #[test]
    fn advance_straight_to_completed_skips_start_stamp() {
        let mut a = asg(100.0, 2.0, 10.0);
        advance(&mut a, 200.0);
        assert_eq!(a.state, TaskState::Completed);
        assert_eq!(a.started_at, None);
        assert_eq!(a.completed_at, Some(112.0));
    }

    #[test]
    fn round_one_cases() {
        for (v, want) in [
            (2.0, 2.0),
            (2.04, 2.0),
            (2.06, 2.1),
            (4.99, 5.0),
            (15.96, 16.0),
        ] {
            assert!(
                (round_one(v) - want).abs() < 1e-9,
                "{v} -> {}",
                round_one(v)
            );
        }
    }

    #[test]
    fn new_assignment_uses_choices() {
        let agents = vec!["agent-luna".to_string(), "agent-mars".to_string()];
        let mut c = Fixed {
            indices: vec![1, 3],
            units: vec![0.0, 0.999],
        };
        let a = new_assignment(&agents, &mut c, 10.0, 10_000).unwrap();
        assert_eq!(a.id, "asg-10000");
        assert_eq!(a.agent, "agent-mars");
        assert_eq!(a.task, PREDEFINED_TASKS[3]);
        assert_eq!(a.state, TaskState::Queued);
        assert_eq!(a.queue_for, 2.0);
        assert_eq!(a.run_for, 16.0);
        assert_eq!(a.created_at, 10.0);
    }

    #[test]
    fn new_assignment_ranges_with_real_rng() {
        let agents = vec!["a".to_string()];
        for _ in 0..200 {
            let a = new_assignment(&agents, &mut RandomChooser, 0.0, 0).unwrap();
            assert!((2.0..=5.0).contains(&a.queue_for), "{}", a.queue_for);
            assert!((9.0..=16.0).contains(&a.run_for), "{}", a.run_for);
            assert!(PREDEFINED_TASKS.contains(&a.task.as_str()));
        }
    }

    #[test]
    fn new_assignment_without_agents() {
        assert_eq!(
            new_assignment(&[], &mut RandomChooser, 0.0, 0),
            Err(GiveError::NoAgents)
        );
    }

    #[test]
    fn board_is_newest_first_and_capped() {
        let board = TaskBoard::default();
        for i in 0..(MAX_ASSIGNMENTS + 5) {
            let mut a = asg(i as f64, 1.0, 1.0);
            a.id = format!("asg-{i}");
            board.push(a);
        }
        let snap = board.snapshot(0.0);
        assert_eq!(snap.len(), MAX_ASSIGNMENTS);
        assert_eq!(snap[0].id, format!("asg-{}", MAX_ASSIGNMENTS + 4));
        assert_eq!(snap.last().unwrap().id, "asg-5");
    }

    #[test]
    fn board_snapshot_advances() {
        let board = TaskBoard::default();
        board.push(asg(0.0, 1.0, 1.0));
        assert_eq!(board.snapshot(1.5)[0].state, TaskState::Running);
        assert_eq!(board.snapshot(3.0)[0].state, TaskState::Completed);
    }

    #[test]
    fn assignment_json_shape() {
        let v = serde_json::to_value(asg(1.5, 2.0, 9.0)).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "id": "asg-1", "agent": "agent-luna", "task": "t", "state": "queued",
                "created_at": 1.5, "started_at": null, "completed_at": null,
                "queue_for": 2.0, "run_for": 9.0
            })
        );
    }
}
