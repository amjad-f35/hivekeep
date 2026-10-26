//! The ResumePausedUser class: drives hivekeep-api's cold resume path.
//!
//! Every ResumeActor targets a suspended Actor, so hivekeep-api takes the
//! Actor lease, binds a worker and waits on a keeper restore; each resume
//! is followed by an asynchronous suspend that makes the Actor cold again.
//! Run against the fake data plane, it measures what a cold resume costs
//! the control plane and its database without the gate or a real restore
//! in the way. The suspends run at the resume rate, so a capacity it finds
//! is one of resume-and-suspend cycles. The Actors themselves get no
//! requests, since none run there.

pub mod ctx;
pub mod knobs;
pub mod pacer;
pub mod runtime;

use std::sync::Arc;

use crate::loadgen::userclass::{Config, Entry, UserTask};

/// The locust user class name.
pub const USER_CLASS: &str = "ResumePausedUser";

fn init(cfg: Arc<Config>) -> Arc<dyn UserTask> {
    Arc::new(runtime::Runtime::new(cfg, USER_CLASS, true))
}

/// The class's registry entry.
pub fn entries() -> Vec<Entry> {
    vec![Entry {
        name: "resumepaused",
        locust_file: "resumepaused.py",
        user_class: USER_CLASS,
        config: Some(|| Box::new(knobs::codec())),
        init,
    }]
}
