//! The ResumePausedUser slice of the runtime config.

use serde::{Deserialize, Serialize};

use crate::loadgen::dynconfig::Typed;

/// The master fills `target_rps` from its `--target-rps` flag or a load
/// ladder step's rate.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Knobs {
    /// Cold resumes per second this worker paces to; zero is unpaced.
    pub target_rps: f64,
}

/// The class's codec: unpaced by default, a negative rate refused.
pub fn codec() -> Typed<Knobs> {
    Typed::new(Knobs::default()).with_validate(|k| {
        if k.target_rps < 0.0 {
            return Err(format!("target_rps cannot be negative: {}", k.target_rps));
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadgen::dynconfig::Holder;

    /// The codec takes a target rate, reads an absent one as unpaced, and
    /// refuses a negative one in favour of the config before it.
    #[test]
    fn knobs_codec() {
        let h = Holder::new(Box::new(codec()));
        assert_eq!(
            h.get::<Knobs>(),
            Knobs::default(),
            "fresh holder is unpaced"
        );
        h.apply(br#"{"target_rps": 2500.5, "someone_elses_key": 1}"#)
            .unwrap();
        let want = Knobs { target_rps: 2500.5 };
        assert_eq!(h.get::<Knobs>(), want);
        assert!(
            h.apply(br#"{"target_rps": -1}"#).is_err(),
            "a negative rate must be refused"
        );
        assert_eq!(
            h.get::<Knobs>(),
            want,
            "a refused payload changed the config"
        );
        h.apply(br#"{"target_rps": null}"#).unwrap();
        assert_eq!(h.get::<Knobs>(), Knobs::default(), "a null rate is unpaced");
    }
}
