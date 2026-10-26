//! Splitting a trajectory's steps into cycles.

/// One cycle's slice of the steps: `[start, end)`, zero-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk {
    /// First step, zero-based.
    pub start: usize,
    /// One past the last step.
    pub end: usize,
}

impl Chunk {
    /// A chunk over `[start, end)`.
    pub const fn new(start: usize, end: usize) -> Self {
        Chunk { start, end }
    }
}

/// Splits `total_steps` into `num_cycles` contiguous chunks whose lengths
/// differ by at most one, the remainder going to the earliest cycles. More
/// cycles than steps run as one cycle per step.
pub fn generate_dynamic_chunks(total_steps: usize, num_cycles: usize) -> Vec<Chunk> {
    if num_cycles <= 1 {
        return vec![Chunk::new(0, total_steps)];
    }
    let total = total_steps.max(1);
    let cycles = num_cycles.min(total);
    let base = total / cycles;
    let remainder = total % cycles;
    let mut out = Vec::with_capacity(cycles);
    let mut start = 0;
    for i in 0..cycles {
        let end = start + base + usize::from(i < remainder);
        out.push(Chunk::new(start, end));
        start = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_dynamic_chunks_table() {
        for (name, steps, cycles, want) in [
            (
                "default 21 steps into 4 cycles",
                21,
                4,
                vec![(0, 6), (6, 11), (11, 16), (16, 21)],
            ),
            ("single cycle", 10, 1, vec![(0, 10)]),
            (
                "num_cycles greater than total_steps",
                2,
                5,
                vec![(0, 1), (1, 2)],
            ),
            ("zero steps", 0, 1, vec![(0, 0)]),
        ] {
            let want: Vec<Chunk> = want.into_iter().map(|(s, e)| Chunk::new(s, e)).collect();
            assert_eq!(generate_dynamic_chunks(steps, cycles), want, "{name}");
        }
    }
}
