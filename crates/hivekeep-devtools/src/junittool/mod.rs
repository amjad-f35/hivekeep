//! CI helpers that tell "every test passed" apart from "no test ran".
//!
//! An exit code alone cannot make that distinction: a package filter that stops
//! matching reports success having run nothing. Runs therefore register the JUnit
//! file they will write before they start, and `verify` requires every registered
//! file to exist and report tests. A run that died part-way is then
//! distinguishable from one that never existed.

pub mod junit;
pub mod manifest;
pub mod report;
