//! Hivekeep's network mediation layer, the gate.

// Test case tables are tuples of many fields.
#![cfg_attr(test, allow(clippy::type_complexity))]

pub mod attr;
pub mod credprovider;
pub mod headers;
pub mod install;

pub mod egress_policy;
pub mod k8s_channel;
pub mod k8s_resolver;
pub mod netns;
pub mod portforward;
pub mod tunnel;
pub mod singleflight;

#[cfg(test)]
pub(crate) mod test_pki;
