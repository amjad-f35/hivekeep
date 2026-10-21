//! The Hivekeep cluster installer.
//!
//! Modules are layered bottom-up: output and templating ([`log`],
//! [`render`]), the install layout ([`installdefaults`]), cluster access
//! ([`kube`]), manifest tooling ([`kustomize`], [`images`], [`builder`]),
//! configuration ([`config`]), the install steps ([`steps`]) and demos
//! ([`demos`]) built on them, the GCP project scaffolding ([`gcp`]), and the
//! command tree ([`cli`]).

pub mod builder;
pub mod config;
pub mod gcp;
pub mod images;
pub mod installdefaults;
pub mod kustomize;
pub mod log;
pub mod render;
