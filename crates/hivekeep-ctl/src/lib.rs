//! `hivectl`, the operator and developer client for Hivekeep.
//!
//! Commands follow `hivectl <verb> <resource>`: `get`, `create`, `update`,
//! `delete`, `suspend`, `resume`, `revert`, `logs`, `top`, `drain`,
//! `mark-available`, `mark-unavailable`, `proxy` and `admin`. Most talk to
//! `hivekeep-api` over gRPC; `logs`, `top` and `admin` also reach the cluster
//! through kubectl.
//!
//! Modules build up from formatting ([`table`], [`yaml`], [`protojson`],
//! [`output`], [`print`]) and parsing ([`refs`], [`label_selector`],
//! [`manifest`]) through the connection ([`connection`], [`client`]) to the
//! command line ([`cli`]), the commands ([`commands`]) and dispatch
//! ([`app`]).

pub mod age;
pub mod ca_pool;
pub mod cli;
pub mod client;
pub mod commands;
pub mod connection;
pub mod context;
pub mod error;
pub mod gate;
pub mod install;
pub mod io;
pub mod jwt_pool;
pub mod kubectl;
pub mod label_selector;
pub mod log_filter;
pub mod manifest;
pub mod output;
pub mod print;
pub mod protojson;
pub mod quantity;
pub mod refs;
pub mod secret;
pub mod table;
pub mod verbosity;
pub mod yaml;
