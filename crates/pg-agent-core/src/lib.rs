//! Core types, traits, and the `Agent` runtime for pg_agentd.
//!
//! This crate intentionally contains **no transport code** — `LocalServer`
//! and `PeerServer` (which translate tonic requests into trait calls) live
//! in the same crate but are gated behind feature flags so the operator CLI
//! (`pg_agentctl`) can pull in `Config` / `PeerRegistry` / `Preflight`
//! without linking the gRPC server bits.
//!
//! See [`../../SPEC.md`](../../SPEC.md) for the architecture and
//! [`../../ROADMAP.md`](../../ROADMAP.md) for the v1.x / v2 / exploratory
//! features that should not be foreclosed by current design choices.

#![forbid(unsafe_code)]
#![allow(dead_code)] // scaffolding — modules are stubs

pub mod agent;
pub mod certreload;
pub mod config;
pub mod errors;
pub mod healthz;
pub mod localdb;
pub mod maintenance;
pub mod pcp;
pub mod peers;
pub mod pgstandby;
pub mod preflight;
pub mod replay_markers;
pub mod sdnotify;
pub mod systemd;
pub mod walstore;

pub use errors::{AgentError, Result};

// Re-export the proto crate so downstream binaries don't need a separate dep.
pub use pg_agent_proto as proto;
