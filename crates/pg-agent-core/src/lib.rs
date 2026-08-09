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
pub mod cluster_view;
pub mod config;
pub mod consensus;
pub mod errors;
pub mod ha;
pub mod healthz;
pub mod inflight_ops;
pub mod localdb;
pub mod localserver;
pub mod maintenance;
pub mod pcp;
pub mod peers;
pub mod peerserver;
pub mod pgpool_supervisor;
pub mod pgstandby;
pub mod preconditions;
pub mod preflight;
pub mod replay_markers;
pub mod retry;
pub mod sdnotify;
pub mod symlinks;
pub mod systemd;
pub mod walstore;

pub use errors::{AgentError, Result};

// Re-export the proto crate so downstream binaries don't need a separate dep.
pub use pg_agent_proto as proto;
