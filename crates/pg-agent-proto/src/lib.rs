//! Generated tonic/prost code for the two gRPC services pg_agentd exposes.
//!
//! - [`PgAgentLocal`](pgagentpb::pg_agent_local_server::PgAgentLocal) — Unix
//!   socket, no auth (filesystem permissions are the access control).
//! - [`PgAgentPeer`](pgagentpb::pg_agent_peer_server::PgAgentPeer) — mTLS TCP,
//!   client cert SAN must be in the pool allowlist.
//! - [`PgAgentRaft`](pgagentpb::pg_agent_raft_server::PgAgentRaft) — the
//!   consensus plane, on the same mTLS listener as `PgAgentPeer` but dialed
//!   over its own client channel.
//!
//! See [`../../SPEC.md`](../../SPEC.md) §3 for the full surface, including
//! the field-level validation rules each handler must apply.

#![allow(clippy::all)]
#![allow(missing_docs)]

pub mod pgagentpb {
    tonic::include_proto!("pgagentpb");
}

pub use pgagentpb::*;
