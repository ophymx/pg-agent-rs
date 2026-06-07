//! Typed errors for pg-agent-core.
//!
//! Mirrors the named errors used in the Go agent so log-grep recipes ported
//! from the Go version still find them. New error variants belong here, not
//! buried inside individual modules.

use std::path::PathBuf;
use thiserror::Error;

pub type Result<T, E = AgentError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum AgentError {
    // ----- Config / topology --------------------------------------------
    #[error("config: read {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("config: parse {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("config: pool must have at least one node")]
    NoPool,
    #[error("config: duplicate node id in pool: {0}")]
    DuplicateId(i32),
    #[error("config: duplicate hostname in pool: {0}")]
    DuplicateHost(String),
    #[error("config: node id must be non-negative: {0}")]
    NegativeId(i32),
    #[error("config: node not found: {0}")]
    NodeNotFound(String),
    #[error("config: local node not found in pool (no node_id configured and no hostname match)")]
    NoLocalNode,
    // NB: the field is `origin` (not `source`) because thiserror auto-wires
    // any field literally named `source` into the error chain and demands it
    // implement `std::error::Error` — which `String` does not.
    #[error("config: {origin} specifies id={id} which is not in pool")]
    LocalNodeMissingFromPool { origin: String, id: i32 },
    #[error("config: node_id file {path}: {message}")]
    NodeIdFile { path: PathBuf, message: String },
    #[error("unknown node: id={id} hostname={hostname:?}")]
    NodeRefUnresolvable { id: i32, hostname: String },
    #[error("node id={id} ({hostname}) is not the local node")]
    NodeRefNotLocal { id: i32, hostname: String },

    // ----- TLS ----------------------------------------------------------
    #[error("config: tls requires ca_cert, cert, and key")]
    TlsMissingField,
    #[error(
        "serve: remote peers present but TLS is not configured; \
         pass --dev to pg_agentd to allow plain-text peer connections \
         (dev/test only — there is intentionally no config-file knob for this)"
    )]
    InsecureRemotePeer,

    // ----- Replication TLS ----------------------------------------------
    #[error(
        "config: postgres.replication_tls requires ca_cert, cert, and key (all three or none)"
    )]
    ReplicationTlsPartial,
    #[error("config: postgres.replication_tls.sslmode must be one of disable, allow, prefer, require, verify-ca, verify-full")]
    ReplicationTlsSslMode,
    #[error("config: postgres.replication_tls cert paths must be absolute and contain only [A-Za-z0-9._/-]")]
    ReplicationTlsBadPath,

    // ----- WAL store ----------------------------------------------------
    #[error("dest_path is outside pgdata root")]
    DestOutsidePgData,

    // ----- Pass-through -------------------------------------------------
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
