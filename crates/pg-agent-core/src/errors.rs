//! Typed errors for pg-agent-core.
//!
//! Mirrors the named errors used in the Go agent so log-grep recipes ported
//! from the Go version still find them. New error variants belong here, not
//! buried inside individual modules.

use thiserror::Error;

pub type Result<T, E = AgentError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum AgentError {
    // ----- Config / topology --------------------------------------------
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
    #[error("config: local node not found in pool")]
    NoLocalNode,

    // ----- TLS ----------------------------------------------------------
    #[error("config: tls requires ca_cert, cert, and key")]
    TlsMissingField,
    #[error(
        "serve: remote peers present but TLS is not configured; \
         set allow_insecure_remote_peer in config.toml AND run pg_agentd with \
         --dev to allow plain-text (dev/test only)"
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
