//! `config.toml` schema, defaults, and projections (`ServeSettings`,
//! `NodePool`, `PostgresRuntime`). See SPEC §8.
//!
//! TODO(v1): implement load/validate/apply_env_overrides; node-id resolution
//! (config field → file → `<state_dir>/node_id` → hostname); peer-listen-addr
//! resolution; `has_remote_peers` (DNS lookup of every pool hostname);
//! `[postgres.replication_tls]` validation.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

pub const DEFAULT_CONFIG_FILE: &str = "/etc/pg_agent/config.toml";
pub const DEFAULT_UNIX_SOCKET: &str = "/run/pg_agentd/pg_agentd.sock";
pub const DEFAULT_AGENT_PORT: u16 = 9701;
pub const DEFAULT_HEALTHZ_PORT: u16 = 9702;
pub const DEFAULT_HEALTHZ_LISTEN: &str = "0.0.0.0";
pub const DEFAULT_PCP_PORT: u16 = 9898;
pub const DEFAULT_PG_PORT: u16 = 5432;
pub const DEFAULT_PCP_USER: &str = "pgpool";
pub const DEFAULT_REPL_USER: &str = "repl";
pub const DEFAULT_PG_VERSION: &str = "17";
pub const DEFAULT_HOME_DIR: &str = "/var/lib/postgresql";
pub const DEFAULT_PG_HOME: &str = "/usr/lib/postgresql/17";
pub const DEFAULT_PG_DATA_DIR: &str = "/var/lib/postgresql/17/main";
pub const DEFAULT_ARCHIVE_DIR: &str = "/var/lib/postgresql/archive";
pub const DEFAULT_PG_SOCKET_DIR: &str = "/var/run/postgresql";
pub const DEFAULT_PG_SERVICE: &str = "postgresql@17-main.service";
pub const DEFAULT_PGPOOL_SERVICE: &str = "pgpool2.service";

// ---------------------------------------------------------------------------
// Env var names
// ---------------------------------------------------------------------------

pub const ENV_SOCKET: &str = "PG_AGENTD_SOCKET";
pub const ENV_AGENT_PORT: &str = "PG_AGENTD_PORT";
pub const ENV_LISTEN: &str = "PG_AGENTD_LISTEN";
pub const ENV_PCP_USER: &str = "PG_AGENTD_PCP_USER";
pub const ENV_PCP_PORT: &str = "PG_AGENTD_PCP_PORT";
pub const ENV_TLS_CA_CERT: &str = "PG_AGENTD_TLS_CA_CERT";
pub const ENV_TLS_CERT: &str = "PG_AGENTD_TLS_CERT";
pub const ENV_TLS_KEY: &str = "PG_AGENTD_TLS_KEY";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub agent_port: Option<u16>,
    #[serde(default)]
    pub unix_socket: Option<String>,
    #[serde(default)]
    pub listen: Option<String>,

    #[serde(default)]
    pub node_id: Option<i32>,
    #[serde(default)]
    pub node_id_file: Option<PathBuf>,
    #[serde(default)]
    /// Root for agent-owned persistent state (maintenance queue, optional
    /// node-id file). Matches the spirit of systemd's `StateDirectory=`.
    pub state_dir: Option<PathBuf>,

    #[serde(default)]
    pub allow_insecure_remote_peer: bool,

    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub pool: Vec<NodeConfig>,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub pcp: PcpConfig,
    #[serde(default)]
    pub healthz: HealthzConfig,

    /// Set by the `--dev` CLI flag — never read from config.toml.
    #[serde(skip)]
    pub dev_mode: bool,

    /// Resolved local node id; -1 means unresolved.
    #[serde(skip)]
    pub local_node_id: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
}

impl TlsConfig {
    /// All three paths present.
    pub fn is_configured(&self) -> bool {
        self.ca_cert.is_some() && self.cert.is_some() && self.key.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: i32,
    pub hostname: String,
}

impl NodeConfig {
    /// Replication slot name for this node: `node{id}`.
    pub fn slot_name(&self) -> String {
        format!("node{}", self.id)
    }

    /// `host:port` for peer gRPC dialing.
    pub fn peer_addr(&self, agent_port: u16) -> String {
        format!("{}:{agent_port}", self.hostname)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresConfig {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub pghome: Option<PathBuf>,
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub socket_dir: Option<PathBuf>,
    #[serde(default)]
    pub repl_user: Option<String>,
    #[serde(default)]
    pub home: Option<PathBuf>,
    #[serde(default)]
    pub archive_dir: Option<PathBuf>,
    #[serde(default)]
    pub service: Option<String>,

    #[serde(default)]
    pub replication_tls: PgReplicationTlsConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PgReplicationTlsConfig {
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
    #[serde(default)]
    pub sslmode: Option<String>,
}

impl PgReplicationTlsConfig {
    /// All three paths present (sslmode defaults to verify-full when so).
    pub fn is_configured(&self) -> bool {
        self.ca_cert.is_some() && self.cert.is_some() && self.key.is_some()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PcpConfig {
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub pgpool_service: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthzConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

// ---------------------------------------------------------------------------
// Runtime projections
// ---------------------------------------------------------------------------

/// Serving-time settings derived from `Config`, retained at runtime instead
/// of the full `Config` object.
#[derive(Debug, Clone)]
pub struct ServeSettings {
    pub unix_socket: String,
    pub agent_port: u16,
    pub peer_listen_addr: String,
    pub tls: TlsConfig,
    pub tls_configured: bool,
    pub has_remote_peers: bool,
    pub insecure_remote_peer_allowed: bool,
    pub dev_mode: bool,
    pub healthz: HealthzSettings,
}

#[derive(Debug, Clone)]
pub struct HealthzSettings {
    pub enabled: bool,
    pub listen_addr: String,
}

/// PostgreSQL-specific runtime values consumed by Agent handlers.
#[derive(Debug, Clone)]
pub struct PostgresRuntime {
    pub port: u16,
    pub data_dir: PathBuf,
    pub repl_user: String,
}

/// The set of nodes that make up the cluster, plus which one is us.
///
/// Pure membership — no operational config lives here. The local node's
/// PostgreSQL runtime is a sibling [`PostgresRuntime`] on `Options`; the
/// drift-warning that needs both (`warn_node_ref_mismatch`, TODO) is a
/// free function so consumers that only do member lookup don't
/// transitively depend on PG runtime.
#[derive(Debug, Clone)]
pub struct NodePool {
    /// Pool members in declaration order. (`NodePool` is a runtime
    /// projection of `Config`, not a TOML-deserialised type — the
    /// `[[pool]]` directive lives on `Config::pool` directly.)
    pub members: Vec<NodeConfig>,
    /// Pool id of the local node, or `-1` if unresolved at config-load time.
    pub local_node_id: i32,
}

// TODO(v1): impl Config::{load, validate, apply_env_overrides,
//   resolve_local_node_id, peer_listen_addr, has_remote_peers,
//   to_serve_settings, to_node_pool, to_postgres_runtime}.
// TODO(v1): impl NodePool::{node_by_id, node_by_hostname, local_node,
//   is_local, resolve_node, resolve_local_node}.
// TODO(v1): free fn warn_node_ref_mismatch(ref, node, local_pg) — only
//   called by the agent when resolving a NodeRef known to be local.
