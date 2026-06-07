//! `config.toml` schema, defaults, and projections (`ServeSettings`,
//! `NodePool`, `PostgresRuntime`). See SPEC §8.
//!
//! Load flow:
//!
//! 1. [`Config::load`] reads the TOML file.
//! 2. [`Config::apply_defaults`] fills omitted fields with the `DEFAULT_*`
//!    constants. Always called by `load`; safe to call again. Infallible.
//! 3. [`Config::resolve_local_node_id`] picks the local node id via the
//!    four-way priority documented on the method.
//! 4. [`Config::validate`] enforces structural rules (pool non-empty,
//!    unique ids/hostnames, replication_tls all-or-nothing, local node
//!    resolved).
//!
//! Optional after `load`: [`Config::apply_env_overrides`] (called by the
//! daemon between file-load and CLI-flag application — see SPEC §8.7 for
//! the precedence order).

use crate::errors::AgentError;
use pg_agent_proto::pgagentpb as pb;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing::warn;

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
// Replication-TLS validation
// ---------------------------------------------------------------------------

/// `sslmode` values libpq recognises.
/// <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>
const ALLOWED_SSLMODES: &[&str] = &[
    "disable",
    "allow",
    "prefer",
    "require",
    "verify-ca",
    "verify-full",
];

/// Restrict cert paths to a conservative ASCII subset. The paths are written
/// verbatim into a single-quoted libpq conninfo, so they must not contain
/// whitespace, quotes, or shell metacharacters that could escape the quoting.
fn allowed_ssl_path() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^/[A-Za-z0-9._/-]+$").unwrap())
}

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
    /// Root for agent-owned persistent state (maintenance queue, optional
    /// node-id file). Matches the spirit of systemd's `StateDirectory=`.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,

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

impl PostgresConfig {
    fn apply_defaults(&mut self) {
        self.port.get_or_insert(DEFAULT_PG_PORT);
        self.pghome.get_or_insert_with(|| DEFAULT_PG_HOME.into());
        self.data_dir
            .get_or_insert_with(|| DEFAULT_PG_DATA_DIR.into());
        self.socket_dir
            .get_or_insert_with(|| DEFAULT_PG_SOCKET_DIR.into());
        self.repl_user
            .get_or_insert_with(|| DEFAULT_REPL_USER.to_string());
        self.home.get_or_insert_with(|| DEFAULT_HOME_DIR.into());
        self.archive_dir
            .get_or_insert_with(|| DEFAULT_ARCHIVE_DIR.into());
        self.service
            .get_or_insert_with(|| DEFAULT_PG_SERVICE.to_string());
    }
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

    /// Reject half-configured blocks (1 or 2 of 3 paths set), any sslmode
    /// libpq wouldn't recognise, and cert paths containing characters that
    /// could escape the single-quoted libpq conninfo they're rendered into.
    pub fn validate(&self) -> Result<(), AgentError> {
        let set = [&self.ca_cert, &self.cert, &self.key]
            .iter()
            .filter(|o| o.is_some())
            .count();
        if set != 0 && set != 3 {
            return Err(AgentError::ReplicationTlsPartial);
        }
        if let Some(mode) = self.sslmode.as_deref() {
            if !ALLOWED_SSLMODES.contains(&mode) {
                return Err(AgentError::ReplicationTlsSslMode);
            }
        }
        if set == 3 {
            let re = allowed_ssl_path();
            for p in [&self.ca_cert, &self.cert, &self.key].into_iter().flatten() {
                let s = p.to_string_lossy();
                if !re.is_match(&s) {
                    return Err(AgentError::ReplicationTlsBadPath);
                }
            }
        }
        Ok(())
    }

    /// `sslmode` value to write into `primary_conninfo`. `None` means "omit
    /// sslmode entirely" — used when the block is not configured.
    pub fn effective_sslmode(&self) -> Option<&str> {
        if !self.is_configured() {
            return None;
        }
        Some(self.sslmode.as_deref().unwrap_or("verify-full"))
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

impl PcpConfig {
    fn apply_defaults(&mut self) {
        self.user
            .get_or_insert_with(|| DEFAULT_PCP_USER.to_string());
        self.port.get_or_insert(DEFAULT_PCP_PORT);
        self.pgpool_service
            .get_or_insert_with(|| DEFAULT_PGPOOL_SERVICE.to_string());
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthzConfig {
    /// Pointer-like Option so we can distinguish "unset" (defaults to true)
    /// from "explicitly false".
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

impl HealthzConfig {
    /// Effective `enabled` with the default (true) applied.
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    fn apply_defaults(&mut self) {
        self.listen
            .get_or_insert_with(|| DEFAULT_HEALTHZ_LISTEN.to_string());
        self.port.get_or_insert(DEFAULT_HEALTHZ_PORT);
    }
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
    pub dev_mode: bool,
    pub healthz: HealthzSettings,
}

impl ServeSettings {
    /// True if serving must be rejected because remote peers are present
    /// but TLS isn't configured and `--dev` wasn't passed. `--dev` is the
    /// only escape hatch — deliberately CLI-only so the choice can't be
    /// committed to a config file by accident.
    pub fn reject_insecure_remote_peer(&self) -> bool {
        self.has_remote_peers && !self.tls_configured && !self.dev_mode
    }
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
/// drift-warning that needs both ([`warn_node_ref_mismatch`]) is a free
/// function so consumers that only do member lookup don't transitively
/// depend on PG runtime.
#[derive(Debug, Clone)]
pub struct NodePool {
    /// Pool members in declaration order. (`NodePool` is a runtime
    /// projection of `Config`, not a TOML-deserialised type — the
    /// `[[pool]]` directive lives on `Config::pool` directly.)
    pub members: Vec<NodeConfig>,
    /// Pool id of the local node, or `-1` if unresolved at config-load time.
    pub local_node_id: i32,
}

impl NodePool {
    pub fn node_by_id(&self, id: i32) -> Result<&NodeConfig, AgentError> {
        self.members
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| AgentError::NodeNotFound(format!("id={id}")))
    }

    pub fn node_by_hostname(&self, hostname: &str) -> Result<&NodeConfig, AgentError> {
        self.members
            .iter()
            .find(|n| n.hostname == hostname)
            .ok_or_else(|| AgentError::NodeNotFound(format!("hostname={hostname:?}")))
    }

    pub fn local_node(&self) -> Result<&NodeConfig, AgentError> {
        if self.local_node_id < 0 {
            return Err(AgentError::NoLocalNode);
        }
        self.node_by_id(self.local_node_id)
    }

    pub fn is_local(&self, node: &NodeConfig) -> bool {
        self.local_node_id >= 0 && node.id == self.local_node_id
    }

    /// Resolve a wire-supplied [`pb::NodeRef`] into its [`NodeConfig`].
    /// Hostname-authoritative — pgpool's view of which host is which wins;
    /// id is the fallback when hostname is empty. Always returns the
    /// agent's own config-sourced `NodeConfig`; the wire-supplied fields
    /// on the `NodeRef` are informational. The caller can run
    /// [`warn_node_ref_mismatch`] separately to log argv/config drift.
    pub fn resolve_node(&self, ref_: &pb::NodeRef) -> Result<&NodeConfig, AgentError> {
        if !ref_.hostname.is_empty() {
            return self.node_by_hostname(&ref_.hostname).map_err(|_| {
                AgentError::NodeRefUnresolvable {
                    id: ref_.id,
                    hostname: ref_.hostname.clone(),
                }
            });
        }
        if ref_.id >= 0 {
            return self
                .node_by_id(ref_.id)
                .map_err(|_| AgentError::NodeRefUnresolvable {
                    id: ref_.id,
                    hostname: ref_.hostname.clone(),
                });
        }
        Err(AgentError::NodeRefUnresolvable {
            id: ref_.id,
            hostname: ref_.hostname.clone(),
        })
    }

    /// Like [`resolve_node`](Self::resolve_node), but verifies the
    /// resolved node is the local one. Used when an operation must
    /// execute on this node and the caller wants an early refusal
    /// otherwise.
    pub fn resolve_local_node(&self, ref_: &pb::NodeRef) -> Result<&NodeConfig, AgentError> {
        let node = self.resolve_node(ref_)?;
        if !self.is_local(node) {
            return Err(AgentError::NodeRefNotLocal {
                id: node.id,
                hostname: node.hostname.clone(),
            });
        }
        Ok(node)
    }
}

/// Log a warning for each identifying field in `ref_` that disagrees with
/// the agent's own config for the resolved node. `pg_port` / `pg_data`
/// drift is only checked when `local_pg` is supplied — the agent has no
/// way to validate a remote node's runtime values.
///
/// The agent's config always wins; this is purely a "your pgpool.conf and
/// pg_agent config don't agree" signal for the operator.
pub fn warn_node_ref_mismatch(
    ref_: &pb::NodeRef,
    node: &NodeConfig,
    local_pg: Option<&PostgresRuntime>,
) {
    if ref_.id >= 0 && ref_.id != node.id {
        warn!(
            ref_id = ref_.id,
            config_id = node.id,
            node = %node.hostname,
            "NodeRef id does not match config; using config value"
        );
    }
    if !ref_.hostname.is_empty() && ref_.hostname != node.hostname {
        warn!(
            ref_hostname = %ref_.hostname,
            config_hostname = %node.hostname,
            node_id = node.id,
            "NodeRef hostname does not match config; using config value"
        );
    }
    if let Some(pg) = local_pg {
        if ref_.pg_port != 0 && ref_.pg_port != pg.port as i32 {
            warn!(
                ref_port = ref_.pg_port,
                config_port = pg.port,
                node = %node.hostname,
                "NodeRef pg_port does not match config; using config value"
            );
        }
        if !ref_.pg_data.is_empty() && Path::new(&ref_.pg_data) != pg.data_dir {
            warn!(
                ref_pg_data = %ref_.pg_data,
                config_pg_data = %pg.data_dir.display(),
                node = %node.hostname,
                "NodeRef pg_data does not match local config; using config value"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Config impl
// ---------------------------------------------------------------------------

impl Config {
    /// Read and validate a config from `path`. If `path` does not exist
    /// the caller (the daemon) decides whether to fall back to
    /// [`Config::with_defaults`].
    pub fn load(path: impl AsRef<Path>) -> Result<Self, AgentError> {
        let path = path.as_ref();
        let data = fs::read_to_string(path).map_err(|source| AgentError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        Self::load_from_str_with_source(&data, path.to_path_buf())
    }

    /// Parse a config from a TOML string. Test helper.
    pub fn load_from_str(toml_str: &str) -> Result<Self, AgentError> {
        Self::load_from_str_with_source(toml_str, PathBuf::from("<string>"))
    }

    fn load_from_str_with_source(toml_str: &str, path: PathBuf) -> Result<Self, AgentError> {
        let mut cfg: Config =
            toml::from_str(toml_str).map_err(|source| AgentError::ConfigParse {
                path: path.clone(),
                source,
            })?;
        cfg.apply_defaults();
        cfg.resolve_local_node_id()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// A Config with only the const defaults applied. No pool, no TLS,
    /// `local_node_id = -1`. Used by the daemon's `--dev` fallback when
    /// no config file is given and the default path doesn't exist.
    pub fn with_defaults() -> Self {
        let mut cfg = Config::default();
        cfg.apply_defaults();
        cfg
    }

    /// Fill omitted fields with their const defaults. Always idempotent.
    /// Does **not** resolve the local node id (call
    /// [`Config::resolve_local_node_id`] for that) or validate (call
    /// [`Config::validate`]).
    pub fn apply_defaults(&mut self) {
        self.agent_port.get_or_insert(DEFAULT_AGENT_PORT);
        self.unix_socket
            .get_or_insert_with(|| DEFAULT_UNIX_SOCKET.to_string());

        self.postgres.apply_defaults();

        // state_dir defaults to <postgres.home>/pg_agent — must happen
        // after postgres defaults so .home is populated.
        if self.state_dir.is_none() {
            let home = self
                .postgres
                .home
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_HOME_DIR));
            self.state_dir = Some(home.join("pg_agent"));
        }

        self.pcp.apply_defaults();
        self.healthz.apply_defaults();
    }

    /// Replace any non-empty config field with the corresponding env-var
    /// value. Allows full configuration without a TOML file (useful for
    /// dev and integration testing). Does **not** re-validate — the
    /// caller is responsible for catching env-introduced invalid values.
    pub fn apply_env_overrides(&mut self) {
        self.apply_env_overrides_from(|k| std::env::var(k).ok())
    }

    /// Closure-based variant of [`apply_env_overrides`](Self::apply_env_overrides)
    /// — `getenv` returns `Some(value)` for present env vars and `None`
    /// otherwise. Used by unit tests to avoid mutating process env state.
    pub fn apply_env_overrides_from<F>(&mut self, getenv: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(v) = getenv(ENV_SOCKET).filter(|s| !s.is_empty()) {
            self.unix_socket = Some(v);
        }
        if let Some(p) = getenv(ENV_AGENT_PORT)
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<u16>().ok())
        {
            self.agent_port = Some(p);
        }
        if let Some(v) = getenv(ENV_LISTEN).filter(|s| !s.is_empty()) {
            self.listen = Some(v);
        }
        if let Some(v) = getenv(ENV_TLS_CA_CERT).filter(|s| !s.is_empty()) {
            self.tls.ca_cert = Some(v.into());
        }
        if let Some(v) = getenv(ENV_TLS_CERT).filter(|s| !s.is_empty()) {
            self.tls.cert = Some(v.into());
        }
        if let Some(v) = getenv(ENV_TLS_KEY).filter(|s| !s.is_empty()) {
            self.tls.key = Some(v.into());
        }
        if let Some(v) = getenv(ENV_PCP_USER).filter(|s| !s.is_empty()) {
            self.pcp.user = Some(v);
        }
        if let Some(p) = getenv(ENV_PCP_PORT)
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<u16>().ok())
        {
            self.pcp.port = Some(p);
        }
    }

    /// Determine which pool entry this process represents. Four-way
    /// priority (first hit wins):
    ///
    /// 1. `node_id` field in `config.toml`
    /// 2. `node_id_file` field — file containing the integer
    /// 3. `<state_dir>/node_id` — mirrors pgpool's `pgpool_node_id` convention
    /// 4. Hostname fallback — `gethostname()` matched against `[[pool]].hostname`
    ///
    /// Sources 1 and 2 error if they point at an id not in the pool —
    /// that's unambiguously a configuration mistake. Sources 3 and 4 are
    /// best-effort; if none match, `local_node_id` stays at `-1` and
    /// [`Config::validate`] will surface the failure.
    ///
    /// Must be called **after** [`apply_defaults`](Self::apply_defaults).
    pub fn resolve_local_node_id(&mut self) -> Result<(), AgentError> {
        self.local_node_id = -1;

        // 1. Explicit node_id in config.
        if let Some(id) = self.node_id {
            return self.set_local_node_by_id(id, "config node_id");
        }

        // 2. node_id_file in config.
        if let Some(p) = self.node_id_file.clone() {
            let id = read_node_id_file(&p)?;
            return self.set_local_node_by_id(id, &format!("node_id_file {}", p.display()));
        }

        // 3. <state_dir>/node_id — created by ansible on every node so
        //    pgpool and pg_agent agree on the local id without a custom
        //    config.toml per host.
        if let Some(state_dir) = self.state_dir.as_ref() {
            let default_id_file = state_dir.join("node_id");
            match fs::read_to_string(&default_id_file) {
                Ok(s) => {
                    let id = s
                        .trim()
                        .parse::<i32>()
                        .map_err(|e| AgentError::NodeIdFile {
                            path: default_id_file.clone(),
                            message: format!("parse: {e}"),
                        })?;
                    return self.set_local_node_by_id(
                        id,
                        &format!("node_id file {}", default_id_file.display()),
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(AgentError::NodeIdFile {
                        path: default_id_file,
                        message: e.to_string(),
                    });
                }
            }
        }

        // 4. Hostname fallback. Errors here are non-fatal — validate()
        //    will report NoLocalNode if nothing matched.
        if let Ok(host_os) = nix::unistd::gethostname() {
            let host = host_os.to_string_lossy();
            for n in &self.pool {
                if n.hostname == *host {
                    self.local_node_id = n.id;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn set_local_node_by_id(&mut self, id: i32, origin: &str) -> Result<(), AgentError> {
        if self.pool.iter().any(|n| n.id == id) {
            self.local_node_id = id;
            Ok(())
        } else {
            Err(AgentError::LocalNodeMissingFromPool {
                origin: origin.to_string(),
                id,
            })
        }
    }

    /// Structural validation. Runs after defaults + local-node-id
    /// resolution. Runtime checks (TLS requirement, insecure-peer
    /// dual-lock) live on `ServeSettings::reject_insecure_remote_peer`
    /// and fire from the daemon's `Agent::serve` path.
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.pool.is_empty() {
            return Err(AgentError::NoPool);
        }

        let mut ids: HashSet<i32> = HashSet::with_capacity(self.pool.len());
        let mut hosts: HashSet<&str> = HashSet::with_capacity(self.pool.len());
        for n in &self.pool {
            if n.id < 0 {
                return Err(AgentError::NegativeId(n.id));
            }
            if !ids.insert(n.id) {
                return Err(AgentError::DuplicateId(n.id));
            }
            if !hosts.insert(&n.hostname) {
                return Err(AgentError::DuplicateHost(n.hostname.clone()));
            }
        }

        // local node must resolve to a pool entry.
        self.local_node()?;

        self.postgres.replication_tls.validate()?;
        Ok(())
    }

    /// Find a pool entry by id.
    pub fn node_by_id(&self, id: i32) -> Result<&NodeConfig, AgentError> {
        self.pool
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| AgentError::NodeNotFound(format!("id={id}")))
    }

    /// Find a pool entry by hostname.
    pub fn node_by_hostname(&self, hostname: &str) -> Result<&NodeConfig, AgentError> {
        self.pool
            .iter()
            .find(|n| n.hostname == hostname)
            .ok_or_else(|| AgentError::NodeNotFound(format!("hostname={hostname:?}")))
    }

    /// The pool entry for this agent process.
    pub fn local_node(&self) -> Result<&NodeConfig, AgentError> {
        if self.local_node_id < 0 {
            return Err(AgentError::NoLocalNode);
        }
        self.node_by_id(self.local_node_id)
    }

    /// True if `node` is the local one.
    pub fn is_local(&self, node: &NodeConfig) -> bool {
        self.local_node_id >= 0 && node.id == self.local_node_id
    }

    /// `host:port` the peer gRPC listener binds to. Host is `listen` if
    /// set, else `0.0.0.0` when TLS is configured (production) and
    /// `127.0.0.1` when it is not (dev / loopback-only).
    pub fn peer_listen_addr(&self) -> String {
        let host: String = if let Some(l) = &self.listen {
            l.clone()
        } else if self.tls.is_configured() {
            "0.0.0.0".to_string()
        } else {
            "127.0.0.1".to_string()
        };
        format!("{host}:{}", self.agent_port.unwrap_or(DEFAULT_AGENT_PORT))
    }

    /// True if any peer (other than the local node) has a non-loopback
    /// hostname. Pure string check — no DNS lookup, no startup blocking.
    /// A hostname like `loopback.test` resolving to `127.0.0.1` would be
    /// treated as remote; the operator's escape hatch is `--dev`.
    pub fn has_remote_peers(&self) -> bool {
        self.pool
            .iter()
            .filter(|n| n.id != self.local_node_id)
            .any(|n| !is_loopback_hostname(&n.hostname))
    }

    /// Project the serving-time settings subset.
    pub fn to_serve_settings(&self) -> ServeSettings {
        let tls_configured = self.tls.is_configured();
        ServeSettings {
            unix_socket: self
                .unix_socket
                .clone()
                .unwrap_or_else(|| DEFAULT_UNIX_SOCKET.to_string()),
            agent_port: self.agent_port.unwrap_or(DEFAULT_AGENT_PORT),
            peer_listen_addr: self.peer_listen_addr(),
            tls: self.tls.clone(),
            tls_configured,
            has_remote_peers: self.has_remote_peers(),
            dev_mode: self.dev_mode,
            healthz: HealthzSettings {
                enabled: self.healthz.is_enabled(),
                listen_addr: format!(
                    "{}:{}",
                    self.healthz
                        .listen
                        .as_deref()
                        .unwrap_or(DEFAULT_HEALTHZ_LISTEN),
                    self.healthz.port.unwrap_or(DEFAULT_HEALTHZ_PORT)
                ),
            },
        }
    }

    /// Project the cluster membership subset.
    pub fn to_node_pool(&self) -> NodePool {
        NodePool {
            members: self.pool.clone(),
            local_node_id: self.local_node_id,
        }
    }

    /// Project the local PostgreSQL runtime subset.
    pub fn to_postgres_runtime(&self) -> PostgresRuntime {
        PostgresRuntime {
            port: self.postgres.port.unwrap_or(DEFAULT_PG_PORT),
            data_dir: self
                .postgres
                .data_dir
                .clone()
                .unwrap_or_else(|| DEFAULT_PG_DATA_DIR.into()),
            repl_user: self
                .postgres
                .repl_user
                .clone()
                .unwrap_or_else(|| DEFAULT_REPL_USER.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// String-level loopback check used by [`Config::has_remote_peers`].
/// Loopback iff `localhost` / `ip6-localhost` or parses as a loopback IP.
/// No DNS lookup performed — the operator's escape hatch for an unusual
/// "DNS name pointing at loopback" setup is `--dev`.
fn is_loopback_hostname(host: &str) -> bool {
    if matches!(host, "localhost" | "ip6-localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn read_node_id_file(path: &Path) -> Result<i32, AgentError> {
    let raw = fs::read_to_string(path).map_err(|e| AgentError::NodeIdFile {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    raw.trim()
        .parse::<i32>()
        .map_err(|e| AgentError::NodeIdFile {
            path: path.to_path_buf(),
            message: format!("parse: {e}"),
        })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_toml() -> &'static str {
        r#"
            node_id = 0

            [[pool]]
            id = 0
            hostname = "127.0.0.1"

            [[pool]]
            id = 1
            hostname = "127.0.0.2"
        "#
    }

    #[test]
    fn load_from_str_minimal_round_trips() {
        let cfg = Config::load_from_str(minimal_toml()).unwrap();
        assert_eq!(cfg.local_node_id, 0);
        assert_eq!(cfg.agent_port, Some(DEFAULT_AGENT_PORT));
        assert_eq!(cfg.unix_socket.as_deref(), Some(DEFAULT_UNIX_SOCKET));
        assert_eq!(cfg.postgres.port, Some(DEFAULT_PG_PORT));
        assert_eq!(cfg.pool.len(), 2);
    }

    #[test]
    fn load_from_str_no_pool_errors() {
        let err = Config::load_from_str("").unwrap_err();
        assert!(matches!(err, AgentError::NoPool), "got {err:?}");
    }

    #[test]
    fn load_from_str_duplicate_id_errors() {
        let toml = r#"
            node_id = 0
            [[pool]]
            id = 0
            hostname = "a"
            [[pool]]
            id = 0
            hostname = "b"
        "#;
        let err = Config::load_from_str(toml).unwrap_err();
        assert!(matches!(err, AgentError::DuplicateId(0)), "got {err:?}");
    }

    #[test]
    fn load_from_str_duplicate_hostname_errors() {
        let toml = r#"
            node_id = 0
            [[pool]]
            id = 0
            hostname = "x"
            [[pool]]
            id = 1
            hostname = "x"
        "#;
        let err = Config::load_from_str(toml).unwrap_err();
        assert!(
            matches!(err, AgentError::DuplicateHost(ref h) if h == "x"),
            "got {err:?}"
        );
    }

    #[test]
    fn load_from_str_negative_id_errors() {
        let toml = r#"
            node_id = -1
            [[pool]]
            id = -1
            hostname = "a"
        "#;
        let err = Config::load_from_str(toml).unwrap_err();
        assert!(matches!(err, AgentError::NegativeId(-1)), "got {err:?}");
    }

    #[test]
    fn load_from_str_node_id_not_in_pool_errors() {
        let toml = r#"
            node_id = 99
            [[pool]]
            id = 0
            hostname = "a"
        "#;
        let err = Config::load_from_str(toml).unwrap_err();
        assert!(
            matches!(err, AgentError::LocalNodeMissingFromPool { id: 99, .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn replication_tls_partial_errors() {
        let cfg = PgReplicationTlsConfig {
            ca_cert: Some("/etc/x/ca.crt".into()),
            cert: None,
            key: None,
            sslmode: None,
        };
        assert!(matches!(
            cfg.validate(),
            Err(AgentError::ReplicationTlsPartial)
        ));
    }

    #[test]
    fn replication_tls_unknown_sslmode_errors() {
        let cfg = PgReplicationTlsConfig {
            ca_cert: None,
            cert: None,
            key: None,
            sslmode: Some("totally-secure".into()),
        };
        assert!(matches!(
            cfg.validate(),
            Err(AgentError::ReplicationTlsSslMode)
        ));
    }

    #[test]
    fn replication_tls_bad_path_errors() {
        let cfg = PgReplicationTlsConfig {
            ca_cert: Some("/etc/x ca.crt".into()), // space rejected
            cert: Some("/etc/x/c.crt".into()),
            key: Some("/etc/x/k.key".into()),
            sslmode: None,
        };
        assert!(matches!(
            cfg.validate(),
            Err(AgentError::ReplicationTlsBadPath)
        ));
    }

    #[test]
    fn replication_tls_unconfigured_validates() {
        let cfg = PgReplicationTlsConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.effective_sslmode(), None);
    }

    #[test]
    fn replication_tls_configured_defaults_to_verify_full() {
        let cfg = PgReplicationTlsConfig {
            ca_cert: Some("/etc/x/ca.crt".into()),
            cert: Some("/etc/x/c.crt".into()),
            key: Some("/etc/x/k.key".into()),
            sslmode: None,
        };
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.effective_sslmode(), Some("verify-full"));
    }

    #[test]
    fn apply_defaults_derives_state_dir_from_home() {
        let mut cfg = Config::default();
        cfg.apply_defaults();
        assert_eq!(
            cfg.state_dir.as_deref(),
            Some(Path::new("/var/lib/postgresql/pg_agent"))
        );
    }

    #[test]
    fn apply_defaults_respects_explicit_home() {
        let mut cfg = Config {
            postgres: PostgresConfig {
                home: Some("/srv/pg".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        cfg.apply_defaults();
        assert_eq!(
            cfg.state_dir.as_deref(),
            Some(Path::new("/srv/pg/pg_agent"))
        );
    }

    #[test]
    fn peer_listen_addr_tls_yields_all_zeros() {
        let cfg = Config {
            agent_port: Some(9701),
            tls: TlsConfig {
                ca_cert: Some("/x/ca".into()),
                cert: Some("/x/c".into()),
                key: Some("/x/k".into()),
            },
            ..Default::default()
        };
        assert_eq!(cfg.peer_listen_addr(), "0.0.0.0:9701");
    }

    #[test]
    fn peer_listen_addr_no_tls_yields_loopback() {
        let cfg = Config {
            agent_port: Some(9701),
            ..Default::default()
        };
        assert_eq!(cfg.peer_listen_addr(), "127.0.0.1:9701");
    }

    #[test]
    fn peer_listen_addr_explicit_listen_wins() {
        let cfg = Config {
            agent_port: Some(9701),
            listen: Some("10.0.0.5".into()),
            ..Default::default()
        };
        assert_eq!(cfg.peer_listen_addr(), "10.0.0.5:9701");
    }

    #[test]
    fn is_loopback_hostname_recognises_loopback_names_and_ips() {
        for h in [
            "localhost",
            "ip6-localhost",
            "127.0.0.1",
            "127.0.0.7",
            "::1",
        ] {
            assert!(is_loopback_hostname(h), "{h:?} should be loopback");
        }
        for h in ["server1", "10.0.0.5", "example.com", "loopback.test", ""] {
            assert!(!is_loopback_hostname(h), "{h:?} should NOT be loopback");
        }
    }

    #[test]
    fn has_remote_peers_loopback_only() {
        let cfg = Config::load_from_str(minimal_toml()).unwrap();
        assert!(!cfg.has_remote_peers());
    }

    #[test]
    fn has_remote_peers_with_real_hostname() {
        let toml = r#"
            node_id = 0
            [[pool]]
            id = 0
            hostname = "127.0.0.1"
            [[pool]]
            id = 1
            hostname = "server2.example.com"
        "#;
        let cfg = Config::load_from_str(toml).unwrap();
        assert!(cfg.has_remote_peers());
    }

    #[test]
    fn to_serve_settings_carries_fields() {
        let cfg = Config::load_from_str(minimal_toml()).unwrap();
        let s = cfg.to_serve_settings();
        assert_eq!(s.agent_port, DEFAULT_AGENT_PORT);
        assert_eq!(s.unix_socket, DEFAULT_UNIX_SOCKET);
        assert!(!s.tls_configured);
        assert!(!s.has_remote_peers);
        assert!(s.healthz.enabled);
        assert_eq!(s.healthz.listen_addr, "0.0.0.0:9702");
    }

    #[test]
    fn reject_insecure_remote_peer_unlocked_only_by_dev() {
        let mut s = Config::load_from_str(minimal_toml())
            .unwrap()
            .to_serve_settings();
        // Force the "remote peers present, no TLS" scenario.
        s.has_remote_peers = true;
        s.tls_configured = false;
        // Without --dev → reject (TLS is mandatory).
        s.dev_mode = false;
        assert!(s.reject_insecure_remote_peer());
        // --dev → allow (operator explicitly opted in for this run).
        s.dev_mode = true;
        assert!(!s.reject_insecure_remote_peer());
    }

    #[test]
    fn to_node_pool_carries_local_id_and_members() {
        let cfg = Config::load_from_str(minimal_toml()).unwrap();
        let pool = cfg.to_node_pool();
        assert_eq!(pool.local_node_id, 0);
        assert_eq!(pool.members.len(), 2);
    }

    #[test]
    fn to_postgres_runtime_unwraps_options() {
        let cfg = Config::load_from_str(minimal_toml()).unwrap();
        let pg = cfg.to_postgres_runtime();
        assert_eq!(pg.port, DEFAULT_PG_PORT);
        assert_eq!(pg.data_dir, Path::new(DEFAULT_PG_DATA_DIR));
        assert_eq!(pg.repl_user, DEFAULT_REPL_USER);
    }

    fn sample_pool() -> NodePool {
        NodePool {
            members: vec![
                NodeConfig {
                    id: 0,
                    hostname: "127.0.0.1".into(),
                },
                NodeConfig {
                    id: 1,
                    hostname: "127.0.0.2".into(),
                },
                NodeConfig {
                    id: 2,
                    hostname: "127.0.0.3".into(),
                },
            ],
            local_node_id: 1,
        }
    }

    #[test]
    fn node_pool_lookups() {
        let p = sample_pool();
        assert_eq!(p.node_by_id(0).unwrap().hostname, "127.0.0.1");
        assert!(p.node_by_id(99).is_err());
        assert_eq!(p.node_by_hostname("127.0.0.2").unwrap().id, 1);
        assert!(p.node_by_hostname("missing").is_err());
        assert_eq!(p.local_node().unwrap().id, 1);
        assert!(p.is_local(&p.members[1]));
        assert!(!p.is_local(&p.members[0]));
    }

    #[test]
    fn node_pool_local_node_unresolved_errors() {
        let p = NodePool {
            members: vec![NodeConfig {
                id: 0,
                hostname: "a".into(),
            }],
            local_node_id: -1,
        };
        assert!(matches!(p.local_node(), Err(AgentError::NoLocalNode)));
    }

    #[test]
    fn node_pool_resolve_node_prefers_hostname() {
        let p = sample_pool();
        let ref_ = pb::NodeRef {
            id: 99, // wrong id
            hostname: "127.0.0.2".into(),
            ..Default::default()
        };
        let n = p.resolve_node(&ref_).unwrap();
        assert_eq!(n.id, 1); // hostname won
    }

    #[test]
    fn node_pool_resolve_node_falls_back_to_id() {
        let p = sample_pool();
        let ref_ = pb::NodeRef {
            id: 2,
            hostname: String::new(),
            ..Default::default()
        };
        let n = p.resolve_node(&ref_).unwrap();
        assert_eq!(n.hostname, "127.0.0.3");
    }

    #[test]
    fn node_pool_resolve_node_rejects_unknown() {
        let p = sample_pool();
        let ref_ = pb::NodeRef {
            id: -1,
            hostname: "ghost".into(),
            ..Default::default()
        };
        assert!(matches!(
            p.resolve_node(&ref_),
            Err(AgentError::NodeRefUnresolvable { .. })
        ));
    }

    #[test]
    fn node_pool_resolve_local_node_refuses_peer() {
        let p = sample_pool();
        let ref_ = pb::NodeRef {
            id: 0, // not the local id (which is 1)
            hostname: String::new(),
            ..Default::default()
        };
        assert!(matches!(
            p.resolve_local_node(&ref_),
            Err(AgentError::NodeRefNotLocal { .. })
        ));
    }

    #[test]
    fn env_overrides_via_closure() {
        let mut cfg = Config::default();
        cfg.apply_defaults();
        let env = |k: &str| match k {
            ENV_SOCKET => Some("/tmp/sock".to_string()),
            ENV_AGENT_PORT => Some("9999".to_string()),
            ENV_PCP_USER => Some("monitor".to_string()),
            ENV_PCP_PORT => Some("notaport".to_string()), // ignored
            ENV_TLS_CERT => Some("".to_string()),         // empty ignored
            _ => None,
        };
        cfg.apply_env_overrides_from(env);
        assert_eq!(cfg.unix_socket.as_deref(), Some("/tmp/sock"));
        assert_eq!(cfg.agent_port, Some(9999));
        assert_eq!(cfg.pcp.user.as_deref(), Some("monitor"));
        // Bad numeric env var leaves the existing default intact.
        assert_eq!(cfg.pcp.port, Some(DEFAULT_PCP_PORT));
        // Empty env-var value is treated as "unset".
        assert!(cfg.tls.cert.is_none());
    }

    #[test]
    fn warn_node_ref_mismatch_does_not_panic_on_drift() {
        let pool = sample_pool();
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: PathBuf::from("/var/lib/postgresql/17/main"),
            repl_user: "repl".into(),
        };
        let drifted = pb::NodeRef {
            id: 99,
            hostname: "ghost".into(),
            pg_port: 6789,
            pg_data: "/elsewhere".into(),
        };
        // We're not capturing tracing output here — just proving the helper
        // is safe to call with every field set to a mismatched value.
        warn_node_ref_mismatch(&drifted, &pool.members[1], Some(&pg));
        warn_node_ref_mismatch(&drifted, &pool.members[1], None);
    }
}
