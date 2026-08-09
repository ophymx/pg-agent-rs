//! pgpool Control Protocol client. Shells out to `pcp_attach_node`,
//! `pcp_detach_node`, `pcp_node_info`, and `pcp_node_count` (all shipped
//! by the `pgpool2` distro package); `-w` disables the password prompt so
//! auth flows through `~/.pcppass` (mode 0600, owned by the postgres
//! user, provisioned by Ansible — see SPEC §10.5 and §13.1).
//!
//! # Hot path: `pcp_node_info -a`
//!
//! `/healthz`'s snapshot loop calls `pcp_node_info -a` every ~1 s —
//! one subprocess invocation, one row per backend, 11 structured
//! fields (per `pcp-node-info.html`) followed by a `last_status_change`
//! timestamp we discard. The parser yields a [`NodeInfo`] per row;
//! [`NodeInfo::is_up`] is the readiness predicate (`status_code < 3`).
//! Same subprocess cost as the simpler `pcp_node_count` we used
//! previously, much richer signal.
//!
//! # `pcp_node_count` semantics (legacy / preflight)
//!
//! Returns the **count of backends defined in pgpool.conf**, NOT the
//! count of currently-up backends. Per upstream docs
//! (`pcp-node-count.html`): *"It does not distinguish between nodes
//! status, ie attached/detached. ALL nodes are counted."* The agent
//! keeps it in the `Pcp` trait for preflight / operator use but no
//! longer relies on it for `/healthz` readiness.

use crate::config::{PcpConfig, DEFAULT_PCP_PORT, DEFAULT_PCP_USER};
use async_trait::async_trait;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tracing::{debug, info};

/// Ceiling on any single PCP invocation. PCP talks to the local pgpool
/// over loopback and every wrapped command is a metadata operation, so
/// this is generous — its job is to stop a wedged pgpool from pinning a
/// handler (or the healthz probe path) forever, not to race normal
/// completion. On expiry the child is SIGKILLed via `kill_on_drop`.
const PCP_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait]
pub trait Pcp: Send + Sync {
    /// Re-attach a detached node to the local pgpool. Used at the tail of
    /// `FollowPrimary` after a standby is back up and replicating.
    async fn attach_node(&self, node_id: i32) -> anyhow::Result<()>;

    /// Detach a node from the local pgpool's routing (`pcp_detach_node`,
    /// non-graceful — no `-g`). No hook path calls this today; it exists
    /// for planned-maintenance flows and the watchdog-off fan-out, where
    /// every attach/detach must be issued per pgpool instance
    /// (docs/pgpool-hook-contract.md §3). Note pgpool fires its
    /// `failover_command` in response to a detach.
    async fn detach_node(&self, node_id: i32) -> anyhow::Result<()>;

    /// Total backends defined in pgpool.conf. Does **not** distinguish
    /// attached from detached — see module docs. Kept for preflight /
    /// operator use; `/healthz` consumes [`Pcp::node_info_all`] instead.
    async fn node_count(&self) -> anyhow::Result<i32>;

    /// Full per-backend status via `pcp_node_info -a` (one subprocess,
    /// one row per backend). Used by the healthz snapshot loop and the
    /// future `pg_agentctl cluster status` fan-out — same subprocess
    /// cost as `node_count` but much richer signal (per-backend
    /// up/down, role, replication state).
    async fn node_info_all(&self) -> anyhow::Result<Vec<NodeInfo>>;
}

// ---------------------------------------------------------------------------
// NodeInfo
// ---------------------------------------------------------------------------

/// One backend's view from `pcp_node_info -a`, parsed verbatim from the
/// 11-field output documented at `pcp-node-info.html`. The `id` field is
/// inferred from line position (pgpool emits rows in `[[pool]]` order
/// 0..N).
///
/// All fields are kept (even the ones the snapshot body doesn't surface
/// today) so the future `/metrics` and `pg_agentctl cluster status`
/// consumers can read whatever they need without re-shelling
/// `pcp_node_info`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub id: i32,
    pub hostname: String,
    pub port: u16,
    /// 0–3 per pgpool docs: 0 = init (never displayed); 1 = up, no
    /// connections; 2 = up, pooled; 3 = down.
    pub status_code: u8,
    /// Pgpool's normalised weight (0.0–1.0).
    pub lb_weight: f64,
    /// Textual status: `"waiting"` / `"up"` / `"down"`.
    pub status_name: String,
    /// Real-time backend status via `PQpingParams` (pgpool 4.3+):
    /// `"up"` / `"down"` / `"unknown"`.
    pub actual_status: String,
    /// Pgpool's view of the role: `"primary"` / `"standby"` (streaming
    /// replication) or `"main"` / `"replica"` (other modes).
    pub role: String,
    /// Real-time role via `pg_is_in_recovery` (pgpool 4.3+). Diverges
    /// from `role` only in misconfigured-multi-primary situations —
    /// useful debug signal.
    pub actual_role: String,
    /// Either bytes or seconds depending on `delay_threshold_by_time`
    /// (pgpool 4.4+). Kept as a string because the unit isn't carried
    /// in the wire output.
    pub replication_delay: String,
    /// `"streaming"` / `"catchup"` / `"none"` from
    /// `pg_stat_replication` (pgpool 4.1+).
    pub replication_state: String,
    /// `"sync"` / `"async"` / `"none"` from `pg_stat_replication`
    /// (pgpool 4.1+).
    pub sync_state: String,
}

impl NodeInfo {
    /// True iff pgpool considers this backend routable. Codes 1
    /// (`"waiting"`) and 2 (`"up"`) both count; code 3 (`"down"`)
    /// doesn't. Code 0 is the init state and shouldn't appear in
    /// `pcp_node_info` output.
    pub fn is_up(&self) -> bool {
        self.status_code > 0 && self.status_code < 3
    }
}

// ---------------------------------------------------------------------------
// PcpCli — production impl
// ---------------------------------------------------------------------------

pub struct PcpCli {
    host: String,
    port: u16,
    user: String,
}

impl PcpCli {
    /// Targets the local pgpool by convention (PCP is a control-plane
    /// protocol; remote nodes are reached over the peer gRPC mesh instead).
    /// Defensively unwraps the Option fields against the const defaults so
    /// this works whether or not [`crate::config::Config::apply_defaults`]
    /// has already run.
    pub fn new(cfg: &PcpConfig) -> Self {
        Self {
            host: "localhost".to_string(),
            port: cfg.port.unwrap_or(DEFAULT_PCP_PORT),
            user: cfg
                .user
                .clone()
                .unwrap_or_else(|| DEFAULT_PCP_USER.to_string()),
        }
    }

    /// Args shared by every PCP invocation. Returned by value so each call
    /// site gets a fresh `Vec<String>` (Command's args expect owned-ish).
    fn common_args(&self) -> Vec<String> {
        vec![
            "-h".to_string(),
            self.host.clone(),
            "-p".to_string(),
            self.port.to_string(),
            "-U".to_string(),
            self.user.clone(),
            // -w = no password prompt; auth uses ~/.pcppass.
            "-w".to_string(),
        ]
    }

    /// Run one PCP binary to completion under [`PCP_TIMEOUT`] and return
    /// its stdout. `desc` is the human-readable invocation label used in
    /// error messages (e.g. `"pcp_attach_node (node 2)"`), which may carry
    /// more context than the bare binary name.
    async fn run_pcp(&self, bin: &str, desc: &str, extra_args: &[&str]) -> anyhow::Result<String> {
        let mut cmd = Command::new(bin);
        cmd.args(self.common_args())
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = tokio::time::timeout(PCP_TIMEOUT, cmd.output())
            .await
            .map_err(|_| {
                // Dropping the output() future SIGKILLs the child
                // (kill_on_drop above), so nothing lingers past this error.
                anyhow::anyhow!("{desc}: timed out after {}s", PCP_TIMEOUT.as_secs())
            })?
            .map_err(|e| spawn_error(bin, e))?;
        if !output.status.success() {
            let tail = combine_output(&output.stdout, &output.stderr);
            anyhow::bail!(
                "{desc}: exit {}: {}",
                exit_code(&output.status),
                tail.trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

#[async_trait]
impl Pcp for PcpCli {
    async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
        info!(node_id, "pcp_attach: starting");
        let id = node_id.to_string();
        self.run_pcp(
            "pcp_attach_node",
            &format!("pcp_attach_node (node {node_id})"),
            &["-n", &id],
        )
        .await?;
        info!(node_id, "pcp_attach: completed");
        Ok(())
    }

    async fn detach_node(&self, node_id: i32) -> anyhow::Result<()> {
        info!(node_id, "pcp_detach: starting");
        let id = node_id.to_string();
        self.run_pcp(
            "pcp_detach_node",
            &format!("pcp_detach_node (node {node_id})"),
            &["-n", &id],
        )
        .await?;
        info!(node_id, "pcp_detach: completed");
        Ok(())
    }

    async fn node_count(&self) -> anyhow::Result<i32> {
        // Demoted to debug — this fires on every healthsnap tick (~1s)
        // and would otherwise dominate journalctl output.
        debug!("pcp_node_count: starting");
        let raw = self.run_pcp("pcp_node_count", "pcp_node_count", &[]).await?;
        let trimmed = raw.trim();
        let n = trimmed
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("pcp_node_count: parse {trimmed:?}: {e}"))?;
        debug!(n, "pcp_node_count: completed");
        Ok(n)
    }

    async fn node_info_all(&self) -> anyhow::Result<Vec<NodeInfo>> {
        // Demoted to debug — fires every healthsnap tick (~1s).
        debug!("pcp_node_info -a: starting");
        // -a = dump every backend in one invocation.
        let raw = self
            .run_pcp("pcp_node_info", "pcp_node_info -a", &["-a"])
            .await?;
        let nodes = parse_node_info_all(&raw)?;
        debug!(n = nodes.len(), "pcp_node_info -a: completed");
        Ok(nodes)
    }
}

// ---------------------------------------------------------------------------
// Helpers (testable)
// ---------------------------------------------------------------------------

/// Friendlier error message when the PCP binary isn't on `$PATH` — common
/// during a botched ansible run where `pgpool2` wasn't installed. Other
/// spawn errors fall through with their stdlib message.
fn spawn_error(bin: &str, e: std::io::Error) -> anyhow::Error {
    if e.kind() == std::io::ErrorKind::NotFound {
        anyhow::anyhow!(
            "{bin}: binary not found on $PATH \
             (install the pgpool2 package on this node, or check that \
             /usr/bin is in pg_agentd.service's effective PATH)"
        )
    } else {
        anyhow::anyhow!("{bin}: spawn: {e}")
    }
}

fn exit_code(status: &std::process::ExitStatus) -> String {
    status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "?".to_string())
}

/// Concatenate stdout + stderr for inclusion in an error message. Doesn't
/// try to preserve the exact interleaving order the subprocess produced
/// (that'd require a single shared pipe + read loop); a labelled
/// concatenation is good enough for diagnostic output.
fn combine_output(stdout: &[u8], stderr: &[u8]) -> String {
    let out = String::from_utf8_lossy(stdout);
    let err = String::from_utf8_lossy(stderr);
    match (out.is_empty(), err.is_empty()) {
        (true, true) => String::new(),
        (false, true) => out.into_owned(),
        (true, false) => err.into_owned(),
        (false, false) => format!("{out}\n{err}"),
    }
}

/// Parse the multi-line output of `pcp_node_info -a` into one
/// `NodeInfo` per line. `id` is assigned by line position (pgpool emits
/// rows in `[[pool]]` order). Each line is 11 structured tokens plus a
/// trailing `last_status_change` timestamp (2 tokens), 13 total.
///
/// Blank lines are skipped. A malformed line aborts the whole parse —
/// silently dropping a row would mean lying about cluster shape.
fn parse_node_info_all(raw: &str) -> anyhow::Result<Vec<NodeInfo>> {
    let mut out = Vec::new();
    for (i, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        out.push(
            parse_node_info_line(out.len() as i32, line)
                .map_err(|e| anyhow::anyhow!("pcp_node_info -a: line {} {line:?}: {e}", i + 1))?,
        );
    }
    Ok(out)
}

fn parse_node_info_line(id: i32, line: &str) -> anyhow::Result<NodeInfo> {
    // Per pcp-node-info.html the 11 structured fields are:
    //   hostname port status_code lb_weight status_name actual_status
    //   role actual_role replication_delay replication_state sync_state
    // followed by the timestamp (2 whitespace-separated tokens we discard).
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 11 {
        anyhow::bail!("expected ≥11 tokens, got {}", tokens.len());
    }
    let port: u16 = tokens[1]
        .parse()
        .map_err(|e| anyhow::anyhow!("port {:?}: {e}", tokens[1]))?;
    let status_code: u8 = tokens[2]
        .parse()
        .map_err(|e| anyhow::anyhow!("status_code {:?}: {e}", tokens[2]))?;
    let lb_weight: f64 = tokens[3]
        .parse()
        .map_err(|e| anyhow::anyhow!("lb_weight {:?}: {e}", tokens[3]))?;
    Ok(NodeInfo {
        id,
        hostname: tokens[0].to_string(),
        port,
        status_code,
        lb_weight,
        status_name: tokens[4].to_string(),
        actual_status: tokens[5].to_string(),
        role: tokens[6].to_string(),
        actual_role: tokens[7].to_string(),
        replication_delay: tokens[8].to_string(),
        replication_state: tokens[9].to_string(),
        sync_state: tokens[10].to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(user: Option<&str>, port: Option<u16>) -> PcpConfig {
        PcpConfig {
            user: user.map(String::from),
            port,
            pgpool_service: None,
        }
    }

    #[test]
    fn new_uses_defaults_when_unset() {
        let cli = PcpCli::new(&cfg_with(None, None));
        assert_eq!(cli.host, "localhost");
        assert_eq!(cli.port, DEFAULT_PCP_PORT);
        assert_eq!(cli.user, DEFAULT_PCP_USER);
    }

    #[test]
    fn new_respects_explicit_values() {
        let cli = PcpCli::new(&cfg_with(Some("monitor"), Some(11001)));
        assert_eq!(cli.host, "localhost");
        assert_eq!(cli.port, 11001);
        assert_eq!(cli.user, "monitor");
    }

    #[test]
    fn common_args_layout() {
        let cli = PcpCli::new(&cfg_with(Some("pgpool"), Some(9898)));
        assert_eq!(
            cli.common_args(),
            vec!["-h", "localhost", "-p", "9898", "-U", "pgpool", "-w",]
        );
    }

    #[test]
    fn combine_output_handles_each_empty_combination() {
        assert_eq!(combine_output(b"", b""), "");
        assert_eq!(combine_output(b"3\n", b""), "3\n");
        assert_eq!(combine_output(b"", b"oops\n"), "oops\n");
        assert_eq!(combine_output(b"hello\n", b"warn\n"), "hello\n\nwarn\n");
    }

    #[test]
    fn combine_output_handles_non_utf8_gracefully() {
        // PCP's output is ASCII in practice; lossy decode just inserts
        // replacement chars rather than panicking.
        let bytes = &[b'o', b'k', 0xFF, b'\n'];
        let s = combine_output(bytes, b"");
        assert!(s.starts_with("ok"));
        assert!(s.ends_with("\n"));
    }

    // ----- parse_node_info_all ---------------------------------------------

    /// Verbatim example from pcp-node-info.html (`pcp_node_info -a` output
    /// for a 2-backend cluster).
    const DOCS_EXAMPLE: &str = "\
/tmp 11002 1 0.500000 waiting up primary primary 0 none none 2021-02-27 14:51:30
/tmp 11003 1 0.500000 waiting up standby standby 0 streaming async 2021-02-27 14:51:30
";

    #[test]
    fn parse_node_info_all_parses_docs_example() {
        let nodes = parse_node_info_all(DOCS_EXAMPLE).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, 0);
        assert_eq!(nodes[0].hostname, "/tmp");
        assert_eq!(nodes[0].port, 11002);
        assert_eq!(nodes[0].status_code, 1);
        assert!((nodes[0].lb_weight - 0.5).abs() < 1e-9);
        assert_eq!(nodes[0].status_name, "waiting");
        assert_eq!(nodes[0].actual_status, "up");
        assert_eq!(nodes[0].role, "primary");
        assert_eq!(nodes[0].actual_role, "primary");
        assert_eq!(nodes[0].replication_delay, "0");
        assert_eq!(nodes[0].replication_state, "none");
        assert_eq!(nodes[0].sync_state, "none");
        // Second node — verify id assigned by position.
        assert_eq!(nodes[1].id, 1);
        assert_eq!(nodes[1].role, "standby");
        assert_eq!(nodes[1].replication_state, "streaming");
    }

    #[test]
    fn parse_node_info_all_handles_down_backend() {
        // Status code 3 is "down" — pgpool would emit it as status_name
        // "down" and the actual_status would also be "down".
        let raw = "\
server1 5432 2 0.333333 up up primary primary 0 none none 2025-12-04 10:00:00
server2 5432 2 0.333333 up up standby standby 1024 streaming async 2025-12-04 10:00:00
server3 5432 3 0.333333 down down standby standby 0 none none 2025-12-04 09:55:00
";
        let nodes = parse_node_info_all(raw).unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes[0].is_up());
        assert!(nodes[1].is_up());
        assert!(!nodes[2].is_up());
        assert_eq!(nodes[2].status_name, "down");
        assert_eq!(nodes[2].status_code, 3);
    }

    #[test]
    fn is_up_predicate_covers_waiting_and_up() {
        let mut n = NodeInfo {
            id: 0,
            hostname: "h".into(),
            port: 5432,
            status_code: 1, // waiting
            lb_weight: 1.0,
            status_name: "waiting".into(),
            actual_status: "up".into(),
            role: "primary".into(),
            actual_role: "primary".into(),
            replication_delay: "0".into(),
            replication_state: "none".into(),
            sync_state: "none".into(),
        };
        assert!(n.is_up(), "status_code=1 (waiting) should be up");
        n.status_code = 2;
        assert!(n.is_up(), "status_code=2 (up) should be up");
        n.status_code = 3;
        assert!(!n.is_up(), "status_code=3 (down) should not be up");
        n.status_code = 0;
        assert!(!n.is_up(), "status_code=0 (init) should not be up");
    }

    #[test]
    fn parse_node_info_all_skips_blank_lines() {
        let raw = "\n\n/tmp 11002 1 0.500000 waiting up primary primary 0 none none 2021-02-27 14:51:30\n\n";
        let nodes = parse_node_info_all(raw).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, 0);
    }

    #[test]
    fn parse_node_info_all_returns_empty_for_empty_input() {
        let nodes = parse_node_info_all("").unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn parse_node_info_all_fails_on_short_line() {
        // Missing the trailing fields — pgpool version-skew or output
        // corruption. Better to error than silently lie about cluster
        // shape.
        let raw = "/tmp 11002 1 0.500000 waiting up\n";
        let err = parse_node_info_all(raw).unwrap_err().to_string();
        assert!(err.contains("expected"));
    }

    #[test]
    fn parse_node_info_all_fails_on_unparseable_port() {
        let raw =
            "/tmp notaport 1 0.500000 waiting up primary primary 0 none none 2021-02-27 14:51:30\n";
        let err = parse_node_info_all(raw).unwrap_err().to_string();
        assert!(err.contains("port"));
    }

    #[test]
    fn parse_node_info_all_fails_on_unparseable_status_code() {
        let raw =
            "/tmp 11002 X 0.500000 waiting up primary primary 0 none none 2021-02-27 14:51:30\n";
        let err = parse_node_info_all(raw).unwrap_err().to_string();
        assert!(err.contains("status_code"));
    }
}
