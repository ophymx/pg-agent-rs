//! pgpool Control Protocol client. Shells out to `pcp_attach_node` and
//! `pcp_node_count` (both are shipped by the `pgpool2` distro package);
//! `-w` disables the password prompt so auth flows through `~/.pcppass`
//! (mode 0600, owned by the postgres user, provisioned by Ansible — see
//! SPEC §10.5 and §13.1).
//!
//! # `pcp_node_count` semantics
//!
//! Returns the **count of backends defined in pgpool.conf**, NOT the
//! count of currently-up backends. Per upstream docs (`pcp-node-count.html`):
//! *"It does not distinguish between nodes status, ie attached/detached.
//! ALL nodes are counted."* The agent uses it for /healthz's
//! `backends_up` (misleadingly named — see SPEC §9.2 caveat) and as a
//! coarse reachability probe for the local pgpool instance.

use crate::config::{PcpConfig, DEFAULT_PCP_PORT, DEFAULT_PCP_USER};
use async_trait::async_trait;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{debug, info};

#[async_trait]
pub trait Pcp: Send + Sync {
    /// Re-attach a detached node to the local pgpool. Used at the tail of
    /// `FollowPrimary` after a standby is back up and replicating.
    async fn attach_node(&self, node_id: i32) -> anyhow::Result<()>;

    /// Total backends defined in pgpool.conf. Does **not** distinguish
    /// attached from detached — see module docs.
    async fn node_count(&self) -> anyhow::Result<i32>;
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
}

#[async_trait]
impl Pcp for PcpCli {
    async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
        info!(node_id, "pcp_attach: starting");
        let mut cmd = Command::new("pcp_attach_node");
        cmd.args(self.common_args())
            .arg("-n")
            .arg(node_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = cmd
            .output()
            .await
            .map_err(|e| spawn_error("pcp_attach_node", e))?;
        if !output.status.success() {
            let tail = combine_output(&output.stdout, &output.stderr);
            anyhow::bail!(
                "pcp_attach_node (node {node_id}): exit {}: {}",
                exit_code(&output.status),
                tail.trim()
            );
        }
        info!(node_id, "pcp_attach: completed");
        Ok(())
    }

    async fn node_count(&self) -> anyhow::Result<i32> {
        // Demoted to debug — this fires on every healthsnap tick (~1s)
        // and would otherwise dominate journalctl output.
        debug!("pcp_node_count: starting");
        let mut cmd = Command::new("pcp_node_count");
        cmd.args(self.common_args())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = cmd
            .output()
            .await
            .map_err(|e| spawn_error("pcp_node_count", e))?;
        if !output.status.success() {
            let tail = combine_output(&output.stdout, &output.stderr);
            anyhow::bail!(
                "pcp_node_count: exit {}: {}",
                exit_code(&output.status),
                tail.trim()
            );
        }

        let raw = String::from_utf8_lossy(&output.stdout);
        let trimmed = raw.trim();
        let n = trimmed
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("pcp_node_count: parse {trimmed:?}: {e}"))?;
        debug!(n, "pcp_node_count: completed");
        Ok(n)
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
}
