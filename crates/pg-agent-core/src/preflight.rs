//! Runtime-prerequisite checks. Each is independent and idempotent; failures
//! are reported, not raised. Designed to be invoked from `pg_agentctl
//! preflight` and from Ansible (parseable output, deterministic exit code).
//!
//! See SPEC §14 for the full check list.

use crate::{config::Config, localdb::LocalDb};
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Ok,
    Warn,
    Err,
}

impl CheckStatus {
    pub fn tag(&self) -> &'static str {
        match self {
            CheckStatus::Ok => "OK  ",
            CheckStatus::Warn => "WARN",
            CheckStatus::Err => "ERR ",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Default, Clone)]
pub struct PreflightReport {
    pub checks: Vec<Check>,
}

impl PreflightReport {
    pub fn has_errors(&self) -> bool {
        self.checks.iter().any(|c| c.status == CheckStatus::Err)
    }

    pub fn print(&self, w: &mut dyn Write) -> std::io::Result<()> {
        let name_width = self.checks.iter().map(|c| c.name.len()).max().unwrap_or(0);
        let (mut errs, mut warns) = (0usize, 0usize);
        for c in &self.checks {
            if c.detail.is_empty() {
                writeln!(
                    w,
                    "{}  {:<width$}",
                    c.status.tag(),
                    c.name,
                    width = name_width
                )?;
            } else {
                writeln!(
                    w,
                    "{}  {:<width$}  {}",
                    c.status.tag(),
                    c.name,
                    c.detail,
                    width = name_width
                )?;
            }
            match c.status {
                CheckStatus::Err => errs += 1,
                CheckStatus::Warn => warns += 1,
                _ => {}
            }
        }
        writeln!(w)?;
        match (errs, warns) {
            (0, 0) => writeln!(w, "preflight: all checks passed")?,
            (0, _) => writeln!(w, "preflight: {warns} warning(s) — OK with warnings")?,
            _ => writeln!(w, "preflight: {errs} error(s), {warns} warning(s) — FAIL")?,
        }
        Ok(())
    }

    /// Machine-readable JSON dump suitable for `ansible.builtin.command` +
    /// `register:` + `from_json`. The field shape is stable.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "checks": self.checks.iter().map(|c| serde_json::json!({
                "name":   c.name,
                "status": c.status.tag().trim(),
                "detail": c.detail,
            })).collect::<Vec<_>>(),
            "has_errors": self.has_errors(),
        })
    }
}

/// Run every check in a deterministic order. DB-backed checks are skipped
/// with a WARN if `db` is None; peer-reachability checks are skipped with
/// a WARN if `skip_peers` is true or if TLS isn't configured.
pub async fn preflight(
    _cfg: &Config,
    _db: Option<Arc<dyn LocalDb>>,
    _skip_peers: bool,
) -> PreflightReport {
    // TODO(v1): port every checker — see SPEC §14.
    //
    // Filesystem (always):
    //   tls material, polkit rule, pgpool_node_id, .pcppass, .pgpass,
    //   pcp.conf, pool_passwd, recovery tools. Path constants per Debian
    //   layout (/etc/pgpool2/).
    //
    // Peer connectivity (skipped if !cfg.tls.is_configured() OR skip_peers):
    //   for each non-local pool entry, dial <peer>:agent_port via tonic
    //   with our mTLS material, call PgAgentPeer::GetStatus, surface
    //   distinct ERR categories for connect-refused / TLS-handshake /
    //   RPC-error. This is the network-level twin of "tls material" —
    //   verifies the cluster mTLS topology end-to-end before a real
    //   failover surfaces a misconfigured link.
    //
    // DB-backed (skipped if db is None):
    //   postgres settings, postgres SSL config, pgpool_recovery extension,
    //   postgres roles, pg_hba.conf.
    PreflightReport::default()
}
