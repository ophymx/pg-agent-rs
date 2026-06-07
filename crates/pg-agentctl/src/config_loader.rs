//! Tiny config-loading helpers shared by every subcommand that needs
//! more than just the socket path or the hookspec constants.
//!
//! Kept separate from `pg_agent_core::config::Config::load` so that
//! [`resolve_socket_path`] can short-circuit without doing a full
//! load when the operator passes `--socket` explicitly — the agent
//! might not be on the same host as the operator's workstation, and
//! requiring a valid `config.toml` just to dial would be hostile.

use anyhow::Context;
use pg_agent_core::config::{Config, DEFAULT_UNIX_SOCKET};
use std::path::{Path, PathBuf};

/// Pick the Unix socket path with this precedence (first hit wins):
///
/// 1. `--socket <path>` CLI flag (if supplied)
/// 2. `unix_socket` field from `config.toml` (if `config_path` exists)
/// 3. [`DEFAULT_UNIX_SOCKET`]
///
/// Returns `Ok(None)` if step 2 is requested but the config file is
/// missing — caller can decide whether to error or fall through to
/// the default. Returns `Err` only on a config that exists but won't
/// parse (the operator wants to know).
#[allow(dead_code)] // wired up by subsequent commits
pub fn resolve_socket_path(
    cli_socket: Option<&Path>,
    config_path: &Path,
) -> anyhow::Result<PathBuf> {
    if let Some(p) = cli_socket {
        return Ok(p.to_path_buf());
    }
    if config_path.exists() {
        let cfg = Config::load(config_path)
            .with_context(|| format!("load config {}", config_path.display()))?;
        if let Some(s) = cfg.unix_socket {
            return Ok(PathBuf::from(s));
        }
    }
    Ok(PathBuf::from(DEFAULT_UNIX_SOCKET))
}

/// Load + validate config. Thin wrapper that gives every subcommand
/// the same error-context shape.
#[allow(dead_code)] // wired up by subsequent commits
pub fn load_config(path: &Path) -> anyhow::Result<Config> {
    Config::load(path).with_context(|| format!("load config {}", path.display()))
}
