//! Socket-path resolver shared by every subcommand that dials the
//! local daemon. Every subcommand now routes through `pg_agentd` over
//! the Unix socket, so this is the only piece of "config" the CLI
//! still has to read — and even that is optional (`--socket` overrides
//! any file, and a missing config falls through to the default).

use anyhow::Context;
use pg_agent_core::config::{Config, DEFAULT_UNIX_SOCKET};
use std::path::{Path, PathBuf};

/// Pick the Unix socket path with this precedence (first hit wins):
///
/// 1. `--socket <path>` CLI flag (if supplied)
/// 2. `unix_socket` field from `config.toml` (if `config_path` exists)
/// 3. [`DEFAULT_UNIX_SOCKET`]
///
/// Returns `Err` only on a config that exists but won't parse — the
/// operator should be told. A missing config file is fine; we fall
/// through to the default socket location.
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
