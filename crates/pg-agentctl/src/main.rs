//! pg_agentctl — operator CLI. See SPEC §13.
//!
//! Designed to be friendly to Ansible:
//!   - Every subcommand has a stable exit code (0 = success, 1 = work to do
//!     or hard failure, 2 = usage error).
//!   - `--json` everywhere relevant. The schema is stable across versions.
//!   - Idempotent by default. Re-running a successful command is a no-op.
//!   - No interactive prompts. `--yes` is implied; destructive commands
//!     require an explicit `--force` flag.

mod client;
mod config_loader;

use anyhow::Context;
use clap::{Parser, Subcommand};
use pg_agent_hookspec as hookspec;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "pg_agentctl", version, about, long_about = None)]
struct Cli {
    /// Emit machine-readable JSON instead of human-readable text where the
    /// subcommand supports it.
    #[arg(long, global = true)]
    json: bool,

    /// Override the Unix socket path. Precedence: this flag → config
    /// `unix_socket` → /run/pg_agentd/pg_agentd.sock. Only relevant
    /// for subcommands that dial the local daemon (cluster init,
    /// maintenance, …).
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Print canonical pgpool.conf / postgresql.conf hook stanzas.
    PrintHooks,

    /// Validate hook directives in a pgpool.conf.
    CheckHooks { path: std::path::PathBuf },

    /// Generate a pgpool include with live backend values from each peer.
    GenPgpool {
        #[arg(long)]
        write: Option<std::path::PathBuf>,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },

    /// Validate that the runtime environment satisfies the agent's prereqs.
    Preflight {
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
        #[arg(long)]
        skip_db: bool,
    },

    /// Maintenance-queue admin.
    Maintenance {
        #[command(subcommand)]
        cmd: MaintenanceCmd,
    },

    /// Cluster-wide operations.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
}

#[derive(Debug, Subcommand)]
enum MaintenanceCmd {
    /// List intents (default: every status).
    List {
        #[arg(long, value_parser = ["pending", "done", "abandoned"])]
        status: Option<String>,
    },
    /// Show one intent in full.
    Show { id: String },
    /// Reschedule a pending intent for immediate retry.
    Retry { id: String },
}

#[derive(Debug, Subcommand)]
enum ClusterCmd {
    /// One-time bootstrap from this primary.
    Init {
        #[arg(long)]
        only_node: Option<i32>,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    // v1.x roadmap items (placeholders so the surface is reserved):
    // Status     — fan-out GetStatus to every peer
    // Pause      — set cluster paused=true via shared-state RPC
    // Resume     — clear pause flag
    // Switchover — planned promotion
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    init_logging();
    let cli = Cli::parse();

    match dispatch(cli).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("pg_agentctl: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(cli: Cli) -> anyhow::Result<ExitCode> {
    match cli.cmd {
        Cmd::PrintHooks => {
            print_hooks(cli.json);
            Ok(ExitCode::SUCCESS)
        }
        Cmd::CheckHooks { path: _ } => {
            // TODO(v1): parse pgpool.conf, validate every entry against
            // hookspec::pgpool_hooks().
            Ok(ExitCode::from(2))
        }
        Cmd::GenPgpool { write, config } => {
            gen_pgpool(config, write, cli.socket.as_deref(), cli.json).await
        }
        Cmd::Preflight { config, skip_db } => preflight(config, skip_db, cli.json).await,
        Cmd::Maintenance { .. } => Ok(ExitCode::from(2)),
        Cmd::Cluster { cmd } => match cmd {
            ClusterCmd::Init { only_node, config } => {
                cluster_init(config, only_node, cli.socket.as_deref(), cli.json).await
            }
        },
    }
}

fn print_hooks(as_json: bool) {
    if as_json {
        let payload = serde_json::json!({
            "pgpool_conf":      hookspec::pgpool_hooks()
                .into_iter()
                .map(|h| serde_json::json!({"key": h.key, "value": h.value}))
                .collect::<Vec<_>>(),
            "postgresql_conf": {
                "restore_command": hookspec::restore_command(),
            }
        });
        println!("{}", serde_json::to_string_pretty(&payload).unwrap());
    } else {
        const KEY_WIDTH: usize = 26;
        println!("# pgpool.conf");
        for h in hookspec::pgpool_hooks() {
            println!("{:<width$} = '{}'", h.key, h.value, width = KEY_WIDTH);
        }
        println!();
        println!("# postgresql.conf");
        println!(
            "{:<width$} = '{}'",
            "restore_command",
            hookspec::restore_command(),
            width = KEY_WIDTH
        );
    }
}

async fn preflight(config_path: PathBuf, skip_db: bool, json: bool) -> anyhow::Result<ExitCode> {
    use pg_agent_core::localdb::{LocalDb, PgLocalDb};
    use std::sync::Arc;

    let cfg = config_loader::load_config(&config_path)?;

    // Try to open a local DB connection unless explicitly skipped.
    // If the connect fails (PG not up yet, socket dir mismatch), pass
    // None and the preflight body downgrades the DB-backed checks to
    // a single WARN. That's the right shape for "pre-bootstrap"
    // preflights where PG may not be running yet.
    let db: Option<Arc<dyn LocalDb>> = if skip_db {
        None
    } else {
        let socket_dir = cfg
            .postgres
            .socket_dir
            .clone()
            .ok_or_else(|| anyhow::anyhow!("postgres.socket_dir unset after apply_defaults"))?;
        let port = cfg
            .postgres
            .port
            .ok_or_else(|| anyhow::anyhow!("postgres.port unset after apply_defaults"))?;
        match PgLocalDb::connect(&socket_dir, port).await {
            Ok(db) => Some(Arc::new(db) as Arc<dyn LocalDb>),
            Err(e) => {
                eprintln!("warning: local DB unreachable ({e}); skipping DB-backed checks");
                None
            }
        }
    };

    let report = pg_agent_core::preflight::preflight(&cfg, db, false).await;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report.to_json())
                .expect("preflight report serialisation")
        );
    } else {
        report.print(&mut std::io::stdout())?;
    }

    if report.has_errors() {
        Ok(ExitCode::FAILURE)
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

async fn cluster_init(
    config_path: PathBuf,
    only_node: Option<i32>,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::ClusterInitRequest;

    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .cluster_init(ClusterInitRequest {
            only_node_id: only_node,
        })
        .await
        .map_err(|s| anyhow::anyhow!("ClusterInit RPC failed: {s}"))?
        .into_inner();

    if json {
        let payload = serde_json::json!({
            "ok":        resp.ok,
            "message":   resp.message,
            "repl_user": resp.repl_user,
            "standbys":  resp.standbys.iter().map(|s| serde_json::json!({
                "node_id":  s.node_id,
                "hostname": s.hostname,
                "ok":       s.ok,
                "message":  s.message,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        if !resp.message.is_empty() {
            println!("{}", resp.message);
        }
        if !resp.repl_user.is_empty() {
            println!("repl_user: {}", resp.repl_user);
        }
        if !resp.standbys.is_empty() {
            println!();
            println!("standbys:");
            for s in &resp.standbys {
                let tag = if s.ok { "OK  " } else { "FAIL" };
                println!(
                    "  {} node {} ({})  {}",
                    tag, s.node_id, s.hostname, s.message
                );
            }
        }
    }

    if resp.ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

async fn gen_pgpool(
    config_path: PathBuf,
    write: Option<PathBuf>,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_core::certreload::CertReloader;
    use pg_agent_core::peers::{PeerPool, PeerRegistry};
    use pg_agent_proto::pgagentpb::NodeConfigRequest;
    use std::sync::Arc;

    let cfg = config_loader::load_config(&config_path)?;

    // Local node goes through the Unix socket; peers through the mTLS
    // peer pool we build right here. The CLI doesn't share the
    // daemon's pool — see SPEC §13 ("the CLI builds its own PeerPool
    // from config").
    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;

    // Build a PeerPool if there is any remote node to dial; skip the
    // CertReloader otherwise so single-node deployments don't need TLS
    // material on disk just to run gen-pgpool.
    let has_remote = cfg.pool.iter().any(|n| n.id != cfg.local_node_id);
    let peer_pool: Option<Arc<PeerPool>> = if has_remote {
        if cfg.tls.is_configured() {
            let reloader = Arc::new(CertReloader::new(cfg.tls.clone())?);
            Some(PeerPool::new(reloader, cfg.agent_port.unwrap_or(0))?)
        } else if cfg.dev_mode {
            Some(PeerPool::new_dev(cfg.agent_port.unwrap_or(0)))
        } else {
            anyhow::bail!(
                "remote pool members configured but [tls] is not — set ca_cert/cert/key \
                 or run with --dev for a single-node test"
            );
        }
    } else {
        None
    };

    let mut rows: Vec<BackendRow> = Vec::with_capacity(cfg.pool.len());
    for node in &cfg.pool {
        let (port, data_dir) = if node.id == cfg.local_node_id {
            let mut local = client::dial_local(&socket).await?;
            let resp = local
                .get_node_config(NodeConfigRequest {})
                .await
                .map_err(|s| anyhow::anyhow!("GetNodeConfig (local) failed: {s}"))?
                .into_inner();
            (resp.pg_port, resp.pg_data_dir)
        } else {
            let pool = peer_pool.as_ref().expect("has_remote => peer_pool is Some");
            let client = pool.client(node).await?;
            let resp = client.get_node_config().await?;
            (resp.pg_port, resp.pg_data_dir)
        };
        rows.push(BackendRow {
            id: node.id,
            hostname: node.hostname.clone(),
            port,
            data_dir,
        });
    }
    rows.sort_by_key(|r| r.id);

    let fragment = render_pgpool_fragment(&rows);

    if json {
        let payload = serde_json::json!({
            "backends": rows.iter().map(|r| serde_json::json!({
                "id":       r.id,
                "hostname": r.hostname,
                "port":     r.port,
                "data_dir": r.data_dir,
            })).collect::<Vec<_>>(),
            "rendered": fragment,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if let Some(out) = write {
        atomic_write(&out, fragment.as_bytes())?;
        eprintln!("wrote {}", out.display());
    } else {
        print!("{fragment}");
    }

    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Clone)]
struct BackendRow {
    id: i32,
    hostname: String,
    port: i32,
    data_dir: String,
}

fn render_pgpool_fragment(rows: &[BackendRow]) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    s.push_str("# Generated by `pg_agentctl gen-pgpool`. Do not edit by hand.\n");
    s.push_str("# Pool size and per-backend addressing are owned by this file;\n");
    s.push_str("# the hook block below is the canonical pg_agent wiring.\n\n");
    for r in rows {
        let i = r.id;
        let _ = writeln!(s, "backend_hostname{i} = '{}'", r.hostname);
        let _ = writeln!(s, "backend_port{i} = {}", r.port);
        let _ = writeln!(s, "backend_data_directory{i} = '{}'", r.data_dir);
        let _ = writeln!(s, "backend_flag{i} = 'ALLOW_TO_FAILOVER'");
        s.push('\n');
    }
    s.push_str("# --- canonical hook block ---\n");
    const KEY_WIDTH: usize = 26;
    for h in hookspec::pgpool_hooks() {
        let _ = writeln!(s, "{:<width$} = '{}'", h.key, h.value, width = KEY_WIDTH);
    }
    s
}

/// Write `bytes` to `path` atomically: same-directory temp file, fsync,
/// rename. Crash anywhere before the rename leaves the original (if
/// any) intact.
fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent: {}", path.display()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("path has no file name: {}", path.display()))?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".tmp");
    let tmp = parent.join(tmp_name);
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("create temp {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn init_logging() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_pgpool_fragment_emits_backend_block_and_hooks() {
        let rows = vec![
            BackendRow {
                id: 0,
                hostname: "pg0.local".into(),
                port: 5432,
                data_dir: "/var/lib/postgresql/17/main".into(),
            },
            BackendRow {
                id: 1,
                hostname: "pg1.local".into(),
                port: 5433,
                data_dir: "/var/lib/postgresql/17/main".into(),
            },
        ];
        let out = render_pgpool_fragment(&rows);
        assert!(out.contains("backend_hostname0 = 'pg0.local'"));
        assert!(out.contains("backend_port0 = 5432"));
        assert!(out.contains("backend_data_directory0 = '/var/lib/postgresql/17/main'"));
        assert!(out.contains("backend_flag0 = 'ALLOW_TO_FAILOVER'"));
        assert!(out.contains("backend_hostname1 = 'pg1.local'"));
        assert!(out.contains("backend_port1 = 5433"));
        assert!(out.contains("failover_command"));
        assert!(out.contains("recovery_1st_stage_command"));
    }

    #[test]
    fn atomic_write_replaces_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("frag.conf");
        std::fs::write(&path, b"old").unwrap();
        atomic_write(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        let leftover: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert!(leftover.is_empty(), "stray tempfile: {leftover:?}");
    }
}
