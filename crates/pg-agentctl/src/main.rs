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
        Cmd::GenPgpool { .. } => {
            // TODO(v1): load config; build peer pool; fan out GetNodeConfig;
            // render fragment; atomic write or stdout.
            Ok(ExitCode::from(2))
        }
        Cmd::Preflight { config, skip_db } => preflight(config, skip_db, cli.json).await,
        Cmd::Maintenance { .. } => Ok(ExitCode::from(2)),
        Cmd::Cluster { .. } => Ok(ExitCode::from(2)),
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

fn init_logging() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}
