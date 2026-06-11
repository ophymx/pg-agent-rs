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
    /// Fan-out GetStatus to every pool member; render a topology table.
    /// Also serves as the mesh-level mTLS reachability check that
    /// `pg_agentd validate-env` doesn't cover.
    Status {
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    /// Reclone a target standby from this primary. Same orchestration
    /// as pgpool's recovery_1st_stage_command hook; different front
    /// door (routes through the local daemon, which owns PCP creds).
    /// Must be invoked on the current primary; will refuse cleanly if
    /// the local node is in recovery.
    Recover {
        /// Pool id of the standby to reclone.
        #[arg(long)]
        target: i32,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    // v1.x roadmap items (placeholders so the surface is reserved):
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
        Cmd::CheckHooks { path } => check_hooks(&path, cli.json),
        Cmd::GenPgpool { write, config } => {
            gen_pgpool(config, write, cli.socket.as_deref(), cli.json).await
        }
        Cmd::Maintenance { cmd } => maintenance(cmd, cli.socket.as_deref(), cli.json).await,
        Cmd::Cluster { cmd } => match cmd {
            ClusterCmd::Init { only_node, config } => {
                cluster_init(config, only_node, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Status { config } => {
                cluster_status(config, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Recover { target, config } => {
                cluster_recover(config, target, cli.socket.as_deref(), cli.json).await
            }
        },
    }
}

/// Surface a `tonic::Status` from the daemon as a single human-readable
/// line. `Status`'s `Display` impl includes the full headers + metadata
/// debug dump, which buries the actual server-side error string in
/// quoted-and-escaped noise (e.g. `status: Internal, message:
/// "recovery_1st_stage: ... \"refusing to basebackup while postgres is
/// running\" ...", details: [], metadata: MetadataMap { headers:
/// {"content-type": ...} }`). The message field alone is what an
/// operator actually wants to read.
fn rpc_failed(rpc: &str, s: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("{rpc} RPC failed: {}", s.message())
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
        .map_err(|s| rpc_failed("ClusterInit", s))?
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

async fn cluster_status(
    config_path: PathBuf,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::ClusterStatusRequest;

    // Thin dialer: the daemon owns the fan-out, the PeerPool, the cert
    // material. The CLI doesn't need TLS material on disk — just the
    // socket. See SPEC §13.
    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .cluster_status(ClusterStatusRequest {})
        .await
        .map_err(|s| rpc_failed("ClusterStatus", s))?
        .into_inner();

    let rows: Vec<StatusRow> = resp.nodes.into_iter().map(StatusRow::from_proto).collect();

    if json {
        let payload = serde_json::json!({
            "all_reachable": resp.all_reachable,
            "nodes": rows.iter().map(status_row_to_json).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_status_table(&rows, &mut std::io::stdout())?;
    }

    if resp.all_reachable {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

/// `cluster recover --target <id>` — ask the local daemon to drive a
/// `recovery_1st_stage` against the named pool member. The daemon is
/// the only side that needs PCP credentials / mTLS material; the CLI
/// is a thin dialer. Same shape as `cluster init` / `cluster status`.
async fn cluster_recover(
    config_path: PathBuf,
    target: i32,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::ClusterRecoverRequest;

    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .cluster_recover(ClusterRecoverRequest {
            target_node_id: target,
        })
        .await
        .map_err(|s| rpc_failed("ClusterRecover", s))?
        .into_inner();

    if json {
        let payload = serde_json::json!({
            "ok":      resp.ok,
            "message": resp.message,
            "target":  target,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if resp.ok {
        if resp.message.is_empty() {
            println!("OK (node {target})");
        } else {
            println!("OK: {}", resp.message);
        }
    } else {
        eprintln!("cluster recover: {}", resp.message);
    }

    if resp.ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

#[derive(Debug)]
struct StatusRow {
    id: i32,
    hostname: String,
    result: Result<pg_agent_proto::pgagentpb::NodeStatus, String>,
}

impl StatusRow {
    fn from_proto(e: pg_agent_proto::pgagentpb::ClusterStatusEntry) -> Self {
        let result = if e.reachable {
            // `status` should always be Some when `reachable` is true; if
            // a future daemon version sends an inconsistent message, fail
            // closed by treating it as unreachable.
            match e.status {
                Some(s) => Ok(s),
                None => Err("daemon: reachable=true but status missing".into()),
            }
        } else {
            Err(e.error)
        };
        Self {
            id: e.node_id,
            hostname: e.hostname,
            result,
        }
    }
}

fn print_status_table(rows: &[StatusRow], w: &mut dyn std::io::Write) -> std::io::Result<()> {
    // Fixed columns; widths grow with content. Eight columns + a final
    // optional "ERR" trailer that only appears on the unreachable
    // summary line below the table.
    let headers = [
        "ID",
        "HOSTNAME",
        "ROLE",
        "PG",
        "PGPOOL",
        "READY",
        "LAG",
        "REPL_STATE",
    ];

    let mut cells: Vec<[String; 8]> = Vec::with_capacity(rows.len());
    for r in rows {
        cells.push(match &r.result {
            Ok(s) => format_status_cells(r.id, &r.hostname, s),
            Err(_) => format_unreachable_cells(r.id, &r.hostname),
        });
    }

    let mut widths = headers.map(|h| h.len());
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.len());
        }
    }

    fn write_row(
        w: &mut dyn std::io::Write,
        row: &[String; 8],
        widths: &[usize; 8],
    ) -> std::io::Result<()> {
        for (i, c) in row.iter().enumerate() {
            if i > 0 {
                write!(w, "  ")?;
            }
            // Right-align the numeric ID + LAG columns; left-align the rest.
            if i == 0 || i == 6 {
                write!(w, "{:>width$}", c, width = widths[i])?;
            } else {
                write!(w, "{:<width$}", c, width = widths[i])?;
            }
        }
        writeln!(w)
    }

    let header_row: [String; 8] = headers.map(String::from);
    write_row(w, &header_row, &widths)?;
    for row in &cells {
        write_row(w, row, &widths)?;
    }

    let unreachable: Vec<&StatusRow> = rows.iter().filter(|r| r.result.is_err()).collect();
    if !unreachable.is_empty() {
        writeln!(w)?;
        writeln!(w, "unreachable nodes:")?;
        for r in &unreachable {
            let err = r.result.as_ref().err().unwrap();
            writeln!(w, "  {} {}: {}", r.id, r.hostname, err)?;
        }
    }
    Ok(())
}

fn format_status_cells(
    id: i32,
    hostname: &str,
    s: &pg_agent_proto::pgagentpb::NodeStatus,
) -> [String; 8] {
    // `is_in_recovery` is only meaningful when we actually reached
    // postgres. When PG is stopped or the systemd probe failed, the
    // proto field is the agent's default `false` — printing "primary"
    // for those would mask a down node as a real primary (and on a
    // split-brain triage screen that's the worst possible default).
    let pg_known = s.is_postgres_status_ok && s.is_postgres_running;
    let role = if !pg_known {
        "unknown"
    } else if s.is_in_recovery {
        "standby"
    } else {
        "primary"
    };
    let pg = service_state(s.is_postgres_running, s.is_postgres_status_ok);
    let pgpool = service_state(s.is_pgpool_running, s.is_pgpool_status_ok);
    let ready = if s.is_ready { "yes" } else { "no" };
    let lag = if pg_known && s.is_in_recovery {
        format_lag_bytes(s.replication_lag_bytes)
    } else {
        "-".into()
    };
    let repl_state = if pg_known && s.is_in_recovery {
        if s.replication_state.is_empty() {
            "unknown".into()
        } else {
            s.replication_state.clone()
        }
    } else {
        "-".into()
    };
    [
        id.to_string(),
        hostname.to_string(),
        role.into(),
        pg.into(),
        pgpool.into(),
        ready.into(),
        lag,
        repl_state,
    ]
}

fn format_unreachable_cells(id: i32, hostname: &str) -> [String; 8] {
    [
        id.to_string(),
        hostname.to_string(),
        "—".into(),
        "—".into(),
        "—".into(),
        "—".into(),
        "—".into(),
        "—".into(),
    ]
}

/// PG/pgpool service-state cell value. Mirrors `pg_agentc status`'s
/// `service_state` so operators see the same vocabulary in both tools.
fn service_state(running: bool, status_ok: bool) -> &'static str {
    if !status_ok {
        "unknown"
    } else if running {
        "running"
    } else {
        "stopped"
    }
}

/// Human-readable byte count: "0", "512 B", "1.2 KiB", "3.4 MiB", "1.1 GiB".
/// Negative inputs are treated as 0 — the proto field is i64 but bytes
/// behind a primary's flush LSN can't actually go negative.
fn format_lag_bytes(n: i64) -> String {
    if n <= 0 {
        return "0".into();
    }
    let n = n as f64;
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    if n < KIB {
        format!("{n:.0} B")
    } else if n < MIB {
        format!("{:.1} KiB", n / KIB)
    } else if n < GIB {
        format!("{:.1} MiB", n / MIB)
    } else {
        format!("{:.2} GiB", n / GIB)
    }
}

fn status_row_to_json(r: &StatusRow) -> serde_json::Value {
    match &r.result {
        Ok(s) => serde_json::json!({
            "id":        r.id,
            "hostname":  r.hostname,
            "reachable": true,
            "error":     serde_json::Value::Null,
            "status": {
                "is_running":                  s.is_running,
                "is_in_recovery":              s.is_in_recovery,
                "is_ready":                    s.is_ready,
                "replication_lag_bytes":       s.replication_lag_bytes,
                "replication_state":           s.replication_state,
                "is_postgres_running":         s.is_postgres_running,
                "is_postgres_status_ok":       s.is_postgres_status_ok,
                "is_pgpool_running":           s.is_pgpool_running,
                "is_pgpool_status_ok":         s.is_pgpool_status_ok,
            },
        }),
        Err(e) => serde_json::json!({
            "id":        r.id,
            "hostname":  r.hostname,
            "reachable": false,
            "error":     e,
            "status":    serde_json::Value::Null,
        }),
    }
}

async fn gen_pgpool(
    config_path: PathBuf,
    write: Option<PathBuf>,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::GetPgpoolBackendsRequest;

    // Thin dialer: the daemon owns the fan-out, the PeerPool, the cert
    // material. The CLI doesn't need TLS material on disk — just the
    // socket. Same shape as `cluster status`.
    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .get_pgpool_backends(GetPgpoolBackendsRequest {})
        .await
        .map_err(|s| rpc_failed("GetPgpoolBackends", s))?
        .into_inner();

    // Refuse to render a partial pgpool.conf. The daemon reports per-
    // node so we can list every unreachable peer in the error, but a
    // missing backend row would silently mis-size the pool — better to
    // fail loudly and let the operator fix the underlying reachability.
    if !resp.all_reachable {
        let bad: Vec<String> = resp
            .backends
            .iter()
            .filter(|b| !b.reachable)
            .map(|b| format!("{} ({}): {}", b.node_id, b.hostname, b.error))
            .collect();
        anyhow::bail!(
            "refusing to render: {} unreachable node(s):\n  {}",
            bad.len(),
            bad.join("\n  ")
        );
    }

    let rows: Vec<BackendRow> = resp
        .backends
        .iter()
        .map(|b| BackendRow {
            id: b.node_id,
            hostname: b.hostname.clone(),
            port: b.pg_port,
            data_dir: b.pg_data.clone(),
        })
        .collect();

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

async fn maintenance(
    cmd: MaintenanceCmd,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::{
        GetMaintenanceRequest, ListMaintenanceRequest, RetryMaintenanceRequest,
    };

    // The maintenance subcommands never need to load config.toml just
    // to find the socket — operators run them against a known-live
    // daemon, and a missing config file shouldn't block triage.
    let socket = match cli_socket {
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from(pg_agent_core::config::DEFAULT_UNIX_SOCKET),
    };
    let mut client = client::dial_local(&socket).await?;

    match cmd {
        MaintenanceCmd::List { status } => {
            let statuses = status.map(|s| vec![s]).unwrap_or_default();
            let resp = client
                .list_maintenance(ListMaintenanceRequest { statuses })
                .await
                .map_err(|s| rpc_failed("ListMaintenance", s))?
                .into_inner();

            // SPEC §13: skipped files go to stderr.
            for s in &resp.skipped {
                eprintln!("warning: skipped {}: {}", s.path, s.error);
            }

            if json {
                let payload = serde_json::json!({
                    "intents": resp.intents.iter().map(intent_to_json).collect::<Vec<_>>(),
                    "skipped": resp.skipped.iter().map(|s| serde_json::json!({
                        "path": s.path, "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else if resp.intents.is_empty() {
                println!("(no maintenance intents)");
            } else {
                println!(
                    "{:<36}  {:<10}  {:<10}  {:>4}  next_retry_at",
                    "ID", "OP", "STATUS", "TRY"
                );
                for i in &resp.intents {
                    println!(
                        "{:<36}  {:<10}  {:<10}  {:>4}  {}",
                        i.id,
                        i.op,
                        i.status,
                        i.attempts,
                        if i.next_retry_at.is_empty() {
                            "-"
                        } else {
                            &i.next_retry_at
                        }
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        MaintenanceCmd::Show { id } => {
            let resp = client
                .get_maintenance(GetMaintenanceRequest { id: id.clone() })
                .await
                .map_err(|s| rpc_failed(&format!("GetMaintenance({id})"), s))?
                .into_inner();
            if json {
                println!("{}", serde_json::to_string_pretty(&intent_to_json(&resp))?);
            } else {
                print_intent_human(&resp);
            }
            Ok(ExitCode::SUCCESS)
        }
        MaintenanceCmd::Retry { id } => {
            let resp = client
                .retry_maintenance(RetryMaintenanceRequest { id: id.clone() })
                .await
                .map_err(|s| rpc_failed(&format!("RetryMaintenance({id})"), s))?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"ok": resp.ok, "message": resp.message})
                    )?
                );
            } else if !resp.message.is_empty() {
                println!("{}", resp.message);
            }
            if resp.ok {
                Ok(ExitCode::SUCCESS)
            } else {
                Ok(ExitCode::FAILURE)
            }
        }
    }
}

fn intent_to_json(i: &pg_agent_proto::pgagentpb::MaintenanceIntent) -> serde_json::Value {
    // Try to decode the payload as JSON for the structured view; fall
    // back to a base64 string if it isn't valid UTF-8/JSON.
    let payload = match std::str::from_utf8(&i.payload) {
        Ok(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => v,
            Err(_) => serde_json::Value::String(s.to_string()),
        },
        Err(_) => serde_json::Value::String(format!("<{} bytes>", i.payload.len())),
    };
    serde_json::json!({
        "id":            i.id,
        "op":            i.op,
        "status":        i.status,
        "attempts":      i.attempts,
        "last_error":    i.last_error,
        "created_at":    i.created_at,
        "updated_at":    i.updated_at,
        "next_retry_at": i.next_retry_at,
        "payload":       payload,
    })
}

fn print_intent_human(i: &pg_agent_proto::pgagentpb::MaintenanceIntent) {
    println!("id:            {}", i.id);
    println!("op:            {}", i.op);
    println!("status:        {}", i.status);
    println!("attempts:      {}", i.attempts);
    println!("created_at:    {}", i.created_at);
    println!("updated_at:    {}", i.updated_at);
    if !i.next_retry_at.is_empty() {
        println!("next_retry_at: {}", i.next_retry_at);
    }
    if !i.last_error.is_empty() {
        println!("last_error:    {}", i.last_error);
    }
    println!("payload:");
    match std::str::from_utf8(&i.payload) {
        Ok(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => println!(
                "{}",
                serde_json::to_string_pretty(&v).unwrap_or_else(|_| s.to_string())
            ),
            Err(_) => println!("{s}"),
        },
        Err(_) => println!("  <{} non-UTF8 bytes>", i.payload.len()),
    }
}

/// Parse `pgpool.conf` enough to extract single-quoted directive values.
///
/// Matches lines of the form `key = 'value'`, ignoring `#` comments
/// (the SPEC §13 contract). Last-write-wins on duplicate keys, matching
/// pgpool's own resolution.
fn parse_pgpool_conf(text: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for raw in text.lines() {
        // Trim leading whitespace and skip comments.
        let line = raw.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Strip trailing `# comment` (best-effort — pgpool's own parser
        // is more lenient, but this is fine for the canonical lines we
        // care about, which never embed `#` in the quoted value).
        let line = match line.find('#') {
            Some(i) => &line[..i],
            None => line,
        };
        let Some(eq) = line.find('=') else {
            continue;
        };
        let key = line[..eq].trim().to_string();
        let value_raw = line[eq + 1..].trim();
        // Single-quoted value: strip exactly one leading and one
        // trailing quote. Otherwise treat the whole rest as the value.
        let value =
            if value_raw.starts_with('\'') && value_raw.ends_with('\'') && value_raw.len() >= 2 {
                value_raw[1..value_raw.len() - 1].to_string()
            } else {
                value_raw.to_string()
            };
        out.insert(key, value);
    }
    out
}

fn check_hooks(path: &std::path::Path, json: bool) -> anyhow::Result<ExitCode> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let parsed = parse_pgpool_conf(&text);

    #[derive(Debug)]
    struct Row {
        key: &'static str,
        want: String,
        got: Option<String>,
        ok: bool,
    }

    let rows: Vec<Row> = hookspec::pgpool_hooks()
        .into_iter()
        .map(|h| {
            let got = parsed.get(h.key).cloned();
            let ok = got.as_deref() == Some(h.value.as_str());
            Row {
                key: h.key,
                want: h.value,
                got,
                ok,
            }
        })
        .collect();

    let all_ok = rows.iter().all(|r| r.ok);

    if json {
        let payload = serde_json::json!({
            "ok":   all_ok,
            "path": path.display().to_string(),
            "rows": rows.iter().map(|r| serde_json::json!({
                "key":  r.key,
                "want": r.want,
                "got":  r.got,
                "ok":   r.ok,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        let name_width = rows.iter().map(|r| r.key.len()).max().unwrap_or(0);
        for r in &rows {
            let tag = if r.ok { "OK " } else { "ERR" };
            match (&r.got, r.ok) {
                (Some(_), true) => {
                    println!("{tag}  {:<width$}", r.key, width = name_width)
                }
                (Some(got), false) => println!(
                    "{tag}  {:<width$}  got {got:?}, want {want:?}",
                    r.key,
                    got = got,
                    want = r.want,
                    width = name_width
                ),
                (None, _) => println!(
                    "{tag}  {:<width$}  missing (want {want:?})",
                    r.key,
                    want = r.want,
                    width = name_width
                ),
            }
        }
        println!();
        if all_ok {
            println!("check-hooks: all directives match");
        } else {
            println!("check-hooks: drift detected");
        }
    }

    if all_ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
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
    fn parse_pgpool_conf_extracts_quoted_values() {
        let s = "\
# leading comment
failover_command = 'pg_agentc failover %d %h %p %P %r %R'
# blank line below

backend_port0 = 5432  # trailing comment
quoted_with_spaces = '  spaces inside  '
        ";
        let m = parse_pgpool_conf(s);
        assert_eq!(
            m.get("failover_command").map(String::as_str),
            Some("pg_agentc failover %d %h %p %P %r %R")
        );
        assert_eq!(m.get("backend_port0").map(String::as_str), Some("5432"));
        assert_eq!(
            m.get("quoted_with_spaces").map(String::as_str),
            Some("  spaces inside  ")
        );
    }

    #[test]
    fn check_hooks_reports_drift_when_value_mismatched() {
        // Build a pgpool.conf where every directive is present except
        // failover_command, which has been munged. Expect non-zero exit.
        let mut text = String::new();
        for h in hookspec::pgpool_hooks() {
            if h.key == "failover_command" {
                text.push_str("failover_command = 'WRONG'\n");
            } else {
                text.push_str(&format!("{} = '{}'\n", h.key, h.value));
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("pgpool.conf");
        std::fs::write(&p, text).unwrap();
        let code = check_hooks(&p, false).unwrap();
        assert_eq!(
            format!("{code:?}"),
            format!("{:?}", ExitCode::FAILURE),
            "expected FAILURE exit"
        );
    }

    #[test]
    fn check_hooks_succeeds_on_canonical_block() {
        let mut text = String::new();
        for h in hookspec::pgpool_hooks() {
            text.push_str(&format!("{} = '{}'\n", h.key, h.value));
        }
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("pgpool.conf");
        std::fs::write(&p, text).unwrap();
        let code = check_hooks(&p, false).unwrap();
        assert_eq!(
            format!("{code:?}"),
            format!("{:?}", ExitCode::SUCCESS),
            "expected SUCCESS exit"
        );
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

    fn ns(
        in_recovery: bool,
        running: bool,
        ready: bool,
        lag: i64,
        repl_state: &str,
    ) -> pg_agent_proto::pgagentpb::NodeStatus {
        pg_agent_proto::pgagentpb::NodeStatus {
            is_running: running,
            is_in_recovery: in_recovery,
            is_ready: ready,
            replication_lag_bytes: lag,
            replication_state: repl_state.into(),
            is_postgres_running: running,
            is_pgpool_running: running,
            is_postgres_status_ok: true,
            is_pgpool_status_ok: true,
            timeline_id: 0,
        }
    }

    #[test]
    fn format_lag_bytes_renders_unit_scale() {
        assert_eq!(format_lag_bytes(0), "0");
        assert_eq!(format_lag_bytes(-1), "0");
        assert_eq!(format_lag_bytes(512), "512 B");
        assert_eq!(format_lag_bytes(2048), "2.0 KiB");
        assert_eq!(format_lag_bytes(5 * 1024 * 1024), "5.0 MiB");
        assert_eq!(format_lag_bytes(2 * 1024 * 1024 * 1024), "2.00 GiB");
    }

    #[test]
    fn service_state_words() {
        assert_eq!(service_state(true, true), "running");
        assert_eq!(service_state(false, true), "stopped");
        assert_eq!(service_state(true, false), "unknown");
    }

    #[test]
    fn print_status_table_renders_mixed_reachability() {
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0.local".into(),
                result: Ok(ns(false, true, true, 0, "")),
            },
            StatusRow {
                id: 1,
                hostname: "pg1.local".into(),
                result: Ok(ns(true, true, true, 1536, "streaming")),
            },
            StatusRow {
                id: 2,
                hostname: "pg2.local".into(),
                result: Err("dial peer: connect refused".into()),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("HOSTNAME"));
        assert!(s.contains("primary"));
        assert!(s.contains("standby"));
        assert!(s.contains("streaming"));
        assert!(s.contains("1.5 KiB"));
        assert!(s.contains("unreachable nodes:"));
        assert!(s.contains("connect refused"));
    }

    #[test]
    fn format_status_cells_role_is_unknown_when_pg_is_down() {
        // Reachable peer, systemd probe succeeded, postgres NOT running.
        // The `is_in_recovery` field is the agent's default `false`
        // (the local query couldn't run), so naive role inference would
        // say "primary". Must show "unknown" instead — otherwise a
        // split-brain triage screen counts the down node as a primary.
        let status = pg_agent_proto::pgagentpb::NodeStatus {
            is_running: false,
            is_in_recovery: false,
            is_ready: false,
            replication_lag_bytes: 0,
            replication_state: String::new(),
            is_postgres_running: false,
            is_pgpool_running: true,
            is_postgres_status_ok: true,
            is_pgpool_status_ok: true,
            timeline_id: 0,
        };
        let cells = format_status_cells(2, "pg2.local", &status);
        assert_eq!(cells[2], "unknown", "role must not default to primary");
        assert_eq!(cells[3], "stopped", "pg cell still reports stopped");
        assert_eq!(cells[5], "no", "ready=no");
        assert_eq!(cells[6], "-", "lag is `-` when role is unknown");
        assert_eq!(cells[7], "-", "repl_state is `-` when role is unknown");
    }

    #[test]
    fn format_status_cells_role_is_unknown_when_pg_status_probe_failed() {
        // systemd probe itself failed (is_postgres_status_ok=false).
        // We can't claim the node is "stopped" either — but we definitely
        // can't claim it's a primary.
        let status = pg_agent_proto::pgagentpb::NodeStatus {
            is_running: false,
            is_in_recovery: false,
            is_ready: false,
            replication_lag_bytes: 0,
            replication_state: String::new(),
            is_postgres_running: false,
            is_pgpool_running: false,
            is_postgres_status_ok: false,
            is_pgpool_status_ok: false,
            timeline_id: 0,
        };
        let cells = format_status_cells(3, "pg3.local", &status);
        assert_eq!(cells[2], "unknown");
        assert_eq!(cells[3], "unknown");
        assert_eq!(cells[4], "unknown");
    }

    #[test]
    fn status_row_to_json_shape_primary_and_unreachable() {
        let primary = StatusRow {
            id: 0,
            hostname: "pg0".into(),
            result: Ok(ns(false, true, true, 0, "")),
        };
        let down = StatusRow {
            id: 2,
            hostname: "pg2".into(),
            result: Err("dial peer: refused".into()),
        };

        let j0 = status_row_to_json(&primary);
        assert_eq!(j0["reachable"], serde_json::Value::Bool(true));
        assert_eq!(j0["error"], serde_json::Value::Null);
        assert_eq!(
            j0["status"]["is_in_recovery"],
            serde_json::Value::Bool(false)
        );

        let j2 = status_row_to_json(&down);
        assert_eq!(j2["reachable"], serde_json::Value::Bool(false));
        assert_eq!(j2["status"], serde_json::Value::Null);
        assert_eq!(
            j2["error"],
            serde_json::Value::String("dial peer: refused".into())
        );
    }
}
