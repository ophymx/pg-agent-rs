//! pg_agentctl — operator CLI. The clap definitions below are the
//! authoritative surface; keep their doc comments good, because they are
//! what `--help` prints and what operators actually read.
//!
//! Four properties are contract, not convention, because automation
//! depends on them: stable exit codes (0 clean / 1 work-to-do or failure
//! / 2 usage), `--json` wherever output would otherwise be parsed, no
//! interactive prompts ever (destructive commands take an explicit flag),
//! and idempotence — re-running a command against an already-correct
//! state is a no-op rather than an error.
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

    /// In-flight state-change ops journal. Currently tracks `cluster
    /// handoff` (more variants coming with switchover, cluster
    /// pause/resume). Operator surface for resuming a crashed
    /// orchestration or marking it abandoned.
    Ops {
        #[command(subcommand)]
        cmd: OpsCmd,
    },

    /// Cluster-wide operations.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
}

#[derive(Debug, Subcommand)]
enum OpsCmd {
    /// List in-flight (or terminal) ops.
    List {
        #[arg(long, value_parser = ["in_progress", "done", "abandoned"])]
        status: Option<String>,
    },
    /// Show one op in full (id, status, phase, payload, timestamps).
    Show { id: String },
    /// Resume a crashed orchestration from its recorded phase. The
    /// daemon verifies the cluster state still matches the recorded
    /// phase before continuing — mismatch → refuses with a structured
    /// "cluster state diverged" message.
    Resume { id: String },
    /// Mark an op terminal-Abandoned. Use when the orchestration is
    /// unrecoverable and you want it out of the way so a fresh
    /// `cluster handoff` (or similar) can begin. Records the reason
    /// in `last_error` for later incident review.
    Abandon {
        id: String,
        #[arg(long, default_value = "")]
        reason: String,
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
        /// Stop PostgreSQL on the target via the peer agent before
        /// running basebackup. Without this flag, a target that still
        /// has PG running is refused — the basebackup safety check
        /// won't wipe a live $PGDATA, and we surface that as a refusal
        /// here rather than dying eight layers deeper inside recovery.
        /// Pass this when you're sure the target's current PG state is
        /// safe to discard (which is the entire point of recloning it).
        #[arg(long)]
        stop_target_pg: bool,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    /// Planned primary handoff. Run from the current primary. Promotes
    /// the target standby (pg_promote on the peer), demotes the local
    /// node to a standby of the new primary, and reattaches in pgpool.
    /// Distinct from `recover` (which rebuilds a broken target FROM the
    /// local primary): handoff REPLACES the local primary WITH the
    /// target. Refuses if target lag exceeds 16 MiB (one WAL segment)
    /// unless `--allow-lag` is set.
    Handoff {
        /// Pool id of the standby to promote.
        #[arg(long)]
        target: i32,
        /// Override the lag pre-check. Without this, the daemon refuses
        /// to promote a target whose replication lag exceeds one WAL
        /// segment. Passing the flag accepts data loss for writes
        /// between the standby's replay LSN and the primary's current
        /// LSN.
        #[arg(long)]
        allow_lag: bool,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    /// EMERGENCY: disarm quorum commit on the current primary
    /// (clears synchronous_standby_names) so commits stop waiting for
    /// a standby ack. Use only when no standby can be brought back
    /// and the business accepts single-copy writes. Journaled (`ops
    /// list` shows the disarm); the agent re-arms automatically the
    /// moment a standby attaches; /healthz shows
    /// sync_commit=disarmed until then.
    AllowAsync {
        /// Required: acknowledge that acknowledged writes become
        /// single-copy promises until a standby attaches.
        #[arg(long)]
        confirm: bool,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    /// Suspend AUTOMATIC role decisions cluster-wide for planned work.
    /// The loop keeps observing and logging; it stops acting — no
    /// takeover, no fence, no re-point — on every member, until
    /// `cluster resume`. Replicated through consensus, so it survives
    /// agent restarts.
    ///
    /// This does NOT stop PostgreSQL or touch replication, and it does
    /// not protect a primary that dies while paused: nothing will
    /// promote in its place until you resume. `cluster status` shows
    /// the pause.
    Pause {
        /// Why — recorded in consensus and shown by `cluster status`.
        /// Required: the next person to find a cluster that is not
        /// failing over needs to know whether that was deliberate.
        #[arg(long)]
        reason: String,
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
    /// Resume automatic role decisions after `cluster pause`.
    Resume {
        #[arg(long, default_value = pg_agent_core::config::DEFAULT_CONFIG_FILE)]
        config: std::path::PathBuf,
    },
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
        Cmd::Ops { cmd } => ops(cmd, cli.socket.as_deref(), cli.json).await,
        Cmd::Cluster { cmd } => match cmd {
            ClusterCmd::Init { only_node, config } => {
                cluster_init(config, only_node, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Status { config } => {
                cluster_status(config, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Recover {
                target,
                stop_target_pg,
                config,
            } => {
                cluster_recover(
                    config,
                    target,
                    stop_target_pg,
                    cli.socket.as_deref(),
                    cli.json,
                )
                .await
            }
            ClusterCmd::Handoff {
                target,
                allow_lag,
                config,
            } => cluster_handoff(config, target, allow_lag, cli.socket.as_deref(), cli.json).await,
            ClusterCmd::AllowAsync { confirm, config } => {
                cluster_allow_async(config, confirm, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Pause { reason, config } => {
                cluster_set_pause(config, true, reason, cli.socket.as_deref(), cli.json).await
            }
            ClusterCmd::Resume { config } => {
                cluster_set_pause(
                    config,
                    false,
                    String::new(),
                    cli.socket.as_deref(),
                    cli.json,
                )
                .await
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
    // socket.
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
            "pause_status": resp.pause_status,
            "nodes": rows.iter().map(status_row_to_json).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        // Above the table, not below it: a paused cluster explains
        // everything else on the screen.
        if !resp.pause_status.is_empty() {
            println!("maintenance mode: {}", resp.pause_status);
        }
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
    stop_target_pg: bool,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::ClusterRecoverRequest;

    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .cluster_recover(ClusterRecoverRequest {
            target_node_id: target,
            stop_target_pg,
        })
        .await
        .map_err(|s| rpc_failed("ClusterRecover", s))?
        .into_inner();

    if json {
        let payload = serde_json::json!({
            "ok":             resp.ok,
            "message":        resp.message,
            "target":         target,
            "stop_target_pg": stop_target_pg,
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

/// `cluster handoff --target <id>` — planned primary handoff. The
/// daemon resolves the local node as the current primary and refuses
/// if it's a standby. Same wire shape as `cluster recover` — JSON or
/// human-readable output, exit code reflects `resp.ok`.
/// `cluster pause` / `cluster resume` — one RPC, two verbs, because
/// they are the same replicated flag and splitting them into separate
/// handlers would let the two drift.
async fn cluster_set_pause(
    config_path: PathBuf,
    paused: bool,
    reason: String,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::SetPauseRequest;

    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .set_pause(SetPauseRequest { paused, reason })
        .await
        .map_err(|s| rpc_failed("SetPause", s))?
        .into_inner();
    if json {
        let payload = serde_json::json!({ "ok": resp.ok, "message": resp.message });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if resp.ok {
        println!("OK: {}", resp.message);
    } else {
        eprintln!(
            "cluster {}: {}",
            if paused { "pause" } else { "resume" },
            resp.message
        );
    }
    if resp.ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

async fn cluster_allow_async(
    config_path: PathBuf,
    confirm: bool,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::AllowAsyncRequest;

    if !confirm {
        eprintln!(
            "allow-async disarms quorum commit: until a standby attaches, every \
             acknowledged write exists on ONE node only and dies with it. \
             Re-run with --confirm to proceed."
        );
        return Ok(ExitCode::FAILURE);
    }
    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .allow_async(AllowAsyncRequest {})
        .await
        .map_err(|s| rpc_failed("AllowAsync", s))?
        .into_inner();
    if json {
        let payload = serde_json::json!({ "ok": resp.ok, "message": resp.message });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if resp.ok {
        println!("OK: {}", resp.message);
    } else {
        eprintln!("cluster allow-async: {}", resp.message);
    }
    if resp.ok {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

async fn cluster_handoff(
    config_path: PathBuf,
    target: i32,
    allow_lag: bool,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::ClusterHandoffRequest;

    let socket = config_loader::resolve_socket_path(cli_socket, &config_path)?;
    let mut client = client::dial_local(&socket).await?;
    let resp = client
        .cluster_handoff(ClusterHandoffRequest {
            target_node_id: target,
            allow_lag,
        })
        .await
        .map_err(|s| rpc_failed("ClusterHandoff", s))?
        .into_inner();

    if json {
        let payload = serde_json::json!({
            "ok":        resp.ok,
            "message":   resp.message,
            "target":    target,
            "allow_lag": allow_lag,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else if resp.ok {
        if resp.message.is_empty() {
            println!("OK (handed off to node {target})");
        } else {
            println!("OK: {}", resp.message);
        }
    } else {
        eprintln!("cluster handoff: {}", resp.message);
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
    // Fixed columns; widths grow with content. The optional LSN column
    // appears only when at least one reachable row reports a primary —
    // hides the column on healthy single-primary clusters where it
    // would always be unique to one row.
    let primaries: Vec<&StatusRow> = rows
        .iter()
        .filter(|r| {
            matches!(&r.result, Ok(s)
                if s.is_postgres_status_ok && s.is_postgres_running && !s.is_in_recovery)
        })
        .collect();
    let show_lsn = !primaries.is_empty();
    // Identify the lead primary by (timeline_id, current_wal_lsn) lex
    // order, but only when 2+ primaries are present (a single primary
    // is unambiguous; no marker needed). Primaries with both fields=0
    // can't be compared — fall back to no marker rather than marking
    // an arbitrary winner.
    let lead_id: Option<i32> = if primaries.len() >= 2 {
        primaries
            .iter()
            .filter_map(|r| {
                r.result.as_ref().ok().and_then(|s| {
                    if s.timeline_id == 0 && s.current_wal_lsn == 0 {
                        None
                    } else {
                        Some((s.timeline_id, s.current_wal_lsn, r.id))
                    }
                })
            })
            .max_by_key(|(tl, lsn, _)| (*tl, *lsn))
            .map(|(_, _, id)| id)
    } else {
        None
    };

    let mut headers: Vec<&str> = vec![
        "ID",
        "HOSTNAME",
        "ROLE",
        "PG",
        "PGPOOL",
        "READY",
        "LAG",
        "REPL_STATE",
    ];
    if show_lsn {
        headers.push("LSN");
    }

    let mut cells: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for r in rows {
        let mut row = match &r.result {
            Ok(s) => format_status_cells(r.id, &r.hostname, s).to_vec(),
            Err(_) => format_unreachable_cells(r.id, &r.hostname).to_vec(),
        };
        // Annotate the lead's ROLE cell with `*`.
        if Some(r.id) == lead_id {
            row[2] = format!("{}*", row[2]);
        }
        if show_lsn {
            row.push(match &r.result {
                Ok(s)
                    if s.is_postgres_status_ok
                        && s.is_postgres_running
                        && s.current_wal_lsn != 0 =>
                {
                    format_lsn(s.current_wal_lsn)
                }
                Ok(_) => "-".into(),
                Err(_) => "—".into(),
            });
        }
        cells.push(row);
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.len());
        }
    }

    fn write_row(
        w: &mut dyn std::io::Write,
        row: &[String],
        widths: &[usize],
    ) -> std::io::Result<()> {
        for (i, c) in row.iter().enumerate() {
            if i > 0 {
                write!(w, "  ")?;
            }
            // Right-align ID and LAG (numeric); left-align the rest.
            if i == 0 || i == 6 {
                write!(w, "{:>width$}", c, width = widths[i])?;
            } else {
                write!(w, "{:<width$}", c, width = widths[i])?;
            }
        }
        writeln!(w)
    }

    let header_row: Vec<String> = headers.iter().map(|h| (*h).to_string()).collect();
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
    if lead_id.is_some() {
        writeln!(w)?;
        writeln!(
            w,
            "* lead primary by (timeline, LSN) — recover other primaries to this one"
        )?;
    }
    Ok(())
}

/// Render a 64-bit `pg_lsn` as PostgreSQL's canonical `XXXXXXXX/XXXXXXXX`
/// hex form. Matches what `psql -c "SELECT pg_current_wal_lsn()"` prints,
/// so an operator can grep+compare without translation.
fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
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
    const KEY_WIDTH: usize = 26;
    s.push_str("# --- canonical hook block (agent-led failover) ---\n");
    s.push_str("# failover_command is a notify-only poke: the HA loop decides.\n");
    s.push_str("# follow_primary_command MUST stay empty (hook-contract §2).\n");
    for h in hookspec::pgpool_hooks() {
        let _ = writeln!(s, "{:<width$} = '{}'", h.key, h.value, width = KEY_WIDTH);
    }
    s.push_str("\n# --- contract settings (decision-critical, not tuning) ---\n");
    for h in hookspec::pgpool_settings() {
        let _ = writeln!(s, "{:<width$} = {}", h.key, h.value, width = KEY_WIDTH);
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

            // Skipped files go to stderr so stdout stays parseable.
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

async fn ops(
    cmd: OpsCmd,
    cli_socket: Option<&std::path::Path>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    use pg_agent_proto::pgagentpb::{
        AbandonInflightOpRequest, GetInflightOpRequest, ListInflightOpsRequest,
        ResumeInflightOpRequest,
    };

    // Same operator-friendliness as `maintenance`: don't require a
    // valid config.toml for ops triage — the daemon's socket is the
    // only thing we need.
    let socket = match cli_socket {
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from(pg_agent_core::config::DEFAULT_UNIX_SOCKET),
    };
    let mut client = client::dial_local(&socket).await?;

    match cmd {
        OpsCmd::List { status } => {
            let statuses = status.map(|s| vec![s]).unwrap_or_default();
            let resp = client
                .list_inflight_ops(ListInflightOpsRequest { statuses })
                .await
                .map_err(|s| rpc_failed("ListInflightOps", s))?
                .into_inner();

            for s in &resp.skipped {
                eprintln!("warning: skipped {}: {}", s.path, s.error);
            }

            if json {
                let payload = serde_json::json!({
                    "ops": resp.ops.iter().map(inflight_to_json).collect::<Vec<_>>(),
                    "skipped": resp.skipped.iter().map(|s| serde_json::json!({
                        "path": s.path, "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else if resp.ops.is_empty() {
                println!("(no in-flight ops)");
            } else {
                println!(
                    "{:<40}  {:<10}  {:<12}  {:<24}  STARTED",
                    "ID", "OP", "STATUS", "PHASE"
                );
                for o in &resp.ops {
                    println!(
                        "{:<40}  {:<10}  {:<12}  {:<24}  {}",
                        o.id, o.op, o.status, o.phase, o.started_at
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        OpsCmd::Show { id } => {
            let resp = client
                .get_inflight_op(GetInflightOpRequest { id: id.clone() })
                .await
                .map_err(|s| rpc_failed(&format!("GetInflightOp({id})"), s))?
                .into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&inflight_to_json(&resp))?
                );
            } else {
                print_inflight_human(&resp);
            }
            Ok(ExitCode::SUCCESS)
        }
        OpsCmd::Resume { id } => {
            let resp = client
                .resume_inflight_op(ResumeInflightOpRequest { id: id.clone() })
                .await
                .map_err(|s| rpc_failed(&format!("ResumeInflightOp({id})"), s))?
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
        OpsCmd::Abandon { id, reason } => {
            let resp = client
                .abandon_inflight_op(AbandonInflightOpRequest {
                    id: id.clone(),
                    reason,
                })
                .await
                .map_err(|s| rpc_failed(&format!("AbandonInflightOp({id})"), s))?
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

fn inflight_to_json(o: &pg_agent_proto::pgagentpb::InflightOp) -> serde_json::Value {
    let payload = match std::str::from_utf8(&o.payload) {
        Ok(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => v,
            Err(_) => serde_json::Value::String(s.to_string()),
        },
        Err(_) => serde_json::Value::String(format!("<{} bytes>", o.payload.len())),
    };
    serde_json::json!({
        "id":           o.id,
        "op":           o.op,
        "status":       o.status,
        "phase":        o.phase,
        "started_at":   o.started_at,
        "updated_at":   o.updated_at,
        "completed_at": o.completed_at,
        "last_error":   o.last_error,
        "payload":      payload,
    })
}

fn print_inflight_human(o: &pg_agent_proto::pgagentpb::InflightOp) {
    println!("id:           {}", o.id);
    println!("op:           {}", o.op);
    println!("status:       {}", o.status);
    println!("phase:        {}", o.phase);
    println!("started_at:   {}", o.started_at);
    println!("updated_at:   {}", o.updated_at);
    if !o.completed_at.is_empty() {
        println!("completed_at: {}", o.completed_at);
    }
    if !o.last_error.is_empty() {
        println!("last_error:   {}", o.last_error);
    }
    let payload = match std::str::from_utf8(&o.payload) {
        Ok(s) => serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .and_then(|v| serde_json::to_string_pretty(&v).ok())
            .unwrap_or_else(|| s.to_string()),
        Err(_) => format!("<{} bytes>", o.payload.len()),
    };
    println!("payload:");
    for line in payload.lines() {
        println!("  {line}");
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
/// Matches lines of the form `key = 'value'`, ignoring `#` comments.
/// Last-write-wins on duplicate keys, matching pgpool's own resolution.
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

    // Agent-led contract: hooks plus the decision-critical
    // settings (use_watchdog off, detach_false_primary on, ...).
    let expected = {
        let mut e = hookspec::pgpool_hooks();
        e.extend(hookspec::pgpool_settings());
        e
    };
    let rows: Vec<Row> = expected
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
        // Agent-led contract: follow hook empty, watchdog hooks gone,
        // decision-critical settings present.
        assert!(out.contains("follow_primary_command     = ''"));
        assert!(!out.contains("wd_escalation_command"));
        assert!(out.contains("use_watchdog"));
        assert!(out.contains("detach_false_primary"));
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
        for h in hookspec::pgpool_settings() {
            text.push_str(&format!("{} = {}\n", h.key, h.value));
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
        for h in hookspec::pgpool_settings() {
            text.push_str(&format!("{} = {}\n", h.key, h.value));
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
        ns_with(in_recovery, running, ready, lag, repl_state, 0, 0)
    }

    fn ns_with(
        in_recovery: bool,
        running: bool,
        ready: bool,
        lag: i64,
        repl_state: &str,
        timeline_id: i32,
        current_wal_lsn: u64,
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
            timeline_id,
            current_wal_lsn,
            last_flush_lsn: current_wal_lsn,
            peer_primary_seen_age_ms: Default::default(),
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
            current_wal_lsn: 0,
            last_flush_lsn: 0,
            peer_primary_seen_age_ms: Default::default(),
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
            current_wal_lsn: 0,
            last_flush_lsn: 0,
            peer_primary_seen_age_ms: Default::default(),
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

    // ----- LSN column + LEAD marker --------------------------------------

    #[test]
    fn format_lsn_renders_xy_hex() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x1A2B_3C4D), "0/1A2B3C4D");
        assert_eq!(format_lsn(0x1_FFFF_FFFF), "1/FFFFFFFF");
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn print_status_table_shows_lsn_column_when_primary_present() {
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0".into(),
                result: Ok(ns_with(false, true, true, 0, "", 7, 0x1A2B_3C4D)),
            },
            StatusRow {
                id: 1,
                hostname: "pg1".into(),
                result: Ok(ns_with(true, true, true, 0, "streaming", 7, 0)),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("LSN"), "LSN column missing: {s}");
        assert!(s.contains("0/1A2B3C4D"), "primary LSN not rendered: {s}");
        // Single primary → no LEAD marker.
        assert!(!s.contains("lead primary"), "unexpected lead footer: {s}");
        assert!(!s.contains("primary*"), "unexpected lead asterisk: {s}");
    }

    #[test]
    fn print_status_table_omits_lsn_column_when_no_primary() {
        // All rows are standbys → no primary → LSN column hidden.
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0".into(),
                result: Ok(ns(true, true, true, 0, "streaming")),
            },
            StatusRow {
                id: 1,
                hostname: "pg1".into(),
                result: Ok(ns(true, true, true, 100, "streaming")),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        // Note: "standby" rows still get "standby" so we can't grep for
        // "LSN" against the role column; assert there's no LSN header.
        assert!(
            !s.split('\n').next().unwrap().contains("LSN"),
            "LSN column should be hidden when no primary present: {s}"
        );
    }

    #[test]
    fn print_status_table_marks_lead_primary_by_lsn_on_same_tl() {
        // Both nodes are primary on TL=7; pg0 has higher LSN → lead.
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0".into(),
                result: Ok(ns_with(false, true, true, 0, "", 7, 0x2_0000_0000)),
            },
            StatusRow {
                id: 1,
                hostname: "pg1".into(),
                result: Ok(ns_with(false, true, true, 0, "", 7, 0x1_FFFF_FFFF)),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        // The lead is annotated with `*` in its ROLE cell. Find the row
        // containing pg0's hostname and check ROLE has the marker.
        let pg0_line = s
            .lines()
            .find(|l| l.contains("pg0"))
            .expect("pg0 row missing");
        assert!(
            pg0_line.contains("primary*"),
            "pg0 expected to be lead: {pg0_line}"
        );
        let pg1_line = s
            .lines()
            .find(|l| l.contains("pg1"))
            .expect("pg1 row missing");
        // pg1 has plain "primary" — no asterisk.
        assert!(pg1_line.contains("primary"));
        assert!(
            !pg1_line.contains("primary*"),
            "pg1 should not be marked lead: {pg1_line}"
        );
        assert!(s.contains("lead primary by (timeline, LSN)"));
    }

    #[test]
    fn print_status_table_marks_lead_primary_by_higher_timeline() {
        // pg0 is on TL=7 with very high LSN; pg1 is on TL=8 with low
        // LSN. TL takes precedence — pg1 wins.
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0".into(),
                result: Ok(ns_with(false, true, true, 0, "", 7, u64::MAX - 1)),
            },
            StatusRow {
                id: 1,
                hostname: "pg1".into(),
                result: Ok(ns_with(false, true, true, 0, "", 8, 1)),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let pg1_line = s.lines().find(|l| l.contains("pg1")).unwrap();
        assert!(
            pg1_line.contains("primary*"),
            "pg1 (higher TL) expected to be lead: {pg1_line}"
        );
    }

    #[test]
    fn print_status_table_no_lead_when_both_primaries_report_unknown_lsn() {
        // Two primaries, both with TL=0 and LSN=0 (pre-feature peers).
        // The comparison degrades to no-marker rather than arbitrary
        // winner.
        let rows = vec![
            StatusRow {
                id: 0,
                hostname: "pg0".into(),
                result: Ok(ns(false, true, true, 0, "")),
            },
            StatusRow {
                id: 1,
                hostname: "pg1".into(),
                result: Ok(ns(false, true, true, 0, "")),
            },
        ];
        let mut buf = Vec::new();
        print_status_table(&rows, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(
            !s.contains("primary*"),
            "should not pick a lead when both primaries are pre-feature"
        );
        assert!(!s.contains("lead primary"));
    }
}
