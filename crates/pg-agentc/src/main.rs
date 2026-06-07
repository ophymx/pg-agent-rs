//! pg_agentc — pgpool-II hook client for pg_agent. See SPEC §15.
//!
//! Behaviour:
//!
//! - Hook name comes from `argv[0]` basename when invoked via a `$PGDATA`
//!   symlink (`recovery_1st_stage`, `pgpool_remote_start`); otherwise from
//!   `argv[1]`. Positional args start one element later in the symlink case.
//! - Connects to `$PG_AGENTD_SOCKET` (default
//!   `/run/pg_agentd/pg_agentd.sock`) as the `postgres` OS user; the socket
//!   is `0600 postgres:postgres` so filesystem permissions are the auth.
//! - Wraps every RPC in `$PG_AGENTC_TIMEOUT` (default 30 m).
//! - Exits 0 when `OpResult.ok == true`; 1 when the agent returned
//!   `ok = false` (with `message` printed to stderr) or any other error;
//!   2 only for usage errors (unknown hook, bad arg count).

use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use hyper_util::rt::TokioIo;
use pg_agent_hookspec as hookspec;
use pg_agent_proto::pgagentpb as pb;
use pg_agent_proto::pgagentpb::pg_agent_local_client::PgAgentLocalClient;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Response;
use tower::service_fn;

const DEFAULT_SOCKET: &str = "/run/pg_agentd/pg_agentd.sock";
const SOCKET_ENV: &str = "PG_AGENTD_SOCKET";
const TIMEOUT_ENV: &str = "PG_AGENTC_TIMEOUT";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

const STATUS_SUBCOMMAND: &str = "status";

/// Every hook + subcommand the dispatcher recognises. Checked up front so
/// unknown hooks fail with a clear message instead of a "can't connect"
/// error when the daemon isn't reachable.
const KNOWN_HOOKS: &[&str] = &[
    hookspec::HOOK_FAILOVER,
    hookspec::HOOK_FOLLOW_PRIMARY,
    hookspec::HOOK_RECOVERY_1ST_STAGE,
    hookspec::HOOK_PGPOOL_REMOTE_START,
    hookspec::HOOK_ESCALATION,
    hookspec::HOOK_DE_ESCALATION,
    hookspec::HOOK_RESTORE_WAL,
    STATUS_SUBCOMMAND,
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match dispatch().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("pg_agentc: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch() -> Result<ExitCode> {
    let argv: Vec<String> = env::args().collect();
    let argv0 = Path::new(argv.first().map_or("", String::as_str))
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    // Symlink invocation under $PGDATA — argv[0] basename IS the hook name,
    // and pgpool's positional args start at argv[1] (no hook-name prefix).
    let (hook, args): (String, &[String]) = if argv0 == hookspec::HOOK_RECOVERY_1ST_STAGE
        || argv0 == hookspec::HOOK_PGPOOL_REMOTE_START
    {
        (argv0, &argv[1..])
    } else if argv.len() >= 2 {
        (argv[1].clone(), &argv[2..])
    } else {
        print_help();
        return Ok(ExitCode::from(2));
    };

    // Synthetic top-level commands handled before dialing the socket.
    match hook.as_str() {
        "help" | "-h" | "--help" => {
            print_help();
            return Ok(ExitCode::SUCCESS);
        }
        "version" | "-v" | "--version" => {
            println!("pg_agentc {}", env!("CARGO_PKG_VERSION"));
            return Ok(ExitCode::SUCCESS);
        }
        "config" => {
            eprintln!(
                "pg_agentc: 'config' subcommands have moved to pg_agentctl. \
                 Try 'pg_agentctl help'."
            );
            return Ok(ExitCode::FAILURE);
        }
        _ => {}
    }

    // Validate the hook name before dialing — otherwise a typo at the
    // pgpool.conf level (e.g. `pg_agentc failovr ...`) would surface as a
    // misleading transport error if pg_agentd happens to be down.
    if !KNOWN_HOOKS.contains(&hook.as_str()) {
        eprintln!("pg_agentc: unknown hook: {hook:?} (try 'pg_agentc help')");
        return Ok(ExitCode::from(2));
    }

    let socket = env::var(SOCKET_ENV).unwrap_or_else(|_| DEFAULT_SOCKET.to_string());
    let timeout = rpc_timeout()?;

    let channel = dial(&socket)
        .await
        .with_context(|| format!("connect to {socket}"))?;
    let mut client = PgAgentLocalClient::new(channel);

    let exit = match hook.as_str() {
        h if h == hookspec::HOOK_FAILOVER => handle_failover(&mut client, args, timeout).await?,
        h if h == hookspec::HOOK_FOLLOW_PRIMARY => {
            handle_follow_primary(&mut client, args, timeout).await?
        }
        h if h == hookspec::HOOK_RECOVERY_1ST_STAGE => {
            handle_recovery_1st_stage(&mut client, args, timeout).await?
        }
        h if h == hookspec::HOOK_PGPOOL_REMOTE_START => {
            handle_remote_start(&mut client, args, timeout).await?
        }
        h if h == hookspec::HOOK_ESCALATION || h == hookspec::HOOK_DE_ESCALATION => {
            handle_escalation(&mut client, &hook, timeout).await?
        }
        h if h == hookspec::HOOK_RESTORE_WAL => {
            handle_restore_wal(&mut client, args, timeout).await?
        }
        STATUS_SUBCOMMAND => handle_status(&mut client, timeout).await?,
        other => {
            eprintln!("pg_agentc: unknown hook: {other:?} (try 'pg_agentc help')");
            return Ok(ExitCode::from(2));
        }
    };
    Ok(exit)
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// Dial the local agent's Unix socket. tonic's `Channel` doesn't know about
/// Unix sockets natively, so we build it with a custom connector that
/// returns a `TokioIo`-wrapped `UnixStream` on every call (gRPC opens one
/// stream per RPC underneath an HTTP/2 multiplexer).
async fn dial(socket: &str) -> Result<Channel> {
    let path = socket.to_string();
    // The URI here is a placeholder — the connector ignores it. Some
    // string is required because Endpoint::try_from validates it.
    Endpoint::try_from("http://[::]:0")
        .context("build endpoint")?
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move {
                let stream = UnixStream::connect(path).await?;
                Ok::<_, io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .context("dial unix socket")
}

// ---------------------------------------------------------------------------
// Hook handlers
// ---------------------------------------------------------------------------

async fn handle_failover(
    client: &mut PgAgentLocalClient<Channel>,
    args: &[String],
    timeout: Duration,
) -> Result<ExitCode> {
    use hookspec::Token::*;
    let p = parse_args(&hookspec::SCHEMA_FAILOVER, args, hookspec::HOOK_FAILOVER)?;

    let req = pb::FailoverRequest {
        detached: Some(node_ref(
            get(&p, DetachedId),
            get(&p, DetachedHost),
            get(&p, DetachedPort),
            get(&p, DetachedData),
        )?),
        new_main: Some(node_ref(
            get(&p, NewMainId),
            get(&p, NewMainHost),
            get(&p, NewMainPort),
            get(&p, NewMainData),
        )?),
        // %M only carries the old-main id — no host/port/pgdata.
        old_main: Some(node_ref(get(&p, OldMainId), "", "", "")?),
        old_primary: Some(node_ref(
            get(&p, OldPrimaryId),
            get(&p, OldPrimaryHost),
            get(&p, OldPrimaryPort),
            "",
        )?),
    };

    let resp = with_deadline(timeout, client.failover(req)).await?;
    finish(hookspec::HOOK_FAILOVER, resp)
}

async fn handle_follow_primary(
    client: &mut PgAgentLocalClient<Channel>,
    args: &[String],
    timeout: Duration,
) -> Result<ExitCode> {
    use hookspec::Token::*;
    let p = parse_args(
        &hookspec::SCHEMA_FOLLOW_PRIMARY,
        args,
        hookspec::HOOK_FOLLOW_PRIMARY,
    )?;

    // %m/%H are "new main" in failover_command; in follow_primary_command
    // they explicitly mean "new primary" (notes §9). Same token layout,
    // different request slot.
    let req = pb::FollowPrimaryRequest {
        detached: Some(node_ref(
            get(&p, DetachedId),
            get(&p, DetachedHost),
            get(&p, DetachedPort),
            get(&p, DetachedData),
        )?),
        new_primary: Some(node_ref(
            get(&p, NewMainId),
            get(&p, NewMainHost),
            get(&p, NewMainPort),
            get(&p, NewMainData),
        )?),
        old_main: Some(node_ref(get(&p, OldMainId), "", "", "")?),
        old_primary: Some(node_ref(
            get(&p, OldPrimaryId),
            get(&p, OldPrimaryHost),
            get(&p, OldPrimaryPort),
            "",
        )?),
    };

    let resp = with_deadline(timeout, client.follow_primary(req)).await?;
    finish(hookspec::HOOK_FOLLOW_PRIMARY, resp)
}

async fn handle_recovery_1st_stage(
    client: &mut PgAgentLocalClient<Channel>,
    args: &[String],
    timeout: Duration,
) -> Result<ExitCode> {
    use hookspec::FixedArg::*;
    let p = parse_args(
        &hookspec::SCHEMA_RECOVERY,
        args,
        hookspec::HOOK_RECOVERY_1ST_STAGE,
    )?;

    // Primary has no id token in this hook's argv (the script runs on the
    // primary itself); the daemon resolves the local node by hostname.
    let req = pb::RecoveryRequest {
        primary: Some(node_ref(
            "",
            get(&p, PrimaryHost),
            get(&p, PrimaryPort),
            get(&p, PrimaryData),
        )?),
        standby: Some(node_ref(
            get(&p, StandbyId),
            get(&p, StandbyHost),
            get(&p, StandbyPort),
            get(&p, StandbyData),
        )?),
    };

    let resp = with_deadline(timeout, client.recovery_first_stage(req)).await?;
    finish(hookspec::HOOK_RECOVERY_1ST_STAGE, resp)
}

async fn handle_remote_start(
    client: &mut PgAgentLocalClient<Channel>,
    args: &[String],
    timeout: Duration,
) -> Result<ExitCode> {
    use hookspec::FixedArg::*;
    let p = parse_args(
        &hookspec::SCHEMA_REMOTE_START,
        args,
        hookspec::HOOK_PGPOOL_REMOTE_START,
    )?;

    // $2 (RemoteData) is the *primary's* PGDATA per pgpool_recovery semantics
    // (notes §11). It's passed along as informational; the daemon discards
    // it and sources the target's data dir from its own config.
    let req = pb::RemoteStartRequest {
        target: Some(node_ref("", get(&p, RemoteHost), "", get(&p, RemoteData))?),
    };

    let resp = with_deadline(timeout, client.remote_start(req)).await?;
    finish(hookspec::HOOK_PGPOOL_REMOTE_START, resp)
}

async fn handle_escalation(
    client: &mut PgAgentLocalClient<Channel>,
    hook_name: &str,
    timeout: Duration,
) -> Result<ExitCode> {
    // Same RPC for escalation and de_escalation; both are no-ops in the
    // HAProxy deployment (SPEC §5.5). The hook_name is only used for the
    // error/info message.
    let resp = with_deadline(timeout, client.escalation(pb::EscalationRequest {})).await?;
    finish(hook_name, resp)
}

async fn handle_restore_wal(
    client: &mut PgAgentLocalClient<Channel>,
    args: &[String],
    timeout: Duration,
) -> Result<ExitCode> {
    use hookspec::PostgresToken::*;
    let p = parse_args(
        &hookspec::SCHEMA_RESTORE_WAL,
        args,
        hookspec::HOOK_RESTORE_WAL,
    )?;

    let req = pb::RestoreWalRequest {
        wal_file: get(&p, WalFile).to_string(),
        dest_path: get(&p, WalDest).to_string(),
    };

    let resp = with_deadline(timeout, client.restore_wal(req)).await?;
    finish(hookspec::HOOK_RESTORE_WAL, resp)
}

async fn handle_status(
    client: &mut PgAgentLocalClient<Channel>,
    timeout: Duration,
) -> Result<ExitCode> {
    let resp = with_deadline(timeout, client.get_status(pb::GetStatusRequest {})).await?;
    let s = resp.into_inner();

    let role = if s.is_in_recovery {
        "standby"
    } else {
        "primary"
    };
    let ready = if s.is_ready { "yes" } else { "no" };

    println!("role:              {role}");
    println!(
        "postgres:          {}",
        service_state(s.is_postgres_running, s.is_postgres_status_ok)
    );
    println!(
        "pgpool:            {}",
        service_state(s.is_pgpool_running, s.is_pgpool_status_ok)
    );
    println!("ready:             {ready}");
    if s.is_in_recovery {
        let state = if s.replication_state.is_empty() {
            "unknown"
        } else {
            &s.replication_state
        };
        println!("replication_state: {state}");
        println!("lag_bytes:         {}", s.replication_lag_bytes);
    }
    Ok(ExitCode::SUCCESS)
}

fn service_state(running: bool, status_ok: bool) -> &'static str {
    if !status_ok {
        "unknown"
    } else if running {
        "running"
    } else {
        "stopped"
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bridge from `HookSchema::parse`'s typed error into anyhow.
fn parse_args<K>(
    schema: &hookspec::HookSchema<K>,
    args: &[String],
    hook: &'static str,
) -> Result<HashMap<K, String>>
where
    K: Copy + Eq + Hash + 'static,
{
    schema.parse(args, hook).map_err(|e| anyhow!("{e}"))
}

/// Lookup helper — missing keys return `""` so callers can pass the result
/// to [`node_ref`] without unwrap noise. `HookSchema::parse` already
/// guarantees every key is present, so the `unwrap_or("")` branch is
/// defence-in-depth.
fn get<K: Eq + Hash + Copy>(map: &HashMap<K, String>, k: K) -> &str {
    map.get(&k).map(String::as_str).unwrap_or("")
}

/// Build a [`pb::NodeRef`] from pgpool's positional format-string values.
///
/// - Empty `id` string → `-1` sentinel (matches pgpool's `%m = -1`
///   semantics when no failover candidate exists).
/// - Non-empty but non-numeric `id` → hard error. pgpool passed garbled
///   input; refusing prevents incorrect node resolution at the agent.
/// - Empty `port` → 0. Non-numeric `port` → 0 (matches Go behaviour;
///   port is informational, the agent uses the config-sourced value).
fn node_ref(id: &str, host: &str, port: &str, pgdata: &str) -> Result<pb::NodeRef> {
    let id: i32 = if id.is_empty() {
        -1
    } else {
        id.parse()
            .map_err(|e| anyhow!("non-numeric node id from pgpool: {id:?}: {e}"))?
    };
    let pg_port: i32 = if port.is_empty() {
        0
    } else {
        port.parse().unwrap_or(0)
    };
    Ok(pb::NodeRef {
        id,
        hostname: host.to_string(),
        pg_port,
        pg_data: pgdata.to_string(),
    })
}

/// Run an RPC future with a wall-clock deadline. tonic's per-call timeout
/// (`Request::set_timeout`) sends a `grpc-timeout` header to the server but
/// does nothing about a transport-level hang — the local timeout is what
/// guarantees the binary actually exits in bounded time.
async fn with_deadline<F, T>(timeout: Duration, fut: F) -> Result<Response<T>>
where
    F: Future<Output = Result<Response<T>, tonic::Status>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(status)) => Err(anyhow!("rpc error: {status}")),
        Err(_) => Err(anyhow!("rpc timeout after {timeout:?}")),
    }
}

/// Translate `OpResult` into a process exit code, mirroring pgpool's
/// expectations: exit 0 on success (so pgpool considers the hook
/// successful), exit 1 with the message on stderr otherwise.
fn finish(hook: &str, resp: Response<pb::OpResult>) -> Result<ExitCode> {
    let r = resp.into_inner();
    if r.ok {
        if !r.message.is_empty() {
            // Even on success, surface any informational message (e.g.
            // "already processed; skipping duplicate") so operators
            // tailing pgpool logs can see what happened.
            eprintln!("{hook}: {}", r.message);
        }
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("{hook}: {}", r.message);
        Ok(ExitCode::FAILURE)
    }
}

fn rpc_timeout() -> Result<Duration> {
    match env::var(TIMEOUT_ENV) {
        Ok(v) => {
            let d =
                humantime::parse(&v).map_err(|e| anyhow!("invalid {TIMEOUT_ENV} {v:?}: {e}"))?;
            if d.is_zero() {
                bail!("invalid {TIMEOUT_ENV} {v:?}: must be greater than zero");
            }
            Ok(d)
        }
        Err(_) => Ok(DEFAULT_TIMEOUT),
    }
}

fn print_help() {
    println!(
        "\
pg_agentc — pgpool-II hook client for pg_agent.

A thin forwarder: marshals pgpool's positional arguments into a single
gRPC call on the local pg_agentd Unix socket, then exits. Carries no
config, performs no node resolution. For operator tooling see pg_agentctl.

ENV
  PG_AGENTD_SOCKET   socket path (default: {DEFAULT_SOCKET})
  PG_AGENTC_TIMEOUT  RPC timeout, accepts e.g. '30m' '1h30m' '45s'
                     (default: 30m)

HOOKS
  failover <args>              pgpool failover_command
  follow_primary <args>        pgpool follow_primary_command
  escalation                   pgpool wd_escalation_command
  de_escalation                pgpool wd_de_escalation_command
  restore-wal <file> <dest>    postgresql restore_command

  Invoked via $PGDATA symlinks by the pgpool_recovery C extension:
  recovery_1st_stage <args>    recovery_1st_stage_command
  pgpool_remote_start <args>   pgpool_remote_start

OTHER
  status                       query local agent for current node status
  help / version

EXIT CODES
  0   success (OpResult.ok = true)
  1   agent reported failure, or transport / timeout error
  2   usage error (unknown hook, bad arg count)
"
    );
}

// Inline duration parser. Avoids pulling the full `humantime` crate just
// for one knob. Accepts `30s`, `30m`, `1h30m`, and a bare integer
// (interpreted as seconds — matches Go's `time.ParseDuration` permissive
// behaviour for the common case).
mod humantime {
    use std::time::Duration;

    pub fn parse(s: &str) -> Result<Duration, &'static str> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty");
        }
        let mut total = Duration::ZERO;
        let mut num = String::new();
        let mut saw_unit = false;
        for ch in s.chars() {
            if ch.is_ascii_digit() {
                num.push(ch);
            } else {
                if num.is_empty() {
                    return Err("unit without number");
                }
                let n: u64 = num.parse().map_err(|_| "bad number")?;
                num.clear();
                let unit = match ch {
                    's' => Duration::from_secs(n),
                    'm' => Duration::from_secs(n * 60),
                    'h' => Duration::from_secs(n * 3600),
                    _ => return Err("unknown unit"),
                };
                total += unit;
                saw_unit = true;
            }
        }
        if !num.is_empty() {
            // Trailing digits with no unit — treat as bare seconds, but
            // only if nothing else was specified (so "1h30" is rejected).
            if saw_unit {
                return Err("trailing digits without unit");
            }
            let n: u64 = num.parse().map_err(|_| "bad number")?;
            total += Duration::from_secs(n);
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ref_empty_id_yields_sentinel() {
        let r = node_ref("", "server1", "", "").unwrap();
        assert_eq!(r.id, -1);
        assert_eq!(r.hostname, "server1");
        assert_eq!(r.pg_port, 0);
    }

    #[test]
    fn node_ref_parses_numeric_id_and_port() {
        let r = node_ref("2", "server3", "5432", "/var/lib/pg").unwrap();
        assert_eq!(r.id, 2);
        assert_eq!(r.hostname, "server3");
        assert_eq!(r.pg_port, 5432);
        assert_eq!(r.pg_data, "/var/lib/pg");
    }

    #[test]
    fn node_ref_rejects_non_numeric_id() {
        let err = node_ref("not-a-number", "server1", "", "").unwrap_err();
        assert!(err.to_string().contains("non-numeric"));
    }

    #[test]
    fn node_ref_silently_drops_garbage_port() {
        // Matches Go: port is informational; if pgpool sends junk we
        // still complete the dispatch and let the agent use its own
        // config-sourced value.
        let r = node_ref("0", "server1", "xyz", "").unwrap();
        assert_eq!(r.pg_port, 0);
    }

    #[test]
    fn node_ref_negative_id_is_passed_through() {
        // pgpool sends %m = -1 when no failover candidate exists; the
        // sentinel must survive parsing so the agent can detect it.
        let r = node_ref("-1", "", "", "").unwrap();
        assert_eq!(r.id, -1);
    }

    #[test]
    fn humantime_parses_compound() {
        assert_eq!(
            humantime::parse("1h30m").unwrap(),
            Duration::from_secs(90 * 60)
        );
    }

    #[test]
    fn humantime_parses_bare_seconds() {
        assert_eq!(humantime::parse("45").unwrap(), Duration::from_secs(45));
    }

    #[test]
    fn humantime_rejects_garbage() {
        assert!(humantime::parse("xyz").is_err());
        assert!(humantime::parse("1h30").is_err()); // trailing digits w/o unit
        assert!(humantime::parse("m").is_err()); // unit without number
    }

    #[test]
    fn humantime_rejects_empty() {
        assert!(humantime::parse("").is_err());
        assert!(humantime::parse("   ").is_err());
    }

    #[test]
    fn service_state_words() {
        assert_eq!(service_state(true, true), "running");
        assert_eq!(service_state(false, true), "stopped");
        assert_eq!(service_state(true, false), "unknown");
        assert_eq!(service_state(false, false), "unknown");
    }
}
