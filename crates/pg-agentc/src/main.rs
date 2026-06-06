//! pg_agentc — pgpool-II hook client. See SPEC §15.
//!
//! Hook dispatch is keyed off the binary name when invoked via a $PGDATA
//! symlink (`recovery_1st_stage`, `pgpool_remote_start`) and otherwise off
//! `argv[1]`. Carries no config; reads only `PG_AGENTD_SOCKET` and
//! `PG_AGENTC_TIMEOUT` from the environment.

use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

const DEFAULT_SOCKET: &str = "/run/pg_agentd/pg_agentd.sock";
const SOCKET_ENV: &str = "PG_AGENTD_SOCKET";
const TIMEOUT_ENV: &str = "PG_AGENTC_TIMEOUT";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let argv0_base = Path::new(&argv[0])
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    // Determine hook name + positional args. Symlink invocation
    // (recovery_1st_stage / pgpool_remote_start) has the hook name as
    // argv[0]'s basename; args start at argv[1]. All other invocations
    // have the hook name as argv[1] and args at argv[2..].
    let (hook, _args) = if argv0_base == pg_agent_hookspec::HOOK_RECOVERY_1ST_STAGE
        || argv0_base == pg_agent_hookspec::HOOK_PGPOOL_REMOTE_START
    {
        (argv0_base.as_str(), &argv[1..])
    } else if argv.len() >= 2 {
        (argv[1].as_str(), &argv[2..])
    } else {
        eprintln!("usage: pg_agentc <hook> [args...]  (try 'pg_agentc help')");
        return ExitCode::from(2);
    };

    match hook {
        "help" | "-h" | "--help" => {
            print_help();
            return ExitCode::SUCCESS;
        }
        "version" | "-v" | "--version" => {
            println!("pg_agentc {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        "config" => {
            eprintln!("pg_agentc: 'config' subcommands have moved to pg_agentctl. Try 'pg_agentctl help'.");
            return ExitCode::FAILURE;
        }
        _ => {}
    }

    let _socket = env::var(SOCKET_ENV).unwrap_or_else(|_| DEFAULT_SOCKET.to_string());
    let _timeout = match env::var(TIMEOUT_ENV) {
        Ok(v) => match humantime::parse_duration(&v) {
            Ok(d) if d > Duration::ZERO => d,
            _ => {
                eprintln!("invalid {TIMEOUT_ENV} duration {v:?}");
                return ExitCode::FAILURE;
            }
        },
        Err(_) => DEFAULT_TIMEOUT,
    };

    // TODO(v1):
    //   - Dial unix://{socket} via tonic (Channel with hyper-util's Uri),
    //     no creds.
    //   - Match hook name → parse args via the hookspec schema → build
    //     the corresponding *Request → call PgAgentLocalClient method.
    //   - Treat ok=true as exit 0, ok=false as exit 1 with message to stderr.
    //   - `status` subcommand: GetStatus + human-readable summary.
    eprintln!("pg_agentc: hook {hook:?} not yet implemented (scaffold)");
    ExitCode::from(1)
}

fn print_help() {
    println!(
        r#"pg_agentc — pgpool-II hook client for pg_agent

A thin forwarder: marshals pgpool's positional arguments into a single
gRPC call on the local pg_agentd Unix socket, then exits. No config, no
node resolution. For operator tooling see `pg_agentctl`.

ENV
  PG_AGENTD_SOCKET   socket path (default: {DEFAULT_SOCKET})
  PG_AGENTC_TIMEOUT  RPC timeout (default: 30m)

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
"#
    );
}

// Minimal in-house humantime parser to avoid pulling the full `humantime`
// crate in just for this. Accepts e.g. "30m", "1h30m", "45s".
mod humantime {
    use std::time::Duration;
    pub fn parse_duration(s: &str) -> Result<Duration, &'static str> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty");
        }
        let mut total = Duration::ZERO;
        let mut num = String::new();
        for ch in s.chars() {
            if ch.is_ascii_digit() {
                num.push(ch);
            } else {
                let n: u64 = num.parse().map_err(|_| "bad number")?;
                num.clear();
                let unit = match ch {
                    's' => Duration::from_secs(n),
                    'm' => Duration::from_secs(n * 60),
                    'h' => Duration::from_secs(n * 3600),
                    _ => return Err("unknown unit"),
                };
                total += unit;
            }
        }
        if !num.is_empty() {
            // bare number = seconds
            let n: u64 = num.parse().map_err(|_| "bad number")?;
            total += Duration::from_secs(n);
        }
        Ok(total)
    }
}
