//! Schemas for every pgpool-II / PostgreSQL hook that `pg_agentc` dispatches
//! and `pg_agentctl` introspects. This is the single source of truth for the
//! positional-argument layouts, format-token tables, and the canonical hook
//! command strings — both consumers cannot drift out of sync.
//!
//! Three token spaces exist:
//!
//! - [`Token`] — pgpool format-string tokens like `%d`, `%h`, `%p`. The
//!   operator sets the format string in `pgpool.conf`; pgpool substitutes
//!   before exec'ing.
//! - [`PostgresToken`] — postgresql.conf format-string tokens like `%f`,
//!   `%p`. PostgreSQL substitutes before exec'ing `restore_command`.
//! - [`FixedArg`] — positional arguments hard-coded in pgpool's C source.
//!   The operator has no knob in pgpool.conf for these.
//!
//! The `%m`/`%H` tokens mean different things in different hooks —
//! "new main" (smallest surviving node id) in `failover_command`, "new
//! primary" in `follow_primary_command` — so agent code verifies role
//! with `pg_is_in_recovery()` rather than trusting the token.

use std::collections::HashMap;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Hook subcommand names
// ---------------------------------------------------------------------------

pub const HOOK_FAILOVER: &str = "failover";
pub const HOOK_FOLLOW_PRIMARY: &str = "follow_primary";
pub const HOOK_RECOVERY_1ST_STAGE: &str = "recovery_1st_stage";
pub const HOOK_PGPOOL_REMOTE_START: &str = "pgpool_remote_start";
pub const HOOK_ESCALATION: &str = "escalation";
pub const HOOK_DE_ESCALATION: &str = "de_escalation";
pub const HOOK_RESTORE_WAL: &str = "restore-wal";

/// The subset of hooks pgpool-II invokes by exec'ing a file under `$PGDATA`
/// (the `pgpool_recovery` C extension exec's `$PGDATA/recovery_1st_stage`
/// and `$PGDATA/pgpool_remote_start` as subprocesses of the primary's
/// PostgreSQL backend). pg_agentd creates / repairs symlinks at these paths
/// pointing at `pg_agentc`.
pub const PGDATA_SYMLINK_HOOKS: &[&str] = &[HOOK_RECOVERY_1ST_STAGE, HOOK_PGPOOL_REMOTE_START];

/// Every hook name the dispatcher accepts. Single source of truth for
/// `pg_agentc`'s up-front validation — a typo at the `pgpool.conf` level
/// (`failovr` instead of `failover`) is rejected with a clear error
/// instead of attempting a no-op dispatch.
pub const HOOK_NAMES: &[&str] = &[
    HOOK_FAILOVER,
    HOOK_FOLLOW_PRIMARY,
    HOOK_RECOVERY_1ST_STAGE,
    HOOK_PGPOOL_REMOTE_START,
    HOOK_ESCALATION,
    HOOK_DE_ESCALATION,
    HOOK_RESTORE_WAL,
];

// ---------------------------------------------------------------------------
// Token enums
// ---------------------------------------------------------------------------

/// Pgpool-II format-string token (`%LETTER`). Used by `failover_command`,
/// `follow_primary_command`, and `wd_escalation_command`/`wd_de_escalation_command`
/// (the watchdog hooks take none of these).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Token {
    DetachedId,     // %d
    DetachedHost,   // %h
    DetachedPort,   // %p
    DetachedData,   // %D
    NewMainId,      // %m
    NewMainHost,    // %H
    OldMainId,      // %M
    OldPrimaryId,   // %P
    NewMainPort,    // %r
    NewMainData,    // %R
    OldPrimaryHost, // %N
    OldPrimaryPort, // %S
}

/// PostgreSQL `postgresql.conf` format-string token. Distinct enum from
/// [`Token`] so the visual `%p` collision (pgpool=port, postgres=path) is a
/// compile-time impossibility.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum PostgresToken {
    WalFile, // %f — WAL segment filename
    WalDest, // %p — destination path
}

/// Positional argument whose order is hardcoded in the `pgpool_recovery` C
/// extension. The operator has no knob to change the layout from any
/// pgpool.conf field.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum FixedArg {
    // recovery_1st_stage positional args (pgpool 4.3+).
    PrimaryData, // $1
    StandbyHost, // $2
    StandbyData, // $3
    PrimaryPort, // $4 (3.4+)
    StandbyId,   // $5 (4.0+)
    StandbyPort, // $6 (4.1+)
    PrimaryHost, // $7 (4.3+)

    // pgpool_remote_start positional args.
    RemoteHost, // $1 — hostname of standby to recover
    RemoteData, // $2 — primary's $PGDATA (see notes §11; we discard this)
}

// ---------------------------------------------------------------------------
// Param and HookSchema
// ---------------------------------------------------------------------------

/// One positional argument: which typed key it maps to, the token as it
/// appears in the config file, and a description of what it carries. `desc`
/// is documentation; runtime parsing uses only `key`.
#[derive(Debug, Clone)]
pub struct Param<K: Copy + Eq + std::hash::Hash> {
    pub key: K,
    pub token: &'static str,
    pub desc: &'static str,
}

/// Ordered list of parameters a hook receives as positional arguments. Single
/// source of truth for a hook's calling convention.
#[derive(Debug, Clone)]
pub struct HookSchema<K: Copy + Eq + std::hash::Hash + 'static> {
    pub params: &'static [Param<K>],
}

#[derive(Debug, Error)]
#[error("{hook}: expected {expected} args, got {got}")]
pub struct ArgCountError {
    pub hook: &'static str,
    pub expected: usize,
    pub got: usize,
}

impl<K: Copy + Eq + std::hash::Hash + 'static> HookSchema<K> {
    /// Parse positional args into a `key → value` map. Returns
    /// [`ArgCountError`] if the caller passed fewer args than the schema
    /// expects (extra trailing args are silently ignored — pgpool 4.x adds
    /// tokens over time and a forward-compatible format string is normal).
    pub fn parse(
        &self,
        args: &[String],
        hook: &'static str,
    ) -> Result<HashMap<K, String>, ArgCountError> {
        if args.len() < self.params.len() {
            return Err(ArgCountError {
                hook,
                expected: self.params.len(),
                got: args.len(),
            });
        }
        let mut map = HashMap::with_capacity(self.params.len());
        for (i, p) in self.params.iter().enumerate() {
            map.insert(p.key, args[i].clone());
        }
        Ok(map)
    }

    /// Tokens in declaration order. Used when emitting canonical
    /// `pgpool.conf` / `postgresql.conf` command strings.
    pub fn tokens(&self) -> Vec<&'static str> {
        self.params.iter().map(|p| p.token).collect()
    }
}

/// Build the expected command value for a pg_agentc-dispatched hook:
/// `pg_agentc <subcommand> <token1> <token2> ...`. Used to derive the
/// values for pgpool.conf and postgresql.conf from the schema definitions.
pub fn pgpool_cmd<K: Copy + Eq + std::hash::Hash + 'static>(
    subcommand: &str,
    schema: &HookSchema<K>,
) -> String {
    let tokens = schema.tokens();
    if tokens.is_empty() {
        format!("pg_agentc {subcommand}")
    } else {
        format!("pg_agentc {subcommand} {}", tokens.join(" "))
    }
}

// ---------------------------------------------------------------------------
// Schemas — the canonical positional layouts.
// ---------------------------------------------------------------------------

/// `failover_command` schema.
///
/// `failover_command = 'pg_agentc failover %d %h %p %D %m %H %M %P %r %R %N %S'`
pub const SCHEMA_FAILOVER: HookSchema<Token> = HookSchema {
    params: &[
        Param {
            key: Token::DetachedId,
            token: "%d",
            desc: "detached node ID",
        },
        Param {
            key: Token::DetachedHost,
            token: "%h",
            desc: "detached node hostname",
        },
        Param {
            key: Token::DetachedPort,
            token: "%p",
            desc: "detached node port",
        },
        Param {
            key: Token::DetachedData,
            token: "%D",
            desc: "detached node $PGDATA",
        },
        Param {
            key: Token::NewMainId,
            token: "%m",
            desc: "new main node ID",
        },
        Param {
            key: Token::NewMainHost,
            token: "%H",
            desc: "new main node hostname",
        },
        Param {
            key: Token::OldMainId,
            token: "%M",
            desc: "old main node ID",
        },
        Param {
            key: Token::OldPrimaryId,
            token: "%P",
            desc: "old primary node ID",
        },
        Param {
            key: Token::NewMainPort,
            token: "%r",
            desc: "new main node port",
        },
        Param {
            key: Token::NewMainData,
            token: "%R",
            desc: "new main node $PGDATA",
        },
        Param {
            key: Token::OldPrimaryHost,
            token: "%N",
            desc: "old primary hostname",
        },
        Param {
            key: Token::OldPrimaryPort,
            token: "%S",
            desc: "old primary port",
        },
    ],
};

/// `follow_primary_command` schema — identical wire shape to failover_command;
/// the semantics of %m/%H differ ("new primary" vs "new main") per notes §9.
pub const SCHEMA_FOLLOW_PRIMARY: HookSchema<Token> = SCHEMA_FAILOVER;

/// `restore_command = 'pg_agentc restore-wal %f %p'`
pub const SCHEMA_RESTORE_WAL: HookSchema<PostgresToken> = HookSchema {
    params: &[
        Param {
            key: PostgresToken::WalFile,
            token: "%f",
            desc: "WAL segment filename",
        },
        Param {
            key: PostgresToken::WalDest,
            token: "%p",
            desc: "destination path on this standby",
        },
    ],
};

/// `recovery_1st_stage` fixed-arg schema (pgpool_recovery C extension).
pub const SCHEMA_RECOVERY: HookSchema<FixedArg> = HookSchema {
    params: &[
        Param {
            key: FixedArg::PrimaryData,
            token: "$1",
            desc: "primary $PGDATA",
        },
        Param {
            key: FixedArg::StandbyHost,
            token: "$2",
            desc: "standby hostname",
        },
        Param {
            key: FixedArg::StandbyData,
            token: "$3",
            desc: "standby $PGDATA",
        },
        Param {
            key: FixedArg::PrimaryPort,
            token: "$4",
            desc: "primary port (pgpool 3.4+)",
        },
        Param {
            key: FixedArg::StandbyId,
            token: "$5",
            desc: "standby node ID (pgpool 4.0+)",
        },
        Param {
            key: FixedArg::StandbyPort,
            token: "$6",
            desc: "standby port (pgpool 4.1+)",
        },
        Param {
            key: FixedArg::PrimaryHost,
            token: "$7",
            desc: "primary hostname (pgpool 4.3+)",
        },
    ],
};

/// `pgpool_remote_start` fixed-arg schema.
pub const SCHEMA_REMOTE_START: HookSchema<FixedArg> = HookSchema {
    params: &[
        Param {
            key: FixedArg::RemoteHost,
            token: "$1",
            desc: "hostname of standby to recover",
        },
        Param {
            key: FixedArg::RemoteData,
            token: "$2",
            desc: "primary's $PGDATA (discarded — see notes §11)",
        },
    ],
};

// ---------------------------------------------------------------------------
// Canonical pgpool.conf / postgresql.conf lines
// ---------------------------------------------------------------------------

/// One row of the canonical `pgpool.conf` hook block.
#[derive(Debug, Clone)]
pub struct PgpoolHookEntry {
    /// Directive name, e.g. `"failover_command"`.
    pub key: &'static str,
    /// Expected value, e.g. `"pg_agentc failover %d %h ..."`.
    pub value: String,
}

/// Build every directive in the canonical `pgpool.conf` hook block —
/// the **agent-led contract** (docs/pgpool-hook-contract.md §4). Values
/// are derived from the schema
/// definitions so token order is always consistent with what
/// `HookSchema::parse` expects.
///
/// The two decisions this block encodes:
///
/// - `failover_command` is **kept, as a notify-only poke**. The handler
///   under lease-driven roles logs the announcement and promotes
///   nothing — the HA loop decides — but the poke buys detection
///   latency over waiting for the next `loop_wait` tick. Its arguments
///   are advisory forever, and that trade is deliberate.
/// - `follow_primary_command` is **empty, not notify-only**: a
///   non-empty value makes pgpool degenerate every healthy standby
///   after a primary failover (hook-contract §2). The agent re-points
///   standbys off the lease instead.
///
/// The `wd_*` escalation hooks are gone with the watchdog.
///
/// `recovery_1st_stage_command` and `pgpool_remote_start` are fixed-arg
/// hooks invoked by the `pgpool_recovery` C extension — only the script
/// name appears in pgpool.conf; the C extension passes positional args
/// itself. `restore_command` lives in postgresql.conf, not pgpool.conf, and
/// is therefore excluded from this list (see [`restore_command`]).
pub fn pgpool_hooks() -> Vec<PgpoolHookEntry> {
    vec![
        PgpoolHookEntry {
            key: "failover_command",
            value: pgpool_cmd(HOOK_FAILOVER, &SCHEMA_FAILOVER),
        },
        PgpoolHookEntry {
            key: "follow_primary_command",
            value: String::new(),
        },
        PgpoolHookEntry {
            key: "recovery_1st_stage_command",
            value: HOOK_RECOVERY_1ST_STAGE.to_string(),
        },
    ]
}

/// Non-hook `pgpool.conf` directives the agent-led contract requires
/// (promotion-authority §6). Emitted by `gen-pgpool` and verified by
/// `check-hooks` alongside the hook block: each of these is
/// decision-critical, not tuning — a wrong value here re-opens a
/// specific defect (watchdog on = a second failover authority;
/// auto_failback on = pgpool re-attaching nodes whose slots the agent
/// manages; detach_false_primary off = routing to an incoherent
/// primary).
///
/// Deliberately absent: `sr_check_period`, `health_check_*` — detection
/// cadence is the operator's tuning, not the contract's.
pub fn pgpool_settings() -> Vec<PgpoolHookEntry> {
    vec![
        PgpoolHookEntry {
            key: "use_watchdog",
            value: "off".to_string(),
        },
        PgpoolHookEntry {
            key: "detach_false_primary",
            value: "on".to_string(),
        },
        PgpoolHookEntry {
            key: "auto_failback",
            value: "off".to_string(),
        },
        PgpoolHookEntry {
            key: "failover_on_backend_error",
            value: "on".to_string(),
        },
    ]
}

/// Canonical value for postgresql.conf's `restore_command`.
pub fn restore_command() -> String {
    pgpool_cmd(HOOK_RESTORE_WAL, &SCHEMA_RESTORE_WAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failover_schema_renders_canonical_command() {
        let line = pgpool_cmd(HOOK_FAILOVER, &SCHEMA_FAILOVER);
        assert_eq!(
            line,
            "pg_agentc failover %d %h %p %D %m %H %M %P %r %R %N %S"
        );
    }

    #[test]
    fn restore_command_renders() {
        assert_eq!(restore_command(), "pg_agentc restore-wal %f %p");
    }

    #[test]
    fn hook_names_contains_every_hook_constant_exactly_once() {
        // Listed constants the dispatcher in pg_agentc cares about. If
        // a new HOOK_* is added but not registered in HOOK_NAMES,
        // pg_agentc would reject it as unknown; conversely, an entry
        // here that doesn't match a const is dead weight. Lock both
        // sides down.
        let expected = [
            HOOK_FAILOVER,
            HOOK_FOLLOW_PRIMARY,
            HOOK_RECOVERY_1ST_STAGE,
            HOOK_PGPOOL_REMOTE_START,
            HOOK_ESCALATION,
            HOOK_DE_ESCALATION,
            HOOK_RESTORE_WAL,
        ];
        assert_eq!(HOOK_NAMES.len(), expected.len(), "HOOK_NAMES length drift");
        for name in expected {
            assert!(HOOK_NAMES.contains(&name), "HOOK_NAMES missing {name:?}");
        }
        // No duplicates.
        let mut sorted: Vec<&str> = HOOK_NAMES.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), HOOK_NAMES.len(), "HOOK_NAMES has duplicates");
    }

    #[test]
    fn pgdata_symlink_hooks_is_a_subset_of_hook_names() {
        for h in PGDATA_SYMLINK_HOOKS {
            assert!(
                HOOK_NAMES.contains(h),
                "PGDATA_SYMLINK_HOOKS entry {h:?} not in HOOK_NAMES"
            );
        }
    }

    #[test]
    fn parse_rejects_too_few_args() {
        let err = SCHEMA_RESTORE_WAL
            .parse(&["onlyone".to_string()], HOOK_RESTORE_WAL)
            .unwrap_err();
        assert_eq!(err.expected, 2);
        assert_eq!(err.got, 1);
    }

    #[test]
    fn parse_extracts_keys() {
        let m = SCHEMA_RESTORE_WAL
            .parse(&["seg".to_string(), "/tmp/p".to_string()], HOOK_RESTORE_WAL)
            .unwrap();
        assert_eq!(m[&PostgresToken::WalFile], "seg");
        assert_eq!(m[&PostgresToken::WalDest], "/tmp/p");
    }
}
