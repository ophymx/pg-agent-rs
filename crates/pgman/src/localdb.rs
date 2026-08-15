//! Local PostgreSQL connection — `pg_promote()`, `CHECKPOINT`, replication
//! slot management, status queries.
//!
//! # Connection model
//!
//! [`PgLocalDb::connect`] opens a [`deadpool_postgres`] pool to the
//! co-located PostgreSQL instance over a **Unix socket** as the `postgres`
//! OS user. PostgreSQL's `peer` authentication maps the OS user to the
//! `postgres` database role — **no password is sent**, no `.pgpass` is
//! read, no md5/scram challenge happens. The socket path is
//! `<socket_dir>/.s.PGSQL.<port>` (PostgreSQL's convention); tokio-postgres
//! detects an absolute `host` path and uses Unix socket transport
//! automatically.
//!
//! This connection is for the AGENT'S work only (failover/checkpoint/slot
//! management/status). Replication-protocol connections (pg_basebackup,
//! pg_rewind, primary_conninfo) are a separate code path that uses
//! [`crate::pgstandby::PgReplicationConfig`] for sslmode + libpq's default
//! does **not** flow through this pool.
//!
//! See SPEC §4.1 for the verbatim SQL each method runs.

use async_trait::async_trait;
use deadpool_postgres::{Config as PoolConfig, ManagerConfig, Pool, RecyclingMethod, Runtime};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tracing::debug;

/// Bound on pool acquisition, connection creation, and recycle. The
/// target is a Unix socket on the same host, so a healthy PostgreSQL
/// answers in microseconds — five seconds means "PG is not accepting
/// connections", and surfacing that beats queueing behind it forever.
const POOL_TIMEOUT: Duration = Duration::from_secs(5);

/// Server-side `statement_timeout` applied to every connection in the
/// pool. The agent's queries are sub-second except `CHECKPOINT` and
/// `pg_promote()`; 300 s matches the agent's peer-RPC LONG_RPC_TIMEOUT so a
/// statement can never outlive the most patient caller budget in the
/// system. Without this, a hung backend pins the handler that called it
/// for as long as the backend stays hung.
const STATEMENT_TIMEOUT: Duration = Duration::from_secs(300);

/// Lag of a standby behind its primary, plus the WAL receiver's state.
/// `bytes = 0, state = ""` on a primary (no receiver, no lag to report).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationLag {
    /// `pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())`.
    pub bytes: i64,
    /// `pg_stat_wal_receiver.status` — `"streaming"`, `"catchup"`, or `""`
    /// when there is no WAL receiver row.
    pub state: String,
}

#[async_trait]
pub trait LocalDb: Send + Sync {
    async fn promote(&self) -> anyhow::Result<()>;
    async fn checkpoint(&self) -> anyhow::Result<()>;

    /// Idempotent — SQLSTATE 42710 (duplicate_object) is treated as success.
    async fn create_slot(&self, name: &str) -> anyhow::Result<()>;
    async fn drop_slot(&self, name: &str) -> anyhow::Result<()>;

    async fn is_in_recovery(&self) -> anyhow::Result<bool>;

    /// Live PostgreSQL timeline ID parsed from the current WAL filename:
    /// `pg_walfile_name(pg_current_wal_lsn())` on a primary,
    /// `pg_walfile_name_offset(pg_last_wal_replay_lsn())` on a standby.
    /// Both return a 24-char filename whose first 8 hex chars are the
    /// timeline. WAL-derived rather than `pg_control_checkpoint()` because
    /// the control file only updates at checkpoint — stale exactly when
    /// the phantom-primary check needs a live value (right after a crashed
    /// primary returns).
    async fn timeline_id(&self) -> anyhow::Result<i32>;

    /// Live write-ahead log position as a 64-bit value
    /// (high 32 | low 32 of `pg_lsn`). On a primary returns
    /// `pg_current_wal_lsn()` — the position of the last WAL record
    /// flushed to disk. On a standby returns `pg_last_wal_replay_lsn()`
    /// — the position replay has caught up to. Used by `cluster status`
    /// to disambiguate which primary is most-up-to-date in a
    /// split-brain (higher LSN on the same timeline = more committed
    /// WAL).
    async fn current_wal_lsn(&self) -> anyhow::Result<u64>;

    /// On a primary returns `ReplicationLag::default()` (zeros).
    async fn replication_lag(&self) -> anyhow::Result<ReplicationLag>;

    /// `SHOW`-equivalent. Empty string + Ok(_) when the setting doesn't exist.
    async fn setting(&self, name: &str) -> anyhow::Result<String>;

    async fn extension_exists(&self, name: &str) -> anyhow::Result<bool>;
    async fn role_exists(&self, name: &str) -> anyhow::Result<bool>;

    /// `CREATE ROLE <name> WITH LOGIN REPLICATION` — idempotent.
    /// Name is validated against the same regex as `repl_user`.
    async fn create_replication_role(&self, name: &str) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// PgLocalDb — production impl
// ---------------------------------------------------------------------------

pub struct PgLocalDb {
    pool: Pool,
}

impl PgLocalDb {
    /// Build a deadpool-postgres pool against the local Unix socket. Does
    /// not eagerly open a connection — the first call exercises the pool.
    pub async fn connect(socket_dir: impl AsRef<Path>, port: u16) -> anyhow::Result<Self> {
        let socket_dir = socket_dir.as_ref();
        let mut cfg = PoolConfig::new();
        // tokio-postgres treats any absolute `host` as a Unix socket
        // directory; the actual file lives at `<host>/.s.PGSQL.<port>`.
        cfg.host = Some(socket_dir.to_string_lossy().into_owned());
        cfg.port = Some(port);
        cfg.user = Some("postgres".to_string());
        cfg.dbname = Some("postgres".to_string());
        cfg.manager = Some(ManagerConfig {
            // Fast = "check the connection is open"; Verified = "round-trip
            // a SELECT 1". For a Unix socket on the same host the fast
            // check is plenty — pg either accepts a new statement or the
            // statement itself errors and we retry at the call site.
            recycling_method: RecyclingMethod::Fast,
        });
        cfg.options = Some(format!(
            "-c statement_timeout={}",
            STATEMENT_TIMEOUT.as_millis()
        ));
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(POOL_TIMEOUT),
                create: Some(POOL_TIMEOUT),
                recycle: Some(POOL_TIMEOUT),
            },
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| anyhow::anyhow!("localdb: build pool: {e}"))?;
        debug!(socket = %socket_dir.display(), port, "localdb: pool built");
        Ok(Self { pool })
    }

    /// Close all idle connections in the pool. New `get()` calls would
    /// still open fresh ones; mostly useful for tests + clean shutdown.
    pub fn close(&self) {
        self.pool.close();
    }
}

#[async_trait]
impl LocalDb for PgLocalDb {
    async fn promote(&self) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        // wait := false — fire the signal and return. The default
        // (wait = true) blocks server-side for up to 60 s, which put
        // the wait outside any caller's deadline: the acceptance
        // suite's E2 watched a promotion stall 40 s inside this call
        // (recovery-end blocked on restore_command against a
        // partitioned peer) while promote_and_wait's deadline sat
        // powerless around it. The caller owns the wait; this call
        // only owns the signal.
        conn.execute("SELECT pg_promote(false)", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_promote: {}", describe_pg(&e)))?;
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        conn.execute("CHECKPOINT", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: CHECKPOINT: {}", describe_pg(&e)))?;
        Ok(())
    }

    async fn create_slot(&self, name: &str) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        let result = conn
            .execute("SELECT pg_create_physical_replication_slot($1)", &[&name])
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_duplicate_object(&e) => {
                // Slot already exists — treat as success so re-running a
                // failover / cluster init flow doesn't churn.
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(
                "localdb: pg_create_physical_replication_slot({name:?}): {}",
                describe_pg(&e)
            )),
        }
    }

    async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        conn.execute("SELECT pg_drop_replication_slot($1)", &[&name])
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "localdb: pg_drop_replication_slot({name:?}): {}",
                    describe_pg(&e)
                )
            })?;
        Ok(())
    }

    async fn is_in_recovery(&self) -> anyhow::Result<bool> {
        let conn = self.get_conn().await?;
        let row = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {}", describe_pg(&e)))?;
        Ok(row.get::<_, bool>(0))
    }

    async fn timeline_id(&self) -> anyhow::Result<i32> {
        let conn = self.get_conn().await?;
        let in_recovery: bool = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {}", describe_pg(&e)))?
            .get(0);
        if in_recovery {
            // pg_walfile_name*() refuses to run during recovery
            // ("recovery is in progress" — found by the docker
            // acceptance suite; every standby reported timeline 0
            // before this). Streaming standby: the WAL receiver's
            // received_tli is the live timeline. Not streaming:
            // fall back to the control file, which a standby updates
            // at restartpoints — mildly stale at worst, and the
            // phantom-check consumer only compares for *higher* peer
            // timelines, so stale-low is the conservative direction.
            let tli: i32 = conn
                .query_one(
                    "SELECT COALESCE( \
                       (SELECT received_tli FROM pg_stat_wal_receiver), \
                       (SELECT timeline_id FROM pg_control_checkpoint()))",
                    &[],
                )
                .await
                .map_err(|e| anyhow::anyhow!("localdb: standby timeline: {}", describe_pg(&e)))?
                .get(0);
            return Ok(tli);
        }
        let filename: String = conn
            .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_walfile_name: {}", describe_pg(&e)))?
            .get(0);
        // PG WAL filename is exactly 24 hex chars: TLI(8) + LOGID(8) + SEGNO(8).
        if filename.len() < 8 {
            anyhow::bail!("localdb: unexpected WAL filename {filename:?}");
        }
        let tli = i32::from_str_radix(&filename[..8], 16).map_err(|e| {
            anyhow::anyhow!("localdb: parse timeline from WAL filename {filename:?}: {e}")
        })?;
        Ok(tli)
    }

    async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
        let conn = self.get_conn().await?;
        let in_recovery: bool = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {}", describe_pg(&e)))?
            .get(0);
        // pg_lsn renders as `XXXXXXXX/XXXXXXXX` (two hex halves of the
        // 64-bit value). tokio-postgres doesn't have a built-in pg_lsn
        // mapping, so we cast to text and parse — mirrors the timeline_id
        // path. The standby flavor must use pg_last_wal_replay_lsn()
        // since pg_current_wal_lsn() errors on a node in recovery.
        let text: String = if in_recovery {
            conn.query_one("SELECT pg_last_wal_replay_lsn()::text", &[])
                .await
                .map_err(|e| {
                    anyhow::anyhow!("localdb: pg_last_wal_replay_lsn: {}", describe_pg(&e))
                })?
                .get(0)
        } else {
            conn.query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .map_err(|e| anyhow::anyhow!("localdb: pg_current_wal_lsn: {}", describe_pg(&e)))?
                .get(0)
        };
        parse_pg_lsn(&text)
    }

    async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
        let conn = self.get_conn().await?;
        let in_recovery: bool = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {}", describe_pg(&e)))?
            .get(0);
        if !in_recovery {
            // Primary — no receiver, no lag to report.
            return Ok(ReplicationLag::default());
        }

        // `pg_wal_lsn_diff` returns `numeric`; without the explicit
        // `::bigint` cast tokio-postgres can't deserialize column 0
        // into `i64` and panics inside `.get(0)`. The diff is always
        // bytes-of-WAL which fits comfortably in i64.
        let bytes: i64 = conn
            .query_one(
                "SELECT coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), \
                 pg_last_wal_replay_lsn())::bigint, 0::bigint)",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_wal_lsn_diff: {}", describe_pg(&e)))?
            .try_get(0)
            .map_err(|e| anyhow::anyhow!("localdb: pg_wal_lsn_diff: decode i64: {e}"))?;

        // pg_stat_wal_receiver may have zero rows if the receiver isn't
        // connected — return the lag value with state="" rather than
        // erroring, matching the Go impl.
        let state_row = conn
            .query_opt(
                "SELECT coalesce(status, '') FROM pg_stat_wal_receiver LIMIT 1",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_stat_wal_receiver: {}", describe_pg(&e)))?;
        let state: String = state_row.map(|r| r.get(0)).unwrap_or_default();

        Ok(ReplicationLag { bytes, state })
    }

    async fn setting(&self, name: &str) -> anyhow::Result<String> {
        let conn = self.get_conn().await?;
        // current_setting(name, missing_ok=true) returns NULL when absent;
        // accept NULL and turn into the empty string, matching the Go impl.
        let row = conn
            .query_one("SELECT current_setting($1, true)", &[&name])
            .await
            .map_err(|e| {
                anyhow::anyhow!("localdb: current_setting({name:?}): {}", describe_pg(&e))
            })?;
        let v: Option<String> = row.get(0);
        Ok(v.unwrap_or_default())
    }

    async fn extension_exists(&self, name: &str) -> anyhow::Result<bool> {
        let conn = self.get_conn().await?;
        let row = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)",
                &[&name],
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("localdb: extension_exists({name:?}): {}", describe_pg(&e))
            })?;
        Ok(row.get(0))
    }

    async fn role_exists(&self, name: &str) -> anyhow::Result<bool> {
        let conn = self.get_conn().await?;
        let row = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                &[&name],
            )
            .await
            .map_err(|e| anyhow::anyhow!("localdb: role_exists({name:?}): {}", describe_pg(&e)))?;
        Ok(row.get(0))
    }

    async fn create_replication_role(&self, name: &str) -> anyhow::Result<()> {
        // CREATE ROLE doesn't accept placeholders for the role name, so
        // the value is interpolated. Guard with the same regex peer RPCs
        // use for repl_user to keep SQL injection off the table; then
        // double-quote the identifier as defense in depth.
        if !allowed_role_name().is_match(name) {
            anyhow::bail!("localdb: invalid replication role name {name:?}");
        }
        let sql = format!(
            "CREATE ROLE {} WITH LOGIN REPLICATION",
            quote_identifier(name)
        );

        let conn = self.get_conn().await?;
        let result = conn.execute(&sql, &[]).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_duplicate_object(&e) => {
                // Role already exists — leave it alone. (Matches the Go
                // impl: don't try to ALTER ROLE; the operator owns role
                // tuning beyond ClusterInit's bootstrap.)
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(
                "localdb: CREATE ROLE {name:?}: {}",
                describe_pg(&e)
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

impl PgLocalDb {
    async fn get_conn(&self) -> anyhow::Result<deadpool_postgres::Object> {
        self.pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("localdb: get connection from pool: {e}"))
    }
}

/// Same regex as `pgstandby::allowed_repl_user` — PostgreSQL role names in
/// this project are restricted to a conservative identifier subset so they
/// can be safely interpolated into `CREATE ROLE` (since the role name is
/// not parameter-bindable).
/// Parse a `pg_lsn` text rendering (`XXXXXXXX/XXXXXXXX`) into a 64-bit
/// value with the high half in the upper 32 bits. Tolerant of variable-
/// length hex segments (PG sometimes strips leading zeros when the
/// value is small) — accepts `0/0` through 16/16 hex digits.
pub fn parse_pg_lsn(s: &str) -> anyhow::Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("localdb: pg_lsn missing '/': {s:?}"))?;
    if hi.is_empty() || lo.is_empty() || hi.len() > 16 || lo.len() > 16 {
        anyhow::bail!("localdb: pg_lsn unexpected width: {s:?}");
    }
    let hi = u64::from_str_radix(hi, 16)
        .map_err(|e| anyhow::anyhow!("localdb: pg_lsn high half {hi:?}: {e}"))?;
    let lo = u64::from_str_radix(lo, 16)
        .map_err(|e| anyhow::anyhow!("localdb: pg_lsn low half {lo:?}: {e}"))?;
    Ok((hi << 32) | lo)
}

fn allowed_role_name() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[A-Za-z0-9_.-]+$").unwrap())
}

/// Wrap a SQL identifier in double quotes, doubling any internal `"`.
/// Defense in depth — [`allowed_role_name`] already restricts the
/// alphabet, so the doubling is a no-op for our inputs, but the code
/// stays correct if the regex ever loosens.
fn quote_identifier(name: &str) -> String {
    let mut s = String::with_capacity(name.len() + 2);
    s.push('"');
    for ch in name.chars() {
        if ch == '"' {
            s.push('"');
        }
        s.push(ch);
    }
    s.push('"');
    s
}

/// True if the error is SQLSTATE 42710 (duplicate_object) — the signal
/// PostgreSQL uses for both "slot already exists" (from
/// `pg_create_physical_replication_slot`) and "role already exists" (from
/// `CREATE ROLE`). Both call sites map this to `Ok(())` for idempotency.
fn is_duplicate_object(err: &tokio_postgres::Error) -> bool {
    err.as_db_error()
        .map(|db| db.code() == &SqlState::DUPLICATE_OBJECT)
        .unwrap_or(false)
}

/// Format a `tokio_postgres::Error` with useful context.
///
/// `tokio_postgres::Error`'s `Display` impl categorises (returns "db error",
/// "tls error", "error connecting to server", …) instead of describing —
/// the real SQLSTATE + message live on the wrapped `DbError`. For
/// server-side errors we emit `SQLSTATE: message` (e.g.
/// `55000: replication slot "node1" is active for PID 12345`). For
/// transport / protocol errors we walk the `source()` chain.
fn describe_pg(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut s = format!("{}: {}", db.code().code(), db.message());
        if let Some(detail) = db.detail() {
            s.push_str(" — ");
            s.push_str(detail);
        }
        if let Some(hint) = db.hint() {
            s.push_str(" (hint: ");
            s.push_str(hint);
            s.push(')');
        }
        return s;
    }
    let mut parts = vec![e.to_string()];
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        parts.push(s.to_string());
        src = s.source();
    }
    parts.join(": ")
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_lsn_canonical() {
        // "0/0" → 0
        assert_eq!(parse_pg_lsn("0/0").unwrap(), 0u64);
        // "0/1A2B3C4D" — primary just after restart
        assert_eq!(parse_pg_lsn("0/1A2B3C4D").unwrap(), 0x1A2B_3C4D);
        // "1/FFFFFFFF" — high half non-zero
        assert_eq!(parse_pg_lsn("1/FFFFFFFF").unwrap(), 0x1_FFFF_FFFF);
        // "FFFFFFFF/FFFFFFFF" — saturated upper bound
        assert_eq!(parse_pg_lsn("FFFFFFFF/FFFFFFFF").unwrap(), u64::MAX);
        // pg accepts variable-width hex; we tolerate it.
        assert_eq!(parse_pg_lsn("00000000/00000000").unwrap(), 0);
    }

    #[test]
    fn parse_pg_lsn_rejects_malformed() {
        assert!(parse_pg_lsn("").is_err());
        assert!(parse_pg_lsn("0").is_err()); // missing '/'
        assert!(parse_pg_lsn("/0").is_err()); // empty high half
        assert!(parse_pg_lsn("0/").is_err()); // empty low half
        assert!(parse_pg_lsn("XYZ/0").is_err());
        assert!(parse_pg_lsn("0/12345678901234567").is_err()); // too long
    }

    #[test]
    fn allowed_role_name_accepts_safe_identifiers() {
        for n in [
            "repl",
            "pooler",
            "node0",
            "with_underscore",
            "dot.style",
            "a-b",
        ] {
            assert!(allowed_role_name().is_match(n), "{n:?} should be accepted");
        }
    }

    #[test]
    fn allowed_role_name_rejects_injection_attempts() {
        for n in [
            "",
            " ",
            "alice; DROP TABLE",
            "alice\"",
            "alice' OR '1'='1",
            "alice\nDROP",
            "alice\tbob",
            "α", // non-ASCII — refuse rather than maybe-allowed
        ] {
            assert!(!allowed_role_name().is_match(n), "{n:?} should be rejected");
        }
    }

    #[test]
    fn quote_identifier_wraps_in_double_quotes() {
        assert_eq!(quote_identifier("repl"), "\"repl\"");
        assert_eq!(quote_identifier("pg.role"), "\"pg.role\"");
    }

    #[test]
    fn quote_identifier_doubles_internal_quotes() {
        // Defensive — allowed_role_name() rejects this input, but the
        // quoting helper must still be correct in isolation.
        assert_eq!(quote_identifier("bad\"name"), "\"bad\"\"name\"");
    }

    #[test]
    fn replication_lag_default_is_primary_state() {
        let lag = ReplicationLag::default();
        assert_eq!(lag.bytes, 0);
        assert_eq!(lag.state, "");
    }
}
