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
//! [`crate::config::PgReplicationConfig`] for sslmode + libpq's default
//! does **not** flow through this pool.
//!
//! See SPEC §4.1 for the verbatim SQL each method runs.

use async_trait::async_trait;
use deadpool_postgres::{Config as PoolConfig, ManagerConfig, Pool, RecyclingMethod, Runtime};
use std::path::Path;
use std::sync::OnceLock;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tracing::debug;

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
        conn.execute("SELECT pg_promote()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_promote: {e}"))?;
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        conn.execute("CHECKPOINT", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: CHECKPOINT: {e}"))?;
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
                "localdb: pg_create_physical_replication_slot({name:?}): {e}"
            )),
        }
    }

    async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
        let conn = self.get_conn().await?;
        conn.execute("SELECT pg_drop_replication_slot($1)", &[&name])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_drop_replication_slot({name:?}): {e}"))?;
        Ok(())
    }

    async fn is_in_recovery(&self) -> anyhow::Result<bool> {
        let conn = self.get_conn().await?;
        let row = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {e}"))?;
        Ok(row.get::<_, bool>(0))
    }

    async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
        let conn = self.get_conn().await?;
        let in_recovery: bool = conn
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_is_in_recovery: {e}"))?
            .get(0);
        if !in_recovery {
            // Primary — no receiver, no lag to report.
            return Ok(ReplicationLag::default());
        }

        let bytes: i64 = conn
            .query_one(
                "SELECT coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), \
                 pg_last_wal_replay_lsn()), 0)",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_wal_lsn_diff: {e}"))?
            .get(0);

        // pg_stat_wal_receiver may have zero rows if the receiver isn't
        // connected — return the lag value with state="" rather than
        // erroring, matching the Go impl.
        let state_row = conn
            .query_opt(
                "SELECT coalesce(status, '') FROM pg_stat_wal_receiver LIMIT 1",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("localdb: pg_stat_wal_receiver: {e}"))?;
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
            .map_err(|e| anyhow::anyhow!("localdb: current_setting({name:?}): {e}"))?;
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
            .map_err(|e| anyhow::anyhow!("localdb: extension_exists({name:?}): {e}"))?;
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
            .map_err(|e| anyhow::anyhow!("localdb: role_exists({name:?}): {e}"))?;
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
            Err(e) => Err(anyhow::anyhow!("localdb: CREATE ROLE {name:?}: {e}")),
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_role_name_accepts_safe_identifiers() {
        for n in [
            "repl",
            "pgpool",
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
