//! Local PostgreSQL connection — `pg_promote()`, `CHECKPOINT`, replication
//! slot management, status queries.
//!
//! Connects over a Unix socket as the `postgres` OS user (peer auth — no
//! password). See SPEC §4.1 for the verbatim SQL each method runs.

use async_trait::async_trait;

/// Replication state on a standby. Empty when there is no receiver.
pub type ReplicationState = String;

#[async_trait]
pub trait LocalDb: Send + Sync {
    async fn promote(&self) -> anyhow::Result<()>;
    async fn checkpoint(&self) -> anyhow::Result<()>;

    /// Idempotent — SQLSTATE 42710 (duplicate_object) is treated as success.
    async fn create_slot(&self, name: &str) -> anyhow::Result<()>;
    async fn drop_slot(&self, name: &str) -> anyhow::Result<()>;

    async fn is_in_recovery(&self) -> anyhow::Result<bool>;

    /// Returns `(lag_bytes, replication_state)`. `(0, "")` on a primary.
    async fn replication_lag(&self) -> anyhow::Result<(i64, ReplicationState)>;

    /// `SHOW`-equivalent. Empty string + Ok(_) when the setting doesn't exist.
    async fn setting(&self, name: &str) -> anyhow::Result<String>;

    async fn extension_exists(&self, name: &str) -> anyhow::Result<bool>;
    async fn role_exists(&self, name: &str) -> anyhow::Result<bool>;

    /// `CREATE ROLE <name> WITH LOGIN REPLICATION` — idempotent.
    /// Name is validated against the same regex as `repl_user`.
    async fn create_replication_role(&self, name: &str) -> anyhow::Result<()>;
}

// TODO(v1): PgLocalDb impl backed by deadpool-postgres pool to the local
// Unix socket; map SQLSTATE 42710 → Ok(()) in create_{slot,role}.
