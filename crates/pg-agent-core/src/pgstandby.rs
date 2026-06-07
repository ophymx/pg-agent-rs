//! Standby-side operations that mutate this node's `$PGDATA`:
//! `pg_basebackup`, `pg_rewind`, and writing recovery config.
//!
//! Subprocess invocations + flag tables are spelled out in SPEC §11.

use async_trait::async_trait;

/// `(bytes_done, bytes_total)`. `bytes_total = 0` means unknown.
pub type ProgressCb = Box<dyn Fn(i64, i64) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct BasebackupOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
    pub slot_name: String,
}

#[derive(Debug, Clone)]
pub struct RewindOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
}

/// Wire-shaped input — local concerns (TLS material, `$PGDATA`) live on the
/// impl, not on this struct. Don't extend with fields the caller can't supply.
#[derive(Debug, Clone)]
pub struct WriteRecoveryConfOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
    pub slot_name: String,
}

#[async_trait]
pub trait StandbyOps: Send + Sync {
    /// Clears `$PGDATA` contents first, then exec's pg_basebackup.
    async fn basebackup(
        &self,
        opts: BasebackupOpts,
        progress: Option<ProgressCb>,
    ) -> anyhow::Result<()>;

    /// Clears `pg_replslot/*` before *and* after (see SPEC §17 invariant 5).
    async fn rewind(&self, opts: RewindOpts, progress: Option<ProgressCb>) -> anyhow::Result<()>;

    /// Writes `$PGDATA/myrecovery.conf` + creates `$PGDATA/standby.signal`.
    async fn write_recovery_conf(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()>;
}

// TODO(v1): PgStandbyExec impl (the only concrete impl). Carries PgHome /
// PgDataDir / ReplicationTls.
// Render myrecovery.conf via a template that rejects ' \r \n in the
// conninfo (defense in depth — inputs already validated).
