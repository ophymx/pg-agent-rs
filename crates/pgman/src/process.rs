//! The process-control seam for the local PostgreSQL instance.
//!
//! Trait only, by design. The agent's production implementation drives
//! systemd over D-Bus and lives with the agent — pulling zbus into this
//! crate would weld "a PostgreSQL instance" to "a systemd unit", and
//! the point of the seam is that those are different facts. A
//! deployment supervising PostgreSQL with `pg_ctl`, a container
//! runtime, or anything else implements these three methods and the
//! rest of the crate neither knows nor cares.
//!
//! Nothing in this crate consumes the trait yet: the future
//! `PostgresInstance` layer (see the crate docs) is its intended
//! caller. It is declared here rather than there so the crate boundary
//! states, from day one, what the instance's process contract is.

use async_trait::async_trait;

/// Start/stop/observe the instance's server process.
///
/// Implementations must be **idempotent at the intent level**: `start`
/// on a running instance and `stop` on a stopped one succeed without
/// side effects, because callers converge on desired state rather than
/// tracking transitions.
#[async_trait]
pub trait ProcessControl: Send + Sync {
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
    /// Reload if running, start if not. The light path for config
    /// changes PostgreSQL honours on SIGHUP — `primary_conninfo` is
    /// reloadable since PG 13, which is what lets a standby re-point
    /// to a new upstream without dropping its sessions.
    async fn reload_or_restart(&self) -> anyhow::Result<()>;
    /// True when the process manager considers the instance running or
    /// in startup. This is process-level liveness only — whether
    /// PostgreSQL is *answering* is [`crate::localdb`]'s to say, and
    /// conflating the two is how a hung postmaster reads as healthy.
    async fn is_active(&self) -> anyhow::Result<bool>;
}
