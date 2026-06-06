//! systemd service management via D-Bus (no `sudo`, polkit-authorised for
//! the `postgres` user). See SPEC §11.

use async_trait::async_trait;

#[async_trait]
pub trait Systemd: Send + Sync {
    async fn start_postgres(&self) -> anyhow::Result<()>;
    async fn stop_postgres(&self) -> anyhow::Result<()>;

    /// True if the unit's ActiveState is one of `active`/`activating`/`reloading`.
    async fn status_postgres(&self) -> anyhow::Result<bool>;
    async fn status_pgpool(&self) -> anyhow::Result<bool>;

    /// ReloadOrRestart — starts the unit if not running.
    async fn reload_or_restart_postgres(&self) -> anyhow::Result<()>;
    async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()>;
}

// TODO(v1): zbus-backed implementation that talks to org.freedesktop.systemd1
// — equivalent to coreos/go-systemd's dbus.Conn (Start/Stop/ReloadOrRestart
// with job-result waiting).
