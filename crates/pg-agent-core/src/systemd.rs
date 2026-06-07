//! systemd service management via D-Bus (no `sudo`, polkit-authorised for
//! the `postgres` user). See SPEC §11.
//!
//! # Gotcha #1 — the SYSTEM bus, not the session bus
//!
//! [`DbusSystemd::new`] connects to the **system** bus
//! ([`zbus::Connection::system`]). The polkit rule we ship
//! (`/usr/share/polkit-1/rules.d/50-pg-agent.rules`) only protects actions
//! issued on the system bus; talking to the session bus would silently
//! bypass polkit entirely AND fail to actually manage system units, with a
//! confusing "unit not found" or "permission denied" error rather than a
//! clean authorisation failure.
//!
//! # Gotcha #2 — job completion is asynchronous via JobRemoved
//!
//! `StartUnit`/`StopUnit`/`ReloadOrRestartUnit` return *immediately* with
//! an object path identifying the queued job. The unit isn't actually
//! started/stopped/reloaded until systemd processes the job and emits a
//! `JobRemoved` signal carrying the job's result (`done`/`failed`/
//! `timeout`/`canceled`/…).
//!
//! Internally `DbusSystemd::run_unit_op` subscribes to the JobRemoved
//! stream **before** issuing the call, so a fast-completing job (e.g. a unit
//! that was already active and the new job was `skipped`) can't escape
//! the wait window. The previous Go implementation followed the same
//! ordering, but missing the subscribe-first ordering would manifest as
//! "job sometimes never completes" intermittent test failures — exactly
//! the kind of race that's easy to introduce by accident.
//!
//! # Gotcha #3 — Subscribe()
//!
//! systemd delivers `JobRemoved` signals without an explicit `Subscribe()`
//! call, but the recommended pattern (which go-systemd follows internally)
//! is to call it once at startup so other signals (`UnitNew`/`UnitRemoved`)
//! also flow. The call doesn't require polkit and is treated as a hint.
//! We tolerate failure so a quirky bus setup doesn't block agent startup.
//!
//! # Polkit context
//!
//! When `start_postgres`/`stop_postgres`/`reload_or_restart_*` fires,
//! systemd invokes polkit to authorise
//! `org.freedesktop.systemd1.manage-units` for the calling subject. Our
//! rule grants this action to the `postgres` user for
//! `postgresql@*.service` and `pgpool2.service`. No interactive prompt, no
//! sudo, no root. The rule itself is installed by Ansible (see SPEC §13.1
//! — pg_agent owns no /etc files).

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use zbus::proxy;
use zbus::zvariant::{OwnedObjectPath, Type};
use zbus::Connection;

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

// ---------------------------------------------------------------------------
// D-Bus proxy
// ---------------------------------------------------------------------------

/// Generated proxy for `org.freedesktop.systemd1.Manager`.
///
/// Only the methods + signals pg-agent actually uses are declared — keeps
/// the macro output small and the surface scoped. Extend deliberately.
#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait Manager {
    fn start_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn stop_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn reload_or_restart_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn list_units_by_names(&self, names: Vec<String>) -> zbus::Result<Vec<UnitStatus>>;
    fn subscribe(&self) -> zbus::Result<()>;

    /// Emitted when systemd dequeues a job. `result` is one of
    /// `done`/`canceled`/`timeout`/`failed`/`dependency`/`skipped`.
    /// Anything other than `done` is treated as failure.
    #[zbus(signal)]
    fn job_removed(&self, id: u32, job: OwnedObjectPath, unit: String, result: String);
}

/// Matches systemd's `ListUnitsByNames` array element layout (`ssssssouso`).
/// The fields are positional — order must not change without an audit
/// against systemd's documented D-Bus signature.
#[derive(Debug, Clone, Type, Deserialize, Serialize)]
struct UnitStatus {
    name: String,
    description: String,
    load_state: String,
    active_state: String,
    sub_state: String,
    followed: String,
    object_path: OwnedObjectPath,
    job_id: u32,
    job_type: String,
    job_path: OwnedObjectPath,
}

// ---------------------------------------------------------------------------
// DbusSystemd
// ---------------------------------------------------------------------------

pub struct DbusSystemd {
    proxy: ManagerProxy<'static>,
    pg_service: String,
    pgpool_service: String,
}

impl DbusSystemd {
    /// Connect to the **system** bus and build the systemd1.Manager proxy.
    ///
    /// `pg_service` is the PostgreSQL unit name (e.g.
    /// `postgresql@17-main.service`); `pgpool_service` is the pgpool-II
    /// unit name (e.g. `pgpool2.service`). Both must match the units
    /// covered by the polkit rule.
    pub async fn new(
        pg_service: impl Into<String>,
        pgpool_service: impl Into<String>,
    ) -> anyhow::Result<Self> {
        // CRITICAL: system bus, not session bus. See module-level docs.
        let conn = Connection::system().await.map_err(|e| {
            anyhow::anyhow!(
                "systemd: connect to system bus failed: {e}. \
                 (Confirm we are NOT accidentally using the session bus — \
                 polkit only protects the system bus, and the session bus has no \
                 visibility into system units.)"
            )
        })?;
        let proxy = ManagerProxy::new(&conn)
            .await
            .map_err(|e| anyhow::anyhow!("systemd: build Manager proxy: {e}"))?;

        // Hint systemd to deliver UnitNew/UnitRemoved signals. JobRemoved
        // is delivered without this but Subscribe() is the recommended
        // one-shot at startup (matches go-systemd's behaviour). Tolerate
        // failure — worst case is signal-stream latency for some signals
        // we don't currently consume.
        if let Err(e) = proxy.subscribe().await {
            warn!(?e, "systemd: Subscribe() failed; continuing");
        }

        Ok(Self {
            proxy,
            pg_service: pg_service.into(),
            pgpool_service: pgpool_service.into(),
        })
    }

    /// Issue a unit-management call and wait for the corresponding
    /// `JobRemoved` signal. Subscribes to the signal stream **before**
    /// issuing the call so a fast-completing job's signal isn't missed
    /// (see module-level gotcha #2).
    ///
    /// `op` is a human-readable verb (`"start"`, `"stop"`, …) used only
    /// in error messages.
    async fn run_unit_op<F, Fut>(&self, op: &str, unit: &str, call: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = zbus::Result<OwnedObjectPath>>,
    {
        let mut stream =
            self.proxy.receive_job_removed().await.map_err(|e| {
                anyhow::anyhow!("systemd: subscribe JobRemoved for {op} {unit}: {e}")
            })?;

        let job_path = call()
            .await
            .map_err(|e| anyhow::anyhow!("systemd: {op} {unit}: {e}"))?;
        debug!(unit, op, job = %job_path.as_str(), "systemd: job submitted");

        while let Some(signal) = stream.next().await {
            let args = signal
                .args()
                .map_err(|e| anyhow::anyhow!("systemd: parse JobRemoved args: {e}"))?;
            if args.job == job_path {
                if job_result_ok(&args.result) {
                    debug!(unit, op, "systemd: job completed");
                    return Ok(());
                }
                anyhow::bail!("systemd: {op} {unit}: job result {:?}", args.result);
            }
        }
        anyhow::bail!("systemd: {op} {unit}: signal stream closed before job completed")
    }

    async fn unit_running(&self, unit: &str) -> anyhow::Result<bool> {
        let statuses = self
            .proxy
            .list_units_by_names(vec![unit.to_string()])
            .await
            .map_err(|e| anyhow::anyhow!("systemd: status {unit}: {e}"))?;
        // ListUnitsByNames returns the requested units in order, or omits
        // ones systemd doesn't know about. Empty → "not loaded" → treat as
        // not running (matches the Go impl).
        match statuses.into_iter().next() {
            None => Ok(false),
            Some(s) => Ok(is_active_state_running(&s.active_state)),
        }
    }
}

#[async_trait]
impl Systemd for DbusSystemd {
    async fn start_postgres(&self) -> anyhow::Result<()> {
        self.run_unit_op("start", &self.pg_service, || {
            self.proxy.start_unit(&self.pg_service, "replace")
        })
        .await
    }

    async fn stop_postgres(&self) -> anyhow::Result<()> {
        self.run_unit_op("stop", &self.pg_service, || {
            self.proxy.stop_unit(&self.pg_service, "replace")
        })
        .await
    }

    async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
        self.run_unit_op("reload-or-restart", &self.pg_service, || {
            self.proxy
                .reload_or_restart_unit(&self.pg_service, "replace")
        })
        .await
    }

    async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()> {
        self.run_unit_op("reload-or-restart", &self.pgpool_service, || {
            self.proxy
                .reload_or_restart_unit(&self.pgpool_service, "replace")
        })
        .await
    }

    async fn status_postgres(&self) -> anyhow::Result<bool> {
        self.unit_running(&self.pg_service).await
    }

    async fn status_pgpool(&self) -> anyhow::Result<bool> {
        self.unit_running(&self.pgpool_service).await
    }
}

// ---------------------------------------------------------------------------
// Pure helpers (testable without a live D-Bus)
// ---------------------------------------------------------------------------

/// systemd's `ActiveState` values that we treat as "the unit is up and
/// processing requests" — matches the Go impl. Any other value (including
/// `inactive`/`deactivating`/`failed` and the rare `maintenance`) is
/// treated as "not running" so the agent can surface a clear "stopped"
/// rather than "unknown" to operators.
fn is_active_state_running(state: &str) -> bool {
    matches!(state, "active" | "activating" | "reloading")
}

/// `JobRemoved.result` values we accept as success. Anything else
/// (`failed`/`canceled`/`timeout`/`dependency`/`skipped`) is treated as
/// failure so the agent can decide whether to retry, give up, or escalate.
fn job_result_ok(result: &str) -> bool {
    result == "done"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_state_running_recognises_running_states() {
        for s in ["active", "activating", "reloading"] {
            assert!(is_active_state_running(s), "{s:?} should be running");
        }
    }

    #[test]
    fn active_state_running_treats_other_states_as_stopped() {
        for s in [
            "inactive",
            "deactivating",
            "failed",
            "maintenance",
            "", // unit not loaded
            "future-state-systemd-might-add",
        ] {
            assert!(!is_active_state_running(s), "{s:?} should NOT be running");
        }
    }

    #[test]
    fn job_result_ok_only_done_succeeds() {
        assert!(job_result_ok("done"));
        for r in [
            "failed",
            "canceled",
            "timeout",
            "dependency",
            "skipped",
            "collected",
            "",
        ] {
            assert!(!job_result_ok(r), "{r:?} should NOT be ok");
        }
    }
}
