//! `/healthz` HTTPS listener and the background snapshot loop that feeds it.
//! See SPEC §9.
//!
//! Cadence (hardcoded): 1 s snapshot interval, 500 ms per-sub-probe timeout,
//! 30 s stale-after, 5 s graceful shutdown.

use crate::{localdb::LocalDb, pcp::Pcp};
use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

pub const POLL_INTERVAL: Duration = Duration::from_secs(1);
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);
pub const STALE_AFTER: Duration = Duration::from_secs(30);
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub const ROLE_UNKNOWN: &str = "unknown";
pub const ROLE_PRIMARY: &str = "primary";
pub const ROLE_REPLICA: &str = "replica";

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub timestamp: DateTime<Utc>,
    pub role: String,

    pub postgres_ok: bool,
    pub postgres_err: String,
    pub in_recovery: bool,
    pub lag_bytes: i64,
    pub wal_receiver_state: String,

    pub pgpool_ok: bool,
    pub pgpool_err: String,
    /// Total backends defined in pgpool.conf (NOT the count of up backends;
    /// `pcp_node_count` returns the configured total — see SPEC §9.2).
    pub backends_configured: i32,
}

pub struct HealthSnapshotter {
    db: Arc<dyn LocalDb>,
    pcp: Arc<dyn Pcp>,
    interval: Duration,
    probe_timeout: Duration,
    snap: ArcSwapOption<HealthSnapshot>,
}

impl HealthSnapshotter {
    pub fn new(
        db: Arc<dyn LocalDb>,
        pcp: Arc<dyn Pcp>,
        interval: Duration,
        probe_timeout: Duration,
    ) -> Self {
        Self {
            db,
            pcp,
            interval,
            probe_timeout,
            snap: ArcSwapOption::from(None),
        }
    }

    pub fn load(&self) -> Option<Arc<HealthSnapshot>> {
        self.snap.load_full()
    }

    // TODO(v1): run(ctx) — initial probe + tokio interval loop, swap into snap.
    // TODO(v1): probe() — spawn two timeouts (postgres + pcp) concurrently,
    // build a non-nil snapshot even when both sub-probes fail.
}

// TODO(v1):
//   - HealthHandler exposing /healthz, /healthz/primary, /healthz/replica.
//     Status code is the contract; JSON body is informational.
//   - start_healthz_server(serve, transport, db, pcp, serve_err) wired with
//     axum + rustls (reusing the CertReloader for server-only TLS).
