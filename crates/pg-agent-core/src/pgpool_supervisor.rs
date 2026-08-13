//! `PgpoolSupervisor` — keeps `pgpool2.service` running.
//!
//! # Motivation
//!
//! pgpool can die from transient causes (DNS resolution hiccup during
//! watchdog init, OOM-killer, brief D-Bus error) and `pgpool2.service`'s
//! `Restart=` policy doesn't always cover the case — see the field log
//! where db0's pgpool stayed `Active: failed` for ~48h after a single
//! `getaddrinfo` failure. pg_agentd is the only process on the host with
//! both a reason to know "pgpool should be up" and a credential to call
//! `StartUnit` over D-Bus; this module wires that.
//!
//! # Lifecycle
//!
//! Spawned by `Agent::serve` only when the startup phantom-primary
//! verdict is `Confirmed` or `NotApplicable` AND `[supervisor] pgpool`
//! is enabled (default true). On `Phantom` / `SplitBrain` /
//! `Unverifiable` the supervisor is NOT spawned — a node whose role
//! hasn't been validated shouldn't have pgpool routing traffic at it.
//!
//! The startup beat is a single best-effort `ensure_running_once`; the
//! continuous beat is `run`, which ticks every `TICK` and on each tick:
//!
//! 1. Queries `Systemd::status_pgpool`. If active, resets the failure
//!    counter and returns.
//! 2. If under the per-attempt cooldown (`MIN_ATTEMPT_GAP`), returns.
//! 3. If at or past the consecutive-failure cap
//!    (`MAX_CONSECUTIVE_FAILURES`), returns. Operator must restart
//!    pg_agentd to retry — by then the systemd `failed` state has been
//!    in place long enough that the operator should be looking.
//! 4. Otherwise attempts `start_pgpool`; on success the failure counter
//!    is reset on the NEXT tick that confirms `active`; on failure the
//!    counter increments.
//!
//! # Why not `reload_or_restart_pgpool`?
//!
//! `ReloadOrRestartUnit` on an **active** unit reloads it — kicking
//! live client connections. The supervisor's job is "ensure the unit is
//! running"; reloading a working pgpool is the opposite of that. The
//! status-then-start pattern keeps the no-op path truly no-op.

use crate::systemd::Systemd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Cadence of the continuous supervisor loop. Each tick performs at
/// most one status probe + at most one start attempt. Five seconds is
/// fast enough for the HA story (a single pgpool death recovers within
/// the next `fastinter` HAProxy probe window of ~2 s + tick latency)
/// and slow enough that an unhealthy host isn't burning CPU on D-Bus
/// round-trips.
pub const TICK: Duration = Duration::from_secs(5);

/// Minimum wall-clock gap between consecutive `start_pgpool` attempts.
/// Higher than `TICK` so a failed start (which can take several seconds
/// of systemd job execution) doesn't immediately spawn another.
pub const MIN_ATTEMPT_GAP: Duration = Duration::from_secs(30);

/// After this many consecutive failed starts, give up until the daemon
/// is restarted. Sized so the loop tries for ~5 minutes
/// (`MAX_CONSECUTIVE_FAILURES * MIN_ATTEMPT_GAP`) — long enough to ride
/// out a slow systemd recovery, short enough that we surface the
/// real failure to the operator instead of fighting it forever.
pub const MAX_CONSECUTIVE_FAILURES: usize = 10;

/// The supervisor itself. Owned by `Agent::serve`; lives on the
/// JoinSet of background tasks.
pub struct PgpoolSupervisor {
    sd: Arc<dyn Systemd>,
    tick: Duration,
    min_attempt_gap: Duration,
    max_consecutive_failures: usize,
    last_attempt: Mutex<Option<Instant>>,
    consecutive_failures: AtomicUsize,
    /// Cumulative count of `start_pgpool` calls the supervisor has
    /// issued. Exposed for tests; not on a public API.
    pub(crate) started_count: AtomicUsize,
}

impl PgpoolSupervisor {
    /// Production constructor — uses the [`TICK`], [`MIN_ATTEMPT_GAP`],
    /// and [`MAX_CONSECUTIVE_FAILURES`] constants.
    pub fn new(sd: Arc<dyn Systemd>) -> Self {
        Self::with_params(sd, TICK, MIN_ATTEMPT_GAP, MAX_CONSECUTIVE_FAILURES)
    }

    /// Test constructor — lets tests run a fast loop without
    /// `tokio::time::pause`. The cooldown gate is still useful to
    /// exercise independently.
    pub fn with_params(
        sd: Arc<dyn Systemd>,
        tick: Duration,
        min_attempt_gap: Duration,
        max_consecutive_failures: usize,
    ) -> Self {
        Self {
            sd,
            tick,
            min_attempt_gap,
            max_consecutive_failures,
            last_attempt: Mutex::new(None),
            consecutive_failures: AtomicUsize::new(0),
            started_count: AtomicUsize::new(0),
        }
    }

    /// One-shot startup beat. Bypasses the cooldown (this is the first
    /// attempt; no prior to be cool down from) but still respects the
    /// failure cap so a daemon that's repeatedly restarted against a
    /// broken pgpool config doesn't spam the journal.
    ///
    /// Returns `Ok(true)` iff we issued a start attempt; `Ok(false)` if
    /// pgpool was already active. `Err` is the status-query failure —
    /// the caller should log and proceed (the continuous loop will
    /// retry).
    pub async fn ensure_running_once(&self) -> anyhow::Result<bool> {
        let running = self.sd.status_pgpool().await?;
        if running {
            debug!("pgpool_supervisor: startup probe — already active");
            return Ok(false);
        }
        info!("pgpool_supervisor: startup probe — pgpool inactive, starting");
        self.attempt_start().await;
        Ok(true)
    }

    /// Continuous loop. Returns only when `shutdown` is cancelled.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        info!(
            tick_ms = self.tick.as_millis(),
            min_attempt_gap_ms = self.min_attempt_gap.as_millis(),
            max_consecutive_failures = self.max_consecutive_failures,
            "pgpool_supervisor: running"
        );
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("pgpool_supervisor: shutdown");
                    return;
                }
                _ = tokio::time::sleep(self.tick) => {
                    self.tick_once().await;
                }
            }
        }
    }

    /// Single tick. Public for tests that drive ticks deterministically
    /// (e.g. under `tokio::time::pause`).
    pub async fn tick_once(&self) {
        // 1. Status probe. A failed probe doesn't count as a failed
        //    start — could be a transient D-Bus issue — so we just log
        //    and skip this tick.
        let running = match self.sd.status_pgpool().await {
            Ok(r) => r,
            Err(e) => {
                warn!(?e, "pgpool_supervisor: status_pgpool failed; skipping tick");
                return;
            }
        };
        if running {
            // Confirmed up; reset the failure counter so a future death
            // gets a fresh cap window to retry against.
            self.consecutive_failures.store(0, Ordering::SeqCst);
            return;
        }

        // 2. Failure cap — stop trying. Operator restarts pg_agentd to
        //    re-arm. Log once per tick at warn so the situation stays
        //    visible without filling the journal at error.
        let n = self.consecutive_failures.load(Ordering::SeqCst);
        if n >= self.max_consecutive_failures {
            warn!(
                consecutive_failures = n,
                "pgpool_supervisor: gave up after consecutive failures; restart pg_agentd to retry"
            );
            return;
        }

        // 3. Cooldown — don't re-attempt within the gap, even if we're
        //    under the cap.
        if let Some(last) = *self.last_attempt.lock().unwrap() {
            if last.elapsed() < self.min_attempt_gap {
                debug!(
                    elapsed_ms = last.elapsed().as_millis(),
                    "pgpool_supervisor: under cooldown; skipping tick"
                );
                return;
            }
        }

        // 4. Attempt.
        info!("pgpool_supervisor: pgpool inactive, attempting start");
        self.attempt_start().await;
    }

    async fn attempt_start(&self) {
        *self.last_attempt.lock().unwrap() = Some(Instant::now());
        self.started_count.fetch_add(1, Ordering::SeqCst);
        match self.sd.start_pgpool().await {
            Ok(()) => {
                info!("pgpool_supervisor: start_pgpool succeeded");
                // Failure counter is only reset after the NEXT tick
                // confirms `active` — a successful StartUnit call
                // doesn't guarantee the unit reached active before
                // exiting again.
            }
            Err(e) => {
                let n = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
                if n >= self.max_consecutive_failures {
                    error!(
                        ?e,
                        consecutive_failures = n,
                        "pgpool_supervisor: failure cap reached; will stop attempting"
                    );
                } else {
                    warn!(
                        ?e,
                        consecutive_failures = n,
                        "pgpool_supervisor: start_pgpool failed"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Test stub: status flips between Active/Inactive on demand, and
    /// start can be configured to fail a leading number of times.
    /// Avoids the agent::tests StubSd's plain-bool `pgpool_running`
    /// field which can't be flipped after construction.
    #[derive(Default)]
    struct FlipSd {
        active: Mutex<bool>,
        status_fails: Mutex<bool>,
        start_calls: AtomicUsize,
        /// # of leading start_pgpool calls that should fail before success.
        start_fail_count: Mutex<usize>,
        /// When `start_pgpool` succeeds, flip `active` to true so the
        /// next status probe sees the running state.
        start_flips_active: Mutex<bool>,
    }

    impl FlipSd {
        fn new(initial_active: bool) -> Self {
            Self {
                active: Mutex::new(initial_active),
                start_flips_active: Mutex::new(true),
                ..Default::default()
            }
        }
        fn set_active(&self, v: bool) {
            *self.active.lock().unwrap() = v;
        }
        fn set_status_fails(&self, v: bool) {
            *self.status_fails.lock().unwrap() = v;
        }
        fn set_start_fail_count(&self, n: usize) {
            *self.start_fail_count.lock().unwrap() = n;
        }
        fn disable_flip_on_start(&self) {
            *self.start_flips_active.lock().unwrap() = false;
        }
    }

    #[async_trait]
    impl Systemd for FlipSd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            let mut remaining = self.start_fail_count.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                anyhow::bail!("stub: start_pgpool boom");
            }
            if *self.start_flips_active.lock().unwrap() {
                *self.active.lock().unwrap() = true;
            }
            Ok(())
        }
        async fn status_postgres(&self) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn status_pgpool(&self) -> anyhow::Result<bool> {
            if *self.status_fails.lock().unwrap() {
                anyhow::bail!("stub: status_pgpool boom");
            }
            Ok(*self.active.lock().unwrap())
        }
        async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn supervisor(sd: Arc<FlipSd>) -> PgpoolSupervisor {
        // Tight params: short cooldown so the cooldown gate can be
        // exercised without `tokio::time::pause`.
        PgpoolSupervisor::with_params(sd, Duration::from_millis(10), Duration::from_millis(50), 3)
    }

    #[tokio::test]
    async fn tick_when_active_no_op() {
        let sd = Arc::new(FlipSd::new(true));
        let sup = supervisor(sd.clone());
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn tick_when_inactive_attempts_start() {
        let sd = Arc::new(FlipSd::new(false));
        let sup = supervisor(sd.clone());
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
        // FlipSd.start_pgpool flips active=true on success; next tick is no-op.
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn tick_respects_cooldown() {
        // Disable flip-on-start so each tick still sees inactive — the
        // cooldown gate is the only thing keeping the second tick from
        // re-attempting.
        let sd = Arc::new(FlipSd::new(false));
        sd.disable_flip_on_start();
        let sup = supervisor(sd.clone());
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
        // Second tick within 50 ms is gated by cooldown.
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // Cooldown elapsed — re-attempts.
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn tick_counts_failures() {
        let sd = Arc::new(FlipSd::new(false));
        sd.disable_flip_on_start();
        sd.set_start_fail_count(3);
        let sup = supervisor(sd.clone());
        // 1st attempt: fails → failure count 1
        sup.tick_once().await;
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // 2nd attempt: fails → 2
        sup.tick_once().await;
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 2);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // 3rd: hits the cap (max_consecutive_failures=3 in supervisor()).
        sup.tick_once().await;
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 3);
        // Reset stub so a 4th attempt WOULD succeed if it ran;
        // verify the supervisor refuses to even probe.
        sd.set_start_fail_count(0);
        tokio::time::sleep(Duration::from_millis(60)).await;
        let calls_before = sd.start_calls.load(Ordering::SeqCst);
        sup.tick_once().await;
        assert_eq!(
            sd.start_calls.load(Ordering::SeqCst),
            calls_before,
            "supervisor must not attempt after reaching the failure cap"
        );
    }

    #[tokio::test]
    async fn ensure_running_once_skips_when_active() {
        let sd = Arc::new(FlipSd::new(true));
        let sup = supervisor(sd.clone());
        let attempted = sup.ensure_running_once().await.unwrap();
        assert!(!attempted);
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ensure_running_once_starts_when_inactive() {
        let sd = Arc::new(FlipSd::new(false));
        let sup = supervisor(sd.clone());
        let attempted = sup.ensure_running_once().await.unwrap();
        assert!(attempted);
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn status_query_failure_does_not_count_as_start_failure() {
        let sd = Arc::new(FlipSd::new(false));
        sd.set_status_fails(true);
        let sup = supervisor(sd.clone());
        sup.tick_once().await;
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn success_resets_failure_counter_on_next_active_tick() {
        let sd = Arc::new(FlipSd::new(false));
        sd.disable_flip_on_start();
        sd.set_start_fail_count(2);
        let sup = supervisor(sd.clone());
        sup.tick_once().await; // fails → 1
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        sup.tick_once().await; // fails → 2
        assert_eq!(sup.consecutive_failures.load(Ordering::SeqCst), 2);
        // External actor: pgpool came up.
        sd.set_active(true);
        sup.tick_once().await;
        assert_eq!(
            sup.consecutive_failures.load(Ordering::SeqCst),
            0,
            "active tick must reset failure counter"
        );
    }

    #[tokio::test]
    async fn run_returns_on_shutdown() {
        let sd = Arc::new(FlipSd::new(true));
        let sup = Arc::new(supervisor(sd));
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { sup.run(s).await });
        // Let the loop enter steady state.
        tokio::time::sleep(Duration::from_millis(15)).await;
        shutdown.cancel();
        // run() must return promptly.
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("run() did not return within 1s of shutdown")
            .expect("run() task panicked");
    }
}
