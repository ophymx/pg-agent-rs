//! The concern layer: one owner for the instance's lifecycle.
//!
//! The mechanism traits ([`ProcessControl`], [`LocalDb`], [`StandbyOps`])
//! are grouped by *how* they act, which makes them individually mockable
//! — and leaves "drive this instance to a role" to be composed at every
//! call site, with the ordering invariants enforced by convention. This
//! module is where that composition gets a single owner, shaped by the
//! HA loop's step-7 executors (docs/promotion-authority.md §10): each
//! method is the execution of one decision family.
//!
//! Three long-standing gaps close structurally here:
//!
//! - **Asynchronous promotion gets one home.** `pg_promote()` returns
//!   before promotion completes; three separate bugs (the agent's
//!   acceptance findings 5, 11, 13) were layers independently coping
//!   with that. [`PostgresInstance::promote_and_wait`] is the one place
//!   that waits.
//! - **Destructive preconditions become structure.** `basebackup`'s
//!   "caller must have verified PostgreSQL isn't running" was a doc
//!   comment at N call sites; [`PostgresInstance::rebuild_as_standby`]
//!   stops the instance before it wipes, unconditionally, because the
//!   state machine owns the ordering.
//! - **Liveness gets one answer.** Process-active, SQL-answering, and
//!   streaming were three separately-consulted facts;
//!   [`PostgresInstance::state`] composes them into one
//!   [`InstanceState`], with "process up but not answering" reported as
//!   [`InstanceState::Unknown`] rather than guessed either way.
//!
//! # What stays outside
//!
//! The *local half* rule from the crate docs applies. Preparing the
//! upstream (creating the replication slot on the new primary,
//! checkpointing it before a rewind), journaling the operation,
//! notifying poolers — all of that is the caller's orchestration. An
//! [`UpstreamSpec`] arriving here means the caller has already made the
//! upstream ready to serve it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::localdb::LocalDb;
use crate::pgstandby::{BasebackupOpts, RewindOpts, StandbyOps, WriteRecoveryConfOpts};
use crate::process::ProcessControl;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One authoritative view of the local instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceState {
    /// The process manager reports the instance not running.
    Down,
    /// Running, in recovery. `streaming` is `pg_stat_wal_receiver`
    /// reporting an active `streaming` connection — a standby that is
    /// up but not streaming is a standby that is silently falling
    /// behind, and callers get to see the difference.
    Standby { streaming: bool },
    /// Running, not in recovery.
    Primary,
    /// The process manager says running, but PostgreSQL is not
    /// answering. Deliberately NOT collapsed into `Down`: a hung or
    /// starting postmaster is not an absent one, and role decisions
    /// built on that conflation act on evidence they don't have.
    Unknown,
}

/// The upstream a standby should replicate from. The caller has already
/// prepared it: the slot exists, and (for a rewind) the source has been
/// checkpointed.
#[derive(Debug, Clone)]
pub struct UpstreamSpec {
    pub host: String,
    pub port: u16,
    pub repl_user: String,
    pub slot_name: String,
}

impl UpstreamSpec {
    fn recovery_conf(&self) -> WriteRecoveryConfOpts {
        WriteRecoveryConfOpts {
            primary_host: self.host.clone(),
            primary_port: self.port,
            repl_user: self.repl_user.clone(),
            slot_name: self.slot_name.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

/// Intent-level operations on the local instance. Every method is
/// convergent: callers re-issue the same intent every tick, so "already
/// there" is success, never an error.
#[async_trait]
pub trait PostgresInstance: Send + Sync {
    /// Snapshot the instance's state. Never errors: not being able to
    /// tell *is* a state ([`InstanceState::Unknown`]), and forcing
    /// callers through a `Result` invites mapping failure onto `Down`.
    async fn state(&self) -> InstanceState;

    /// Promote and wait until recovery actually ends.
    ///
    /// `pg_promote()` is asynchronous; this method owns the wait.
    /// Idempotent: an already-primary instance returns `Ok`
    /// immediately. On deadline expiry the error says so explicitly —
    /// **promotion may still complete afterwards** (the signal cannot
    /// be recalled), and the caller's next state() poll is the truth.
    async fn promote_and_wait(&self, deadline: Duration) -> anyhow::Result<()>;

    /// Stop the instance. Idempotent. The demote primitive: fast,
    /// unconditional, no cleverness — this runs precisely when the node
    /// has lost the right (or the quorum contact) to serve.
    async fn ensure_stopped(&self) -> anyhow::Result<()>;

    /// Re-point a running standby at a new upstream: rewrite the
    /// recovery config, then reload — `primary_conninfo` is reloadable
    /// (PG ≥ 13), so sessions survive and the walreceiver reconnects.
    ///
    /// This is the *light* path: it assumes the local timeline can
    /// follow the new upstream (true for surviving standbys after a
    /// clean failover, with `recovery_target_timeline = 'latest'`). A
    /// diverged instance needs [`Self::rebuild_as_standby`].
    async fn follow(&self, upstream: &UpstreamSpec) -> anyhow::Result<()>;

    /// Converge on "streaming standby of `upstream`" from any local
    /// state — wrong role, stopped, diverged. Stops the instance,
    /// tries `pg_rewind`, falls back to a full basebackup on any rewind
    /// failure, writes the recovery config, starts.
    ///
    /// Destructive by design (the fallback wipes `$PGDATA`) — which is
    /// why the stop is unconditional and internal rather than a caller
    /// precondition in a doc comment.
    ///
    /// Returns after `start`; it does not wait for streaming. Rebuild
    /// duration is dominated by the data copy and the caller is a
    /// convergence loop — poll [`Self::state`] for
    /// `Standby { streaming: true }`.
    async fn rebuild_as_standby(&self, upstream: &UpstreamSpec) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Production impl
// ---------------------------------------------------------------------------

/// How often [`Instance::promote_and_wait`] polls `is_in_recovery`.
const PROMOTE_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// [`PostgresInstance`] over the crate's mechanism traits.
pub struct Instance {
    process: Arc<dyn ProcessControl>,
    db: Arc<dyn LocalDb>,
    standby: Arc<dyn StandbyOps>,
}

impl Instance {
    pub fn new(
        process: Arc<dyn ProcessControl>,
        db: Arc<dyn LocalDb>,
        standby: Arc<dyn StandbyOps>,
    ) -> Self {
        Self {
            process,
            db,
            standby,
        }
    }
}

#[async_trait]
impl PostgresInstance for Instance {
    async fn state(&self) -> InstanceState {
        // Process first: a dead process answers no SQL, and asking the
        // process manager is cheaper than a connection timeout.
        match self.process.is_active().await {
            Ok(false) => return InstanceState::Down,
            Ok(true) => {}
            // Cannot even ask the process manager — that is Unknown,
            // not Down; claiming Down here would license a rebuild of
            // an instance that may be running fine.
            Err(_) => return InstanceState::Unknown,
        }
        match self.db.is_in_recovery().await {
            Ok(false) => InstanceState::Primary,
            Ok(true) => {
                let streaming = self
                    .db
                    .replication_lag()
                    .await
                    .map(|lag| lag.state == "streaming")
                    .unwrap_or(false);
                InstanceState::Standby { streaming }
            }
            Err(_) => InstanceState::Unknown,
        }
    }

    async fn promote_and_wait(&self, deadline: Duration) -> anyhow::Result<()> {
        // Idempotence: promoting a primary is a no-op, not an error —
        // the convergence loop will re-issue this intent every tick
        // until the lease or the instance changes.
        match self.db.is_in_recovery().await {
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(e) => anyhow::bail!("promote: cannot read recovery state: {e}"),
        }

        self.db
            .promote()
            .await
            .map_err(|e| anyhow::anyhow!("promote: pg_promote: {e}"))?;

        // pg_promote() has fired; now the only truth is recovery state.
        let started = tokio::time::Instant::now();
        loop {
            if started.elapsed() >= deadline {
                // The signal cannot be recalled: promotion may still
                // complete after this error. Callers must treat this as
                // "unknown outcome, poll state()", never as "did not
                // promote" — acting on the latter reading is how a
                // cluster ends up retrying a promotion that already
                // succeeded (acceptance finding 11).
                anyhow::bail!(
                    "promote: still in recovery after {deadline:?}; promotion \
                     may yet complete — poll state() before concluding anything"
                );
            }
            tokio::time::sleep(PROMOTE_POLL_INTERVAL).await;
            match self.db.is_in_recovery().await {
                Ok(false) => {
                    info!("instance: promotion complete");
                    return Ok(());
                }
                Ok(true) => continue,
                // Transient query failures during promotion are normal
                // (the server briefly refuses connections at the
                // timeline switch); keep polling until the deadline.
                Err(e) => {
                    warn!(err = %e, "instance: recovery-state poll failed; retrying");
                    continue;
                }
            }
        }
    }

    async fn ensure_stopped(&self) -> anyhow::Result<()> {
        self.process.stop().await
    }

    async fn follow(&self, upstream: &UpstreamSpec) -> anyhow::Result<()> {
        self.standby
            .write_recovery_conf(upstream.recovery_conf())
            .await
            .map_err(|e| anyhow::anyhow!("follow: write recovery conf: {e}"))?;
        self.process
            .reload_or_restart()
            .await
            .map_err(|e| anyhow::anyhow!("follow: reload: {e}"))
    }

    async fn rebuild_as_standby(&self, upstream: &UpstreamSpec) -> anyhow::Result<()> {
        // The stop is internal and unconditional — this is the method
        // that turns basebackup's "caller must have verified PostgreSQL
        // isn't running" from a comment into structure.
        self.process
            .stop()
            .await
            .map_err(|e| anyhow::anyhow!("rebuild: stop: {e}"))?;

        let rewind = RewindOpts {
            primary_host: upstream.host.clone(),
            primary_port: upstream.port,
            repl_user: upstream.repl_user.clone(),
        };
        let rewound = match self.standby.rewind(rewind, None).await {
            Ok(()) => true,
            Err(e) => {
                warn!(err = %e, "instance: rewind failed; falling back to basebackup");
                false
            }
        };
        if !rewound {
            let bb = BasebackupOpts {
                primary_host: upstream.host.clone(),
                primary_port: upstream.port,
                repl_user: upstream.repl_user.clone(),
                slot_name: upstream.slot_name.clone(),
            };
            self.standby
                .basebackup(bb, None)
                .await
                .map_err(|e| anyhow::anyhow!("rebuild: basebackup: {e}"))?;
        }

        self.standby
            .write_recovery_conf(upstream.recovery_conf())
            .await
            .map_err(|e| anyhow::anyhow!("rebuild: write recovery conf: {e}"))?;

        self.process
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("rebuild: start: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::localdb::ReplicationLag;
    use crate::pgstandby::ProgressCb;
    use std::sync::Mutex;

    /// Scripted mechanism stubs sharing one call journal, so tests can
    /// assert ordering across traits — the thing the concern layer
    /// exists to own.
    #[derive(Default)]
    struct World {
        calls: Mutex<Vec<&'static str>>,
        active: Mutex<Option<bool>>, // None = is_active errors
        /// Sequence of is_in_recovery answers; last entry repeats.
        recovery: Mutex<Vec<Option<bool>>>, // None = query errors
        streaming: Mutex<bool>,
        rewind_fails: Mutex<bool>,
    }

    impl World {
        fn log(&self, what: &'static str) {
            self.calls.lock().unwrap().push(what);
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
        fn next_recovery(&self) -> Option<bool> {
            let mut seq = self.recovery.lock().unwrap();
            if seq.len() > 1 {
                seq.remove(0)
            } else {
                *seq.first().unwrap_or(&None)
            }
        }
    }

    struct Proc(Arc<World>);
    #[async_trait]
    impl ProcessControl for Proc {
        async fn start(&self) -> anyhow::Result<()> {
            self.0.log("start");
            Ok(())
        }
        async fn stop(&self) -> anyhow::Result<()> {
            self.0.log("stop");
            Ok(())
        }
        async fn reload_or_restart(&self) -> anyhow::Result<()> {
            self.0.log("reload");
            Ok(())
        }
        async fn is_active(&self) -> anyhow::Result<bool> {
            self.0
                .active
                .lock()
                .unwrap()
                .ok_or_else(|| anyhow::anyhow!("stub: manager unreachable"))
        }
    }

    struct Db(Arc<World>);
    #[async_trait]
    impl LocalDb for Db {
        async fn promote(&self) -> anyhow::Result<()> {
            self.0.log("pg_promote");
            Ok(())
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            self.0
                .next_recovery()
                .ok_or_else(|| anyhow::anyhow!("stub: query failed"))
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            Ok(ReplicationLag {
                bytes: 0,
                state: if *self.0.streaming.lock().unwrap() {
                    "streaming".into()
                } else {
                    String::new()
                },
            })
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            unreachable!()
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    struct Standby(Arc<World>);
    #[async_trait]
    impl StandbyOps for Standby {
        async fn basebackup(&self, _: BasebackupOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            self.0.log("basebackup");
            Ok(())
        }
        async fn rewind(&self, _: RewindOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            self.0.log("rewind");
            if *self.0.rewind_fails.lock().unwrap() {
                anyhow::bail!("stub: rewind failed")
            }
            Ok(())
        }
        async fn write_recovery_conf(&self, _: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            self.0.log("write_recovery_conf");
            Ok(())
        }
    }

    fn instance(world: &Arc<World>) -> Instance {
        Instance::new(
            Arc::new(Proc(world.clone())),
            Arc::new(Db(world.clone())),
            Arc::new(Standby(world.clone())),
        )
    }

    fn upstream() -> UpstreamSpec {
        UpstreamSpec {
            host: "db0".into(),
            port: 5432,
            repl_user: "repl".into(),
            slot_name: "node1".into(),
        }
    }

    fn world() -> Arc<World> {
        let w = Arc::new(World::default());
        *w.active.lock().unwrap() = Some(true);
        *w.recovery.lock().unwrap() = vec![Some(true)];
        w
    }

    // ----- state ------------------------------------------------------------

    #[tokio::test]
    async fn state_maps_the_three_liveness_sources_to_one_answer() {
        let w = world();
        let i = instance(&w);

        assert_eq!(i.state().await, InstanceState::Standby { streaming: false });

        *w.streaming.lock().unwrap() = true;
        assert_eq!(i.state().await, InstanceState::Standby { streaming: true });

        *w.recovery.lock().unwrap() = vec![Some(false)];
        assert_eq!(i.state().await, InstanceState::Primary);

        *w.active.lock().unwrap() = Some(false);
        assert_eq!(i.state().await, InstanceState::Down);
    }

    /// Process up + SQL not answering is Unknown — never Down. Mapping
    /// it to Down licenses rebuilding an instance that may be fine.
    #[tokio::test]
    async fn a_running_process_that_wont_answer_is_unknown_not_down() {
        let w = world();
        *w.recovery.lock().unwrap() = vec![None];
        assert_eq!(instance(&w).state().await, InstanceState::Unknown);

        // And an unreachable process manager is Unknown too.
        *w.active.lock().unwrap() = None;
        assert_eq!(instance(&w).state().await, InstanceState::Unknown);
    }

    // ----- promote_and_wait -------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn promote_waits_through_recovery_and_transient_poll_failures() {
        let w = world();
        // in recovery → promote fires → one poll error (normal at the
        // timeline switch) → still recovering → done.
        *w.recovery.lock().unwrap() = vec![Some(true), None, Some(true), Some(false)];
        instance(&w)
            .promote_and_wait(Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(w.calls(), vec!["pg_promote"]);
    }

    #[tokio::test]
    async fn promote_on_a_primary_is_a_noop() {
        let w = world();
        *w.recovery.lock().unwrap() = vec![Some(false)];
        instance(&w)
            .promote_and_wait(Duration::from_secs(1))
            .await
            .unwrap();
        assert!(w.calls().is_empty(), "no pg_promote on a primary");
    }

    #[tokio::test(start_paused = true)]
    async fn promote_deadline_error_says_the_outcome_is_unknown() {
        let w = world();
        *w.recovery.lock().unwrap() = vec![Some(true)]; // never leaves recovery
        let err = instance(&w)
            .promote_and_wait(Duration::from_secs(2))
            .await
            .unwrap_err()
            .to_string();
        // The wording is the contract: finding 11 was a caller reading
        // "timeout" as "did not promote" while the promotion had
        // already succeeded server-side.
        assert!(err.contains("may yet complete"), "got: {err}");
    }

    // ----- follow -----------------------------------------------------------

    #[tokio::test]
    async fn follow_rewrites_conf_then_reloads_in_that_order() {
        let w = world();
        instance(&w).follow(&upstream()).await.unwrap();
        assert_eq!(w.calls(), vec!["write_recovery_conf", "reload"]);
    }

    // ----- rebuild_as_standby ----------------------------------------------

    #[tokio::test]
    async fn rebuild_stops_before_any_destructive_step() {
        let w = world();
        instance(&w).rebuild_as_standby(&upstream()).await.unwrap();
        assert_eq!(
            w.calls(),
            vec!["stop", "rewind", "write_recovery_conf", "start"],
            "stop must precede rewind; conf before start"
        );
    }

    #[tokio::test]
    async fn rebuild_falls_back_to_basebackup_when_rewind_fails() {
        let w = world();
        *w.rewind_fails.lock().unwrap() = true;
        instance(&w).rebuild_as_standby(&upstream()).await.unwrap();
        assert_eq!(
            w.calls(),
            vec![
                "stop",
                "rewind",
                "basebackup",
                "write_recovery_conf",
                "start"
            ]
        );
    }
}
