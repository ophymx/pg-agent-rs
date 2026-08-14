//! Management of a single PostgreSQL instance.
//!
//! Extracted from `pg-agent-core` when the agent pivoted failover
//! authority away from pgpool (docs/promotion-authority.md): the layer
//! that was about to gain its most dangerous caller — the HA loop's
//! executors — deserved a crate boundary first, so that "this process
//! owns the local PostgreSQL instance" survives whatever the layers
//! above it pivot to next.
//!
//! # What belongs here
//!
//! Everything about **one local instance**, grouped by mechanism:
//!
//! - [`localdb`] — operations over a libpq connection: `pg_promote()`,
//!   `CHECKPOINT`, replication slots, recovery/timeline/LSN/lag
//!   introspection, settings and roles.
//! - [`pgstandby`] — rebuilding this instance as a standby:
//!   `pg_basebackup`, `pg_rewind`, `myrecovery.conf` +
//!   `standby.signal`.
//! - [`walstore`] — the local WAL archive: serving segments out,
//!   restoring segments in (with pgdata-confinement checks).
//! - [`process`] — the seam for starting/stopping the instance's
//!   server process. Trait only: the systemd implementation lives with
//!   the agent, and a non-systemd deployment (pg_ctl, container
//!   supervisor) implements the same three methods.
//!
//! # What is deliberately absent
//!
//! Anything about *other* nodes or *other* software: no peers, no
//! cluster topology, no consensus, no pgpool. Where an operation
//! brushes against a neighbor concern, it takes data rather than
//! knowledge — e.g. [`pgstandby::StandbyExec`] recreates the symlinks
//! it is told about after a basebackup (because `pg_basebackup`
//! silently skips non-tablespace symlinks — a PostgreSQL fact), without
//! knowing they are pgpool hooks (an agent fact).
//!
//! # Intended evolution: `PostgresInstance`
//!
//! The traits here are **mechanism seams** — grouped by *how* they act
//! (SQL, subprocess, filesystem, init system), which is what makes them
//! individually mockable. What they do not provide is a concern seam:
//! "drive this instance to a role" today lives in each caller, composed
//! out of these traits with ordering invariants enforced by convention
//! (stop → wipe/clone or rewind → recovery config → start), and the
//! instance's lifecycle state machine exists only implicitly across
//! those call sites.
//!
//! The planned shape — not yet built, recorded here so the next layer
//! is written against it rather than around it:
//!
//! ```ignore
//! /// One authoritative view of the local instance.
//! enum InstanceState {
//!     Down,
//!     Starting,
//!     Standby { streaming: bool },
//!     Promoting,          // pg_promote() issued, still in recovery
//!     Primary,
//!     Rebuilding { phase: RebuildPhase },
//! }
//!
//! trait PostgresInstance {
//!     async fn state(&self) -> InstanceState;
//!     /// Promote and wait until recovery actually ends (pg_promote is
//!     /// asynchronous; every caller today re-implements the wait).
//!     async fn promote_and_wait(&self, deadline: Duration) -> Result<()>;
//!     /// Converge on "standby of `primary`", choosing rewind vs full
//!     /// clone, owning the stop/wipe/configure/start ordering that
//!     /// callers currently each spell out.
//!     async fn ensure_standby_of(&self, primary: &ConnTarget) -> Result<()>;
//! }
//! ```
//!
//! The mechanism traits stay — `PostgresInstance` composes them, it
//! does not replace them.

#![forbid(unsafe_code)]

pub mod localdb;
pub mod pgstandby;
pub mod process;
pub mod walstore;
