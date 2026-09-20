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
//! - [`timeline`] — this instance's control point and the timeline
//!   history around it: which timelines it could follow, and which
//!   would fork it. Pure functions over what `$PGDATA` records, so the
//!   rule is readable and testable apart from the fetching.
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
//! # The concern layer: [`instance`]
//!
//! The traits above are **mechanism seams** — grouped by *how* they act
//! (SQL, subprocess, filesystem, init system), which is what makes them
//! individually mockable. [`instance::PostgresInstance`] is the concern
//! seam on top: intent-level, convergent operations (`promote_and_wait`,
//! `ensure_stopped`, `follow`, `rebuild_as_standby`) plus one
//! authoritative [`instance::InstanceState`], composing the mechanism
//! traits rather than replacing them. Its shape is derived from the HA
//! loop's step-7 executors — see the module docs.

#![forbid(unsafe_code)]

pub mod instance;
pub mod localdb;
pub mod pgstandby;
pub mod process;
pub mod timeline;
pub mod walstore;
