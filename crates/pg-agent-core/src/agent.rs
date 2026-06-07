//! `Agent` — the long-running runtime. Owns the shared deps and the
//! background subsystems (LocalServer + PeerServer + MaintenanceWorker +
//! /healthz listener). See SPEC §12.

use crate::{
    certreload::CertReloader,
    config::{NodePool, PostgresRuntime, ServeSettings},
    localdb::LocalDb,
    maintenance::MaintenanceStore,
    pcp::Pcp,
    peers::PeerRegistry,
    pgstandby::StandbyOps,
    replay_markers::ReplayMarkerStore,
    systemd::Systemd,
    walstore::WalStore,
};
use async_trait::async_trait;
use pg_agent_proto::pgagentpb as pb;
use std::sync::Arc;
use std::time::Duration;

/// Read-only "ask the node about itself" surface shared by both proto
/// services. Both `LocalServer` and `PeerServer` delegate their
/// `GetStatus`/`GetNodeConfig` handlers here so a single canonical
/// implementation drives both responses.
#[async_trait]
pub trait NodeInfo: Send + Sync {
    async fn get_status(&self) -> anyhow::Result<pb::NodeStatus>;
    async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse>;
}

/// External collaborators. Every field must be non-nil at agent construction.
pub struct AgentDeps {
    pub db: Arc<dyn LocalDb>,
    pub peers: Arc<dyn PeerRegistry>,
    pub standby: Arc<dyn StandbyOps>,
    pub pcp: Arc<dyn Pcp>,
    pub sd: Arc<dyn Systemd>,
    pub replay: Arc<dyn ReplayMarkerStore>,
    pub wal: Arc<dyn WalStore>,
}

pub struct Options {
    pub serve: ServeSettings,
    pub node_pool: NodePool,
    pub postgres: PostgresRuntime,
    pub maintenance_store: Arc<dyn MaintenanceStore>,
    pub maintenance_sweep_interval: Duration,
    pub cert_reloader: Option<Arc<CertReloader>>,
}

pub struct Agent {
    // TODO(v1): fields mirror Go's agent.Agent — serve, topo, pg, deps,
    // maintenance worker, cert reloader.
    _deps: AgentDeps,
    _opts: Options,
}

impl Agent {
    pub fn new(deps: AgentDeps, opts: Options) -> anyhow::Result<Self> {
        Ok(Self {
            _deps: deps,
            _opts: opts,
        })
    }

    /// Serve until `ctx` is cancelled. Brings up Unix socket + peer TCP +
    /// healthz + maintenance worker; sends `READY=1` via sd_notify; on
    /// shutdown sends `STOPPING=1` and gracefully drains.
    pub async fn serve(
        &self,
        _shutdown: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        // TODO(v1): bring up subsystems per SPEC §12.
        Ok(())
    }
}

// TODO(v1):
//   - NodeInfo impl on Agent (parallel postgres + systemd queries,
//     readiness verdict from §5.9).
//   - bestEffortCleanupContext-equivalent helper for cleanup paths that
//     must run after the hook ctx cancels.
//   - LocalServer + PeerServer (tonic services that translate gRPC requests
//     into trait calls; live in this crate to avoid a circular dep).
