//! Persistent outbound mTLS connections to peer agents + the `PeerTransport`
//! that carries the cert material shared with the inbound peer server and
//! the `/healthz` HTTPS listener.
//!
//! Connection-age cap: 12 h with a 5-minute grace, so SIGHUP-rotated certs
//! reach the wire within ~12 h without tearing down healthy channels.
//! See SPEC §7.

use crate::config::NodeConfig;
use async_trait::async_trait;
use std::sync::Arc;

/// Outbound peer client registry. Hands out a [`PeerClient`] keyed by
/// `NodeConfig` so callers work with resolved config objects rather than
/// raw addresses. The concrete implementation is `PeerPool` (TODO).
#[async_trait]
pub trait PeerRegistry: Send + Sync {
    /// Returns a client targeting `node`. Caller must not call this for the
    /// local node.
    async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>>;

    /// Tear down all peer connections.
    async fn close(&self) -> anyhow::Result<()>;
}

/// What a peer agent does for us. Mirrors `PgAgentPeer` RPC by RPC; the
/// streaming RPCs (Basebackup, Rewind, FetchWal) expose `mpsc::Receiver`
/// to keep the handler code transport-agnostic.
///
/// Methods are added as use cases need them — currently only
/// `drop_slot` (consumed by the maintenance worker's slot-cleanup retry
/// path). When the full `LocalServer` lands the rest of the
/// `PgAgentPeer` surface will follow.
#[async_trait]
pub trait PeerClient: Send + Sync {
    /// `pg_drop_replication_slot($1)` on the target peer's PostgreSQL.
    /// 42710 / "does not exist" should still surface as an error here —
    /// the caller (maintenance worker) decides retry vs. abandon.
    async fn drop_slot(&self, slot_name: &str) -> anyhow::Result<()>;

    // TODO(v1): start/stop/reload/promote/create_slot/configure_standby/
    // basebackup/rewind/fetch_wal/reload_pgpool/remove_vip/get_status/
    // get_node_config.
}

// TODO(v1):
//   - PeerTransport { reloader: Option<Arc<CertReloader>>, allowed_peers: HashSet<String> }
//   - PeerPool that lazily dials each peer and caches the tonic::Channel,
//     with the 12 h MaxConnectionAge equivalent.
//   - verify_peer_san() callback for inbound mTLS SAN allowlist check.

// ---------------------------------------------------------------------------
// NoOpPeerRegistry — placeholder until PeerPool lands
// ---------------------------------------------------------------------------

/// Placeholder [`PeerRegistry`] that refuses every `client()` call. Lets the
/// daemon start when `PeerPool` is not yet implemented; the maintenance
/// worker will exhaust retries on any cross-node intent and mark it
/// abandoned (acceptable until peer dialing actually works). Tests can also
/// use this when they don't need cross-peer behavior.
///
/// TODO(v1): replace with `PeerPool` everywhere it is constructed.
pub struct NoOpPeerRegistry;

#[async_trait]
impl PeerRegistry for NoOpPeerRegistry {
    async fn client(&self, _node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
        anyhow::bail!("peer registry not yet implemented (NoOpPeerRegistry)")
    }
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
