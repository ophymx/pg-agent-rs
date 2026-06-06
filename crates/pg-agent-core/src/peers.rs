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

/// Outbound peer client surface. Implementations key on `NodeConfig` so
/// callers work with resolved config objects rather than raw addresses.
#[async_trait]
pub trait Peers: Send + Sync {
    /// Returns a client targeting `node`. Caller must not call this for the
    /// local node.
    async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>>;

    /// Tear down all peer connections.
    async fn close(&self) -> anyhow::Result<()>;
}

/// What a peer agent does for us. Mirrors `PgAgentPeer` RPC by RPC; the
/// streaming RPCs (Basebackup, Rewind, FetchWal) expose `mpsc::Receiver`
/// to keep the handler code transport-agnostic.
#[async_trait]
pub trait PeerClient: Send + Sync {
    // TODO(v1): mirror the proto surface. Keep async fn signatures small
    // and let the impl translate to tonic. Streaming RPCs should return
    // `impl Stream<Item = …>` or `mpsc::Receiver` so handlers don't import
    // tonic types.
}

// TODO(v1):
//   - PeerTransport { reloader: Option<Arc<CertReloader>>, allowed_peers: HashSet<String> }
//   - PeerPool that lazily dials each peer and caches the tonic::Channel,
//     with the 12 h MaxConnectionAge equivalent.
//   - verify_peer_san() callback for inbound mTLS SAN allowlist check.
