//! pgpool Control Protocol client — shells out to `pcp_attach_node` and
//! `pcp_node_count` (`-w` for no password prompt; auth via `~/.pcppass`).
//!
//! Note: `pcp_node_count` returns the *configured* backend count per pgpool's
//! upstream docs — not the count of currently-up backends. See SPEC §9.2.

use async_trait::async_trait;

#[async_trait]
pub trait Pcp: Send + Sync {
    async fn attach_node(&self, node_id: i32) -> anyhow::Result<()>;
    /// Total backends defined in pgpool.conf (does not distinguish attached
    /// from detached).
    async fn node_count(&self) -> anyhow::Result<i32>;
}

// TODO(v1): PcpCli impl that exec's pcp_attach_node / pcp_node_count.
