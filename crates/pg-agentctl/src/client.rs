//! Local Unix-socket gRPC client for the operator CLI.
//!
//! Every `pg_agentctl` subcommand that talks to a running `pg_agentd`
//! dials it via the Unix socket at `unix_socket` from config (or
//! `--socket` override). No mTLS — the socket is mode `0600
//! postgres:postgres` and filesystem permissions ARE the access control
//! (SPEC §10.1). Symmetric with how `pg_agentc` already reaches the
//! daemon, just exposed as a small helper instead of duplicated.
//!
//! See [`crate::config_loader::resolve_socket_path`] for how the path
//! falls through CLI flag → config → default.

use anyhow::Context;
use pg_agent_proto::pgagentpb::pg_agent_local_client::PgAgentLocalClient;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Per-call deadline applied to every dial. Tight enough that a hung
/// daemon fails fast on the operator's prompt; wide enough for a real
/// startup where the agent is still binding listeners.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect to the daemon's local Unix socket and hand back a
/// generated tonic client. The URI scheme stays `http://` even
/// though the wire is a Unix socket — tonic uses the scheme to
/// decide whether to negotiate TLS itself, and our custom connector
/// is the actual transport. Same trick as the peer pool's mTLS path
/// (`pg-agent-core::peers`).
#[allow(dead_code)] // wired up by subsequent commits (preflight, cluster init, …)
pub async fn dial_local(socket: &Path) -> anyhow::Result<PgAgentLocalClient<Channel>> {
    // `Endpoint::from_static("http://[::]:50051")` works as a dummy
    // because the connector below ignores the URI completely. Any
    // valid-shaped URI would do; this one matches tonic's own
    // examples.
    let endpoint = Endpoint::from_static("http://[::]:50051").connect_timeout(CONNECT_TIMEOUT);

    let socket: PathBuf = socket.to_path_buf();
    let socket_display = socket.display().to_string();

    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                let stream = UnixStream::connect(&socket).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .with_context(|| {
            format!(
                "dial pg_agentd unix socket {socket_display} \
                 (is the daemon running? `systemctl status pg_agentd.service`)"
            )
        })?;

    Ok(PgAgentLocalClient::new(channel))
}
