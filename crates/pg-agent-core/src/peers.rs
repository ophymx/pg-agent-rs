//! Outbound peer mesh — [`PeerPool`] dials each `[[pool]]` member via
//! mTLS gRPC and caches the resulting [`tonic::transport::Channel`].
//!
//! # Hot-reload + connection age
//!
//! The client cert (what we *present* to remote peers) is refreshed per
//! handshake via [`crate::certreload::ReloadingClientCertResolver`]. The
//! CA root store (used to *verify* remote peers) is snapshotted at
//! [`PeerPool::new`] — matching the symmetric snapshot in the inbound
//! [`crate::peerserver::PeerServer`]. CA rotation requires a daemon
//! restart.
//!
//! Channels are cached by node id, but only for [`MAX_CONNECTION_AGE`]
//! (12 h, see SPEC §7.3). Past that, the next `client()` call dials
//! fresh — which forces a new TLS handshake and so picks up any
//! SIGHUP-rotated client cert. Without this cap, a long-lived channel
//! could outlive a cert rotation indefinitely.

use crate::certreload::{CertReloader, ReloadingClientCertResolver};
use crate::config::NodeConfig;
use crate::pgstandby::{BasebackupOpts, RewindOpts, WriteRecoveryConfOpts};
use async_trait::async_trait;
use pg_agent_proto::pgagentpb::{
    pg_agent_peer_client::PgAgentPeerClient, BasebackupRequest, ConfigureStandbyRequest,
    CreateSlotRequest, DropSlotRequest, FetchWalRequest, GetStatusRequest, NodeConfigRequest,
    NodeConfigResponse, NodeStatus, OpProgress, PromoteRequest, RewindRequest, StartPgpoolRequest,
    StartRequest, StopRequest,
};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Trait surface
// ---------------------------------------------------------------------------

/// Outbound peer client registry. Hands out a [`PeerClient`] keyed by
/// `NodeConfig` so callers work with resolved config objects rather than
/// raw addresses.
#[async_trait]
pub trait PeerRegistry: Send + Sync {
    /// Returns a client targeting `node`. Caller must not call this for the
    /// local node.
    async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>>;

    /// Tear down all peer connections.
    async fn close(&self) -> anyhow::Result<()>;
}

/// What a peer agent does for us. Mirrors `PgAgentPeer` RPC by RPC.
///
/// Methods are added as their callers materialise. Currently:
/// - `drop_slot` — consumed by the maintenance worker's slot-cleanup retry
/// - `get_node_config` — used by preflight to probe peer reachability +
///   identity
/// - `start` — `RemoteStart` (LocalServer hook) calls this on the target
///   peer to bring its PostgreSQL up
#[async_trait]
pub trait PeerClient: Send + Sync {
    /// `pg_drop_replication_slot($1)` on the target peer's PostgreSQL.
    /// 42710 / "does not exist" should still surface as an error here —
    /// the caller (maintenance worker) decides retry vs. abandon.
    async fn drop_slot(&self, slot_name: &str) -> anyhow::Result<()>;

    /// `pg_create_physical_replication_slot($1)` on the target peer's
    /// PostgreSQL. Used by `LocalServer::ClusterHandoff` so the
    /// newly-promoted primary has a slot for the soon-to-be-standby
    /// (the old primary) before basebackup/rewind starts. Idempotent —
    /// the server-side handler (`peerserver.rs::create_slot`) treats
    /// SQLSTATE 42710 (duplicate_object) as success.
    async fn create_slot(&self, slot_name: &str) -> anyhow::Result<()>;

    /// Read-only "what is your PG port + data dir" call. Cheap enough to
    /// use as a connectivity probe; preflight calls it to verify the
    /// mTLS round-trip works end-to-end before any failover surfaces a
    /// misconfigured link.
    async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse>;

    /// Start PostgreSQL on the peer via its `Systemd::start_postgres`.
    /// Used by `LocalServer::RemoteStart` (the `pgpool_remote_start`
    /// hook). Surface non-`ok` `OpResult` as `Err` so the caller doesn't
    /// have to inspect the payload.
    async fn start(&self) -> anyhow::Result<()>;

    /// StartUnit on the peer's `pgpool2.service` via its
    /// `Systemd::start_pgpool`. Used by `LocalServer::ClusterRecover`
    /// after recovery_first_stage to bring pgpool back up on the
    /// freshly re-cloned target. Idempotent.
    async fn start_pgpool(&self) -> anyhow::Result<()>;

    /// Stream a WAL segment from the peer's archive. Returns
    ///   - `Ok(Some(reader))` — segment found; the reader streams the
    ///     contents (one tonic `WalChunk` per buffered read).
    ///   - `Ok(None)` — segment not on this peer (`Status::NotFound`).
    ///     Callers in `LocalServer::RestoreWal` skip to the next peer.
    ///   - `Err(_)` — anything else (RPC error, transport, bad name).
    ///
    /// The `Option` shape collapses the NotFound vs other-error
    /// distinction at the trait layer so the caller doesn't have to
    /// introspect a tonic Status.
    async fn fetch_wal(
        &self,
        wal_file: &str,
    ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>>;

    /// Mirror of `PgAgentLocal::GetStatus` — read the peer's runtime view.
    /// Used by `FollowPrimary` to skip a deliberately-stopped detached
    /// node and (eventually) by preflight + `pg_agentctl cluster status`.
    async fn get_status(&self) -> anyhow::Result<NodeStatus>;

    /// Stop PostgreSQL on the peer via its `Systemd::stop_postgres`.
    async fn stop(&self) -> anyhow::Result<()>;

    /// Drive `pg_rewind` on the peer against the given primary. The
    /// peer streams `OpProgress`; this method drains the stream and
    /// returns `Ok(())` only after a `phase = "done"` frame. Anything
    /// else (stream error, EOF without "done") surfaces as `Err`.
    async fn rewind(&self, opts: RewindOpts) -> anyhow::Result<()>;

    /// Drive `pg_basebackup` on the peer. Same stream-drain semantics
    /// as `rewind` — `Ok(())` iff a `phase = "done"` frame arrives.
    async fn basebackup(&self, opts: BasebackupOpts) -> anyhow::Result<()>;

    /// Write `recovery.conf` (or equivalent) on the peer so it can
    /// follow `opts.primary_host` as a streaming standby.
    async fn configure_standby(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()>;

    /// Trigger `pg_promote()` on the peer. Used by `LocalServer::Failover`
    /// to promote the chosen new main after the primary goes down.
    async fn promote(&self) -> anyhow::Result<()>;

    // Deliberately absent: the `Reload`, `ReloadPgpool`, and
    // `RemoveVip` RPCs are reserved in pgagent_peer.proto for forward
    // compatibility but not called by any v1 workflow:
    //   - Reload / ReloadPgpool: every config reload in v1 is local
    //     (systemd reload + SIGHUP on the node whose config changed).
    //   - RemoveVip: SPEC §18 — HAProxy fronts the cluster; no VIP to
    //     manage. (Future watchdog `delegate_IP` support tracked in
    //     ROADMAP exploratory.)
}

// ---------------------------------------------------------------------------
// PeerPool — production PeerRegistry
// ---------------------------------------------------------------------------

/// Outbound channel age cap (SPEC §7.3). A SIGHUP-rotated client cert
/// reaches every peer connection within this window, *without* tearing
/// down healthy in-flight channels. The cap is 12 h; the 5-minute grace
/// in the Go version isn't ported — checked-on-access semantics make a
/// soft grace meaningless.
pub const MAX_CONNECTION_AGE: Duration = Duration::from_secs(12 * 60 * 60);

/// Default per-dial connect deadline. Wide enough for healthy LANs with
/// initial TLS handshake; tight enough that an unreachable peer fails
/// fast instead of dragging hook latency.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default per-request deadline. Hook RPCs that need longer (basebackup,
/// rewind) override via their own context.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-call deadline for unary RPCs that block on systemctl + PG state:
/// `Start`, `Stop`, `Promote`. Post-basebackup recovery can hold a
/// fresh standby in "starting" for a couple of minutes before the
/// notify-style postgresql unit reports `active`, and the systemd
/// `StartUnit` D-Bus call doesn't return until then. Five minutes is
/// comfortable headroom without being so long that a wedged peer goes
/// unnoticed.
pub const LONG_RPC_TIMEOUT: Duration = Duration::from_secs(300);

/// HTTP/2 keepalive: ping every 60 s once the channel is idle. Cheap
/// liveness signal that surfaces a partition before it bites a real RPC.
pub const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Client-side bound on establishing a `FetchWal` stream. Five seconds,
/// matching PRECONDITION_TIMEOUT's reasoning: evidence (here, a WAL
/// segment) we cannot *start* receiving in five seconds is on a peer we
/// should skip — the fan-out tries the next one.
pub const FETCH_WAL_SETUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Production [`PeerRegistry`]. mTLS when `cert_reloader.is_some()`,
/// plain TCP otherwise (only valid in `--dev` / single-node deployments;
/// the daemon's preflight rejects mixed remote-peer + no-TLS configs).
pub struct PeerPool {
    agent_port: u16,
    cert_reloader: Option<Arc<CertReloader>>,
    /// Snapshot of the rustls `ClientConfig` for mTLS dials. `None` in
    /// dev (plain TCP). Captured at construction time; SIGHUP refreshes
    /// the *client cert* via the embedded `ReloadingClientCertResolver`,
    /// but the CA root store is snapshotted (matches the inbound side).
    tls_config: Option<Arc<ClientConfig>>,
    channels: Mutex<HashMap<i32, ChannelEntry>>,
    max_age: Duration,
}

struct ChannelEntry {
    client: Arc<dyn PeerClient>,
    created_at: Instant,
}

impl PeerPool {
    /// Build an mTLS pool. Snapshots CA roots from the reloader's current
    /// bundle. Cheap — no I/O. Lazy per-peer dial on first `client()`.
    pub fn new(reloader: Arc<CertReloader>, agent_port: u16) -> anyhow::Result<Arc<Self>> {
        let cfg = build_client_config(&reloader);
        Ok(Arc::new(Self {
            agent_port,
            cert_reloader: Some(reloader),
            tls_config: Some(cfg),
            channels: Mutex::new(HashMap::new()),
            max_age: MAX_CONNECTION_AGE,
        }))
    }

    /// Build a plain-TCP pool for `--dev` / single-node deployments where
    /// no mTLS material has been provisioned. Caller is responsible for
    /// ensuring no remote peers exist (`ServeSettings::reject_insecure_remote_peer`
    /// catches the mismatch at startup).
    pub fn new_dev(agent_port: u16) -> Arc<Self> {
        Arc::new(Self {
            agent_port,
            cert_reloader: None,
            tls_config: None,
            channels: Mutex::new(HashMap::new()),
            max_age: MAX_CONNECTION_AGE,
        })
    }

    /// Test-only constructor with a shorter connection-age cap, so age
    /// expiry is exercisable without burning 12 h of wall clock.
    #[cfg(test)]
    fn with_max_age(self: &Arc<Self>, max_age: Duration) -> Arc<Self> {
        Arc::new(Self {
            agent_port: self.agent_port,
            cert_reloader: self.cert_reloader.clone(),
            tls_config: self.tls_config.clone(),
            channels: Mutex::new(HashMap::new()),
            max_age,
        })
    }

    async fn dial(&self, node: &NodeConfig) -> anyhow::Result<Channel> {
        // Scheme stays `http://` even when we mTLS: tonic uses the scheme
        // to decide whether to negotiate TLS *itself*, and with our custom
        // connector doing the handshake (`connect_with_connector` below)
        // we don't want tonic touching TLS at all. The wire is still
        // encrypted — TlsStream sits between tonic and the socket.
        let addr = format!("http://{}:{}", node.hostname, self.agent_port);
        let endpoint = Endpoint::from_shared(addr.clone())
            .map_err(|e| anyhow::anyhow!("peer dial {addr}: invalid uri: {e}"))?
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            // Channel-wide ceiling only. It must NOT be
            // DEFAULT_REQUEST_TIMEOUT: this tower layer cancels the
            // response future regardless of any per-request
            // `set_timeout`, so a 30 s value silently capped the
            // 300 s LONG_RPC_TIMEOUT that Start/Stop/Promote ask for.
            // Observed in the docker acceptance suite: a partition-
            // triggered failover reported `promote: Timeout expired`
            // at exactly 30 s while the promotion had in fact
            // succeeded server-side (pg_promote() alone waits up to
            // 60 s by default). Short RPCs get their deadline from
            // `short_rpc` below instead.
            .timeout(LONG_RPC_TIMEOUT)
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL);

        match self.tls_config.clone() {
            Some(cfg) => connect_mtls(endpoint, cfg, self.agent_port)
                .await
                .map_err(|e| anyhow::anyhow!("peer dial {addr}: {}", describe(&e))),
            None => endpoint
                .connect()
                .await
                .map_err(|e| anyhow::anyhow!("peer dial {addr}: {}", describe(&e))),
        }
    }
}

/// `tonic::transport::Error::Display` collapses to "transport error" with
/// no causal chain. Walk `.source()` to recover the actual reason.
fn describe(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut src = err.source();
    while let Some(e) = src {
        parts.push(e.to_string());
        src = e.source();
    }
    parts.join(": ")
}

#[async_trait]
impl PeerRegistry for PeerPool {
    async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
        let mut map = self.channels.lock().await;
        if let Some(entry) = map.get(&node.id) {
            if entry.created_at.elapsed() < self.max_age {
                debug!(node = node.id, host = %node.hostname, "peer: cache hit");
                return Ok(entry.client.clone());
            }
            debug!(
                node = node.id,
                age_s = entry.created_at.elapsed().as_secs(),
                "peer: cache entry aged out — redialing"
            );
        }
        info!(node = node.id, host = %node.hostname, "peer: dialing");
        let channel = self.dial(node).await?;
        let client: Arc<dyn PeerClient> = Arc::new(PeerChannel {
            inner: PgAgentPeerClient::new(channel),
        });
        map.insert(
            node.id,
            ChannelEntry {
                client: client.clone(),
                created_at: Instant::now(),
            },
        );
        Ok(client)
    }

    async fn close(&self) -> anyhow::Result<()> {
        let mut map = self.channels.lock().await;
        let n = map.len();
        map.clear();
        debug!(closed = n, "peer pool: closed");
        Ok(())
    }
}

/// Build a tonic `Channel` via a custom connector that does TCP + TLS in
/// one async step. The connector is shaped to satisfy
/// [`Endpoint::connect_with_connector`]'s `Service<Uri>` bound: each
/// invocation yields a `TokioIo<TlsStream<TcpStream>>` (hyper's Read/Write
/// traits via the bridge).
/// Outbound mTLS config: CA roots snapshotted from the reloader's
/// current bundle, client cert resolved per handshake so a SIGHUP
/// rotation is picked up without tearing channels down.
///
/// Shared with the consensus plane, which dials its own channels but
/// must present the same identity to the same allowlist — a Raft plane
/// with its own cert story would be a second thing to rotate and a
/// second way to be locked out of your own cluster.
pub fn build_client_config(reloader: &Arc<CertReloader>) -> Arc<ClientConfig> {
    let bundle = reloader.current();
    // RootCertStore impls Clone in rustls 0.23; the bundle's Arc is
    // shared with the inbound side so we clone the contents into a
    // fresh ClientConfig.
    let roots = (*bundle.roots).clone();
    let resolver = Arc::new(ReloadingClientCertResolver::new(reloader.clone()));
    let mut cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_cert_resolver(resolver);
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Arc::new(cfg)
}

/// Shared with the consensus plane ([`crate::raftnet`]), which dials its
/// own channels but over the same mTLS material and the same connector.
pub(crate) async fn connect_mtls(
    endpoint: Endpoint,
    cfg: Arc<ClientConfig>,
    port: u16,
) -> Result<Channel, tonic::transport::Error> {
    let connector = TlsConnector::from(cfg);
    endpoint
        .connect_with_connector(service_fn(move |uri: Uri| {
            let connector = connector.clone();
            async move {
                let host = uri.host().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "peer uri has no host")
                })?;
                let host_owned = host.to_string();
                let tcp = tokio::net::TcpStream::connect((host, port)).await?;
                let server_name = ServerName::try_from(host_owned).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid server name: {e}"),
                    )
                })?;
                let tls = connector.connect(server_name, tcp).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls))
            }
        }))
        .await
}

// ---------------------------------------------------------------------------
// PeerChannel — wraps PgAgentPeerClient<Channel>, impls PeerClient
// ---------------------------------------------------------------------------

struct PeerChannel {
    inner: PgAgentPeerClient<Channel>,
}

/// Bound a fast unary RPC at [`DEFAULT_REQUEST_TIMEOUT`].
///
/// The channel's own ceiling is [`LONG_RPC_TIMEOUT`] so `Start` / `Stop`
/// / `Promote` get the budget they ask for; everything else is a
/// metadata call that must fail fast instead of riding that ceiling.
/// Enforced client-side here rather than via `Request::set_timeout`,
/// which only writes the `grpc-timeout` header for the *server* to
/// honour — useless when the peer is unreachable, which is exactly when
/// the deadline matters.
async fn short_rpc<T>(
    what: &str,
    fut: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    match tokio::time::timeout(DEFAULT_REQUEST_TIMEOUT, fut).await {
        Ok(v) => v,
        Err(_) => anyhow::bail!(
            "peer {what}: timed out after {}s",
            DEFAULT_REQUEST_TIMEOUT.as_secs()
        ),
    }
}

#[async_trait]
impl PeerClient for PeerChannel {
    async fn create_slot(&self, slot_name: &str) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let req = CreateSlotRequest {
            slot_name: slot_name.to_string(),
        };
        short_rpc("create_slot", async move {
            let resp = client
                .create_slot(req)
                .await
                .map_err(|s| anyhow::anyhow!("peer create_slot: {}", s.message()))?
                .into_inner();
            if !resp.ok {
                anyhow::bail!("peer create_slot: {}", resp.message);
            }
            Ok(())
        })
        .await
    }

    async fn drop_slot(&self, slot_name: &str) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let req = DropSlotRequest {
            slot_name: slot_name.to_string(),
        };
        short_rpc("drop_slot", async move {
            let resp = client
                .drop_slot(req)
                .await
                .map_err(|s| anyhow::anyhow!("peer drop_slot: {}", s.message()))?
                .into_inner();
            if !resp.ok {
                anyhow::bail!("peer drop_slot: {}", resp.message);
            }
            Ok(())
        })
        .await
    }

    async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
        let mut client = self.inner.clone();
        short_rpc("get_node_config", async move {
            Ok(client
                .get_node_config(NodeConfigRequest {})
                .await
                .map_err(|s| anyhow::anyhow!("peer get_node_config: {}", s.message()))?
                .into_inner())
        })
        .await
    }

    async fn start(&self) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let mut req = tonic::Request::new(StartRequest {});
        req.set_timeout(LONG_RPC_TIMEOUT);
        let resp = client
            .start(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer start: {}", s.message()))?
            .into_inner();
        if !resp.ok {
            anyhow::bail!("peer start: {}", resp.message);
        }
        Ok(())
    }

    async fn start_pgpool(&self) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let mut req = tonic::Request::new(StartPgpoolRequest {});
        req.set_timeout(LONG_RPC_TIMEOUT);
        let resp = client
            .start_pgpool(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer start_pgpool: {}", s.message()))?
            .into_inner();
        if !resp.ok {
            anyhow::bail!("peer start_pgpool: {}", resp.message);
        }
        Ok(())
    }

    async fn fetch_wal(
        &self,
        wal_file: &str,
    ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
        use futures_util::TryStreamExt;
        let mut client = self.inner.clone();
        let req = FetchWalRequest {
            wal_file: wal_file.to_string(),
        };
        // Client-side bound on establishing the stream (the same
        // partition trap as PRECONDITION_TIMEOUT and the consensus
        // plane's LEADER_RPC_TIMEOUT — third occurrence of the class,
        // found by acceptance E2): without it, a black-holed peer holds
        // this call to the 300 s channel ceiling, and this call sits on
        // the promotion-critical path via restore_command. Bounds the
        // setup only; the data stream, once flowing, is governed by the
        // channel and HTTP/2 keepalive.
        let established = tokio::time::timeout(FETCH_WAL_SETUP_TIMEOUT, client.fetch_wal(req))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "peer fetch_wal: no response within {FETCH_WAL_SETUP_TIMEOUT:?} \
                     (peer unreachable?)"
                )
            })?;
        match established {
            Ok(resp) => {
                let stream = resp.into_inner();
                // Map gRPC stream items to bytes::Bytes (Buf) + io::Error
                // for tokio_util::io::StreamReader. Errors mid-stream
                // surface to the caller (write_restore) as io::Error.
                let bytes_stream = stream
                    .map_ok(|chunk| bytes::Bytes::from(chunk.data))
                    .map_err(|s| std::io::Error::other(s.to_string()));
                Ok(Some(Box::new(tokio_util::io::StreamReader::new(
                    bytes_stream,
                ))))
            }
            Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
            Err(s) => Err(anyhow::anyhow!("peer fetch_wal: {}", s.message())),
        }
    }

    async fn get_status(&self) -> anyhow::Result<NodeStatus> {
        let mut client = self.inner.clone();
        short_rpc("get_status", async move {
            Ok(client
                .get_status(GetStatusRequest {})
                .await
                .map_err(|s| anyhow::anyhow!("peer get_status: {}", s.message()))?
                .into_inner())
        })
        .await
    }

    async fn stop(&self) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let mut req = tonic::Request::new(StopRequest {});
        req.set_timeout(LONG_RPC_TIMEOUT);
        let resp = client
            .stop(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer stop: {}", s.message()))?
            .into_inner();
        if !resp.ok {
            anyhow::bail!("peer stop: {}", resp.message);
        }
        Ok(())
    }

    async fn rewind(&self, opts: RewindOpts) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let req = RewindRequest {
            primary_host: opts.primary_host,
            primary_port: i32::from(opts.primary_port),
            repl_user: opts.repl_user,
        };
        let stream = client
            .rewind(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer rewind: {}", s.message()))?
            .into_inner();
        drain_progress_stream("peer rewind", stream).await
    }

    async fn basebackup(&self, opts: BasebackupOpts) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let req = BasebackupRequest {
            primary_host: opts.primary_host,
            primary_port: i32::from(opts.primary_port),
            repl_user: opts.repl_user,
            slot_name: opts.slot_name,
        };
        let stream = client
            .basebackup(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer basebackup: {}", s.message()))?
            .into_inner();
        drain_progress_stream("peer basebackup", stream).await
    }

    async fn configure_standby(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let req = ConfigureStandbyRequest {
            primary_host: opts.primary_host,
            primary_port: i32::from(opts.primary_port),
            repl_user: opts.repl_user,
            slot_name: opts.slot_name,
        };
        let resp = client
            .configure_standby(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer configure_standby: {}", s.message()))?
            .into_inner();
        if !resp.ok {
            anyhow::bail!("peer configure_standby: {}", resp.message);
        }
        Ok(())
    }

    async fn promote(&self) -> anyhow::Result<()> {
        let mut client = self.inner.clone();
        let mut req = tonic::Request::new(PromoteRequest {});
        req.set_timeout(LONG_RPC_TIMEOUT);
        let resp = client
            .promote(req)
            .await
            .map_err(|s| anyhow::anyhow!("peer promote: {}", s.message()))?
            .into_inner();
        if !resp.ok {
            anyhow::bail!("peer promote: {}", resp.message);
        }
        Ok(())
    }
}

/// Drain a server-streaming `OpProgress` until a `phase == "done"` frame
/// arrives or the stream errors / EOFs early. Intermediate events are
/// logged at debug level — the caller doesn't need them.
async fn drain_progress_stream(
    operation: &'static str,
    mut stream: tonic::Streaming<OpProgress>,
) -> anyhow::Result<()> {
    loop {
        match stream.message().await {
            Ok(Some(prog)) => {
                debug!(
                    %operation,
                    phase = %prog.phase,
                    bytes_done = prog.bytes_done,
                    bytes_total = prog.bytes_total,
                    "{operation}: progress"
                );
                if prog.phase == "done" {
                    return Ok(());
                }
            }
            Ok(None) => {
                anyhow::bail!("{operation}: stream ended without 'done' phase");
            }
            Err(s) => {
                anyhow::bail!("{operation}: stream error: {}", s.message());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NoOpPeerRegistry — useful for tests that don't need cross-peer behavior
// ---------------------------------------------------------------------------

/// [`PeerRegistry`] that refuses every `client()` call. Useful for tests
/// that don't exercise cross-peer paths and for the construction phase of
/// the daemon before TLS / dev-mode wiring is settled.
pub struct NoOpPeerRegistry;

#[async_trait]
impl PeerRegistry for NoOpPeerRegistry {
    async fn client(&self, _node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
        anyhow::bail!("peer registry: NoOpPeerRegistry (cross-peer calls disabled)")
    }
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::NodeInfo;
    use crate::config::TlsConfig;
    use crate::localdb::{LocalDb, ReplicationLag};
    use crate::peerserver::{PeerServer, PeerTlsConfig};
    use crate::pgstandby::{
        BasebackupOpts, ProgressCb, RewindOpts, StandbyOps, WriteRecoveryConfOpts,
    };
    use crate::systemd::Systemd;
    use crate::walstore::WalStore;
    use pg_agent_proto::pgagentpb::NodeStatus;
    use rcgen::{CertificateParams, IsCa, KeyPair};
    use std::collections::HashSet;
    use std::path::Path;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    // ----- Fakes ------------------------------------------------------------

    struct FakeNodeInfo;

    #[async_trait]
    impl NodeInfo for FakeNodeInfo {
        async fn get_status(&self) -> anyhow::Result<NodeStatus> {
            Ok(NodeStatus {
                is_running: true,
                is_in_recovery: false,
                is_ready: true,
                replication_lag_bytes: 0,
                replication_state: String::new(),
                is_postgres_running: true,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
                timeline_id: 0,
                current_wal_lsn: 0,
            })
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5432,
                pg_data_dir: "/var/lib/postgresql/17/main".into(),
            })
        }
    }

    // ----- rcgen helpers (copied from peerserver tests; same shape) --------

    fn gen_ca() -> (rcgen::Certificate, KeyPair, String) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();
        (ca_cert, ca_key, ca_pem)
    }

    fn gen_leaf(ca_cert: &rcgen::Certificate, ca_key: &KeyPair, sans: &[&str]) -> (String, String) {
        let leaf_key = KeyPair::generate().unwrap();
        let params =
            CertificateParams::new(sans.iter().map(|s| (*s).to_string()).collect::<Vec<_>>())
                .unwrap();
        let cert = params.signed_by(&leaf_key, ca_cert, ca_key).unwrap();
        (cert.pem(), leaf_key.serialize_pem())
    }

    fn write_pems(dir: &Path, name: &str, ca: &str, leaf: &str, key: &str) -> TlsConfig {
        let ca_path = dir.join(format!("{name}-ca.crt"));
        let cert_path = dir.join(format!("{name}.crt"));
        let key_path = dir.join(format!("{name}.key"));
        std::fs::write(&ca_path, ca).unwrap();
        std::fs::write(&cert_path, leaf).unwrap();
        std::fs::write(&key_path, key).unwrap();
        TlsConfig {
            ca_cert: Some(ca_path),
            cert: Some(cert_path),
            key: Some(key_path),
        }
    }

    /// Spin up a PeerServer in mTLS mode bound to 127.0.0.1:0; return the
    /// port + a shutdown handle. Allowlist defaults to the loopback name
    /// the client cert will present.
    async fn spawn_mtls_server(
        tls: PeerTlsConfig,
    ) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let h = tokio::spawn(async move {
            let sd: Arc<dyn Systemd> = Arc::new(NoOpSystemd);
            let db: Arc<dyn LocalDb> = Arc::new(NoOpDb);
            let standby: Arc<dyn StandbyOps> = Arc::new(NoOpStandby);
            let wal: Arc<dyn WalStore> = Arc::new(NoOpWal);
            let _ = PeerServer::new(
                Arc::new(FakeNodeInfo),
                sd,
                db,
                standby,
                wal,
                Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new()),
            )
            .serve(listener, Some(tls), s)
            .await;
        });
        // Brief settle so the server's accept loop is ready.
        tokio::time::sleep(Duration::from_millis(50)).await;
        (port, shutdown, h)
    }

    // ----- minimal no-op deps for the PeerServer test instance ---------

    struct NoOpSystemd;
    #[async_trait]
    impl Systemd for NoOpSystemd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn status_postgres(&self) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn status_pgpool(&self) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct NoOpDb;
    #[async_trait]
    impl LocalDb for NoOpDb {
        async fn promote(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            Ok(ReplicationLag::default())
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct NoOpStandby;
    #[async_trait]
    impl StandbyOps for NoOpStandby {
        async fn basebackup(&self, _: BasebackupOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn rewind(&self, _: RewindOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn write_recovery_conf(&self, _: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct NoOpWal;
    #[async_trait]
    impl WalStore for NoOpWal {
        async fn open_archive(
            &self,
            _: &str,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, pgman::walstore::WalStoreError>
        {
            Err(pgman::walstore::WalStoreError::WalNotFound("noop".into()))
        }
        async fn write_restore(
            &self,
            _: &std::path::Path,
            _: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        ) -> Result<(), pgman::walstore::WalStoreError> {
            Ok(())
        }
    }

    // ----- direct unit tests ----------------------------------------------

    #[tokio::test]
    async fn no_op_registry_errors_on_client() {
        let reg = NoOpPeerRegistry;
        let node = NodeConfig {
            id: 1,
            hostname: "x".into(),
        };
        match reg.client(&node).await {
            Err(e) => assert!(e.to_string().contains("NoOpPeerRegistry"), "got {e}"),
            Ok(_) => panic!("NoOpPeerRegistry should refuse"),
        }
        reg.close().await.unwrap();
    }

    #[tokio::test]
    async fn new_dev_builds_without_tls() {
        let pool = PeerPool::new_dev(9701);
        assert!(pool.tls_config.is_none());
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn new_with_reloader_builds_tls_config() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();
        let (leaf, key) = gen_leaf(&ca_cert, &ca_key, &["localhost"]);
        let tls_cfg = write_pems(tmp.path(), "self", &ca_pem, &leaf, &key);
        let reloader = Arc::new(CertReloader::new(tls_cfg).unwrap());
        let pool = PeerPool::new(reloader, 9701).unwrap();
        assert!(pool.tls_config.is_some());
        pool.close().await.unwrap();
    }

    // ----- mTLS round-trip --------------------------------------------------

    #[tokio::test]
    async fn mtls_round_trip_get_node_config() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();

        // Server cert SAN = "127.0.0.1" so rustls's server-name check
        // passes when we dial via that hostname. Client cert SAN =
        // "node1.local" → allowlist below.
        let (server_leaf, server_key) = gen_leaf(&ca_cert, &ca_key, &["127.0.0.1"]);
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["node1.local"]);

        let server_tls_files = write_pems(tmp.path(), "server", &ca_pem, &server_leaf, &server_key);
        let server_reloader = Arc::new(CertReloader::new(server_tls_files).unwrap());

        let client_tls_files = write_pems(tmp.path(), "client", &ca_pem, &client_leaf, &client_key);
        let client_reloader = Arc::new(CertReloader::new(client_tls_files).unwrap());

        let mut allowlist = HashSet::new();
        allowlist.insert("node1.local".to_string());

        let server_tls = PeerTlsConfig {
            reloader: server_reloader,
            allowed_peer_sans: allowlist,
        };
        let (port, shutdown, handle) = spawn_mtls_server(server_tls).await;

        let pool = PeerPool::new(client_reloader, port).unwrap();
        let node = NodeConfig {
            id: 99,
            hostname: "127.0.0.1".into(),
        };
        let client = pool.client(&node).await.expect("client dial");
        let resp = client
            .get_node_config()
            .await
            .expect("get_node_config round trip");
        assert_eq!(resp.pg_port, 5432);

        // Cache hit on second call (same Arc pointer means PeerPool
        // returned the cached client).
        let again = pool.client(&node).await.unwrap();
        assert!(
            Arc::ptr_eq(&client, &again),
            "second call should return cached client"
        );

        pool.close().await.unwrap();
        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn cache_redials_after_age_expiry() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();
        let (server_leaf, server_key) = gen_leaf(&ca_cert, &ca_key, &["127.0.0.1"]);
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["node1.local"]);

        let server_tls_files = write_pems(tmp.path(), "server", &ca_pem, &server_leaf, &server_key);
        let server_reloader = Arc::new(CertReloader::new(server_tls_files).unwrap());
        let client_tls_files = write_pems(tmp.path(), "client", &ca_pem, &client_leaf, &client_key);
        let client_reloader = Arc::new(CertReloader::new(client_tls_files).unwrap());

        let mut allowlist = HashSet::new();
        allowlist.insert("node1.local".to_string());
        let (port, shutdown, handle) = spawn_mtls_server(PeerTlsConfig {
            reloader: server_reloader,
            allowed_peer_sans: allowlist,
        })
        .await;

        // 5 ms age cap so the second call falls past it.
        let pool_default = PeerPool::new(client_reloader, port).unwrap();
        let pool = pool_default.with_max_age(Duration::from_millis(5));
        let node = NodeConfig {
            id: 7,
            hostname: "127.0.0.1".into(),
        };

        let first = pool.client(&node).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let second = pool.client(&node).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "aged-out entry should yield a fresh client"
        );

        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn drop_slot_round_trip_succeeds() {
        // PeerServer's drop_slot is now wired through to the LocalDb stub
        // (NoOpDb here) — verifies the end-to-end client → server →
        // localdb path completes without error.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();
        let (server_leaf, server_key) = gen_leaf(&ca_cert, &ca_key, &["127.0.0.1"]);
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["node1.local"]);
        let server_tls_files = write_pems(tmp.path(), "server", &ca_pem, &server_leaf, &server_key);
        let client_tls_files = write_pems(tmp.path(), "client", &ca_pem, &client_leaf, &client_key);

        let server_reloader = Arc::new(CertReloader::new(server_tls_files).unwrap());
        let client_reloader = Arc::new(CertReloader::new(client_tls_files).unwrap());
        let mut allowlist = HashSet::new();
        allowlist.insert("node1.local".to_string());
        let (port, shutdown, handle) = spawn_mtls_server(PeerTlsConfig {
            reloader: server_reloader,
            allowed_peer_sans: allowlist,
        })
        .await;

        let pool = PeerPool::new(client_reloader, port).unwrap();
        let node = NodeConfig {
            id: 1,
            hostname: "127.0.0.1".into(),
        };
        let client = pool.client(&node).await.unwrap();
        client
            .drop_slot("node2")
            .await
            .expect("drop_slot round trip");

        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn dial_fails_fast_when_peer_unreachable() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["node1.local"]);
        let client_tls_files = write_pems(tmp.path(), "client", &ca_pem, &client_leaf, &client_key);
        let client_reloader = Arc::new(CertReloader::new(client_tls_files).unwrap());

        let pool = PeerPool::new(client_reloader, 1).unwrap(); // port 1 — unbound
        let node = NodeConfig {
            id: 1,
            hostname: "127.0.0.1".into(),
        };
        // `Arc<dyn PeerClient>` doesn't impl Debug, so unwrap_err won't
        // compile — match on the Result instead.
        match pool.client(&node).await {
            Err(e) => assert!(e.to_string().contains("peer dial"), "got {e}"),
            Ok(_) => panic!("expected dial to fail (port 1 is not bound)"),
        }
    }

    // Compile-time check: `cert_reloader` field is still in scope even if
    // future paths stop reading it. Sink the warning explicitly.
    #[allow(dead_code)]
    fn _assert_field_kept(p: &PeerPool) -> Option<&Arc<CertReloader>> {
        p.cert_reloader.as_ref()
    }
}
