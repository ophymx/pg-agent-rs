//! `PgAgentPeer` tonic service — mTLS TCP gRPC for the persistent peer mesh.
//!
//! Inbound auth is mTLS: the client must present a cert whose chain
//! validates against our CA AND whose DNS SAN appears in the configured
//! allowlist (the hostnames of every node in `[[pool]]`). Both checks live
//! in `AllowlistClientCertVerifier` (private to this module).
//!
//! # Scope
//!
//! Only the read-only RPCs (`GetStatus`, `GetNodeConfig`) are wired through
//! to [`NodeInfo`]. The 12 action RPCs return `Status::unimplemented` until
//! the corresponding handlers in `agent.rs` land. The streaming RPCs
//! (`Basebackup`, `Rewind`, `FetchWal`) declare `Pin<Box<dyn Stream>>`
//! associated types so the trait compiles — no stream is ever constructed.
//!
//! # Cert hot-reload semantics
//!
//! The **server cert** (what we present to inbound peers) refreshes on
//! every handshake via [`crate::certreload::ReloadingServerCertResolver`]
//! — SIGHUP-rotated material reaches new connections immediately.
//!
//! The **root store** (used to validate the inbound peer's cert chain)
//! is snapshotted from [`crate::certreload::CertBundle::roots`] at
//! `serve()` time. CA rotation is treated as "rare enough to restart
//! the daemon for". Server-cert rotation is the hot path; CA rotation is
//! a planned event.

use crate::agent::NodeInfo;
use crate::certreload::{extract_sans, CertReloader, ReloadingServerCertResolver};
use crate::localdb::LocalDb;
use crate::pgstandby::{
    allowed_slot_name, BasebackupOpts, ProgressCb, RewindOpts, StandbyOps, WriteRecoveryConfOpts,
};
use crate::raftnet::RaftGrpcService;
use crate::systemd::Systemd;
use crate::walstore::WalStore;
use futures_core::Stream;
use pg_agent_proto::pgagentpb::{
    pg_agent_peer_server::{PgAgentPeer, PgAgentPeerServer},
    pg_agent_raft_server::PgAgentRaftServer,
    AttachNodeRequest, BasebackupRequest, ConfigureStandbyRequest, CreateSlotRequest,
    DropSlotRequest, FetchWalRequest, GetStatusRequest, NodeConfigRequest, NodeConfigResponse,
    NodeStatus, OpProgress, OpResult, PromoteRequest, RewindRequest, StartPgpoolRequest,
    StartRequest, StopRequest, WalChunk,
};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
use rustls::DistinguishedName;
use rustls::{DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{codec::CompressionEncoding, transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// PeerTlsConfig
// ---------------------------------------------------------------------------

/// Inbound mTLS configuration for the peer server. Combines the live
/// cert material (via [`CertReloader`]) with the SAN allowlist (the
/// hostnames of every peer in `[[pool]]`).
///
/// Constructed in [`crate::agent::Agent::serve`] from `Options` —
/// callers don't build this directly.
#[derive(Clone)]
pub struct PeerTlsConfig {
    pub reloader: Arc<CertReloader>,
    /// DNS SANs that, when presented on the inbound TLS handshake, are
    /// accepted. Built from `NodePool::members[*].hostname` (every node in
    /// `[[pool]]`, including the local one — a node may legitimately dial
    /// itself in some edge cases).
    pub allowed_peer_sans: HashSet<String>,
}

// ---------------------------------------------------------------------------
// Custom ClientCertVerifier
// ---------------------------------------------------------------------------

/// Verifier that delegates chain + signature validation to
/// [`WebPkiClientVerifier`] and then enforces a DNS-SAN allowlist on top.
///
/// Rejects with `rustls::Error::General` when the chain validates but no
/// SAN matches — the rustls handshake then fails with a descriptive
/// reason, surfaced as a TLS error to the client and a `warn!` to our
/// logs. Cert-chain failures (expired, untrusted issuer, signature bad)
/// surface through the WebPki path with their own error variants.
#[derive(Debug)]
struct AllowlistClientCertVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    allowed_dns_sans: HashSet<String>,
}

impl AllowlistClientCertVerifier {
    fn new(
        roots: Arc<RootCertStore>,
        allowed: HashSet<String>,
    ) -> Result<Arc<Self>, rustls::server::VerifierBuilderError> {
        let inner = WebPkiClientVerifier::builder(roots).build()?;
        Ok(Arc::new(Self {
            inner,
            allowed_dns_sans: allowed,
        }))
    }
}

impl ClientCertVerifier for AllowlistClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        // 1. Standard PKI chain + validity-window check.
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;

        // 2. SAN allowlist enforcement. Parse SANs from the DER cert.
        // extract_sans is infallible on shape (returns empty vecs for a
        // cert with no SAN ext) but can fail on malformed DER — which
        // should be impossible here because WebPki just accepted it.
        let (dns_sans, _ip_sans) = extract_sans(end_entity.as_ref())
            .map_err(|e| rustls::Error::General(format!("parse client cert SANs: {e}")))?;

        if dns_sans
            .iter()
            .any(|s| self.allowed_dns_sans.contains(s.as_str()))
        {
            debug!(?dns_sans, "peer cert: SAN allowlist match");
            Ok(verified)
        } else {
            warn!(
                ?dns_sans,
                allowed = ?self.allowed_dns_sans,
                "peer cert: rejected — no DNS SAN in allowlist"
            );
            Err(rustls::Error::General(format!(
                "peer cert DNS SANs {dns_sans:?} not in allowlist"
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

/// Build the rustls `ServerConfig` from a [`PeerTlsConfig`]:
///   - `ReloadingServerCertResolver` for the leaf cert (refreshes per handshake)
///   - `AllowlistClientCertVerifier` for inbound client cert + SAN allowlist
///     (roots snapshotted at build time — see module-level docs)
fn build_server_config(tls: &PeerTlsConfig) -> anyhow::Result<ServerConfig> {
    let bundle = tls.reloader.current();
    let verifier =
        AllowlistClientCertVerifier::new(bundle.roots.clone(), tls.allowed_peer_sans.clone())
            .map_err(|e| anyhow::anyhow!("peer server: build client cert verifier: {e}"))?;

    let cert_resolver = Arc::new(ReloadingServerCertResolver::new(tls.reloader.clone()));

    let mut server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(cert_resolver);

    // tonic's gRPC wire protocol is HTTP/2-only; advertise it via ALPN so
    // browsers/clients negotiate it correctly.
    server_config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(server_config)
}

// ---------------------------------------------------------------------------
// PeerServer
// ---------------------------------------------------------------------------

pub struct PeerServer {
    node_info: Arc<dyn NodeInfo>,
    sd: Arc<dyn Systemd>,
    db: Arc<dyn LocalDb>,
    standby: Arc<dyn StandbyOps>,
    wal: Arc<dyn WalStore>,
    /// Consulted by [`PeerServer::drop_slot`] before destroying a slot.
    /// The slot lives here, so this node's journal — not the caller's —
    /// is the authority on whether an orchestration still needs it.
    inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
    /// LOCAL pgpool control plane, for [`PeerServer::attach_node`] —
    /// the receiving half of `cluster recover`'s attach fan-out
    /// (hook-contract §3: only this node can attach on this instance).
    pcp: Arc<dyn crate::pcp::Pcp>,
    /// The consensus plane, served on this same listener when Raft is
    /// running (promotion-authority §5, "Transport"): same port, same
    /// certs, same SAN allowlist. `None` only where no Raft instance is
    /// wired — the peer-server unit tests, never a running daemon.
    raft: Option<PgAgentRaftServer<RaftGrpcService>>,
}

impl PeerServer {
    pub fn new(
        node_info: Arc<dyn NodeInfo>,
        sd: Arc<dyn Systemd>,
        db: Arc<dyn LocalDb>,
        standby: Arc<dyn StandbyOps>,
        wal: Arc<dyn WalStore>,
        inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
        pcp: Arc<dyn crate::pcp::Pcp>,
    ) -> Self {
        Self {
            node_info,
            sd,
            db,
            standby,
            wal,
            inflight,
            pcp,
            raft: None,
        }
    }

    /// Also serve the consensus plane on this listener.
    ///
    /// Sharing the listener is deliberate and is only the *inbound*
    /// half: outbound, Raft dials its own channels, because heartbeats
    /// must not queue behind a basebackup. See [`crate::raftnet`].
    pub fn with_raft(
        mut self,
        raft: crate::raftnet::PgAgentRaftHandle,
        reader: crate::raftstore::ClusterStateReader,
    ) -> Self {
        self.raft = Some(RaftGrpcService::new(raft, reader).into_server());
        self
    }

    /// The orchestration that owns `slot_name`, if any — in flight, or
    /// finished with the rebuilt node not yet observed alive
    /// (ownership ends at that event;
    /// [`crate::localserver::CROSS_OP_GRACE`] is only the backstop for
    /// a node that never comes up).
    ///
    /// Slots are always named `node{id}`, which is the link
    /// between a slot and the op that owns the node it belongs to. An
    /// unparseable name means no owner: the guard exists to protect
    /// known orchestrations, not to block anything unfamiliar.
    async fn slot_owner(&self, slot_name: &str) -> Option<crate::inflight_ops::InflightOp> {
        let db = self.db.clone();
        let s = slot_name.to_string();
        crate::inflight_ops::owner_of_slot_observing(
            self.inflight.as_ref(),
            slot_name,
            crate::localserver::CROSS_OP_GRACE,
            move || async move { db.slot_active(&s).await },
        )
        .await
    }

    /// Serve until `shutdown` cancels.
    ///
    /// `tls == None` is the dev-mode path: plain TCP, no client cert
    /// verification (caller is responsible for `--dev` + no remote peers).
    /// `tls == Some(...)` wraps every accepted TCP stream with mTLS:
    /// chain validation + SAN allowlist before tonic ever sees the bytes.
    pub async fn serve(
        self,
        listener: TcpListener,
        tls: Option<PeerTlsConfig>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        match tls {
            None => self.serve_plain(listener, shutdown).await,
            Some(cfg) => self.serve_mtls(listener, cfg, shutdown).await,
        }
    }

    async fn serve_plain(
        mut self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("peer server: starting (plain TCP — dev mode)");
        let incoming = TcpListenerStream::new(listener);
        let raft = self.raft.take();
        Server::builder()
            .add_optional_service(raft)
            .add_service(peer_service(self))
            .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await })
            .await
            .map(|()| {
                info!("peer server: shut down cleanly");
            })
            .map_err(|e| {
                warn!(?e, "peer server: shut down with error");
                e.into()
            })
    }

    async fn serve_mtls(
        mut self,
        listener: TcpListener,
        tls: PeerTlsConfig,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let server_config = build_server_config(&tls)?;
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        info!(
            allowlist_size = tls.allowed_peer_sans.len(),
            "peer server: starting (mTLS)"
        );

        // Accept loop: TCP-accept → TLS-accept → push to channel. A failed
        // TLS handshake is logged and dropped — it must not propagate into
        // tonic (which would terminate the entire incoming stream on a
        // single bad client).
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, std::io::Error>,
        >(64);
        let accept_shutdown = shutdown.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_shutdown.cancelled() => {
                        debug!("peer server: accept loop received shutdown");
                        return;
                    }
                    res = listener.accept() => {
                        let (stream, peer_addr) = match res {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(?e, "peer server: tcp accept");
                                continue;
                            }
                        };
                        let acceptor = acceptor.clone();
                        let tx = tx.clone();
                        // Per-connection handshake task — slow / hung
                        // handshakes don't stall the accept loop.
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    if tx.send(Ok(tls_stream)).await.is_err() {
                                        debug!(
                                            ?peer_addr,
                                            "peer server: incoming sink closed; dropping connection"
                                        );
                                    }
                                }
                                Err(e) => {
                                    // Includes our AllowlistClientCertVerifier
                                    // rejections (TLS error with our General msg).
                                    warn!(?e, ?peer_addr, "peer server: tls handshake failed");
                                }
                            }
                        });
                    }
                }
            }
        });

        let incoming = ReceiverStream::new(rx);
        let raft = self.raft.take();
        let serve_result = Server::builder()
            .add_optional_service(raft)
            .add_service(peer_service(self))
            .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await })
            .await;

        accept_handle.abort();
        match serve_result {
            Ok(()) => {
                info!("peer server: shut down cleanly");
                Ok(())
            }
            Err(e) => {
                warn!(?e, "peer server: shut down with error");
                Err(e.into())
            }
        }
    }
}

// Associated stream types for the three server-streaming RPCs. We never
// construct values of these in the skeleton (every handler returns
// unimplemented), but the trait requires the types to exist.
type ProgressStream = Pin<Box<dyn Stream<Item = Result<OpProgress, Status>> + Send + 'static>>;
type WalChunkStream = Pin<Box<dyn Stream<Item = Result<WalChunk, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl PgAgentPeer for PeerServer {
    type BasebackupStream = ProgressStream;
    type RewindStream = ProgressStream;
    type FetchWalStream = WalChunkStream;

    // ----- read-only --------------------------------------------------------

    async fn get_status(
        &self,
        _req: Request<GetStatusRequest>,
    ) -> Result<Response<NodeStatus>, Status> {
        self.node_info
            .get_status()
            .await
            .map(Response::new)
            .map_err(internal)
    }

    async fn get_node_config(
        &self,
        _req: Request<NodeConfigRequest>,
    ) -> Result<Response<NodeConfigResponse>, Status> {
        self.node_info
            .get_node_config()
            .await
            .map(Response::new)
            .map_err(internal)
    }

    // ----- service control ----------------------------------------------

    async fn start(&self, _req: Request<StartRequest>) -> Result<Response<OpResult>, Status> {
        info!("peer: Start");
        self.sd.start_postgres().await.map_err(internal)?;
        Ok(Response::new(ok()))
    }

    async fn stop(&self, _req: Request<StopRequest>) -> Result<Response<OpResult>, Status> {
        info!("peer: Stop");
        self.sd.stop_postgres().await.map_err(internal)?;
        Ok(Response::new(ok()))
    }

    async fn start_pgpool(
        &self,
        _req: Request<StartPgpoolRequest>,
    ) -> Result<Response<OpResult>, Status> {
        info!("peer: StartPgpool");
        self.sd.start_pgpool().await.map_err(internal)?;
        Ok(Response::new(ok()))
    }

    /// The receiving half of `cluster recover`'s attach fan-out
    /// (hook-contract §3). Finding-16 semantics, server-side:
    /// - attach ONLY a backend this instance holds DOWN — blindly
    ///   attaching an up backend makes pgpool re-run its failover
    ///   processing and transiently degenerate healthy backends;
    /// - when the current primary's backend is down here too, attach
    ///   it FIRST — a standby attach into a primary-less map blocks in
    ///   `find_primary_node_repeatedly` (300 s), wedging every later
    ///   pcp request behind it.
    /// pgpool being down (or pcp failing) is an `ok=false` answer, not
    /// a gRPC error: the caller's fan-out is best-effort per instance.
    async fn attach_node(
        &self,
        req: Request<AttachNodeRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        info!(
            node_id = req.node_id,
            primary_node_id = req.primary_node_id,
            "peer: AttachNode"
        );
        let infos = match self.pcp.node_info_all().await {
            Ok(v) => v,
            Err(e) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!("attach_node: local pgpool map unavailable: {e}"),
                }));
            }
        };
        let is_down = |id: i32| infos.iter().any(|n| n.id == id && !n.is_up());
        if req.primary_node_id != req.node_id && is_down(req.primary_node_id) {
            if let Err(e) = self.pcp.attach_node(req.primary_node_id).await {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "attach_node: refusing to attach node {} into a primary-less map — \
                         attaching primary backend {} first failed: {e}",
                        req.node_id, req.primary_node_id
                    ),
                }));
            }
        }
        if !is_down(req.node_id) {
            return Ok(Response::new(OpResult {
                ok: true,
                message: format!(
                    "attach_node: backend {} already up (or absent) on this instance; no action",
                    req.node_id
                ),
            }));
        }
        match self.pcp.attach_node(req.node_id).await {
            Ok(()) => Ok(Response::new(OpResult {
                ok: true,
                message: format!("attach_node: backend {} attached", req.node_id),
            })),
            Err(e) => Ok(Response::new(OpResult {
                ok: false,
                message: format!("attach_node: pcp_attach_node({}): {e}", req.node_id),
            })),
        }
    }

    async fn promote(&self, _req: Request<PromoteRequest>) -> Result<Response<OpResult>, Status> {
        info!("peer: Promote");
        self.db.promote().await.map_err(internal)?;
        Ok(Response::new(ok()))
    }

    // ----- replication slot management ----------------------------------

    async fn create_slot(
        &self,
        req: Request<CreateSlotRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        info!(slot = %req.slot_name, "peer: CreateSlot");
        validate_slot_name(&req.slot_name)?;
        self.db
            .create_slot(&req.slot_name)
            .await
            .map_err(internal)?;
        Ok(Response::new(ok()))
    }

    /// Drop a replication slot on this node — **unless** a local
    /// orchestration owns it.
    ///
    /// The guard belongs here rather than only at the call sites
    /// because callers are plural and some of them are *stale*: a
    /// queued `drop_slot_cleanup` maintenance intent on another node
    /// retries with exponential backoff, so a drop request created
    /// before a recovery started can land minutes into it. Observed in
    /// the acceptance suite: a peer's retry loop deleted the slot
    /// `recovery_1st_stage` had just created, once per backoff step,
    /// leaving a rebuilt standby that could never stream. Only this
    /// node knows whether the slot is currently spoken for.
    ///
    /// Answers `ok=true` when refusing, deliberately: the caller's
    /// cleanup is genuinely no longer needed, and returning an error
    /// would keep a maintenance intent retrying against a slot that is
    /// now in legitimate use.
    async fn drop_slot(&self, req: Request<DropSlotRequest>) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        info!(slot = %req.slot_name, "peer: DropSlot");
        validate_slot_name(&req.slot_name)?;
        if let Some(owner) = self.slot_owner(&req.slot_name).await {
            info!(
                slot = %req.slot_name,
                op = %owner.payload.op_name(),
                id = %owner.id,
                phase = %owner.phase,
                "peer: DropSlot refused — an orchestration owns this slot"
            );
            return Ok(Response::new(OpResult {
                ok: true,
                message: format!(
                    "slot {} retained: in-flight {} (id={}, phase={}) owns it",
                    req.slot_name,
                    owner.payload.op_name(),
                    owner.id,
                    owner.phase
                ),
            }));
        }
        self.db.drop_slot(&req.slot_name).await.map_err(internal)?;
        Ok(Response::new(ok()))
    }

    // ----- standby config ------------------------------------------------

    async fn configure_standby(
        &self,
        req: Request<ConfigureStandbyRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        info!(
            primary_host = %req.primary_host,
            primary_port = req.primary_port,
            repl_user = %req.repl_user,
            slot = %req.slot_name,
            "peer: ConfigureStandby"
        );
        let opts = WriteRecoveryConfOpts {
            primary_host: req.primary_host,
            primary_port: u16::try_from(req.primary_port)
                .map_err(|_| Status::invalid_argument("primary_port must fit in u16 and be > 0"))?,
            repl_user: req.repl_user,
            slot_name: req.slot_name,
        };
        opts.validate()
            .map_err(|e| Status::invalid_argument(format!("invalid configure_standby: {e}")))?;
        self.standby
            .write_recovery_conf(opts)
            .await
            .map_err(internal)?;
        Ok(Response::new(ok()))
    }

    // ----- streaming: subprocess-backed ----------------------------------

    async fn basebackup(
        &self,
        req: Request<BasebackupRequest>,
    ) -> Result<Response<Self::BasebackupStream>, Status> {
        let req = req.into_inner();
        info!(
            slot = %req.slot_name,
            primary_host = %req.primary_host,
            "peer: Basebackup"
        );

        let opts = BasebackupOpts {
            primary_host: req.primary_host,
            primary_port: u16::try_from(req.primary_port)
                .map_err(|_| Status::invalid_argument("primary_port must fit in u16 and be > 0"))?,
            repl_user: req.repl_user,
            slot_name: req.slot_name,
        };
        opts.validate()
            .map_err(|e| Status::invalid_argument(format!("invalid basebackup: {e}")))?;

        // Refuse to wipe a live datadir. pg_basebackup itself refuses a
        // non-empty target dir, and StandbyOps::basebackup clears `$PGDATA`
        // contents — catching the "PG running" case here is the only way
        // to keep us from blowing away a live cluster.
        //
        // "Live" includes MID-SHUTDOWN: `status_postgres` reports
        // `deactivating` as not-running, but the dying postmaster still
        // owns $PGDATA (its shutdown checkpoint can take seconds, and a
        // fence-then-recover legitimately arrives inside that window —
        // observed as the pgdata clear racing the postmaster's own file
        // deletions). Wait, bounded, for the unit to settle before
        // judging.
        let settle_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while self.sd.postgres_settling().await.map_err(internal)? {
            if tokio::time::Instant::now() >= settle_deadline {
                return Err(Status::failed_precondition(
                    "refusing to basebackup: postgres unit stuck in a transitional \
                     state for 30s (mid-start or mid-shutdown owns $PGDATA)",
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        let pg_running = self.sd.status_postgres().await.map_err(internal)?;
        if pg_running {
            return Err(Status::failed_precondition(
                "refusing to basebackup while postgres is running",
            ));
        }

        let standby = self.standby.clone();
        Ok(Response::new(spawn_progress_stream(
            "Basebackup",
            "streaming",
            move |tx| async move { standby.basebackup(opts, Some(progress_cb(tx))).await },
        )))
    }

    async fn rewind(
        &self,
        req: Request<RewindRequest>,
    ) -> Result<Response<Self::RewindStream>, Status> {
        let req = req.into_inner();
        info!(primary_host = %req.primary_host, "peer: Rewind");

        let opts = RewindOpts {
            primary_host: req.primary_host,
            primary_port: u16::try_from(req.primary_port)
                .map_err(|_| Status::invalid_argument("primary_port must fit in u16 and be > 0"))?,
            repl_user: req.repl_user,
        };
        opts.validate()
            .map_err(|e| Status::invalid_argument(format!("invalid rewind: {e}")))?;

        let standby = self.standby.clone();
        Ok(Response::new(spawn_progress_stream(
            "Rewind",
            "rewinding",
            move |tx| async move { standby.rewind(opts, Some(progress_cb(tx))).await },
        )))
    }

    // ----- streaming: WAL fetch ------------------------------------------

    async fn fetch_wal(
        &self,
        req: Request<FetchWalRequest>,
    ) -> Result<Response<Self::FetchWalStream>, Status> {
        let req = req.into_inner();
        info!(wal_file = %req.wal_file, "peer: FetchWal");

        if req.wal_file.is_empty() {
            return Err(Status::invalid_argument("wal_file is required"));
        }

        // WalStore returns typed errors (WalNotFound / WalInvalid /
        // other). Map them onto the corresponding gRPC codes so the
        // archive_command on the requesting standby can pause vs. fail.
        let file = self.wal.open_archive(&req.wal_file).await.map_err(|e| {
            warn!(?e, wal_file = %req.wal_file, "peer: FetchWal open failed");
            match e {
                pgman::walstore::WalStoreError::WalNotFound(name) => {
                    Status::not_found(format!("WAL segment not found: {name}"))
                }
                pgman::walstore::WalStoreError::WalInvalid { wal_file, reason } => {
                    Status::invalid_argument(format!("invalid wal_file {wal_file:?}: {reason}"))
                }
                other => Status::internal(format!("open {}: {other}", req.wal_file)),
            }
        })?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<WalChunk, Status>>(4);
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut file = file;
            // `WalChunk.data` is a `Bytes` (see pg-agent-proto/build.rs), so
            // the segment is read straight into a `BytesMut` and each filled
            // prefix is split off and frozen. `split` leaves the untouched
            // tail capacity behind, and `reserve` is a no-op whenever that
            // tail is already a full chunk wide — so a 16 MiB segment costs
            // a handful of allocations and no per-chunk copy. The previous
            // `read` into a reused `Vec` plus `buf[..n].to_vec()` paid an
            // allocation and a full 1 MiB memcpy on every chunk.
            let mut buf = bytes::BytesMut::with_capacity(WAL_CHUNK_SIZE);
            loop {
                // The reserve also keeps `Ok(0)` unambiguous: `read_buf`
                // returns 0 both at EOF and when there is no spare capacity
                // to read into, and this guarantees there always is some.
                buf.reserve(WAL_CHUNK_SIZE);
                match file.read_buf(&mut buf).await {
                    Ok(0) => return, // EOF — channel closes when tx drops
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("read: {e}")))).await;
                        return;
                    }
                }
                if tx
                    .send(Ok(WalChunk {
                        data: buf.split().freeze(),
                    }))
                    .await
                    .is_err()
                {
                    return; // Client dropped
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::FetchWalStream
        ))
    }
}

fn ok() -> OpResult {
    OpResult {
        ok: true,
        message: String::new(),
    }
}

/// 1 MiB. Kept well under tonic's default 4 MiB max message size so a
/// chunk + framing overhead never trips the encoder.
const WAL_CHUNK_SIZE: usize = 1024 * 1024;

/// The peer service with zstd negotiated in both directions.
///
/// This exists for `FetchWal`: a 16 MiB WAL segment is highly
/// compressible, and one closed early by `archive_timeout` or a forced
/// switch is mostly zero padding, which collapses to almost nothing.
/// tonic 0.12 configures compression per *service*, not per method, so
/// the small RPCs (`GetStatus`, `OpResult`, …) ride along; at a few
/// hundred bytes and a low call rate that CPU is noise.
///
/// zstd rather than gzip because tonic hardcodes the level: gzip is
/// `flate2::Compression::new(6)`, roughly 30–50 MB/s, slow enough to
/// become the bottleneck on a LAN faster than ~400 Mbps — it would trade
/// bandwidth for wall-clock on exactly the promotion-critical path we
/// are trying to speed up. zstd's default level 3 runs an order of
/// magnitude faster at a better ratio.
///
/// Negotiation makes this safe across a rolling upgrade in both
/// directions. `send_compressed` only takes effect when the caller
/// advertised zstd in `grpc-accept-encoding`, so this server still
/// answers a pre-upgrade peer in plain identity framing; and a
/// post-upgrade client asking for zstd from a pre-upgrade server simply
/// gets an uncompressed reply.
fn peer_service(inner: PeerServer) -> PgAgentPeerServer<PeerServer> {
    PgAgentPeerServer::new(inner)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
}

/// Bounded buffer between the subprocess progress callback and the gRPC
/// stream consumer. Progress events are non-essential — `try_send` drops
/// new ones when the buffer fills, which is preferable to either
/// `blocking_send` (deadlock risk from an async context) or unbounded
/// growth.
const PROGRESS_BUFFER: usize = 16;

/// Build a `Fn(i64,i64)` progress callback that pushes "phase=streaming"
/// OpProgress events into the given sender. Phase is fixed at the call
/// site (`spawn_progress_stream` overwrites the final message); the cb
/// only ever emits the intermediate phase.
fn progress_cb(tx: tokio::sync::mpsc::Sender<Result<OpProgress, Status>>) -> ProgressCb {
    Box::new(move |done, total| {
        // try_send: drop the event rather than block the subprocess
        // driver (which calls this from an async stderr-drain task).
        let _ = tx.try_send(Ok(OpProgress {
            phase: "streaming".to_string(),
            bytes_done: done,
            bytes_total: total,
            message: String::new(),
        }));
    })
}

/// Spawn the subprocess driver, route its progress events into a tonic
/// stream, and tack on a final `phase = "done"` (or an Internal error)
/// once it returns. `op_label` is the RPC name for log breadcrumbs
/// (`"Basebackup"` / `"Rewind"`); `intermediate_phase` is the wire
/// field on intermediate progress events (`"streaming"` / `"rewinding"`).
/// The final phase is always `"done"`.
///
/// The completion / failure log lines are local to the node where the
/// subprocess actually ran. The orchestrator on the other end of the
/// stream already sees the error in its tonic response, but the worker
/// side would otherwise have no breadcrumb at all — and the worker is
/// where the operator looks first, because the worker is the node whose
/// data directory is being rebuilt.
fn spawn_progress_stream<F, Fut>(
    op_label: &'static str,
    intermediate_phase: &'static str,
    f: F,
) -> ProgressStream
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<OpProgress, Status>>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<OpProgress, Status>>(PROGRESS_BUFFER);
    let driver_tx = tx.clone();
    tokio::spawn(async move {
        let result = f(driver_tx).await;
        let final_msg = match result {
            Ok(()) => {
                info!(op = op_label, "peer streaming op: completed");
                Ok(OpProgress {
                    phase: "done".to_string(),
                    bytes_done: 0,
                    bytes_total: 0,
                    message: String::new(),
                })
            }
            Err(e) => {
                warn!(op = op_label, err = %e, "peer streaming op: failed");
                Err(Status::internal(format!("{intermediate_phase}: {e}")))
            }
        };
        let _ = tx.send(final_msg).await;
    });
    Box::pin(ReceiverStream::new(rx)) as ProgressStream
}

/// Reject empty or non-alphabet slot names with InvalidArgument before
/// the libpq call. PG would otherwise reject with a less helpful error,
/// and a strict alphabet defeats identifier-quoting accidents.
///
/// `tonic::Status` is large (~176 bytes); clippy's
/// `result_large_err` flags every `Result<_, Status>` shape. The trait-
/// generated handler impls are unavoidable; this helper is allowed
/// because boxing here would just move the cost without removing it.
#[allow(clippy::result_large_err)]
fn validate_slot_name(name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("slot_name is required"));
    }
    if !allowed_slot_name().is_match(name) {
        return Err(Status::invalid_argument(
            "slot_name contains invalid characters",
        ));
    }
    Ok(())
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsConfig;
    use crate::localdb::ReplicationLag;
    use crate::pgstandby::{BasebackupOpts, ProgressCb, RewindOpts};
    use async_trait::async_trait;
    use rcgen::{CertificateParams, IsCa, KeyPair};
    use rustls::pki_types::ServerName;
    use rustls::ClientConfig;
    use std::convert::TryFrom;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use tokio_rustls::TlsConnector;

    // ----- FakeNodeInfo -----------------------------------------------------

    struct FakeNodeInfo;

    #[async_trait]
    impl NodeInfo for FakeNodeInfo {
        async fn get_status(&self) -> anyhow::Result<NodeStatus> {
            Ok(NodeStatus {
                is_running: true,
                is_in_recovery: true,
                is_ready: true,
                replication_lag_bytes: 42,
                replication_state: "streaming".into(),
                is_postgres_running: true,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
                timeline_id: 0,
                current_wal_lsn: 0,
                last_flush_lsn: 0,
                peer_primary_seen_age_ms: Default::default(),
            })
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5433,
                pg_data_dir: "/d".into(),
            })
        }
    }

    // ----- Stub deps ------------------------------------------------------

    /// Scripted local-pgpool view for the attach fan-out receiver:
    /// `infos` is what `node_info_all` reports; attaches are recorded
    /// and flip that backend up.
    #[derive(Default)]
    struct StubPcp {
        infos: StdMutex<Vec<crate::pcp::NodeInfo>>,
        attach_calls: StdMutex<Vec<i32>>,
        node_info_fails: std::sync::atomic::AtomicBool,
    }

    impl StubPcp {
        fn backend(id: i32, up: bool) -> crate::pcp::NodeInfo {
            crate::pcp::NodeInfo {
                id,
                hostname: format!("db{id}"),
                port: 5432,
                status_code: if up { 2 } else { 3 },
                lb_weight: 0.33,
                status_name: if up { "up" } else { "down" }.into(),
                actual_status: "up".into(),
                role: "standby".into(),
                actual_role: "standby".into(),
                replication_delay: "0".into(),
                replication_state: "none".into(),
                sync_state: "none".into(),
            }
        }
        fn with_backends(states: &[(i32, bool)]) -> Arc<Self> {
            let s = Arc::new(Self::default());
            *s.infos.lock().unwrap() = states
                .iter()
                .map(|(id, up)| Self::backend(*id, *up))
                .collect();
            s
        }
    }

    #[async_trait]
    impl crate::pcp::Pcp for StubPcp {
        async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
            self.attach_calls.lock().unwrap().push(node_id);
            let mut infos = self.infos.lock().unwrap();
            if let Some(n) = infos.iter_mut().find(|n| n.id == node_id) {
                *n = Self::backend(node_id, true);
            }
            Ok(())
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<crate::pcp::NodeInfo>> {
            if self
                .node_info_fails
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("scripted: pgpool not running")
            }
            Ok(self.infos.lock().unwrap().clone())
        }
    }

    #[derive(Default)]
    struct StubSd {
        start_calls: AtomicUsize,
        stop_calls: AtomicUsize,
        reload_pg_calls: AtomicUsize,
        fail: AtomicBool,
    }

    #[async_trait]
    impl Systemd for StubSd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("stub sd: start boom");
            }
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
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
            self.reload_pg_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubDb {
        promote_calls: AtomicUsize,
        created_slots: StdMutex<Vec<String>>,
        dropped_slots: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            self.promote_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_conf(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_slot(&self, name: &str) -> anyhow::Result<()> {
            self.created_slots.lock().unwrap().push(name.to_string());
            Ok(())
        }
        async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
            self.dropped_slots.lock().unwrap().push(name.to_string());
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
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
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

    #[derive(Default)]
    struct StubStandby {
        recovery_calls: StdMutex<Vec<WriteRecoveryConfOpts>>,
        /// (bytes_done, bytes_total) pairs emitted via the progress
        /// callback before the operation returns.
        progress_steps: StdMutex<Vec<(i64, i64)>>,
        /// When true, basebackup/rewind return Err instead of Ok.
        fail: AtomicBool,
    }

    #[async_trait]
    impl StandbyOps for StubStandby {
        async fn basebackup(
            &self,
            _: BasebackupOpts,
            progress: Option<ProgressCb>,
        ) -> anyhow::Result<()> {
            if let Some(cb) = &progress {
                for (d, t) in self.progress_steps.lock().unwrap().iter() {
                    cb(*d, *t);
                }
            }
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("stub basebackup boom");
            }
            Ok(())
        }
        async fn rewind(&self, _: RewindOpts, progress: Option<ProgressCb>) -> anyhow::Result<()> {
            if let Some(cb) = &progress {
                for (d, t) in self.progress_steps.lock().unwrap().iter() {
                    cb(*d, *t);
                }
            }
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("stub rewind boom");
            }
            Ok(())
        }
        async fn write_recovery_conf(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            self.recovery_calls.lock().unwrap().push(opts);
            Ok(())
        }
        async fn detach_recovery_conf(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// WalStore stub that serves files staged via `stage`. Missing files
    /// surface as `WalNotFound`; names staged via `stage_invalid` surface
    /// as `WalInvalid`.
    #[derive(Default)]
    struct StubWal {
        archived: StdMutex<std::collections::HashMap<String, Vec<u8>>>,
        invalid: StdMutex<std::collections::HashSet<String>>,
    }

    impl StubWal {
        fn stage(&self, name: &str, content: Vec<u8>) {
            self.archived
                .lock()
                .unwrap()
                .insert(name.to_string(), content);
        }
        fn stage_invalid(&self, name: &str) {
            self.invalid.lock().unwrap().insert(name.to_string());
        }
    }

    #[async_trait]
    impl WalStore for StubWal {
        async fn open_archive(
            &self,
            wal_file: &str,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, pgman::walstore::WalStoreError>
        {
            if self.invalid.lock().unwrap().contains(wal_file) {
                return Err(pgman::walstore::WalStoreError::WalInvalid {
                    wal_file: wal_file.to_string(),
                    reason: "stub invalid".to_string(),
                });
            }
            let content = {
                let archived = self.archived.lock().unwrap();
                archived.get(wal_file).cloned()
            };
            match content {
                Some(bytes) => Ok(Box::new(std::io::Cursor::new(bytes))
                    as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                None => Err(pgman::walstore::WalStoreError::WalNotFound(
                    wal_file.to_string(),
                )),
            }
        }
        async fn write_restore(
            &self,
            _: &std::path::Path,
            _: Box<dyn tokio::io::AsyncBufRead + Send + Unpin>,
        ) -> Result<(), pgman::walstore::WalStoreError> {
            Ok(())
        }
    }

    /// Build a PeerServer + return Arc clones of stubs so the test can
    /// inspect call counters.
    #[allow(clippy::type_complexity)]
    fn make_server() -> (
        PeerServer,
        Arc<StubSd>,
        Arc<StubDb>,
        Arc<StubStandby>,
        Arc<StubWal>,
    ) {
        let sd = Arc::new(StubSd::default());
        let db = Arc::new(StubDb::default());
        let standby = Arc::new(StubStandby::default());
        let wal = Arc::new(StubWal::default());
        let server = PeerServer::new(
            Arc::new(FakeNodeInfo),
            sd.clone(),
            db.clone(),
            standby.clone(),
            wal.clone(),
            Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new()),
            Arc::new(StubPcp::default()),
        );
        (server, sd, db, standby, wal)
    }

    // ----- read-only ------------------------------------------------------

    #[tokio::test]
    async fn get_status_routes_to_node_info() {
        let (s, ..) = make_server();
        let resp = s
            .get_status(Request::new(GetStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.is_in_recovery);
        assert_eq!(resp.replication_lag_bytes, 42);
    }

    #[tokio::test]
    async fn get_node_config_routes_to_node_info() {
        let (s, ..) = make_server();
        let resp = s
            .get_node_config(Request::new(NodeConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pg_port, 5433);
    }

    // ----- service control ------------------------------------------------

    #[tokio::test]
    async fn start_calls_systemd() {
        let (s, sd, ..) = make_server();
        s.start(Request::new(StartRequest::default()))
            .await
            .unwrap();
        assert_eq!(sd.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_propagates_systemd_failure_as_internal() {
        let (s, sd, ..) = make_server();
        sd.fail.store(true, Ordering::SeqCst);
        let err = s
            .start(Request::new(StartRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("start boom"));
    }

    #[tokio::test]
    async fn stop_calls_systemd() {
        let (s, sd, ..) = make_server();
        s.stop(Request::new(StopRequest::default())).await.unwrap();
        assert_eq!(sd.stop_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn promote_calls_db() {
        let (s, _sd, db, _standby, _wal) = make_server();
        s.promote(Request::new(PromoteRequest::default()))
            .await
            .unwrap();
        assert_eq!(db.promote_calls.load(Ordering::SeqCst), 1);
    }

    // ----- slots ----------------------------------------------------------

    #[tokio::test]
    async fn create_slot_calls_db() {
        let (s, _sd, db, _standby, _wal) = make_server();
        s.create_slot(Request::new(CreateSlotRequest {
            slot_name: "node1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(*db.created_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    // ----- attach_node (hook-contract §3 fan-out receiver) -----------------

    fn server_with_pcp(pcp: Arc<StubPcp>) -> PeerServer {
        PeerServer::new(
            Arc::new(FakeNodeInfo),
            Arc::new(StubSd::default()),
            Arc::new(StubDb::default()),
            Arc::new(StubStandby::default()),
            Arc::new(StubWal::default()),
            Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new()),
            pcp,
        )
    }

    #[tokio::test]
    async fn attach_node_attaches_a_down_backend() {
        let pcp = StubPcp::with_backends(&[(0, true), (1, false)]);
        let s = server_with_pcp(pcp.clone());
        let resp = s
            .attach_node(Request::new(AttachNodeRequest {
                node_id: 1,
                primary_node_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn attach_node_leaves_an_up_backend_alone() {
        // Blindly attaching an up backend makes pgpool re-run its
        // failover processing and transiently degenerate healthy
        // backends (finding 16's harness lesson, now server-side).
        let pcp = StubPcp::with_backends(&[(0, true), (1, true)]);
        let s = server_with_pcp(pcp.clone());
        let resp = s
            .attach_node(Request::new(AttachNodeRequest {
                node_id: 1,
                primary_node_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("no action"), "{}", resp.message);
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn attach_node_fixes_a_primaryless_map_first() {
        // A standby attach into a map with no up primary blocks in
        // find_primary_node_repeatedly — the primary's backend goes
        // first.
        let pcp = StubPcp::with_backends(&[(0, false), (1, false)]);
        let s = server_with_pcp(pcp.clone());
        let resp = s
            .attach_node(Request::new(AttachNodeRequest {
                node_id: 1,
                primary_node_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(
            *pcp.attach_calls.lock().unwrap(),
            vec![0, 1],
            "primary backend must be attached before the standby"
        );
    }

    #[tokio::test]
    async fn attach_node_reports_unavailable_pgpool_as_not_ok() {
        // Best-effort contract: pgpool down is an ok=false ANSWER, not
        // a gRPC error — the caller's fan-out moves on.
        let pcp = StubPcp::with_backends(&[(0, true), (1, false)]);
        pcp.node_info_fails
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let s = server_with_pcp(pcp.clone());
        let resp = s
            .attach_node(Request::new(AttachNodeRequest {
                node_id: 1,
                primary_node_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_slot_rejects_empty_name() {
        let (s, _sd, db, _standby, _wal) = make_server();
        let err = s
            .create_slot(Request::new(CreateSlotRequest {
                slot_name: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(db.created_slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_slot_rejects_injection_attempt() {
        let (s, _sd, db, _standby, _wal) = make_server();
        let err = s
            .create_slot(Request::new(CreateSlotRequest {
                slot_name: "node1; DROP TABLE x;".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(db.created_slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn drop_slot_calls_db() {
        let (s, _sd, db, _standby, _wal) = make_server();
        s.drop_slot(Request::new(DropSlotRequest {
            slot_name: "node1".into(),
        }))
        .await
        .unwrap();
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    /// A stale `drop_slot_cleanup` maintenance intent on *another* node
    /// retries with backoff, so a drop request created before a recovery
    /// started routinely lands in the middle of it. Only this node knows
    /// the slot is spoken for, so the guard lives on the server side —
    /// observed in the acceptance suite deleting a freshly-created slot
    /// once per backoff step.
    #[tokio::test]
    async fn drop_slot_refuses_while_an_orchestration_owns_it() {
        let inflight = Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new());
        inflight.seed_in_progress(
            crate::inflight_ops::InflightPayload::Recovery {
                primary_node_id: 0,
                standby_node_id: 1,
                standby_hostname: "db1".into(),
                slot_name: "node1".into(),
            },
            "slot_created",
        );
        let db = Arc::new(StubDb::default());
        let s = PeerServer::new(
            Arc::new(FakeNodeInfo),
            Arc::new(StubSd::default()),
            db.clone(),
            Arc::new(StubStandby::default()),
            Arc::new(StubWal::default()),
            inflight,
            Arc::new(StubPcp::default()),
        );

        let resp = s
            .drop_slot(Request::new(DropSlotRequest {
                slot_name: "node1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        // ok=true on purpose: the caller's cleanup is genuinely no
        // longer needed, and an error would keep the intent retrying.
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("retained"), "{}", resp.message);
        assert!(
            db.dropped_slots.lock().unwrap().is_empty(),
            "the orchestration's slot must survive"
        );

        // A slot belonging to a node no op owns is dropped normally.
        s.drop_slot(Request::new(DropSlotRequest {
            slot_name: "node2".into(),
        }))
        .await
        .unwrap();
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node2".to_string()]);
    }

    #[tokio::test]
    async fn drop_slot_rejects_empty_name() {
        let (s, ..) = make_server();
        let err = s
            .drop_slot(Request::new(DropSlotRequest {
                slot_name: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ----- configure_standby ---------------------------------------------

    #[tokio::test]
    async fn configure_standby_writes_recovery_conf() {
        let (s, _sd, _db, standby, _wal) = make_server();
        s.configure_standby(Request::new(ConfigureStandbyRequest {
            primary_host: "primary.local".into(),
            primary_port: 5432,
            repl_user: "repl".into(),
            slot_name: "node1".into(),
        }))
        .await
        .unwrap();
        let calls = standby.recovery_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].primary_host, "primary.local");
        assert_eq!(calls[0].primary_port, 5432);
        assert_eq!(calls[0].slot_name, "node1");
    }

    #[tokio::test]
    async fn configure_standby_rejects_bad_host() {
        let (s, _sd, _db, standby, _wal) = make_server();
        let err = s
            .configure_standby(Request::new(ConfigureStandbyRequest {
                primary_host: "primary host=attacker".into(), // libpq injection attempt
                primary_port: 5432,
                repl_user: "repl".into(),
                slot_name: "node1".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(standby.recovery_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn configure_standby_rejects_zero_port() {
        let (s, ..) = make_server();
        let err = s
            .configure_standby(Request::new(ConfigureStandbyRequest {
                primary_host: "primary.local".into(),
                primary_port: 0,
                repl_user: "repl".into(),
                slot_name: "node1".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn configure_standby_rejects_negative_port() {
        let (s, ..) = make_server();
        let err = s
            .configure_standby(Request::new(ConfigureStandbyRequest {
                primary_host: "primary.local".into(),
                primary_port: -1,
                repl_user: "repl".into(),
                slot_name: "node1".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ----- streaming: basebackup -----------------------------------------

    fn valid_basebackup_req() -> BasebackupRequest {
        BasebackupRequest {
            primary_host: "primary.local".into(),
            primary_port: 5432,
            repl_user: "repl".into(),
            slot_name: "node1".into(),
        }
    }

    type BoxedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

    /// Drain a tonic streaming Response into a Vec, capped by a deadline.
    async fn drain<T: 'static + Send>(resp: Response<BoxedStream<T>>) -> Result<Vec<T>, Status> {
        use futures_util::StreamExt;
        let mut stream = resp.into_inner();
        let mut out = Vec::new();
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(item))) => out.push(item),
                Ok(Some(Err(e))) => return Err(e),
                Ok(None) => return Ok(out),
                Err(_) => panic!("stream drain timed out"),
            }
        }
    }

    #[tokio::test]
    async fn basebackup_streams_progress_then_done() {
        let (s, _sd, _db, standby, _wal) = make_server();
        standby
            .progress_steps
            .lock()
            .unwrap()
            .extend([(0, 1024), (512, 1024), (1024, 1024)]);
        let resp = s
            .basebackup(Request::new(valid_basebackup_req()))
            .await
            .expect("basebackup");
        let events = drain::<OpProgress>(resp).await.expect("stream");
        // Final message is "done". Earlier ones are "streaming" with the
        // bytes_done/total from the stub. Intermediate count may be lower
        // than 3 if try_send dropped under a tight scheduler — accept any
        // count as long as a "done" arrives.
        assert!(events.iter().any(|e| e.phase == "done"));
        assert_eq!(events.last().unwrap().phase, "done");
        // At least one streaming event landed.
        assert!(events
            .iter()
            .any(|e| e.phase == "streaming" && e.bytes_total == 1024));
    }

    #[tokio::test]
    async fn basebackup_refuses_when_postgres_running() {
        // StubSd defaults to pg_running=false; spin a tiny pg-running
        // variant so basebackup's precondition check sees `true`.
        struct PgRunning;
        #[async_trait]
        impl Systemd for PgRunning {
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
                Ok(true)
            }
            async fn status_pgpool(&self) -> anyhow::Result<bool> {
                Ok(true)
            }
            async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
                Ok(())
            }
        }
        let server = PeerServer::new(
            Arc::new(FakeNodeInfo),
            Arc::new(PgRunning),
            Arc::new(StubDb::default()),
            Arc::new(StubStandby::default()),
            Arc::new(StubWal::default()),
            Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new()),
            Arc::new(StubPcp::default()),
        );
        let result = server
            .basebackup(Request::new(valid_basebackup_req()))
            .await;
        match result {
            Err(e) => {
                assert_eq!(e.code(), tonic::Code::FailedPrecondition);
                assert!(e.message().contains("postgres is running"));
            }
            Ok(_) => panic!("expected FailedPrecondition"),
        }
    }

    #[tokio::test]
    async fn basebackup_rejects_bad_host() {
        let (s, ..) = make_server();
        let mut req = valid_basebackup_req();
        req.primary_host = "primary host=attacker".into();
        let result = s.basebackup(Request::new(req)).await;
        match result {
            Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("expected InvalidArgument"),
        }
    }

    #[tokio::test]
    async fn basebackup_surface_subprocess_error_in_stream() {
        let (s, _sd, _db, standby, _wal) = make_server();
        standby.fail.store(true, Ordering::SeqCst);
        let resp = s
            .basebackup(Request::new(valid_basebackup_req()))
            .await
            .expect("basebackup call should accept; failure surfaces in-stream");
        let err = drain::<OpProgress>(resp)
            .await
            .expect_err("stream should terminate with error");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("basebackup boom"));
    }

    // ----- streaming: rewind ----------------------------------------------

    fn valid_rewind_req() -> RewindRequest {
        RewindRequest {
            primary_host: "primary.local".into(),
            primary_port: 5432,
            repl_user: "repl".into(),
        }
    }

    #[tokio::test]
    async fn rewind_streams_progress_then_done() {
        let (s, _sd, _db, standby, _wal) = make_server();
        standby
            .progress_steps
            .lock()
            .unwrap()
            .extend([(0, 200), (200, 200)]);
        let resp = s
            .rewind(Request::new(valid_rewind_req()))
            .await
            .expect("rewind");
        let events = drain::<OpProgress>(resp).await.expect("stream");
        assert_eq!(events.last().unwrap().phase, "done");
    }

    #[tokio::test]
    async fn rewind_rejects_zero_port() {
        let (s, ..) = make_server();
        let mut req = valid_rewind_req();
        req.primary_port = 0;
        let result = s.rewind(Request::new(req)).await;
        match result {
            Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("expected InvalidArgument"),
        }
    }

    // ----- streaming: fetch_wal -------------------------------------------

    #[tokio::test]
    async fn fetch_wal_streams_archive_content() {
        let (s, _sd, _db, _standby, wal) = make_server();
        // Stage content slightly larger than a chunk so we get 2+ chunks.
        let content: Vec<u8> = (0..(WAL_CHUNK_SIZE + 4096))
            .map(|i| (i & 0xff) as u8)
            .collect();
        wal.stage("000000010000000000000001", content.clone());

        let resp = s
            .fetch_wal(Request::new(FetchWalRequest {
                wal_file: "000000010000000000000001".into(),
            }))
            .await
            .expect("fetch_wal");
        let chunks = drain::<WalChunk>(resp).await.expect("stream");
        let assembled: Vec<u8> = chunks.into_iter().flat_map(|c| c.data).collect();
        assert_eq!(assembled, content);
    }

    /// The handler tests above call `fetch_wal` directly, which bypasses
    /// the service wrapper and so proves nothing about compression. This
    /// one goes over a real socket through the generated client, which is
    /// the only place the zstd negotiation, the `Bytes` codec and the
    /// multi-chunk read loop all run together.
    #[tokio::test]
    async fn fetch_wal_round_trips_compressed_over_a_real_channel() {
        use futures_util::StreamExt;
        use pg_agent_proto::pgagentpb::pg_agent_peer_client::PgAgentPeerClient;

        let (server, _sd, _db, _standby, wal) = make_server();

        // Two chunks and a bit, and compressible the way a real segment
        // is: a repeating body followed by the zero padding PostgreSQL
        // leaves behind when a segment is closed early.
        let mut content: Vec<u8> = (0..(2 * WAL_CHUNK_SIZE)).map(|i| (i % 251) as u8).collect();
        content.extend(std::iter::repeat_n(0u8, WAL_CHUNK_SIZE / 2));
        wal.stage("000000010000000000000007", content.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { server.serve(listener, None, s).await });

        let mut peer = PgAgentPeerClient::connect(format!("http://{addr}"))
            .await
            .unwrap()
            .accept_compressed(CompressionEncoding::Zstd);

        let resp = peer
            .fetch_wal(Request::new(FetchWalRequest {
                wal_file: "000000010000000000000007".into(),
            }))
            .await
            .expect("fetch_wal");

        // The server must have taken up the offer — otherwise this test
        // would still pass on content alone while silently shipping
        // uncompressed, which is the regression worth catching.
        assert_eq!(
            resp.metadata().get("grpc-encoding").map(|v| v.as_bytes()),
            Some(&b"zstd"[..]),
            "server should compress once the client advertises zstd"
        );

        let mut stream = resp.into_inner();
        let mut assembled: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            assembled.extend_from_slice(&chunk.expect("chunk").data);
        }
        assert_eq!(assembled, content, "segment must survive the round trip");

        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn fetch_wal_returns_not_found_for_unknown_segment() {
        let (s, ..) = make_server();
        let result = s
            .fetch_wal(Request::new(FetchWalRequest {
                wal_file: "missing.wal".into(),
            }))
            .await;
        match result {
            Err(e) => {
                assert_eq!(e.code(), tonic::Code::NotFound);
                assert!(e.message().contains("WAL segment not found"));
            }
            Ok(_) => panic!("expected NotFound"),
        }
    }

    #[tokio::test]
    async fn fetch_wal_returns_invalid_argument_for_invalid_filename() {
        let (s, _sd, _db, _standby, wal) = make_server();
        wal.stage_invalid("bad name");
        let result = s
            .fetch_wal(Request::new(FetchWalRequest {
                wal_file: "bad name".into(),
            }))
            .await;
        match result {
            Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("expected InvalidArgument"),
        }
    }

    #[tokio::test]
    async fn fetch_wal_rejects_empty_filename() {
        let (s, ..) = make_server();
        let result = s
            .fetch_wal(Request::new(FetchWalRequest {
                wal_file: String::new(),
            }))
            .await;
        match result {
            Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("expected InvalidArgument"),
        }
    }

    #[test]
    fn internal_maps_anyhow_to_internal_status() {
        let err = internal(anyhow::anyhow!("boom"));
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), "boom");
    }

    // ----- mTLS integration -------------------------------------------------
    //
    // We mint a CA + two leaf certs (one with an allowed SAN, one with a
    // disallowed SAN), spin up PeerServer in mTLS mode, and verify the
    // SAN-allowlist gate at the TLS layer. The accept loop logs disallowed
    // handshakes and drops them — observable as a TLS error on the client.

    /// (leaf_pem, key_pem) — leaf has the given DNS SANs, signed by the
    /// given CA. `rcgen::Certificate` and `KeyPair` are non-Clone, so the
    /// CA must be passed by reference and outlive every leaf it issues.
    fn gen_leaf(ca_cert: &rcgen::Certificate, ca_key: &KeyPair, sans: &[&str]) -> (String, String) {
        let leaf_key = KeyPair::generate().unwrap();
        let params =
            CertificateParams::new(sans.iter().map(|s| (*s).to_string()).collect::<Vec<_>>())
                .unwrap();
        let cert = params.signed_by(&leaf_key, ca_cert, ca_key).unwrap();
        (cert.pem(), leaf_key.serialize_pem())
    }

    /// (ca_cert, ca_key, ca_pem) — keep `ca_cert`/`ca_key` in scope while
    /// any leaves it issues are still being constructed.
    fn gen_ca() -> (rcgen::Certificate, KeyPair, String) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();
        (ca_cert, ca_key, ca_pem)
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

    /// Spin up PeerServer in mTLS mode bound to 127.0.0.1:0 and return
    /// (port, shutdown handle).
    async fn spawn_mtls(
        tls: PeerTlsConfig,
    ) -> (
        u16,
        CancellationToken,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let h = tokio::spawn(async move {
            let (server, _sd, _db, _standby, _wal) = make_server();
            server.serve(listener, Some(tls), s).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (port, shutdown, h)
    }

    /// Build a client-side rustls config that presents `(leaf_pem, key_pem)`
    /// and trusts `ca_pem` as the server root.
    fn build_client_config(ca_pem: &str, leaf_pem: &str, key_pem: &str) -> ClientConfig {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut roots = RootCertStore::empty();
        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_pem.as_bytes())
            .collect::<Result<_, _>>()
            .unwrap();
        for c in ca_certs {
            roots.add(c).unwrap();
        }

        let leaf_chain: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut leaf_pem.as_bytes())
                .collect::<Result<_, _>>()
                .unwrap();
        let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
            .unwrap()
            .unwrap();

        let mut cfg = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(leaf_chain, key)
            .unwrap();
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        cfg
    }

    /// TLS handshake + a small post-handshake probe.
    ///
    /// rustls finishes the client-side handshake (TLS 1.3 in particular)
    /// before the server has validated the client cert, so a pure
    /// connect() returns Ok even when the server is about to alert + drop
    /// the connection. We send the HTTP/2 client preface (what tonic
    /// expects first byte after handshake) and read a response — a
    /// rejected connection EOFs / errors on the read; an accepted one
    /// gets the server's HTTP/2 SETTINGS frame back.
    async fn handshake(
        port: u16,
        server_name: &str,
        client_cfg: ClientConfig,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let name = ServerName::try_from(server_name.to_string()).unwrap();
        let mut tls = connector.connect(name, stream).await?;

        // HTTP/2 client preface — what tonic expects on the new conn.
        tls.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
        tls.flush().await?;

        // If the server accepted our cert, tonic responds with its
        // SETTINGS frame within milliseconds. If it rejected, the conn is
        // gone (read returns 0) or the cert-alert surfaces as an io error.
        let mut buf = [0u8; 8];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), tls.read(&mut buf))
            .await
            .map_err(|_| anyhow::anyhow!("post-handshake read timed out"))??;
        if n == 0 {
            anyhow::bail!("server closed connection after handshake");
        }
        Ok(())
    }

    #[tokio::test]
    async fn mtls_handshake_accepts_allowed_san() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();

        // Server + client material from the same CA. Build them both
        // while ca_cert/ca_key are in scope, then drop the CA references.
        let (server_leaf, server_key) = gen_leaf(&ca_cert, &ca_key, &["server.local"]);
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["node1.local"]);

        let server_tls_cfg = write_pems(tmp.path(), "server", &ca_pem, &server_leaf, &server_key);
        let reloader = Arc::new(CertReloader::new(server_tls_cfg).unwrap());

        let mut allowed = HashSet::new();
        allowed.insert("node1.local".to_string());

        let tls = PeerTlsConfig {
            reloader,
            allowed_peer_sans: allowed,
        };
        let (port, shutdown, handle) = spawn_mtls(tls).await;

        let client_cfg = build_client_config(&ca_pem, &client_leaf, &client_key);
        let result = handshake(port, "server.local", client_cfg).await;

        shutdown.cancel();
        let _ = handle.await;

        result.expect("handshake should succeed for allowed SAN");
    }

    #[tokio::test]
    async fn mtls_handshake_rejects_disallowed_san() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca_cert, ca_key, ca_pem) = gen_ca();

        let (server_leaf, server_key) = gen_leaf(&ca_cert, &ca_key, &["server.local"]);
        // Client SAN is "intruder.local" — NOT in the allowlist below.
        let (client_leaf, client_key) = gen_leaf(&ca_cert, &ca_key, &["intruder.local"]);

        let server_tls_cfg = write_pems(tmp.path(), "server", &ca_pem, &server_leaf, &server_key);
        let reloader = Arc::new(CertReloader::new(server_tls_cfg).unwrap());

        let mut allowed = HashSet::new();
        allowed.insert("node1.local".to_string());
        allowed.insert("node2.local".to_string());

        let tls = PeerTlsConfig {
            reloader,
            allowed_peer_sans: allowed,
        };
        let (port, shutdown, handle) = spawn_mtls(tls).await;

        let client_cfg = build_client_config(&ca_pem, &client_leaf, &client_key);
        let result = handshake(port, "server.local", client_cfg).await;

        shutdown.cancel();
        let _ = handle.await;

        let err = result.expect_err("handshake should fail for disallowed SAN");
        let msg = err.to_string();
        // The reject can manifest as either a TLS alert from the server
        // (rustls bubbles our General error) or an EOF/aborted handshake
        // depending on alpn-vs-tls timing. Both are acceptable rejections.
        assert!(
            msg.to_lowercase().contains("certificate")
                || msg.to_lowercase().contains("handshake")
                || msg.to_lowercase().contains("eof")
                || msg.to_lowercase().contains("connection"),
            "unexpected handshake error: {msg}"
        );
    }

    #[tokio::test]
    async fn mtls_handshake_rejects_untrusted_ca() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        // Server's CA.
        let (server_ca_cert, server_ca_key, server_ca_pem) = gen_ca();
        let (server_leaf, server_key) =
            gen_leaf(&server_ca_cert, &server_ca_key, &["server.local"]);

        // Client signed by a DIFFERENT CA, but with an allowed SAN. The
        // WebPki chain check rejects before the allowlist is even
        // consulted.
        let (other_ca_cert, other_ca_key, _other_ca_pem) = gen_ca();
        let (client_leaf, client_key) = gen_leaf(&other_ca_cert, &other_ca_key, &["node1.local"]);

        let server_tls_cfg = write_pems(
            tmp.path(),
            "server",
            &server_ca_pem,
            &server_leaf,
            &server_key,
        );
        let reloader = Arc::new(CertReloader::new(server_tls_cfg).unwrap());

        let mut allowed = HashSet::new();
        allowed.insert("node1.local".to_string());
        let tls = PeerTlsConfig {
            reloader,
            allowed_peer_sans: allowed,
        };
        let (port, shutdown, handle) = spawn_mtls(tls).await;

        // Client trusts the SERVER's CA (so server-cert validation passes
        // on the client side), but presents a leaf the server doesn't
        // trust.
        let client_cfg = build_client_config(&server_ca_pem, &client_leaf, &client_key);
        let result = handshake(port, "server.local", client_cfg).await;

        shutdown.cancel();
        let _ = handle.await;

        result.expect_err("handshake should fail for untrusted client cert");
    }

    // ----- builder --------------------------------------------------------

    #[test]
    fn allowlist_verifier_builds_from_real_root_store() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (ca_cert, _ca_key, ca_pem) = gen_ca();
        let _ = ca_cert; // unused — we only need the PEM bytes

        let mut roots = RootCertStore::empty();
        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_pem.as_bytes())
            .collect::<Result<_, _>>()
            .unwrap();
        for c in ca_certs {
            roots.add(c).unwrap();
        }

        let mut allowed = HashSet::new();
        allowed.insert("peer.example".to_string());
        let v = AllowlistClientCertVerifier::new(Arc::new(roots), allowed).unwrap();
        assert!(ClientCertVerifier::client_auth_mandatory(v.as_ref()));
    }

    #[test]
    fn allowlist_verifier_empty_root_store_errors() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let roots = RootCertStore::empty();
        let mut allowed = HashSet::new();
        allowed.insert("peer.example".to_string());
        let err =
            AllowlistClientCertVerifier::new(Arc::new(roots), allowed).expect_err("empty roots");
        // VerifierBuilderError prints variant name; precise message is webpki-internal.
        let _ = format!("{err:?}");
    }

    // ----- consensus plane on the shared listener ---------------------------

    /// `with_raft` must actually mount `PgAgentRaft` on the same
    /// listener as `PgAgentPeer` — the inbound half of the transport
    /// design. Serving it on a second port would still pass every test
    /// in `raftnet`, so the assertion has to be made here: one socket,
    /// both services answering.
    #[tokio::test]
    async fn with_raft_serves_both_services_on_one_listener() {
        use crate::raftnet::{RaftChannelFactory, RAFT_CONNECT_TIMEOUT};
        use crate::raftstore::{open_database, RedbLogStore, RedbStateMachine};
        use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
        use pg_agent_proto::pgagentpb::pg_agent_peer_client::PgAgentPeerClient;

        let dir = TempDir::new().unwrap();
        let db_raft = open_database(dir.path()).unwrap();
        let reader = crate::raftstore::ClusterStateReader::new(db_raft.clone());
        let raft = openraft::Raft::new(
            0u64,
            Arc::new(openraft::Config::default().validate().unwrap()),
            RaftChannelFactory::new_dev(),
            RedbLogStore::new(db_raft.clone()),
            RedbStateMachine::new(db_raft).unwrap(),
        )
        .await
        .unwrap();

        let (server, ..) = make_server();
        let server = server.with_raft(raft, reader);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { server.serve(listener, None, s).await });

        // The peer plane answers.
        let mut peer = PgAgentPeerClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let status = peer
            .get_status(Request::new(GetStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.replication_lag_bytes, 42);

        // ...and so does the consensus plane, on that same socket. An
        // uninitialized Raft still answers a vote; what is being proved
        // is that the request was routed, not what it decided.
        let mut factory = RaftChannelFactory::new_dev();
        let mut net = factory.new_client(0, &BasicNode::new(addr.clone())).await;
        let resp = net
            .vote(
                openraft::raft::VoteRequest::new(openraft::Vote::new(1, 0), None),
                openraft::network::RPCOption::new(RAFT_CONNECT_TIMEOUT),
            )
            .await
            .expect("raft vote must be routed on the shared listener");
        assert!(resp.vote.leader_id().voted_for().is_some());

        shutdown.cancel();
        let _ = handle.await;
    }

    /// Without `with_raft` the consensus service must be absent, not
    /// merely idle — a node that has not joined Raft should refuse the
    /// RPC rather than answer for a state machine it does not have.
    #[tokio::test]
    async fn without_raft_the_consensus_service_is_unimplemented() {
        use pg_agent_proto::pgagentpb::pg_agent_raft_client::PgAgentRaftClient;
        use pg_agent_proto::pgagentpb::RaftFrame;

        let (server, ..) = make_server();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { server.serve(listener, None, s).await });

        let mut client = PgAgentRaftClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let err = client
            .vote(Request::new(RaftFrame {
                payload: b"{}".to_vec(),
            }))
            .await
            .expect_err("no raft service should be mounted");
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        shutdown.cancel();
        let _ = handle.await;
    }
}
