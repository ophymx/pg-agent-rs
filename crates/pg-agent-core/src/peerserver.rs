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
//! the daemon for" — see SPEC §7.4. Server-cert rotation is the hot
//! path; CA rotation is a planned event.

use crate::agent::NodeInfo;
use crate::certreload::{extract_sans, CertReloader, ReloadingServerCertResolver};
use futures_core::Stream;
use pg_agent_proto::pgagentpb::{
    pg_agent_peer_server::{PgAgentPeer, PgAgentPeerServer},
    BasebackupRequest, ConfigureStandbyRequest, CreateSlotRequest, DropSlotRequest,
    FetchWalRequest, GetStatusRequest, NodeConfigRequest, NodeConfigResponse, NodeStatus,
    OpProgress, OpResult, PromoteRequest, ReloadPgpoolRequest, ReloadRequest, RemoveVipRequest,
    RewindRequest, StartRequest, StopRequest, WalChunk,
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
use tonic::{transport::Server, Request, Response, Status};
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
    // TODO(v1): action deps — Arc<dyn Systemd>, Arc<dyn LocalDb>,
    // Arc<dyn StandbyOps>, Arc<dyn WalStore>.
}

impl PeerServer {
    pub fn new(node_info: Arc<dyn NodeInfo>) -> Self {
        Self { node_info }
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
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("peer server: starting (plain TCP — dev mode)");
        let incoming = TcpListenerStream::new(listener);
        Server::builder()
            .add_service(PgAgentPeerServer::new(self))
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
        self,
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
        let serve_result = Server::builder()
            .add_service(PgAgentPeerServer::new(self))
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

    // ----- action surface (TODO(v1)) ----------------------------------------

    async fn start(&self, _req: Request<StartRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("start"))
    }
    async fn stop(&self, _req: Request<StopRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("stop"))
    }
    async fn reload(&self, _req: Request<ReloadRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("reload"))
    }
    async fn reload_pgpool(
        &self,
        _req: Request<ReloadPgpoolRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("reload_pgpool"))
    }
    async fn promote(&self, _req: Request<PromoteRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("promote"))
    }
    async fn create_slot(
        &self,
        _req: Request<CreateSlotRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("create_slot"))
    }
    async fn drop_slot(
        &self,
        _req: Request<DropSlotRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("drop_slot"))
    }
    async fn configure_standby(
        &self,
        _req: Request<ConfigureStandbyRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("configure_standby"))
    }
    async fn remove_vip(
        &self,
        _req: Request<RemoveVipRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("remove_vip"))
    }

    async fn basebackup(
        &self,
        _req: Request<BasebackupRequest>,
    ) -> Result<Response<Self::BasebackupStream>, Status> {
        Err(Status::unimplemented("basebackup"))
    }
    async fn rewind(
        &self,
        _req: Request<RewindRequest>,
    ) -> Result<Response<Self::RewindStream>, Status> {
        Err(Status::unimplemented("rewind"))
    }
    async fn fetch_wal(
        &self,
        _req: Request<FetchWalRequest>,
    ) -> Result<Response<Self::FetchWalStream>, Status> {
        Err(Status::unimplemented("fetch_wal"))
    }
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
    use async_trait::async_trait;
    use rcgen::{CertificateParams, IsCa, KeyPair};
    use rustls::pki_types::ServerName;
    use rustls::ClientConfig;
    use std::convert::TryFrom;
    use std::path::Path;
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
            })
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5433,
                pg_data_dir: "/d".into(),
            })
        }
    }

    fn server() -> PeerServer {
        PeerServer::new(Arc::new(FakeNodeInfo))
    }

    // ----- direct-impl tests (no transport) ---------------------------------

    #[tokio::test]
    async fn get_status_routes_to_node_info() {
        let s = server();
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
        let s = server();
        let resp = s
            .get_node_config(Request::new(NodeConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pg_port, 5433);
    }

    #[tokio::test]
    async fn promote_returns_unimplemented() {
        let s = server();
        let err = s
            .promote(Request::new(PromoteRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn basebackup_returns_unimplemented() {
        // `BasebackupStream` is `Pin<Box<dyn Stream>>` which doesn't impl
        // Debug, so unwrap_err() won't compile — match instead.
        let s = server();
        match s
            .basebackup(Request::new(BasebackupRequest::default()))
            .await
        {
            Err(e) => assert_eq!(e.code(), tonic::Code::Unimplemented),
            Ok(_) => panic!("expected unimplemented"),
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
            PeerServer::new(Arc::new(FakeNodeInfo))
                .serve(listener, Some(tls), s)
                .await
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
}
