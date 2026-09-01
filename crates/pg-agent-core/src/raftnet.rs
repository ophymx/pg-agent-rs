//! The consensus transport (docs/promotion-authority.md §5, "Transport").
//!
//! Three pieces, all thin:
//!
//! - [`RaftGrpcService`] — the inbound `PgAgentRaft` service, handing
//!   decoded requests to the local [`Raft`] and encoding what it returns.
//! - [`RaftPeerNetwork`] — the outbound side, one per target node.
//! - [`RaftChannelFactory`], the [`RaftNetworkFactory`] that mints
//!   those and owns the channels.
//!
//! # Raft dials its own channel
//!
//! Inbound, this shares everything with `PgAgentPeer`: same listener,
//! same port, same certs, same SAN allowlist. Outbound it does not share
//! [`PeerPool`](crate::peers::PeerPool)'s channels, and that is the
//! point rather than an oversight. `AppendEntries` heartbeats are small,
//! frequent and latency-critical; `Basebackup` streams gigabytes. On one
//! HTTP/2 connection a saturated basebackup can starve heartbeats at the
//! TCP layer and trigger a spurious election — during a recovery, which
//! is precisely when the cluster can least afford one.
//!
//! The channel settings differ accordingly. `PeerPool` sets a 300 s
//! channel timeout because `Start`/`Stop`/`Promote` genuinely need it;
//! here the ceiling is [`RAFT_CHANNEL_TIMEOUT`], because a consensus RPC
//! that has not answered in seconds has already failed for every purpose
//! openraft has — it will retry, and a request still queued behind a
//! dead peer's TCP buffer is worse than one that gave up.
//!
//! # Failing the right way
//!
//! Every outbound error maps to [`Unreachable`], never
//! `NetworkError`. The difference is openraft's retry cadence:
//! `NetworkError` means "retry immediately", `Unreachable` means "back
//! off first". A partitioned peer answered by
//! immediate retries is a spin loop against a socket that will not
//! answer for minutes, and the CPU it burns is on the node still trying
//! to hold a quorum together.

// `RPCError` is openraft's and large; every signature here is dictated by
// the traits being implemented, so boxing it would only add a conversion
// at each trait boundary. Same call as `raftstore`.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};
use pg_agent_proto::pgagentpb::{
    pg_agent_raft_client::PgAgentRaftClient,
    pg_agent_raft_server::{PgAgentRaft, PgAgentRaftServer},
    RaftFrame,
};
use rustls::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::consensus::ClusterState;
use crate::raftstore::{
    ClusterStateReader, CommandResponse, ConsensusCommand, PgAgentTypeConfig, RaftNodeId,
};

/// Channel-wide ceiling for consensus RPCs. Deliberately far below
/// [`crate::peers::LONG_RPC_TIMEOUT`] — see the module docs.
pub const RAFT_CHANNEL_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-dial connect deadline. Matches the peer plane's; a Raft peer that
/// cannot complete a TCP+TLS handshake in this window is unreachable for
/// consensus purposes regardless of what it is doing.
pub const RAFT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// HTTP/2 keepalive on the consensus channel. Shorter than the peer
/// plane's 60 s: this is the connection whose silence must be noticed.
pub const RAFT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Client-side deadline on leader-forwarded RPCs (`Propose`,
/// `ReadState`).
///
/// This exists because of a bug the acceptance suite's R4 scenario
/// caught on the first real partition: the HA tick forwarded its read
/// to the raft leader that had just been isolated, over a cached
/// channel whose only bound was the 30 s channel ceiling — so one tick
/// blocked 34 s while a 1 s re-election had already moved leadership.
/// The next tick would have used the new leader; there was no next
/// tick inside the partition window. Same lesson as the peer plane's
/// PRECONDITION_TIMEOUT: `Request::set_timeout` only writes a header
/// for the *server* to honour, and the server is precisely who is
/// unreachable, so the deadline must be enforced on this side of the
/// wire. Five seconds is far above a healthy forward (one intra-LAN
/// round trip plus a ReadIndex quorum round trip) and far below the
/// smallest sane `retry_timeout` cadence the HA loop runs on.
pub const LEADER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Raft handle, with this crate's type config already applied.
pub type PgAgentRaftHandle = Raft<PgAgentTypeConfig>;

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------
//
// Requests go over the wire as serialized frames rather than mirrored
// protobuf messages; the reasoning is in pgagent_raft.proto and is not
// repeated here. What matters at this layer: the frame is JSON, and a
// decode failure is a protocol error, never something to paper over.

fn encode_frame<T: Serialize>(value: &T) -> Result<RaftFrame, Status> {
    serde_json::to_vec(value)
        .map(|payload| RaftFrame { payload })
        .map_err(|e| Status::internal(format!("raft: encode frame: {e}")))
}

fn decode_frame<T: for<'de> Deserialize<'de>>(frame: &RaftFrame) -> Result<T, Status> {
    serde_json::from_slice(&frame.payload)
        .map_err(|e| Status::invalid_argument(format!("raft: decode frame: {e}")))
}

/// Client-side counterpart. Every failure here is `Unreachable` — see
/// the module docs on retry cadence.
fn decode_reply<T, E>(frame: &RaftFrame) -> Result<T, RPCError<RaftNodeId, BasicNode, E>>
where
    T: for<'de> Deserialize<'de>,
    E: std::error::Error,
{
    serde_json::from_slice(&frame.payload).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
}

fn unreachable<E>(e: impl std::error::Error + 'static) -> RPCError<RaftNodeId, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(&e))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Inbound `PgAgentRaft`: the three protocol RPCs openraft speaks, plus
/// the two the agent speaks when it is not the leader.
///
/// Note what is *not* here: no authorization check. That is not an
/// omission, it is where the check lives — the mTLS handshake and SAN
/// allowlist in `PeerServer` have already run before tonic sees a byte,
/// and they are the same gate `PgAgentPeer`'s mutating RPCs sit behind.
#[derive(Clone)]
pub struct RaftGrpcService {
    raft: PgAgentRaftHandle,
    /// Serves `ReadState` after the local `ensure_linearizable`.
    reader: ClusterStateReader,
}

impl RaftGrpcService {
    pub fn new(raft: PgAgentRaftHandle, reader: ClusterStateReader) -> Self {
        Self { raft, reader }
    }

    /// Wrap as a tonic service, ready for `Server::add_service`.
    pub fn into_server(self) -> PgAgentRaftServer<Self> {
        PgAgentRaftServer::new(self)
    }
}

/// Status returned when a forwarded request lands on a node that is not
/// the leader.
///
/// `FailedPrecondition` rather than a redirect: this handler does not
/// forward in turn (see the `.proto`), and the caller's next tick will
/// re-read leadership from its own Raft metrics anyway. Naming the
/// leader it believes in would just invite the caller to act on
/// information that is already one hop stale.
fn not_leader(what: &str, e: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("raft: {what}: not the leader: {e}"))
}

#[tonic::async_trait]
impl PgAgentRaft for RaftGrpcService {
    async fn append_entries(
        &self,
        request: Request<RaftFrame>,
    ) -> Result<Response<RaftFrame>, Status> {
        let req: AppendEntriesRequest<PgAgentTypeConfig> = decode_frame(request.get_ref())?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft: append_entries: {e}")))?;
        Ok(Response::new(encode_frame(&resp)?))
    }

    async fn vote(&self, request: Request<RaftFrame>) -> Result<Response<RaftFrame>, Status> {
        let req: VoteRequest<RaftNodeId> = decode_frame(request.get_ref())?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft: vote: {e}")))?;
        Ok(Response::new(encode_frame(&resp)?))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftFrame>,
    ) -> Result<Response<RaftFrame>, Status> {
        let req: InstallSnapshotRequest<PgAgentTypeConfig> = decode_frame(request.get_ref())?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("raft: install_snapshot: {e}")))?;
        Ok(Response::new(encode_frame(&resp)?))
    }

    async fn propose(&self, request: Request<RaftFrame>) -> Result<Response<RaftFrame>, Status> {
        let cmd: ConsensusCommand = decode_frame(request.get_ref())?;
        let written = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| not_leader("propose", e))?;
        Ok(Response::new(encode_frame(&written.data)?))
    }

    async fn read_state(
        &self,
        _request: Request<RaftFrame>,
    ) -> Result<Response<RaftFrame>, Status> {
        // Confirm leadership against a quorum *and* wait for the state
        // machine to catch up to the read index, then read. In that
        // order: reading first would answer from a state machine this
        // node has not yet established it is entitled to speak for.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| not_leader("read_state", e))?;
        let state = self
            .reader
            .read()
            .map_err(|e| Status::internal(format!("raft: read_state: {e}")))?;
        Ok(Response::new(encode_frame(&state)?))
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Outbound connection to one Raft peer.
///
/// The channel is dialed lazily on first use, not in
/// [`RaftNetworkFactory::new_client`]: that method cannot report an
/// error, so eager dialing there would either panic or silently produce
/// a client that never works. Deferring means a peer that is down when
/// membership is created starts working the moment it comes back,
/// without openraft having to rebuild anything.
pub struct RaftPeerNetwork {
    target: RaftNodeId,
    addr: String,
    tls: Option<Arc<ClientConfig>>,
    client: Option<PgAgentRaftClient<Channel>>,
}

impl RaftPeerNetwork {
    async fn client<E>(
        &mut self,
    ) -> Result<&mut PgAgentRaftClient<Channel>, RPCError<RaftNodeId, BasicNode, E>>
    where
        E: std::error::Error,
    {
        if self.client.is_none() {
            debug!(target = self.target, addr = %self.addr, "raft: dialing peer");
            let channel = dial(&self.addr, self.tls.clone())
                .await
                .map_err(|e| unreachable(TransportError(e.to_string())))?;
            self.client = Some(PgAgentRaftClient::new(channel));
        }
        Ok(self.client.as_mut().expect("dialed above"))
    }

    /// Drop the cached channel so the next RPC redials. Called on every
    /// transport failure: a channel that has failed once is frequently a
    /// half-open socket that will keep failing, and openraft's own retry
    /// would otherwise reuse it forever.
    fn invalidate(&mut self) {
        self.client = None;
    }
}

/// Transport failures reach openraft as `AnyError`, which needs a
/// concrete `Error` to wrap. `tonic::transport::Error` collapses to
/// "transport error" with the cause only reachable through `source()`,
/// so the chain is flattened into this before it is handed over.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TransportError(String);

async fn dial(addr: &str, tls: Option<Arc<ClientConfig>>) -> anyhow::Result<Channel> {
    // `http://` even under mTLS: our own connector performs the
    // handshake, and tonic must not also try to. Same reasoning, and the
    // same connector, as the peer plane.
    let uri = format!("http://{addr}");
    let endpoint = Endpoint::from_shared(uri.clone())
        .map_err(|e| anyhow::anyhow!("raft dial {uri}: invalid uri: {e}"))?
        .connect_timeout(RAFT_CONNECT_TIMEOUT)
        .timeout(RAFT_CHANNEL_TIMEOUT)
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(RAFT_KEEP_ALIVE_INTERVAL);

    match tls {
        Some(cfg) => {
            // The connector takes the port separately (it resolves the
            // host from the uri), so it has to come back out of `addr`.
            let port = addr
                .rsplit_once(':')
                .and_then(|(_, p)| p.parse::<u16>().ok())
                .ok_or_else(|| anyhow::anyhow!("raft dial {uri}: address has no port"))?;
            crate::peers::connect_mtls(endpoint, cfg, port)
                .await
                .map_err(|e| anyhow::anyhow!("raft dial {uri}: {e}"))
        }
        None => endpoint
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("raft dial {uri}: {e}")),
    }
}

impl RaftNetwork<PgAgentTypeConfig> for RaftPeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<PgAgentTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        let frame = RaftFrame {
            payload: serde_json::to_vec(&rpc)
                .map_err(|e| unreachable(TransportError(e.to_string())))?,
        };
        let mut request = Request::new(frame);
        request.set_timeout(option.hard_ttl());

        // hard_ttl is enforced HERE, not only via set_timeout above: the
        // header form is honoured by the server, and a black-holed peer
        // is exactly the case where there is no server to honour it.
        let ttl = option.hard_ttl();
        let client = self.client().await?;
        match tokio::time::timeout(ttl, client.append_entries(request)).await {
            Ok(Ok(reply)) => decode_reply(reply.get_ref()),
            Ok(Err(status)) => {
                self.invalidate();
                Err(unreachable(TransportError(status.to_string())))
            }
            Err(_) => {
                self.invalidate();
                Err(unreachable(TransportError(format!(
                    "no reply within {ttl:?} (client-side deadline)"
                ))))
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let frame = RaftFrame {
            payload: serde_json::to_vec(&rpc)
                .map_err(|e| unreachable(TransportError(e.to_string())))?,
        };
        let mut request = Request::new(frame);
        request.set_timeout(option.hard_ttl());

        // hard_ttl is enforced HERE, not only via set_timeout above: the
        // header form is honoured by the server, and a black-holed peer
        // is exactly the case where there is no server to honour it.
        let ttl = option.hard_ttl();
        let client = self.client().await?;
        match tokio::time::timeout(ttl, client.vote(request)).await {
            Ok(Ok(reply)) => decode_reply(reply.get_ref()),
            Ok(Err(status)) => {
                self.invalidate();
                Err(unreachable(TransportError(status.to_string())))
            }
            Err(_) => {
                self.invalidate();
                Err(unreachable(TransportError(format!(
                    "no reply within {ttl:?} (client-side deadline)"
                ))))
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<PgAgentTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<
            RaftNodeId,
            BasicNode,
            RaftError<RaftNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let frame = RaftFrame {
            payload: serde_json::to_vec(&rpc)
                .map_err(|e| unreachable(TransportError(e.to_string())))?,
        };
        let mut request = Request::new(frame);
        request.set_timeout(option.hard_ttl());

        // hard_ttl is enforced HERE, not only via set_timeout above: the
        // header form is honoured by the server, and a black-holed peer
        // is exactly the case where there is no server to honour it.
        let ttl = option.hard_ttl();
        let client = self.client().await?;
        match tokio::time::timeout(ttl, client.install_snapshot(request)).await {
            Ok(Ok(reply)) => decode_reply(reply.get_ref()),
            Ok(Err(status)) => {
                self.invalidate();
                Err(unreachable(TransportError(status.to_string())))
            }
            Err(_) => {
                self.invalidate();
                Err(unreachable(TransportError(format!(
                    "no reply within {ttl:?} (client-side deadline)"
                ))))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Leader forwarding client
// ---------------------------------------------------------------------------

/// Dials whichever node openraft names as leader, for the two RPCs only
/// a leader can serve.
///
/// Separate from [`RaftChannelFactory`] because the lifetimes differ:
/// openraft keeps one network per *peer* for as long as that peer is a
/// member, while leadership moves. Channels are cached by address, so a
/// stable leader is dialed once and a leadership change costs one dial.
#[derive(Clone)]
pub struct LeaderClient {
    tls: Option<Arc<ClientConfig>>,
    channels: Arc<Mutex<HashMap<String, PgAgentRaftClient<Channel>>>>,
    /// Client-side bound on every forwarded RPC. See
    /// [`LEADER_RPC_TIMEOUT`] for why this cannot be a header.
    deadline: Duration,
}

impl LeaderClient {
    pub fn new(tls: Option<Arc<ClientConfig>>) -> Self {
        Self {
            tls,
            channels: Arc::new(Mutex::new(HashMap::new())),
            deadline: LEADER_RPC_TIMEOUT,
        }
    }

    pub fn new_dev() -> Self {
        Self::new(None)
    }

    /// Override the forwarded-RPC deadline (tests).
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    async fn client(&self, addr: &str) -> anyhow::Result<PgAgentRaftClient<Channel>> {
        {
            let cache = self.channels.lock().await;
            if let Some(c) = cache.get(addr) {
                return Ok(c.clone());
            }
        }
        let channel = dial(addr, self.tls.clone()).await?;
        let client = PgAgentRaftClient::new(channel);
        self.channels
            .lock()
            .await
            .insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// Forget the channel to `addr`. Called whenever an RPC over it
    /// fails, for the same reason the replication path invalidates: a
    /// channel that failed once is often half-open, and leadership has
    /// probably moved anyway.
    async fn invalidate(&self, addr: &str) {
        self.channels.lock().await.remove(addr);
    }

    /// Propose a command on the leader. A *lost* CAS comes back as an
    /// ordinary [`CommandResponse`] — losing a race is an outcome, not
    /// a failure, and collapsing it into `Err` would make the caller
    /// unable to tell "someone else won" from "we never asked".
    pub async fn propose(
        &self,
        addr: &str,
        cmd: &ConsensusCommand,
    ) -> anyhow::Result<CommandResponse> {
        let mut client = self.client(addr).await?;
        let frame = RaftFrame {
            payload: serde_json::to_vec(cmd)?,
        };
        match tokio::time::timeout(self.deadline, client.propose(Request::new(frame))).await {
            Ok(Ok(reply)) => Ok(serde_json::from_slice(&reply.get_ref().payload)?),
            Ok(Err(status)) => {
                self.invalidate(addr).await;
                Err(anyhow::anyhow!("raft propose via {addr}: {status}"))
            }
            Err(_) => {
                self.invalidate(addr).await;
                Err(anyhow::anyhow!(
                    "raft propose via {addr}: no reply within {:?} \
                     (leader unreachable or gone)",
                    self.deadline
                ))
            }
        }
    }

    /// Linearizable read on the leader.
    pub async fn read_state(&self, addr: &str) -> anyhow::Result<ClusterState> {
        let mut client = self.client(addr).await?;
        let frame = RaftFrame {
            payload: Vec::new(),
        };
        match tokio::time::timeout(self.deadline, client.read_state(Request::new(frame))).await {
            Ok(Ok(reply)) => Ok(serde_json::from_slice(&reply.get_ref().payload)?),
            Ok(Err(status)) => {
                self.invalidate(addr).await;
                Err(anyhow::anyhow!("raft read_state via {addr}: {status}"))
            }
            Err(_) => {
                self.invalidate(addr).await;
                Err(anyhow::anyhow!(
                    "raft read_state via {addr}: no reply within {:?} \
                     (leader unreachable or gone)",
                    self.deadline
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Mints one [`RaftPeerNetwork`] per target.
///
/// `tls == None` is the dev / single-node path, mirroring
/// [`PeerPool::new_dev`](crate::peers::PeerPool::new_dev): plain TCP,
/// valid only where the daemon's preflight has already established there
/// are no remote peers.
#[derive(Clone)]
pub struct RaftChannelFactory {
    tls: Option<Arc<ClientConfig>>,
    /// Addresses last seen for each target, so a `new_client` call whose
    /// `BasicNode` carries an empty address (possible when membership
    /// was bootstrapped without one) can still resolve.
    known: Arc<Mutex<HashMap<RaftNodeId, String>>>,
}

impl RaftChannelFactory {
    pub fn new(tls: Option<Arc<ClientConfig>>) -> Self {
        Self {
            tls,
            known: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Plain-TCP factory for dev and for tests.
    pub fn new_dev() -> Self {
        Self::new(None)
    }
}

impl RaftNetworkFactory<PgAgentTypeConfig> for RaftChannelFactory {
    type Network = RaftPeerNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        let mut known = self.known.lock().await;
        if !node.addr.is_empty() {
            known.insert(target, node.addr.clone());
        }
        let addr = known.get(&target).cloned().unwrap_or_default();
        if addr.is_empty() {
            // Not fatal, and not something this method may report: the
            // resulting client fails every RPC as Unreachable, which is
            // the honest description of a peer with no address.
            warn!(
                target,
                "raft: no address known for peer; RPCs to it will fail as unreachable"
            );
        }
        RaftPeerNetwork {
            target,
            addr,
            tls: self.tls.clone(),
            client: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raftstore::{open_database, ConsensusCommand, RedbLogStore, RedbStateMachine};
    use openraft::Config;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;

    /// One in-process node: a real Raft over the real redb store, served
    /// by the real gRPC service on a real localhost socket.
    struct Node {
        raft: PgAgentRaftHandle,
        addr: String,
        _dir: TempDir,
    }

    async fn spawn_node(id: RaftNodeId, shutdown: CancellationToken) -> Node {
        let dir = TempDir::new().unwrap();
        let db = open_database(dir.path()).unwrap();
        let reader = crate::raftstore::ClusterStateReader::new(db.clone());
        let log = RedbLogStore::new(db.clone());
        let sm = RedbStateMachine::new(db).unwrap();

        // Timings well below the production defaults: these tests want
        // an election in milliseconds, not the seconds a real cluster
        // deliberately waits (config.rs's election_timeout_ms default is
        // long on purpose, so a rolling restart does not cascade).
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft = Raft::new(id, config, RaftChannelFactory::new_dev(), log, sm)
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let service = RaftGrpcService::new(raft.clone(), reader).into_server();
        let s = shutdown.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    s.cancelled().await
                })
                .await
        });

        Node {
            raft,
            addr,
            _dir: dir,
        }
    }

    /// Three real nodes over three real sockets elect a leader and commit
    /// a lease takeover. This is the transport's actual claim — that
    /// openraft can drive consensus through `PgAgentRaft` — and nothing
    /// short of running it proves the framing, the service, and the
    /// network impl agree.
    #[tokio::test]
    async fn three_nodes_elect_a_leader_and_commit_a_takeover() {
        let shutdown = CancellationToken::new();

        let mut nodes = Vec::new();
        for id in 0..3u64 {
            nodes.push(spawn_node(id, shutdown.clone()).await);
        }

        let mut members = BTreeMap::new();
        for (id, node) in nodes.iter().enumerate() {
            members.insert(id as RaftNodeId, BasicNode::new(node.addr.clone()));
        }
        nodes[0].raft.initialize(members).await.unwrap();

        // Wait for a leader rather than assuming node 0 wins — which one
        // leads is Raft's business, and asserting on it would make this
        // test a timing coin flip.
        //
        // Gate on the `initialize` entries being APPLIED rather than on a
        // leader merely being believed in: the write below goes straight
        // to that leader and fails if it has not committed its own log
        // yet. See `raftconsensus::tests::cluster` for the full account.
        for (i, n) in nodes.iter().enumerate() {
            n.raft
                .wait(Some(Duration::from_secs(15)))
                .metrics(
                    |m| {
                        m.current_leader.is_some()
                            && m.last_applied.map(|l| l.index).unwrap_or(0) >= 1
                    },
                    "leader elected and the initialize entries applied",
                )
                .await
                .unwrap_or_else(|e| panic!("node {i} never became ready: {e}"));
        }
        // Leadership can move between the gate above and the write
        // below, and `ForwardToLeader` names whoever holds it now — so
        // re-read the leader and follow the redirect rather than caching
        // the first answer. The redirect is a pre-proposal rejection, so
        // nothing was appended and re-issuing the takeover is safe; a CAS
        // is not idempotent and a blind retry of a COMMITTED one would
        // wrongly report `Lost`.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let write = loop {
            let leader = nodes[0]
                .raft
                .current_leader()
                .await
                .expect("a leader is known once the initialize entries are applied");
            let attempt = nodes[leader as usize]
                .raft
                .client_write(ConsensusCommand::Takeover {
                    candidate: 2,
                    expected: None,
                    at: chrono::DateTime::from_timestamp(1_000, 0).unwrap(),
                })
                .await;
            match attempt {
                Ok(w) => break w,
                Err(openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(..),
                )) if tokio::time::Instant::now() < deadline => {
                    // Leadership moved; go around and ask again.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("client_write to the leader failed: {e}"),
            }
        };

        match write.data {
            crate::raftstore::CommandResponse::Takeover(
                crate::consensus::TakeoverOutcome::Won { lease },
            ) => {
                assert_eq!(lease.holder, 2);
            }
            other => panic!("expected the takeover to win, got {other:?}"),
        }

        // And it reached the followers, which is the only reason any of
        // this exists.
        let committed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let applied: Vec<_> = nodes
                    .iter()
                    .map(|n| n.raft.metrics().borrow().last_applied)
                    .collect();
                if applied.iter().all(|a| *a == applied[0] && a.is_some()) {
                    return applied[0];
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("followers never caught up");
        assert!(committed.is_some());

        shutdown.cancel();
    }

    /// A peer that is not listening must fail as `Unreachable`, not
    /// `NetworkError` — openraft backs off on the former and hot-retries
    /// on the latter, and hot-retrying a partitioned peer burns CPU on
    /// the node still trying to hold quorum.
    #[tokio::test]
    async fn unreachable_peer_reports_unreachable() {
        let mut factory = RaftChannelFactory::new_dev();
        // Port 1 on loopback: reserved, never listening.
        let mut net = factory.new_client(7, &BasicNode::new("127.0.0.1:1")).await;

        let rpc = VoteRequest::new(openraft::Vote::new(1, 7), None);
        let err = net
            .vote(rpc, RPCOption::new(Duration::from_millis(500)))
            .await
            .expect_err("dial to a closed port must fail");

        assert!(
            matches!(err, RPCError::Unreachable(_)),
            "expected Unreachable, got {err:?}"
        );
    }

    /// A peer that accepts TCP but never answers — the shape of a
    /// network partition against an established channel, where there is
    /// no RST and no server to honour a header deadline — must fail at
    /// the client-side deadline, not the 30 s channel ceiling. This is
    /// the regression test for the R4 finding: one HA tick blocked 34 s
    /// on a forwarded read to a just-isolated leader, consuming the
    /// whole partition window.
    #[tokio::test]
    async fn black_holed_leader_fails_at_the_client_side_deadline() {
        // Accept connections and then say nothing, forever.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });

        let leader = LeaderClient::new_dev().with_deadline(Duration::from_millis(400));
        let started = std::time::Instant::now();
        let err = leader
            .read_state(&addr)
            .await
            .expect_err("a silent peer must not look like a leader");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "deadline did not bind: took {elapsed:?}"
        );
        assert!(
            err.to_string().contains("no reply within"),
            "unexpected error: {err}"
        );

        // The replication path has the same obligation, via hard_ttl.
        let mut factory = RaftChannelFactory::new_dev();
        let mut net = factory.new_client(3, &BasicNode::new(addr.clone())).await;
        let started = std::time::Instant::now();
        let err = net
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 3), None),
                RPCOption::new(Duration::from_millis(400)),
            )
            .await
            .expect_err("vote to a silent peer must time out");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "hard_ttl did not bind client-side"
        );
        assert!(matches!(err, RPCError::Unreachable(_)));
    }

    /// A `BasicNode` with no address is a misconfiguration, not a panic.
    #[tokio::test]
    async fn addressless_peer_fails_rather_than_panics() {
        let mut factory = RaftChannelFactory::new_dev();
        let mut net = factory.new_client(9, &BasicNode::new("")).await;

        let rpc = VoteRequest::new(openraft::Vote::new(1, 9), None);
        let err = net
            .vote(rpc, RPCOption::new(Duration::from_millis(500)))
            .await
            .expect_err("a peer with no address cannot be reached");
        assert!(matches!(err, RPCError::Unreachable(_)));
    }
}
