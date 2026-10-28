use crate::dht::id::NodeId;
use crate::dht::message::{self, Inbound, Method, QueryArgs, ResponseValues};
use crate::dht::routing::{Node, RoutingTable};
use crate::dht::state;
use crate::dht::storage::PeerStore;
use crate::dht::token::TokenIssuer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

pub const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
pub const NODE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
pub const SAVE_INTERVAL: Duration = Duration::from_secs(5 * 60);
pub const PENDING_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub port: u16,
    pub id: NodeId,
    pub state_dir: Option<PathBuf>,
    pub bootstrap: Vec<String>,
}

impl NodeConfig {
    pub fn new(port: u16, id: NodeId) -> Self {
        NodeConfig {
            port,
            id,
            state_dir: None,
            bootstrap: vec![
                "router.bittorrent.com:6881".into(),
                "dht.transmissionbt.com:6881".into(),
                "dht.libtorrent.org:6881".into(),
            ],
        }
    }

    pub fn state_file(&self) -> Option<PathBuf> {
        self.state_dir
            .as_ref()
            .map(|dir| state::state_file_path(&dir.to_string_lossy(), self.port))
    }
}

#[derive(Debug, Clone)]
pub enum MonitorEvent {
    GetPeers { info_hash: NodeId },
    AnnouncePeer { info_hash: NodeId, addr: SocketAddr },
    Startup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerResponse {
    pub sender: NodeId,
    pub values: ResponseValues,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum QueryError {
    #[error("query timed out")]
    Timeout,
    #[error("query failed: {0}")]
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct GetPeersResult {
    pub sender: NodeId,
    pub token: Option<Vec<u8>>,
    pub peers: Vec<SocketAddr>,
    pub nodes: Vec<Node>,
}

#[derive(Debug)]
enum Command {
    Ping {
        addr: SocketAddr,
        reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    },
    FindNode {
        addr: SocketAddr,
        target: NodeId,
        reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    },
    GetPeers {
        addr: SocketAddr,
        target: NodeId,
        reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    },
    AnnouncePeer {
        addr: SocketAddr,
        info_hash: NodeId,
        token: Vec<u8>,
        port: u16,
        reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    },
    RemoveNode {
        id: NodeId,
        addr: SocketAddr,
    },
    SaveState {
        reply: oneshot::Sender<Result<(), String>>,
    },
    TableSnapshot {
        reply: oneshot::Sender<Vec<Node>>,
    },
}

#[derive(Clone)]
pub struct DhtNode {
    own_id: NodeId,
    port: u16,
    saved_addrs: Vec<SocketAddr>,
    cmd_tx: mpsc::Sender<Command>,
}

impl DhtNode {
    pub async fn start(
        config: NodeConfig,
        events: mpsc::Sender<MonitorEvent>,
    ) -> Result<Self, String> {
        let addr = format!("0.0.0.0:{}", config.port);
        let socket =
            UdpSocket::bind(&addr).await.map_err(|e| format!("bind {}: {}", addr, e))?;
        let (cmd_tx, cmd_rx) = mpsc::channel(1024);
        let cmd_tx = Arc::new(cmd_tx);
        let mut own_id = config.id;
        let mut saved_addrs: Vec<SocketAddr> = Vec::new();
        if let Some(path) = &config.state_file() {
            match state::load(path) {
                Ok((loaded_id, addrs)) if loaded_id.is_valid() => {
                    own_id = loaded_id;
                    saved_addrs = addrs;
                }
                _ => {}
            }
        }
        let actor = Actor {
            socket,
            own_id,
            port: config.port,
            table: RoutingTable::new(own_id),
            pending: HashMap::new(),
            seq: 0,
            peers: PeerStore::new(),
            tokens: TokenIssuer::new(),
            events,
            state_file: config.state_file(),
            cmd_tx: Arc::downgrade(&cmd_tx),
        };
        let handle = DhtNode {
            own_id,
            port: config.port,
            saved_addrs: saved_addrs.clone(),
            cmd_tx: (*cmd_tx).clone(),
        };
        tokio::spawn(actor.run(cmd_rx));
        Ok(handle)
    }

    pub fn saved_bootstrap_addrs(&self) -> &[SocketAddr] {
        &self.saved_addrs
    }

    pub fn own_id(&self) -> NodeId {
        self.own_id
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn ping(&self, addr: SocketAddr) -> Result<NodeId, QueryError> {
        let rx = self
            .query_command(|reply| Command::Ping { addr, reply })
            .await?;
        let response = recv_with_timeout(rx).await?;
        Ok(response.sender)
    }

    pub async fn find_node(
        &self,
        addr: SocketAddr,
        target: NodeId,
    ) -> Result<(NodeId, Vec<Node>), QueryError> {
        let rx = self
            .query_command(|reply| Command::FindNode { addr, target, reply })
            .await?;
        let response = recv_with_timeout(rx).await?;
        Ok((response.sender, response.values.nodes))
    }

    pub async fn get_peers(
        &self,
        addr: SocketAddr,
        target: NodeId,
    ) -> Result<GetPeersResult, QueryError> {
        let rx = self
            .query_command(|reply| Command::GetPeers { addr, target, reply })
            .await?;
        let response = recv_with_timeout(rx).await?;
        Ok(GetPeersResult {
            sender: response.sender,
            token: response.values.token.clone(),
            peers: response.values.peers,
            nodes: response.values.nodes,
        })
    }

    pub async fn announce_peer(
        &self,
        addr: SocketAddr,
        info_hash: NodeId,
        token: Vec<u8>,
        port: u16,
    ) -> Result<NodeId, QueryError> {
        let rx = self
            .query_command(|reply| Command::AnnouncePeer {
                addr,
                info_hash,
                token,
                port,
                reply,
            })
            .await?;
        let response = recv_with_timeout(rx).await?;
        Ok(response.sender)
    }

    pub async fn table_snapshot(&self) -> Vec<Node> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::TableSnapshot { reply: tx })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn save_state(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SaveState { reply: tx })
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())?
    }

    async fn query_command<F>(
        &self,
        make: F,
    ) -> Result<oneshot::Receiver<Result<PeerResponse, QueryError>>, QueryError>
    where
        F: FnOnce(oneshot::Sender<Result<PeerResponse, QueryError>>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(make(tx))
            .await
            .map_err(|_| QueryError::Failed("node stopped".into()))?;
        Ok(rx)
    }
}

async fn recv_with_timeout(
    rx: oneshot::Receiver<Result<PeerResponse, QueryError>>,
) -> Result<PeerResponse, QueryError> {
    match tokio::time::timeout(QUERY_TIMEOUT, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(QueryError::Failed("node stopped".into())),
        Err(_) => Err(QueryError::Timeout),
    }
}

struct PendingQuery {
    reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    deadline: Instant,
}

struct Actor {
    socket: UdpSocket,
    own_id: NodeId,
    port: u16,
    table: RoutingTable,
    pending: HashMap<Vec<u8>, PendingQuery>,
    seq: u16,
    peers: PeerStore,
    tokens: TokenIssuer,
    events: mpsc::Sender<MonitorEvent>,
    state_file: Option<PathBuf>,
    cmd_tx: Weak<mpsc::Sender<Command>>,
}

impl Actor {
    async fn run(mut self, mut cmd_rx: mpsc::Receiver<Command>) {
        let mut save_tick = tokio::time::interval(SAVE_INTERVAL);
        let mut sweep_tick = tokio::time::interval(crate::dht::storage::SWEEP_INTERVAL);
        let mut keepalive_tick = tokio::time::interval(KEEPALIVE_INTERVAL);
        let mut pending_tick = tokio::time::interval(PENDING_SWEEP_INTERVAL);
        let mut buf = vec![0u8; 65536];
        loop {
            tokio::select! {
                command = cmd_rx.recv() => match command {
                    None => {
                        self.persist_state();
                        break;
                    }
                    Some(command) => self.handle_command(command).await,
                },
                result = self.socket.recv_from(&mut buf) => match result {
                    Ok((len, addr)) => {
                        let data = buf[..len].to_vec();
                        self.handle_packet(data, addr).await;
                    }
                    Err(e) => {
                        tracing::warn!("dht node {} recv error: {}", self.port, e);
                    }
                },
                _ = save_tick.tick() => self.persist_state_async(),
                _ = sweep_tick.tick() => {
                    let removed = self.peers.sweep();
                    if removed > 0 {
                        tracing::debug!("dht node {} swept {} stored peers", self.port, removed);
                    }
                }
                _ = keepalive_tick.tick() => self.check_inactive_nodes(),
                _ = pending_tick.tick() => self.expire_pending(),
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Ping { addr, reply } => {
                self.send_query(Method::Ping, addr, QueryArgs::default(), reply).await
            }
            Command::FindNode { addr, target, reply } => {
                self.send_query(Method::FindNode, addr, QueryArgs { target: Some(target), ..Default::default() }, reply).await
            }
            Command::GetPeers { addr, target, reply } => {
                self.send_query(Method::GetPeers, addr, QueryArgs { info_hash: Some(target), ..Default::default() }, reply).await
            }
            Command::AnnouncePeer { addr, info_hash, token, port, reply } => {
                self.send_query(Method::AnnouncePeer, addr, QueryArgs { info_hash: Some(info_hash), token: Some(token), port: Some(port), ..Default::default() }, reply).await
            }
            Command::RemoveNode { id, addr } => {
                self.table.remove(&id, &addr);
            }
            Command::SaveState { reply } => {
                let _ = reply.send(self.persist_state_result());
            }
            Command::TableSnapshot { reply } => {
                let _ = reply.send(self.table.all());
            }
        }
    }

    async fn send_query(
        &mut self,
        method: Method,
        addr: SocketAddr,
        args: QueryArgs,
        reply: oneshot::Sender<Result<PeerResponse, QueryError>>,
    ) {
        if addr.ip().is_loopback() && addr.port() == self.port {
            let _ = reply.send(Err(QueryError::Failed("refuse to query self".into())));
            return;
        }
        let tid = method.make_tid(self.seq);
        self.seq = self.seq.wrapping_add(1);
        let packet = message::encode_query(&tid, method, &self.own_id, &args);
        if let Err(e) = self.socket.send_to(&packet, addr).await {
            let _ = reply.send(Err(QueryError::Failed(format!("udp send: {}", e))));
            return;
        }
        self.pending.insert(
            tid,
            PendingQuery {
                reply,
                deadline: Instant::now() + QUERY_TIMEOUT,
            },
        );
    }

    fn expire_pending(&mut self) {
        let now = Instant::now();
        let expired: Vec<Vec<u8>> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(tid, _)| tid.clone())
            .collect();
        for tid in expired {
            if let Some(pending) = self.pending.remove(&tid) {
                let _ = pending.reply.send(Err(QueryError::Timeout));
            }
        }
    }

    async fn handle_packet(&mut self, data: Vec<u8>, addr: SocketAddr) {
        match message::decode(&data) {
            Ok(Inbound::Query { tid, method, sender, args }) => {
                self.table.insert(sender, addr);
                self.answer_query(tid, method, args, addr).await;
            }
            Ok(Inbound::Response { tid, sender, values }) => {
                self.table.insert(sender, addr);
                self.resolve_pending(tid, Ok(PeerResponse { sender, values }));
            }
            Ok(Inbound::Error { tid }) => {
                self.resolve_pending(tid, Err(QueryError::Failed("remote error".into())));
            }
            Err(e) => {
                tracing::debug!("dht node {} bad packet from {}: {}", self.port, addr, e);
            }
        }
    }

    fn resolve_pending(&mut self, tid: Vec<u8>, result: Result<PeerResponse, QueryError>) {
        if let Some(pending) = self.pending.remove(&tid) {
            let _ = pending.reply.send(result);
        } else {
            tracing::debug!("dht node {} response for unknown tid", self.port);
        }
    }

    async fn answer_query(&mut self, tid: Vec<u8>, method: Method, args: QueryArgs, addr: SocketAddr) {
        match method {
            Method::Ping => {
                let packet = message::encode_ping_response(&tid, &self.own_id);
                let _ = self.socket.send_to(&packet, addr).await;
            }
            Method::FindNode => {
                let Some(target) = args.target else {
                    self.send_error(&tid, addr, 203, "missing target").await;
                    return;
                };
                let closest = self.table.closest(&target, crate::dht::routing::K);
                let packet = message::encode_response(
                    &tid,
                    &self.own_id,
                    ResponseValues { nodes: closest, ..Default::default() },
                    false,
                );
                let _ = self.socket.send_to(&packet, addr).await;
            }
            Method::GetPeers => {
                let Some(info_hash) = args.info_hash else {
                    self.send_error(&tid, addr, 203, "missing info_hash").await;
                    return;
                };
                let token = self.tokens.generate(addr.ip());
                let peers = self.peers.query(&info_hash);
                let include_peers = !peers.is_empty();
                let values = if peers.is_empty() {
                    ResponseValues {
                        token: Some(token),
                        nodes: self.table.closest(&info_hash, crate::dht::routing::K),
                        ..Default::default()
                    }
                } else {
                    ResponseValues {
                        token: Some(token),
                        peers,
                        ..Default::default()
                    }
                };
                let packet = message::encode_response(&tid, &self.own_id, values, include_peers);
                let _ = self.socket.send_to(&packet, addr).await;
                match self.events.try_send(MonitorEvent::GetPeers { info_hash }) {
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::debug!("dht node {} event queue full, dropped get_peers event", self.port);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                    Ok(()) => {}
                }
            }
            Method::AnnouncePeer => {
                let Some(info_hash) = args.info_hash else {
                    self.send_error(&tid, addr, 203, "missing info_hash").await;
                    return;
                };
                let Some(token) = &args.token else {
                    self.send_error(&tid, addr, 203, "missing token").await;
                    return;
                };
                if !self.tokens.verify(addr.ip(), token) {
                    tracing::debug!("dht node {} announce token mismatch from {}", self.port, addr);
                    self.send_error(&tid, addr, 203, "bad token").await;
                    return;
                }
                let port = if args.implied_port.unwrap_or(false) || args.port.unwrap_or(0) == 0 {
                    addr.port()
                } else {
                    args.port.unwrap()
                };
                let peer_addr = SocketAddr::new(addr.ip(), port);
                self.peers.insert(info_hash, peer_addr);
                match self
                    .events
                    .try_send(MonitorEvent::AnnouncePeer { info_hash, addr: peer_addr })
                {
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::debug!("dht node {} event queue full, dropped announce_peer event", self.port);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                    Ok(()) => {}
                }
                let packet = message::encode_ping_response(&tid, &self.own_id);
                let _ = self.socket.send_to(&packet, addr).await;
            }
        }
    }

    async fn send_error(&self, tid: &[u8], addr: SocketAddr, code: i64, message: &str) {
        let packet = message::encode_error(tid, code, message);
        let _ = self.socket.send_to(&packet, addr).await;
    }

    fn check_inactive_nodes(&mut self) {
        let inactive = self.table.inactive_since(NODE_TIMEOUT);
        if inactive.is_empty() {
            return;
        }
        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            return;
        };
        let handle = DhtNode {
            own_id: self.own_id,
            port: self.port,
            saved_addrs: Vec::new(),
            cmd_tx: (*cmd_tx).clone(),
        };
        for node in inactive {
            let handle = handle.clone();
            tokio::spawn(async move {
                let mut alive = false;
                for _ in 0..3 {
                    if handle.ping(node.addr).await.is_ok() {
                        alive = true;
                        break;
                    }
                }
                if !alive {
                    let _ = handle
                        .cmd_tx
                        .send(Command::RemoveNode { id: node.id, addr: node.addr })
                        .await;
                }
            });
        }
    }

    fn persist_state(&mut self) {
        let _ = self.persist_state_result();
    }

    fn persist_state_async(&self) {
        let Some(path) = &self.state_file else {
            return;
        };
        let path = path.clone();
        let node_id = self.own_id;
        let nodes: Vec<SocketAddr> = self.table.all().iter().map(|n| n.addr).collect();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = state::save(&path, &node_id, &nodes) {
                tracing::warn!("dht state save failed: {}", e);
            }
        });
    }

    fn persist_state_result(&self) -> Result<(), String> {
        let Some(path) = &self.state_file else {
            return Ok(());
        };
        let nodes: Vec<SocketAddr> = self.table.all().iter().map(|n| n.addr).collect();
        state::save(path, &self.own_id, &nodes)
    }
}

pub const BOOTSTRAP_SAVED_TRY_COUNT: usize = 16;

pub async fn bootstrap(node: &DhtNode, bootstrap_hosts: &[String], events: mpsc::Sender<MonitorEvent>) {
    loop {
        let mut joined_addr: Option<SocketAddr> = None;
        let mut saved: Vec<SocketAddr> = node.saved_bootstrap_addrs().to_vec();
        if saved.len() > BOOTSTRAP_SAVED_TRY_COUNT {
            let step = saved.len() / BOOTSTRAP_SAVED_TRY_COUNT;
            saved = saved.into_iter().step_by(step.max(1)).collect();
        }
        let saved_pings: Vec<_> = saved
            .iter()
            .map(|addr| async move { (*addr, node.ping(*addr).await.is_ok()) })
            .collect();
        for (addr, ok) in futures::future::join_all(saved_pings).await {
            if ok {
                joined_addr = Some(addr);
                break;
            }
        }
        if joined_addr.is_none() {
            for host in bootstrap_hosts {
                let Ok(addrs) = tokio::net::lookup_host(host).await else {
                    continue;
                };
                let resolved: Vec<SocketAddr> = addrs.collect();
                let pings: Vec<_> = resolved
                    .iter()
                    .map(|addr| async move { (*addr, node.ping(*addr).await.is_ok()) })
                    .collect();
                for (addr, ok) in futures::future::join_all(pings).await {
                    if ok {
                        joined_addr = Some(addr);
                        break;
                    }
                }
                if joined_addr.is_some() {
                    break;
                }
            }
        }
        if let Some(addr) = joined_addr {
            let _ = node.find_node(addr, node.own_id()).await;
            let alive = crate::dht::search::find_node(node, node.own_id()).await;
            tracing::debug!(
                "dht node {} bootstrap find_node(self) finished with {} alive nodes",
                node.port(),
                alive.len()
            );
            let _ = events.send(MonitorEvent::Startup).await;
            return;
        }
        tracing::warn!("dht node {} bootstrap failed, retry in 10s", node.port());
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}


