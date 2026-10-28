use crate::dht::id::NodeId;
use crate::dht::node::{DhtNode, GetPeersResult};
use crate::dht::routing::Node;
use std::collections::HashSet;
use std::net::SocketAddr;

pub const SEARCH_WIDTH: usize = 32;
pub const SEARCH_RETRIES: usize = 4;

#[derive(Debug, Clone)]
pub struct Tracker {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub token: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchResult {
    pub trackers: Vec<Tracker>,
    pub peers: Vec<SocketAddr>,
    pub alive: Vec<Node>,
}

#[derive(Debug, Clone)]
struct Candidate {
    dist: [u8; 20],
    id: NodeId,
    addr: SocketAddr,
}

pub async fn find_node(node: &DhtNode, target: NodeId) -> Vec<Node> {
    run_search(node, Method::FindNode, target).await.alive
}

pub async fn get_peers(node: &DhtNode, info_hash: NodeId) -> SearchResult {
    run_search(node, Method::GetPeers, info_hash).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Method {
    FindNode,
    GetPeers,
}

async fn run_search(node: &DhtNode, method: Method, target: NodeId) -> SearchResult {
    let mut result = SearchResult::default();
    let start = node.table_snapshot().await;
    let mut next: Vec<Candidate> = start
        .iter()
        .map(|n| Candidate {
            dist: target.distance(&n.id),
            id: n.id,
            addr: n.addr,
        })
        .collect();
    next.sort_by(|a, b| a.dist.cmp(&b.dist));
    next.truncate(SEARCH_WIDTH);
    let mut queried: HashSet<(NodeId, SocketAddr)> = HashSet::new();
    let mut retries = 0usize;
    while retries < SEARCH_RETRIES {
        if next.is_empty() {
            break;
        }
        for candidate in &next {
            queried.insert((candidate.id, candidate.addr));
        }
        let responses = query_all(node, method, target, &next).await;
        let mut new_nodes: Vec<Node> = Vec::new();
        for outcome in &responses {
            result.alive.push(Node::new(outcome.result.sender, outcome.addr));
            if !outcome.result.peers.is_empty() {
                result.trackers.push(Tracker {
                    id: outcome.result.sender,
                    addr: outcome.addr,
                    token: outcome.result.token.clone(),
                });
                result.peers.extend(outcome.result.peers.clone());
            }
            new_nodes.extend(outcome.result.nodes.clone());
        }
        new_nodes.retain(|n| !queried.contains(&(n.id, n.addr)));
        new_nodes.sort_by(|a, b| target.distance_cmp(&a.id, &b.id));
        new_nodes.truncate(SEARCH_WIDTH);
        next = new_nodes
            .into_iter()
            .map(|n| Candidate {
                dist: target.distance(&n.id),
                id: n.id,
                addr: n.addr,
            })
            .collect();
        next.sort_by(|a, b| a.dist.cmp(&b.dist));
        let min_alive = result
            .alive
            .iter()
            .map(|n| target.distance(&n.id))
            .min();
        let min_queue = next.first().map(|c| c.dist);
        retries = match (min_queue, min_alive) {
            (Some(queue), Some(alive)) if queue < alive => 0,
            _ => retries + 1,
        };
    }
    result
}

struct QueryOutcome {
    addr: SocketAddr,
    result: GetPeersResult,
}

async fn query_all(
    node: &DhtNode,
    method: Method,
    target: NodeId,
    candidates: &[Candidate],
) -> Vec<QueryOutcome> {
    let mut futures = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let addr = candidate.addr;
            futures.push(async move {
                let outcome = match method {
                    Method::FindNode => node.find_node(addr, target).await.map(|(sender, nodes)| {
                        GetPeersResult {
                            sender,
                            token: None,
                            peers: Vec::new(),
                            nodes,
                        }
                    }),
                    Method::GetPeers => node.get_peers(addr, target).await,
                };
                outcome.map(|result| QueryOutcome { addr, result })
            });
        }
    futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}
