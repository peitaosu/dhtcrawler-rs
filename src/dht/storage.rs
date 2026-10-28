use crate::dht::id::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub const PEER_EXPIRE_TIME: Duration = Duration::from_secs(30 * 60);
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub const MAX_PEERS_PER_HASH: usize = 64;
pub const MAX_QUERY_PEERS: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPeer {
    addr: SocketAddr,
    stored_at: Instant,
}

#[derive(Debug, Default)]
pub struct PeerStore {
    hashes: HashMap<NodeId, Vec<StoredPeer>>,
}

impl PeerStore {
    pub fn new() -> Self {
        PeerStore::default()
    }

    pub fn insert(&mut self, info_hash: NodeId, addr: SocketAddr) {
        let peers = self.hashes.entry(info_hash).or_default();
        if let Some(existing) = peers.iter_mut().find(|p| p.addr == addr) {
            existing.stored_at = Instant::now();
            return;
        }
        if peers.len() >= MAX_PEERS_PER_HASH {
            peers.remove(0);
        }
        peers.push(StoredPeer {
            addr,
            stored_at: Instant::now(),
        });
    }

    pub fn query(&self, info_hash: &NodeId) -> Vec<SocketAddr> {
        self.hashes
            .get(info_hash)
            .map(|peers| peers.iter().take(MAX_QUERY_PEERS).map(|p| p.addr).collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn sweep(&mut self) -> usize {
        let mut removed = 0;
        self.hashes.retain(|_, peers| {
            let before = peers.len();
            peers.retain(|p| p.stored_at.elapsed() < PEER_EXPIRE_TIME);
            removed += before - peers.len();
            !peers.is_empty()
        });
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> NodeId {
        let mut bytes = [0u8; 20];
        bytes[0] = byte;
        bytes[19] = byte + 1;
        NodeId(bytes)
    }

    #[test]
    fn insert_query_dedup() {
        let mut store = PeerStore::new();
        let addr: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        store.insert(hash(1), addr);
        store.insert(hash(1), addr);
        assert_eq!(store.query(&hash(1)), vec![addr]);
        assert!(store.query(&hash(2)).is_empty());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn sweep_empty() {
        let mut store = PeerStore::new();
        store.insert(hash(3), "5.5.5.5:1".parse().unwrap());
        assert_eq!(store.sweep(), 0);
        let mut drained = PeerStore::new();
        drained.insert(hash(4), "6.6.6.6:1".parse().unwrap());
        drained.hashes.get_mut(&hash(4)).unwrap().iter_mut().for_each(|p| {
            p.stored_at = Instant::now() - PEER_EXPIRE_TIME - Duration::from_secs(1);
        });
        assert_eq!(drained.sweep(), 1);
        assert!(drained.is_empty());
    }

    #[test]
    fn per_hash_cap_evicts_oldest_and_query_is_capped() {
        let mut store = PeerStore::new();
        for i in 0..MAX_PEERS_PER_HASH + 5 {
            store.insert(hash(9), format!("7.7.7.7:{}", i).parse().unwrap());
        }
        let peers = store.query(&hash(9));
        assert_eq!(peers.len(), MAX_QUERY_PEERS);
        let all = store.hashes.get(&hash(9)).unwrap().len();
        assert_eq!(all, MAX_PEERS_PER_HASH);
        assert!(peers.contains(&"7.7.7.7:5".parse::<SocketAddr>().unwrap()));
        assert!(!peers.contains(&"7.7.7.7:0".parse::<SocketAddr>().unwrap()));
    }
}
