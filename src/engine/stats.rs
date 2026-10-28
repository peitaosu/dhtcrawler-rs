use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone)]
pub struct CrawlerStats {
    pub announce_count: Arc<AtomicU64>,
    pub get_peers_count: Arc<AtomicU64>,
}

impl CrawlerStats {
    pub fn announce(&self) {
        self.announce_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_peers(&self) {
        self.get_peers_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CrawlerStatsSnapshot {
        CrawlerStatsSnapshot {
            announce_count: self.announce_count.load(Ordering::Relaxed),
            get_peers_count: self.get_peers_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CrawlerStatsSnapshot {
    pub announce_count: u64,
    pub get_peers_count: u64,
}

#[derive(Debug, Default, Clone)]
pub struct ReaderStats {
    pub download_ok: Arc<AtomicU64>,
    pub download_failed: Arc<AtomicU64>,
    pub cache_hits: Arc<AtomicU64>,
}

impl ReaderStats {
    pub fn download_ok(&self) {
        self.download_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn download_failed(&self) {
        self.download_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReaderStatsSnapshot {
        ReaderStatsSnapshot {
            download_ok: self.download_ok.load(Ordering::Relaxed),
            download_failed: self.download_failed.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReaderStatsSnapshot {
    pub download_ok: u64,
    pub download_failed: u64,
    pub cache_hits: u64,
}

pub const ANNOUNCE_PEER_TTL_SECS: u64 = 30 * 60;
pub const ANNOUNCE_PEERS_PER_HASH: usize = 16;
pub const ANNOUNCE_SWEEP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

#[derive(Debug, Clone)]
struct PeerRecord {
    addr: SocketAddr,
    stored_at: std::time::Instant,
}

impl Default for PeerRecord {
    fn default() -> Self {
        PeerRecord {
            addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            stored_at: std::time::Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
pub struct AnnouncePeerCache {
    peers: Mutex<HashMap<String, Vec<PeerRecord>>>,
    max_per_hash: usize,
}

impl AnnouncePeerCache {
    pub fn new() -> Self {
        AnnouncePeerCache {
            peers: Mutex::new(HashMap::new()),
            max_per_hash: ANNOUNCE_PEERS_PER_HASH,
        }
    }

    pub fn insert(&self, info_hash: &str, addr: SocketAddr) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        let records = peers.entry(info_hash.to_string()).or_default();
        if let Some(existing) = records.iter_mut().find(|r| r.addr == addr) {
            existing.stored_at = std::time::Instant::now();
            return;
        }
        if records.len() >= self.max_per_hash {
            records.remove(0);
        }
        records.push(PeerRecord {
            addr,
            stored_at: std::time::Instant::now(),
        });
    }

    pub fn peers_for(&self, info_hash: &str) -> Vec<SocketAddr> {
        let Ok(peers) = self.peers.lock() else {
            return Vec::new();
        };
        peers
            .get(info_hash)
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.stored_at.elapsed().as_secs() < ANNOUNCE_PEER_TTL_SECS)
                    .map(|r| r.addr)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn sweep(&self) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        peers.retain(|_, records| {
            records.retain(|r| r.stored_at.elapsed().as_secs() < ANNOUNCE_PEER_TTL_SECS);
            !records.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crawler_stats_counters() {
        let stats = CrawlerStats::default();
        stats.announce();
        stats.announce();
        stats.get_peers();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.announce_count, 2);
        assert_eq!(snapshot.get_peers_count, 1);
    }

    #[test]
    fn reader_stats_counters() {
        let stats = ReaderStats::default();
        stats.download_ok();
        stats.download_failed();
        stats.cache_hit();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.download_ok, 1);
        assert_eq!(snapshot.download_failed, 1);
        assert_eq!(snapshot.cache_hits, 1);
    }

    #[test]
    fn announce_peer_cache_dedup_and_eviction() {
        let cache = AnnouncePeerCache::new();
        let hash = "A".repeat(40);
        cache.insert(&hash, "1.1.1.1:1".parse().unwrap());
        cache.insert(&hash, "1.1.1.1:1".parse().unwrap());
        assert_eq!(cache.peers_for(&hash).len(), 1);
        for i in 0..20 {
            cache.insert(&hash, format!("9.9.9.9:{}", i).parse().unwrap());
        }
        assert!(cache.peers_for(&hash).len() <= ANNOUNCE_PEERS_PER_HASH);
        assert!(cache.peers_for(&"B".repeat(40)).is_empty());
    }
}
