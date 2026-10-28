use crate::engine::hash_cache::HashCache;
use crate::engine::stats::{AnnouncePeerCache, CrawlerStats};
use crate::common::config::CrawlerConfig;
use crate::dht::{search, DhtNode, MonitorEvent, NodeConfig};
use crate::dht::NodeId;
use crate::store::Store;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub const STATS_DUMP_INTERVAL: Duration = Duration::from_secs(30 * 60);

#[async_trait::async_trait]
pub trait PeerSource: Send + Sync {
    async fn peers_for(&self, info_hash: &str) -> Vec<SocketAddr>;
}

struct SearchPeerSource {
    nodes: Vec<DhtNode>,
    announce_cache: Arc<AnnouncePeerCache>,
}

#[async_trait::async_trait]
impl PeerSource for SearchPeerSource {
    async fn peers_for(&self, info_hash: &str) -> Vec<SocketAddr> {
        let cache_hits = self.announce_cache.peers_for(info_hash);
        if !cache_hits.is_empty() {
            return cache_hits;
        }
        let Some(hash_id) = NodeId::from_hex(info_hash) else {
            return cache_hits;
        };
        let nodes = self.nodes.clone();
        let Some(node) = nodes
            .iter()
            .min_by_key(|node| hash_id.distance(&node.own_id()))
            .cloned()
        else {
            return cache_hits;
        };
        let result = search::get_peers(&node, hash_id).await;
        let mut peers = cache_hits;
        peers.extend(result.peers);
        let mut seen = std::collections::HashSet::new();
        peers.retain(|peer| seen.insert(*peer));
        peers
    }
}

pub struct Crawler {
    pub nodes: Vec<DhtNode>,
    pub stats: Arc<CrawlerStats>,
    pub announce_cache: Arc<AnnouncePeerCache>,
    pub peer_source: Arc<dyn PeerSource>,
    pub hash_cache: Option<Arc<HashCache>>,
}

impl Crawler {
    pub async fn start(
        crawler_config: &CrawlerConfig,
        store: Arc<dyn Store>,
    ) -> Result<Self, String> {
        let node_count = crawler_config.node_count.max(1);
        let ids = NodeId::random_discrete_ids(node_count as usize);
        let mut nodes = Vec::with_capacity(node_count as usize);
        let stats = Arc::new(CrawlerStats::default());
        let announce_cache = Arc::new(AnnouncePeerCache::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(4096);
        for (index, id) in ids.into_iter().enumerate() {
            let port = crawler_config
                .start_port
                .checked_add(index as u16)
                .ok_or("port range overflow")?;
            let mut config = NodeConfig::new(port, id);
            config.state_dir = Some(crawler_config.state_dir.clone().into());
            config.bootstrap = crawler_config.bootstrap.clone();
            match DhtNode::start(config, event_tx.clone()).await {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    tracing::error!("start dht node on port {} failed: {}", port, e);
                }
            }
        }
        if nodes.is_empty() {
            return Err("no dht node could be started".into());
        }
        for node in &nodes {
            let node = node.clone();
            let bootstrap = crawler_config.bootstrap.clone();
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                crate::dht::bootstrap(&node, &bootstrap, event_tx).await;
            });
        }
        let peer_source = Arc::new(SearchPeerSource {
            nodes: nodes.clone(),
            announce_cache: announce_cache.clone(),
        });
        let shared_cache = Arc::new(HashCache::new(
            store.clone(),
            crawler_config.hash_max_cache,
            crawler_config.cache_max_time_secs,
        ));
        let crawler = Crawler {
            nodes,
            stats: stats.clone(),
            announce_cache: announce_cache.clone(),
            peer_source: peer_source.clone(),
            hash_cache: Some(shared_cache.clone()),
        };
        for node in &crawler.nodes {
            let node = node.clone();
            let query_interval = Duration::from_secs(crawler_config.query_interval_secs.max(1));
            tokio::spawn(async move {
                loop {
                    let target = NodeId::random();
                    let result = search::get_peers(&node, target).await;
                    tracing::debug!(
                        "dht node {} searched random hash, {} peers, {} trackers",
                        node.port(),
                        result.peers.len(),
                        result.trackers.len()
                    );
                    tokio::time::sleep(query_interval).await;
                }
            });
        }
        let timer_cache = shared_cache.clone();
        tokio::spawn(async move {
            timer_cache.flush_timer().await;
        });
        let event_stats = stats.clone();
        let event_announce_cache = announce_cache.clone();
        let event_cache = shared_cache.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    MonitorEvent::GetPeers { info_hash } => {
                        event_stats.get_peers();
                        event_cache.insert(info_hash.to_hex()).await;
                    }
                    MonitorEvent::AnnouncePeer { info_hash, addr } => {
                        event_stats.announce();
                        event_announce_cache.insert(&info_hash.to_hex(), addr);
                        event_cache.insert(info_hash.to_hex()).await;
                    }
                    MonitorEvent::Startup => {
                        tracing::info!("dht instance joined the network");
                    }
                }
            }
        });
        let dump_stats = stats.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(STATS_DUMP_INTERVAL);
            loop {
                timer.tick().await;
                tracing::info!("crawler stats: {:?}", dump_stats.snapshot());
            }
        });
        let sweep_cache = announce_cache.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(crate::engine::stats::ANNOUNCE_SWEEP_INTERVAL);
            loop {
                timer.tick().await;
                sweep_cache.sweep();
            }
        });
        Ok(crawler)
    }

    pub async fn save_state(&self) {
        for node in &self.nodes {
            if let Err(e) = node.save_state().await {
                tracing::warn!("save dht node {} state failed: {}", node.port(), e);
            }
        }
    }

    pub async fn flush_caches(&self) {
        if let Some(hash_cache) = &self.hash_cache {
            hash_cache.flush_pending().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn peer_source_from_announce_cache() {
        let cache = Arc::new(AnnouncePeerCache::new());
        let hash = "B".repeat(40);
        cache.insert(&hash, "1.2.3.4:5".parse().unwrap());
        let source = SearchPeerSource {
            nodes: Vec::new(),
            announce_cache: cache,
        };
        let peers = source.peers_for(&hash).await;
        assert_eq!(peers, vec!["1.2.3.4:5".parse().unwrap()]);
    }
}
