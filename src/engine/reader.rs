use crate::engine::crawler::PeerSource;
use crate::engine::download_cache::DownloadCache;
use crate::engine::stats::ReaderStats;
use crate::common::config::ReaderConfig;
use crate::common::hex;
use crate::common::segment;
use crate::metadata::{bep9, torrent};
use crate::store::{Store, TorrentDoc};
use std::sync::Arc;
use std::time::Duration;

pub const BEP9_PHASE_BUDGET_SECS: u64 = 120;
pub const BEP9_PER_PEER_TIMEOUT_SECS: u64 = 8;
pub const PEER_SEARCH_BUDGET_SECS: u64 = 30;

pub struct Reader {
    store: Arc<dyn Store>,
    config: ReaderConfig,
    download_cache: Arc<DownloadCache>,
    stats: Arc<ReaderStats>,
    peer_source: Arc<dyn PeerSource>,
    local_cache: Arc<crate::metadata::LocalCache>,
}

impl Reader {
    pub fn new(
        store: Arc<dyn Store>,
        config: ReaderConfig,
        peer_source: Arc<dyn PeerSource>,
    ) -> Self {
        let local_cache = Arc::new(crate::metadata::LocalCache::new(
            config.torrent_path.clone(),
        ));
        Reader {
            download_cache: Arc::new(DownloadCache::new(
                store.clone(),
                config.max_download_cache,
            )),
            store,
            config,
            stats: Arc::new(ReaderStats::default()),
            peer_source,
            local_cache,
        }
    }

    pub fn stats(&self) -> ReaderStatsSnapshotAlias {
        self.stats.snapshot()
    }

    pub async fn flush_pending(&self) {
        self.download_cache.flush_pending().await;
    }

    fn requeue_with_attempt(&self, mut pending: crate::store::PendingHash) {
        if pending.attempts >= crate::store::MAX_REQUEUE_ATTEMPTS {
            tracing::warn!(
                "dropping hash {} after {} failed attempts",
                pending.hash,
                pending.attempts
            );
            return;
        }
        pending.attempts += 1;
        let store = self.store.clone();
        tokio::task::spawn(async move {
            if let Err(e) = store.save_wait_download(pending).await {
                tracing::warn!("requeue to wait_download failed: {}", e);
            }
        });
    }

    pub async fn run(self: Arc<Self>) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_download_per_reader.max(1),
        ));
        let mut idle_counter = 0u32;
        loop {
            let Some(pending) = self.next_hash().await else {
                idle_counter += 1;
                if idle_counter % 60 == 1 {
                    tracing::debug!("reader idle, waiting for new hashes");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            idle_counter = 0;
            if self.try_fast_path(&pending).await {
                continue;
            }
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.download_cache.insert(pending);
                    continue;
                }
            };
            let reader = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                reader.process_download(pending).await;
            });
        }
    }

    async fn next_hash(&self) -> Option<crate::store::PendingHash> {
        match self.store.pop_pending_hash().await {
            Ok(pending) => {
                if pending.is_some() {
                    return pending;
                }
            }
            Err(e) => {
                tracing::warn!("pop pending hash failed: {}", e);
            }
        }
        self.download_cache.get_one().await
    }

    async fn try_fast_path(&self, pending: &crate::store::PendingHash) -> bool {
        match self.store.inc_announce(&pending.hash, pending.req_cnt.max(1)).await {
            Ok(true) => {
                if let Err(e) = self
                    .store
                    .daterange_insert(&pending.hash, pending.req_cnt.max(1), false)
                    .await
                {
                    tracing::warn!("daterange insert failed: {}", e);
                }
                if let Err(e) = self.store.stats_updated().await {
                    tracing::warn!("stats updated failed: {}", e);
                }
                if let Err(e) = self.store.stats_get_peers().await {
                    tracing::warn!("stats get_peers failed: {}", e);
                }
                true
            }
            Ok(false) => false,
            Err(e) => {
                tracing::warn!(
                    "inc_announce failed for {}: {}, requeueing to wait_download",
                    pending.hash,
                    e
                );
                self.requeue_with_attempt(pending.clone());
                true
            }
        }
    }

    async fn process_download(self: Arc<Self>, pending: crate::store::PendingHash) {
        let hash = pending.hash.clone();
        if let Some(content) = self.load_cached(&hash).await {
            self.stats.cache_hit();
            self.handle_content(&pending, &hash, content).await;
            return;
        }
        let deadline =
            std::time::Instant::now() + Duration::from_secs(BEP9_PHASE_BUDGET_SECS);
        if self
            .clone()
            .bep9_phase(pending.clone(), hash.clone(), deadline)
            .await
            .is_some()
        {
            return;
        }
        self.stats.download_failed();
        tracing::debug!("download metadata for {} failed", hash);
        self.requeue_with_attempt(pending);
    }

    async fn bep9_phase(
        self: Arc<Self>,
        pending: crate::store::PendingHash,
        hash: String,
        deadline: std::time::Instant,
    ) -> Option<()> {
        let Some(info_hash) = crate::dht::NodeId::from_hex(&hash) else {
            tracing::warn!("invalid info hash {}", hash);
            return None;
        };
        let search_budget = Duration::from_secs(PEER_SEARCH_BUDGET_SECS);
        let peers = match tokio::time::timeout(search_budget, self.peer_source.peers_for(&hash)).await {
            Ok(peers) => peers,
            Err(_) => {
                tracing::debug!("peer search for {} exceeded {}s budget", hash, PEER_SEARCH_BUDGET_SECS);
                Vec::new()
            }
        };
        tracing::debug!(
            "peer search for {} found {} peers (budget {}s)",
            hash,
            peers.len(),
            PEER_SEARCH_BUDGET_SECS
        );
        for peer in peers {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let per_peer = remaining.min(Duration::from_secs(BEP9_PER_PEER_TIMEOUT_SECS));
            match bep9::fetch_metadata(peer, info_hash.0, bep9::random_peer_id(), per_peer).await {
                Ok(metadata) => {
                    self.stats.download_ok();
                    let content = torrent::wrap_as_torrent_file(&metadata);
                    self.handle_content(&pending, &hash, content).await;
                    return Some(());
                }
                Err(e) => {
                    tracing::debug!("bep9 metadata from {} failed: {}", peer, e);
                }
            }
        }
        None
    }

    async fn load_cached(&self, hash: &str) -> Option<Vec<u8>> {
        if let Some(content) = self.local_cache.load(hash) {
            return Some(content);
        }
        if self.config.load_from_db {
            match self.store.load_torrent_content(hash).await {
                Ok(content) => content,
                Err(e) => {
                    tracing::warn!("load torrent content {} from db failed: {}", hash, e);
                    None
                }
            }
        } else {
            None
        }
    }

    async fn handle_content(
        &self,
        pending: &crate::store::PendingHash,
        hash: &str,
        content: Vec<u8>,
    ) {
        match torrent::info_hash(&content) {
            Ok(content_hash) if hex::encode_upper(&content_hash).eq_ignore_ascii_case(hash) => {}
            Ok(_) => {
                tracing::warn!("metadata hash mismatch for {}, dropping content", hash);
                return;
            }
            Err(e) => {
                tracing::warn!("verify metadata for {} failed: {}, dropping", hash, e);
                return;
            }
        }
        if self.config.save_torrent {
            if self.config.save_to_file {
                if let Err(e) = self.local_cache.save(hash, &content) {
                    tracing::warn!("save torrent file {} failed: {}", hash, e);
                }
            }
            if self.config.save_to_db {
                let _ = self.store.save_torrent_content(hash, &content).await;
            }
        }
        match torrent::parse(&content) {
            Ok(torrent) => {
                let announce = pending.req_cnt.max(1);
                let doc = self.build_doc(hash, &torrent, announce);
                if let Err(e) = self.store.insert_torrent(doc).await {
                    tracing::warn!("insert torrent {} failed: {}", hash, e);
                    return;
                }
                if let Err(e) = self
                    .store
                    .daterange_insert(hash, announce.max(1), true)
                    .await
                {
                    tracing::warn!("daterange insert failed: {}", e);
                }
                if let Err(e) = self.store.stats_new_saved().await {
                    tracing::warn!("stats new_saved failed: {}", e);
                }
                if let Err(e) = self.store.stats_get_peers().await {
                    tracing::warn!("stats get_peers failed: {}", e);
                }
                tracing::info!("saved torrent {} ({})", hash, torrent.name());
            }
            Err(e) => {
                tracing::warn!("parse torrent {} failed: {}", hash, e);
            }
        }
    }

    fn build_doc(
        &self,
        hash: &str,
        torrent: &torrent::Torrent,
        announce: u64,
    ) -> TorrentDoc {
        let name = torrent.name();
        TorrentDoc {
            hash: hash.to_string(),
            name: name.to_string(),
            length: torrent.length(),
            created_at: crate::common::time_util::now_seconds(),
            announce,
            name_array: segment::seg_tokens(self.config.text_seg, name),
            files: torrent.files(),
        }
    }
}

pub type ReaderStatsSnapshotAlias = crate::engine::stats::ReaderStatsSnapshot;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use std::net::SocketAddr;

    struct EmptyPeers;

    #[async_trait::async_trait]
    impl PeerSource for EmptyPeers {
        async fn peers_for(&self, _info_hash: &str) -> Vec<SocketAddr> {
            Vec::new()
        }
    }

    fn reader_config() -> ReaderConfig {
        ReaderConfig {
            torrent_path: std::env::temp_dir()
                .join(format!("reader-test-{}-{}", std::process::id(), std::time::SystemTime::now().elapsed().unwrap_or_default().as_nanos()))
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        }
    }

    fn info_dict_content() -> Vec<u8> {
        b"d4:infod6:lengthi1e4:name4:test12:piece lengthi16384eee".to_vec()
    }

    fn info_dict_hash() -> String {
        let value = crate::common::bencode::decode(&info_dict_content()).unwrap();
        let info = value.str_key("info").unwrap();
        let info_bytes = crate::common::bencode::encode(info);
        hex::encode_upper(&crate::metadata::torrent::info_hash(&info_bytes).unwrap())
    }

    #[tokio::test]
    async fn fast_path_updates_existing_torrents() {
        let store = Arc::new(MemoryStore::new());
        let hash = info_dict_hash();
        let torrent = crate::metadata::parse(&info_dict_content()).unwrap();
        let doc = TorrentDoc {
            hash: hash.clone(),
            name: torrent.name().to_string(),
            length: torrent.length(),
            created_at: 1000,
            announce: 1,
            files: torrent.files(),
            name_array: vec![],
        };
        store.insert_torrent(doc).await.unwrap();
        let reader = Reader::new(store.clone(), reader_config(), Arc::new(EmptyPeers));
        let pending = crate::store::PendingHash {
            hash: hash.clone(),
            req_cnt: 3,
            req_at: 0,
            attempts: 0,
        };
        assert!(reader.try_fast_path(&pending).await);
        let doc = store.get_torrent(&hash).await.unwrap().unwrap();
        assert_eq!(doc.announce, 4);
    }

    #[tokio::test]
    async fn content_with_wrong_hash_is_rejected() {
        let store = Arc::new(MemoryStore::new());
        let reader = Reader::new(
            store.clone(),
            ReaderConfig {
                save_torrent: true,
                save_to_db: true,
                ..reader_config()
            },
            Arc::new(EmptyPeers),
        );
        let pending = crate::store::PendingHash {
            hash: "B".repeat(40),
            req_cnt: 2,
            req_at: 0,
            attempts: 0,
        };
        reader.handle_content(&pending, &"B".repeat(40), info_dict_content()).await;
        assert!(store.get_torrent(&"B".repeat(40)).await.unwrap().is_none());
        assert!(store
            .load_torrent_content(&"B".repeat(40))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn matching_content_is_saved_and_parsed() {
        let store = Arc::new(MemoryStore::new());
        let reader = Reader::new(
            store.clone(),
            ReaderConfig {
                save_torrent: true,
                save_to_file: false,
                save_to_db: true,
                ..reader_config()
            },
            Arc::new(EmptyPeers),
        );
        let hash = info_dict_hash();
        let pending = crate::store::PendingHash {
            hash: hash.clone(),
            req_cnt: 2,
            req_at: 0,
            attempts: 0,
        };
        reader.handle_content(&pending, &hash, info_dict_content()).await;
        let doc = store.get_torrent(&hash).await.unwrap().unwrap();
        assert_eq!(doc.name, "test");
        assert_eq!(doc.announce, 2);
        assert!(store.load_torrent_content(&hash).await.unwrap().is_some());
    }
}
