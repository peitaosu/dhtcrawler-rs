use crate::store::{PendingHash, Store};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub const DEFAULT_MAX_DOWNLOAD_CACHE: usize = 100;

pub struct DownloadCache {
    store: Arc<dyn Store>,
    cache: Mutex<VecDeque<PendingHash>>,
    max_cache: usize,
}

impl DownloadCache {
    pub fn new(store: Arc<dyn Store>, max_cache: usize) -> Self {
        DownloadCache {
            store,
            cache: Mutex::new(VecDeque::new()),
            max_cache: if max_cache == 0 { DEFAULT_MAX_DOWNLOAD_CACHE } else { max_cache },
        }
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, VecDeque<PendingHash>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn insert(&self, pending: PendingHash) {
        let mut spilled: Option<Vec<PendingHash>> = None;
        {
            let mut cache = self.lock_cache();
            cache.push_back(pending);
            if cache.len() >= self.max_cache {
                let spill_count = self.spill_count();
                let drained: Vec<PendingHash> = (0..spill_count)
                    .filter_map(|_| cache.pop_front())
                    .collect();
                spilled = Some(drained);
            }
        }
        if let Some(spilled) = spilled {
            self.spill_to_store(spilled);
        }
    }

    fn spill_count(&self) -> usize {
        (self.max_cache - 2 * self.max_cache / 3).max(1)
    }

    fn spill_to_store(&self, spilled: Vec<PendingHash>) {
        if spilled.is_empty() {
            return;
        }
        tracing::debug!("download cache full, spilling {} docs to wait_download", spilled.len());
        let store = self.store.clone();
        tokio::task::spawn(async move {
            for doc in spilled {
                if let Err(e) = store.save_wait_download(doc).await {
                    tracing::warn!("spill to wait_download failed: {}", e);
                }
            }
        });
    }

    pub async fn get_one(&self) -> Option<PendingHash> {
        let cached = self.lock_cache().pop_front();
        if cached.is_some() {
            return cached;
        }
        match self.store.pop_wait_download().await {
            Ok(doc) => doc,
            Err(e) => {
                tracing::warn!("pop wait_download failed: {}", e);
                None
            }
        }
    }

    pub async fn flush_pending(&self) {
        let drained: Vec<PendingHash> = self.lock_cache().drain(..).collect();
        for pending in drained {
            if let Err(e) = self.store.save_wait_download(pending).await {
                tracing::warn!("flush to wait_download failed: {}", e);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.lock_cache().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn hash(letter: u8) -> String {
        (letter as char).to_string().repeat(40)
    }

    fn pending(letter: u8) -> PendingHash {
        PendingHash {
            hash: hash(letter),
            req_cnt: 1,
            req_at: 0,
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn fifo_order_and_refill_from_store() {
        let store = Arc::new(MemoryStore::new());
        let cache = DownloadCache::new(store.clone(), 100);
        cache.insert(pending(b'A'));
        cache.insert(pending(b'B'));
        assert_eq!(cache.get_one().await.unwrap().hash, hash(b'A'));
        assert_eq!(cache.get_one().await.unwrap().hash, hash(b'B'));
        assert!(cache.get_one().await.is_none());
        store.save_wait_download(pending(b'C')).await.unwrap();
        assert_eq!(cache.get_one().await.unwrap().hash, hash(b'C'));
    }

    #[tokio::test]
    async fn spills_oldest_third_when_full() {
        let store = Arc::new(MemoryStore::new());
        let cache = DownloadCache::new(store.clone(), 3);
        cache.insert(pending(b'A'));
        cache.insert(pending(b'B'));
        cache.insert(pending(b'C'));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(store.pop_wait_download().await.unwrap().map(|p| p.hash), Some(hash(b'A')));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_one().await.unwrap().hash, hash(b'B'));
        assert_eq!(cache.get_one().await.unwrap().hash, hash(b'C'));
    }
}
