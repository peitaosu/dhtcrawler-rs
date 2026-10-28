use crate::store::{Store, StoreResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const DEFAULT_HASH_MAX_CACHE: usize = 300;
pub const DEFAULT_CACHE_MAX_TIME_SECS: u64 = 120;

pub struct HashCache {
    store: Arc<dyn Store>,
    cache: Mutex<HashMap<String, u64>>,
    max_cache: usize,
    cache_time: Duration,
}

impl HashCache {
    pub fn new(
        store: Arc<dyn Store>,
        max_cache: usize,
        cache_time_secs: u64,
    ) -> Self {
        HashCache {
            store,
            cache: Mutex::new(HashMap::new()),
            max_cache: if max_cache == 0 { DEFAULT_HASH_MAX_CACHE } else { max_cache },
            cache_time: Duration::from_secs(if cache_time_secs == 0 {
                DEFAULT_CACHE_MAX_TIME_SECS
            } else {
                cache_time_secs
            }),
        }
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, HashMap<String, u64>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub async fn insert(&self, hash: String) {
        let mut entries: Option<Vec<(String, u64)>> = None;
        {
            let mut cache = self.lock_cache();
            *cache.entry(hash).or_insert(0) += 1;
            if cache.len() >= self.max_cache {
                entries = Some(cache.drain().collect());
            }
        }
        if let Some(entries) = entries {
            if let Err(e) = self.flush(&entries).await {
                tracing::warn!("hash cache flush failed: {}", e);
                self.restore(&entries);
            }
        }
    }

    fn restore(&self, entries: &[(String, u64)]) {
        let mut cache = self.lock_cache();
        for (hash, req_cnt) in entries {
            *cache.entry(hash.clone()).or_insert(0) += req_cnt;
        }
    }

    pub fn len(&self) -> usize {
        self.lock_cache().len()
    }

    pub async fn flush_timer(&self) {
        let mut interval = tokio::time::interval(self.cache_time);
        loop {
            interval.tick().await;
            let entries: Vec<(String, u64)> = self.lock_cache().drain().collect();
            if entries.is_empty() {
                continue;
            }
            if let Err(e) = self.flush(&entries).await {
                tracing::warn!("hash cache flush failed: {}", e);
                self.restore(&entries);
            }
        }
    }

    pub async fn flush_pending(&self) {
        let entries: Vec<(String, u64)> = self.lock_cache().drain().collect();
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.flush(&entries).await {
            tracing::warn!("hash cache flush failed: {}", e);
            self.restore(&entries);
        }
    }

    pub async fn flush(&self, entries: &[(String, u64)]) -> StoreResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let (inserted, req_sum) = self.store.insert_pending_hashes(entries).await?;
        self.store.stats_cache_query_inserted(inserted).await?;
        self.store.stats_query_inserted(req_sum).await?;
        tracing::debug!("hash cache flushed {} hashes ({} requests)", inserted, req_sum);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryStore, Store};

    fn cache(max: usize, time: u64) -> HashCache {
        HashCache::new(Arc::new(MemoryStore::new()), max, time)
    }

    #[tokio::test]
    async fn merges_duplicates_until_max() {
        let cache = cache(3, 120);
        cache.insert("A".repeat(40)).await;
        cache.insert("A".repeat(40)).await;
        cache.insert("B".repeat(40)).await;
        assert_eq!(cache.len(), 2);
        cache.insert("C".repeat(40)).await;
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn flush_writes_pending_hashes() {
        let store = Arc::new(MemoryStore::new());
        let cache = HashCache::new(store.clone(), 300, 120);
        cache.insert("D".repeat(40)).await;
        cache.insert("D".repeat(40)).await;
        let entries: Vec<(String, u64)> = cache
            .cache
            .lock()
            .unwrap()
            .drain()
            .collect();
        cache.flush(&entries).await.unwrap();
        let popped = store.pop_pending_hash().await.unwrap().unwrap();
        assert_eq!(popped.hash, "D".repeat(40));
        assert_eq!(popped.req_cnt, 2);
        let today = crate::common::time_util::now_day_seconds();
        let stats = store.stats_day(today).await.unwrap().unwrap();
        assert_eq!(stats.unique, 1);
        assert_eq!(stats.recv, 2);
    }

    #[tokio::test]
    async fn timer_task_flushes_after_interval() {
        let store = Arc::new(MemoryStore::new());
        let cache = Arc::new(HashCache::new(store.clone(), 300, 1));
        cache.insert("E".repeat(40)).await;
        let timer_cache = cache.clone();
        tokio::spawn(async move {
            timer_cache.flush_timer().await;
        });
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let popped = store.pop_pending_hash().await.unwrap();
        assert_eq!(popped.map(|p| p.hash), Some("E".repeat(40)));
    }
}
