use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const OUT_OF_DATE: Duration = Duration::from_secs(10 * 60);
pub const CACHE_SIZE: usize = 1000;

pub struct CacheEntry<T> {
    stored_at: Instant,
    value: T,
}

pub struct ResultCache<T> {
    entries: Mutex<HashMap<String, CacheEntry<T>>>,
}

impl<T: Clone> ResultCache<T> {
    pub fn new() -> Self {
        ResultCache {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn lock_entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, CacheEntry<T>>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn get_fresh(&self, key: &str) -> Option<T> {
        let entries = self.lock_entries();
        entries
            .get(key)
            .filter(|e| e.stored_at.elapsed() < OUT_OF_DATE)
            .map(|e| e.value.clone())
    }

    pub fn get_stale(&self, key: &str) -> Option<(T, bool)> {
        let entries = self.lock_entries();
        entries
            .get(key)
            .map(|e| (e.value.clone(), e.stored_at.elapsed() >= OUT_OF_DATE))
    }

    pub fn insert(&self, key: &str, value: T) {
        let mut entries = self.lock_entries();
        if entries.len() >= CACHE_SIZE {
            let mut sorted: Vec<(String, Instant)> = entries
                .iter()
                .map(|(k, e)| (k.clone(), e.stored_at))
                .collect();
            sorted.sort_by_key(|(_, t)| *t);
            let keep = CACHE_SIZE / 2;
            for (key, _) in sorted.into_iter().skip(keep) {
                entries.remove(&key);
            }
        }
        entries.insert(
            key.to_string(),
            CacheEntry {
                stored_at: Instant::now(),
                value,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.lock_entries().len()
    }
}

impl<T: Clone> Default for ResultCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_entry_is_returned() {
        let cache: ResultCache<u32> = ResultCache::new();
        cache.insert("a", 1);
        assert_eq!(cache.get_fresh("a"), Some(1));
        assert_eq!(cache.get_fresh("b"), None);
    }

    #[test]
    fn stale_entry_reported_via_get_stale() {
        let cache: ResultCache<u32> = ResultCache::new();
        let Ok(mut entries) = cache.entries.lock() else {
            unreachable!();
        };
        entries.insert(
            "old".into(),
            CacheEntry {
                stored_at: Instant::now() - OUT_OF_DATE - Duration::from_secs(1),
                value: 7,
            },
        );
        drop(entries);
        assert_eq!(cache.get_fresh("old"), None);
        assert_eq!(cache.get_stale("old"), Some((7, true)));
        assert_eq!(cache.get_stale("missing"), None);
    }

    #[test]
    fn evicts_oldest_half_when_full() {
        let cache: ResultCache<u32> = ResultCache::new();
        for i in 0..1000 {
            cache.insert(&format!("k{}", i), i as u32);
        }
        assert_eq!(cache.len(), 1000);
        cache.insert("overflow", 1);
        assert!(cache.len() <= 501);
        assert_eq!(cache.get_fresh("overflow"), Some(1));
    }
}
