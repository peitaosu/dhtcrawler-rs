use crate::store::{
    now_day, DayStats, PendingHash, SearchStats, Store, StoreError, StoreResult, TorrentDoc,
    DAY_SECONDS, DATERANGE_RETENTION_DAYS,
};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Default)]
struct MemoryState {
    pending: VecDeque<PendingHash>,
    wait_download: VecDeque<PendingHash>,
    torrents: HashMap<String, TorrentDoc>,
    torrent_order: Vec<String>,
    daterange: HashMap<i64, HashMap<String, u64>>,
    stats: HashMap<i64, DayStats>,
    contents: HashMap<String, Vec<u8>>,
}

#[derive(Default, Clone)]
pub struct MemoryStore {
    state: std::sync::Arc<Mutex<MemoryState>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn update_stats<F>(&self, mutate: F) -> StoreResult<()>
    where
        F: FnOnce(&mut DayStats),
    {
        let mut state = self.lock()?;
        let day = now_day();
        let stats = state.stats.entry(day).or_insert_with(|| DayStats::for_day(day));
        mutate(stats);
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MemoryState>, StoreError> {
        self.state.lock().map_err(|_| StoreError::new("store mutex poisoned"))
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn insert_pending_hashes(&self, hashes: &[(String, u64)]) -> StoreResult<(u64, u64)> {
        let mut state = self.lock()?;
        let now = crate::common::time_util::now_seconds();
        let mut inserted = 0u64;
        let mut req_sum = 0u64;
        for (hash, req_cnt) in hashes {
            if let Some(existing) = state.pending.iter_mut().find(|p| &p.hash == hash) {
                existing.req_cnt += req_cnt;
            } else {
                inserted += 1;
                state.pending.push_back(PendingHash {
                    hash: hash.clone(),
                    req_cnt: *req_cnt,
                    req_at: now,
                    attempts: 0,
                });
            }
            req_sum += req_cnt;
        }
        Ok((inserted, req_sum))
    }

    async fn pop_pending_hash(&self) -> StoreResult<Option<PendingHash>> {
        let mut state = self.lock()?;
        Ok(state.pending.pop_front())
    }

    async fn save_wait_download(&self, pending: PendingHash) -> StoreResult<()> {
        let mut state = self.lock()?;
        state.wait_download.retain(|existing| existing.hash != pending.hash);
        state.wait_download.push_back(pending);
        Ok(())
    }

    async fn pop_wait_download(&self) -> StoreResult<Option<PendingHash>> {
        let mut state = self.lock()?;
        Ok(state.wait_download.pop_front())
    }

    async fn insert_torrent(&self, doc: TorrentDoc) -> StoreResult<()> {
        let mut state = self.lock()?;
        if !state.torrents.contains_key(&doc.hash) {
            state.torrent_order.push(doc.hash.clone());
        }
        state.torrents.insert(doc.hash.clone(), doc);
        Ok(())
    }

    async fn inc_announce(&self, hash: &str, inc: u64) -> StoreResult<bool> {
        let mut state = self.lock()?;
        match state.torrents.get_mut(hash) {
            Some(doc) => {
                doc.announce += inc;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn get_torrent(&self, hash: &str) -> StoreResult<Option<TorrentDoc>> {
        let state = self.lock()?;
        Ok(state.torrents.get(hash).cloned())
    }

    async fn get_torrents(&self, hashes: &[String]) -> StoreResult<Vec<TorrentDoc>> {
        let state = self.lock()?;
        Ok(hashes
            .iter()
            .filter_map(|hash| state.torrents.get(hash).cloned())
            .collect())
    }

    async fn torrents_page(&self, offset: u64, limit: u64) -> StoreResult<Vec<TorrentDoc>> {
        let state = self.lock()?;
        let out = state
            .torrent_order
            .iter()
            .skip(offset as usize)
            .take(limit as usize)
            .filter_map(|hash| state.torrents.get(hash).cloned())
            .collect();
        Ok(out)
    }

    async fn count_torrents(&self) -> StoreResult<u64> {
        let state = self.lock()?;
        Ok(state.torrents.len() as u64)
    }

    async fn search_torrents(
        &self,
        keyword: &str,
        offset: u64,
        limit: u64,
    ) -> StoreResult<(Vec<TorrentDoc>, SearchStats)> {
        let start = Instant::now();
        let state = self.lock()?;
        let keyword_lower = keyword.trim().to_lowercase();
        let mut results = Vec::new();
        if !keyword_lower.is_empty() {
            let mut skipped = 0u64;
            for hash in &state.torrent_order {
                if let Some(doc) = state.torrents.get(hash) {
                    if !matches_keyword(doc, &keyword_lower) {
                        continue;
                    }
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                    results.push(doc.clone());
                    if results.len() as u64 >= limit {
                        break;
                    }
                }
            }
        }
        let found = results.len() as u64;
        Ok((
            results,
            SearchStats {
                found,
                cost_micros: start.elapsed().as_micros() as u64,
            },
        ))
    }

    async fn top_by_announce(&self, count: u64) -> StoreResult<Vec<TorrentDoc>> {
        let state = self.lock()?;
        let mut docs: Vec<TorrentDoc> = state.torrents.values().cloned().collect();
        docs.sort_by(|a, b| b.announce.cmp(&a.announce));
        docs.truncate(count as usize);
        Ok(docs)
    }

    async fn recent_torrents(&self, count: u64) -> StoreResult<Vec<TorrentDoc>> {
        let state = self.lock()?;
        let mut hashes: Vec<&String> = state.torrent_order.iter().collect();
        hashes.reverse();
        let mut out = Vec::new();
        for hash in hashes {
            if let Some(doc) = state.torrents.get(hash) {
                out.push(doc.clone());
                if out.len() as u64 >= count {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn all_torrents(&self) -> StoreResult<Vec<TorrentDoc>> {
        let state = self.lock()?;
        let mut docs: Vec<TorrentDoc> = state
            .torrent_order
            .iter()
            .filter_map(|hash| state.torrents.get(hash).cloned())
            .collect();
        docs.reverse();
        Ok(docs)
    }

    async fn daterange_insert(&self, hash: &str, req_cnt: u64, upsert: bool) -> StoreResult<()> {
        let mut state = self.lock()?;
        let day = now_day();
        let entries = state.daterange.entry(day).or_default();
        match entries.get_mut(hash) {
            Some(existing) => *existing += req_cnt,
            None => {
                if upsert {
                    entries.insert(hash.to_string(), req_cnt);
                }
            }
        }
        Ok(())
    }

    async fn daterange_lookup(&self, day: i64, count: u64) -> StoreResult<Vec<(String, u64)>> {
        let state = self.lock()?;
        let Some(entries) = state.daterange.get(&day) else {
            return Ok(Vec::new());
        };
        let mut out: Vec<(String, u64)> = entries
            .iter()
            .map(|(hash, reqs)| (hash.clone(), *reqs))
            .collect();
        out.sort_by(|a, b| b.1.cmp(&a.1));
        out.truncate(count as usize);
        Ok(out)
    }

    async fn daterange_cleanup(&self) -> StoreResult<()> {
        let mut state = self.lock()?;
        let cutoff = now_day() - DATERANGE_RETENTION_DAYS * DAY_SECONDS;
        state.daterange.retain(|day, _| *day >= cutoff);
        Ok(())
    }

    async fn stats_new_saved(&self) -> StoreResult<()> {
        self.update_stats(|s| s.new += 1)
    }

    async fn stats_updated(&self) -> StoreResult<()> {
        self.update_stats(|s| s.update += 1)
    }

    async fn stats_get_peers(&self) -> StoreResult<()> {
        self.update_stats(|s| s.process += 1)
    }

    async fn stats_query_inserted(&self, count: u64) -> StoreResult<()> {
        self.update_stats(|s| s.recv += count)
    }

    async fn stats_cache_query_inserted(&self, count: u64) -> StoreResult<()> {
        self.update_stats(|s| s.unique += count)
    }

    async fn stats_day(&self, day: i64) -> StoreResult<Option<DayStats>> {
        let state = self.lock()?;
        Ok(state.stats.get(&day).copied())
    }

    async fn save_torrent_content(&self, hash: &str, content: &[u8]) -> StoreResult<()> {
        let mut state = self.lock()?;
        state.contents.insert(hash.to_string(), content.to_vec());
        Ok(())
    }

    async fn load_torrent_content(&self, hash: &str) -> StoreResult<Option<Vec<u8>>> {
        let state = self.lock()?;
        Ok(state.contents.get(hash).cloned())
    }
}

fn matches_keyword(doc: &TorrentDoc, keyword_lower: &str) -> bool {
    let tokens: Vec<&str> = keyword_lower.split_whitespace().collect();
    if tokens.is_empty() {
        return false;
    }
    let mut words: std::collections::HashSet<String> = doc
        .name
        .to_lowercase()
        .split_whitespace()
        .map(|word| word.to_string())
        .collect();
    words.extend(doc.name_array.iter().map(|entry| entry.to_lowercase()));
    tokens.iter().all(|token| words.contains(&token.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FileEntry;

    fn doc(hash: &str, name: &str, announce: u64) -> TorrentDoc {
        TorrentDoc {
            hash: hash.to_string(),
            name: name.to_string(),
            length: 100,
            created_at: 1000,
            announce,
            files: vec![FileEntry {
                path: name.to_string(),
                length: 100,
            }],
            name_array: vec![name.to_string()],
        }
    }

    #[tokio::test]
    async fn pending_hash_queue_merge_and_pop() {
        let store = MemoryStore::new();
        let (inserted, req_sum) = store
            .insert_pending_hashes(&[("A".repeat(40), 2), ("B".repeat(40), 1)])
            .await
            .unwrap();
        assert_eq!((inserted, req_sum), (2, 3));
        let (inserted, req_sum) = store
            .insert_pending_hashes(&[("A".repeat(40), 5)])
            .await
            .unwrap();
        assert_eq!((inserted, req_sum), (0, 5));
        let popped = store.pop_pending_hash().await.unwrap().unwrap();
        assert_eq!(popped.hash, "A".repeat(40));
        assert_eq!(popped.req_cnt, 7);
        let popped = store.pop_pending_hash().await.unwrap().unwrap();
        assert_eq!(popped.hash, "B".repeat(40));
        assert!(store.pop_pending_hash().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn wait_download_roundtrip() {
        let store = MemoryStore::new();
        store
            .save_wait_download(PendingHash {
                hash: "C".repeat(40),
                req_cnt: 3,
                req_at: 42,
                attempts: 1,
            })
            .await
            .unwrap();
        let popped = store.pop_wait_download().await.unwrap().unwrap();
        assert_eq!(popped.hash, "C".repeat(40));
        assert_eq!(popped.attempts, 1);
        assert!(store.pop_wait_download().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn wait_download_is_fifo_and_upserts_by_hash() {
        let store = MemoryStore::new();
        store.save_wait_download(PendingHash { hash: "A".repeat(40), req_cnt: 1, req_at: 1, attempts: 0 }).await.unwrap();
        store.save_wait_download(PendingHash { hash: "B".repeat(40), req_cnt: 1, req_at: 2, attempts: 0 }).await.unwrap();
        store.save_wait_download(PendingHash { hash: "A".repeat(40), req_cnt: 9, req_at: 3, attempts: 2 }).await.unwrap();
        let popped = store.pop_wait_download().await.unwrap().unwrap();
        assert_eq!(popped.hash, "B".repeat(40));
        let popped = store.pop_wait_download().await.unwrap().unwrap();
        assert_eq!(popped.hash, "A".repeat(40));
        assert_eq!(popped.req_cnt, 9);
        assert_eq!(popped.attempts, 2);
        assert!(store.pop_wait_download().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn torrents_page_slices_insertion_order() {
        let store = MemoryStore::new();
        for letter in b'P'..=b'S' {
            let hash = (letter as char).to_string().repeat(40);
            store.insert_torrent(doc(hash.as_str(), "x", 1)).await.unwrap();
        }
        let page = store.torrents_page(2, 2).await.unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].hash, "R".repeat(40));
        assert_eq!(page[1].hash, "S".repeat(40));
        assert!(store.torrents_page(4, 2).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn torrent_insert_lookup_announce() {
        let store = MemoryStore::new();
        store.insert_torrent(doc("D".repeat(40).as_str(), "ubuntu", 1)).await.unwrap();
        assert_eq!(store.count_torrents().await.unwrap(), 1);
        assert!(!store.inc_announce("E".repeat(40).as_str(), 1).await.unwrap());
        assert!(store.inc_announce("D".repeat(40).as_str(), 4).await.unwrap());
        let torrent = store.get_torrent("D".repeat(40).as_str()).await.unwrap().unwrap();
        assert_eq!(torrent.announce, 5);
    }

    #[tokio::test]
    async fn search_finds_by_name_tokens() {
        let store = MemoryStore::new();
        store.insert_torrent(doc("F".repeat(40).as_str(), "Ubuntu 22.04 LTS", 1)).await.unwrap();
        store.insert_torrent(doc("G".repeat(40).as_str(), "Debian ISO", 1)).await.unwrap();
        let (results, stats) = store.search_torrents("ubuntu", 0, 50).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Ubuntu 22.04 LTS");
        assert_eq!(stats.found, 1);
        let (results, _) = store.search_torrents("nothing", 0, 50).await.unwrap();
        assert!(results.is_empty());
        let (results, _) = store.search_torrents("", 0, 50).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn search_offset_pages() {
        let store = MemoryStore::new();
        store.insert_torrent(doc("M".repeat(40).as_str(), "ubuntu one", 1)).await.unwrap();
        store.insert_torrent(doc("N".repeat(40).as_str(), "ubuntu two", 1)).await.unwrap();
        store.insert_torrent(doc("O".repeat(40).as_str(), "ubuntu three", 1)).await.unwrap();
        let (results, _) = store.search_torrents("ubuntu", 0, 2).await.unwrap();
        assert_eq!(results.len(), 2);
        let (results, _) = store.search_torrents("ubuntu", 2, 2).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "ubuntu three");
        let (results, _) = store.search_torrents("ubuntu", 4, 2).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn tops_and_recent() {
        let store = MemoryStore::new();
        store.insert_torrent(doc("H".repeat(40).as_str(), "a", 3)).await.unwrap();
        store.insert_torrent(doc("I".repeat(40).as_str(), "b", 9)).await.unwrap();
        let top = store.top_by_announce(1).await.unwrap();
        assert_eq!(top[0].name, "b");
        let recent = store.recent_torrents(1).await.unwrap();
        assert_eq!(recent[0].name, "b");
        let all = store.all_torrents().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn daterange_lookup_and_cleanup() {
        let store = MemoryStore::new();
        store.daterange_insert("J".repeat(40).as_str(), 5, true).await.unwrap();
        store.daterange_insert("J".repeat(40).as_str(), 2, true).await.unwrap();
        let today = now_day();
        let entries = store.daterange_lookup(today, 10).await.unwrap();
        assert_eq!(entries, vec![("J".repeat(40), 7)]);
        let mut state = store.lock().unwrap();
        state
            .daterange
            .insert(today - DATERANGE_RETENTION_DAYS * DAY_SECONDS - 1, Default::default());
        drop(state);
        store.daterange_cleanup().await.unwrap();
        let state = store.lock().unwrap();
        assert!(!state.daterange.contains_key(&(today - DATERANGE_RETENTION_DAYS * DAY_SECONDS - 1)));
    }

    #[tokio::test]
    async fn stats_counters() {
        let store = MemoryStore::new();
        store.stats_new_saved().await.unwrap();
        store.stats_updated().await.unwrap();
        store.stats_query_inserted(10).await.unwrap();
        store.stats_cache_query_inserted(4).await.unwrap();
        store.stats_get_peers().await.unwrap();
        let today = now_day();
        let stats = store.stats_day(today).await.unwrap().unwrap();
        assert_eq!(stats.new, 1);
        assert_eq!(stats.update, 1);
        assert_eq!(stats.recv, 10);
        assert_eq!(stats.unique, 4);
        assert_eq!(stats.process, 1);
        assert!(store.stats_day(today - 999 * DAY_SECONDS).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn torrent_content_roundtrip() {
        let store = MemoryStore::new();
        assert!(store.load_torrent_content("K".repeat(40).as_str()).await.unwrap().is_none());
        store.save_torrent_content("K".repeat(40).as_str(), b"data").await.unwrap();
        assert_eq!(
            store.load_torrent_content("K".repeat(40).as_str()).await.unwrap(),
            Some(b"data".to_vec())
        );
    }
}
