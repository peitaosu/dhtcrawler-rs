pub mod memory;
pub mod mongo;

pub use memory::MemoryStore;

use async_trait::async_trait;
use crate::common::time_util;
use crate::metadata::FileEntry;

pub const DAY_SECONDS: i64 = 24 * 60 * 60;
pub const DATERANGE_RETENTION_DAYS: i64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingHash {
    pub hash: String,
    pub req_cnt: u64,
    pub req_at: i64,
    pub attempts: u32,
}

pub const MAX_REQUEUE_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentDoc {
    pub hash: String,
    pub name: String,
    pub length: u64,
    pub created_at: i64,
    pub announce: u64,
    pub files: Vec<FileEntry>,
    pub name_array: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DayStats {
    pub day: i64,
    pub recv: u64,
    pub process: u64,
    pub update: u64,
    pub new: u64,
    pub unique: u64,
}

impl DayStats {
    pub fn for_day(day: i64) -> Self {
        DayStats {
            day,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchStats {
    pub found: u64,
    pub cost_micros: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store error: {0}")]
    Error(String),
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> Self {
        StoreError::Error(message.into())
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

#[async_trait]
pub trait Store: Send + Sync {
    async fn insert_pending_hashes(&self, hashes: &[(String, u64)]) -> StoreResult<(u64, u64)>;

    async fn pop_pending_hash(&self) -> StoreResult<Option<PendingHash>>;

    async fn save_wait_download(&self, pending: PendingHash) -> StoreResult<()>;

    async fn pop_wait_download(&self) -> StoreResult<Option<PendingHash>>;

    async fn insert_torrent(&self, doc: TorrentDoc) -> StoreResult<()>;

    async fn inc_announce(&self, hash: &str, inc: u64) -> StoreResult<bool>;

    async fn get_torrent(&self, hash: &str) -> StoreResult<Option<TorrentDoc>>;

    async fn get_torrents(&self, hashes: &[String]) -> StoreResult<Vec<TorrentDoc>>;

    async fn torrents_page(&self, offset: u64, limit: u64) -> StoreResult<Vec<TorrentDoc>>;

    async fn count_torrents(&self) -> StoreResult<u64>;

    async fn search_torrents(
        &self,
        keyword: &str,
        offset: u64,
        limit: u64,
    ) -> StoreResult<(Vec<TorrentDoc>, SearchStats)>;

    async fn top_by_announce(&self, count: u64) -> StoreResult<Vec<TorrentDoc>>;

    async fn recent_torrents(&self, count: u64) -> StoreResult<Vec<TorrentDoc>>;

    async fn all_torrents(&self) -> StoreResult<Vec<TorrentDoc>>;

    async fn daterange_insert(&self, hash: &str, req_cnt: u64, upsert: bool) -> StoreResult<()>;

    async fn daterange_lookup(&self, day: i64, count: u64) -> StoreResult<Vec<(String, u64)>>;

    async fn daterange_cleanup(&self) -> StoreResult<()>;

    async fn stats_new_saved(&self) -> StoreResult<()>;

    async fn stats_updated(&self) -> StoreResult<()>;

    async fn stats_get_peers(&self) -> StoreResult<()>;

    async fn stats_query_inserted(&self, count: u64) -> StoreResult<()>;

    async fn stats_cache_query_inserted(&self, count: u64) -> StoreResult<()>;

    async fn stats_day(&self, day: i64) -> StoreResult<Option<DayStats>>;

    async fn save_torrent_content(&self, hash: &str, content: &[u8]) -> StoreResult<()>;

    async fn load_torrent_content(&self, hash: &str) -> StoreResult<Option<Vec<u8>>>;
}

pub fn now_day() -> i64 {
    time_util::now_day_seconds()
}
