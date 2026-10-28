use crate::store::{
    now_day, DayStats, PendingHash, SearchStats, Store, StoreError, StoreResult, TorrentDoc,
    DAY_SECONDS, DATERANGE_RETENTION_DAYS,
};
use async_trait::async_trait;
use mongodb::bson::{doc, Bson, Binary, Document};
use mongodb::{Client, Collection, IndexModel};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::time::Instant;

const DB_HASH: &str = "dht_hash";
const COLL_HASH: &str = "hash";
const COLL_WAIT_DOWNLOAD: &str = "wait_download";
const DB_TORRENTS: &str = "torrents";
const COLL_TORRENTS: &str = "hashes";
const DB_HASH_DATE: &str = "hash_date";
const COLL_HASH_DATE: &str = "hashes";
const DB_SYSTEM: &str = "dht_system";
const COLL_STATS: &str = "stats";
const DB_TORFILES: &str = "torfiles";
const COLL_TORFILES: &str = "torrents";
const HASH_FLUSH_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct MongoStore {
    client: Client,
}

impl MongoStore {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let client = Client::with_options(
            mongodb::options::ClientOptions::parse(url)
                .await
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        Ok(MongoStore { client })
    }

    pub async fn init(&self) -> Result<(), String> {
        let torrents = self.client.database(DB_TORRENTS).collection::<Document>(COLL_TORRENTS);
        let name_index = IndexModel::builder().keys(doc! { "name_array": "text" }).build();
        let date_index = IndexModel::builder().keys(doc! { "created_at": 1 }).build();
        torrents
            .create_index(name_index)
            .await
            .map_err(|e| e.to_string())?;
        torrents
            .create_index(date_index)
            .await
            .map_err(|e| e.to_string())?;
        let hash_date = self.client.database(DB_HASH_DATE).collection::<Document>(COLL_HASH_DATE);
        let day_index = IndexModel::builder().keys(doc! { "date": 1 }).build();
        hash_date
            .create_index(day_index)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn hash_collection(&self) -> Collection<Document> {
        self.client.database(DB_HASH).collection::<Document>(COLL_HASH)
    }

    fn wait_collection(&self) -> Collection<Document> {
        self.client
            .database(DB_HASH)
            .collection::<Document>(COLL_WAIT_DOWNLOAD)
    }

    fn torrent_collection(&self) -> Collection<Document> {
        self.client
            .database(DB_TORRENTS)
            .collection::<Document>(COLL_TORRENTS)
    }

    fn date_collection(&self) -> Collection<Document> {
        self.client
            .database(DB_HASH_DATE)
            .collection::<Document>(COLL_HASH_DATE)
    }

    fn stats_collection(&self) -> Collection<Document> {
        self.client.database(DB_SYSTEM).collection::<Document>(COLL_STATS)
    }

    fn torfiles_collection(&self) -> Collection<Document> {
        self.client
            .database(DB_TORFILES)
            .collection::<Document>(COLL_TORFILES)
    }

    async fn inc_stat(&self, field: &str, count: u64) -> StoreResult<()> {
        let day = now_day();
        self.stats_collection()
            .update_one(doc! { "_id": day }, doc! { "$inc": { field: count as i64 } })
            .upsert(true)
            .await
            .map_err(map_error)?;
        Ok(())
    }
}

fn map_error(e: mongodb::error::Error) -> StoreError {
    StoreError::new(e.to_string())
}

fn hash_filter(hash: &str) -> Document {
    doc! { "_id": hash }
}

fn decode_pending(document: &Document) -> Option<PendingHash> {
    Some(PendingHash {
        hash: document.get_str("_id").ok()?.to_string(),
        req_cnt: document.get_i64("req_cnt").unwrap_or(0).max(0) as u64,
        req_at: document.get_i64("req_at").unwrap_or(0),
        attempts: document.get_i64("attempts").unwrap_or(0).max(0) as u32,
    })
}

fn encode_pending(pending: &PendingHash) -> Document {
    doc! {
        "_id": pending.hash.clone(),
        "req_cnt": pending.req_cnt as i64,
        "req_at": pending.req_at,
        "attempts": pending.attempts as i64,
    }
}

pub fn encode_torrent_doc(doc: &TorrentDoc) -> Document {
    let files: Vec<Document> = doc
        .files
        .iter()
        .map(|f| doc! { "name": f.path.clone(), "length": f.length as i64 })
        .collect();
    let mut out = doc! {
        "_id": doc.hash.clone(),
        "name": doc.name.clone(),
        "length": doc.length as i64,
        "created_at": doc.created_at,
        "announce": doc.announce as i64,
        "name_array": doc.name_array.clone(),
    };
    if !files.is_empty() {
        out.insert("files", mongodb::bson::Bson::Array(
            files.into_iter().map(mongodb::bson::Bson::Document).collect(),
        ));
    }
    out
}

fn decode_torrent_doc(document: &Document) -> Option<TorrentDoc> {
    let files = match document.get("files") {
        Some(mongodb::bson::Bson::Array(entries)) => entries
            .iter()
            .filter_map(|entry| {
                let doc = entry.as_document()?;
                Some(crate::metadata::FileEntry {
                    path: doc.get_str("name").ok()?.to_string(),
                    length: doc.get_i64("length").ok()?.max(0) as u64,
                })
            })
            .collect(),
        _ => Vec::new(),
    };
    Some(TorrentDoc {
        hash: document.get_str("_id").ok()?.to_string(),
        name: document.get_str("name").ok()?.to_string(),
        length: document.get_i64("length").ok()?.max(0) as u64,
        created_at: document.get_i64("created_at").unwrap_or(0),
        announce: document
            .get("announce")
            .and_then(|v| v.as_f64().map(|f| f as u64).or_else(|| v.as_i64().map(|i| i as u64)))
            .unwrap_or(0),
        files,
        name_array: document
            .get_array("name_array")
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

#[async_trait]
impl Store for MongoStore {
    async fn insert_pending_hashes(&self, hashes: &[(String, u64)]) -> StoreResult<(u64, u64)> {
        let now = crate::common::time_util::now_seconds();
        let collection = self.hash_collection();
        let results: Vec<(bool, u64)> = futures::stream::iter(hashes.iter().cloned())
            .map(|(hash, req_cnt)| {
                let collection = collection.clone();
                async move {
                    let result = collection
                        .update_one(
                            hash_filter(&hash),
                            doc! {
                                "$inc": { "req_cnt": req_cnt as i64 },
                                "$set": { "req_at": now },
                                "$setOnInsert": { "_id": hash.clone() },
                            },
                        )
                        .upsert(true)
                        .await
                        .map_err(map_error)?;
                    Ok::<(bool, u64), StoreError>((result.matched_count == 0, req_cnt))
                }
            })
            .buffer_unordered(HASH_FLUSH_CONCURRENCY)
            .try_collect()
            .await?;
        let inserted = results.iter().filter(|(is_new, _)| *is_new).count() as u64;
        let req_sum = results.iter().map(|(_, req_cnt)| *req_cnt).sum();
        Ok((inserted, req_sum))
    }

    async fn pop_pending_hash(&self) -> StoreResult<Option<PendingHash>> {
        let document = self
            .hash_collection()
            .find_one_and_delete(doc! {})
            .sort(doc! { "req_at": 1 })
            .await
            .map_err(map_error)?;
        Ok(document.as_ref().and_then(decode_pending))
    }

    async fn save_wait_download(&self, pending: PendingHash) -> StoreResult<()> {
        self.wait_collection()
            .update_one(hash_filter(&pending.hash), doc! { "$set": encode_pending(&pending) })
            .upsert(true)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn pop_wait_download(&self) -> StoreResult<Option<PendingHash>> {
        let document = self
            .wait_collection()
            .find_one_and_delete(doc! {})
            .await
            .map_err(map_error)?;
        Ok(document.as_ref().and_then(decode_pending))
    }

    async fn insert_torrent(&self, doc: TorrentDoc) -> StoreResult<()> {
        self.torrent_collection()
            .update_one(hash_filter(&doc.hash), doc! { "$set": encode_torrent_doc(&doc) })
            .upsert(true)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn inc_announce(&self, hash: &str, inc: u64) -> StoreResult<bool> {
        let result = self
            .torrent_collection()
            .update_one(hash_filter(hash), doc! { "$inc": { "announce": inc as i64 } })
            .await
            .map_err(map_error)?;
        Ok(result.matched_count > 0)
    }

    async fn get_torrent(&self, hash: &str) -> StoreResult<Option<TorrentDoc>> {
        let document = self
            .torrent_collection()
            .find_one(hash_filter(hash))
            .await
            .map_err(map_error)?;
        Ok(document.as_ref().and_then(decode_torrent_doc))
    }

    async fn get_torrents(&self, hashes: &[String]) -> StoreResult<Vec<TorrentDoc>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let cursor = self
            .torrent_collection()
            .find(doc! { "_id": { "$in": hashes } })
            .await
            .map_err(map_error)?;
        let mut by_hash: HashMap<String, TorrentDoc> = HashMap::new();
        let mut cursor = cursor;
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                by_hash.insert(doc.hash.clone(), doc);
            }
        }
        Ok(hashes
            .iter()
            .filter_map(|hash| by_hash.get(hash).cloned())
            .collect())
    }

    async fn torrents_page(&self, offset: u64, limit: u64) -> StoreResult<Vec<TorrentDoc>> {
        let mut cursor = self
            .torrent_collection()
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .skip(offset)
            .limit(limit as i64)
            .await
            .map_err(map_error)?;
        let mut results = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    async fn count_torrents(&self) -> StoreResult<u64> {
        self.torrent_collection()
            .count_documents(doc! {})
            .await
            .map_err(map_error)
    }

    async fn search_torrents(
        &self,
        keyword: &str,
        offset: u64,
        limit: u64,
    ) -> StoreResult<(Vec<TorrentDoc>, SearchStats)> {
        let start = Instant::now();
        let filter = doc! { "$text": { "$search": keyword } };
        let mut cursor = self
            .torrent_collection()
            .find(filter)
            .skip(offset)
            .limit(limit as i64)
            .await
            .map_err(map_error)?;
        let mut results = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                results.push(doc);
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
        let mut cursor = self
            .torrent_collection()
            .find(doc! {})
            .sort(doc! { "announce": -1 })
            .limit(count as i64)
            .await
            .map_err(map_error)?;
        let mut results = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    async fn recent_torrents(&self, count: u64) -> StoreResult<Vec<TorrentDoc>> {
        let mut cursor = self
            .torrent_collection()
            .find(doc! {})
            .sort(doc! { "created_at": -1 })
            .limit(count as i64)
            .await
            .map_err(map_error)?;
        let mut results = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    async fn all_torrents(&self) -> StoreResult<Vec<TorrentDoc>> {
        let mut cursor = self
            .torrent_collection()
            .find(doc! {})
            .sort(doc! { "created_at": -1 })
            .await
            .map_err(map_error)?;
        let mut results = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            if let Some(doc) = decode_torrent_doc(&document) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    async fn daterange_insert(&self, hash: &str, req_cnt: u64, upsert: bool) -> StoreResult<()> {
        let day = now_day();
        self.date_collection()
            .update_one(
                doc! { "_id": { "hash": hash, "date": day } },
                doc! {
                    "$inc": { "reqs": req_cnt as i64 },
                    "$setOnInsert": { "date": day },
                },
            )
            .upsert(upsert)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn daterange_lookup(&self, day: i64, count: u64) -> StoreResult<Vec<(String, u64)>> {
        let mut cursor = self
            .date_collection()
            .find(doc! { "date": day })
            .sort(doc! { "reqs": -1 })
            .limit(count as i64)
            .projection(doc! { "_id": 1, "reqs": 1 })
            .await
            .map_err(map_error)?;
        let mut out = Vec::new();
        while let Some(item) = cursor.next().await {
            let document = item.map_err(map_error)?;
            let hash = document.get_str("_id").unwrap_or_default().to_string();
            let reqs = document.get_i64("reqs").unwrap_or(0).max(0) as u64;
            out.push((hash, reqs));
        }
        Ok(out)
    }

    async fn daterange_cleanup(&self) -> StoreResult<()> {
        let cutoff = now_day() - DATERANGE_RETENTION_DAYS * DAY_SECONDS;
        self.date_collection()
            .delete_many(doc! { "date": { "$lte": cutoff } })
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn stats_new_saved(&self) -> StoreResult<()> {
        self.inc_stat("new_saved", 1).await
    }

    async fn stats_updated(&self) -> StoreResult<()> {
        self.inc_stat("updated", 1).await
    }

    async fn stats_get_peers(&self) -> StoreResult<()> {
        self.inc_stat("get_peers", 1).await
    }

    async fn stats_query_inserted(&self, count: u64) -> StoreResult<()> {
        self.inc_stat("get_peers_query", count).await
    }

    async fn stats_cache_query_inserted(&self, count: u64) -> StoreResult<()> {
        self.inc_stat("inserted_query", count).await
    }

    async fn stats_day(&self, day: i64) -> StoreResult<Option<DayStats>> {
        let document = self
            .stats_collection()
            .find_one(doc! { "_id": day })
            .await
            .map_err(map_error)?;
        Ok(document.map(|document| DayStats {
            day,
            recv: document.get_i64("get_peers_query").unwrap_or(0).max(0) as u64,
            process: document.get_i64("get_peers").unwrap_or(0).max(0) as u64,
            update: document.get_i64("updated").unwrap_or(0).max(0) as u64,
            new: document.get_i64("new_saved").unwrap_or(0).max(0) as u64,
            unique: document.get_i64("inserted_query").unwrap_or(0).max(0) as u64,
        }))
    }

    async fn save_torrent_content(&self, hash: &str, content: &[u8]) -> StoreResult<()> {
        let binary = Bson::Binary(Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: content.to_vec(),
        });
        self.torfiles_collection()
            .update_one(
                hash_filter(hash),
                doc! { "$set": { "_id": hash, "content": binary } },
            )
            .upsert(true)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn load_torrent_content(&self, hash: &str) -> StoreResult<Option<Vec<u8>>> {
        let document = self
            .torfiles_collection()
            .find_one(hash_filter(hash))
            .await
            .map_err(map_error)?;
        Ok(document.and_then(|d| {
            match d.get("content") {
                Some(Bson::Binary(binary)) => Some(binary.bytes.clone()),
                _ => None,
            }
        }))
    }
}

