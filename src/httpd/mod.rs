pub mod api;
pub mod cache;

use crate::httpd::cache::ResultCache;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use crate::common::time_util;
use crate::store::{DayStats, Store, TorrentDoc, DAY_SECONDS};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

pub const SEARCH_LIMIT: u64 = 50;
pub const TODAY_TOP_COUNT: u64 = 50;
pub const TOP_LIST_COUNT: u64 = 50;
pub const TODAY_FALLBACK_DAYS: i64 = 2;

pub struct AppState {
    store: Arc<dyn Store>,
    search_cache: ResultCache<(Vec<TorrentDoc>, u64)>,
    top_cache: ResultCache<Vec<TorrentDoc>>,
    stats_cache: ResultCache<(u64, Vec<DayStats>)>,
}

impl AppState {
    pub fn new(store: Arc<dyn Store>) -> Self {
        AppState {
            store,
            search_cache: ResultCache::new(),
            top_cache: ResultCache::new(),
            stats_cache: ResultCache::new(),
        }
    }
}

pub async fn run(store: Arc<dyn Store>, bind: &str, port: u16) -> Result<(), String> {
    let state = Arc::new(AppState::new(store));
    spawn_background_refresh(state.clone());
    let app = router().with_state(state);
    let addr = format!("{}:{}", bind, port);
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("bind {}: {}", addr, e))?;
    tracing::info!("httpd listening on {}", addr);
    axum::serve(listener, app)
        .await
        .map_err(|e| e.to_string())
}

fn spawn_background_refresh(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(cache::OUT_OF_DATE);
        loop {
            timer.tick().await;
            refresh_top_and_stats(&state).await;
        }
    });
}

async fn refresh_top_and_stats(state: &Arc<AppState>) {
    match state.top_cache.get_stale("top") {
        Some((docs, stale)) => {
            if stale {
                if let Ok(fresh) = compute_today_top(&state.store, TODAY_TOP_COUNT).await {
                    state.top_cache.insert("top", fresh);
                } else {
                    state.top_cache.insert("top", docs);
                }
            }
        }
        None => {
            if let Ok(fresh) = compute_today_top(&state.store, TODAY_TOP_COUNT).await {
                state.top_cache.insert("top", fresh);
            }
        }
    }
    match state.stats_cache.get_stale("stats") {
        Some((value, stale)) => {
            if stale {
                if let Ok(fresh) = compute_stats(&state.store).await {
                    state.stats_cache.insert("stats", fresh);
                } else {
                    state.stats_cache.insert("stats", value);
                }
            }
        }
        None => {
            if let Ok(fresh) = compute_stats(&state.store).await {
                state.stats_cache.insert("stats", fresh);
            }
        }
    }
}

async fn compute_today_top(store: &Arc<dyn Store>, limit: u64) -> Result<Vec<TorrentDoc>, String> {
    let today = time_util::now_day_seconds();
    for fallback in 0..=TODAY_FALLBACK_DAYS {
        let day = today - fallback * DAY_SECONDS;
        let entries = store
            .daterange_lookup(day, limit)
            .await
            .map_err(|e| e.to_string())?;
        if entries.is_empty() {
            continue;
        }
        let hashes: Vec<String> = entries.iter().map(|(hash, _)| hash.clone()).collect();
        let docs = store
            .get_torrents(&hashes)
            .await
            .map_err(|e| e.to_string())?;
        let by_hash: HashMap<&str, &TorrentDoc> = docs
            .iter()
            .map(|doc| (doc.hash.as_str(), doc))
            .collect();
        let mut top = Vec::new();
        for (hash, reqs) in entries {
            if let Some(doc) = by_hash.get(hash.as_str()) {
                let mut doc = (*doc).clone();
                doc.announce = reqs;
                top.push(doc);
            }
        }
        return Ok(top);
    }
    Ok(Vec::new())
}

async fn compute_stats(store: &Arc<dyn Store>) -> Result<(u64, Vec<DayStats>), String> {
    let total = store.count_torrents().await.map_err(|e| e.to_string())?;
    let today = time_util::now_day_seconds();
    let mut days = Vec::with_capacity(3);
    for offset in 0..3 {
        let day = today - offset * DAY_SECONDS;
        let stats = store
            .stats_day(day)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or_else(|| DayStats::for_day(day));
        days.push(stats);
    }
    Ok((total, days))
}

async fn search_page(
    state: &Arc<AppState>,
    keyword: &str,
    page: u64,
) -> Result<(Vec<TorrentDoc>, u64), String> {
    let cache_key = format!("{}|{}", keyword, page);
    if let Some(cached) = state.search_cache.get_fresh(&cache_key) {
        return Ok(cached);
    }
    let offset = page.saturating_sub(1).saturating_mul(SEARCH_LIMIT);
    let (docs, stats) = state
        .store
        .search_torrents(keyword, offset, SEARCH_LIMIT)
        .await
        .map_err(|e| e.to_string())?;
    let value = (docs, stats.cost_micros);
    state.search_cache.insert(&cache_key, value.clone());
    Ok(value)
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/search", get(api_search))
        .route("/api/today_top", get(api_today_top))
        .route("/api/stats", get(api_stats))
        .route("/api/index", get(api_index))
}

fn query_arg(params: &HashMap<String, String>) -> String {
    params.get("q").cloned().unwrap_or_default().trim().to_string()
}

fn page_arg(params: &HashMap<String, String>) -> u64 {
    params
        .get("p")
        .and_then(|p| p.trim().parse::<u64>().ok())
        .map(|p| p.max(1))
        .unwrap_or(1)
}

fn hash_arg(params: &HashMap<String, String>) -> String {
    query_arg(params).to_uppercase()
}

fn files_arg(params: &HashMap<String, String>) -> Option<usize> {
    params
        .get("files")
        .and_then(|f| f.trim().parse::<usize>().ok())
        .filter(|f| *f > 0)
}

fn limit_arg(params: &HashMap<String, String>) -> Option<u64> {
    params
        .get("limit")
        .and_then(|l| l.trim().parse::<u64>().ok())
        .filter(|l| *l > 0)
}

fn validate_hash(hash: &str) -> bool {
    crate::common::hex::is_hash(hash.as_bytes())
}

async fn api_search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    let keyword = query_arg(&params);
    let page = page_arg(&params);
    if keyword.is_empty() {
        return (
            StatusCode::OK,
            axum::Json(api::error_response(
                "null input, suggest api/search?q=keyword",
            )),
        );
    }
    match search_page(&state, &keyword, page).await {
        Ok((docs, cost_micros)) => (
            StatusCode::OK,
            axum::Json(api::search_response(&keyword, &docs, page, cost_micros, files_arg(&params))),
        ),
        Err(e) => (StatusCode::OK, axum::Json(api::error_response(&e))),
    }
}

async fn api_today_top(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::Json<serde_json::Value> {
    let limit = limit_arg(&params).unwrap_or(TODAY_TOP_COUNT);
    let docs = if limit == TODAY_TOP_COUNT {
        match state.top_cache.get_fresh("top") {
            Some(docs) => docs,
            None => compute_today_top(&state.store, limit).await.unwrap_or_default(),
        }
    } else {
        compute_today_top(&state.store, limit).await.unwrap_or_default()
    };
    axum::Json(api::list_response(&docs))
}

async fn api_stats(State(state): State<Arc<AppState>>) -> axum::Json<serde_json::Value> {
    let (total, days) = match state.stats_cache.get_fresh("stats") {
        Some(cached) => cached,
        None => match compute_stats(&state.store).await {
            Ok(value) => {
                state.stats_cache.insert("stats", value.clone());
                value
            }
            Err(_) => (0, Vec::new()),
        },
    };
    axum::Json(api::stats_response(total, &days))
}

async fn api_index(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    let hash = hash_arg(&params);
    if hash.is_empty() || !validate_hash(&hash) {
        return (
            StatusCode::OK,
            axum::Json(api::error_response("invalid input")),
        );
    }
    match state.store.get_torrent(&hash).await {
        Ok(Some(doc)) => (StatusCode::OK, axum::Json(api::torrent_detail(&doc))),
        _ => (
            StatusCode::OK,
            axum::Json(api::error_response("not found")),
        ),
    }
}
