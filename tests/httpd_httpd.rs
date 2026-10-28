use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use dhtcrawler::metadata::FileEntry;
use dhtcrawler::store::{MemoryStore, Store, TorrentDoc};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn sample_doc(hash_letter: char, name: &str, announce: u64, created: i64) -> TorrentDoc {
    TorrentDoc {
        hash: hash_letter.to_string().repeat(40),
        name: name.to_string(),
        length: 100,
        created_at: created,
        announce,
        files: vec![FileEntry {
            path: name.to_string(),
            length: 100,
        }],
        name_array: vec![name.to_string()],
    }
}

async fn setup() -> (Arc<MemoryStore>, Router) {
    let store = Arc::new(MemoryStore::new());
    store.insert_torrent(sample_doc('A', "ubuntu 22.04", 9, 1000)).await.unwrap();
    store.insert_torrent(sample_doc('B', "debian iso", 2, 2000)).await.unwrap();
    let state = Arc::new(dhtcrawler::httpd::AppState::new(store.clone()));
    let app = dhtcrawler::httpd::router();
    (store, app.with_state(state))
}

async fn get(router: &Router, path: &str) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, String::from_utf8(body).unwrap())
}

#[tokio::test]
async fn removed_routes_are_gone() {
    let (_store, router) = setup().await;
    for path in ["/", "/search?q=ubuntu", "/top", "/today_top", "/recent", "/stats", "/view?q=A"] {
        let (status, _) = get(&router, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "path {}", path);
    }
}

#[tokio::test]
async fn api_search_json() {
    let (_store, router) = setup().await;
    let (status, body) = get(&router, "/api/search?q=ubuntu").await;
    assert_eq!(status, StatusCode::OK);
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["keyword"], "ubuntu");
    assert_eq!(value["results"].as_array().unwrap().len(), 1);
    assert_eq!(value["results"][0]["hash"], "A".repeat(40));
}

#[tokio::test]
async fn api_search_requires_keyword() {
    let (_store, router) = setup().await;
    let (_status, body) = get(&router, "/api/search").await;
    assert!(body.contains("null input"));
}

#[tokio::test]
async fn api_search_pagination() {
    let (_store, router) = setup().await;
    let (_status, page1) = get(&router, "/api/search?q=ubuntu&p=1").await;
    let value: serde_json::Value = serde_json::from_str(&page1).unwrap();
    assert_eq!(value["page"], 1);
    let (_status, page2) = get(&router, "/api/search?q=ubuntu&p=2").await;
    let value: serde_json::Value = serde_json::from_str(&page2).unwrap();
    assert_eq!(value["page"], 2);
    assert_eq!(value["results"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn api_index_json() {
    let (_store, router) = setup().await;
    let (_status, body) = get(&router, &format!("/api/index?q={}", "A".repeat(40))).await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["name"], "ubuntu 22.04");
    assert_eq!(value["req"], 9);
    let (_status, body) = get(&router, "/api/index?q=zzz").await;
    assert!(body.contains("invalid input"));
    let (_status, body) = get(&router, &format!("/api/index?q={}", "F".repeat(40))).await;
    assert!(body.contains("not found"));
}

#[tokio::test]
async fn api_search_returns_all_files_by_default() {
    let (_store, router) = setup().await;
    let (_status, body) = get(&router, "/api/search?q=ubuntu").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["results"][0]["files"].as_array().unwrap().len(), 1);
    assert!(value["results"][0]["files"].as_array().unwrap().iter().all(|f| f["name"] != "__more__"));
}

#[tokio::test]
async fn api_search_files_limit() {
    let (_store, router) = setup().await;
    let (_status, body) = get(&router, "/api/search?q=ubuntu&files=1").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["results"][0]["files"].as_array().unwrap().len(), 1);
    let (_status, body) = get(&router, "/api/search?q=ubuntu&files=0").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["results"][0]["files"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn api_today_top_limit() {
    let (store, router) = setup().await;
    store.daterange_insert(&"A".repeat(40), 3, true).await.unwrap();
    let (_status, body) = get(&router, "/api/today_top?limit=1").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["results"].as_array().unwrap().len(), 1);
    assert_eq!(value["results"][0]["req"], 3);
    let (_status, body) = get(&router, "/api/today_top?limit=0").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["results"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn api_stats_json() {
    let (_store, router) = setup().await;
    let (_status, body) = get(&router, "/api/stats").await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["total"], 2);
    assert_eq!(value["stats"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn search_cache_serves_repeats() {
    let (_store, router) = setup().await;
    let first = get(&router, "/api/search?q=ubuntu").await;
    let second = get(&router, "/api/search?q=ubuntu").await;
    assert_eq!(first.1, second.1);
}
