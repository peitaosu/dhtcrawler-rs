mod fake_peer;

use crate::fake_peer::{build_info_dict, info_hash_of, spawn_fake_peer};
use dhtcrawler::common::config::{AppConfig, CrawlerConfig, ReaderConfig};
use dhtcrawler::dht::node::{DhtNode, NodeConfig};
use dhtcrawler::dht::NodeId;
use dhtcrawler::engine::{Crawler, Reader};
use dhtcrawler::store::{MemoryStore, Store};
use std::time::Duration;

#[tokio::test]
async fn bep9_fetch_metadata_from_fake_peer() {
    let info_dict = build_info_dict("ubuntu iso", 4096);
    let hash = info_hash_of(&info_dict);
    let peer = spawn_fake_peer(hash, info_dict.clone()).await;
    let metadata = dhtcrawler::metadata::fetch_metadata(
        peer.addr,
        hash,
        dhtcrawler::metadata::bep9::random_peer_id(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(metadata, info_dict);
}

#[tokio::test]
async fn bep9_rejects_wrong_hash() {
    let info_dict = build_info_dict("other torrent", 4096);
    let hash = info_hash_of(&info_dict);
    let wrong_hash = {
        let mut h = hash;
        h[0] ^= 0xff;
        h
    };
    let peer = spawn_fake_peer(hash, info_dict.clone()).await;
    let result = dhtcrawler::metadata::fetch_metadata(
        peer.addr,
        wrong_hash,
        dhtcrawler::metadata::bep9::random_peer_id(),
        Duration::from_secs(5),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn bep9_multi_piece_metadata() {
    let big_name = "x".repeat(100000);
    let info_dict = build_info_dict(&big_name, 12345);
    let hash = info_hash_of(&info_dict);
    assert!(info_dict.len() > dhtcrawler::metadata::bt_message::METADATA_PIECE_SIZE);
    let peer = spawn_fake_peer(hash, info_dict.clone()).await;
    let metadata = dhtcrawler::metadata::fetch_metadata(
        peer.addr,
        hash,
        dhtcrawler::metadata::bep9::random_peer_id(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(metadata, info_dict);
}

fn test_app_config(crawler_port: u16) -> AppConfig {
    let mut config = AppConfig::default();
    config.crawler = CrawlerConfig {
        start_port: crawler_port,
        node_count: 1,
        hash_max_cache: 1,
        cache_max_time_secs: 1,
        query_interval_secs: 600,
        state_dir: std::env::temp_dir()
            .join(format!("e2e-state-{}", std::process::id()))
            .to_string_lossy()
            .to_string(),
        bootstrap: vec![format!("127.0.0.1:{}", crawler_port)],
    };
    config.reader = ReaderConfig {
        max_download_cache: 100,
        max_download_per_reader: 10,
        save_torrent: true,
        save_to_file: false,
        save_to_db: true,
        load_from_db: false,
        torrent_path: std::env::temp_dir()
            .join(format!("e2e-tors-{}", std::process::id()))
            .to_string_lossy()
            .to_string(),
        text_seg: dhtcrawler::common::config::SegMethod::Simple,
    };
    config
}

async fn send_dht_queries_to_crawler(
    crawler_port: u16,
    info_hash: [u8; 20],
    bt_peer_port: u16,
) {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket
        .connect(format!("127.0.0.1:{}", crawler_port))
        .await
        .unwrap();
    let client_id = dhtcrawler::dht::NodeId::random();
    let get_peers = dhtcrawler::dht::message::encode_query(
        &dhtcrawler::dht::message::Method::GetPeers.make_tid(1),
        dhtcrawler::dht::message::Method::GetPeers,
        &client_id,
        &dhtcrawler::dht::message::QueryArgs {
            info_hash: Some(dhtcrawler::dht::NodeId(info_hash)),
            ..Default::default()
        },
    );
    socket.send(&get_peers).await.unwrap();
    let mut buffer = vec![0u8; 2048];
    let len = socket.recv(&mut buffer).await.unwrap();
    let dhtcrawler::dht::message::Inbound::Response { values, .. } =
        dhtcrawler::dht::message::decode(&buffer[..len]).unwrap()
    else {
        panic!("expected get_peers response");
    };
    let token = values.token.expect("token in response");
    let announce = dhtcrawler::dht::message::encode_query(
        &dhtcrawler::dht::message::Method::AnnouncePeer.make_tid(2),
        dhtcrawler::dht::message::Method::AnnouncePeer,
        &client_id,
        &dhtcrawler::dht::message::QueryArgs {
            info_hash: Some(dhtcrawler::dht::NodeId(info_hash)),
            port: Some(bt_peer_port),
            token: Some(token),
            ..Default::default()
        },
    );
    socket.send(&announce).await.unwrap();
    let len = socket.recv(&mut buffer).await.unwrap();
    assert!(
        dhtcrawler::dht::message::decode(&buffer[..len]).is_ok(),
        "announce_peer response decodes"
    );
}

#[tokio::test]
async fn full_pipeline_dht_to_search() {
    let crawler_port = 19401u16;
    let http_port = 19481u16;
    let config = test_app_config(crawler_port);
    let store = std::sync::Arc::new(MemoryStore::new());

    let info_dict = build_info_dict("EndToEnd Torrent 1080p", 4096);
    let hash = info_hash_of(&info_dict);
    let hash_hex = dhtcrawler::common::hex::encode_upper(&hash);
    let bt_peer = spawn_fake_peer(hash, info_dict.clone()).await;

    let crawler = Crawler::start(&config.crawler, store.clone()).await.unwrap();
    let reader = std::sync::Arc::new(Reader::new(
        store.clone(),
        config.reader.clone(),
        crawler.peer_source.clone(),
    ));
    tokio::spawn({
        let reader = reader.clone();
        async move {
            reader.run().await;
        }
    });
    tokio::spawn({
        let store = store.clone();
        async move {
            dhtcrawler::httpd::run(store, "127.0.0.1", http_port).await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    send_dht_queries_to_crawler(crawler_port, hash, bt_peer.addr.port()).await;

    let mut torrent_found = false;
    for _ in 0..60 {
        if store.get_torrent(&hash_hex).await.unwrap().is_some() {
            torrent_found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(torrent_found, "torrent should be saved by the reader");
    let doc = store.get_torrent(&hash_hex).await.unwrap().unwrap();
    assert_eq!(doc.name, "EndToEnd Torrent 1080p");
    assert!(doc.name_array.contains(&"EndToEnd".to_string()));

    let today = dhtcrawler::common::time_util::now_day_seconds();
    let stats = store.stats_day(today).await.unwrap().unwrap();
    assert!(stats.recv >= 1, "queries were recorded");
    assert!(stats.unique >= 1, "inserted queries were recorded");
    assert!(stats.new >= 1, "new saved was recorded");

    let response = reqwest::get(format!("http://127.0.0.1:{}/api/search?q=EndToEnd", http_port))
        .await
        .unwrap();
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body["results"]
            .as_array()
            .expect("results array")
            .iter()
            .filter(|r| r["hash"] == hash_hex)
            .count(),
        1
    );

    let response = reqwest::get(format!("http://127.0.0.1:{}/search?q=EndToEnd", http_port))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    let stats_api = reqwest::get(format!("http://127.0.0.1:{}/api/stats", http_port))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(stats_api.get("total").is_some());
    assert_eq!(stats_api["stats"].as_array().expect("stats days").len(), 3);

    crawler.save_state().await;
}

#[tokio::test]
async fn two_dht_instances_share_a_network() {
    let port_a = 19510u16;
    let port_b = 19511u16;
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let node_a = DhtNode::start(NodeConfig::new(port_a, NodeId::random()), tx.clone())
        .await
        .unwrap();
    let node_b = DhtNode::start(NodeConfig::new(port_b, NodeId::random()), tx)
        .await
        .unwrap();
    let addr_a: std::net::SocketAddr = format!("127.0.0.1:{}", port_a).parse().unwrap();
    let responder = node_b.ping(addr_a).await.unwrap();
    assert_eq!(responder, node_a.own_id());
    let table = node_b.table_snapshot().await;
    assert!(table.iter().any(|n| n.addr.port() == port_a));
}
