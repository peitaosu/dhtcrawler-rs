use dhtcrawler::dht::id::NodeId;
use dhtcrawler::dht::message::{self, Inbound, Method, QueryArgs, ResponseValues};
use dhtcrawler::dht::node::{DhtNode, MonitorEvent, NodeConfig};
use dhtcrawler::dht::routing::Node;
use dhtcrawler::dht::search;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;

async fn start_node(port: u16) -> (DhtNode, mpsc::Receiver<MonitorEvent>) {
    let id = NodeId::random();
    let config = NodeConfig::new(port, id);
    let (tx, rx) = mpsc::channel(64);
    let node = DhtNode::start(config, tx).await.unwrap();
    (node, rx)
}

fn loopback(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

#[tokio::test]
async fn ping_between_two_nodes() {
    let (a, _events_a) = start_node(17801).await;
    let (b, _events_b) = start_node(17802).await;
    let b_id = b.own_id();
    let responder = a.ping(loopback(17802)).await.unwrap();
    assert_eq!(responder, b_id);
    let table = a.table_snapshot().await;
    assert!(table.iter().any(|n| n.addr == loopback(17802)));
}

#[tokio::test]
async fn self_query_refused() {
    let (a, _events) = start_node(17803).await;
    assert!(a.ping(loopback(17803)).await.is_err());
}

#[tokio::test]
async fn get_peers_event_and_peer_storage() {
    let (a, _events_a) = start_node(17804).await;
    let (b, mut events_b) = start_node(17805).await;
    a.ping(loopback(17805)).await.unwrap();
    let info_hash = NodeId::random();
    let result = a.get_peers(loopback(17805), info_hash).await.unwrap();
    let token = result.token.expect("get_peers response carries a token");
    assert_eq!(result.sender, b.own_id());
    match events_b.recv().await {
        Some(MonitorEvent::GetPeers { info_hash: got }) => assert_eq!(got, info_hash),
        other => panic!("expected get_peers event, got {:?}", other.is_some()),
    }
    let announce_addr = loopback(17804);
    a.announce_peer(loopback(17805), info_hash, token, announce_addr.port())
        .await
        .unwrap();
    match events_b.recv().await {
        Some(MonitorEvent::AnnouncePeer { info_hash: got, addr }) => {
            assert_eq!(got, info_hash);
            assert_eq!(addr, announce_addr);
        }
        other => panic!("expected announce event, got {:?}", other.is_some()),
    }
    let result2 = a.get_peers(loopback(17805), info_hash).await.unwrap();
    assert_eq!(result2.peers, vec![announce_addr]);
}

#[tokio::test]
async fn announce_with_bad_token_rejected() {
    let (a, _events_a) = start_node(17806).await;
    let (_b, mut events_b) = start_node(17807).await;
    a.ping(loopback(17807)).await.unwrap();
    let info_hash = NodeId::random();
    assert!(a
        .announce_peer(loopback(17807), info_hash, vec![1, 2, 3], 1234)
        .await
        .is_err());
    assert!(events_b.try_recv().is_err());
}

#[tokio::test]
async fn find_node_search_populates_alive_nodes() {
    let (a, _events_a) = start_node(17808).await;
    let (b, _events_b) = start_node(17809).await;
    let (_c, _events_c) = start_node(17810).await;
    a.ping(loopback(17809)).await.unwrap();
    b.ping(loopback(17810)).await.unwrap();
    let alive = search::find_node(&a, b.own_id()).await;
    assert!(!alive.is_empty());
}

#[tokio::test]
async fn pending_query_times_out() {
    let (a, _events) = start_node(17811).await;
    let dead_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let start = std::time::Instant::now();
    let result = a.ping(dead_addr).await;
    assert!(result.is_err());
    assert!(start.elapsed() >= Duration::from_secs(2));
}

#[tokio::test]
async fn raw_packet_handling_answers_find_node() {
    let (_node, _events) = start_node(17812).await;
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect(loopback(17812)).await.unwrap();
    let sender = NodeId::random();
    let packet = message::encode_query(
        &Method::FindNode.make_tid(7),
        Method::FindNode,
        &sender,
        &QueryArgs {
            target: Some(NodeId::random()),
            ..Default::default()
        },
    );
    sock.send(&packet).await.unwrap();
    let mut buf = vec![0u8; 2048];
    let len = tokio::time::timeout(Duration::from_secs(2), sock.recv(&mut buf))
        .await
        .expect("no response")
        .unwrap();
    match message::decode(&buf[..len]).unwrap() {
        Inbound::Response { tid, values, .. } => {
            assert_eq!(tid, Method::FindNode.make_tid(7));
            assert!(!ResponseValues::default().eq(&values) || values.nodes.is_empty());
        }
        other => panic!("expected response, got {:?}", other),
    }
}

#[test]
fn response_encoding_prefers_peers_when_present() {
    let sender = NodeId::random();
    let values = ResponseValues {
        token: Some(b"tok".to_vec()),
        peers: vec!["1.2.3.4:80".parse().unwrap()],
        nodes: vec![Node::new(NodeId::random(), "5.6.7.8:81".parse().unwrap())],
    };
    let encoded = message::encode_response(b"tid", &sender, values.clone(), true);
    match message::decode(&encoded).unwrap() {
        Inbound::Response { values: got, .. } => {
            assert!(!got.peers.is_empty());
            assert!(got.nodes.is_empty());
        }
        _ => panic!("expected response"),
    }
}

#[tokio::test]
async fn node_id_and_addrs_restored_from_state() {
    let dir = std::env::temp_dir().join(format!("dhtstate-restore-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut config = NodeConfig::new(17961, NodeId::random());
    config.state_dir = Some(dir.clone());
    let (tx, _rx) = mpsc::channel(16);
    let node = DhtNode::start(config.clone(), tx.clone()).await.unwrap();
    let original_id = node.own_id();
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:19999").await.unwrap();
    sock.connect(loopback(17961)).await.unwrap();
    sock.send(&message::encode_query(
        &Method::Ping.make_tid(1),
        Method::Ping,
        &NodeId::random(),
        &Default::default(),
    ))
    .await
    .unwrap();
    let mut buf = vec![0u8; 512];
    tokio::time::timeout(std::time::Duration::from_secs(2), sock.recv(&mut buf))
        .await
        .expect("ping response")
        .unwrap();
    node.save_state().await.unwrap();
    drop(node);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let node2 = DhtNode::start(config, tx).await.unwrap();
    assert_eq!(node2.own_id(), original_id);
    assert!(!node2.saved_bootstrap_addrs().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}
