use crate::dht::id::NodeId;
use crate::dht::routing::Node;
use crate::common::bencode::{self, Value};
use std::net::{IpAddr, SocketAddr};

pub const PROTOCOL_NODE_SIZE: usize = 26;
pub const PROTOCOL_PEER_SIZE: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

impl Method {
    pub fn tag(&self) -> &'static str {
        match self {
            Method::Ping => "ping",
            Method::FindNode => "find_node",
            Method::GetPeers => "get_peers",
            Method::AnnouncePeer => "announce_peer",
        }
    }

    fn tid_prefix(&self) -> [u8; 2] {
        match self {
            Method::Ping => *b"pn",
            Method::FindNode => *b"fn",
            Method::GetPeers => *b"gp",
            Method::AnnouncePeer => *b"an",
        }
    }

    pub fn make_tid(&self, sequence: u16) -> Vec<u8> {
        let mut tid = Vec::with_capacity(4);
        tid.extend_from_slice(&self.tid_prefix());
        tid.extend_from_slice(&sequence.to_be_bytes());
        tid
    }

    pub fn from_tag(tag: &[u8]) -> Option<Method> {
        match tag {
            b"ping" => Some(Method::Ping),
            b"find_node" => Some(Method::FindNode),
            b"get_peers" => Some(Method::GetPeers),
            b"announce_peer" => Some(Method::AnnouncePeer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryArgs {
    pub target: Option<NodeId>,
    pub info_hash: Option<NodeId>,
    pub token: Option<Vec<u8>>,
    pub port: Option<u16>,
    pub implied_port: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseValues {
    pub nodes: Vec<Node>,
    pub peers: Vec<SocketAddr>,
    pub token: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum Inbound {
    Query {
        tid: Vec<u8>,
        method: Method,
        sender: NodeId,
        args: QueryArgs,
    },
    Response {
        tid: Vec<u8>,
        sender: NodeId,
        values: ResponseValues,
    },
    Error {
        tid: Vec<u8>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("decode error: {0}")]
    Decode(String),
    #[error("invalid field: {0}")]
    InvalidField(String),
}

pub fn encode_query(tid: &[u8], method: Method, sender: &NodeId, args: &QueryArgs) -> Vec<u8> {
    let mut a = vec![(b"id".to_vec(), Value::Bytes(sender.0.to_vec()))];
    if let Some(target) = args.target {
        a.push((b"target".to_vec(), Value::Bytes(target.0.to_vec())));
    }
    if let Some(info_hash) = args.info_hash {
        a.push((b"info_hash".to_vec(), Value::Bytes(info_hash.0.to_vec())));
    }
    if let Some(token) = &args.token {
        a.push((b"token".to_vec(), Value::Bytes(token.clone())));
    }
    if let Some(port) = args.port {
        a.push((b"port".to_vec(), Value::Int(port as i64)));
    }
    if let Some(implied) = args.implied_port {
        a.push((b"implied_port".to_vec(), Value::Int(implied as i64)));
    }
    a.sort_by(|x, y| x.0.cmp(&y.0));
    bencode::encode(&Value::Dict(sorted(vec![
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        (b"q".to_vec(), Value::Bytes(method.tag().as_bytes().to_vec())),
        (b"a".to_vec(), Value::Dict(a)),
    ])))
}

pub fn encode_ping_response(tid: &[u8], sender: &NodeId) -> Vec<u8> {
    encode_response(tid, sender, ResponseValues::default(), false)
}

pub fn encode_response(tid: &[u8], sender: &NodeId, values: ResponseValues, include_peers: bool) -> Vec<u8> {
    let mut r = vec![(b"id".to_vec(), Value::Bytes(sender.0.to_vec()))];
    if let Some(token) = &values.token {
        r.push((b"token".to_vec(), Value::Bytes(token.clone())));
    }
    if include_peers && !values.peers.is_empty() {
        let peers: Vec<Value> = values
            .peers
            .iter()
            .map(|addr| Value::Bytes(encode_peer(addr)))
            .collect();
        r.push((b"values".to_vec(), Value::List(peers)));
    } else if !values.nodes.is_empty() {
        r.push((b"nodes".to_vec(), Value::Bytes(encode_nodes(&values.nodes))));
    }
    r.sort_by(|x, y| x.0.cmp(&y.0));
    bencode::encode(&Value::Dict(sorted(vec![
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        (b"r".to_vec(), Value::Dict(r)),
    ])))
}

pub fn encode_error(tid: &[u8], code: i64, message: &str) -> Vec<u8> {
    bencode::encode(&Value::Dict(sorted(vec![
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"e".to_vec())),
        (b"e".to_vec(), Value::List(vec![Value::Int(code), Value::Bytes(message.as_bytes().to_vec())])),
    ])))
}

fn sorted(mut pairs: Vec<(Vec<u8>, Value)>) -> Vec<(Vec<u8>, Value)> {
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

pub fn decode(data: &[u8]) -> Result<Inbound, MessageError> {
    let value = bencode::decode(data).map_err(|e| MessageError::Decode(e.to_string()))?;
    let dict = value.as_dict().ok_or_else(|| MessageError::Decode("not a dict".into()))?;
    let tid = value
        .str_key("t")
        .and_then(|v| v.as_bytes())
        .ok_or_else(|| MessageError::InvalidField("missing tid".into()))?
        .to_vec();
    let y = value
        .str_key("y")
        .and_then(|v| v.as_bytes())
        .ok_or_else(|| MessageError::InvalidField("missing message type".into()))?;
    match y {
        b"q" => decode_query(&value, tid, dict),
        b"r" => decode_response(&value, tid),
        b"e" => Ok(Inbound::Error { tid }),
        _ => Err(MessageError::InvalidField("unknown message type".into())),
    }
}

fn decode_query(value: &Value, tid: Vec<u8>, _dict: &[(Vec<u8>, Value)]) -> Result<Inbound, MessageError> {
    let method_tag = value
        .str_key("q")
        .and_then(|v| v.as_bytes())
        .ok_or_else(|| MessageError::InvalidField("missing query method".into()))?;
    let method = Method::from_tag(method_tag)
        .ok_or_else(|| MessageError::InvalidField(format!("unknown method {:?}", method_tag)))?;
    let args_value = value
        .str_key("a")
        .ok_or_else(|| MessageError::InvalidField("missing query args".into()))?;
    let sender = required_id(args_value, "id")?;
    let args = QueryArgs {
        target: optional_id(args_value, "target"),
        info_hash: optional_id(args_value, "info_hash"),
        token: args_value
            .str_key("token")
            .and_then(|v| v.as_bytes())
            .map(|b| b.to_vec()),
        port: args_value.str_key("port").and_then(|v| v.as_int()).map(|p| p as u16),
        implied_port: args_value
            .str_key("implied_port")
            .and_then(|v| v.as_int())
            .map(|v| v != 0),
    };
    if matches!(method, Method::GetPeers | Method::AnnouncePeer) && args.info_hash.is_none() {
        return Err(MessageError::InvalidField("missing info_hash".into()));
    }
    Ok(Inbound::Query { tid, method, sender, args })
}

fn decode_response(value: &Value, tid: Vec<u8>) -> Result<Inbound, MessageError> {
    let r = value
        .str_key("r")
        .ok_or_else(|| MessageError::InvalidField("missing response body".into()))?;
    let sender = required_id(r, "id")?;
    let mut values = ResponseValues::default();
    if let Some(nodes_buf) = r.str_key("nodes").and_then(|v| v.as_bytes()) {
        values.nodes = decode_nodes(nodes_buf)?;
    }
    if let Some(token) = r.str_key("token").and_then(|v| v.as_bytes()) {
        values.token = Some(token.to_vec());
    }
    if let Some(peer_list) = r.str_key("values").and_then(|v| v.as_list()) {
        for item in peer_list {
            if let Some(buf) = item.as_bytes() {
                values.peers.push(decode_peer(buf)?);
            }
        }
    }
    Ok(Inbound::Response { tid, sender, values })
}

fn required_id(value: &Value, key: &str) -> Result<NodeId, MessageError> {
    let id = value
        .str_key(key)
        .and_then(|v| v.as_bytes())
        .ok_or_else(|| MessageError::InvalidField(format!("missing {}", key)))?;
    NodeId::from_bytes(id).ok_or_else(|| MessageError::InvalidField(format!("bad {}", key)))
}

fn optional_id(value: &Value, key: &str) -> Option<NodeId> {
    value.str_key(key).and_then(|v| v.as_bytes()).and_then(NodeId::from_bytes)
}

pub fn encode_peer(addr: &SocketAddr) -> Vec<u8> {
    match addr.ip() {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    }
    .into_iter()
    .chain(addr.port().to_be_bytes())
    .collect()
}

pub fn decode_peer(data: &[u8]) -> Result<SocketAddr, MessageError> {
    if data.len() == PROTOCOL_PEER_SIZE {
        let octets: [u8; 4] = data[..4].try_into().unwrap();
        let port = u16::from_be_bytes(data[4..6].try_into().unwrap());
        return Ok(SocketAddr::from((std::net::Ipv4Addr::from(octets), port)));
    }
    if data.len() == 18 {
        let octets: [u8; 16] = data[..16].try_into().unwrap();
        let port = u16::from_be_bytes(data[16..18].try_into().unwrap());
        return Ok(SocketAddr::from((std::net::Ipv6Addr::from(octets), port)));
    }
    Err(MessageError::InvalidField("bad compact peer size".into()))
}

pub fn encode_node(node: &Node) -> Vec<u8> {
    node.id
        .0
        .to_vec()
        .into_iter()
        .chain(encode_peer(&node.addr))
        .collect()
}

pub fn encode_nodes(nodes: &[Node]) -> Vec<u8> {
    nodes.iter().flat_map(encode_node).collect()
}

pub fn decode_nodes(data: &[u8]) -> Result<Vec<Node>, MessageError> {
    let mut nodes = Vec::new();
    if data.is_empty() {
        return Ok(nodes);
    }
    if data.len() % PROTOCOL_NODE_SIZE == 0 {
        for chunk in data.chunks(PROTOCOL_NODE_SIZE) {
            push_compact_node(chunk, &mut nodes)?;
        }
        return Ok(nodes);
    }
    if data.len() % (20 + 18) == 0 {
        for chunk in data.chunks(20 + 18) {
            push_compact_node(chunk, &mut nodes)?;
        }
        return Ok(nodes);
    }
    Err(MessageError::InvalidField("bad compact node list".into()))
}

fn push_compact_node(chunk: &[u8], nodes: &mut Vec<Node>) -> Result<(), MessageError> {
    let id = NodeId::from_bytes(&chunk[..20])
        .ok_or_else(|| MessageError::InvalidField("bad node id".into()))?;
    let addr = decode_peer(&chunk[20..])?;
    nodes.push(Node::new(id, addr));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_id(byte: u8) -> NodeId {
        let mut bytes = [byte; 20];
        bytes[19] = byte ^ 0x55;
        NodeId(bytes)
    }

    #[test]
    fn tid_format() {
        let tid = Method::GetPeers.make_tid(0x1234);
        assert_eq!(tid, b"gp\x12\x34".to_vec());
    }

    #[test]
    fn query_roundtrip() {
        let sender = sample_id(1);
        let args = QueryArgs {
            info_hash: Some(sample_id(2)),
            ..Default::default()
        };
        let encoded = encode_query(&Method::GetPeers.make_tid(1), Method::GetPeers, &sender, &args);
        let Inbound::Query { tid, method, sender: got_sender, args: got_args } = decode(&encoded).unwrap()
        else {
            panic!("not a query");
        };
        assert_eq!(tid, Method::GetPeers.make_tid(1));
        assert_eq!(method, Method::GetPeers);
        assert_eq!(got_sender, sender);
        assert_eq!(got_args.info_hash, args.info_hash);
    }

    #[test]
    fn response_roundtrip_with_nodes() {
        let sender = sample_id(3);
        let values = ResponseValues {
            token: Some(b"token".to_vec()),
            nodes: vec![Node::new(sample_id(4), "1.2.3.4:6881".parse().unwrap())],
            ..Default::default()
        };
        let encoded = encode_response(b"tid1", &sender, values, false);
        let Inbound::Response { values: got, .. } = decode(&encoded).unwrap() else {
            panic!("not a response");
        };
        assert_eq!(got.token.as_deref(), Some(b"token".as_slice()));
        assert_eq!(got.nodes.len(), 1);
        assert_eq!(got.nodes[0].addr.to_string(), "1.2.3.4:6881");
    }

    #[test]
    fn response_roundtrip_with_peers() {
        let sender = sample_id(5);
        let values = ResponseValues {
            token: Some(b"t".to_vec()),
            peers: vec!["5.6.7.8:1234".parse().unwrap()],
            ..Default::default()
        };
        let encoded = encode_response(b"tid2", &sender, values, true);
        let Inbound::Response { values: got, .. } = decode(&encoded).unwrap() else {
            panic!("not a response");
        };
        assert_eq!(got.peers, vec!["5.6.7.8:1234".parse().unwrap()]);
        assert!(got.nodes.is_empty());
    }

    #[test]
    fn announce_query_roundtrip() {
        let sender = sample_id(6);
        let args = QueryArgs {
            info_hash: Some(sample_id(7)),
            port: Some(6881),
            token: Some(vec![1, 2, 3]),
            implied_port: Some(true),
            ..Default::default()
        };
        let encoded = encode_query(&Method::AnnouncePeer.make_tid(9), Method::AnnouncePeer, &sender, &args);
        let Inbound::Query { method, args: got, .. } = decode(&encoded).unwrap() else {
            panic!("not a query");
        };
        assert_eq!(method, Method::AnnouncePeer);
        assert_eq!(got.port, Some(6881));
        assert_eq!(got.implied_port, Some(true));
        assert_eq!(got.token.as_deref(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn compact_peer_codec() {
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        let encoded = encode_peer(&addr);
        assert_eq!(encoded.len(), 6);
        assert_eq!(decode_peer(&encoded).unwrap(), addr);
        assert!(decode_peer(&[1, 2, 3]).is_err());
    }

    #[test]
    fn compact_nodes_codec() {
        let nodes = vec![
            Node::new(sample_id(8), "1.1.1.1:1".parse().unwrap()),
            Node::new(sample_id(9), "2.2.2.2:2".parse().unwrap()),
        ];
        let encoded = encode_nodes(&nodes);
        assert_eq!(encoded.len(), 52);
        let decoded = decode_nodes(&encoded).unwrap();
        assert_eq!(decoded.len(), nodes.len());
        for (got, want) in decoded.iter().zip(nodes.iter()) {
            assert_eq!(got.id, want.id);
            assert_eq!(got.addr, want.addr);
        }
        assert!(decode_nodes(&[0u8; 5]).is_err());
    }

    #[test]
    fn compact_nodes_tolerates_ipv6_and_rejects_mixed() {
        let node6 = Node::new(sample_id(10), "[2001:db8::1]:6881".parse().unwrap());
        let mut encoded = encode_node(&node6);
        assert_eq!(encoded.len(), 38);
        let decoded = decode_nodes(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].addr, "[2001:db8::1]:6881".parse::<std::net::SocketAddr>().unwrap());
        encoded.extend_from_slice(&[1, 2, 3]);
        assert!(decode_nodes(&encoded).is_err());
        let v4 = encode_node(&Node::new(sample_id(11), "3.3.3.3:3".parse().unwrap()));
        let mixed = [v4.as_slice(), &[0xffu8; 7]].concat();
        assert!(decode_nodes(&mixed).is_err());
    }

    #[test]
    fn error_message() {
        let encoded = encode_error(b"tx", 203, "protocol error");
        assert!(matches!(decode(&encoded).unwrap(), Inbound::Error { .. }));
    }

    #[test]
    fn garbage_rejected() {
        assert!(decode(b"not bencode").is_err());
        assert!(decode(b"i42e").is_err());
    }
}
